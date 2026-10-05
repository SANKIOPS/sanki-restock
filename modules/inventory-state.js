'use strict';
const fs = require('fs');
const path = require('path');
const { shopifyClient } = require('./shopify-client');
const API = '2026-07';
const names = ['available', 'on_hand', 'committed', 'quality_control', 'damaged', 'reserved', 'safety_stock'];
const dataDir = process.env.DATA_PATH ? path.dirname(process.env.DATA_PATH) : path.join(__dirname, '..');
let cached, inflight, generation = 0;
function fail(message, status = 409) { const e = new Error(message); e.status = status; throw e; }
async function graphql(query, variables, client = shopifyClient) {
  for (let attempt = 0; attempt < 5; attempt++) {
    const r = await client.request(`https://${client.store}/admin/api/${API}/graphql.json`, { method: 'POST', body: JSON.stringify({ query, variables }), timeout: 20000 });
    const d = await r.json();
    if (attempt < 4 && d.errors?.some(e => e.extensions?.code === 'THROTTLED')) {
      const cost = d.extensions?.cost, limit = cost?.throttleStatus;
      const delay = Math.min(10000, Math.max(1000, Math.ceil(((cost?.requestedQueryCost || 100) - (limit?.currentlyAvailable || 0)) / (limit?.restoreRate || 50) * 1000) + 250));
      await client.sleep(delay); continue;
    }
    if (!r.ok || d.errors?.length || !d.data) fail('Shopify inventory could not be confirmed. ' + (d.errors?.[0]?.message || `Response ${r.status}`), 502);
    return d.data;
  }
}
function locationMapping() {
  const file = path.join(dataDir, 'showroom-settings.json');
  if (!fs.existsSync(file)) return {};
  const s = JSON.parse(fs.readFileSync(file, 'utf8'));
  return { Display: s.frontLocationId ? `gid://shopify/Location/${s.frontLocationId}` : '', Warehouse: s.backLocationId ? `gid://shopify/Location/${s.backLocationId}` : '' };
}
function normalizeItem(n) {
  if (!n?.variant?.product) return null;
  const levels = n.inventoryLevels.nodes.map(l => {
    const quantities = Object.fromEntries(l.quantities.map(q => [q.name, q.quantity]));
    if (!names.every(k => Number.isSafeInteger(quantities[k]))) fail('Shopify returned incomplete inventory quantities.', 502);
    return { locationId: l.location.id, location: l.location.name, ...quantities };
  });
  return { id: n.id, sku: (n.sku || '').trim().toUpperCase(), tracked: n.tracked, variantId: n.variant.id, variant: n.variant.title, inventoryPolicy: n.variant.inventoryPolicy, unitCost: Number(n.unitCost?.amount) || 0, product: n.variant.product, levels };
}
const levelFields = 'nodes{location{id name} quantities(names:$names){name quantity}} pageInfo{hasNextPage endCursor}';
async function fetchSnapshot(client = shopifyClient) {
  const items = []; let after = null;
  do {
    const d = await graphql(`query($after:String,$names:[String!]!){inventoryItems(first:50,after:$after){nodes{id sku tracked unitCost{amount} variant{id title inventoryPolicy product{id handle title productType tags status featuredImage{url}}} inventoryLevels(first:2){${levelFields}}} pageInfo{hasNextPage endCursor}}}`, { after, names }, client);
    const page = d.inventoryItems;
    if (!page?.nodes || !page.pageInfo) fail('Shopify inventory page is incomplete.', 502);
    for (const n of page.nodes) {
      let pi = n.inventoryLevels.pageInfo;
      while (pi.hasNextPage) {
        const more = await graphql(`query($id:ID!,$after:String,$names:[String!]!){inventoryItem(id:$id){inventoryLevels(first:100,after:$after){${levelFields}}}}`, { id: n.id, after: pi.endCursor, names }, client);
        const levels = more.inventoryItem?.inventoryLevels;
        if (!levels?.nodes || !levels.pageInfo || (levels.pageInfo.hasNextPage && levels.pageInfo.endCursor === pi.endCursor)) fail('Shopify location pagination is incomplete.', 502);
        n.inventoryLevels.nodes.push(...levels.nodes); pi = levels.pageInfo;
      }
      const item = normalizeItem(n); if (item) items.push(item);
    }
    if (page.pageInfo.hasNextPage && (!page.pageInfo.endCursor || page.pageInfo.endCursor === after)) fail('Shopify inventory pagination is incomplete.', 502);
    after = page.pageInfo.hasNextPage ? page.pageInfo.endCursor : null;
  } while (after);
  return { at: new Date().toISOString(), items, mapping: locationMapping() };
}
async function snapshot(force = false) {
  if (!force && cached && Date.now() - Date.parse(cached.at) < 60000) return cached;
  if (inflight) { await inflight; if (force) return snapshot(true); if (cached) return cached; }
  const version = generation;
  inflight = fetchSnapshot().then(s => { if (version === generation) cached = s; return s; });
  let result;
  try { result = await inflight; } finally { inflight = null; }
  if (version !== generation) return snapshot(false);
  return result;
}
function invalidate() { generation++; cached = null; }
function quantities(item, mapping = {}) {
  const out = { displayQty: 0, warehouseQty: 0, otherQty: 0, availableQty: 0, cleaningQty: 0, notForSaleQty: 0, otherUnavailableQty: 0, committedQty: 0, totalQty: 0 };
  for (const l of item.levels) {
    const cleaning = item.careCleaning?.[l.locationId] || 0;
    out.availableQty += l.available; out.totalQty += l.on_hand; out.cleaningQty += cleaning; out.notForSaleQty += l.damaged;
    out.committedQty += l.committed; out.otherUnavailableQty += l.reserved + l.safety_stock + l.quality_control - cleaning;
    if (l.locationId === mapping.Display) out.displayQty += l.available;
    else if (l.locationId === mapping.Warehouse) out.warehouseQty += l.available;
    else out.otherQty += l.available;
  }
  return out;
}
function catalog(s, physical) {
  s = withCare(s);
  const old = new Map(physical.map(p => [p.handle, p])); const products = new Map();
  for (const item of s.items) {
    const p = item.product, base = old.get(p.handle) || {};
    if (!products.has(p.handle)) products.set(p.handle, { ...base, handle: p.handle, title: p.title, productType: base.productType || p.productType || 'Uncategorized', category: base.category || p.productType || 'Uncategorized', collection: base.collection || 'Uncategorized', image: p.featuredImage?.url || base.image || null, images: p.featuredImage?.url ? [p.featuredImage.url] : (base.images || []), variants: [] });
    const previous = (base.variants || []).find(v => String(v.sku).trim().toUpperCase() === item.sku) || {};
    products.get(p.handle).variants.push({ ...previous, sku: item.sku, variant: item.variant, inventoryItemId: item.id, ...quantities(item, s.mapping) });
  }
  return [...products.values()].map(p => {
    for (const k of Object.keys(quantities({ levels: [] }))) p[k] = p.variants.reduce((n, v) => n + v[k], 0);
    return p;
  });
}
function withCare(s, register = require('./inventory-care-store').load()) {
  const cleaning = new Map();
  for (const b of register.batches) for (const l of require('./inventory-care-store').balances(register, b)) {
    const key = l.inventoryItemId + ':' + l.locationId; cleaning.set(key, (cleaning.get(key) || 0) + l.cleaning);
  }
  return { ...s, items: s.items.map(i => ({ ...i, careCleaning: Object.fromEntries(i.levels.map(l => [l.locationId, cleaning.get(i.id + ':' + l.locationId) || 0])) })) };
}
module.exports = { graphql, fail, snapshot, fetchSnapshot, invalidate, quantities, catalog, normalizeItem, withCare };

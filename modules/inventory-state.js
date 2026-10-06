'use strict';
const fs = require('fs');
const path = require('path');
const { ShopifyClient } = require('./shopify-client');
// Inventory GraphQL reads must not wait behind long REST order backfills in
// the shared client's queue. GraphQL throttling is handled below using Shopify's
// advertised query budget; care writes still share the stock-movement serial lock.
const inventoryClient = new ShopifyClient({ minIntervalMs: 250 });
const API = '2026-07';
const names = ['available', 'on_hand', 'committed', 'quality_control', 'damaged', 'reserved', 'safety_stock'];
const dataDir = process.env.DATA_PATH ? path.dirname(process.env.DATA_PATH) : path.join(__dirname, '..');
const metadata = require('./inventory-metadata');
const snapshotCache = require('./inventory-snapshot-cache').createSnapshotCache({ fetchSnapshot: () => fetchSnapshot(), file: process.env.INVENTORY_SNAPSHOT_PATH || path.join(dataDir, 'inventory-snapshot.json') });
const enrichMetadata = metadata.createMetadataReader({ physical: require('../public/inventory-data.json'), query: (query, variables) => graphql(query, variables) });
function fail(message, status = 409) { const e = new Error(message); e.status = status; throw e; }
async function graphql(query, variables, client = inventoryClient) {
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
const itemFields = `id sku tracked unitCost{amount} variant{id title inventoryPolicy product{id handle title productType tags status featuredImage{url}}} inventoryLevels(first:2){${levelFields}}`;
async function completeItem(n, client) {
  if (!n) return null;
  let pi = n.inventoryLevels.pageInfo;
  while (pi.hasNextPage) {
    const more = await graphql(`query($id:ID!,$after:String,$names:[String!]!){inventoryItem(id:$id){inventoryLevels(first:100,after:$after){${levelFields}}}}`, { id: n.id, after: pi.endCursor, names }, client);
    const levels = more.inventoryItem?.inventoryLevels;
    if (!levels?.nodes || !levels.pageInfo || (levels.pageInfo.hasNextPage && levels.pageInfo.endCursor === pi.endCursor)) fail('Shopify location pagination is incomplete.', 502);
    n.inventoryLevels.nodes.push(...levels.nodes); pi = levels.pageInfo;
  }
  return normalizeItem(n);
}
async function fetchSnapshot(client = inventoryClient, query = null) {
  const items = []; let after = null;
  do {
    const d = await graphql(`query($after:String,$query:String,$names:[String!]!){inventoryItems(first:50,after:$after,query:$query){nodes{${itemFields}} pageInfo{hasNextPage endCursor}}}`, { after, query, names }, client);
    const page = d.inventoryItems;
    if (!page?.nodes || !page.pageInfo) fail('Shopify inventory page is incomplete.', 502);
    for (const n of page.nodes) {
      const item = await completeItem(n, client); if (item) items.push(item);
    }
    if (page.pageInfo.hasNextPage && (!page.pageInfo.endCursor || page.pageInfo.endCursor === after)) fail('Shopify inventory pagination is incomplete.', 502);
    after = page.pageInfo.hasNextPage ? page.pageInfo.endCursor : null;
  } while (after);
  // Fetch custom fields once per new product, rather than once per variant on
  // every quantity refresh. Injected test clients can keep their reads isolated.
  const enriched = !query && client === inventoryClient ? await enrichMetadata(items) : items;
  return { at: new Date().toISOString(), items: enriched, mapping: locationMapping() };
}
async function snapshotSkus(skus, client = inventoryClient) {
  const selected = [...new Set(skus.map(s => String(s || '').trim().toUpperCase()))];
  if (!selected.length || selected.length > 50 || selected.some(s => !s || s.length > 100)) fail('Add between 1 and 50 valid SKU lines.', 400);
  const live = await fetchSnapshot(client, selected.map(s => 'sku:' + JSON.stringify(s)).join(' OR '));
  return { ...live, items: live.items.filter(i => selected.includes(i.sku)) };
}
async function snapshotItems(ids, client = inventoryClient) {
  const selected = [...new Set(ids)];
  if (!selected.length || selected.length > 50) fail('Choose between 1 and 50 inventory items.', 400);
  const d = await graphql(`query($ids:[ID!]!,$names:[String!]!){nodes(ids:$ids){... on InventoryItem{${itemFields}}}}`, { ids: selected, names }, client);
  if (!Array.isArray(d.nodes)) fail('Shopify inventory items are incomplete.', 502);
  const items = [];
  for (const n of d.nodes) { const item = await completeItem(n, client); if (item) items.push(item); }
  return { at: new Date().toISOString(), items, mapping: locationMapping() };
}
function snapshot(force = false) { return snapshotCache.snapshot(force); }
function readSnapshot(force = false) { return snapshotCache.read(force); }
function invalidate() { snapshotCache.invalidate(); }
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
function catalog(s, physical, purchases = metadata.loadPurchases()) {
  s = withCare(s);
  const old = new Map(physical.map(p => [p.handle, p])); const products = new Map();
  const postedByHandle = new Map();
  for (const item of s.items) postedByHandle.set(item.product.handle, (postedByHandle.get(item.product.handle) || []).concat(purchases.get(item.sku) || []));
  for (const item of s.items) {
    const p = item.product, base = old.get(p.handle) || {};
    if (!products.has(p.handle)) {
      const posted = postedByHandle.get(p.handle) || [];
      products.set(p.handle, { ...base, ...metadata.resolveMetadata(p, base, posted), handle: p.handle, title: p.title, image: p.featuredImage?.url || base.image || null, images: p.featuredImage?.url ? [p.featuredImage.url] : (base.images || []), variants: [] });
    }
    const previous = (base.variants || []).find(v => String(v.sku).trim().toUpperCase() === item.sku) || {};
    products.get(p.handle).variants.push({ ...previous, sku: item.sku, variant: item.variant, inventoryItemId: item.id, ...quantities(item, s.mapping) });
  }
  return [...products.values()].map(p => {
    for (const k of Object.keys(quantities({ levels: [] }))) p[k] = p.variants.reduce((n, v) => n + v[k], 0);
    return p;
  });
}
function withCare(s, register = require('./inventory-care-store').load()) {
  // A retained display snapshot must use the care balance at that update,
  // rather than overlay a newer confirmation on older Shopify quantities.
  const at = Date.parse(s.at);
  if (Number.isFinite(at)) register = { ...register, operations: register.operations.filter(o => !o.confirmedAt || Date.parse(o.confirmedAt) <= at) };
  const cleaning = new Map();
  for (const b of register.batches) for (const l of require('./inventory-care-store').balances(register, b)) {
    const key = l.inventoryItemId + ':' + l.locationId; cleaning.set(key, (cleaning.get(key) || 0) + l.cleaning);
  }
  return { ...s, items: s.items.map(i => ({ ...i, careCleaning: Object.fromEntries(i.levels.map(l => [l.locationId, cleaning.get(i.id + ':' + l.locationId) || 0])) })) };
}
module.exports = { graphql, fail, snapshot, readSnapshot, snapshotSkus, snapshotItems, fetchSnapshot, invalidate, quantities, catalog, normalizeItem, withCare };

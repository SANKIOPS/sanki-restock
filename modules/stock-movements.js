'use strict';
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const express = require('express');
const { shopifyClient } = require('./shopify-client');
const storePath = process.env.STOCK_MOVEMENTS_PATH || path.join(process.env.DATA_PATH ? path.dirname(process.env.DATA_PATH) : path.join(__dirname, '..'), 'stock_movements.json');
const locations = ['Display', 'Warehouse'];
function fail(message, status = 409) { const e = new Error(message); e.status = status; throw e; }
function canApprove(user) {
  const roles = user?.roles || [user?.role];
  const managers = (process.env.STOCK_MOVEMENT_APPROVERS || '').split(',').map(x => x.trim()).filter(Boolean);
  return roles.some(r => ['owner', 'admin'].includes(r)) || managers.includes(user?.username);
}
function empty() { return { version: 1, baseline: null, positions: [], movements: [] }; }
function load(file = storePath) {
  if (!fs.existsSync(file)) return empty();
  const s = JSON.parse(fs.readFileSync(file, 'utf8'));
  if (s.version !== 1 || !Array.isArray(s.positions) || !Array.isArray(s.movements)) fail('Stock movement store requires repair.', 503);
  return s;
}
function save(s, file = storePath) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  const tmp = file + '.' + crypto.randomUUID() + '.tmp';
  fs.writeFileSync(tmp, JSON.stringify(s, null, 2), { mode: 0o600 });
  fs.renameSync(tmp, file);
}
function ready(s) { return s.baseline?.reconciled === true && !!s.baseline?.reviewedBy && !!s.baseline?.reconciledAt; }
function point(p) {
  if (!p || !locations.includes(p.location) || (p.rack != null && typeof p.rack !== 'string')) fail('Select a valid location and rack.', 400);
  const rack = (p.rack || '').trim();
  if (rack.length > 40) fail('Rack code is too long.', 400);
  return { location: p.location, rack };
}
function same(a, b) { return a.location === b.location && a.rack === b.rack; }
function submit(s, body, user) {
  if (!ready(s)) fail('Reconcile and approve the physical-count baseline before moving stock.');
  if (!user?.username) fail('A named staff login is required.', 403);
  const sku = String(body.sku || '').trim().toUpperCase();
  const quantity = body.quantity;
  const from = point(body.from), to = point(body.to);
  if (!sku || !Number.isSafeInteger(quantity) || quantity < 1 || same(from, to)) fail('Enter a SKU, whole-piece quantity and different destination.', 400);
  if (!/^[a-zA-Z0-9-]{16,80}$/.test(body.requestId || '')) fail('A valid request ID is required.', 400);
  const existing = s.movements.find(m => m.requestId === body.requestId);
  if (existing) {
    if (existing.sku !== sku || existing.quantity !== quantity || !same(existing.from, from) || !same(existing.to, to) || existing.submittedBy !== user.username) fail('Request ID was already used for another move.');
    return existing;
  }
  if (s.movements.some(m => m.sku === sku && m.status === 'correction_required')) fail('Resolve the physical correction for this SKU first.');
  const source = s.positions.find(p => p.sku === sku && same(p, from));
  if (!source || source.quantity < quantity) fail('Not enough counted pieces on the selected source rack.');
  // Destination codes must be drawn from the reviewed rack list, not free text.
  if (to.rack && !(s.baseline.racks?.[to.location] || []).includes(to.rack)) fail('Destination rack is not in the reviewed rack list.', 400);
  source.quantity -= quantity;
  let target = s.positions.find(p => p.sku === sku && same(p, to));
  if (!target) { target = { sku, ...to, quantity: 0 }; s.positions.push(target); }
  target.quantity += quantity;
  const m = { id: crypto.randomUUID(), requestId: body.requestId, sku, quantity, from, to, submittedBy: user.username, submittedAt: new Date().toISOString(), status: 'pending', note: String(body.note || '').trim().slice(0, 500) };
  s.movements.push(m);
  return m;
}
async function graphql(query, variables) {
  if (!shopifyClient.store || !shopifyClient.token) fail('Shopify is not configured.', 503);
  const r = await shopifyClient.request(`https://${shopifyClient.store}/admin/api/2026-07/graphql.json`, { method: 'POST', headers: { 'Content-Type': 'application/json', 'X-Shopify-Access-Token': shopifyClient.token }, body: JSON.stringify({ query, variables }) });
  if (!r.ok) fail('Shopify transfer could not be confirmed. Retry the same movement.', 503);
  const d = await r.json();
  if (d.errors?.length) fail('Shopify rejected the transfer request. Check configuration and retry.', 503);
  return d.data;
}
async function sync(s, m, persist) {
  if (m.from.location === m.to.location) return;
  const mapping = s.baseline.skus?.[m.sku];
  const fromId = s.baseline.locations?.[m.from.location], toId = s.baseline.locations?.[m.to.location];
  if (!/^gid:\/\/shopify\/InventoryItem\/\d+$/.test(mapping?.inventoryItemId || '') || ![fromId, toId].every(id => /^gid:\/\/shopify\/Location\/\d+$/.test(id || ''))) fail('This SKU needs verified Shopify item and location mapping.', 409);
  if (!m.syncInput) {
    const data = await graphql('query($item:ID!,$from:ID!,$to:ID!){inventoryItem(id:$item){source:inventoryLevel(locationId:$from){quantities(names:["available"]){name quantity}} destination:inventoryLevel(locationId:$to){quantities(names:["available"]){name quantity}}}}', { item: mapping.inventoryItemId, from: fromId, to: toId });
    const item = data?.inventoryItem;
    if (!item?.source || !item?.destination) fail('SKU is not stocked at both Shopify locations.');
    const a = item.source.quantities.find(q => q.name === 'available')?.quantity;
    const b = item.destination.quantities.find(q => q.name === 'available')?.quantity;
    if (!Number.isSafeInteger(a) || !Number.isSafeInteger(b) || a < m.quantity) fail('Shopify stock differs from the count. Review this transfer before retrying.');
    m.syncInput = { name: 'available', reason: 'correction', referenceDocumentUri: `sanki://stock-movement/${m.id}`, quantities: [ { inventoryItemId: mapping.inventoryItemId, locationId: fromId, quantity: a - m.quantity, changeFromQuantity: a }, { inventoryItemId: mapping.inventoryItemId, locationId: toId, quantity: b + m.quantity, changeFromQuantity: b } ] };
    // Persist the exact CAS input before sending: retries cannot double-transfer.
    persist(s);
  }
  const d = await graphql('mutation($input:InventorySetQuantitiesInput!,$key:String!){inventorySetQuantities(input:$input) @idempotent(key:$key){userErrors{field message} inventoryAdjustmentGroup{createdAt}}}', { input: m.syncInput, key: m.id });
  if (!d?.inventorySetQuantities?.inventoryAdjustmentGroup || d.inventorySetQuantities.userErrors?.length) fail('Shopify transfer was not applied. Stock may have changed; manager review is required.');
}
let queue = Promise.resolve();
function serial(fn) { const next = queue.then(fn); queue = next.catch(() => {}); return next; }
const router = express.Router();
router.get('/api/stock-movements', (req, res) => {
  try {
    const s = load();
    res.json({ success: true, ready: ready(s), canApprove: canApprove(req.user), baseline: s.baseline ? { reconciledAt: s.baseline.reconciledAt, racks: s.baseline.racks } : null, positions: s.positions, movements: s.movements.slice(-500).reverse() });
  } catch (e) { res.status(e.status || 503).json({ success: false, error: 'Movement data unavailable. Contact the inventory manager.' }); }
});
router.post('/api/stock-movements', (req, res) => serial(async () => {
  const s = load(); const m = submit(s, req.body, req.user); save(s);
  res.json({ success: true, movement: m });
}).catch(e => res.status(e.status || 500).json({ success: false, error: e.message })));
router.post('/api/stock-movements/:id/review', (req, res) => serial(async () => {
  if (!canApprove(req.user)) fail('Only the owner or an assigned stock manager can review moves.', 403);
  const s = load(), m = s.movements.find(m => m.id === req.params.id);
  if (!m) fail('Movement not found.', 404);
  if (m.submittedBy === req.user.username) fail('A different manager must review your move.', 403);
  if (!['approve', 'correction'].includes(req.body.action)) fail('Choose approval or physical correction.', 400);
  if (m.status === 'approved') return res.json({ success: true, movement: m });
  if (req.body.action === 'correction') {
    if (m.syncInput) fail('A Shopify request has already been attempted. Confirm its result before physical correction.');
    const reason = String(req.body.reason || '').trim();
    if (!reason || reason.length > 500) fail('Describe the physical correction needed.', 400);
    m.status = 'correction_required'; m.reviewNote = reason; m.reviewedBy = req.user.username; save(s);
  } else {
    if (m.status === 'correction_required') fail('This move requires a physical correction, not approval.');
    if (s.movements.some(x => x.sku === m.sku && x.id !== m.id && x.status !== 'approved' && s.movements.indexOf(x) < s.movements.indexOf(m))) fail('Review earlier moves for this SKU first.');
    m.status = 'sync_pending'; m.reviewedBy = req.user.username; save(s);
    try { await sync(s, m, save); m.status = 'approved'; m.approvedAt = new Date().toISOString(); delete m.syncError; save(s); }
    catch (e) { m.syncError = e.message; save(s); throw e; }
  }
  res.json({ success: true, movement: m });
}).catch(e => res.status(e.status || 500).json({ success: false, error: e.message })));
module.exports = { router, submit, ready, canApprove, load, save };

'use strict';
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const express = require('express');
const inventory = require('./inventory-state');
const countedRackOptions = require('./counted-rack-options.json');
const care = require('./inventory-care-store');
const storePath = process.env.STOCK_MOVEMENTS_PATH || path.join(process.env.DATA_PATH ? path.dirname(process.env.DATA_PATH) : path.join(__dirname, '..'), 'stock_movements.json');
const locations = ['Display', 'Warehouse'];
function fail(message, status = 409) { const e = new Error(message); e.status = status; throw e; }
function canApprove(user) {
  const roles = user?.roles || [user?.role];
  const managers = (process.env.STOCK_MOVEMENT_APPROVERS || '').split(',').map(x => x.trim()).filter(Boolean);
  // Tushar runs inventory; this grant stays scoped to his inventory login.
  const inventoryLead = user?.username === 'tushar' && roles.some(r => ['inventory', 'warehouse'].includes(r));
  return inventoryLead || roles.some(r => ['owner', 'admin'].includes(r)) || managers.includes(user?.username);
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
function rackChoices(s) { return ready(s) ? s.baseline.racks : { Display: countedRackOptions.Display, Warehouse: countedRackOptions.Warehouse }; }
function point(p) {
  if (!p || !locations.includes(p.location) || (p.rack != null && typeof p.rack !== 'string')) fail('Select a valid location and rack.', 400);
  const rack = (p.rack || '').trim();
  if (rack.length > 40) fail('Rack code is too long.', 400);
  return { location: p.location, rack };
}
function same(a, b) { return a.location === b.location && a.rack === b.rack; }
function outstanding(m) { return !['approved', 'cancelled'].includes(m.status); }
function liveReady(live) {
  const ids = locations.map(k => live.mapping[k]);
  return ids.every(id => /^gid:\/\/shopify\/Location\/\d+$/.test(id || '')) && ids[0] !== ids[1];
}
function livePositions(s, live) {
  if (!liveReady(live)) return [];
  const counts = new Map(); for (const i of live.items) counts.set(i.sku, (counts.get(i.sku) || 0) + 1);
  return live.items.filter(i => i.sku && i.tracked && counts.get(i.sku) === 1).flatMap(i => locations.map(location => {
    const level = i.levels.find(l => l.locationId === live.mapping[location]);
    const pending = s.movements.filter(m => m.sku === i.sku && m.from.location === location && outstanding(m)).reduce((n,m) => n + m.quantity, 0);
    return { sku:i.sku, location, rack:'', quantity:Math.max(0, (level?.available || 0) - pending), stocked:!!level, title:i.product.title };
  }));
}
function submitLive(s, body, user, live) {
  if (!liveReady(live)) fail('Configure different Display and Warehouse Shopify locations before moving stock.');
  if (!user?.username) fail('A named staff login is required.', 403);
  const sku = String(body.sku || '').trim().toUpperCase(), quantity = body.quantity;
  const from = point(body.from), to = point(body.to);
  if (!sku || !Number.isSafeInteger(quantity) || quantity < 1 || same(from,to)) fail('Enter a SKU, whole-piece quantity and different destination.',400);
  if (!/^[a-zA-Z0-9-]{16,80}$/.test(body.requestId || '')) fail('A valid request ID is required.',400);
  const existing = s.movements.find(m => m.requestId === body.requestId);
  if (existing) {
    if (existing.sku !== sku || existing.quantity !== quantity || !same(existing.from,from) || !same(existing.to,to) || existing.submittedBy !== user.username) fail('Request ID was already used for another move.');
    return existing;
  }
  if (body.physicalConfirmed !== true) fail('Confirm the physical pieces and selected racks before submitting.',400);
  const matching = live.items.filter(i => i.sku === sku);
  if (matching.length !== 1 || !matching[0].tracked) fail('SKU must match one tracked Shopify inventory item.');
  const item = matching[0];
  if (s.movements.some(m => m.sku === sku && outstanding(m))) fail('Review the existing movement for this SKU first.');
  if (care.blockedSku(care.load(),sku)) fail('Confirm the pending dry cleaning Shopify result for this SKU first.');
  for (const p of [from,to]) if (p.rack && !rackChoices(s)[p.location].includes(p.rack)) fail('Choose a rack from the selected location.',400);
  const source = item.levels.find(l => l.locationId === live.mapping[from.location]);
  const target = item.levels.find(l => l.locationId === live.mapping[to.location]);
  if (!source || !target) fail('SKU must be stocked at both selected Shopify locations.');
  if (source.available < quantity) fail('Not enough available pieces at the selected source location.');
  const m = { id:crypto.randomUUID(), requestId:body.requestId, mode:'live', sku, quantity, from, to, inventoryItemId:item.id, locationIds:{...live.mapping}, physicalConfirmed:true, submittedBy:user.username, submittedAt:new Date().toISOString(), status:'pending', note:String(body.note || '').trim().slice(0,500) };
  s.movements.push(m); return m;
}
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
  const careStore = care.load();
  if (care.blockedSku(careStore, sku)) fail('Confirm the pending dry cleaning Shopify result for this SKU first.');
  const source = s.positions.find(p => p.sku === sku && same(p, from));
  const reserved = care.reservedAt(careStore, sku, s.baseline.locations?.[from.location], from.rack);
  if (!source || source.quantity - reserved < quantity) fail('Not enough available counted pieces on the selected source rack.');
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
async function sync(s, m, persist, send = inventory.graphql) {
  if (m.from.location === m.to.location) return;
  const mapping = m.mode === 'live' ? {inventoryItemId:m.inventoryItemId} : s.baseline.skus?.[m.sku];
  const ids = m.mode === 'live' ? m.locationIds : s.baseline.locations;
  const fromId = ids?.[m.from.location], toId = ids?.[m.to.location];
  if (!/^gid:\/\/shopify\/InventoryItem\/\d+$/.test(mapping?.inventoryItemId || '') || ![fromId, toId].every(id => /^gid:\/\/shopify\/Location\/\d+$/.test(id || ''))) fail('This SKU needs verified Shopify item and location mapping.', 409);
  if (!m.syncInput) {
    const data = await send('query($item:ID!,$from:ID!,$to:ID!){inventoryItem(id:$item){source:inventoryLevel(locationId:$from){quantities(names:["available"]){name quantity}} destination:inventoryLevel(locationId:$to){quantities(names:["available"]){name quantity}}}}', { item: mapping.inventoryItemId, from: fromId, to: toId });
    const item = data?.inventoryItem;
    if (!item?.source || !item?.destination) fail('SKU is not stocked at both Shopify locations.');
    const a = item.source.quantities.find(q => q.name === 'available')?.quantity;
    const b = item.destination.quantities.find(q => q.name === 'available')?.quantity;
    if (!Number.isSafeInteger(a) || !Number.isSafeInteger(b) || a < m.quantity) fail('Shopify stock differs from the count. Review this transfer before retrying.');
    m.syncInput = { name: 'available', reason: 'correction', referenceDocumentUri: `sanki://stock-movement/${m.id}`, quantities: [ { inventoryItemId: mapping.inventoryItemId, locationId: fromId, quantity: a - m.quantity, changeFromQuantity: a }, { inventoryItemId: mapping.inventoryItemId, locationId: toId, quantity: b + m.quantity, changeFromQuantity: b } ] };
    // Persist the exact CAS input before sending: retries cannot double-transfer.
    m.firstAttemptAt = new Date().toISOString(); persist(s);
  }
  if (m.syncRejected) fail('Shopify rejected this transfer. Record and resolve the physical correction before creating a new movement.');
  if (m.firstAttemptAt && Date.now() - Date.parse(m.firstAttemptAt) > 23*60*60*1000) fail('The safe retry window has expired. Confirm this transfer in Shopify inventory history with the inventory manager.');
  const d = await send('mutation($input:InventorySetQuantitiesInput!,$key:String!){inventorySetQuantities(input:$input) @idempotent(key:$key){userErrors{field message code} inventoryAdjustmentGroup{createdAt}}}', { input: m.syncInput, key: m.id });
  const result = d?.inventorySetQuantities;
  if (!result?.inventoryAdjustmentGroup && result?.userErrors?.length && result.userErrors.every(e => /^INVALID_|^CHANGE_FROM_QUANTITY_/.test(e.code || ''))) { m.syncRejected=true; persist(s); }
  if (!d?.inventorySetQuantities?.inventoryAdjustmentGroup || d.inventorySetQuantities.userErrors?.length) fail('Shopify transfer was not applied. Stock may have changed; manager review is required.');
}
let queue = Promise.resolve();
function serial(fn) { const next = queue.then(fn); queue = next.catch(() => {}); return next; }
const router = express.Router();
router.get('/api/stock-movements', async (req, res) => {
  try {
    const initial = load();
    if (req.query.registerOnly === '1') return res.json({ success:true, registerOnly:true, ready:false, liveMode:!ready(initial), username:req.user.username, canApprove:canApprove(req.user), positions:[], movements:initial.movements.slice(-500).reverse() });
    const live = ready(initial) ? null : await inventory.snapshot();
    // A long inventory read must not return an old approval/cancellation state.
    const s = load();
    const register = care.load();
    const positions = live ? livePositions(s,live) : s.positions.map(p => ({ ...p, quantity: p.quantity - care.reservedAt(register, p.sku, s.baseline?.locations?.[p.location], p.rack) }));
    res.json({ success: true, ready: live ? liveReady(live) : true, liveMode:!!live, at:live?.at, username:req.user.username, canApprove: canApprove(req.user), baseline: s.baseline ? { reconciledAt: s.baseline.reconciledAt, racks: s.baseline.racks } : null, rackOptions: rackChoices(s), rackSource: ready(s) ? 'Approved movement baseline' : countedRackOptions.source, positions, movements: s.movements.slice(-500).reverse() });
  } catch (e) { res.status(e.status || 503).json({ success: false, error: 'Movement data unavailable. Contact the inventory manager.' }); }
});
router.post('/api/stock-movements', (req, res) => serial(async () => {
  const s = load(); const m = ready(s) ? submit(s, req.body, req.user) : submitLive(s,req.body,req.user,await inventory.snapshot()); save(s);
  res.json({ success: true, movement: m });
}).catch(e => res.status(e.status || 500).json({ success: false, error: e.message })));
async function review(s, id, body, user, persist = save, send = inventory.graphql) {
  if (!canApprove(user)) fail('Only the owner or an assigned stock manager can review moves.', 403);
  const m = s.movements.find(m => m.id === id);
  if (!m) fail('Movement not found.', 404);
  if (!['approve', 'cancel', 'correction', 'resolve'].includes(body.action)) fail('Choose approval, cancellation or physical correction.', 400);
  if (['approved','cancelled'].includes(m.status)) return m;
  if (body.action === 'cancel') {
    if (m.mode !== 'live' || m.status !== 'pending' || m.syncInput || m.firstAttemptAt) fail('Only an awaiting-approval request with no Shopify attempt can be cancelled.');
    const reason = String(body.reason || '').trim();
    if (!reason || reason.length > 500) fail('Record the reason for cancellation.', 400);
    m.status='cancelled';m.cancellationReason=reason;m.cancelledBy=user.username;m.cancelledAt=new Date().toISOString();persist(s);
  } else if (body.action === 'resolve') {
    if (m.mode !== 'live' || m.status !== 'correction_required' || (m.syncInput && !m.syncRejected)) fail('This movement cannot be resolved before confirming its Shopify result.');
    const reason=String(body.reason || '').trim();
    if (!reason || reason.length>500 || body.physicalCorrected !== true) fail('Verify the pieces returned to the source rack and record the correction.',400);
    m.status='cancelled';m.resolutionNote=reason;m.resolvedBy=user.username;m.resolvedAt=new Date().toISOString();persist(s);
  } else if (body.action === 'correction') {
    if (m.syncInput && !m.syncRejected) fail('A Shopify request has already been attempted. Confirm its result before physical correction.');
    const reason = String(body.reason || '').trim();
    if (!reason || reason.length > 500) fail('Describe the physical correction needed.', 400);
    m.status = 'correction_required'; m.reviewNote = reason; m.reviewedBy = user.username; persist(s);
  } else {
    if (m.status === 'correction_required') fail('This move requires a physical correction, not approval.');
    if (care.blockedSku(care.load(),m.sku)) fail('Confirm the pending dry cleaning Shopify result for this SKU first.');
    if (s.movements.some(x => x.sku === m.sku && x.id !== m.id && outstanding(x) && s.movements.indexOf(x) < s.movements.indexOf(m))) fail('Review earlier moves for this SKU first.');
    m.status = 'sync_pending'; m.reviewedBy = user.username; m.reviewedAt = new Date().toISOString(); persist(s);
    try { await sync(s,m,persist,send); m.status='approved';m.approvedAt=new Date().toISOString();delete m.syncError;persist(s);inventory.invalidate(); }
    catch(e) { m.syncError=e.message;persist(s);throw e; }
  }
  return m;
}
router.post('/api/stock-movements/:id/review', (req, res) => serial(async () => {
  const s=load(), m=await review(s,req.params.id,req.body,req.user);
  res.json({ success: true, movement: m });
}).catch(e => res.status(e.status || 500).json({ success: false, error: e.message })));
module.exports = { router, submit, submitLive, livePositions, liveReady, review, sync, ready, rackChoices, canApprove, load, save, serial };

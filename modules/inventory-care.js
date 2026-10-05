'use strict';
const express = require('express');
const crypto = require('crypto');
const state = require('./inventory-state');
const store = require('./inventory-care-store');
const moves = require('./stock-movements');
const router = express.Router();
const reasons = { cleaning: ['Dry cleaning', 'Stain removal'], miscellaneous: ['Damaged', 'Defective', 'Altered customer return', 'Repair required', 'Quality inspection', 'Other — not for sale'] };
const bucket = { cleaning: 'quality_control', miscellaneous: 'damaged' };
function manager(user) { return moves.canApprove(user); }
function named(user) { if (!user?.username) state.fail('A named staff login is required.', 403); }
function text(value, max = 500) { if (typeof value !== 'string' || value.length > max) state.fail('Text is missing or too long.', 400); return value.trim(); }
function requestKey(value) { if (!/^[a-zA-Z0-9-]{16,80}$/.test(value || '')) state.fail('A valid request ID is required.', 400); return value; }
function duplicate(s, body, user) {
  const op = s.operations.find(o => o.requestId === body.requestId);
  if (!op) return null;
  if (op.requestedBy !== user.username || op.fingerprint !== JSON.stringify(body)) state.fail('This request ID belongs to a different action.');
  return op;
}
function ensureNoConflict(s, lines, ignoreId) {
  for (const l of lines) {
    if (s.operations.some(o => o.id !== ignoreId && ['sync_pending', 'review_required'].includes(o.status) && o.lines.some(x => x.sku === l.sku))) state.fail('Resolve the pending Shopify result for ' + l.sku + ' first.');
    const m = moves.load();
    if (m.movements.some(o => o.sku === l.sku && !['approved','cancelled'].includes(o.status))) state.fail('Review the existing stock movement for ' + l.sku + ' first.');
  }
}
function makeOpen(s, body, user, live) {
  named(user); requestKey(body.requestId);
  if (duplicate(s, body, user)) return duplicate(s, body, user);
  if (!Object.hasOwn(reasons, body.kind) || !reasons[body.kind].includes(body.reason)) state.fail('Choose a valid stock category and reason.', 400);
  const vendor = body.kind === 'cleaning' ? text(body.vendor || '', 120) : '', note = text(body.note || '');
  if (body.kind === 'cleaning' && !vendor) state.fail('Enter the cleaner name.', 400);
  const expectedReturn = body.kind === 'cleaning' ? text(body.expectedReturn || '', 10) : '';
  if (body.kind === 'cleaning' && (!/^\d{4}-\d{2}-\d{2}$/.test(expectedReturn) || !Number.isFinite(Date.parse(expectedReturn)) || new Date(expectedReturn).toISOString().slice(0, 10) !== expectedReturn)) state.fail('Enter a valid expected return date.', 400);
  if (!Array.isArray(body.lines) || !body.lines.length || body.lines.length > 50) state.fail('Add between 1 and 50 SKU lines.', 400);
  const seen = new Set();
  const lines = body.lines.map(raw => {
    if (!Number.isSafeInteger(raw.quantity) || raw.quantity < 1) state.fail('Enter positive whole-piece quantities.', 400);
    const sku = text(raw.sku || '', 100).toUpperCase(), matching = live.items.filter(i => i.sku === sku);
    if (!sku || matching.length !== 1 || !matching[0].tracked) state.fail('SKU must match one tracked Shopify inventory item.');
    const item = matching[0], locationId = text(raw.locationId || '', 100), level = item.levels.find(l => l.locationId === locationId);
    const rack = text(raw.rack || '', 40), key = item.id + ':' + locationId;
    if (!level || seen.has(key)) state.fail('Choose a stocked location; combine duplicate SKU/location lines.', 400);
    seen.add(key);
    if (item.inventoryPolicy !== 'DENY') state.fail('Disable selling when out of stock for ' + sku + ' in Shopify before setting stock aside.');
    if (level.available < raw.quantity) state.fail('Not enough available pieces at ' + level.location + ' for ' + sku + '.');
    return { id: crypto.randomUUID(), sku, inventoryItemId: item.id, title: item.product.title, variant: item.variant, locationId, location: level.location, rack, quantity: raw.quantity };
  });
  ensureNoConflict(s, lines);
  const movementStore = moves.load();
  if (moves.ready(movementStore)) {
    for (const l of lines) {
      const location = Object.keys(movementStore.baseline.locations || {}).find(k => movementStore.baseline.locations[k] === l.locationId);
      const p = movementStore.positions.find(p => p.sku === l.sku && p.location === location && p.rack === l.rack);
      if (!p || p.quantity - store.reservedAt(s, l.sku, l.locationId, l.rack) < l.quantity) state.fail('Not enough reviewed stock on the selected source rack for ' + l.sku + '.');
    }
  }
  const batch = { id: crypto.randomUUID(), kind: body.kind, reason: body.reason, vendor, expectedReturn, note, createdBy: user.username, createdAt: new Date().toISOString(), lines };
  const op = { id: crypto.randomUUID(), requestId: body.requestId, fingerprint: JSON.stringify(body), batchId: batch.id, action: 'open', requestedBy: user.username, requestedAt: batch.createdAt, status: 'awaiting_approval', lines: lines.map(l => ({ lineId: l.id, sku: l.sku, quantity: l.quantity, from: 'available', to: body.kind })) };
  s.batches.push(batch); s.operations.push(op); return op;
}
function makeAction(s, batchId, body, user) {
  named(user); if (!manager(user)) state.fail('Only the owner or an assigned stock manager can release or reclassify stock.', 403);
  requestKey(body.requestId); const old = duplicate(s, body, user); if (old) { if (old.batchId !== batchId) state.fail('Request belongs to another batch.'); return old; }
  const batch = s.batches.find(b => b.id === batchId); if (!batch) state.fail('Batch not found.', 404);
  if (!['release', 'classify', 'dispose'].includes(body.action) || !Array.isArray(body.lines) || !body.lines.length || body.lines.length > 50) state.fail('Choose a valid return action and SKU lines.', 400);
  const note = text(body.note || '');
  if (!note) state.fail('Record the inspection result or reason.', 400);
  if (body.action === 'release' && body.inspected !== true) state.fail('Confirm the pieces were inspected and are ready for sale.', 400);
  if (body.action === 'dispose' && body.disposed !== true) state.fail('Confirm these pieces were permanently removed from owned stock.', 400);
  const balances = store.balances(s, batch), seen = new Set();
  const lines = body.lines.map(raw => {
    const l = balances.find(l => l.id === raw.lineId), from = raw.from;
    if (!l || !['cleaning', 'miscellaneous'].includes(from) || !Number.isSafeInteger(raw.quantity) || raw.quantity < 1 || raw.quantity > l[from] || (body.action === 'classify' && from !== 'cleaning') || (body.action === 'dispose' && from !== 'miscellaneous')) state.fail('Quantity exceeds outstanding pieces or the return category is invalid.', 400);
    const key = l.id; if (seen.has(key)) state.fail('Record one action per SKU/location line.', 400); seen.add(key);
    return { lineId: l.id, sku: l.sku, quantity: raw.quantity, from, to: body.action === 'release' ? 'available' : body.action === 'dispose' ? 'disposed' : 'miscellaneous' };
  });
  ensureNoConflict(s, lines);
  const op = { id: crypto.randomUUID(), requestId: body.requestId, fingerprint: JSON.stringify(body), batchId, action: body.action, note, inspected: body.inspected === true, requestedBy: user.username, requestedAt: new Date().toISOString(), status: 'awaiting_approval', lines };
  s.operations.push(op); return op;
}
function buildInput(s, op, live) {
  const batch = s.batches.find(b => b.id === op.batchId), expected = new Map();
  if (op.action === 'open') {
    const m = moves.load();
    if (moves.ready(m)) for (const a of op.lines) {
      const l = batch.lines.find(l => l.id === a.lineId);
      const location = Object.keys(m.baseline.locations || {}).find(k => m.baseline.locations[k] === l.locationId);
      const p = m.positions.find(p => p.sku === l.sku && p.location === location && p.rack === l.rack);
      if (!p || p.quantity - store.reservedAt(s, l.sku, l.locationId, l.rack) < a.quantity) state.fail('Available stock on the reviewed source rack changed. Review this batch.');
    }
  }
  if (op.action === 'dispose') return { name: 'damaged', reason: 'damaged', referenceDocumentUri: `gid://sanki/InventoryCare/${op.id}`, changes: op.lines.map(a => {
    const l = batch.lines.find(l => l.id === a.lineId), item = live.items.find(i => i.id === l.inventoryItemId), level = item?.levels.find(x => x.locationId === l.locationId);
    if (!level || !item.tracked || item.sku !== l.sku || level.damaged < a.quantity || store.balances(s, batch).find(x => x.id === l.id).miscellaneous < a.quantity) state.fail('Not enough confirmed not-for-sale stock to dispose.');
    const registered = s.batches.flatMap(b => store.balances(s, b)).filter(x => x.inventoryItemId === item.id && x.locationId === level.locationId).reduce((n, x) => n + x.miscellaneous, 0);
    if (registered > level.damaged) state.fail('Shopify and the registered not-for-sale pieces differ. Review this SKU before disposal.');
    return { inventoryItemId: item.id, locationId: level.locationId, delta: -a.quantity, changeFromQuantity: level.damaged, ledgerDocumentUri: `gid://sanki/InventoryCare/${batch.id}` };
  }) };
  const changes = op.lines.map(a => {
    const l = batch.lines.find(l => l.id === a.lineId), item = live.items.find(i => i.id === l.inventoryItemId);
    const level = item?.levels.find(x => x.locationId === l.locationId);
    if (!level || !item.tracked || item.sku !== l.sku) state.fail('SKU or location mapping changed; review this batch.');
    if (op.action === 'open' && item.inventoryPolicy !== 'DENY') state.fail('Disable overselling for ' + l.sku + ' before activating this batch.');
    const from = bucket[a.from] || a.from, to = bucket[a.to] || a.to;
    if (op.action !== 'open') {
      const balance = store.balances(s, batch).find(x => x.id === l.id);
      if (balance[a.from] < a.quantity) state.fail('Outstanding quantity changed; review the return.');
      const registered = s.batches.flatMap(b => store.balances(s, b)).filter(x => x.inventoryItemId === item.id && x.locationId === level.locationId).reduce((n, x) => n + x[a.from], 0);
      if (registered > level[from]) state.fail('Shopify and the registered outstanding pieces differ. Review this SKU before returning any pieces.');
    }
    function terminal(name) {
      const key = item.id + ':' + level.locationId + ':' + name;
      if (!expected.has(key)) expected.set(key, level[name]);
      return { name, locationId: level.locationId, ledgerDocumentUri: name === 'available' ? null : `gid://sanki/InventoryCare/${batch.id}`, changeFromQuantity: expected.get(key) };
    }
    const source = terminal(from), destination = terminal(to);
    if (source.changeFromQuantity < a.quantity) state.fail('Shopify has fewer ' + from + ' pieces than this action requires for ' + l.sku + '.');
    expected.set(item.id + ':' + level.locationId + ':' + from, source.changeFromQuantity - a.quantity);
    expected.set(item.id + ':' + level.locationId + ':' + to, destination.changeFromQuantity + a.quantity);
    return { inventoryItemId: item.id, quantity: a.quantity, from: source, to: destination };
  });
  return { reason: op.action === 'release' ? 'correction' : 'damaged', referenceDocumentUri: `gid://sanki/InventoryCare/${op.id}`, changes };
}
async function confirm(s, op, user, deps = {}) {
  const persist = deps.save || store.save, getLive = deps.snapshot || state.snapshot, send = deps.graphql || state.graphql;
  if (!manager(user)) state.fail('Only the owner or an assigned stock manager can confirm this action.', 403);
  if (op.status === 'confirmed') return op;
  if (['cancelled', 'review_required'].includes(op.status)) state.fail('This action needs review or has been cancelled.');
  ensureNoConflict(s, op.lines, op.id);
  if (op.firstAttemptAt && Date.now() - Date.parse(op.firstAttemptAt) > 23 * 60 * 60 * 1000) state.fail('The retry window expired. Verify the Shopify history with the inventory manager before reconciliation.');
  if (!op.syncInput) { op.syncInput = buildInput(s, op, await getLive(true)); }
  op.status = 'sync_pending'; op.confirmedBy = user.username; op.firstAttemptAt ||= new Date().toISOString(); persist(s);
  try {
    const mutation = op.action === 'dispose' ? 'inventoryAdjustQuantities' : 'inventoryMoveQuantities';
    const type = op.action === 'dispose' ? 'InventoryAdjustQuantitiesInput' : 'InventoryMoveQuantitiesInput';
    const d = await send(`mutation($input:${type}!,$key:String!){${mutation}(input:$input) @idempotent(key:$key){userErrors{field message code} inventoryAdjustmentGroup{createdAt}}}`, { input: op.syncInput, key: op.id });
    const result = d[mutation];
    if (result?.userErrors?.length) {
      const definitive = !result.inventoryAdjustmentGroup && result.userErrors.every(e => /^(INVALID_|CHANGE_FROM_QUANTITY_STALE$|DIFFERENT_LOCATIONS$|SAME_QUANTITY_NAME$|NON_MUTABLE_INVENTORY_ITEM$)/.test(e.code || ''));
      op.status = definitive ? 'review_required' : 'sync_pending'; op.error = result.userErrors.map(e => e.message).join(' ').slice(0, 1000); persist(s);
      state.fail('Shopify rejected the change: ' + op.error);
    }
    if (!result?.inventoryAdjustmentGroup) state.fail('Shopify did not confirm the change. Retry this same action.', 502);
    op.status = 'confirmed'; op.confirmedAt = new Date().toISOString(); delete op.error; persist(s); state.invalidate();
    return op;
  } catch (e) {
    if (op.status === 'sync_pending') { op.error = e.message; persist(s); }
    state.invalidate(); throw e;
  }
}
function registerData(s, live, user) {
  live = state.withCare(live, s);
  const batches = s.batches.map(b => ({ ...b, balances: store.balances(s, b) }));
  const totals = live.items.reduce((n, i) => { const q = state.quantities(i, live.mapping); for (const k of Object.keys(q)) n[k] = (n[k] || 0) + q[k]; return n; }, {});
  const alerts = [];
  for (const item of live.items) for (const l of item.levels) if ((item.careCleaning?.[l.locationId] || 0) > l.quality_control) alerts.push(item.sku + ' at ' + l.location + ': Shopify cleaning stock differs from the register.');
  return { success: true, at: live.at, totals, alerts, canManage: manager(user), reasons, mapping: live.mapping, items: live.items, batches, operations: s.operations.slice().reverse() };
}
router.get('/api/inventory-care', async (req, res) => {
  try { res.json(registerData(store.load(), await state.snapshot(req.query.refresh === '1'), req.user)); }
  catch (e) { res.status(e.status || 503).json({ success: false, error: e.message }); }
});
router.post('/api/inventory-care/batches', (req, res) => moves.serial(async () => {
  const s = store.load(), old = duplicate(s, req.body, req.user);
  const live = old ? null : await state.snapshot(true);
  const op = old || makeOpen(s, req.body, req.user, live); store.save(s);
  if (manager(req.user)) await confirm(s, op, req.user, live ? { snapshot: async () => live } : {});
  res.json({ success: true, operation: op });
}).catch(e => res.status(e.status || 500).json({ success: false, error: e.message })));
router.post('/api/inventory-care/batches/:id/actions', (req, res) => moves.serial(async () => {
  const s = store.load(), op = makeAction(s, req.params.id, req.body, req.user); store.save(s); await confirm(s, op, req.user);
  res.json({ success: true, operation: op });
}).catch(e => res.status(e.status || 500).json({ success: false, error: e.message })));
router.post('/api/inventory-care/operations/:id/review', (req, res) => moves.serial(async () => {
  if (!manager(req.user)) state.fail('Only the owner or an assigned stock manager can review this action.', 403);
  const s = store.load(), op = s.operations.find(o => o.id === req.params.id);
  if (!op) state.fail('Action not found.', 404);
  if (req.body.action === 'confirm') await confirm(s, op, req.user);
  else if (req.body.action === 'cancel') {
    if (!['awaiting_approval', 'review_required'].includes(op.status)) state.fail('A dispatched Shopify request must be confirmed before cancellation.');
    op.status = 'cancelled'; op.reviewNote = text(req.body.note || '');
    if (!op.reviewNote) state.fail('Record the reason for cancellation.', 400);
    op.reviewedBy = req.user.username; op.reviewedAt = new Date().toISOString(); store.save(s);
  } else state.fail('Choose confirmation or cancellation.', 400);
  res.json({ success: true, operation: op });
}).catch(e => res.status(e.status || 500).json({ success: false, error: e.message })));
module.exports = { router, makeOpen, makeAction, buildInput, confirm, registerData, reasons };

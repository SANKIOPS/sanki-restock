'use strict';
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const storePath = process.env.INVENTORY_CARE_PATH || path.join(process.env.DATA_PATH ? path.dirname(process.env.DATA_PATH) : path.join(__dirname, '..'), 'inventory_care.json');
function empty() { return { version: 1, batches: [], operations: [] }; }
function load(file = storePath) {
  if (!fs.existsSync(file)) return empty();
  const s = JSON.parse(fs.readFileSync(file, 'utf8'));
  if (s.version !== 1 || !Array.isArray(s.batches) || !Array.isArray(s.operations)) throw Error('Dry cleaning register requires repair.');
  return s;
}
function save(s, file = storePath) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  const tmp = file + '.' + crypto.randomUUID() + '.tmp';
  fs.writeFileSync(tmp, JSON.stringify(s, null, 2), { mode: 0o600 }); fs.renameSync(tmp, file);
}
function balances(s, batch) {
  const result = new Map(batch.lines.map(l => [l.id, { ...l, cleaning: 0, miscellaneous: 0, released: 0, disposed: 0 }]));
  for (const op of s.operations.filter(o => o.batchId === batch.id && o.status === 'confirmed')) {
    for (const a of op.lines) {
      const l = result.get(a.lineId);
      if (!l) throw Error('Dry cleaning register contains an unknown line.');
      if (op.action === 'open') l[batch.kind] += a.quantity;
      else if (op.action === 'release') { l[a.from] -= a.quantity; l.released += a.quantity; }
      else if (op.action === 'classify') { l.cleaning -= a.quantity; l.miscellaneous += a.quantity; }
      else if (op.action === 'dispose') { l.miscellaneous -= a.quantity; l.disposed += a.quantity; }
      if (l.cleaning < 0 || l.miscellaneous < 0) throw Error('Dry cleaning register contains invalid balances.');
    }
  }
  return [...result.values()];
}
// Movement positions retain their reviewed opening quantities. Only confirmed
// care operations reserve those pieces; releasing them restores the same rack.
function reservedAt(s, sku, locationId, rack) {
  return s.batches.reduce((n, b) => n + balances(s, b).filter(l => l.sku === sku && l.locationId === locationId && l.rack === rack).reduce((q, l) => q + l.cleaning + l.miscellaneous + l.disposed, 0), 0);
}
function blockedSku(s, sku) {
  return s.operations.some(o => ['sync_pending', 'review_required'].includes(o.status) && o.lines.some(l => l.sku === sku));
}
module.exports = { load, save, balances, reservedAt, blockedSku, empty };

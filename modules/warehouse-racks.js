'use strict';
const fs = require('fs');
const path = require('path');
const reference = require('./warehouse-rack-reference.json');
const movementPath = process.env.STOCK_MOVEMENTS_PATH || path.join(process.env.DATA_PATH ? path.dirname(process.env.DATA_PATH) : path.join(__dirname, '..'), 'stock_movements.json');
const normalizeSku = sku => String(sku || '').replace(/\s+/g, '').toUpperCase();

// These counts locate rack references only. They never replace Shopify quantities
// or create an audited movement baseline. Repeated reads replay each approved
// movement once from the immutable sheet, so retries cannot double-apply it.
function createIndex(movements = [], data = reference) {
  const index = new Map(Object.entries(data.skus).map(([sku, counts]) => [sku, { counts: { ...counts }, needsVerification: false }]));
  const cutoff = Date.parse(data.effectiveFrom), seen = new Set();
  for (const m of [...movements].sort((a,b) => String(a.approvedAt).localeCompare(String(b.approvedAt)))) {
    if (m.status !== 'approved' || !m.id || seen.has(m.id) || !(Date.parse(m.approvedAt) >= cutoff) || !Number.isSafeInteger(m.quantity) || m.quantity < 1) continue;
    seen.add(m.id);
    const sku = normalizeSku(m.sku);
    if (!index.has(sku)) index.set(sku, { counts: {}, needsVerification: false });
    const item = index.get(sku);
    if (m.from?.location === 'Warehouse') {
      const rack = m.from.rack;
      if (!rack || (item.counts[rack] || 0) < m.quantity) item.needsVerification = true;
      if (rack) item.counts[rack] = Math.max(0, (item.counts[rack] || 0) - m.quantity);
    }
    if (m.to?.location === 'Warehouse') {
      if (m.to.rack) item.counts[m.to.rack] = (item.counts[m.to.rack] || 0) + m.quantity;
      else item.needsVerification = true;
    }
  }
  return sku => {
    const item = index.get(normalizeSku(sku));
    const racks = item ? Object.keys(item.counts).filter(r => item.counts[r] > 0).sort((a,b)=>a.localeCompare(b,undefined,{numeric:true})) : [];
    return { warehouseRacks: racks, warehouseRack: racks.join(', ') || null, displayRack: null, rackReferenceNeedsVerification: !!item?.needsVerification, warehouseRackSource: data.source };
  };
}
function readIndex() {
  let movements = [];
  try { const store = JSON.parse(fs.readFileSync(movementPath, 'utf8')); if (!Array.isArray(store.movements)) throw Error('Invalid movement history'); movements = store.movements; }
  catch (e) { if (e.code !== 'ENOENT') throw e; }
  return createIndex(movements);
}
module.exports = { reference, normalizeSku, createIndex, readIndex };

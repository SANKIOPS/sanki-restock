'use strict';
// Read-only join to the existing automatic Velocity evidence. Never edit
// orders/dispatch, infer delivery from fulfilment, or use the sync timestamp.
function carrierDay(value) {
  const text = String(value || '').trim();
  if (!text) return '';
  let date;
  if (/^\d{4}-\d{2}-\d{2}(?:$|[ T]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?$)/.test(text)) date = text.slice(0, 10);
  else if (/^\d{4}-\d{2}-\d{2}T.*(?:Z|[+-]\d{2}:?\d{2})$/.test(text) && Number.isFinite(Date.parse(text))) date = new Date(Date.parse(text) + 19800000).toISOString().slice(0, 10);
  return date && Number.isFinite(Date.parse(date + 'T00:00:00Z')) && new Date(date + 'T00:00:00Z').toISOString().slice(0, 10) === date ? date : '';
}
const delivered = value => /^(delivered|delivery completed|dlv)$/i.test(String(value || '').trim());
const inactive = value => /^(cancelled|canceled|externally_fulfilled)$/i.test(String(value || '').trim());
function velocityCompletion(order, dispatch = {}, velocity = {}, today = new Date(Date.now() + 19800000).toISOString().slice(0, 10)) {
  const index = velocity.index || {}, linked = index.byOrder?.[String(order.id)] || [];
  const shipments = [...(order.shipments || []), ...linked].filter(s => s && !inactive(s.status));
  if (dispatch.velocityAwb) shipments.push({ awb: dispatch.velocityAwb });
  const awbs = [...new Set(shipments.map(s => String(s.awb || '').trim()).filter(a => a && !/^(null|undefined|n\/a|none)$/i.test(a)))];
  if (!awbs.length) return null;
  const created = carrierDay(order.createdAt || order.processedAt);
  const dates = [], missing = [];
  for (const awb of awbs) {
    const candidates = [velocity.cache?.shipments?.[awb], velocity.tracking?.shipments?.[awb], index.shipments?.[awb]].filter(v => v && typeof v === 'object');
    candidates.sort((a, b) => String(a.lastUpdated || '').localeCompare(String(b.lastUpdated || '')));
    const evidence = Object.assign({}, ...candidates);
    const date = carrierDay(evidence.deliveredDate);
    if (!delivered(evidence.status) || !date || !created || date < created || date > today) missing.push(awb);
    else dates.push(date);
  }
  if (missing.length) {
    // An in-transit order is not an exception merely because it is unfinished.
    // A purported/partial delivery does need review, without inventing a date.
    if (!dates.length && !order.deliveryComplete && dispatch.packingStatus !== 'delivered') return null;
    return { date: '', evidence: 'Velocity evidence incomplete', issue: 'Velocity delivery evidence is missing, incomplete or invalid for AWB(s): ' + missing.join(', ') + '. The sale remains excluded until every active shipment has a valid actual delivery date.' };
  }
  return { date: dates.sort().at(-1), evidence: 'Velocity actual carrier delivery', awbs };
}
module.exports = { carrierDay, velocityCompletion };

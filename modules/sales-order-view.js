// Read-only presentation model. Provider remittance is never bank verification.
const number = value => Number.isFinite(Number(value)) ? Number(value) : 0;
const round = value => Math.round((value + Number.EPSILON) * 100) / 100;
function integrateOrder(order, velocity = {}, remittances = [], legacy = [], finance = [], online = []) {
  const old = legacy.find(row => String(row.id) === String(order.id));
  const source = (order.shipments && order.shipments.length ? order.shipments : null) || (old && (old.fulfillments || []).filter(f => f.status !== 'cancelled').map(f => ({awb:f.tracking_number, courier:f.tracking_company,status:f.shipment_status || f.status}))) || [];
  const shipments = source.map(s => { const v = velocity[String(s.awb || '').trim()]; return v ? {...s, courier:v.courierName || s.courier, status:v.status || s.status, updatedAt:v.lastUpdated, deliveredAt:v.deliveredDate, codAmount:v.codAmount, location:v.currentLocation} : s; });
  const payments = (order.paymentTransactions || []).filter(t => ['sale','capture'].includes(t.kind) && t.status === 'success');
  const known = order.paymentTransactionSchemaVersion === 2;
  const codEvidence=finance.filter(r=>shipments.some(s=>String(s.awb)===r.awb));
  const onlineEvidence=online.filter(r=>r.orderId===String(order.id));
  const providerCollected=round(onlineEvidence.filter(r=>r.type==='Payment').reduce((n,r)=>n+number(r.gross),0)+codEvidence.filter(r=>!r.error&&r.deliveryDate).reduce((n,r)=>n+number(r.amount),0));
  const received = known || providerCollected>0 ? Math.max(known ? round(payments.reduce((sum,t) => sum + number(t.amount),0)) : 0,providerCollected) : null;
  const refunded = number(order.moneyRefunded) + number(order.storeCreditIssued);
  const balance = received === null ? null : round(Math.max(0,number(order.total) - received));
  const gateway = (order.paymentGateways || []).join(' ');
  const cod = /cod|cash on delivery/i.test(gateway)||codEvidence.some(r=>!r.error&&r.amount>0);
  const paymentType = order.channel === 'POS' ? 'Prepaid' : received > 0 && balance > 0 ? (cod?'Partial COD':'Partially paid') : cod ? 'COD' : order.financialStatus === 'paid' ? 'Prepaid' : 'Unconfirmed';
  const evidence = remittances.filter(r => String(r.orderId || '') === String(order.id) || (r.awb && shipments.some(s => String(s.awb) === String(r.awb))));
  const providerEvidence=codEvidence.some(r=>r.utr)||onlineEvidence.some(r=>r.utr);
  return {...order, codEvidence, onlineEvidence, shipments, payments, received, balance, refunded, paymentType, paymentMethods: [...new Set(payments.map(p=>p.gateway).filter(Boolean))], remittanceEvidence:evidence.map(r=>({batch:r.settlementId || '', date:r.date, reference:r.utr, amount:r.amount, status:r.status})), settlementStatus:providerEvidence ? 'Provider reports settlement · bank verification pending' : evidence.length ? 'Remittance reported · bank verification pending' : order.channel === 'POS' ? 'POS clearance linking pending' : 'Settlement evidence pending', deliveryStatus:shipments.length ? [...new Set(shipments.map(s=>s.status || 'Unknown'))].join(' / ') : order.channel === 'POS' ? 'No shipment recorded' : 'Awaiting dispatch', orderStatus:order.cancelledAt ? 'Cancelled' : order.financialStatus === 'refunded' ? 'Refunded' : order.financialStatus === 'partially_refunded' ? 'Partially refunded' : order.fulfillmentStatus || 'Unfulfilled'};
}
module.exports = {integrateOrder};

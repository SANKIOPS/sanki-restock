const { purchaseBillingAmount } = require('./purchase-payment-status');

function invoiceAmounts(invoice, pos, settings = {}) {
  const children = (invoice.poIds || []).map(id => pos[id]);
  if (!children.length || children.some(po => !po)) return null;
  const purchaseAmounts = children.map(po => purchaseBillingAmount(po, settings));
  const purchaseAmountInr = purchaseAmounts.reduce((sum, amount) => sum + amount, 0);
  const bills = children.map(po => (invoice.childBills || {})[po.id] || {});
  const charges = invoice.combined || {};
  const rate = Number(charges.exchangeRate);
  const chargeKeys = ['combinedFreightYuan', 'localTransportationYuan', 'fixedTransportationYuan', 'extraChargesYuan'];
  const complete = bills.every(b => b.totalQuantity != null && b.billValueYuan != null) &&
    chargeKeys.every(key => charges[key] != null) && Number.isFinite(rate) && rate > 0;
  const vendorBillYuan = complete ? bills.reduce((sum, bill) => sum + Number(bill.billValueYuan), 0) : null;
  const vendorTotalYuan = complete ? vendorBillYuan + chargeKeys.reduce((sum, key) => sum + Number(charges[key]), 0) : null;
  return { purchaseAmounts, purchaseAmountInr, vendorBillYuan, vendorTotalYuan,
    vendorAmountInr: complete ? Math.round(vendorTotalYuan * rate) : null, rate };
}

function allocateAmount(ids, weights, total) {
  const basis = weights.reduce((sum, amount) => sum + amount, 0);
  const allocations = {}; let assigned = 0;
  ids.forEach((id, index) => {
    const amount = index === ids.length - 1 ? total - assigned : Math.round(total * (basis > 0 ? weights[index] / basis : 1 / ids.length));
    allocations[id] = amount; assigned += amount;
  });
  return allocations;
}

function finalizedByPo(procurementStore) {
  const result = {};
  Object.values(procurementStore.combinedVendorInvoices || {}).forEach(invoice => {
    if (!invoice.finalized) return;
    Object.entries(invoice.finalized.allocations || {}).forEach(([id, amount]) => {
      result[id] = { invoice, amount };
    });
  });
  return result;
}

module.exports = { invoiceAmounts, allocateAmount, finalizedByPo };

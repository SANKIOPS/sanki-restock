function purchaseBillingAmount(po, defaults = {}) {
  const num = x => Number(x) || 0;
  const rate = num(po.exRate != null ? po.exRate : defaults.exRate);
  const extraCosts = (num(po.localTransportYuan) + num(po.otherCostsYuan)) * rate;
  if (po.status === 'posted' && ((po.newProducts || []).length || (po.existingAdds || []).length)) {
    let total = 0;
    (po.newProducts || []).forEach(p => (p.variants || []).forEach(v => { total += num(v.landed) * num(v.qty); }));
    (po.existingAdds || []).forEach(v => { total += num(v.landed) * num(v.qty); });
    return Math.round(total + extraCosts);
  }
  const india = po.origin === 'india';
  const freight = num(po.freightPerGram != null ? po.freightPerGram : defaults.freightPerGram);
  const total = (po.lines || []).reduce((n, l) => n + num(l.qty) * (india ? num(l.perPcsYuan) : num(l.perPcsYuan) * rate + num(l.weightGrams) * freight), 0);
  return Math.round(total + (india ? num(po.transportTotal) : 0) + extraCosts);
}
// Read-only projection of accounting's PO payment ledger.
function purchasePaymentStatus(po, accounting, includeDetails, defaults, finalizedAmount) {
  const amount = finalizedAmount == null ? purchaseBillingAmount(po, defaults) : Number(finalizedAmount);
  if (!accounting || po.historical) return { status: 'not_recorded', amount: po.historical ? undefined : amount };
  const cfg = accounting.procurementAccounting || {};
  const state = (cfg.paymentsByPo || {})[po.id];
  const tracked = po.status === 'posted' && String(po.postedAt || '') >= String(cfg.trackPostedFrom || '2026-08-21T00:00:00+05:30');
  if (!state && !tracked && finalizedAmount == null) return { status: 'not_recorded', amount };
  const payments = state && Array.isArray(state.payments) ? state.payments : [];
  const num = x => Number(x) || 0;
  const paidAmount = Math.round(payments.reduce((n, p) => n + num(p.amount), 0));
  return { status: amount > 0 && paidAmount >= amount ? 'paid' : paidAmount > 0 ? 'partially_paid' : 'unpaid',
    amount, paidAmount, balanceDue: Math.max(0, amount - paidAmount),
    ...(includeDetails ? { mediator: state && state.mediator || cfg.mediator || 'LG', payments } : {}) };
}
module.exports = { purchasePaymentStatus, purchaseBillingAmount };

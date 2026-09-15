function purchaseBillingAmount(po, defaults = {}) {
  const num = x => Number(x) || 0;
  if (po.status === 'posted' && ((po.newProducts || []).length || (po.existingAdds || []).length)) {
    let total = 0;
    (po.newProducts || []).forEach(p => (p.variants || []).forEach(v => { total += num(v.landed) * num(v.qty); }));
    (po.existingAdds || []).forEach(v => { total += num(v.landed) * num(v.qty); });
    return Math.round(total);
  }
  const india = po.origin === 'india';
  const rate = num(po.exRate != null ? po.exRate : defaults.exRate);
  const freight = num(po.freightPerGram != null ? po.freightPerGram : defaults.freightPerGram);
  const total = (po.lines || []).reduce((n, l) => n + num(l.qty) * (india ? num(l.perPcsYuan) : num(l.perPcsYuan) * rate + num(l.weightGrams) * freight), 0);
  return Math.round(total + (india ? num(po.transportTotal) : 0));
}
// Read-only projection of accounting's PO payment ledger.
function purchasePaymentStatus(po, accounting, includeDetails, defaults) {
  if (!accounting || po.historical) return { status: 'not_recorded', amount: po.historical ? undefined : purchaseBillingAmount(po, defaults) };
  const cfg = accounting.procurementAccounting || {};
  const state = (cfg.paymentsByPo || {})[po.id];
  const tracked = po.status === 'posted' && String(po.postedAt || '') >= String(cfg.trackPostedFrom || '2026-08-21T00:00:00+05:30');
  if (!state && !tracked) return { status: 'not_recorded', amount: purchaseBillingAmount(po, defaults) };
  const payments = state && Array.isArray(state.payments) ? state.payments : [];
  const num = x => Number(x) || 0;
  const amount = purchaseBillingAmount(po, defaults);
  const paidAmount = Math.round(payments.reduce((n, p) => n + num(p.amount), 0));
  return { status: amount > 0 && paidAmount >= amount ? 'paid' : paidAmount > 0 ? 'partially_paid' : 'unpaid',
    amount, paidAmount, balanceDue: Math.max(0, amount - paidAmount),
    ...(includeDetails ? { mediator: state && state.mediator || cfg.mediator || 'Logistics Mediator', payments } : {}) };
}
module.exports = { purchasePaymentStatus, purchaseBillingAmount };

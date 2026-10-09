/* Non-cash credit selection for the ordinary vendor payment form. */
(function (root) {
  'use strict';
  var labels = { voucher: 'Voucher / Coupon / Gift card', store_credit: 'Store credit', vendor_credit: 'Vendor credit' };
  function money(value) { return Math.round((Number(value) || 0) * 100) / 100; }
  function eligible(credit, date) { return !!credit && money(credit.remainingAmount) > 0 && date >= credit.date && (!credit.expiryDate || date <= credit.expiryDate); }
  function key(credit) { return credit.refundId + '/' + credit.componentId; }
  function selection(state) {
    var coupon = state.mode !== 'money', credit = coupon && state.credit, couponAmount = coupon ? Number(state.couponAmount) : 0;
    var bills = money(state.bills), advance = !coupon && state.applyAdvance ? Math.min(money(state.advance), bills) : 0;
    var error = '';
    if (coupon && !eligible(credit, state.date)) error = 'Select an available coupon/credit valid on this payment date.';
    else if (coupon && (!(couponAmount > 0) || !Number.isFinite(couponAmount) || Math.abs(couponAmount * 100 - Math.round(couponAmount * 100)) > .00001 || couponAmount > money(credit.remainingAmount) || couponAmount > bills)) error = 'Credit used must be positive and no greater than the coupon balance or selected unpaid bills.';
    else if (coupon && state.today && state.date > state.today) error = 'Enter the actual credit-use date, not a future date.';
    else if (coupon && state.billDates.some(function (date) { return date > state.date; })) error = 'The credit-use date cannot be before the selected expense date.';
    else if (coupon && state.mode !== 'credit' && (!Number.isFinite(Number(state.bankAmount)) || Number(state.bankAmount) < 0 || Math.abs(Number(state.bankAmount) * 100 - Math.round(Number(state.bankAmount) * 100)) > .00001)) error = 'Enter the actual bank/cash/card amount with at most two decimal places; it cannot be negative.';
    var net = money(Math.max(0, bills - advance - (error ? 0 : couponAmount)));
    return { error: error, couponAmount: money(couponAmount), advance: advance, net: net,
      bankAmount: state.mode === 'credit' ? 0 : money(state.bankAmount),
      remainingCredit: credit ? money(credit.remainingAmount - (error ? 0 : couponAmount)) : 0,
      refundCredit: coupon && !error ? { refundId: credit.refundId, componentId: credit.componentId, amount: money(couponAmount) } : null };
  }
  var api = { labels: labels, money: money, eligible: eligible, key: key, selection: selection };
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  else root.expensePaymentCredits = api;
}(typeof window !== 'undefined' ? window : this));

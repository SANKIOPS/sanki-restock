'use strict';
const test = require('node:test'), assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), vm = require('node:vm'), crypto = require('node:crypto');
const credits = require('../public/expense-payment-credits');
const html = fs.readFileSync(path.join(__dirname, '../public/expenses.html'), 'utf8');
const credit = { refundId: 'RF-1', componentId: 'PART-001', mode: 'voucher', reference: 'COUPON-1512', issuer: 'Merchant', date: '2026-10-05', expiryDate: '2027-10-05', remainingAmount: 1512 };
const state = (extra = {}) => ({ mode: 'split', credit, couponAmount: 1512, bills: 2000, billDates: ['2026-10-08'], date: '2026-10-08', today: '2026-10-08', advance: 500, applyAdvance: true, bankAmount: 488, ...extra });

test('coupon + bank selection applies 1512 credit and 488 real money without automatic cash advance', () => {
  const s = credits.selection(state()); assert.equal(s.error, ''); assert.equal(s.net, 488); assert.equal(s.bankAmount, 488);
  assert.equal(s.advance, 0); assert.equal(s.remainingCredit, 0); assert.deepEqual(s.refundCredit, { refundId: 'RF-1', componentId: 'PART-001', amount: 1512 });
});
test('coupon-only partial payment keeps unpaid bill and unused coupon balances distinct', () => {
  const s = credits.selection(state({ mode: 'credit', couponAmount: 1000 }));
  assert.equal(s.net, 1000); assert.equal(s.remainingCredit, 512); assert.equal(s.bankAmount, 0); assert.equal(s.error, '');
});
test('switching back to money discards coupon and preserves normal money advance calculation', () => {
  const s = credits.selection(state({ mode: 'money', couponAmount: 99999, credit: null }));
  assert.equal(s.error, ''); assert.equal(s.advance, 500); assert.equal(s.net, 1500); assert.equal(s.refundCredit, null); assert.equal(s.couponAmount, 0);
});
test('date, remaining credit, unpaid bill and paise precision are enforced in selection', () => {
  for (const extra of [{ credit: null }, { date: '2026-10-04' }, { date: '2027-10-06' }, { couponAmount: 1512.001 },
    { couponAmount: 1513 }, { couponAmount: 0 }, { couponAmount: -1 }, { couponAmount: 'no' }, { bills: 1000 }, { billDates: ['2026-10-09'] }, { date: '2026-10-09' },
    { bankAmount: -1 }, { bankAmount: 'not-money' }, { bankAmount: 488.001 }]) {
    assert.ok(credits.selection(state(extra)).error, JSON.stringify(extra));
  }
  assert.equal(credits.eligible(credit, '2027-10-05'), true); assert.equal(credits.eligible({ ...credit, remainingAmount: 0 }, '2026-10-08'), false);
});

function ui() {
  const nodes = {};
  const field = id => nodes[id] || (nodes[id] = { value: '', textContent: '', innerHTML: '', style: {}, disabled: false, checked: false,
    setAttribute(name, value) { this[name] = value; }, removeAttribute(name) { delete this[name]; } });
  const bills = [{ due: 2000, date: '2026-10-08' }];
  const sandbox = { window: { crypto: { randomUUID: () => 'uuid-for-test' } }, document: { querySelectorAll: () => bills.map(b => ({ getAttribute: key => key === 'data-due' ? b.due : b.date })) },
    expensePaymentCredits: credits, el: field, fmt: n => '₹' + n, esc: s => String(s || '').replaceAll('<', '&lt;'), today: () => '2026-10-08',
    payMode: 'vendor', payRefundCredits: [credit], payVendorCreditAvailable: 500, payUrls: [], paySubmitting: false, payCandidatesReady: true, payRequestId: '', payOpenRevision: 0 };
  const code = html.slice(html.indexOf('    function newVendorPaymentRequest()'), html.indexOf('    window.openPay ='));
  vm.runInNewContext(code, sandbox);
  field('paySettlementMode').value = 'split'; field('payRefundCredit').value = credits.key(credit); field('payCouponAmount').value = '1512'; field('payDate').value = '2026-10-08';
  field('payAmount').value = '488'; field('payApplyVendorCredit').checked = true; field('payNote').value = 'Coupon accepted by merchant';
  return { sandbox, field, nodes };
}
test('ordinary payment UI hides bank fields for credit-only and restores them for split/money', () => {
  const { sandbox: s, field: f } = ui(); f('paySettlementMode').value = 'credit'; s.updateVendorPaymentSelection(true);
  assert.equal(f('payAmount').value, 0); assert.equal(f('payAmount').readOnly, true); assert.equal(f('payAcctWrap').style.display, 'none');
  assert.equal(f('payTypeWrap').style.display, 'none'); assert.equal(f('payProofWrap').style.display, 'none'); assert.equal(f('payGo').disabled, false);
  assert.equal(f('payNote').required, true); assert.match(f('payCreditSummary').textContent, /Still unpaid ₹488/);
  f('paySettlementMode').value = 'split'; s.updateVendorPaymentSelection(true);
  assert.equal(f('payAmount').value, 488); assert.equal(f('payAcctWrap').style.display, ''); assert.equal(f('payProofWrap').style.display, ''); assert.equal(f('payGo').disabled, true);
  assert.equal(f('payAmount').min, '0.01');
  s.payUrls.push('proof.jpg'); s.updateVendorPaymentSelection(false); assert.equal(f('payGo').disabled, false);
  f('payNote').value = ''; s.updateVendorPaymentSelection(false); assert.equal(f('payGo').disabled, true);
  f('paySettlementMode').value = 'money'; s.updateVendorPaymentSelection(true);
  assert.equal(f('payAmount').value, 1500); assert.equal(f('payCouponWrap').style.display, 'none'); assert.equal(f('payNote').required, false);
  assert.equal(f('payApplyVendorCredit').disabled, false); assert.equal(f('payBatchWrap')['data-apply-vendor-credit'], 'true');
});
test('expired credit options are disabled and selection includes code, merchant, balance and expiry', () => {
  const { sandbox: s, field: f } = ui(); s.renderPaymentCreditOptions(false);
  assert.match(f('payRefundCredit').innerHTML, /COUPON-1512.*Merchant.*available ₹1512.*expires 2027-10-05/);
  f('payDate').value = '2027-10-06'; s.renderPaymentCreditOptions(false);
  assert.match(f('payRefundCredit').innerHTML, /disabled.*unavailable on this date/);
});
test('opening another payment mode resets coupon controls, memo requirement and stale async revision', () => {
  const { sandbox: s, field: f } = ui(); s.resetPaymentCredits();
  assert.equal(s.payOpenRevision, 1); assert.equal(s.payRefundCredits.length, 0); assert.equal(f('paySettlementMode').value, 'money');
  assert.equal(f('paySettlementWrap').style.display, 'none'); assert.equal(f('payAcctWrap').style.display, ''); assert.equal(f('payNote').required, false);
  assert.match(html, /openRevision!==payOpenRevision/); assert.match(html, /window\.openReimburse = function\(id,split\)\{\s*resetPaymentCredits/);
  assert.equal(s.payCandidatesReady,false); assert.match(html,/if\(uploadRevision!==payOpenRevision\)return/);
});
test('form uses one atomic endpoint, idempotent request and lock for credit + money', () => {
  assert.match(html, /refundCredit:creditState&&creditState\.refundCredit/); assert.match(html, /requestId:payMode==='vendor'\?payRequestId/);
  assert.match(html, /if\(paySubmitting\)return/); assert.match(html, /paySubmitting=true;var paymentControls/);
  assert.match(html, /Coupon \/ merchant credit used/); assert.match(html, /filter\(function\(a\)\{return !a.accountingExcluded/);
});
test('payment credit helper is content-versioned to avoid stale browser behavior', () => {
  const source = fs.readFileSync(path.join(__dirname, '../public/expense-payment-credits.js'), 'utf8');
  const hash = crypto.createHash('sha256').update(source.replace(/\r\n/g, '\n').trimEnd()).digest('hex').slice(0,12);
  assert.ok(html.includes('/expense-payment-credits.js?v=' + hash));
});

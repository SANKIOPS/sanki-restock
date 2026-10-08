'use strict';
const test = require('node:test'), assert = require('node:assert/strict');
const refunds = require('../modules/expense-refunds');
function expense(id = 'EX-1', amount = 100, paid = amount, extra = {}) { return { id, nature: 'SANKI', date: '2026-10-01', vendor: 'Supplier', ledger: 'Flowers', amount, paidAmount: paid, status: paid === amount ? 'paid' : 'partially_paid', approvedAt: '2026-10-01T12:00:00Z', payments: paid ? [{ id: 'PAY-001', date: '2026-10-01', amount: paid, account: 'IndusInd Bank 8181', ...extra }] : [], ...extra }; }
function fixture(e = expense()) { return { expenses: { [e.id]: e }, vendorAdvances: [], receipts: [], expenseRefunds: [] }; }
function context(s) { return { today: '2026-10-08', username: 'owner', expenses: Object.values(s.expenses), checkSource() {}, checkAccount: (sources, c) => c.account || 'IndusInd Bank 8181', checkExistingMovement() {}, checkRecord() {} }; }
function input(amount = 100, extra = {}) { return { requestId: 'request-0001', sources: [{ key: 'expense:EX-1', amount }], status: 'received', date: '2026-10-07', reasonType: 'return', reason: 'Returned incorrect goods', components: [{ mode: 'bank', amount, account: 'IndusInd Bank 8181' }], ...extra }; }
function post(s, b) { const c = context(s); return refunds.post(s, refunds.prepare(s, b, c), c).refund; }
test('full refund preserves originals, posts one receipt and leaves vendor settled', () => { const s = fixture(), original = JSON.stringify(s.expenses); post(s, input()); assert.equal(JSON.stringify(s.expenses), original); assert.equal(s.receipts.length, 1); assert.equal(s.receipts[0].amount, 100); assert.equal(refunds.expenseDue(s, s.expenses['EX-1']), 0); assert.equal(refunds.billCreditEntries(s)[0].plAmount, 100); assert.equal(refunds.vendorLedgerRows(s).reduce((n, r) => n + r.out - r.in, 0), 0); });
test('partial refunds cap original payment across expense and payment selectors', () => { const s = fixture(); post(s, input(40)); assert.equal(refunds.catalog(s, Object.values(s.expenses))[0].remaining, 60); assert.throws(() => post(s, input(61, { requestId: 'request-0002', sources: [{ key: 'payment:EX-1/PAY-001', amount: 61 }] })), /remaining refundable/); assert.equal(s.receipts.length, 1); post(s, input(60, { requestId: 'request-0003' })); assert.equal(refunds.decorateExpense(s, s.expenses['EX-1']).refundStatus, 'fully_refunded'); });
test('integer paise, real dates and same-source overlap are enforced', () => { const s = fixture(); assert.throws(() => post(s, input(10.001)), /two decimal/); assert.throws(() => post(s, input(10, { date: '2026-02-30' })), /valid date/); assert.throws(() => post(s, input(10, { sources: [{ key: 'expense:EX-1', amount: 5 }, { key: 'payment:EX-1/PAY-001', amount: 5 }] })), /not both/); assert.throws(() => post(s, input(10, { components: [{ mode: 'bank', amount: 9 }] })), /must equal/); });
test('retry is idempotent even after available balance becomes zero', () => { const s = fixture(), b = input(); post(s, b); assert.equal(refunds.post(s, refunds.prepare(s, b, context(s)), context(s)).already, true); assert.equal(s.receipts.length, 1); assert.throws(() => post(s, { ...b, reason: 'different reason' }), /different details/); });
test('cash and voucher split creates only actual monetary receipt plus credit wallet', () => { const s = fixture(); post(s, input(100, { components: [{ mode: 'bank', account: 'IndusInd Bank 8181', amount: 30 }, { mode: 'voucher', issuer: 'Supplier', reference: 'CREDIT100', expiryDate: '2026-12-31', amount: 70 }] })); assert.equal(s.receipts.length, 1); assert.equal(s.receipts[0].amount, 30); assert.equal(s.vendorAdvances.length, 1); assert.equal(s.vendorAdvances[0].remainingAmount, 70); assert.equal(s.vendorAdvances[0].creditOnly, true); });
test('voucher refund strips an obsolete bank account and stores only merchant credit', () => {
  for (const mode of ['voucher', 'store_credit', 'vendor_credit']) {
    const s = fixture(expense('EX-1', 1512));
    const record = post(s, input(1512, { components: [{ mode, amount: 1512, account: 'Axis Bank 3448', reference: '000012345678901234567890' }] }));
    assert.equal(record.components[0].account, ''); assert.equal(record.components[0].issuer, 'Supplier');
    assert.equal(s.receipts.length, 0); assert.equal(s.vendorAdvances[0].account, 'Refund credit');
    assert.equal(s.vendorAdvances[0].remainingAmount, 1512); assert.equal(s.vendorAdvances[0].paymentReference, '000012345678901234567890');
    const reloaded = JSON.parse(JSON.stringify(s)); assert.equal(refunds.viewRecord(reloaded, reloaded.expenseRefunds[0], '2026-10-08').components[0].reference, '000012345678901234567890');
  }
});
test('unpaid credit note reduces bill and payable without a receipt', () => { const s = fixture(expense('EX-1', 100, 0)); post(s, input(100, { components: [{ mode: 'credit_note', amount: 100 }] })); assert.equal(s.receipts.length, 0); assert.equal(refunds.expenseDue(s, s.expenses['EX-1']), 0); assert.equal(refunds.expenseTotals(s, 'EX-1').refunded, 0); assert.equal(refunds.billCreditEntries(s)[0].amount, 100); });
test('refund of a partially paid bill does not recreate an already owed amount', () => { const s = fixture(expense('EX-1', 200, 100)); post(s, input(50)); assert.equal(refunds.expenseDue(s, s.expenses['EX-1']), 100); post(s, input(100, { requestId: 'request-0002', components: [{ mode: 'credit_note', amount: 100 }] })); assert.equal(refunds.expenseDue(s, s.expenses['EX-1']), 0); });
test('overpayment advance refund reduces advance, never expense or P&L', () => { const s = fixture(); s.vendorAdvances.push({ id: 'VA-1', date: '2026-10-01', nature: 'SANKI', vendor: 'Supplier', amount: 218, remainingAmount: 218, account: 'IndusInd Bank 8181' }); post(s, input(100, { sources: [{ key: 'advance:VA-1', amount: 100 }], reasonType: 'overpayment' })); assert.equal(s.vendorAdvances[0].remainingAmount, 118); assert.equal(refunds.billCreditEntries(s).length, 0); assert.equal(refunds.expenseDue(s, s.expenses['EX-1']), 0); });
test('consolidated payment refund uses selected bill allocations, not batch gross', () => { const s = fixture(expense('EX-1', 40, 40)); s.expenses['EX-2'] = expense('EX-2', 60, 60); Object.values(s.expenses).forEach(e => Object.assign(e.payments[0], { batchPaymentId: 'BATCH1', grossPaymentAmount: 100 })); post(s, input(100, { sources: [{ key: 'payment:EX-1/PAY-001', amount: 40 }, { key: 'payment:EX-2/PAY-001', amount: 60 }] })); assert.equal(s.receipts.length, 1); assert.equal(refunds.expenseTotals(s, 'EX-1').refunded, 40); assert.equal(refunds.expenseTotals(s, 'EX-2').refunded, 60); });
test('personal refund reduces reimbursement due or creates employee recovery', () => { const s = fixture(expense('EX-1', 100, 100, { paidAlready: true, personalPaidAmount: 100, reimbursementAmount: 0, personalFunds: true, claimant: 'shivam' })); post(s, input(40, { components: [{ mode: 'bank', amount: 40, receiver: 'payer', account: 'Shivam personal bank' }] })); assert.equal(refunds.reimbursementPosition(s, s.expenses['EX-1']).pending, 60); s.expenses['EX-1'].reimbursementAmount = 100; assert.equal(refunds.reimbursementPosition(s, s.expenses['EX-1']).recoverable, 40); s.receipts.push({ refundRecoveryExpenseId: 'EX-1', amount: 20, date: '2026-10-08' }); assert.equal(refunds.reimbursementPosition(s, s.expenses['EX-1']).recoverable, 20); });
test('a company refund of an employee payment keeps reimbursement owed to employee', () => { const s = fixture(expense('EX-1', 100, 100, { personalFunds: true, personalPaidAmount: 100 })); post(s, input()); assert.equal(refunds.reimbursementPosition(s, s.expenses['EX-1']).pending, 100); });
test('pending returns have no ledger effect and receipts consume pending budget', () => { const s = fixture(), pending = post(s, input(80, { status: 'pending' })); assert.equal(s.receipts.length, 0); assert.equal(refunds.expenseTotals(s, 'EX-1').billCredit, 0); post(s, input(50, { requestId: 'request-0002', parentId: pending.id })); assert.equal(refunds.viewRecord(s, pending, '2026-10-08').pendingAmount, 30); assert.throws(() => post(s, input(31, { requestId: 'request-0003', parentId: pending.id })), /outstanding pending/); });
test('existing refund/card receipt is linked without duplicate monetary posting', () => { const s = fixture(); post(s, input(100, { components: [{ mode: 'card', amount: 100, account: 'Card 1234', externalMovementId: 'CCE-1' }] })); assert.equal(s.receipts.length, 0); assert.equal(refunds.billCreditEntries(s)[0].plAmount, 0); assert.equal(refunds.expenseDue(s, s.expenses['EX-1']), 0); assert.ok(refunds.protectsMovement(s, 'CCE-1')); });
test('same existing movement cannot be repeated within a split receipt', () => { const s = fixture(); assert.throws(() => post(s, input(100, { components: [{ mode: 'bank', amount: 50, externalMovementId: 'REC-1' }, { mode: 'bank', amount: 50, externalMovementId: 'REC-1' }] })), /each existing receipt once/); });

test('partly paid pending return covers the whole bill and accepts paid/unpaid receipts separately', () => {
  const s = fixture(expense('EX-1', 200, 80)), pending = post(s, input(200, { status: 'pending' }));
  assert.equal(refunds.viewRecord(s, pending, '2026-10-08').pendingAmount, 200);
  assert.equal(refunds.expenseDue(s, s.expenses['EX-1']), 120);
  post(s, input(80, { parentId: pending.id, requestId: 'pending-cash-0002' }));
  const credit = post(s, input(120, { parentId: pending.id, requestId: 'pending-credit-0003', components: [{ mode: 'credit_note', amount: 120 }] }));
  assert.equal(credit.amount, 120); assert.equal(refunds.expenseDue(s, s.expenses['EX-1']), 0);
  assert.equal(refunds.viewRecord(s, pending, '2026-10-08').displayStatus, 'completed');
});
test('unpaid pending return has no financial allocation until credit note is received', () => {
  const s = fixture(expense('EX-1', 100, 0)), pending = post(s, input(100, { status: 'pending' }));
  assert.equal(pending.sources[0].paymentAllocations.length, 0); assert.equal(s.receipts.length, 0);
  assert.equal(refunds.expenseDue(s, s.expenses['EX-1']), 100);
});
test('bill history of combined refund exposes only this expense allocation', () => {
  const s = fixture(expense('EX-1', 40)); s.expenses['EX-2'] = expense('EX-2', 60);
  post(s, input(100, { sources: [{ key: 'expense:EX-1', amount: 40 }, { key: 'expense:EX-2', amount: 60 }] }));
  const row = refunds.decorateExpense(s, s.expenses['EX-1']).refundHistory[0];
  assert.equal(row.amount, 40); assert.equal(row.sources, undefined); assert.equal(row.components, undefined);
});
test('voucher ledger belongs to accepting merchant, not original supplier', () => {
  const s = fixture(); post(s, input(100, { components: [{ mode: 'voucher', amount: 100, issuer: 'Replacement Merchant', reference: 'V-NEW' }] }));
  assert.equal(s.vendorAdvances[0].vendor, 'Replacement Merchant'); assert.equal(s.vendorAdvances[0].issuer, 'Replacement Merchant');
});
test('personal refund does not enter company bank movements; date-limited positions remain accurate', () => {
  const s = fixture(expense('EX-1', 100, 100, { personalFunds: true, personalPaidAmount: 100 }));
  post(s, input(40, { components: [{ mode: 'cash', receiver: 'payer', account: 'Shivam Cash', amount: 40 }] }));
  assert.equal(s.receipts.length, 0); assert.equal(refunds.reimbursementPosition(s, s.expenses['EX-1'], '2026-10-06').pending, 100);
  assert.equal(refunds.reimbursementPosition(s, s.expenses['EX-1'], '2026-10-08').pending, 60);
  assert.equal(refunds.reimbursementPosition(s, s.expenses['EX-1'], '2026-09-30').pending, 0);
});
test('advance refund protects original consolidated payment and reimbursement movements', () => {
  const s = fixture(); s.expenses['EX-1'].payments[0].batchPaymentId = 'GROSS-BATCH';
  s.vendorAdvances.push({ id: 'VA-1', nature: 'SANKI', vendor: 'Supplier', amount: 218, remainingAmount: 218, date: '2026-10-01', account: 'IndusInd Bank 8181', batchPaymentId: 'GROSS-BATCH', paymentReference: 'EX-1/PAY-001' });
  post(s, input(100, { sources: [{ key: 'advance:VA-1', amount: 100 }], reasonType: 'overpayment' }));
  assert.equal(refunds.hasSource(s, 'EX-1'), true); assert.equal(refunds.protectsMovement(s, 'EX-1/PAY-001'), true);
  assert.equal(refunds.protectsMovement(s, 'EX-1/REIM-001'), true);
});

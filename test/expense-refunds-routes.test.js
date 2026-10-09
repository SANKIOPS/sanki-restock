'use strict';
const test = require('node:test'), assert = require('node:assert/strict'), fs = require('node:fs'), os = require('node:os'), path = require('node:path'), vm = require('node:vm');
const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'sanki-expense-refunds-'));
process.env.DATA_PATH = path.join(temp, 'data.json');
fs.writeFileSync(path.join(temp, 'expenses.json'), '{}');
const express = require('express'), expensesModule = require('../modules/expenses');
const app = express(); app.use(express.json()); app.use((req, res, next) => { var role = req.headers['x-role'] || 'owner'; req.user = { username: role === 'admin' ? 'prashant' : 'owner-test', role, roles: [role] }; next(); });
app.use(expensesModule.router); app.use(require('../modules/credit-cards').router);
const server = app.listen(0, '127.0.0.1');
test.after(async () => { await new Promise(resolve => server.close(resolve)); fs.rmSync(temp, { recursive: true, force: true }); });
const filename = path.join(temp, 'expenses.json'), account = 'IndusInd Bank 8181', base = '/api/expenses/expense-refunds';
const read = () => JSON.parse(fs.readFileSync(filename, 'utf8'));
const write = s => fs.writeFileSync(filename, JSON.stringify(s));
async function api(route, body, options = {}) { await new Promise(resolve => server.listening ? resolve() : server.once('listening', resolve)); const res = await fetch('http://127.0.0.1:' + server.address().port + route, { method: body === undefined ? 'GET' : options.method || 'POST', headers: { 'Content-Type': 'application/json', 'x-role': options.role || 'owner' }, body: body === undefined ? undefined : JSON.stringify(body) }); return { status: res.status, ...await res.json() }; }
function expense(id = 'EX-TEST', amount = 100, paid = amount) { return { id, date: '2026-10-01', nature: 'SANKI', vendor: 'Refund Supplier', ledger: 'Flowers', type: 'variable', channel: 'Shared', amount, paidAmount: paid, status: paid === amount ? 'paid' : 'approved', approvedAt: '2026-10-01T12:00:00Z', createdBy: 'shivam', claimant: 'shivam', payments: paid ? [{ id: 'PAY-001', date: '2026-10-01', amount: paid, account }] : [], personalPaidAmount: 0, reimbursementAmount: 0 }; }
async function seed(e = expense()) { await api('/api/expenses/config'); const s = read(); Object.assign(s, { expenses: { [e.id]: e }, expenseRefunds: [], expenseRefundSeq: 0, receipts: [], vendorAdvances: [], vendorPaymentRequests: {}, transfers: [], reconciliationExpenses: [], bankStatements: {}, bankDateOverrides: {}, bankReconciliationDrafts: {}, cashReconciliations: [], openingBalances: { [account]: 0 }, vendors: { 'refund supplier': { name: 'Refund Supplier' } }, auditLog: [] }); write(s); fs.writeFileSync(path.join(temp, 'credit-cards.json'), JSON.stringify({ cards: {}, statements: {} })); return s; }
function input(amount = 100, extra = {}) { return { requestId: 'request-test-0001', sources: [{ key: 'expense:EX-TEST', amount }], status: 'received', date: '2026-10-07', reasonType: 'return', reason: 'Goods returned and money received', components: [{ mode: 'bank', amount, account }], ...extra }; }

async function seedPaymentCoupon(credit = 1512, bill = 2000, mode = 'voucher') {
  await seed(expense('EX-TEST', credit));
  const refund = await api(base, input(credit, { components: [{ mode, amount: credit, issuer: 'Refund Supplier', reference: 'COUPON-' + credit, expiryDate: '2026-12-31' }] }));
  assert.equal(refund.success, true, refund.error);
  const s = read(); s.expenses['EX-NEXT'] = { ...expense('EX-NEXT', bill, 0), date: '2026-10-08' }; write(s);
  return { expenseIds: ['EX-NEXT'], refundCredit: { refundId: refund.refund.id, componentId: 'PART-001', amount: credit },
    amount: bill - credit, account, paymentProof: '/api/expenses/photo/split-coupon-test.jpg', paymentType: 'UPI',
    date: '2026-10-08', note: 'Coupon actually accepted on replacement order', requestId: 'normal-coupon-payment-0001' };
}

test('normal payment candidates offer separate same-merchant coupons without widening access', async () => {
  await seedPaymentCoupon();
  const candidates = await api('/api/expenses/EX-NEXT/payment-candidates');
  assert.equal(candidates.refundCredits.length, 1); assert.equal(candidates.availableVendorCredit, 0);
  assert.equal(candidates.refundCredits[0].reference, 'COUPON-1512'); assert.equal(candidates.refundCredits[0].remainingAmount, 1512);
  assert.deepEqual((await api('/api/expenses/EX-NEXT/payment-candidates', undefined, { role: 'accounting' })).refundCredits, []);
  let s = read(); s.expenses['EX-NEXT'].vendor = 'Different Merchant'; write(s);
  assert.deepEqual((await api('/api/expenses/EX-NEXT/payment-candidates')).refundCredits, []);
  s = read(); s.expenses['EX-NEXT'].vendor = 'Refund Supplier'; s.expenses['EX-NEXT'].nature = 'SAMAST'; write(s);
  assert.deepEqual((await api('/api/expenses/EX-NEXT/payment-candidates')).refundCredits, []);
  s = read(); s.expenses['EX-NEXT'].nature = 'SANKI'; s.expenses['EX-TEST'].ownerOnly = true; write(s);
  assert.deepEqual((await api('/api/expenses/EX-NEXT/payment-candidates', undefined, { role: 'admin' })).refundCredits, []);
});

test('normal split payment consumes coupon and debits only real money, retries once and uses existing undo', async () => {
  const body = await seedPaymentCoupon();
  const paid = await api('/api/expenses/vendor-payments/batch', body); assert.equal(paid.success, true, paid.error);
  let s = read(); const bill = s.expenses['EX-NEXT'], wallet = s.vendorAdvances[0];
  assert.equal(paid.refundCreditApplied, 1512); assert.equal(paid.total, 488); assert.equal(bill.paidAmount, 2000); assert.equal(bill.status, 'paid');
  assert.equal(bill.payments.length, 1); assert.equal(bill.payments[0].amount, 488); assert.equal(wallet.remainingAmount, 0);
  assert.equal(s.receipts.length, 0); assert.equal(s.expenses['EX-TEST'].payments[0].amount, 1512);
  const use = wallet.applications[0]; assert.equal(use.creditReference, 'COUPON-1512'); assert.equal(use.refundCredit, true);
  assert.equal(use.batchPaymentId, bill.payments[0].batchPaymentId);
  assert.ok(s.auditLog.some(x => x.action === 'EXPENSE_REFUND_CREDIT_REDEEMED'));
  const list = await api('/api/expenses/list?id=EX-NEXT'); assert.equal(list.expenses[0].netExpenseAmount, 2000); assert.equal(list.expenses[0].balanceDue, 0);
  const ledger = await api('/api/expenses/account-ledger?nature=SANKI&account=' + encodeURIComponent(account));
  const rows = ledger.entries || ledger.rows; assert.equal(rows.find(e => e.id === 'EX-NEXT/PAY-001').debit, 488);
  assert.equal(rows.some(e => e.id === wallet.id || e.kind === 'expense_refund'), false);
  assert.equal((await api('/api/expenses/vendor-payments/batch', body)).already, true);
  assert.equal(read().expenses['EX-NEXT'].payments.length, 1); assert.equal(read().vendorAdvances[0].applications.length, 1);
  assert.equal((await api('/api/expenses/vendor-payments/batch', { ...body, amount: 500 })).status, 409);
  const reversed = await api(base + '/' + body.refundCredit.refundId + '/redemptions/' + use.id + '/void', { reason: 'Correct the coupon use, keep real bank payment' });
  assert.equal(reversed.success, true, reversed.error); s = read();
  assert.equal(s.vendorAdvances[0].remainingAmount, 1512); assert.equal(s.expenses['EX-NEXT'].paidAmount, 488);
  assert.equal(s.expenses['EX-NEXT'].payments[0].amount, 488); assert.equal(s.expenses['EX-NEXT'].status, 'partially_paid');
});

test('coupon-only full or partial payments require no bank account/proof and preserve unused credit', async () => {
  for (const bill of [1000, 2000]) {
    const body = await seedPaymentCoupon(1512, bill); body.amount = 0; body.refundCredit.amount = 1000; body.account = ''; delete body.paymentProof;
    const paid = await api('/api/expenses/vendor-payments/batch', body); assert.equal(paid.success, true, paid.error);
    const s = read(); assert.equal(s.vendorAdvances[0].remainingAmount, 512); assert.equal(s.expenses['EX-NEXT'].paidAmount, 1000);
    assert.equal((s.expenses['EX-NEXT'].payments || []).length, 0); assert.equal(s.receipts.length, 0);
    assert.equal(s.expenses['EX-NEXT'].status, bill === 1000 ? 'paid' : 'partially_paid');
  }
});

test('normal multi-bill coupon allocation is oldest first and cannot overconsume coupon or advances', async () => {
  const body = await seedPaymentCoupon(1512, 2000); let s = read();
  s.expenses['EX-OLDER'] = { ...expense('EX-OLDER', 500, 0), date: '2026-10-07' };
  s.vendorAdvances.push({ id: 'CASH-ADVANCE', nature: 'SANKI', vendor: 'Refund Supplier', amount: 200, remainingAmount: 200, date: '2026-10-01', applications: [] }); write(s);
  Object.assign(body, { expenseIds: ['EX-NEXT', 'EX-OLDER'], applyVendorCredit: true, amount: 788 });
  const paid = await api('/api/expenses/vendor-payments/batch', body); assert.equal(paid.success, true, paid.error);
  assert.equal(paid.refundCreditApplied, 1512); assert.equal(paid.vendorCreditApplied, 200);
  assert.deepEqual(paid.refundCreditAllocations.map(a => a.amount), [500, 1012]);
  s = read(); assert.equal(s.expenses['EX-OLDER'].status, 'paid'); assert.equal(s.expenses['EX-NEXT'].status, 'paid');
  assert.equal(s.vendorAdvances[0].applications.length, 2); assert.equal(s.vendorAdvances[0].remainingAmount, 0);
  assert.equal(s.vendorAdvances[1].remainingAmount, 0); assert.notEqual(s.vendorAdvances[0].applications[0].id, s.vendorAdvances[0].applications[1].id);
});

test('split coupon payment validates every leg before mutation', async () => {
  const body = await seedPaymentCoupon();
  for (const patch of [{ paymentProof: '' }, { account: 'Not an authorized account' }, { note: '' },
    { refundCredit: { ...body.refundCredit, amount: 1512.001 } }, { refundCredit: { ...body.refundCredit, amount: 1513 } },
    { refundCredit: { ...body.refundCredit, refundId: 'RF-MISSING' } }, { date: '2026-10-06' }, { amount: 488.001 }, { requestId: '' }]) {
    const before = read(); const result = await api('/api/expenses/vendor-payments/batch', { ...body, ...patch }); assert.ok(result.status >= 400, JSON.stringify(patch));
    const after = read(); assert.deepEqual(after.vendorAdvances, before.vendorAdvances); assert.deepEqual(after.expenses, before.expenses);
    assert.deepEqual(after.vendorPaymentRequests, before.vendorPaymentRequests); assert.deepEqual(after.auditLog, before.auditLog);
  }
});

test('normal coupon flow preserves expiry, merchant/entity and Owner-only restrictions server-side', async () => {
  const body = await seedPaymentCoupon();
  assert.equal((await api('/api/expenses/vendor-payments/batch', body, { role: 'accounting' })).status, 403);
  for (const update of ['expiry', 'merchant', 'entity', 'source-private', 'target-private', 'voided', 'excluded']) {
    const fresh = await seedPaymentCoupon(); const s = read(); let role = 'owner';
    if (update === 'expiry') s.vendorAdvances[0].expiryDate = '2026-10-07';
    if (update === 'merchant') s.expenses['EX-NEXT'].vendor = 'Other Merchant';
    if (update === 'entity') s.expenses['EX-NEXT'].nature = 'SAMAST';
    if (update === 'source-private') { s.expenses['EX-TEST'].ownerOnly = true; role = 'admin'; }
    if (update === 'target-private') { s.expenses['EX-NEXT'].ownerOnly = true; role = 'admin'; }
    if (update === 'voided') s.expenseRefunds[0].status = 'voided';
    if (update === 'excluded') s.vendorAdvances[0].accountingExcluded = true;
    write(s); const result = await api('/api/expenses/vendor-payments/batch', fresh, { role }); assert.ok(result.status >= 400, update + ': ' + JSON.stringify(result));
    assert.equal(read().vendorAdvances[0].remainingAmount, 1512); assert.equal(read().expenses['EX-NEXT'].paidAmount, 0);
  }
});

test('store credit and vendor credit also work through the ordinary payment procedure', async () => {
  for (const mode of ['store_credit', 'vendor_credit']) {
    const body = await seedPaymentCoupon(100, 150, mode); const result = await api('/api/expenses/vendor-payments/batch', body);
    assert.equal(result.success, true, result.error); assert.equal(result.refundCreditApplied, 100); assert.equal(read().expenses['EX-NEXT'].payments[0].amount, 50);
  }
});
test('real router posts once, previews read-only, P&L/account/vendor/list agree', async () => { await seed(); const original = read().expenses; const preview = await api(base + '/preview', input()); assert.equal(preview.success, true); assert.equal(read().expenseRefunds.length, 0); const saved = await api(base, input()); assert.equal(saved.success, true, saved.error); for(const k of ['amount','paidAmount','date','vendor'])assert.equal(read().expenses['EX-TEST'][k],original['EX-TEST'][k]);assert.equal(read().expenses['EX-TEST'].payments[0].amount,original['EX-TEST'].payments[0].amount); assert.equal(expensesModule.summaryForPL('2026-10-01', '2026-10-08').Shared.variable, 0); const ledger = await api('/api/expenses/account-ledger?nature=SANKI&account=' + encodeURIComponent(account)); assert.equal(ledger.success, true); const entries = ledger.entries || ledger.rows; assert.equal(entries.filter(e => e.kind === 'expense_refund').length, 1); assert.equal((await api('/api/expenses/vendors?nature=SANKI')).vendors.find(v => v.name === 'Refund Supplier').ledgerClosingBalance, 0); const list = await api('/api/expenses/list?id=EX-TEST'); assert.equal(list.expenses[0].netExpenseAmount, 0); assert.equal(list.expenses[0].payments[0].amount, 100); const retry = await api(base, input()); assert.equal(retry.already, true); assert.equal(read().receipts.length, 1); });
test('permission and source-identity checks cannot be bypassed through API', async () => { await seed(); assert.equal((await api(base, input(), { role: 'accounting' })).status, 403); let s = read(); s.expenses['EX-TEST'].ownerOnly = true; write(s); assert.equal((await api(base, input(), { role: 'admin' })).status, 403); assert.equal((await api(base, undefined, { role: 'admin' })).sources.length, 0); assert.equal(read().receipts.length, 0); });
test('finalized receiving dates reject new postings but allow linking existing evidence', async () => { await seed(); let s = read(); s.bankStatements[account] = { reconciledThrough: '2026-10-07' }; write(s); assert.equal((await api(base, input())).status, 409); s.receipts.push({ id: 'REC-OLD', nature: 'SANKI', date: '2026-10-07', account, amount: 100, receiptType: 'refund' }); write(s); const result = await api(base, input(100, { components: [{ mode: 'bank', amount: 100, account, externalMovementId: 'REC-OLD' }] })); assert.equal(result.success, true, result.error); assert.equal(read().receipts.length, 1); assert.equal((await api(base)).existingMovements.length, 0); });
test('source edits/deletes and reconciled refund reversal are blocked; normal reversal audited', async () => { await seed(); const r = await api(base, input()); assert.equal((await api('/api/expenses/EX-TEST', { amount: 90, editReason: 'change expense' })).status, 409); assert.equal((await api('/api/expenses/EX-TEST', { reason: 'delete expense' }, { method: 'DELETE' })).status, 409); assert.equal((await api('/api/expenses/EX-TEST/payments/PAY-001', { reason: 'remove payment' }, { method: 'DELETE' })).status, 409); let s = read(); s.bankDateOverrides[s.receipts[0].id] = { bankDate: '2026-10-07' }; write(s); assert.equal((await api(base + '/' + r.refund.id + '/void', { reason: 'wrong refund' })).status, 409); s.bankDateOverrides = {}; write(s); const result = await api(base + '/' + r.refund.id + '/void', { reason: 'Wrong source, reversing for correction' }); assert.equal(result.success, true, result.error); assert.equal(read().receipts[0].accountingExcluded, true); assert.equal(expensesModule.summaryForPL('2026-10-01', '2026-10-08').Shared.variable, 100); assert.ok(read().auditLog.some(r => r.action === 'EXPENSE_REFUND_VOIDED')); });
test('voucher redemption, idempotent retry, expiry and auditable undo', async () => { await seed(); const r = await api(base, input(100, { components: [{ mode: 'voucher', amount: 100, issuer: 'Refund Supplier', reference: 'VOUCHER-100', expiryDate: '2026-10-08' }] })); assert.equal(r.success, true, r.error); let s = read(); s.expenses['EX-NEXT'] = expense('EX-NEXT', 60, 0); write(s); const b = { componentId: 'PART-001', expenseId: 'EX-NEXT', amount: 60, date: '2026-10-08', reason: 'Settling replacement bill', requestId: 'credit-application-0001' }, path = base + '/' + r.refund.id; const redeem = await api(path + '/redeem', b); assert.equal(redeem.success, true, redeem.error); assert.equal((await api(path + '/redeem', b)).already, true); assert.equal(read().vendorAdvances[0].remainingAmount, 40); assert.equal(read().receipts.length, 0); assert.equal((await api(path + '/void', { reason: 'wrong voucher' })).status, 409); const use = read().vendorAdvances[0].applications[0]; assert.equal((await api(path + '/redemptions/' + use.id + '/void', { reason: 'Wrong bill for credit application' })).success, true); assert.equal(read().vendorAdvances[0].remainingAmount, 100); assert.equal(read().expenses['EX-NEXT'].paidAmount, 0); assert.equal((await api(path + '/redeem', { ...b, requestId: 'expired-credit-0001', date: '2026-10-09' })).status, 400); assert.equal((await api(path + '/void', { reason: 'Reverse unredeemed voucher' })).success, true); });
test('voucher preview and persisted ledger ignore a stale bank destination', async () => {
  await seed();
  const body = input(100, { components: [{ mode: 'voucher', amount: 100, account, issuer: '', reference: '00001234' }] });
  const preview = await api(base + '/preview', body); assert.equal(preview.success, true, preview.error);
  assert.equal(preview.preview.components[0].account, ''); assert.equal(preview.preview.components[0].issuer, 'Refund Supplier');
  assert.deepEqual(preview.impact.moneyIntoAccounts, []); assert.equal(read().expenseRefunds.length, 0);
  const saved = await api(base, body); assert.equal(saved.success, true, saved.error);
  assert.equal(read().receipts.length, 0); assert.equal(read().vendorAdvances[0].remainingAmount, 100);
  assert.equal(read().vendorAdvances[0].paymentReference, '00001234');
  const listed = await api(base + '?search=00001234'); assert.equal(listed.refunds.length, 1);
  assert.equal(listed.refunds[0].components[0].account, ''); assert.equal(listed.summary.moneyReceived, 0);
  assert.equal(listed.summary.creditsAvailable, 100);
  const ledger = await api('/api/expenses/account-ledger?nature=SANKI&account=' + encodeURIComponent(account));
  assert.equal((ledger.entries || ledger.rows).filter(e => e.kind === 'expense_refund').length, 0);
});
test('fully credited unpaid bill is removed from payables and cannot be repaid', async () => { await seed(expense('EX-TEST', 100, 0)); const r = await api(base, input(100, { components: [{ mode: 'credit_note', amount: 100 }] })); assert.equal(r.success, true, r.error); assert.equal((await api('/api/expenses/pending-payments')).expenses.length, 0); assert.equal(read().expenses['EX-TEST'].status, 'approved'); const payment = await api('/api/expenses/vendor-payments/batch', { expenseIds: ['EX-TEST'], amount: 100, account, paymentProof: '/api/expenses/photo/test.jpg' }); assert.equal(payment.status, 400); });
test('personally refunded reimbursed expense creates recovery, not company refund receipt', async () => { const e = expense(); Object.assign(e, { paidAlready: true, personalPaidAmount: 100, reimbursementAmount: 100, reimbursementStatus: 'reimbursed', reimbursementPayments: [{ id: 'REIM-001', date: '2026-10-02', amount: 100, account }] }); Object.assign(e.payments[0], { personalFunds: true, account: 'Shivam Cash' }); await seed(e); const r = await api(base, input(40, { components: [{ mode: 'cash', receiver: 'payer', account: 'Shivam Cash', amount: 40 }] })); assert.equal(r.success, true, r.error); assert.equal(r.view.summary.moneyReceived, 0); assert.equal(r.view.recoverables[0].amount, 40); const b = { expenseId: 'EX-TEST', amount: 20, date: '2026-10-08', account, mode: 'bank', requestId: 'recovery-request-0001', reason: 'Employee repaid excess reimbursement' }; const recovery = await api(base + '/recoveries', b); assert.equal(recovery.success, true, recovery.error); assert.equal(recovery.view.recoverables[0].amount, 20); assert.equal((await api(base + '/recoveries', b)).already, true); assert.equal((await api(base + '/' + r.refund.id + '/void', { reason: 'Incorrect refund' })).status, 409); });
test('credit-card manual refund counts once when later linked to statement credit', async () => { const e = expense(); e.payments[0].account = 'Test Card 1234'; e.payments[0].creditCardId = 'CC-1'; await seed(e); const cc = { cards: { 'CC-1': { id: 'CC-1', name: 'Test Card', last4: '1234', openingOutstanding: 0, active: true } }, statements: {}, payments: [], audit: [] }; fs.writeFileSync(path.join(temp, 'credit-cards.json'), JSON.stringify(cc)); const refund = await api(base, input(40, { components: [{ mode: 'card', amount: 40, account: 'Test Card 1234' }] })); assert.equal(refund.success, true, refund.error); let cards = await api('/api/expenses/credit-cards'); assert.equal(cards.cards[0].outstanding, 60); cc.statements['CCS-1'] = { id: 'CCS-1', cardId: 'CC-1', status: 'review', rows: [{ id: '1', date: '2026-10-07', credit: 40, debit: 0, amount: 40, classification: 'refund', narration: 'Supplier refund', merchant: 'Refund Supplier', nature: 'SANKI', channel: 'POS', type: 'variable', category: 'Flowers', expenseRefundReceiptId: read().receipts[0].id }] }; fs.writeFileSync(path.join(temp, 'credit-cards.json'), JSON.stringify(cc)); const finalized = await api('/api/expenses/credit-cards/statements/CCS-1/finalize', {}); assert.equal(finalized.success, true, finalized.error); assert.equal(expensesModule.summaryForPL('2026-10-01', '2026-10-08').Shared.variable, 60); assert.equal(expensesModule.summaryForPL('2026-10-01', '2026-10-08').POS.variable, 0); const ledger = await api('/api/expenses/credit-cards/CC-1/ledger'); assert.equal(ledger.entries.filter(x => x.credit === 40).length, 1); });
test('inline UI scripts compile and refund markup is present', () => { const html = fs.readFileSync(path.join(__dirname, '../public/expenses.html'), 'utf8'); for (const m of html.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/g)) if (m[1].trim()) assert.doesNotThrow(() => new vm.Script(m[1])); assert.match(html, /data-t="refunds"/); assert.doesNotThrow(() => new vm.Script(fs.readFileSync(path.join(__dirname, '../public/expense-refunds.js'), 'utf8'))); });

test('refund-linked accounting fields and batch sibling account changes are protected', async () => {
  const e = expense(); Object.assign(e, { paymentType: 'Bank', account, fundedBy: 'company', billPhoto: '/api/expenses/photo/bill.jpg' });
  e.payments[0].batchPaymentId = 'BATCH-GUARDED'; await seed(e);
  let s = read(); s.expenses['EX-SIBLING'] = expense('EX-SIBLING', 60); Object.assign(s.expenses['EX-SIBLING'], { paymentType: 'Bank', account });
  s.expenses['EX-SIBLING'].payments[0].batchPaymentId = 'BATCH-GUARDED'; write(s);
  assert.equal((await api(base, input(40))).success, true);
  for (const change of [{ account: 'Prashant Axis 3645' }, { paymentAccount: 'Prashant Axis 3645' }, { paymentType: 'Cash' }, { requestedAmount: 90 }, { fundedBy: 'claimant' }]) {
    assert.equal((await api('/api/expenses/EX-TEST', { ...change, editReason: 'Correct source' })).status, 409);
  }
  assert.equal((await api('/api/expenses/EX-SIBLING', { paymentAccount: 'Prashant Axis 3645', editReason: 'Correct batch paying account' })).status, 409);
  assert.equal(read().expenses['EX-SIBLING'].payments[0].account, account);
  const unchanged = await api('/api/expenses/EX-TEST', { amount: 100, date: e.date, nature: e.nature, vendor: e.vendor, paymentType: e.paymentType, paymentAccount: account, paidAlready: false, isInstallment: false, requestedAmount: 100, particulars: 'Corrected description', editReason: 'Description clarification' });
  assert.equal(unchanged.success, true, unchanged.error);
});
test('credit notes constrain single-payment route, not just consolidated payments', async () => {
  await seed(expense('EX-TEST', 100, 0));
  assert.equal((await api(base, input(40, { components: [{ mode: 'credit_note', amount: 40 }] }))).success, true);
  const overpay = await api('/api/expenses/EX-TEST/pay', { amount: 61, account, paymentType: 'Bank', paymentProof: '/api/expenses/photo/pay.jpg', proof: '/api/expenses/photo/pay.jpg', date: '2026-10-08' });
  assert.equal(overpay.status, 400); assert.match(overpay.error, /outstanding.*60/);
  const paid = await api('/api/expenses/EX-TEST/pay', { amount: 60, account, paymentType: 'Bank', paymentProof: '/api/expenses/photo/pay.jpg', proof: '/api/expenses/photo/pay.jpg', date: '2026-10-08' });
  assert.equal(paid.success, true, paid.error); assert.equal(paid.expense.status, 'paid');
});
test('owner-only refund history stays private after logging, including pending returns', async () => {
  const e = expense(); e.ownerOnly = true; await seed(e);
  const record = await api(base, input(100, { status: 'pending' })); assert.equal(record.success, true, record.error);
  assert.equal((await api(base, undefined, { role: 'admin' })).refunds.length, 0);
  assert.equal((await api(base + '/' + record.refund.id + '/void', { reason: 'Attempted correction' }, { role: 'admin' })).status, 403);
});
test('pending source amounts are unaffected by list date filters', async () => {
  await seed(); const pending = await api(base, input(100, { status: 'pending', date: '2026-10-01' }));
  assert.equal(pending.success, true, pending.error);
  const received = await api(base, input(40, { parentId: pending.refund.id, requestId: 'parent-receipt-0002', date: '2026-10-07' }));
  assert.equal(received.success, true, received.error);
  const filtered = await api(base + '?to=2026-10-01'); assert.equal(filtered.refunds.length, 1);
  assert.equal(filtered.refunds[0].pendingAmount, 60); assert.equal(filtered.refunds[0].pendingSources[0].amount, 60);
});
test('voucher to a different accepting merchant appears on that vendor ledger and is redeemable', async () => {
  await seed(); const r = await api(base, input(100, { components: [{ mode: 'voucher', amount: 100, issuer: 'Other Merchant', reference: 'OTH-001' }] }));
  assert.equal(r.success, true, r.error); let s = read(); s.expenses['EX-OTHER'] = { ...expense('EX-OTHER', 60, 0), vendor: 'Other Merchant' }; write(s);
  const vendors = (await api('/api/expenses/vendors?nature=SANKI')).vendors;
  assert.equal(vendors.find(v => v.name === 'Refund Supplier').ledgerClosingBalance, 0);
  assert.equal(vendors.find(v => v.name === 'Other Merchant').ledgerClosingBalance, -40);
  const redeem = await api(base + '/' + r.refund.id + '/redeem', { componentId: 'PART-001', expenseId: 'EX-OTHER', amount: 60, date: '2026-10-08', reason: 'Replacement from voucher merchant', requestId: 'different-merchant-redemption' });
  assert.equal(redeem.success, true, redeem.error); assert.equal(read().receipts.length, 0);
  assert.equal((await api(base)).sources.some(x => x.expenseId === 'EX-OTHER'), true);
});
test('personal refund, recovery and balance-sheet positions use actual dates and current caps', async () => {
  const e = expense(); Object.assign(e, { paidAlready: true, personalPaidAmount: 100, reimbursementAmount: 100, reimbursementStatus: 'reimbursed', reimbursementPayments: [{ id: 'REIM-001', date: '2026-10-02', amount: 100, account }] });
  Object.assign(e.payments[0], { personalFunds: true, account: 'Shivam Cash' }); await seed(e);
  const r = await api(base, input(40, { components: [{ mode: 'cash', receiver: 'payer', account: 'Shivam Cash', amount: 40 }] }));
  assert.equal(r.success, true, r.error); assert.equal(read().receipts.length, 0);
  assert.equal((await api('/api/expenses/balance-sheet?asOf=2026-10-06&nature=SANKI')).assets.employeeRefundRecoveries, 0);
  assert.equal((await api('/api/expenses/balance-sheet?asOf=2026-10-08&nature=SANKI')).assets.employeeRefundRecoveries, 40);
  const recovered = await api(base + '/recoveries', { expenseId: e.id, amount: 30, date: '2026-10-08', account, mode: 'bank', requestId: 'recovery-current-0001', reason: 'Employee returned reimbursement' });
  assert.equal(recovered.success, true, recovered.error);
  const backdated = await api(base + '/recoveries', { expenseId: e.id, amount: 20, date: '2026-10-07', account, mode: 'bank', requestId: 'recovery-backdated-0002', reason: 'Backdated employee return' });
  assert.equal(backdated.status, 409); assert.equal(read().receipts.length, 1);
});
test('split voucher wallets have independent redemption identities and undo correctly', async () => {
  await seed(); const r = await api(base, input(100, { components: [{ mode: 'voucher', amount: 40, issuer: 'Refund Supplier', reference: 'SPLIT-V-1' }, { mode: 'voucher', amount: 60, issuer: 'Refund Supplier', reference: 'SPLIT-V-2' }] }));
  assert.equal(r.success, true, r.error); let s = read(); s.expenses['EX-A'] = expense('EX-A', 20, 0); s.expenses['EX-B'] = expense('EX-B', 20, 0); write(s);
  for (const [part, expenseId] of [['PART-001','EX-A'],['PART-002','EX-B']]) {
    const result = await api(base + '/' + r.refund.id + '/redeem', { componentId: part, expenseId, amount: 20, date: '2026-10-08', requestId: 'split-redemption-' + part, reason: 'Apply split credit' });
    assert.equal(result.success, true, result.error);
  }
  assert.equal((await api('/api/expenses/EX-B', { reason: 'Delete redeemed bill' }, { method: 'DELETE' })).status, 409);
  assert.equal((await api('/api/expenses/EX-B', { vendor: 'Wrong Merchant', editReason: 'Change merchant' })).status, 409);
  s = read(); const first = s.vendorAdvances[0].applications[0].id, second = s.vendorAdvances[1].applications[0].id;
  assert.notEqual(first, second);
  assert.equal((await api(base + '/' + r.refund.id + '/redemptions/' + second + '/void', { reason: 'Reverse second credit only' })).success, true);
  s = read(); assert.equal(s.expenses['EX-A'].paidAmount, 20); assert.equal(s.expenses['EX-B'].paidAmount, 0);
  assert.equal(s.vendorAdvances[0].remainingAmount, 20); assert.equal(s.vendorAdvances[1].remainingAmount, 60);
});
test('vendor rename preserves voucher issuer and unused credit merchant cannot be deleted', async () => {
  await seed(); const r = await api(base, input(100, { components: [{ mode: 'voucher', amount: 100, issuer: 'Voucher Merchant', reference: 'VM-REF' }] }));
  assert.equal(r.success, true, r.error);
  assert.equal((await api('/api/expenses/vendors/manage/delete', { nature: 'SANKI', name: 'Voucher Merchant', reason: 'Remove unused merchant' })).status, 409);
  const renamed = await api('/api/expenses/vendors/manage/edit', { nature: 'SANKI', name: 'Voucher Merchant', newName: 'Final Voucher Merchant' });
  assert.equal(renamed.success, true, renamed.error);
  const s = read(); assert.equal(s.vendorAdvances[0].issuer, 'Final Voucher Merchant'); assert.equal(s.vendorAdvances[0].vendor, 'Final Voucher Merchant');
  assert.equal(s.expenseRefunds[0].components[0].issuer, 'Final Voucher Merchant');
  assert.equal(s.expenseRefunds[0].components[0].originalIssuer, 'Voucher Merchant');
});

test('split coupon payments preserve cash and credit-card accounting, not just UPI', async () => {
  for (const type of ['Cash', 'Credit']) {
    const body = await seedPaymentCoupon();
    body.paymentType = type; body.account = type === 'Cash' ? 'Counter Cash' : 'Test Card 1234';
    if (type === 'Credit') {
      body.creditCardId = 'CC-1';
      fs.writeFileSync(path.join(temp, 'credit-cards.json'), JSON.stringify({ cards: { 'CC-1': { id: 'CC-1', name: 'Test Card', last4: '1234', openingOutstanding: 0, active: true } }, statements: {}, payments: [], audit: [] }));
    }
    const paid = await api('/api/expenses/vendor-payments/batch', body); assert.equal(paid.success, true, paid.error);
    const bill = read().expenses['EX-NEXT']; assert.equal(bill.paidAmount, 2000); assert.equal(bill.payments[0].amount, 488);
    assert.equal(bill.payments[0].account, body.account); assert.equal(read().vendorAdvances[0].remainingAmount, 0);
    assert.equal(read().receipts.length, 0);
    if (type === 'Credit') {
      assert.equal(bill.payments[0].creditCardId, 'CC-1');
      assert.equal((await api('/api/expenses/credit-cards')).cards[0].outstanding, 488);
      assert.equal((await api('/api/expenses/vendor-payments/batch', { ...body, creditCardId: 'CC-2' })).status, 409);
    } else {
      const ledger = await api('/api/expenses/account-ledger?nature=SANKI&account=' + encodeURIComponent(body.account));
      assert.equal((ledger.entries || ledger.rows).find(e => e.id === 'EX-NEXT/PAY-001').debit, 488);
    }
    assert.equal((await api('/api/expenses/vendor-payments/batch', body)).already, true);
    assert.equal(read().expenses['EX-NEXT'].payments.length, 1);
  }
});

test('a genuine bank reconciliation warning cannot partly redeem a split coupon payment', async () => {
  const body = await seedPaymentCoupon(), s = read();
  s.transfers = [{ id: 'TR-WARNING', nature: 'SANKI', date: '2026-10-08', fromAccount: account, toAccount: 'Axis Bank 3448', amount: 10 }]; write(s);
  const before = read(), failed = await api('/api/expenses/vendor-payments/batch', body);
  assert.equal(failed.status, 409); assert.equal(failed.requiresOverride, true);
  const after = read(); assert.deepEqual(after.vendorAdvances, before.vendorAdvances); assert.deepEqual(after.expenses, before.expenses);
  assert.deepEqual(after.vendorPaymentRequests, before.vendorPaymentRequests); assert.deepEqual(after.auditLog, before.auditLog);
  // Credit-only settlement changes no bank/cash movement, leaving the warning
  // intact rather than suppressing it or adding an override.
  const creditOnly = await api('/api/expenses/vendor-payments/batch', { ...body, amount: 0, account: '', paymentProof: '' });
  assert.equal(creditOnly.success, true, creditOnly.error); assert.equal(read().expenses['EX-NEXT'].paidAmount, 1512);
  assert.equal(read().expenses['EX-NEXT'].payments.length, 0); assert.deepEqual(read().transfers, before.transfers);
});

test('normal money-only payment retries are idempotent without a coupon', async () => {
  await seed(expense('EX-NEXT', 100, 0));
  const body = { expenseIds: ['EX-NEXT'], amount: 100, account, date: '2026-10-08', paymentType: 'UPI', paymentProof: '/api/expenses/photo/money-only.jpg', requestId: 'normal-money-payment-0001' };
  const paid = await api('/api/expenses/vendor-payments/batch', body); assert.equal(paid.success, true, paid.error);
  assert.equal((await api('/api/expenses/vendor-payments/batch', body)).already, true);
  assert.equal(read().expenses['EX-NEXT'].payments.length, 1); assert.equal(read().expenses['EX-NEXT'].paidAmount, 100);
  assert.equal((await api('/api/expenses/vendor-payments/batch', { ...body, amount: 101 })).status, 409);
});

'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { expenseFinancialLock } = require('../modules/expense-financial-lock');

const payment = changes => ({ id: 'PAY-001', amount: 100, account: 'Axis Bank 3448', date: '2026-09-05', ...changes });
const expense = changes => ({ id: 'EX-00100', nature: 'SANKI', amount: 100, date: '2026-09-04', status: 'paid', paidAmount: 100,
  approvedAt: '2026-09-04T12:00:00Z', payments: [payment()], ...changes });
const ref = 'EX-00100/PAY-001';
const finalized = row => ({ bankStatements: { BANK: { imports: [{ reconciliationRows: [row] }] } } });
function freeze(value) { if (value && typeof value === 'object') { Object.values(value).forEach(freeze); Object.freeze(value); } return value; }

test('unmatched paid, approved and unpaid expenses are not locked just by status or bank date range', () => {
  for (const status of ['paid', 'partially_paid', 'approved', 'pending']) {
    assert.deepEqual(expenseFinancialLock({ bankStatements: { 'Axis Bank 3448': { reconciledThrough: '2026-10-01', imports: [] } } }, expense({ status })), { locked: false, reason: '', references: [] });
  }
  assert.deepEqual(expenseFinancialLock({}, expense({ status: 'approved', payments: [], paidAmount: 0 })), { locked: false, reason: '', references: [] });
});

test('direct finalized linkedRecordIds freeze amount/date', () => {
  const result = expenseFinancialLock(finalized({ linkedRecordIds: [ref] }), expense());
  assert.equal(result.locked, true); assert.match(result.reason, /finalized reconciliation/); assert.deepEqual(result.references, [ref]);
});

test('finalized ledger snapshots and carried reconciliation links freeze payments', () => {
  for (const field of ['reconciliationRows', 'carriedReconciliationRows']) {
    const store = { bankStatements: { BANK: { imports: [{ [field]: [{ ledger: { id: ref } }] }] } } };
    assert.equal(expenseFinancialLock(store, expense()).locked, true);
  }
});

test('carried-forward and aggregate snapshot source IDs remain protected', () => {
  for (const record of [
    { carryForwardMovements: [{ id: 'AGGREGATE', sourceIds: [ref] }] },
    { reconciliationRows: [{ ledger: { id: 'AGGREGATE', linkedEntries: [{ id: ref }] } }] },
    { reconciliationRows: [{ ledger: { id: 'AGGREGATE', linkedEntryIds: [ref] } }] }
  ]) assert.equal(expenseFinancialLock({ bankStatements: { BANK: { imports: [record] } } }, expense()).locked, true);
});

test('all supported active decision forms protect their actual payment references', () => {
  for (const action of ['accept_match', 'link_existing', 'split_allocation', 'link_multiple_bank_entries', 'create_expense']) {
    assert.equal(expenseFinancialLock({ bankReconciliationDrafts: { D: { resolutions: { 'bank-0': { action, appId: ref } } } } }, expense()).locked, true, action);
  }
  for (const action of ['link_multiple_existing', 'link_with_rounding', 'vendor_advance_split']) {
    assert.equal(expenseFinancialLock({ bankReconciliationDrafts: { D: { resolutions: { 'bank-0': { action, appIds: ['OTHER', ref] } } } } }, expense()).locked, true, action);
  }
});

test('bare local PAY-001 references never freeze a different expense', () => {
  assert.equal(expenseFinancialLock(finalized({ linkedRecordIds: ['PAY-001', 'EX-OTHER/PAY-001'] }), expense()).locked, false);
});

test('legacy whole-expense bank IDs freeze a real payment row without inventing local payment IDs', () => {
  const e = expense();
  for (const store of [finalized({ linkedRecordIds: [e.id] }), { bankDateOverrides: { [e.id]: { bankDate: '2026-09-05' } } }]) {
    const result = expenseFinancialLock(store, e);
    assert.equal(result.locked, true); assert.deepEqual(result.references, [e.id]);
  }
  assert.equal(expenseFinancialLock(finalized({ linkedRecordIds: [e.id] }), expense({ payments: [], paidAmount: 0 })).locked, false);
  assert.equal(expenseFinancialLock(finalized({ linkedRecordIds: [e.id] }), expense({ payments: [payment({ creditOnly: true })] })).locked, false);
});

test('caller-supplied blocked references augment evidence without mutation', () => {
  const refs = new Set([ref]);
  const before = [...refs];
  assert.equal(expenseFinancialLock({}, expense(), { blockedReferences: refs }).locked, true);
  assert.deepEqual([...refs], before);
});

test('batchPaymentId can be the reconciled ID', () => {
  const e = expense({ payments: [payment({ batchPaymentId: 'BPAY-1' })] });
  assert.deepEqual(expenseFinancialLock(finalized({ linkedRecordIds: ['BPAY-1'] }), e).references, ['BPAY-1']);
});

test('explicit array of batchPaymentIds is supported', () => {
  const e = expense({ payments: [payment({ batchPaymentIds: ['BPAY-1', 'BPAY-2'] })] });
  assert.equal(expenseFinancialLock(finalized({ linkedRecordIds: ['BPAY-2'] }), e).locked, true);
});

test('split batch sibling reference freezes every monetary allocation', () => {
  const e = expense({ payments: [payment({ batchPaymentId: 'BPAY-SHARED' })] });
  const sibling = expense({ id: 'EX-OTHER', payments: [payment({ batchPaymentId: 'BPAY-SHARED' })] });
  const store = { ...finalized({ linkedRecordIds: ['EX-OTHER/PAY-001'] }), expenses: { [e.id]: e, [sibling.id]: sibling } };
  assert.equal(expenseFinancialLock(store, e).locked, true);
  assert.equal(expenseFinancialLock(store, expense({ payments: [payment({ batchPaymentId: 'BPAY-UNRELATED' })] })).locked, false);
});

test('gross vendor payment advance reference freezes another split allocation', () => {
  const e = expense({ payments: [payment({ batchPaymentId: 'BPAY-GROSS' })] });
  const store = { ...finalized({ linkedRecordIds: ['EX-FIRST/PAY-001'] }), vendorAdvances: [{ id: 'ADV-1', amount: 200, grossPaymentAmount: 500,
    batchPaymentId: 'BPAY-GROSS', paymentReference: 'EX-FIRST/PAY-001', account: 'Axis Bank 3448', date: '2026-09-05' }] };
  assert.equal(expenseFinancialLock(store, e).locked, true);
});

test('grouped bank links protect payment and stale active grouped markers do not', () => {
  const group = { type: 'multiple_bank_entries', reconciliationDraft: 'D', bankTransactions: [{ date: '2026-09-05', debit: 50 }, { date: '2026-09-06', debit: 50 }] };
  assert.equal(expenseFinancialLock({ bankReconciliationLinks: { [ref]: group } }, expense()).locked, true);
  const store = { bankReconciliationLinks: { [ref]: { ...group, provisional: true } }, bankReconciliationDrafts: { D: { resolutions: {} } } };
  assert.equal(expenseFinancialLock(store, expense()).locked, false);
  store.bankReconciliationDrafts.D.resolutions['bank-0'] = { action: 'link_multiple_bank_entries', appId: ref };
  assert.equal(expenseFinancialLock(store, expense()).locked, true);
});

test('legacy/finalized date overrides freeze payment even if old draft is gone', () => {
  for (const override of [{ bankDate: '2026-09-05' }, { bankDate: '2026-09-05', reconciliationDraft: 'DELETED-DRAFT' }]) {
    assert.equal(expenseFinancialLock({ bankDateOverrides: { [ref]: override } }, expense()).locked, true);
  }
});

test('orphan date override in an active draft does not create a phantom lock', () => {
  const store = { bankDateOverrides: { [ref]: { bankDate: '2026-09-05', reconciliationDraft: 'D', provisional: true } }, bankReconciliationDrafts: { D: { resolutions: {} } } };
  assert.equal(expenseFinancialLock(store, expense()).locked, false);
  store.bankReconciliationDrafts.D.resolutions['bank-0'] = { action: 'accept_match', appId: ref };
  assert.equal(expenseFinancialLock(store, expense()).locked, true);
});

test('explicit payment origin and card statement IDs are protected', () => {
  for (const changes of [{ bankReconciliationDraft: 'D' }, { creditCardStatementId: 'CCS-1' }]) {
    const result = expenseFinancialLock({}, expense({ payments: [payment(changes)] }));
    assert.equal(result.locked, true); assert.deepEqual(result.references, [ref]);
  }
});

test('bank-created legacy expense source is protected', () => {
  assert.equal(expenseFinancialLock({}, expense({ payments: [], reconciliationSource: { draftId: 'D', rowId: 'bank-0' } })).locked, true);
});

test('legacy statement-linked monetary row without a local payment ID remains locked', () => {
  const result = expenseFinancialLock({}, expense({ payments: [payment({ id: '', bankReconciliationDraft: 'OLD-DRAFT' })] }));
  assert.equal(result.locked, true); assert.deepEqual(result.references, ['EX-00100']);
});

test('reconciled personal-funds payments are real monetary movements', () => {
  assert.equal(expenseFinancialLock(finalized({ linkedRecordIds: [ref] }), expense({ paidAlready: true, payments: [payment({ personalFunds: true })] })).locked, true);
});

test('reimbursement and reimbursement batch evidence freeze related expense', () => {
  const reimbursement = { id: 'REIM-001', amount: 100, account: 'Counter Cash', date: '2026-09-08', batchId: 'RB-1' };
  const e = expense({ payments: [], reimbursementPayments: [reimbursement] });
  for (const id of ['EX-00100/REIM-001', 'EX-00100/REIM-001/RECEIVED', 'RB-1']) {
    assert.equal(expenseFinancialLock(finalized({ linkedRecordIds: [id] }), e).locked, true, id);
  }
});

test('reimbursement sibling batch evidence freezes both constituent expenses', () => {
  const one = expense({ payments: [], reimbursementPayments: [{ id: 'REIM-001', batchId: 'RB-1', amount: 40, account: 'Bank', date: '2026-09-08' }] });
  const two = expense({ id: 'EX-OTHER', payments: [], reimbursementPayments: [{ id: 'REIM-001', batchId: 'RB-1', amount: 60, account: 'Bank', date: '2026-09-08' }] });
  const store = { ...finalized({ linkedRecordIds: ['EX-OTHER/REIM-001'] }), expenses: { one, two } };
  assert.equal(expenseFinancialLock(store, one).locked, true);
});

test('historical excluded reimbursement and credit-only payment do not create fake monetary locks', () => {
  for (const excluded of [{ accountingExcluded: true }, { historicalSettlement: true }, { creditOnly: true }, { refundCredit: true }, { amount: 0 }, { amount: -100 }, { amount: 'bad' }, { account: 'Refund credit' }]) {
    const e = expense({ payments: [payment(excluded)], reimbursementPayments: [{ ...payment({ id: 'REIM-001' }), ...excluded }] });
    const store = { ...finalized({ linkedRecordIds: [ref, 'EX-00100/REIM-001'] }), cashReconciliations: [{ status: 'approved', account: 'Counter Cash', verifiedThrough: '2026-10-01' }] };
    assert.equal(expenseFinancialLock(store, e).locked, false, JSON.stringify(excluded));
  }
});

test('approved cash through payment date locks financial fields regardless of expense entity', () => {
  const e = expense({ nature: 'SAMAST', payments: [payment({ account: 'Counter Cash' })] });
  const store = { cashReconciliations: [{ id: 'CCR-1', status: 'approved', account: 'Counter Cash', nature: 'SANKI', verifiedThrough: '2026-09-05' }] };
  const result = expenseFinancialLock(store, e);
  assert.equal(result.locked, true); assert.match(result.reason, /Owner-approved cash period through 2026-09-05/); assert.deepEqual(result.references, [ref]);
});

test('cash period uses actual payment date, not earlier expense date', () => {
  const e = expense({ date: '2026-09-01', payments: [payment({ account: 'Counter Cash', date: '2026-09-06' })] });
  const store = { cashReconciliations: [{ status: 'approved', account: 'Counter Cash', verifiedThrough: '2026-09-05' }] };
  assert.equal(expenseFinancialLock(store, e).locked, false);
});

test('pending/rejected and other-account cash approvals do not lock', () => {
  const e = expense({ payments: [payment({ account: 'Counter Cash' })] });
  for (const record of [{ status: 'pending', account: 'Counter Cash' }, { status: 'rejected', account: 'Counter Cash' }, { status: 'approved', account: 'Prashant Cash' }]) {
    assert.equal(expenseFinancialLock({ cashReconciliations: [{ ...record, verifiedThrough: '2026-10-01' }] }, e).locked, false);
  }
});

test('cash reimbursements are protected even when personally funded source is not reconciled', () => {
  const e = expense({ payments: [payment({ account: 'Employee personal bank', personalFunds: true })], reimbursementPayments: [{ id: 'REIM-001', amount: 100, account: 'Counter Cash', date: '2026-09-08' }] });
  assert.equal(expenseFinancialLock({ cashReconciliations: [{ status: 'approved', account: 'Counter Cash', verifiedThrough: '2026-09-10' }] }, e).locked, true);
});

test('cash-funded vendor advance applications follow real advance payment evidence', () => {
  const e = expense({ payments: [], vendorAdvanceApplications: [{ vendorAdvanceId: 'ADV-1', amount: 100, date: '2026-09-08' }] });
  const advance = { id: 'ADV-1', amount: 200, account: 'Counter Cash', date: '2026-09-05' };
  assert.equal(expenseFinancialLock({ vendorAdvances: [advance], ...finalized({ linkedRecordIds: ['ADV-1'] }) }, e).locked, true);
  assert.equal(expenseFinancialLock({ vendorAdvances: [advance], cashReconciliations: [{ status: 'approved', account: 'Counter Cash', verifiedThrough: '2026-09-06' }] }, e).locked, true);
});

test('refund credit redemption neither inherits original bank link nor pretends to be legacy cash', () => {
  const e = expense({ account: 'Counter Cash', payments: [], vendorAdvanceApplications: [{ vendorAdvanceId: 'ADV-CREDIT', amount: 100, date: '2026-09-05', refundCredit: true }] });
  const store = { ...finalized({ linkedRecordIds: ['ADV-CREDIT'] }), vendorAdvances: [{ id: 'ADV-CREDIT', amount: 100, creditOnly: true, account: 'Refund credit', date: '2026-09-05' }], cashReconciliations: [{ status: 'approved', account: 'Counter Cash', verifiedThrough: '2026-10-01' }] };
  assert.equal(expenseFinancialLock(store, e).locked, false);
});

test('legacy monetary expense needs linked evidence or approved cash, not paid status alone', () => {
  const e = expense({ payments: [], account: 'Counter Cash', paidAt: '2026-09-05T10:00:00Z' });
  assert.equal(expenseFinancialLock({}, e).locked, false);
  assert.equal(expenseFinancialLock(finalized({ linkedRecordIds: [e.id] }), e).locked, true);
  assert.equal(expenseFinancialLock({ cashReconciliations: [{ status: 'approved', account: 'Counter Cash', verifiedThrough: '2026-09-06' }] }, e).locked, true);
  assert.equal(expenseFinancialLock({}, { ...e, paidAmount: 0 }).locked, false);
});

test('any reconciled installment locks the expense amount and date; references are unique and stable', () => {
  const e = expense({ payments: [payment(), payment({ id: 'PAY-002', amount: 50 })] });
  const store = { ...finalized({ linkedRecordIds: [ref, ref] }), bankDateOverrides: { [ref]: { bankDate: '2026-09-05' } } };
  assert.deepEqual(expenseFinancialLock(store, e).references, [ref]);
  assert.equal(expenseFinancialLock(store, e, { role: 'owner' }).locked, true);
});

test('predicate accepts frozen inputs and cannot mutate store, expense or supplied reference set', () => {
  const e = freeze(expense());
  const store = freeze({ ...finalized({ linkedRecordIds: [ref] }), expenses: { [e.id]: e } });
  const before = JSON.stringify(store), expenseBefore = JSON.stringify(e), refs = new Set([ref]);
  assert.equal(expenseFinancialLock(store, e, { blockedReferences: refs }).locked, true);
  assert.equal(JSON.stringify(store), before); assert.equal(JSON.stringify(e), expenseBefore); assert.deepEqual([...refs], [ref]);
  assert.deepEqual(expenseFinancialLock(undefined, undefined), { locked: false, reason: '', references: [] });
});

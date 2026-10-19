'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const authority = require('../modules/bank-amount-authority');
const createPolicy = require('../modules/bank-amount-authority-policy');
const account = 'Axis Bank 3448', nature = 'SANKI';
function fixture({ amount = 500, bankAmount = 499.50, direction = 'debit', type = 'expense_payment', mode = 'auto' } = {}) {
  const bank = { date: '2026-10-19', reference: 'VERIFIED-TRANSACTION-123', description: 'Verified transaction', debit: direction === 'debit' ? bankAmount : 0, credit: direction === 'credit' ? bankAmount : 0, balance: 1000 };
  const movement = { id: type === 'expense_payment' ? 'EX-TEST/PAY-001' : 'SOURCE-TEST', date: '2026-10-18', debit: direction === 'debit' ? amount : 0, credit: direction === 'credit' ? amount : 0 };
  const store = { expenses: {}, transfers: [], receipts: [], adjustments: [], bankStatements: {}, bankDateOverrides: {}, bankReconciliationDrafts: {} };
  const draft = { id: 'BRD-TEST', account, nature, transactions: [bank], resolutions: {} }; store.bankReconciliationDrafts[draft.id] = draft;
  if (type === 'expense_payment') store.expenses['EX-TEST'] = { id: 'EX-TEST', date: '2026-10-01', nature, amount, requestedAmount: amount, paidAmount: amount, status: 'paid', approvedAt: '2026-10-01T00:00:00Z', payments: [{ id: 'PAY-001', amount, account, date: movement.date, proof: '/payment-proof.jpg' }] };
  if (type === 'receipt') store.receipts.push({ id: movement.id, nature, account, amount, date: movement.date, source: 'Ordinary customer receipt' });
  if (type === 'adjustment') store.adjustments.push({ id: movement.id, nature, account, amount: direction === 'debit' ? -amount : amount, date: movement.date, note: 'Ordinary manual movement' });
  if (type === 'transfer') store.transfers.push({ id: movement.id, nature, fromAccount: direction === 'debit' ? account : 'Other Bank', toAccount: direction === 'credit' ? account : 'Other Bank', amount, date: movement.date });
  const options = { mode, bankRowId: 'bank-0' };
  return { store, draft, bank, movement, options, make() { return authority.plan(store, draft, bank, movement, options); } };
}
function unchanged(f, callback) { const before = JSON.stringify(f.store); assert.throws(callback); assert.equal(JSON.stringify(f.store), before); }

test('planning is immutable and the ₹500 → ₹499.50 correction updates payment and bill only on apply', () => {
  const f = fixture(), before = JSON.stringify(f.store), plan = f.make();
  assert.equal(JSON.stringify(f.store), before); assert.ok(Object.isFrozen(plan)); assert.ok(Object.isFrozen(plan.before));
  assert.equal(plan.amountDifference, -.5); assert.equal(plan.after.expense.amount, 499.5);
  const result = authority.apply(f.store, JSON.parse(JSON.stringify(plan))), expense = f.store.expenses['EX-TEST'];
  assert.equal(result.applied, 1); assert.equal(expense.amount, 499.5); assert.equal(expense.requestedAmount, 499.5); assert.equal(expense.paidAmount, 499.5); assert.equal(expense.payments[0].amount, 499.5);
  assert.equal(expense.status, 'paid'); assert.equal(expense.date, '2026-10-01'); assert.equal(expense.payments[0].date, f.bank.date); assert.equal(expense.payments[0].proof, '/payment-proof.jpg');
  assert.equal(result.audits[0].before.amount, 500); assert.equal(result.audits[0].before.date, '2026-10-18'); assert.equal(result.audits[0].before.expense.amount, 500);
  assert.equal(result.audits[0].after.expense.amount, 499.5); assert.equal(result.audits[0].bankReference, f.bank.reference);
  assert.equal(expense.bankAmountAuthority.originalAmount, 500); assert.equal(expense.bankAmountAuthority.amount, 499.5);
  assert.equal(expense.payments[0].bankAmountAuthority.planId, plan.id); assert.equal(expense.payments[0].bankAmountAuthority.amount, 499.5);
});

for (const difference of [-1, -.99, -.01, 0, .01, .99, 1]) test('automatic inclusive paise threshold allows ' + difference, () => {
  const f = fixture({ bankAmount: 500 + difference }); assert.equal(f.make().amountDifference, difference);
});
for (const difference of [-1.01, 1.01, 10, -10]) test('automatic threshold rejects ' + difference + ' without writes', () => {
  const f = fixture({ bankAmount: 500 + difference }); unchanged(f, () => f.make());
});
test('manual matching has no amount-difference cap but still changes both bill and payment', () => {
  const f = fixture({ bankAmount: 15000, mode: 'manual' }), plan = f.make(); authority.apply(f.store, plan);
  assert.equal(f.store.expenses['EX-TEST'].amount, 15000); assert.equal(f.store.expenses['EX-TEST'].paidAmount, 15000);
});
for (const type of ['receipt', 'adjustment', 'transfer']) for (const direction of ['debit', 'credit']) {
  if (type === 'receipt' && direction === 'debit') continue;
  test(type + ' keeps correct ' + direction + ' sign with bank amount/date', () => {
    const f = fixture({ type, direction, bankAmount: 500.75 }), plan = f.make(); authority.apply(f.store, plan);
    const record = f.store[type === 'receipt' ? 'receipts' : type === 'adjustment' ? 'adjustments' : 'transfers'][0];
    assert.equal(record.amount, type === 'adjustment' && direction === 'debit' ? -500.75 : 500.75); assert.equal(record.date, f.bank.date);
  });
}
test('partial payments preserve unpaid amount and independent requestedAmount', () => {
  const f = fixture(), e = f.store.expenses['EX-TEST']; e.amount = 1000; e.requestedAmount = 1200; e.status = 'partially_paid';
  const plan = f.make(); authority.apply(f.store, plan); const after = f.store.expenses[e.id];
  assert.equal(after.amount, 999.5); assert.equal(after.paidAmount, 499.5); assert.equal(after.amount - after.paidAmount, 500); assert.equal(after.status, 'partially_paid'); assert.equal(after.requestedAmount, 1200);
});
test('multiple payment plans on one expense aggregate deltas once and retain its unpaid balance', () => {
  const f = fixture(), e = f.store.expenses['EX-TEST']; e.amount = 1000; e.requestedAmount = 1000; e.paidAmount = 700; e.status = 'partially_paid';
  e.payments.push({ id: 'PAY-002', amount: 200, account, date: '2026-10-17' });
  const secondBank = { ...f.bank, reference: 'SECOND-REFERENCE', debit: 201 }; f.draft.transactions.push(secondBank);
  const plans = [f.make(), authority.plan(f.store, f.draft, secondBank, { id: e.id + '/PAY-002', debit: 200, credit: 0 }, { mode: 'auto', bankRowId: 'bank-1' })];
  const result = authority.applyAll(f.store, plans), after = f.store.expenses[e.id];
  assert.equal(result.applied, 2); assert.equal(after.amount, 1000.5); assert.equal(after.paidAmount, 700.5); assert.equal(after.requestedAmount, 1000.5); assert.equal(after.amount - after.paidAmount, 300);
  assert.equal(result.audits[0].after.expense.amount, 1000.5); assert.equal(result.audits[1].after.expense.amount, 1000.5);
  assert.equal(after.bankAmountAuthority.originalAmount, 1000); assert.deepEqual(after.bankAmountAuthority.planIds, plans.map(plan => plan.id));
  assert.equal(after.payments[0].bankAmountAuthority.amount, 499.5); assert.equal(after.payments[1].bankAmountAuthority.amount, 201);
});
test('applyAll validates all plans before changing any source', () => {
  const first = fixture(), second = fixture({ type: 'receipt', direction: 'credit' });
  first.store.receipts = second.store.receipts; first.draft.transactions.push(second.bank);
  const plans = [first.make(), authority.plan(first.store, first.draft, second.bank, second.movement, { mode: 'auto', bankRowId: 'bank-1' })];
  first.store.receipts[0].amount = 123; unchanged(first, () => authority.applyAll(first.store, plans));
});
test('replay is a no-op even after finalization removes the draft; duplicate plans in one apply are rejected', () => {
  const f = fixture(), plan = f.make(); unchanged(f, () => authority.applyAll(f.store, [plan, plan]));
  authority.apply(f.store, plan); delete f.store.bankReconciliationDrafts[f.draft.id]; const before = JSON.stringify(f.store), replay = authority.apply(f.store, plan);
  assert.equal(replay.applied, 0); assert.equal(replay.alreadyApplied, 1); assert.equal(JSON.stringify(f.store), before);
});
for (const [name, mutate] of Object.entries({
  'bank amount': f => { f.bank.debit = 499.25; }, 'bank date': f => { f.bank.date = '2026-10-18'; }, 'bank reference': f => { f.bank.reference = 'DIFFERENT'; },
  'source amount': f => { f.store.expenses['EX-TEST'].payments[0].amount = 499; }, 'source proof': f => { f.store.expenses['EX-TEST'].payments[0].proof = '/changed.jpg'; },
  'source bill': f => { f.store.expenses['EX-TEST'].amount = 600; }, 'removed draft': f => { delete f.store.bankReconciliationDrafts[f.draft.id]; }
})) test('stale ' + name + ' blocks apply without any writes', () => {
  const f = fixture(), plan = f.make(); mutate(f); unchanged(f, () => authority.apply(f.store, plan));
});
test('tampered plans cannot change amounts or relax automatic mode', () => {
  for (const mutate of [p => { p.after.amount = 1; }, p => { p.mode = 'manual'; }, p => { p.bankAmount = 1; }]) {
    const f = fixture(), plan = JSON.parse(JSON.stringify(f.make())); mutate(plan); unchanged(f, () => authority.apply(f.store, plan));
  }
});
for (const [name, mutate] of Object.entries({
  personal: f => { f.store.expenses['EX-TEST'].payments[0].personalFunds = true; }, batch: f => { f.store.expenses['EX-TEST'].payments[0].batchPaymentId = 'BATCH'; },
  reimbursement: f => { f.store.expenses['EX-TEST'].reimbursementPayments = [{ id: 'REIM' }]; }, advance: f => { f.store.expenses['EX-TEST'].vendorAdvanceApplications = [{ amount: 1 }]; },
  refund: f => { f.store.expenseRefunds = [{ id: 'REF', status: 'pending', sources: [{ expenseId: 'EX-TEST' }] }]; },
  'protected callback': f => { f.options.isProtected = () => true; }, 'locked callback': f => { f.options.isLocked = () => ({ locked: true }); },
  'finalized own payment': f => { f.store.bankStatements.OTHER = { imports: [{ reconciliationRows: [{ linkedRecordIds: [f.movement.id] }] }] }; },
  'finalized sibling': f => { f.store.bankStatements.OTHER = { imports: [{ reconciliationRows: [{ ledger: { id: 'EX-TEST/PAY-002' } }] }] }; f.store.expenses['EX-TEST'].payments.push({ id: 'PAY-002', amount: 1, account }); },
  'foreign active claim': f => { f.store.bankReconciliationDrafts.OTHER = { id: 'OTHER', resolutions: { 'bank-0': { appId: f.movement.id } } }; },
  'finalized override': f => { f.store.bankDateOverrides[f.movement.id] = { bankDate: f.bank.date, reconciliationDraft: 'FINALIZED' }; },
  'reversal metadata': f => { f.bank.reversal = true; }, 'source mismatch': f => { f.movement.debit = 499.75; },
  'opposite direction': f => { f.bank.credit = f.bank.debit; f.bank.debit = 0; }, 'mixed bank sides': f => { f.bank.credit = 1; },
  'invalid bank date': f => { f.bank.date = '2026-02-30'; }, 'unapproved expense': f => { delete f.store.expenses['EX-TEST'].approvedAt; },
  'inconsistent paid amount': f => { f.store.expenses['EX-TEST'].paidAmount = 400; },
  'known bank charge': f => { f.store.expenses['EX-TEST'].ledger = 'BANK CHARGES'; }
})) test('manual mode does not bypass ' + name, () => {
  const f = fixture({ mode: 'manual' }); mutate(f); unchanged(f, () => f.make());
});
test('current draft provisional date and exact one-to-one resolution do not freeze the correction', () => {
  const f = fixture(); f.draft.resolutions['bank-0'] = { action: 'accept_match', appId: f.movement.id }; f.store.bankDateOverrides[f.movement.id] = { bankDate: f.bank.date, reconciliationDraft: f.draft.id, provisional: true };
  assert.doesNotThrow(() => f.make());
});
test('new protections introduced after planning are checked again before apply', () => {
  const f = fixture(), plan = f.make(); f.store.bankDateOverrides[f.movement.id] = { reconciliationDraft: 'NEW-FINALIZATION', bankDate: f.bank.date }; unchanged(f, () => authority.apply(f.store, plan));
  const other = fixture(), otherPlan = other.make(); unchanged(other, () => authority.apply(other.store, otherPlan, { isLocked: () => true }));
});
for (const value of [0, -1, NaN, Infinity, .001, '', null]) test('manual invalid bank amount ' + value + ' is rejected', () => {
  const f = fixture({ bankAmount: value, mode: 'manual' }); unchanged(f, () => f.make());
});
test('same value and valid date remain auditable without creating extra accounting entries', () => {
  const f = fixture({ amount: 500, bankAmount: 500 }), plan = f.make(); authority.apply(f.store, plan);
  assert.equal(f.store.adjustments.length, 0); assert.equal(f.store.receipts.length, 0); assert.equal(f.store.transfers.length, 0); assert.equal(Object.keys(f.store.expenses).length, 1);
});
test('shared physical bank can correct a caller-authorized source without relabelling its business entity', () => {
  const f = fixture(); f.draft.nature = 'SAMAST'; f.store.expenses['EX-TEST'].nature = 'PERSONAL'; const plan = f.make(); authority.apply(f.store, plan);
  assert.equal(f.store.expenses['EX-TEST'].nature, 'PERSONAL'); assert.equal(f.store.expenses['EX-TEST'].amount, 499.5);
});
test('plain records with report, refund, salary or fee provenance cannot bypass protection', () => {
  for (const [type, change] of [['transfer', { settledThroughPaytm: 'SETTLED' }], ['receipt', { receiptType: 'loan_receipt' }], ['receipt', { receiptType: 'customer_refund' }], ['adjustment', { category: 'PAYTM CHARGES' }]]) {
    const f = fixture({ type, direction: type === 'receipt' ? 'credit' : 'debit', mode: 'manual' });
    Object.assign(f.store[type === 'transfer' ? 'transfers' : type === 'receipt' ? 'receipts' : 'adjustments'][0], change); unchanged(f, () => f.make());
  }
});
test('an underlying reconciliation expense adjustment cannot be edited independently', () => {
  const f = fixture({ type: 'adjustment', mode: 'manual' }); f.store.reconciliationExpenses = [{ id: 'BRE', adjustmentId: f.movement.id, amount: 500 }]; unchanged(f, () => f.make());
});
function policyFor(f) {
  return createPolicy(authority, { accountEligible: () => true, reference: value => String(value || '').replace(/[^a-z0-9]/gi, '').toLowerCase(), identity: () => true, movements: () => [f.movement], options: () => ({}) });
}
function privateFixture() {
  const f = fixture({ mode: 'manual' }), expense = f.store.expenses['EX-TEST'];
  f.draft.nature = 'SAMAST'; expense.nature = 'PERSONAL'; expense.amount = 9876.54; expense.requestedAmount = 9876.54; expense.status = 'partially_paid';
  return f;
}
test('private source nature is immutable and follows source into correction audit, metadata and private journal', () => {
  const f = privateFixture(), plan = f.make(); assert.equal(plan.nature, 'SAMAST'); assert.equal(plan.sourceNature, 'PERSONAL');
  assert.equal(plan.before.expense.amount, 9876.54); assert.equal(plan.after.expense.amount, 9876.04);
  const result = authority.apply(f.store, plan);
  assert.equal(result.audits[0].sourceNature, 'PERSONAL'); assert.equal(result.audits[0].before.expense.amount, 9876.54);
  assert.equal(f.store.bankAmountAuthorityApplications[plan.id].sourceNature, 'PERSONAL');
  assert.equal(f.store.expenses['EX-TEST'].bankAmountAuthority.sourceNature, 'PERSONAL');
});
test('private invoice totals never appear in public plan while permitted bank payment and delta remain visible', () => {
  const f = privateFixture(), plan = f.make(), publicPlan = policyFor(f).publicPlan(plan);
  assert.equal(publicPlan.privacyRestricted, true); assert.equal(publicPlan.originalAmount, 500); assert.equal(publicPlan.bankAmount, 499.5); assert.equal(publicPlan.amountDifference, -.5);
  assert.equal(Object.hasOwn(publicPlan, 'expenseAmountBefore'), false); assert.equal(Object.hasOwn(publicPlan, 'expenseAmountAfter'), false);
  assert.doesNotMatch(JSON.stringify(publicPlan), /9876|sourceFingerprint|before|requestedAmount/);
});
test('ordinary business invoice totals retain their existing public correction summary', () => {
  const f = fixture(), publicPlan = policyFor(f).publicPlan(f.make());
  assert.equal(publicPlan.expenseAmountBefore, 500); assert.equal(publicPlan.expenseAmountAfter, 499.5); assert.equal(publicPlan.privacyRestricted, undefined);
});
test('saved decision resolves an opaque pointer privately and public row never contains its full PERSONAL plan', () => {
  const f = privateFixture(), plan = f.make(), pointer = { planId: plan.id };
  f.store.bankAmountAuthorityProposals = { [plan.id]: plan }; f.draft.resolutions['bank-0'] = { action: 'link_existing', appId: f.movement.id, bankAmountAuthority: pointer };
  const view = { rows: [{ id: 'bank-0', status: 'resolved', bank: f.bank, app: f.movement, resolution: f.draft.resolutions['bank-0'] }], reconciliationConflicts: [], ledgerClosing: -500, bankClosing: -499.5 };
  policyFor(f).decorate(f.store, f.draft, view);
  assert.equal(view.bankAmountAuthorityCount, 1); assert.equal(view.reconciliationConflicts.length, 0); assert.equal(view.balanceResolved, true);
  assert.deepEqual(view.rows[0].resolution.bankAmountAuthority, pointer); assert.equal(view.rows[0].bankAmountAuthority.privacyRestricted, true);
  assert.doesNotMatch(JSON.stringify(view), /9876|sourceFingerprint|bankSnapshot|requestedAmount|"before"|"after"/);
  assert.equal(f.store.bankAmountAuthorityProposals[plan.id].before.expense.amount, 9876.54, 'private snapshot remains available for Owner finalization');
});
test('missing private proposal blocks finalization instead of falling back to a public or client-supplied plan', () => {
  const f = privateFixture(), plan = f.make();
  f.draft.resolutions['bank-0'] = { action: 'accept_match', appId: f.movement.id, bankAmountAuthority: { planId: plan.id } };
  const view = { rows: [{ id: 'bank-0', status: 'resolved', bank: f.bank, app: f.movement, resolution: f.draft.resolutions['bank-0'] }], reconciliationConflicts: [], ledgerClosing: -500, bankClosing: -499.5 };
  policyFor(f).decorate(f.store, f.draft, view);
  assert.equal(view.bankAmountAuthorityCount, 0); assert.equal(view.canFinalizeTransactions, false); assert.match(view.rows[0].bankAmountAuthorityBlocked, /private.*proposal.*missing/i);
});
for (const changed of ['draft', 'bank']) test('an already-applied proposal cannot be reused against changed ' + changed + ' evidence', () => {
  const f = privateFixture(), plan = f.make(); authority.apply(f.store, plan);
  f.store.bankAmountAuthorityProposals = { [plan.id]: plan };
  if (changed === 'draft') { const oldId = f.draft.id; f.draft = { ...f.draft, id: 'BRD-DIFFERENT' }; delete f.store.bankReconciliationDrafts[oldId]; f.store.bankReconciliationDrafts[f.draft.id] = f.draft; }
  if (changed === 'bank') f.bank.debit = 399.5;
  f.draft.resolutions['bank-0'] = { action: 'link_existing', appId: f.movement.id, bankAmountAuthority: { planId: plan.id } };
  f.movement.debit = 499.5;
  const view = { rows: [{ id: 'bank-0', status: 'resolved', bank: f.bank, app: f.movement, resolution: f.draft.resolutions['bank-0'] }], reconciliationConflicts: [], ledgerClosing: -499.5, bankClosing: -f.bank.debit };
  const before = JSON.stringify(f.store); policyFor(f).decorate(f.store, f.draft, view);
  assert.equal(view.bankAmountAuthorityCount, 0); assert.equal(view.canFinalizeTransactions, false);
  assert.match(view.rows[0].bankAmountAuthorityBlocked, /different or changed statement evidence/);
  assert.equal(JSON.stringify(f.store), before); assert.doesNotMatch(JSON.stringify(view), /9876|sourceFingerprint|bankSnapshot/);
});

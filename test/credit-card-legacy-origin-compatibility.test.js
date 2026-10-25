'use strict';

// Fictional historical representations only; these tests never read or write a
// production accounting snapshot, bank statement or source document.
const test = require('node:test');
const assert = require('node:assert/strict');
const settlements = require('../modules/credit-card-settlements');
const clone = value => JSON.parse(JSON.stringify(value));
const account = 'ICICI Bank 0993', date = '2098-06-12', amount = 2468.12;
const finalizedAt = '2098-06-14T10:22:33.444Z', by = 'synthetic-owner';
const expenseId = 'EX-SYNTHETIC-ORIGIN', paymentId = 'PAY-SYNTHETIC-ORIGIN', movementId = expenseId + '/' + paymentId;
const access = (nature, name) => nature === 'PERSONAL' && name === account;
function fixture() {
  const card = { id: 'CC-SYNTHETIC-ORIGIN', name: 'Synthetic Card', last4: '9009', active: true };
  const row = { id: 'CCR-SYNTHETIC-ORIGIN', date, amount, debit: 0, credit: amount, classification: 'card_payment', narration: 'PAYMENT RECEIVED THANK YOU' };
  const statement = { id: 'CCS-SYNTHETIC-ORIGIN', cardId: card.id, status: 'review', rows: [row] };
  const payment = { id: paymentId, date, amount, account, paymentType: 'UPI', personalFunds: true, proof: '/synthetic-proof-only.jpg' };
  const expense = { id: expenseId, date, nature: 'PERSONAL', amount, paidAmount: amount, personalPaidAmount: amount, account,
    ledger: 'Credit Card', vendor: 'Credit card Payment 9009', particulars: 'Credit card Payment', status: 'paid',
    approvedAt: date + 'T12:00:00.000Z', paidAlready: true, fundedBy: 'claimant', reimbursementStatus: 'not_applicable',
    reimbursementAmount: 0, reimbursementPayments: [], paymentType: 'UPI', payments: [payment] };
  const bank = { transactionId: 'BTX-SYNTHETIC-ORIGIN', date, debit: amount, credit: 0, reference: '', description: 'Synthetic finalized bank debit' };
  const reportRow = { id: 'bank-0', bank, ledger: { id: movementId, date, debit: amount, credit: 0 }, linkedRecordIds: [movementId], decision: 'Matched with ledger entry' };
  const record = { id: 'BST-SYNTHETIC-ORIGIN', from: date, to: '2098-06-14', finalizedAt, finalizedBy: by, reconciliationRows: [reportRow] };
  const override = { originalDate: date, bankDate: date, bankReference: '', reconciliationDraft: 'BRD-SYNTHETIC-ORIGIN', remark: 'Synthetic historical review', by, at: finalizedAt };
  const last = { at: finalizedAt, by, through: record.to, draftId: override.reconciliationDraft, transactionsReconciled: true, balanceReconciled: true };
  const audit = { id: 'AUD-SYNTHETIC-ORIGIN', at: finalizedAt, user: by, action: 'BANK_RECONCILIATION_FINALIZED', subjectType: 'account', subjectId: account, account, nature: 'PERSONAL', after: clone(last) };
  const book = { imports: [record] };
  const store = { cards: { [card.id]: card }, payments: [], statements: { [statement.id]: statement } };
  const exp = { expenses: { [expense.id]: expense }, bankStatements: { ['PERSONAL|' + account]: book }, bankDateOverrides: { [movementId]: override }, auditLog: [audit] };
  return { store, exp, statement, row, payment, expense, bank, reportRow, record, override, audit, book, last };
}
function candidate(f) { return settlements.candidates(f.store, f.exp, f.statement, f.row, access).find(source => source.sourceKind === 'legacy_expense_payment'); }
function input(source) { return { settlementMode: 'link_existing', settlementSourceId: source.id, settlementSourceFingerprint: source.fingerprint, settlementLegacyConfirmed: true, settlementReason: 'Synthetic verified legacy card repayment with retained bank evidence' }; }
function review(f, fields) { return settlements.reviewSettlement(f.store, f.exp, f.statement, f.row, fields || input(candidate(f)), access); }
function blocked(f, reason) {
  const before = clone(f), source = candidate(f);
  assert.ok(source); assert.equal(source.eligible, false, source.blockedReason); assert.match(source.blockedReason, reason);
  assert.ok(review(f).error); assert.deepEqual(f, before, 'failed evidence checks must never repair or rewrite accounting stores');
}

test('a historical import missing draftId may use an exact account-scoped immutable finalization audit', () => {
  const f = fixture(), before = clone(f), source = candidate(f);
  assert.equal(source.eligible, true, source.blockedReason); assert.equal(source.reference, '');
  assert.equal(review(f).error, ''); assert.deepEqual(f, before);
  assert.equal(f.record.draftId, undefined, 'compatibility must not migrate the old import');
});

test('the exact current book finalization may independently prove a missing historical import draftId', () => {
  const f = fixture(); f.exp.auditLog = []; f.book.lastReconciliation = clone(f.last);
  const before = clone(f), source = candidate(f); assert.equal(source.eligible, true, source.blockedReason);
  assert.equal(review(f).error, ''); assert.deepEqual(f, before);
});

test('ordinary imports with explicit draftId retain their existing source fingerprint despite unrelated audit history', () => {
  const f = fixture(); f.record.draftId = f.override.reconciliationDraft; f.exp.auditLog = [];
  const before = candidate(f); assert.equal(before.eligible, true);
  f.exp.auditLog.push({ id: 'UNRELATED', action: 'SYNTHETIC', after: { note: 'Unrelated change' } });
  assert.equal(candidate(f).fingerprint, before.fingerprint);
});

test('date, amount or timestamp proximity alone cannot reconstruct missing source-draft provenance', () => {
  for (const mutate of [
    f => { f.exp.auditLog = []; },
    f => { f.audit.after.at = '2098-06-14T10:22:33.445Z'; },
    f => { f.audit.after.at = '2098-06-14 10:22'; },
    f => { f.audit.after.through = '2098-06-13'; },
    f => { f.audit.after.by = 'different-synthetic-user'; },
    f => { f.audit.user = 'different-synthetic-user'; },
    f => { f.audit.account = 'Different bank 1001'; },
    f => { f.audit.subjectId = 'Different bank 1001'; },
    f => { f.audit.nature = 'SANKI'; },
    f => { f.audit.action = 'BANK_RECONCILIATION_FINALIZATION_REQUESTED'; },
    f => { f.audit.after.transactionsReconciled = false; },
    f => { delete f.audit.id; },
    f => { delete f.record.to; },
    f => { delete f.record.finalizedBy; }
  ]) { const f = fixture(); mutate(f); blocked(f, /provenance|source draft/i); }
});

test('explicit foreign source drafts and contradictory independent evidence remain blocked', () => {
  for (const mutate of [
    f => { f.record.draftId = 'BRD-SYNTHETIC-FOREIGN'; },
    f => { f.audit.after.draftId = 'BRD-SYNTHETIC-FOREIGN'; },
    f => { f.book.lastReconciliation = { ...clone(f.last), draftId: 'BRD-SYNTHETIC-FOREIGN' }; },
    f => { f.exp.auditLog.push(clone(f.audit)); },
    f => { f.book.imports.push({ id: 'BST-SYNTHETIC-DUPLICATE', to: f.record.to, finalizedAt, finalizedBy: by, reconciliationRows: [] }); }
  ]) { const f = fixture(); mutate(f); blocked(f, /source draft|source-draft/i); }
});

test('independent finalization proof cannot replace the exact qualified payment-to-bank-row mapping', () => {
  const f = fixture(); f.reportRow.ledger.id = 'EX-SYNTHETIC-OTHER/PAY-OTHER'; f.reportRow.linkedRecordIds = [f.reportRow.ledger.id];
  blocked(f, /exact, finalized bank reconciliation link/i);
});

test('an independent historical finalization cannot approve a currently provisional bank-date decision', () => {
  const f = fixture(); f.book.lastReconciliation = clone(f.last); f.override.provisional = true;
  blocked(f, /provisional bank-date decision/i);
});

test('date, original-date, transaction identity and reference conflicts have distinct fail-closed reasons', () => {
  for (const [mutate, reason] of [
    [f => { f.override.bankDate = '2098-06-13'; }, /saved bank date/i],
    [f => { f.override.originalDate = '2098-06-11'; }, /original payment date/i],
    [f => { f.override.bankTransactionId = 'BTX-SYNTHETIC-OTHER'; }, /bank-transaction identity/i],
    [f => { f.override.bankReference = 'REF-SYNTHETIC-A'; f.bank.reference = 'REF-SYNTHETIC-B'; }, /reference differs/i]
  ]) { const f = fixture(); mutate(f); blocked(f, reason); }
});

test('the accepted immutable audit provenance is bound to the displayed source fingerprint', () => {
  for (const mutate of [
    f => { f.audit.note = 'Synthetic audit annotation changed'; },
    f => { f.audit.id = 'AUD-SYNTHETIC-REPLACED'; }
  ]) {
    const f = fixture(); f.book.lastReconciliation = clone(f.last);
    const displayed = input(candidate(f)); assert.equal(review(f, displayed).error, '');
    mutate(f); const before = clone(f);
    assert.equal(candidate(f).eligible, true); assert.match(review(f, displayed).error, /source changed/i); assert.deepEqual(f, before);
  }
});

test('a later legitimate book finalization does not stale an unchanged repayment proved by its immutable audit', () => {
  const f = fixture(); f.book.lastReconciliation = clone(f.last);
  const source = candidate(f), displayed = input(source), accepted = review(f, displayed); assert.equal(accepted.error, '');
  Object.assign(f.row, accepted.fields);
  f.book.lastReconciliation = { at: '2098-07-01T09:10:11.222Z', by, through: '2098-06-30', draftId: 'BRD-SYNTHETIC-LATER', transactionsReconciled: true, balanceReconciled: true };
  const before = clone(f), refreshed = candidate(f);
  assert.equal(refreshed.eligible, true); assert.equal(refreshed.fingerprint, source.fingerprint);
  assert.equal(review(f, displayed).error, ''); assert.equal(settlements.validateStatement(f.store, f.exp, f.statement, access), '');
  assert.deepEqual(f, before);
});

test('book-only provenance remains fingerprinted when no immutable audit exists', () => {
  const f = fixture(); f.exp.auditLog = []; f.book.lastReconciliation = clone(f.last);
  const displayed = input(candidate(f)); assert.equal(review(f, displayed).error, '');
  f.book.lastReconciliation.note = 'Synthetic book evidence annotation changed'; const before = clone(f);
  assert.equal(candidate(f).eligible, true); assert.match(review(f, displayed).error, /source changed/i); assert.deepEqual(f, before);
});

test('removing the preferred audit cannot silently replace its saved fingerprint with the fallback book proof', () => {
  const f = fixture(); f.book.lastReconciliation = clone(f.last);
  const displayed = input(candidate(f)); assert.equal(review(f, displayed).error, '');
  f.exp.auditLog = []; const before = clone(f);
  assert.equal(candidate(f).eligible, true); assert.match(review(f, displayed).error, /source changed/i); assert.deepEqual(f, before);
});

test('removing accepted provenance blocks both later review and statement finalization without mutations', () => {
  const f = fixture(), source = candidate(f), saved = review(f, input(source)); assert.equal(saved.error, '');
  Object.assign(f.row, saved.fields); assert.equal(settlements.validateStatement(f.store, f.exp, f.statement, access), '');
  f.exp.auditLog = []; const before = clone(f);
  assert.match(settlements.validateStatement(f.store, f.exp, f.statement, access), /provenance/i); assert.deepEqual(f, before);
});

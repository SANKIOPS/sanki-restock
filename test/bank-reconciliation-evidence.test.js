'use strict';
const test = require('node:test'), assert = require('node:assert/strict');
const { snapshot } = require('../modules/bank-reconciliation-evidence');
const clone = value => JSON.parse(JSON.stringify(value));
const account = 'Synthetic Bank 9099', movementId = 'EX-SYNTHETIC/PAY-ONLY', draftId = 'BRD-SYNTHETIC', rowId = 'bank-0';
function fixture() {
  const bank = { date: '2098-01-02', debit: 123.45, credit: 0, reference: 'FICTIONAL', description: 'Synthetic bank evidence', transactionId: 'BTX-SYNTHETIC' };
  return { bankStatements: { ['PERSONAL|' + account]: { transactions: { TX: { ...bank, id: bank.transactionId, reconciliationDraftId: draftId } }, imports: [{ id: 'BST-SYNTHETIC', finalizedAt: '2098-01-03T00:00:00.000Z', file: '/private/source.pdf', reconciliationRows: [{ id: rowId, bank, ledger: { id: movementId, date: bank.date, debit: bank.debit, credit: 0, privateMemo: 'not projected' }, linkedRecordIds: [movementId] }] }] } }, bankDateOverrides: { [movementId]: { bankDate: bank.date, reconciliationDraft: draftId } }, bankReconciliationDrafts: { [draftId]: { id: draftId, account, nature: 'PERSONAL', transactions: [bank], resolutions: { [rowId]: { action: 'create_internal_transfer', otherAccount: 'Synthetic Other 9098', principalAmount: 123.45 } } } }, transfers: [], auditLog: [{ id: 'AUD-SYNTHETIC', action: 'BANK_RECONCILIATION_FINALIZED', account, nature: 'PERSONAL', ip: 'not projected', after: { draftId, at: '2098-01-03T00:00:00.000Z' } }, { action: 'BANK_RECONCILIATION_FINALIZED', account: 'Other private account', nature: 'PERSONAL', after: { draftId: 'FOREIGN' } }] };
}
function query(patch = {}) { return { account, nature: 'PERSONAL', bookKey: 'PERSONAL|' + account, ...patch }; }
test('exact movement evidence is bounded and does not mutate financial or saved reconciliation state', () => {
  const store = fixture(), before = clone(store), result = snapshot(store, query({ movementId }));
  assert.equal(result.success, true); assert.equal(result.readOnly, true); assert.equal(result.movement.records.length, 1); assert.equal(result.movement.storedTransactions.length, 1); assert.equal(result.movement.finalizationAudit.length, 1);
  assert.equal(result.movement.records[0].file, undefined); assert.equal(result.movement.records[0].rows[0].ledger.privateMemo, undefined); assert.equal(result.movement.finalizationAudit[0].ip, undefined); assert.deepEqual(store, before);
  result.movement.bankDateOverride.bankDate = 'changed'; assert.deepEqual(store, before, 'response never aliases mutable records');
});
test('saved transfer claim is shown separately from posted money and an open row has no invented decision', () => {
  const store = fixture(), before = clone(store), result = snapshot(store, query({ draftId, rowId }));
  assert.equal(result.pending.decision.action, 'create_internal_transfer'); assert.deepEqual(result.pending.postedTransfers, []); assert.deepEqual(store, before);
  delete store.bankReconciliationDrafts[draftId].resolutions[rowId]; assert.equal(snapshot(store, query({ draftId, rowId })).pending.decision, null);
});
test('evidence does not leak foreign movements, accounts, draft rows or malformed requests', () => {
  const store = fixture(), before = clone(store);
  for (const patch of [{}, { movementId: 'FOREIGN' }, { movementId, draftId, rowId: '' }, { movementId, account: 'Foreign', bookKey: 'PERSONAL|Foreign' }, { draftId, rowId: 'bank-1' }, { draftId, rowId: 'bank--1' }, { draftId, rowId, nature: 'SANKI' }, { draftId, rowId, account: 'Foreign' }]) assert.ok(snapshot(store, query(patch)).error);
  assert.deepEqual(store, before);
});
test('the read-only route retains existing bank-access checks and never invokes recovery or save helpers', () => {
  const fs = require('node:fs'), source = fs.readFileSync(require.resolve('../modules/expenses'), 'utf8'), start = source.indexOf("router.get('/api/expenses/bank-reconciliation-evidence'"), end = source.indexOf("router.get('/api/expenses/account-ledger'", start), route = source.slice(start, end);
  assert.ok(start >= 0 && end > start); assert.match(route, /canAccessBankReconciliation\(req,s,nature,account\)/); assert.match(route, /canManagePersonalAccounting\(req\)/); assert.match(route, /readLedgerEditSnapshot\(\)\.store/); assert.match(route, /status\(403\)/); assert.doesNotMatch(route, /loadStore|saveStore|repairOrphaned|recoverStored|\.post\(/);
});

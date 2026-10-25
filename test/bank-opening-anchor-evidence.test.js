'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { snapshot, LIMITS } = require('../modules/bank-opening-anchor-evidence');
const { verifiedReconciliationAnchor } = require('../modules/reconciliation-opening');
const account = 'Synthetic Bank 9099', draftId = 'BRD-SYNTHETIC-OPENING', hash = 'a'.repeat(64);
const query = patch => ({ account, nature: 'SANKI', bookKey: account, draftId, ...patch });
const clone = value => JSON.parse(JSON.stringify(value));
function freeze(value) { if (value && typeof value === 'object') { Object.values(value).forEach(freeze); Object.freeze(value); } return value; }
function fixture() {
  const partial = { id: 'BST-PARTIAL', draftId: 'D-PARTIAL', from: '2098-10-18', to: '2098-10-18',
    finalizedAt: '2098-10-24T11:00:00.000Z', balanceReconciled: true, balanceDifference: 0,
    statementSummary: { to: '2098-10-18', closingBalance: 248209.82, validated: true },
    reconciliationRows: [{ bank: { transactionId: 'BTX-PAYTM', description: 'SECRET-PERSONAL-NARRATION' }, ledger: { id: 'SECRET-LEDGER' } }] };
  const complete = { id: 'BST-COMPLETE', draftId: 'D-COMPLETE', from: '2098-10-16', to: '2098-10-18',
    file: '/SECRET-RETAINED-PATH.pdf', hash, balanceReconciled: true, balanceDifference: 0,
    statementSummary: { from: '2098-10-16', to: '2098-10-18', closingBalance: 165673.07, validated: true, secret: 'SECRET-SUMMARY' },
    reconciliationBankCoverageComplete: false,
    reconciliationRows: [{ bank: { transactionId: 'BTX-TRANSFER1' } }, { bank: { transactionId: 'BTX-TRANSFER2' } }],
    combinedReconciliationIds: [] };
  return { expenses: { secret: 'SECRET-EXPENSE' }, bankStatements: { [account]: {
    reconciledThrough: '2098-10-18', imports: [partial, complete,
      { id: 'SECRET-OLDER', to: '2098-10-15', statementSummary: { to: '2098-10-15', closingBalance: 100 } }],
    lastReconciliation: { draftId: 'D-PARTIAL', through: '2098-10-18', closingBalance: 248209.82,
      balanceReconciled: true, balanceDifference: 0, by: 'SECRET-USER' } },
    'PERSONAL|Other': { imports: [{ id: 'SECRET-OTHER-BOOK' }] } },
    bankReconciliationDrafts: { [draftId]: { id: draftId, account, nature: 'SANKI', fileHash: hash,
      temporaryFile: '/SECRET-DRAFT.pdf', transactions: [{ description: 'SECRET-TRANSACTION' }],
      summary: { from: '2098-10-19', to: '2098-10-24', openingBalance: 165673.07, totalDebits: 19507.7,
        totalCredits: 103372.83, closingBalance: 249538.2, validated: true },
      sourceStatements: [{ from: '2098-10-19', to: '2098-10-24', fileHash: hash, temporaryFile: '/SECRET-SOURCE.pdf' }],
      resolutions: { 'bank-0': { reason: 'SECRET-REVIEW' } } } } };
}

test('conflicting same-date closings show exact existing anchor decision and bounded proof fields only', () => {
  const store = fixture(), before = clone(store), result = snapshot(freeze(store), query());
  assert.equal(result.success, true); assert.equal(result.readOnly, true);
  assert.equal(result.statementSummary.openingBalance, 165673.07);
  assert.equal(result.candidateDate, '2098-10-18'); assert.equal(result.candidateCount, 2);
  assert.deepEqual(result.anchor, verifiedReconciliationAnchor(store.bankStatements[account], { before: '2098-10-19', nature: 'SANKI', account }));
  assert.equal(result.anchor.previousClosing, null); assert.match(result.anchor.anchorConflict.message, /different closing balances/);
  assert.deepEqual(result.candidates.find(x => x.id === 'BST-COMPLETE').coverage.physicalBankTransactionIds, ['BTX-TRANSFER1', 'BTX-TRANSFER2']);
  assert.equal(result.candidates.find(x => x.id === 'BST-COMPLETE').coverage.reconciliationBankCoverageComplete, false);
  assert.equal(result.candidates.find(x => x.id === 'BST-COMPLETE').statementSummary.validated, true);
  assert.deepEqual(result.lastReconciliation.matchingImportIds, ['BST-PARTIAL']);
  assert.doesNotMatch(JSON.stringify(result), /SECRET-/); assert.deepEqual(store, before);
  result.statementSummary.openingBalance = 0; result.candidates[0].coverage.physicalBankTransactionIds.push('MODIFIED');
  result.lastReconciliation.closingBalance = 0; result.retainedSource.sources[0].sha256 = 'MODIFIED';
  assert.deepEqual(store, before, 'response does not alias immutable source records');
});

test('coverage and combined lineage are shown without inventing supersession or changing selector behavior', () => {
  for (const combined of [false, true]) {
    const store = fixture(), complete = store.bankStatements[account].imports[1];
    if (combined) complete.combinedReconciliationIds = ['BST-PARTIAL'];
    else { complete.reconciliationBankCoverageComplete = true; complete.reconciliationBankTransactionIds = ['BTX-PAYTM', 'BTX-TRANSFER1', 'BTX-TRANSFER2']; }
    const result = snapshot(freeze(store), query()), record = result.candidates.find(x => x.id === 'BST-COMPLETE');
    assert.equal(result.anchor.previousClosing, 165673.07); assert.equal(record.selected, true);
    assert.equal(result.candidates.find(x => x.id === 'BST-PARTIAL').selected, false);
    assert.deepEqual(record.combinedReconciliationIds, combined ? ['BST-PARTIAL'] : []);
    assert.equal(record.coverage.physicalBankTransactionCount, combined ? 2 : 3);
    assert.equal(record.coverage.physicalBankTransactionIdsTruncated, false);
  }
});

test('source metadata never claims file verification or exposes paths, names, raw files, or invalid hashes', () => {
  const store = fixture(), draft = store.bankReconciliationDrafts[draftId];
  draft.originalName = 'C:/SECRET-ORIGINAL.pdf'; draft.sourceStatements[0].fileHash = '/SECRET-NOT-A-HASH';
  const result = snapshot(store, query());
  assert.equal(result.retainedSource.sha256, hash); assert.equal(result.retainedSource.retentionMetadataPresent, true);
  assert.equal(result.retainedSource.contentVerified, false);
  assert.equal(result.retainedSource.sources[0].sha256, null); assert.equal(result.retainedSource.sources[0].hashPresent, true);
  assert.equal(result.candidates.find(x => x.id === 'BST-COMPLETE').retainedSource.sha256, hash);
  assert.doesNotMatch(JSON.stringify(result), /SECRET-/);
});

test('exact draft, account and explicit nature are required; no implicit newest draft or prototype lookup', () => {
  const store = freeze(fixture());
  for (const patch of [{ draftId: '' }, { account: '' }, { nature: '' }, { nature: 'sanki' }, { draftId: [] }, { account: 'x'.repeat(257) }]) assert.equal(snapshot(store, query(patch)).status, 400);
  for (const patch of [{ draftId: 'OTHER' }, { draftId: '__proto__' }, { nature: 'PERSONAL' }, { account: 'Other' }]) assert.equal(snapshot(store, query(patch)).status, 404);
});

test('same-day overlap follows existing anchorBefore policy and invalid saved periods fail closed', () => {
  const store = fixture(), draft = store.bankReconciliationDrafts[draftId];
  draft.summary.from = '2098-10-18';
  assert.equal(snapshot(store, query()).anchorBefore, '2098-10-24');
  draft.summary.from = '2098-02-30'; draft.summary.to = '2098-02-30';
  assert.equal(snapshot(store, query()).status, 409);
});

test('contradictory period evidence and last snapshot dates remain visible; no uploaded opening is promoted', () => {
  const store = fixture(), book = store.bankStatements[account];
  book.imports[1].statementSummary.to = '2098-10-17';
  const result = snapshot(store, query());
  assert.match(result.anchor.anchorConflict.message, /contradictory period/);
  assert.equal(result.candidates.find(x => x.id === 'BST-COMPLETE').period.contradictory, true);
  book.imports = []; book.lastReconciliation = null;
  assert.equal(snapshot(store, query()).anchor.previousClosing, null);
  assert.deepEqual(snapshot(store, query()).candidates, []);
});

test('large candidate, coverage, lineage and source evidence is explicitly bounded, never silently complete', () => {
  const store = fixture(), book = store.bankStatements[account], template = book.imports[1];
  template.reconciliationBankTransactionIds = Array.from({ length: LIMITS.coverageIds + 1 }, (_, i) => 'BTX-' + i);
  template.combinedReconciliationIds = Array.from({ length: LIMITS.lineageIds + 1 }, (_, i) => 'BST-LINEAGE-' + i);
  template.sourceStatements = Array.from({ length: LIMITS.sources + 1 }, () => ({ fileHash: hash }));
  book.imports = Array.from({ length: LIMITS.candidates + 1 }, (_, i) => ({ ...template, id: 'BST-' + String(i).padStart(3, '0') }));
  const result = snapshot(store, query());
  assert.equal(result.candidates.length, LIMITS.candidates); assert.equal(result.candidatesTruncated, true);
  const record = result.candidates[0];
  assert.equal(record.coverage.physicalBankTransactionIds.length, LIMITS.coverageIds);
  assert.equal(record.coverage.physicalBankTransactionIdsTruncated, true);
  assert.equal(record.coverage.persistedBankTransactionIdsTruncated, true);
  assert.equal(record.combinedReconciliationIds.length, LIMITS.lineageIds); assert.equal(record.combinedReconciliationIdsTruncated, true);
  assert.equal(record.retainedSource.sources.length, LIMITS.sources); assert.equal(record.retainedSource.sourcesTruncated, true);
});

'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { createCutoffDayExtensionProof } = require('../modules/reconciliation-cutoff-extension');
const { verifiedReconciliationAnchor, reconciliationCoverageBook, finalizedMovementCoverageIds } = require('../modules/reconciliation-opening');
const CUT = '2098-06-18', BEFORE = '2098-06-19';
const clone = value => structuredClone(value);
function freeze(value) { if (value && typeof value === 'object') { Object.values(value).forEach(freeze); Object.freeze(value); } return value; }
const bank = (id, when, debit, credit, balance, importId) => ({ id, date: when, debit, credit, balance,
  firstSeenImport: importId, lastSeenImport: importId, reference: id, description: 'Synthetic evidence ' + id });
function fixture() {
  const partialId = 'BST-SYNTHETIC-PARTIAL', completeId = 'BST-SYNTHETIC-COMPLETE';
  const priorRows = Array.from({ length: 9 }, (_, n) => bank('BTX-PRIOR-' + n,
    n < 5 ? '2098-06-16' : '2098-06-17', 100, 0, 5800 - n * 100, partialId));
  const shared = { ...bank('BTX-SHARED', CUT, 0, 200, 5200, partialId), row: 10, reconciliationDraftRowIndex: 9 };
  const additions = [bank('BTX-ADD-A', CUT, 1111.11, 0, 4088.89, completeId),
    bank('BTX-ADD-B', CUT, 22.22, 0, 4066.67, completeId), bank('BTX-ADD-C', CUT, 333.33, 0, 3733.34, completeId)]
    .map((row, n) => ({ ...row, row: n + 2, reconciliationDraftRowIndex: n + 1 }));
  const report = rows => rows.map((row, n) => ({ id: 'bank-' + n,
    bank: { ...clone(row), transactionId: row.id }, linkedRecordIds: ['LEDGER-' + row.id] }));
  const successful = { balanceReconciled: true, balanceDifference: 0, closingBalanceDeferred: false };
  const partial = { ...successful, id: partialId, draftId: 'BRD-PARTIAL', from: '2098-06-16', to: CUT, rows: 10,
    finalizedAt: '2098-06-24T10:00:00Z', statementSummary: { from: '2098-06-10', to: CUT,
      openingBalance: 6000, totalDebits: 1000, totalCredits: 200, closingBalance: 5200, validated: true },
    reconciliationRows: report([...priorRows, shared]) };
  const complete = { ...successful, id: completeId, draftId: 'BRD-COMPLETE', from: CUT, to: CUT, rows: 3,
    finalizedAt: '2098-06-20T10:00:00Z', statementSummary: { from: CUT, to: CUT,
      openingBalance: 5000, totalDebits: 1466.66, totalCredits: 200, closingBalance: 3733.34, validated: true },
    reconciliationBankCoverageComplete: true, reconciliationBankTransactionIds: [shared.id, ...additions.map(row => row.id)],
    reconciliationRows: report(additions) };
  return { reconciledThrough: CUT, imports: [partial, complete],
    transactions: Object.fromEntries([...priorRows, shared, ...additions].map(row => [row.id, row])),
    lastReconciliation: { ...successful, draftId: complete.draftId, through: CUT, closingBalance: 3733.34 } };
}
const proof = book => createCutoffDayExtensionProof(book)(book.imports[1], book.imports[0]);
const anchor = book => verifiedReconciliationAnchor(book, { before: BEFORE, account: 'Synthetic Bank 6060', nature: 'SANKI' });
function blocked(name, mutate, expectAnchorConflict = true) {
  test(name, () => {
    const book = fixture(); mutate(book);
    const before = clone(book); assert.equal(proof(book), false);
    if (expectAnchorConflict) assert.equal(anchor(book).anchorConflict.type, 'ambiguous_opening_anchor');
    assert.deepEqual(book, before);
  });
}
function replaceFacts(book, id, patch) {
  Object.assign(book.transactions[id], patch);
  for (const record of book.imports) for (const key of ['reconciliationRows', 'carriedReconciliationRows'])
    for (const row of record[key] || []) if (row.bank && row.bank.transactionId === id) Object.assign(row.bank, patch);
}

test('legacy ten-row multi-day record is superseded by complete four-row cutoff chain without losing history', () => {
  const book = freeze(fixture()), before = clone(book);
  assert.equal(proof(book), true);
  const result = anchor(book);
  assert.equal(result.previousClosing, 3733.34); assert.equal(result.anchorRecordId, book.imports[1].id);
  const coverage = reconciliationCoverageBook(book, result);
  assert.deepEqual(coverage.imports.map(row => row.id), book.imports.map(row => row.id));
  const covered = finalizedMovementCoverageIds({}, coverage);
  for (const record of book.imports) for (const row of record.reconciliationRows) assert.ok(covered.has(row.linkedRecordIds[0]));
  assert.deepEqual(book, before);
  assert.equal(book.imports[0].reconciliationBankCoverageComplete, undefined, 'no coverage flag is invented');
});
test('proof is independent of physical ID order, source serials, timestamps and import order', () => {
  const book = fixture(), expected = anchor(book);
  book.imports[0].finalizedAt = '2199-12-31T23:59:59Z'; book.imports[1].finalizedAt = '2000-01-01T00:00:00Z';
  book.imports.forEach(record => record.reconciliationRows.reverse()); book.imports[1].reconciliationBankTransactionIds.reverse();
  for (const row of Object.values(book.transactions)) row.row = 999;
  assert.equal(proof(book), true); book.imports.reverse();
  assert.deepEqual(anchor(book), expected);
});
test('stale last snapshot of the proven partial is ignored, not overwritten', () => {
  const book = fixture(); Object.assign(book.lastReconciliation, { draftId: 'BRD-PARTIAL', closingBalance: 5200 });
  const before = clone(book); assert.equal(anchor(book).previousClosing, 3733.34); assert.deepEqual(book, before);
});
test('candidate with a full persisted older coverage list also uses terminal-day proof', () => {
  const book = fixture(), old = book.imports[0]; old.reconciliationBankCoverageComplete = true;
  old.reconciliationBankTransactionIds = old.reconciliationRows.map(row => row.bank.transactionId);
  assert.equal(proof(book), true); assert.equal(anchor(book).previousClosing, 3733.34);
});
test('exact decimal strings produce the same proof', () => {
  const book = fixture();
  for (const id of Object.keys(book.transactions)) {
    const row = book.transactions[id]; replaceFacts(book, id, Object.fromEntries(['debit', 'credit', 'balance'].map(key => [key, String(row[key])])));
  }
  assert.equal(proof(book), true);
});
test('multiple old terminal rows are accepted only as the complete initial prefix', () => {
  const book = fixture(), old = book.imports[0], next = book.imports[1];
  book.transactions['BTX-ADD-A'].firstSeenImport = old.id;
  old.reconciliationRows.push(clone(next.reconciliationRows[0])); old.rows += 1; next.rows -= 1;
  Object.assign(old.statementSummary, { totalDebits: 2111.11, closingBalance: 4088.89 });
  assert.equal(proof(book), true); assert.equal(anchor(book).previousClosing, 3733.34);
});
blocked('old terminal rows interleaved with new rows are not a prefix', book => {
  const old = book.imports[0], next = book.imports[1];
  book.transactions['BTX-ADD-B'].firstSeenImport = old.id;
  old.reconciliationRows.push(clone(next.reconciliationRows[1])); old.rows += 1; next.rows -= 1;
});
test('negative bank balances do not alter the exact-money proof', () => {
  const book = fixture();
  for (const [id, row] of Object.entries(book.transactions)) replaceFacts(book, id, { balance: (Number(row.balance) - 10000).toFixed(2) });
  for (const record of book.imports) for (const key of ['openingBalance', 'closingBalance'])
    record.statementSummary[key] = (Number(record.statementSummary[key]) - 10000).toFixed(2);
  book.lastReconciliation.closingBalance = '-6266.66';
  assert.equal(proof(book), true); assert.equal(anchor(book).previousClosing, -6266.66);
});
test('last snapshot conflicting with its own import remains blocked', () => {
  const book = fixture(); book.lastReconciliation.closingBalance = 1;
  assert.equal(proof(book), true); assert.match(anchor(book).anchorConflict.message, /own historical/);
});
test('a third incomparable finalized cutoff record remains a conflict', () => {
  const book = fixture(); book.imports.push({ id: 'BST-OTHER', from: CUT, to: CUT,
    balanceReconciled: true, balanceDifference: 0, closingBalanceDeferred: false,
    statementSummary: { from: CUT, to: CUT, closingBalance: 7777, validated: true },
    reconciliationRows: [{ bank: { transactionId: 'BTX-UNRESOLVED-OTHER' } }] });
  assert.equal(proof(book), true); assert.equal(anchor(book).previousClosing, null);
});

for (const index of [0, 1]) for (const [name, mutate] of [
  ['explicit incomplete coverage', r => { r.reconciliationBankCoverageComplete = false; }],
  ['failed balance', r => { r.balanceReconciled = false; }],
  ['deferred balance', r => { r.closingBalanceDeferred = true; }],
  ['nonzero balance difference', r => { r.balanceDifference = 0.01; }],
  ['missing success', r => { delete r.balanceReconciled; }],
  ['unvalidated source', r => { r.statementSummary.validated = false; }],
  ['adjusted closing', r => { r.companyAdjustedClosingBalance = r.statementSummary.closingBalance; }],
  ['zero adjustment metadata', r => { r.closingBalanceAdjustment = 0; }],
  ['pending approval', r => { r.pendingApproval = true; }],
  ['source total discrepancy', r => { r.statementSummary.totalDebits = 1; }],
  ['invalid record start', r => { r.from = '2098-02-30'; }],
  ['invalid source start', r => { r.statementSummary.from = '2098-02-30'; }],
  ['contradictory top-level close', r => { r.closingBalance = 1; }],
  ['contradictory ledger close', r => { r.ledgerClosingBalance = 1; }],
  ['invalid ledger close', r => { r.ledgerClosingBalance = null; }],
]) blocked((index ? 'new: ' : 'old: ') + name, b => mutate(b.imports[index]));

blocked('legacy count mismatch', b => { b.imports[0].rows = 9; });
blocked('legacy missing report row', b => { b.imports[0].reconciliationRows.pop(); });
blocked('legacy duplicate bank report identity', b => { b.imports[0].reconciliationRows.push(clone(b.imports[0].reconciliationRows[0])); });
blocked('legacy blank report identity', b => { b.imports[0].reconciliationRows[0].bank.transactionId = ''; });
blocked('legacy report physical ID not firstSeen in this import', b => { b.transactions['BTX-PRIOR-0'].firstSeenImport = 'BST-OTHER'; });
blocked('legacy extra firstSeen row', b => { b.transactions.EXTRA = bank('BTX-EXTRA', '2098-06-17', 1, 0, 999, b.imports[0].id); });
blocked('legacy report money conflicts with stored same ID', b => { b.imports[0].reconciliationRows[9].bank.balance = 5200.01; });
blocked('legacy report date conflicts with stored same ID', b => { b.imports[0].reconciliationRows[9].bank.date = '2098-06-17'; });
blocked('legacy report reference conflicts with stored same ID', b => { b.imports[0].reconciliationRows[9].bank.reference = 'OTHER-REFERENCE'; });
blocked('legacy purported persisted set is different', b => { b.imports[0].reconciliationBankTransactionIds = ['BTX-SHARED']; });
blocked('new coverage flag absent', b => { delete b.imports[1].reconciliationBankCoverageComplete; });
blocked('new coverage ID missing', b => { b.imports[1].reconciliationBankTransactionIds.pop(); });
blocked('new coverage duplicate ID', b => { b.imports[1].reconciliationBankTransactionIds.push('BTX-SHARED'); });
blocked('new coverage unknown ID', b => { b.imports[1].reconciliationBankTransactionIds.push('BTX-UNKNOWN'); });
blocked('new firstSeen count mismatch', b => { b.imports[1].rows = 4; });
blocked('complete new coverage cannot replace a missing retained report', b => { b.imports[1].reconciliationRows = []; });
blocked('every newly imported ID needs retained report evidence', b => { b.imports[1].reconciliationRows.pop(); });
blocked('new multi-day source is outside narrow rule', b => { b.imports[1].from = b.imports[1].statementSummary.from = '2098-06-17'; });
blocked('unknown stored cutoff bank ID cannot disappear', b => { b.transactions.EXTRA = bank('BTX-EXTRA', CUT, 1, 0, 1111, 'BST-UNRELATED'); });
blocked('unknown malformed stored date cannot establish cutoff completeness', b => { b.transactions.EXTRA = bank('BTX-EXTRA', '', 1, 0, 1111, 'BST-UNRELATED'); });
blocked('duplicate physical stored ID', b => { b.transactions.DUPLICATE = clone(b.transactions['BTX-SHARED']); });
blocked('canonical key and ID mismatch', b => { b.transactions['a'.repeat(24)] = b.transactions['BTX-SHARED']; delete b.transactions['BTX-SHARED']; });
blocked('missing earlier-day stored row', b => { delete b.transactions['BTX-PRIOR-0']; });
blocked('unrepresented old cutoff row', b => { replaceFacts(b, 'BTX-PRIOR-0', { date: CUT }); });
blocked('old row beyond cutoff', b => { replaceFacts(b, 'BTX-PRIOR-0', { date: BEFORE }); });
blocked('old terminal closing not at prefix boundary', b => {
  b.imports[0].statementSummary.closingBalance = 5100; b.imports[0].statementSummary.totalDebits = 1100;
});
blocked('aggregate-valid but broken new running balance', b => { replaceFacts(b, 'BTX-ADD-A', { balance: 4088.90 }); });
blocked('new row financial total mismatch', b => { replaceFacts(b, 'BTX-ADD-A', { debit: 1111.12 }); });
blocked('multiple outgoing rows from same predecessor', b => { replaceFacts(b, 'BTX-ADD-B', { balance: 5177.78 }); });
for (const value of [null, '', '1x', ' 200', '2e2', '200.000000001', 200.000000001, Infinity, -1])
  blocked('raw invalid/sub-paise money rejected: ' + String(value), b => { replaceFacts(b, 'BTX-SHARED', { credit: value }); });
blocked('sub-paise report observation is not rounded', b => { b.imports[0].reconciliationRows[9].bank.credit = '200.000000001'; });
blocked('empty money side is not zero', b => { replaceFacts(b, 'BTX-SHARED', { debit: '' }); });
blocked('zero-value movement does not imply ordering', b => { replaceFacts(b, 'BTX-ADD-B', { debit: 0 }); });
blocked('two-sided movement cannot imply ordering', b => { replaceFacts(b, 'BTX-ADD-B', { credit: 1 }); });
blocked('repeated balance loop fails closed despite exact aggregates', b => {
  replaceFacts(b, 'BTX-ADD-A', { debit: 200, balance: 5000 });
  replaceFacts(b, 'BTX-ADD-B', { debit: 100, balance: 4900 });
  replaceFacts(b, 'BTX-ADD-C', { debit: 100, balance: 4800 });
  Object.assign(b.imports[1].statementSummary, { totalDebits: 400, closingBalance: 4800 });
  b.lastReconciliation.closingBalance = 4800;
});
test('record from another book cannot be compared by this factory', () => {
  const book = fixture(), otherBook = fixture();
  assert.equal(createCutoffDayExtensionProof(book)(otherBook.imports[1], book.imports[0]), false);
});
test('malformed snapshots and oversized evidence fail closed', () => {
  for (const book of [null, {}, { imports: [], transactions: [] }]) assert.equal(createCutoffDayExtensionProof(book)({}, {}), false);
  const book = fixture(); book.imports[0].reconciliationRows = Array(100001).fill({});
  assert.equal(proof(book), false);
});
test('independent proof attempts cannot mutate an immutable snapshot', () => {
  const book = freeze(fixture()), before = clone(book), check = createCutoffDayExtensionProof(book);
  assert.equal(check(book.imports[1], book.imports[0]), true);
  assert.equal(check(book.imports[0], book.imports[1]), false);
  assert.deepEqual(book, before);
});

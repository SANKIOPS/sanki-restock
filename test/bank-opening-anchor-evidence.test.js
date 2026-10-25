'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { snapshot, LIMITS } = require('../modules/bank-opening-anchor-evidence');
const { verifiedReconciliationAnchor } = require('../modules/reconciliation-opening');
const { bankEvidenceIndex, resolveBankRowEvidence } = require('../modules/bank-opening-row-evidence');
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
  assert.equal(result.candidates.reduce((count, candidate) => count + candidate.coverage.physicalBankRows.length, 0), LIMITS.totalRowFacts);
  assert.equal(result.candidates[1].coverage.rowFactsTruncated, true);
  assert.equal(result.candidates[1].coverage.allRowFactsUsable, false);
});

function rowFixture() {
  const store = fixture(), book = store.bankStatements[account], old = book.imports[0], current = book.imports[1];
  old.from = '2098-10-16'; old.rows = 3;
  current.from = '2098-10-18'; current.rows = 3; current.reconciliationBankCoverageComplete = true;
  current.statementSummary.from = '2098-10-18'; current.statementSummary.openingBalance = 1000;
  old.statementSummary.closingBalance = 1100; current.statementSummary.closingBalance = 550;
  book.lastReconciliation.closingBalance = 1100;
  const make = (id, date, debit, credit, balance, importId, row) => ({ id, date, debit, credit, balance, row,
    firstSeenImport: importId, lastSeenImport: importId, reconciliationDraftId: importId === old.id ? old.draftId : current.draftId,
    reconciliationDraftRowIndex: row - 1, description: 'SECRET-MERCHANT', reference: 'SECRET-REFERENCE', privateAccount: 'SECRET-ACCOUNT' });
  const rows = [
    make('BTX-EARLIER-A', '2098-10-16', 1, 0, 200, old.id, 1),
    make('BTX-EARLIER-B', '2098-10-17', 1, 0, 199, old.id, 2),
    make('BTX-SHARED', '2098-10-18', 0, 100, 1100, old.id, 10),
    make('BTX-Z-FIRST', '2098-10-18', 300, 0, 800, current.id, 2),
    make('BTX-A-SECOND', '2098-10-18', 200, 0, 600, current.id, 3),
    make('BTX-M-THIRD', '2098-10-18', 50, 0, 550, current.id, 4)
  ];
  book.transactions = Object.fromEntries(rows.slice().reverse().map(row => [row.id.slice(4), row]));
  const report = row => ({ id: 'bank-' + (row.row - 1), bank: { ...row, transactionId: row.id }, ledger: { id: 'SECRET-LEDGER' } });
  old.reconciliationRows = rows.slice(0, 3).map(report);
  current.reconciliationRows = rows.slice(3).map(report);
  current.reconciliationBankTransactionIds = rows.slice(2).map(row => row.id);
  store.bankStatements['PERSONAL|Other'].transactions = { unrelated: { ...rows[2], balance: 999 } };
  return { store, book, old, current, rows };
}
const evidenceRow = (result, candidateId, bankId) => result.candidates.find(item => item.id === candidateId).coverage.physicalBankRows.find(row => row.id === bankId);

test('same-book exact row facts distinguish earlier days and retained mixed source positions without choosing a closing', () => {
  const { store, book, old, current } = rowFixture(), before = clone(store);
  const result = snapshot(freeze(store), query()), oldEvidence = result.candidates.find(item => item.id === old.id);
  const currentEvidence = result.candidates.find(item => item.id === current.id), coverage = currentEvidence.coverage;
  assert.equal(result.anchor.previousClosing, null, 'diagnostic facts do not change existing all-period selector');
  assert.deepEqual(result.anchor, verifiedReconciliationAnchor(book, { before: '2098-10-19', nature: 'SANKI', account }));
  assert.deepEqual(oldEvidence.coverage.earlierDayBankTransactionIds, ['BTX-EARLIER-A', 'BTX-EARLIER-B']);
  assert.deepEqual(oldEvidence.coverage.cutoffDayBankTransactionIds, ['BTX-SHARED']);
  assert.equal(coverage.cutoffDayBankTransactionIds.length, 4); assert.equal(coverage.allRowFactsUsable, true);
  assert.equal(oldEvidence.coverage.firstSeenStoredRowCount, 3);
  assert.deepEqual(oldEvidence.coverage.firstSeenBankTransactionIds, ['BTX-EARLIER-A', 'BTX-EARLIER-B', 'BTX-SHARED']);
  assert.equal(Object.hasOwn(oldEvidence.coverage, 'reconciliationBankCoverageComplete'), false, 'legacy coverage is not promoted');
  assert.equal(coverage.firstSeenStoredRowCount, 3); assert.equal(coverage.physicalBankTransactionCount, 4);
  const shared = evidenceRow(result, current.id, 'BTX-SHARED');
  assert.deepEqual(shared.facts, { date: '2098-10-18', debit: 0, credit: 100, balance: 1100 });
  assert.equal(shared.candidateReportMatchCount, 0); assert.equal(shared.retainedReportMatchCount, 1);
  assert.deepEqual(shared.observations[0].provenance, { firstSeenImport: old.id, lastSeenImport: old.id });
  assert.equal(shared.observations[0].sourcePosition.row, 10);
  assert.equal(shared.observations[0].sourcePosition.matchesCandidateDraft, false);
  assert.equal(evidenceRow(result, current.id, 'BTX-Z-FIRST').observations[0].sourcePosition.row, 2);
  assert.match(coverage.ordering, /not transaction order/); assert.match(coverage.ordering, /do not assert complete period coverage/);
  assert.doesNotMatch(JSON.stringify(result), /SECRET-/); assert.deepEqual(store, before);
  shared.facts.balance = 1; shared.observations[0].provenance.firstSeenImport = 'MODIFIED';
  shared.observations[0].sourcePosition.row = 999;
  assert.deepEqual(store, before, 'all returned row fields are independent projections');
});

test('missing rows, duplicate IDs and canonical key/identity collisions never supply balance-chain facts', () => {
  for (const kind of ['missing', 'duplicate', 'key_mismatch']) {
    const { store, book, current } = rowFixture();
    if (kind === 'missing') delete book.transactions.SHARED;
    if (kind === 'duplicate') book.transactions.DUPLICATE = { ...book.transactions.SHARED };
    if (kind === 'key_mismatch') {
      book.transactions['a'.repeat(24)] = book.transactions.SHARED; delete book.transactions.SHARED;
    }
    const row = evidenceRow(snapshot(freeze(store), query()), current.id, 'BTX-SHARED');
    assert.equal(row.usableForBalanceChain, false); assert.equal(row.facts, null);
    assert.ok(row.issues.includes({ missing: 'missing_stored_row', duplicate: 'duplicate_stored_id', key_mismatch: 'stored_key_identity_mismatch' }[kind]));
    assert.equal(row.observations.length > 0, true, 'retained evidence is still visible, not silently chosen');
  }
});

test('contradictory same-ID date, amounts, balance and private identity evidence fail closed', () => {
  for (const [key, value] of [['date', '2098-10-17'], ['debit', 1], ['credit', 101], ['balance', 1101],
    ['reference', 'SECRET-OTHER-REFERENCE'], ['description', 'SECRET-OTHER-MERCHANT']]) {
    const { store, book, current } = rowFixture();
    book.imports[0].reconciliationRows[2].bank[key] = value;
    const result = snapshot(freeze(store), query()), row = evidenceRow(result, current.id, 'BTX-SHARED');
    assert.equal(row.usableForBalanceChain, false); assert.equal(row.facts, null);
    assert.ok(row.conflictingFields.includes(key)); assert.ok(row.issues.includes('conflicting_evidence'));
    assert.equal(result.candidates.find(item => item.id === current.id).coverage.allRowFactsUsable, false);
    assert.doesNotMatch(JSON.stringify(result), /SECRET-/);
  }
});

test('incomplete retained facts, invalid dates or sub-cent amounts do not become inferred bank evidence', () => {
  for (const patch of [{ date: '2098-02-30' }, { debit: '1,000' }, { credit: '' }, { balance: null }, { balance: 12.345 },
    { debit: -1 }, { debit: 1, credit: 1 }, { debit: 0, credit: 0 }]) {
    const { book, current } = rowFixture();
    Object.assign(book.transactions.SHARED, patch);
    const before = clone(book), row = resolveBankRowEvidence('BTX-SHARED', bankEvidenceIndex(freeze(book)), current);
    assert.equal(row.usableForBalanceChain, false); assert.equal(row.facts, null);
    assert.ok(row.issues.includes('incomplete_or_invalid_row_facts')); assert.deepEqual(book, before);
  }
});

test('row evidence accepts exact decimal paise only, without rounding tiny fractions or accepting scientific notation', () => {
  const candidate = { id: 'BST-EXACT', draftId: 'BRD-EXACT' };
  const resolve = balance => {
    const bank = { id: 'BTX-EXACT', date: '2098-10-18', debit: 1, credit: 0, balance };
    return resolveBankRowEvidence(bank.id, bankEvidenceIndex(freeze({ transactions: { EXACT: bank }, imports: [] })), candidate);
  };
  for (const value of [12.000000001, '12.000000001', '12.000', '1.2e1', '12e0', 1e-7, '1e-7',
    null, undefined, '', '  ', false, NaN, Infinity, '-Infinity', '90071992547409.92']) {
    const row = resolve(value);
    assert.equal(row.usableForBalanceChain, false, 'reject ' + String(value));
    assert.equal(row.facts, null);
    assert.ok(row.issues.includes('incomplete_or_invalid_row_facts'));
    assert.ok(row.observations[0].issues.includes('missing_or_invalid_balance'));
  }
  for (const [value, expected] of [[12, 12], ['12', 12], ['12.0', 12], ['12.00', 12], ['12.01', 12.01], [-0.01, -0.01]]) {
    const row = resolve(value);
    assert.equal(row.usableForBalanceChain, true, 'accept ' + String(value));
    assert.equal(row.facts.balance, expected);
  }
});

test('bounded observations still inspect all retained reports and flag conflicts beyond the visible observations', () => {
  const { store, book, current } = rowFixture(), report = book.imports[0].reconciliationRows[2];
  for (let i = 0; i < LIMITS.rowObservations + 1; i++) book.imports.push({ id: 'BST-HISTORICAL-' + i,
    to: '2098-10-15', carriedReconciliationRows: [clone(report)] });
  book.imports.at(-1).carriedReconciliationRows[0].bank.balance = 1;
  const row = evidenceRow(snapshot(freeze(store), query()), current.id, 'BTX-SHARED');
  assert.equal(row.observations.length, LIMITS.rowObservations); assert.equal(row.observationsTruncated, true);
  assert.equal(row.observationCount, LIMITS.rowObservations + 3);
  assert.equal(row.usableForBalanceChain, false); assert.ok(row.conflictingFields.includes('balance'));
});

test('only retained positions and safe import IDs are exposed; future-date facts do not prove candidate period coverage', () => {
  const { store, book, current } = rowFixture(), row = book.transactions['Z-FIRST'];
  row.date = current.reconciliationRows[0].bank.date = '2098-10-19';
  delete row.row; delete row.reconciliationDraftRowIndex; delete row.reconciliationDraftId;
  row.firstSeenImport = '/SECRET-PATH'; row.lastSeenImport = '/SECRET-PATH';
  const result = snapshot(freeze(store), query()), evidence = evidenceRow(result, current.id, row.id);
  assert.equal(evidence.dateRelationToCutoff, 'after');
  assert.deepEqual(evidence.observations[0].sourcePosition, {}); assert.deepEqual(evidence.observations[0].provenance, {});
  assert.deepEqual(result.candidates.find(item => item.id === current.id).coverage.afterCutoffBankTransactionIds, [row.id]);
  assert.doesNotMatch(JSON.stringify(result), /SECRET-/);
});

test('malformed physical identities and first-seen IDs are redacted rather than leaking paths or narration', () => {
  const { store, book, current } = rowFixture(), invalid = '/SECRET-PATH.pdf';
  book.transactions.BAD = { ...book.transactions.SHARED, id: invalid, firstSeenImport: current.id };
  current.reconciliationBankTransactionIds.push(invalid);
  const result = snapshot(freeze(store), query()), coverage = result.candidates.find(item => item.id === current.id).coverage;
  assert.equal(coverage.invalidFirstSeenBankTransactionIdCount, 1);
  assert.equal(coverage.invalidPhysicalBankTransactionIdCount, 1);
  assert.equal(coverage.firstSeenBankTransactionIdsTruncated, true);
  assert.equal(coverage.rowFactsTruncated, true); assert.equal(coverage.allRowFactsUsable, false);
  assert.ok(coverage.physicalBankRows.some(row => row.id === null && row.issues.includes('invalid_identity')));
  assert.doesNotMatch(JSON.stringify(result), /SECRET-/);
  const raw = resolveBankRowEvidence(invalid, bankEvidenceIndex(book), current);
  assert.equal(raw.id, null); assert.equal(raw.facts, null); assert.doesNotMatch(JSON.stringify(raw), /SECRET-/);
});

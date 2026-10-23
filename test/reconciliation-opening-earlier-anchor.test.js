'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { verifiedReconciliationAnchor, reconciliationCoverageBook,
  finalizedMovementCoverageIds, calculateReconciliationOpening } = require('../modules/reconciliation-opening');

function axisBook() {
  return { reconciledThrough: '2026-10-18', lastReconciliation: {
    draftId: 'D18', through: '2026-10-18', closingBalance: 248209.82,
  }, imports: [
    { id: 'I18', draftId: 'D18', to: '2026-10-18', statementSummary: { to: '2026-10-18', closingBalance: 248209.82 },
      reconciliationRows: [{ ledger: { id: 'PAYTM18' } }] },
    { id: 'I15', draftId: 'D15', to: '2026-10-15', statementSummary: { to: '2026-10-15', closingBalance: 264515.82 },
      reconciliationRows: [{ ledger: { id: 'TR-PRIOR' } }] },
  ] };
}

test('overlapping same-day statement selects the newest earlier close without changing the default API', () => {
  const book = axisBook();
  assert.equal(verifiedReconciliationAnchor(book).previousClosing, 248209.82);
  assert.deepEqual(verifiedReconciliationAnchor(book, { before: '2026-10-18' }), {
    cutoffDate: '2026-10-15', previousClosing: 264515.82, anchorSource: 'finalized_import', anchorRecordId: 'I15',
  });
  book.imports.reverse();
  assert.equal(verifiedReconciliationAnchor(book, { before: '2026-10-18' }).anchorRecordId, 'I15');
  assert.equal(verifiedReconciliationAnchor(book, { before: '2026-10-19' }).previousClosing, 248209.82);
});

test('the selected earlier anchor excludes only its own covered money, retaining later transfers and automatic fees', () => {
  const book = axisBook();
  book.imports[0].reconciliationRows.push({ ledger: { id: 'TR-GAP' } }, { ledger: { id: 'ADJ-GAP' } });
  const store = { transfers: [{ id: 'TR-GAP', reconciliationDraft: 'D18' }],
    adjustments: [{ id: 'ADJ-GAP', reconciliationDraft: 'D18', automaticAxisTransferCharge: true }] };
  const anchor = verifiedReconciliationAnchor(book, { before: '2026-10-18' });
  const scoped = reconciliationCoverageBook(book, anchor);
  assert.equal(scoped.lastReconciliation, null);
  assert.deepEqual(scoped.imports.map(record => record.id), ['I15']);
  const covered = finalizedMovementCoverageIds(store, scoped);
  assert.deepEqual(Array.from(covered), ['TR-PRIOR']);
  assert.ok(finalizedMovementCoverageIds(store, book).has('TR-GAP'), 'matching locks still use global finalized coverage');
  const opening = calculateReconciliationOpening({ ...anchor, baseOpening: 47844.86,
    statementFrom: '2026-10-18', statementOpening: 234515.82, coveredMovementIds: covered,
    movements: [
      { id: 'TR-PRIOR', date: '2026-10-15', credit: 900000 },
      { id: 'TR-GAP', date: '2026-10-17', debit: 29994.1 },
      { id: 'ADJ-GAP', date: '2026-10-17', debit: 5.9, automaticAxisTransferCharge: true },
      { id: 'ADJ-CUTOFF-LEGACY', date: '2026-10-15', debit: 5.9, automaticAxisTransferCharge: true },
      { id: 'PAYTM18', date: '2026-10-18', credit: 13694 },
    ] });
  assert.equal(opening.currentOpening, 234515.82);
  assert.equal(opening.carryForwardNet, -30000);
  assert.equal(opening.openingContinuityGap, 0);
  assert.deepEqual(opening.carryForwardMovements.map(row => row.id), ['ADJ-GAP', 'TR-GAP']);
});

test('conflicting closes on the newest eligible date fail closed rather than use upload order or an older close', () => {
  const book = axisBook();
  book.imports.push({ id: 'I15-CONFLICT', to: '2026-10-15', closingBalance: 123 });
  book.imports.push({ id: 'I14', to: '2026-10-14', closingBalance: 50 });
  const anchor = verifiedReconciliationAnchor(book, { before: '2026-10-18' });
  assert.equal(anchor.previousClosing, null);
  assert.equal(anchor.anchorConflict.type, 'ambiguous_opening_anchor');
  assert.equal(anchor.anchorConflict.cutoffDate, '2026-10-15');
});

test('contradictory eligible import periods fail closed and do not authorize a date relabel', () => {
  const book = axisBook();
  book.imports[1].statementSummary.to = '2026-10-14';
  const anchor = verifiedReconciliationAnchor(book, { before: '2026-10-18' });
  assert.equal(anchor.previousClosing, null);
  assert.equal(anchor.anchorConflict.type, 'ambiguous_opening_anchor');
  assert.deepEqual(reconciliationCoverageBook(book, { cutoffDate: '2026-10-15' }).imports, []);
});

test('company-adjusted historical closes stay adjusted instead of restoring excluded personal bank money', () => {
  const book = axisBook();
  book.imports[1].companyAdjustedClosingBalance = 260000;
  const anchor = verifiedReconciliationAnchor(book, { before: '2026-10-18' });
  assert.equal(anchor.previousClosing, 260000);
  assert.equal(reconciliationCoverageBook(book, anchor).imports[0].companyAdjustedClosingBalance, 260000);
});

function ownerAdjustedBook() {
  const book = axisBook();
  Object.assign(book.imports[1], {
    balanceReconciled: false, reconciled: false, closingBalanceDeferred: true, balanceDifference: 125.5,
    companyAdjustedClosingBalance: 260000, calculatedCompanyBalance: 260125.5,
    closingBalanceAdjustment: -125.5, closingBalanceAdjustmentReason: 'Owner-confirmed company funds after review',
    closingBalanceAdjustedBy: 'fixture-owner', closingBalanceAdjustedAt: '2026-10-16T10:30:00.000Z',
  });
  return book;
}

const ownerAdjustedContext = { before: '2026-10-18', nature: 'SANKI', account: 'Prashant Axis 3645' };

test('durable Owner correction metadata preserves an approved 3645 company closing despite legacy failure flags', () => {
  const book = ownerAdjustedBook(), before = JSON.stringify(book);
  const anchor = verifiedReconciliationAnchor(book, ownerAdjustedContext);
  assert.equal(anchor.previousClosing, 260000);
  assert.equal(anchor.anchorRecordId, 'I15');
  assert.equal(anchor.anchorConflict, undefined);
  assert.equal(JSON.stringify(book), before, 'approved correction and original bank/failure history stay untouched');
});

test('Owner correction exception is restricted to the exact company-funds account and entity', () => {
  for (const context of [
    { before: '2026-10-18' },
    { ...ownerAdjustedContext, account: 'Axis Bank 3448' },
    { ...ownerAdjustedContext, nature: 'PERSONAL' },
  ]) {
    const result = verifiedReconciliationAnchor(ownerAdjustedBook(), context);
    assert.equal(result.previousClosing, null);
    assert.equal(result.anchorConflict.type, 'ambiguous_opening_anchor');
  }
});

test('incomplete or inconsistent adjustment metadata cannot excuse a deferred 3645 closing', () => {
  for (const mutate of [
    record => { delete record.closingBalanceAdjustmentReason; },
    record => { delete record.closingBalanceAdjustedBy; },
    record => { delete record.closingBalanceAdjustedAt; },
    record => { delete record.calculatedCompanyBalance; },
    record => { delete record.closingBalanceAdjustment; },
    record => { record.closingBalanceAdjustment = -120; },
    record => { record.closingBalanceAdjustedAt = 'invalid'; },
    record => { record.closingBalanceAdjustedAt = '0'; },
    record => { record.companyAdjustedClosingBalance = ''; },
  ]) {
    const book = ownerAdjustedBook();
    mutate(book.imports[1]);
    const result = verifiedReconciliationAnchor(book, ownerAdjustedContext);
    assert.equal(result.previousClosing, null);
    assert.equal(result.anchorConflict.type, 'ambiguous_opening_anchor');
  }
});

test('Owner-approved zero company closing and matching last snapshot retain their authority', () => {
  const book = ownerAdjustedBook();
  Object.assign(book.imports[1], { companyAdjustedClosingBalance: 0,
    calculatedCompanyBalance: 125.5, closingBalanceAdjustment: -125.5 });
  book.lastReconciliation = { ...book.imports[1], through: '2026-10-18' };
  const anchor = verifiedReconciliationAnchor(book, ownerAdjustedContext);
  assert.equal(anchor.previousClosing, 0);
  assert.equal(anchor.anchorSource, 'last_reconciliation');
  assert.equal(anchor.anchorConflict, undefined);
});

test('a relabeled historical last snapshot uses its actual import date and never inherits the later partial cutoff', () => {
  const book = axisBook();
  book.lastReconciliation = { draftId: 'D15', through: '2026-10-18', closingBalance: 264515.82 };
  const anchor = verifiedReconciliationAnchor(book, { before: '2026-10-18' });
  assert.equal(anchor.cutoffDate, '2026-10-15');
  assert.equal(anchor.anchorSource, 'last_reconciliation');
  assert.equal(anchor.previousClosing, 264515.82);
  assert.equal(reconciliationCoverageBook(book, anchor).lastReconciliation.draftId, 'D15');
});

test('a failed newer ledger-only snapshot blocks older fallback until its balance is reconciled', () => {
  const book = axisBook();
  book.imports.push({ id: 'I17', to: '2026-10-17', ledgerClosingBalance: 111,
    closingBalanceDeferred: true, balanceReconciled: false });
  const blocked = verifiedReconciliationAnchor(book, { before: '2026-10-18' });
  assert.equal(blocked.previousClosing, null);
  assert.equal(blocked.anchorConflict.cutoffDate, '2026-10-17');
  Object.assign(book.imports.at(-1), { balanceReconciled: true, closingBalanceDeferred: false, balanceDifference: 0 });
  assert.equal(verifiedReconciliationAnchor(book, { before: '2026-10-18' }).previousClosing, 111);
});

for (const failure of [{ balanceReconciled: false }, { reconciled: false },
  { closingBalanceDeferred: true }, { balanceDifference: 5.9 }, { balanceDifference: 'invalid' }]) {
  test('an explicitly unreconciled newer bank close cannot silently anchor or fall back: ' + JSON.stringify(failure), () => {
    const book = axisBook();
    book.imports.push({ id: 'I17-FAILED', to: '2026-10-17', statementSummary: {
      to: '2026-10-17', closingBalance: 234515.82, validated: true,
    }, ...failure });
    const result = verifiedReconciliationAnchor(book, { before: '2026-10-18' });
    assert.equal(result.previousClosing, null);
    assert.equal(result.anchorConflict.cutoffDate, '2026-10-17');
    assert.match(result.anchorConflict.message, /deferred or unreconciled/);
  });
}

function sameDayPartialAndComplete({ explicitCombined = false, completeIds = true } = {}) {
  const partial = { id: 'I18-PARTIAL', draftId: 'D18-PARTIAL', to: '2026-10-18', balanceReconciled: true,
    balanceDifference: 0, finalizedAt: '2026-10-23T18:00:00Z', statementSummary: {
      to: '2026-10-18', closingBalance: 248209.82, validated: true,
    }, reconciliationRows: [{ bank: { transactionId: 'BTX-PAYTM18' } }] };
  const complete = { id: 'I18-COMPLETE', draftId: 'D18-COMPLETE', to: '2026-10-18', balanceReconciled: true,
    balanceDifference: 0, finalizedAt: '2026-10-23T10:00:00Z', statementSummary: {
      to: '2026-10-18', closingBalance: 165673.07, validated: true,
    }, reconciliationRows: [
      { bank: { transactionId: 'BTX-TRANSFER1' } }, { bank: { transactionId: 'BTX-TRANSFER2' } },
    ], carriedReconciliationRows: completeIds ? [{ bank: { transactionId: 'BTX-PAYTM18' } }] : [],
    combinedReconciliationIds: explicitCombined ? ['I18-PARTIAL'] : [] };
  return { reconciledThrough: '2026-10-18', imports: [partial, complete], lastReconciliation: {
    draftId: partial.draftId, through: '2026-10-18', closingBalance: 248209.82,
    balanceReconciled: true, balanceDifference: 0, at: '2026-10-23T18:00:00Z',
  } };
}

test('a validated complete same-day statement supersedes a partial close using physical bank coverage, not latest timestamp', () => {
  const book = sameDayPartialAndComplete();
  assert.deepEqual(verifiedReconciliationAnchor(book, { before: '2026-10-19' }), {
    cutoffDate: '2026-10-18', previousClosing: 165673.07,
    anchorSource: 'finalized_import', anchorRecordId: 'I18-COMPLETE',
  });
  book.imports.reverse();
  assert.equal(verifiedReconciliationAnchor(book, { before: '2026-10-19' }).previousClosing, 165673.07);
});

test('explicit combined-import lineage proves supersession when ordinary report rows omit previously reconciled bank evidence', () => {
  const book = sameDayPartialAndComplete({ explicitCombined: true, completeIds: false });
  assert.equal(verifiedReconciliationAnchor(book, { before: '2026-10-19' }).previousClosing, 165673.07);
});

test('persisted reconciliationBankTransactionIds supplies full physical coverage without duplicating report rows', () => {
  const book = sameDayPartialAndComplete({ completeIds: false });
  book.imports[1].reconciliationBankTransactionIds = ['BTX-PAYTM18', 'BTX-TRANSFER1', 'BTX-TRANSFER2'];
  assert.equal(verifiedReconciliationAnchor(book, { before: '2026-10-19' }).previousClosing, 165673.07);
});

test('explicit incomplete bank coverage cannot prove physical-ID supersession on either side', () => {
  for (const incompleteIndex of [0, 1]) {
    const book = sameDayPartialAndComplete();
    book.imports[incompleteIndex].reconciliationBankCoverageComplete = false;
    const blocked = verifiedReconciliationAnchor(book, { before: '2026-10-19' });
    assert.equal(blocked.previousClosing, null);
    assert.equal(blocked.anchorConflict.type, 'ambiguous_opening_anchor');
    book.imports[1].combinedReconciliationIds = ['I18-PARTIAL'];
    assert.equal(verifiedReconciliationAnchor(book, { before: '2026-10-19' }).previousClosing, 165673.07,
      'explicit combined-import lineage is a separate authoritative proof');
  }
});

test('different same-day closes without supersession evidence stay a genuine conflict even if the newer upload looks complete', () => {
  const book = sameDayPartialAndComplete({ completeIds: false });
  book.imports[1].finalizedAt = '2026-10-24T18:00:00Z';
  const result = verifiedReconciliationAnchor(book, { before: '2026-10-19' });
  assert.equal(result.previousClosing, null);
  assert.match(result.anchorConflict.message, /different closing balances/);
});

test('equal physical coverage with contradictory closes is not supersession', () => {
  const book = sameDayPartialAndComplete();
  book.imports[1].reconciliationRows = [];
  const result = verifiedReconciliationAnchor(book, { before: '2026-10-19' });
  assert.equal(result.previousClosing, null);
  assert.equal(result.anchorConflict.type, 'ambiguous_opening_anchor');
});

test('failed or unvalidated full-looking statements cannot supersede a verified partial period', () => {
  for (const mutation of [record => { record.closingBalanceDeferred = true; },
    record => { record.statementSummary.validated = false; },
    record => { delete record.balanceReconciled; }]) {
    const book = sameDayPartialAndComplete();
    mutation(book.imports[1]);
    const result = verifiedReconciliationAnchor(book, { before: '2026-10-19' });
    assert.equal(result.previousClosing, null);
    assert.equal(result.anchorConflict.type, 'ambiguous_opening_anchor');
  }
});

test('a conflicting last snapshot cannot overrule its own import evidence', () => {
  const book = sameDayPartialAndComplete();
  book.lastReconciliation.closingBalance = 987;
  const result = verifiedReconciliationAnchor(book, { before: '2026-10-19' });
  assert.equal(result.previousClosing, null);
  assert.match(result.anchorConflict.message, /own historical closing evidence/);
});

test('legacy exact-draft provenance is retained only when its last snapshot supplies a valid earlier date', () => {
  const book = { reconciledThrough: '2026-10-15', lastReconciliation: {
    draftId: 'LEGACY', through: '2026-10-15', closingBalance: 0,
  }, imports: [{ id: 'IL', draftId: 'LEGACY' }, { id: 'UNRELATED', draftId: 'OTHER' }] };
  const anchor = verifiedReconciliationAnchor(book, { before: '2026-10-18' });
  assert.equal(anchor.previousClosing, 0);
  const scoped = reconciliationCoverageBook(book, anchor);
  assert.deepEqual(scoped.imports.map(record => record.id), ['IL']);
  assert.equal(scoped.lastReconciliation.draftId, 'LEGACY');
});

test('missing, invalid and current/future-only evidence never substitutes the uploaded statement opening', () => {
  const book = axisBook();
  book.imports = book.imports.slice(0, 1);
  assert.equal(verifiedReconciliationAnchor(book, { before: '2026-10-18' }).previousClosing, null);
  assert.equal(verifiedReconciliationAnchor(book, { before: '2026-02-30' }).previousClosing, null);
  assert.equal(verifiedReconciliationAnchor({ reconciledThrough: '2026-10-18', imports: [{
    id: 'I15', to: '2026-10-15', statementSummary: { openingBalance: 234515.82 },
  }] }, { before: '2026-10-18' }).previousClosing, null);
});

test('earlier-anchor selection and coverage scoping do not mutate any original evidence', () => {
  const book = axisBook(), before = JSON.stringify(book);
  const anchor = verifiedReconciliationAnchor(book, { before: '2026-10-18' });
  const scoped = reconciliationCoverageBook(book, anchor);
  assert.notEqual(scoped, book);
  assert.notEqual(scoped.imports, book.imports);
  assert.equal(JSON.stringify(book), before);
  assert.deepEqual(verifiedReconciliationAnchor(book, { before: '2026-10-18' }), anchor);
});

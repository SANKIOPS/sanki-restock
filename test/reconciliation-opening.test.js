'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { calculateReconciliationOpening, finalizedMovementCoverageIds, pendingCarryForwardCandidates } = require('../modules/reconciliation-opening');

function octoberOptions(extra = {}) {
  return Object.assign({
    baseOpening: 0,
    previousClosing: 597.67,
    cutoffDate: '2026-10-06',
    statementFrom: '2026-10-07',
    statementOpening: 97.67,
    movements: [{ id: 'EX-00733/PAY-001', date: '2026-10-06', description: 'Maheshwari fresh mart', debit: 500, credit: 0 }],
  }, extra);
}

test('an existing late cutoff-day payment carries the finalized 597.67 to the actual 97.67 opening', () => {
  const opening = calculateReconciliationOpening(octoberOptions());
  assert.equal(opening.previousClosing, 597.67);
  assert.equal(opening.carryForwardNet, -500);
  assert.equal(opening.currentOpening, 97.67);
  assert.equal(opening.openingContinuityGap, 0);
  assert.equal(opening.continuityComparisonAvailable, true);
  assert.deepEqual(opening.carryForwardMovements.map(row => row.id), ['EX-00733/PAY-001']);
});

test('previously linked cutoff-day money is already in the prior closing and is not carried again', () => {
  const opening = calculateReconciliationOpening(octoberOptions({ previouslyLinkedIds: new Set(['EX-00733/PAY-001']) }));
  assert.equal(opening.currentOpening, 597.67);
  assert.equal(opening.carryForwardNet, 0);
  assert.equal(opening.openingContinuityGap, -500);
  assert.deepEqual(opening.carryForwardMovements, []);
});

test('a linked source payment excludes its consolidated reimbursement batch', () => {
  const opening = calculateReconciliationOpening(octoberOptions({
    movements: [{ id: 'BATCH-1', sourceIds: ['EX-1/REIMB-1', 'EX-2/REIMB-1'], date: '2026-10-06', debit: 500 }],
    previouslyLinkedIds: ['EX-2/REIMB-1'],
  }));
  assert.equal(opening.carryForwardNet, 0);
  assert.equal(opening.currentOpening, 597.67);
});

test('explicit finalized covered movements are not added twice even when legacy linked IDs are absent', () => {
  const opening = calculateReconciliationOpening(octoberOptions({ coveredMovementIds: ['EX-00733/PAY-001'] }));
  assert.equal(opening.currentOpening, 597.67);
  assert.equal(opening.carryForwardNet, 0);
});

test('current-period and pre-cutoff entries are not part of opening carry-forward', () => {
  const opening = calculateReconciliationOpening(octoberOptions({ movements: [
    { id: 'BEFORE', date: '2026-10-05', debit: 100 },
    { id: 'START', date: '2026-10-07', debit: 200 },
    { id: 'AFTER', date: '2026-10-08', credit: 700 },
  ] }));
  assert.equal(opening.currentOpening, 597.67);
  assert.deepEqual(opening.carryForwardMovements, []);
});

test('an overlapping statement retains the existing cutoff opening behavior', () => {
  const opening = calculateReconciliationOpening(octoberOptions({ statementFrom: '2026-10-05', statementOpening: 747.67 }));
  assert.equal(opening.currentOpening, 597.67);
  assert.equal(opening.carryForwardNet, 0);
  assert.equal(opening.continuityComparisonAvailable, false);
  assert.equal(opening.openingContinuityGap, null);
});

test('a statement beginning on the cutoff keeps cutoff movements in the comparison period, not opening', () => {
  const opening = calculateReconciliationOpening(octoberOptions({ statementFrom: '2026-10-06' }));
  assert.equal(opening.currentOpening, 597.67);
  assert.deepEqual(opening.carryForwardMovements, []);
  assert.equal(opening.continuityComparisonAvailable, false);
});

test('a multi-day gap carries every eligible existing movement with both inflows and outflows', () => {
  const opening = calculateReconciliationOpening(octoberOptions({ statementFrom: '2026-10-10', statementOpening: 170.22, movements: [
    { id: 'CUT', date: '2026-10-06', debit: 500 },
    { id: 'IN', date: '2026-10-07', credit: 100.55 },
    { id: 'OUT', date: '2026-10-09', debit: 28 },
    { id: 'NEW', date: '2026-10-10', debit: 900 },
  ] }));
  assert.equal(opening.carryForwardNet, -427.45);
  assert.equal(opening.currentOpening, 170.22);
  assert.equal(opening.openingContinuityGap, 0);
  assert.deepEqual(opening.carryForwardMovements.map(row => row.id), ['CUT', 'IN', 'OUT']);
});

test('eligible carry-forward is not discarded or adjusted just because a genuine bank opening gap remains', () => {
  const opening = calculateReconciliationOpening(octoberOptions({ statementOpening: 87.67 }));
  assert.equal(opening.carryForwardNet, -500);
  assert.equal(opening.currentOpening, 97.67);
  assert.equal(opening.openingContinuityGap, -10);
  assert.equal(opening.carryForwardMovements.length, 1);
});

test('previous automatic Axis fees and accounting-excluded rows never become fresh carry-forward', () => {
  const opening = calculateReconciliationOpening(octoberOptions({ statementFrom: '2026-10-09', movements: [
    { id: 'ADJ-FEE-CUT', date: '2026-10-06', debit: 5.9, automaticAxisTransferCharge: true },
    { id: 'ADJ-FEE-GAP', date: '2026-10-08', debit: 5.9, automaticAxisTransferCharge: true },
    { id: 'VOID', date: '2026-10-06', debit: 500, accountingExcluded: true },
    { id: 'EX-00733/PAY-001', date: '2026-10-06', debit: 500 },
  ] }));
  assert.equal(opening.currentOpening, 97.67);
  assert.deepEqual(opening.carryForwardMovements.map(row => row.id), ['EX-00733/PAY-001']);
});

test('duplicate IDs and payment aliases count the same ledger money once', () => {
  const opening = calculateReconciliationOpening(octoberOptions({ movements: [
    { id: 'EX-1/REIMB-1', date: '2026-10-06', debit: 200 },
    { id: 'BATCH-1', sourceIds: ['EX-1/REIMB-1', 'EX-2/REIMB-1'], date: '2026-10-06', debit: 500 },
    { id: 'BATCH-1', sourceIds: ['EX-1/REIMB-1', 'EX-2/REIMB-1'], date: '2026-10-06', debit: 500 },
  ] }));
  assert.equal(opening.currentOpening, 97.67);
  assert.equal(opening.carryForwardMovements.length, 1);
  assert.equal(opening.carryForwardMovements[0].id, 'BATCH-1');
});

test('historical/no-cutoff drafts preserve base opening and do not use the latest unrelated closing', () => {
  const opening = calculateReconciliationOpening(octoberOptions({ cutoffDate: '', baseOpening: 123.45 }));
  assert.equal(opening.currentOpening, 123.45);
  assert.equal(opening.previousClosing, null);
  assert.equal(opening.carryForwardNet, 0);
  assert.equal(opening.openingContinuityGap, null);
});

test('opening preview is deterministic and leaves ledger data and reconciliation status unchanged', () => {
  const options = octoberOptions();
  options.movements[0].sourceIds = ['source-1'];
  options.movements.forEach(movement => { Object.freeze(movement.sourceIds); Object.freeze(movement); });
  Object.freeze(options.movements);
  Object.freeze(options);
  const before = JSON.stringify(options);
  const first = calculateReconciliationOpening(options);
  const second = calculateReconciliationOpening(options);
  assert.deepEqual(first, second);
  assert.equal(JSON.stringify(options), before);
  assert.equal(first.carryForwardMovements[0].reconciled, undefined);
  assert.equal(first.carryForwardMovements[0].bankReconciliation, undefined);
});

test('missing bank opening evidence is not reported as a zero continuity gap', () => {
  const opening = calculateReconciliationOpening(octoberOptions({ statementOpening: undefined }));
  assert.equal(opening.currentOpening, 97.67);
  assert.equal(opening.continuityComparisonAvailable, false);
  assert.equal(opening.openingContinuityGap, null);
});

test('finalized coverage includes report ledger/source IDs and carried links without inventing compound IDs', () => {
  const book = { imports: [{ id: 'BST-INDUS', draftId: 'BRD-INDUS', reconciliationRows: [
    { linkedRecordIds: ['TR-1'], ledger: { id: 'EX-1/PAY-1, EX-2/PAY-1', sourceIds: ['SOURCE-1'],
      linkedEntries: [{ id: 'EX-1/PAY-1' }, { id: 'BATCH-2', sourceIds: ['EX-2/REIMB-1', 'EX-3/REIMB-1'] }] } },
  ], carriedReconciliationRows: [{ linkedRecordIds: ['CARRIED-1'], ledger: { id: 'CARRIED-2' } }] }] };
  const covered = finalizedMovementCoverageIds({}, book);
  assert.deepEqual(Array.from(covered).sort(), ['BATCH-2', 'CARRIED-1', 'CARRIED-2', 'EX-1/PAY-1',
    'EX-2/REIMB-1', 'EX-3/REIMB-1', 'SOURCE-1', 'TR-1']);
  assert.equal(covered.has('EX-2/PAY-1'), false, 'no splitting display text into invented financial identity');
});

test('raw rows generated during same-book finalization are covered even without a report ledger snapshot', () => {
  const book = { imports: [{ id: 'BST-INDUS', draftId: 'BRD-INDUS', reconciliationRows: [] }] };
  const store = {
    transfers: [{ id: 'TR-POSTED', reconciliationDraft: 'BRD-INDUS' },
      { id: 'TR-CORRECTED', bankReconciliationEvidence: { recordId: 'BST-INDUS' } }],
    receipts: [{ id: 'REC-POSTED', bankReconciliationEvidence: { draftId: 'BRD-INDUS' } }],
    adjustments: [{ id: 'ADJ-POSTED', reconciliationDraft: 'BRD-INDUS' }],
    vendorAdvances: [{ id: 'ADV-POSTED', paymentReference: 'ADV-PAYMENT-REF', reconciliationDraft: 'BRD-INDUS' }],
    paytmSettlements: [{ id: 'PTM-POSTED', reconciliationDraft: 'BRD-INDUS' }],
    bankTruthMovements: [{ id: 'BTR-POSTED', reconciliationDraft: 'BRD-INDUS' }],
    reconciliationExpenses: [{ id: 'BRE-1', adjustmentId: 'ADJ-SPLIT', settlementId: 'PTM-SPLIT', reconciliationDraft: 'BRD-INDUS' }],
    expenses: { 'EX-1': { id: 'EX-1', payments: [{ id: 'PAY-1', bankReconciliationDraft: 'BRD-INDUS' }],
      reimbursementPayments: [{ id: 'REIMB-1', batchId: 'REIMB-BATCH', bankReconciliationDraft: 'BRD-INDUS' }] } },
  };
  const covered = finalizedMovementCoverageIds(store, book);
  assert.deepEqual(Array.from(covered).sort(), ['ADJ-POSTED', 'ADJ-SPLIT', 'ADV-PAYMENT-REF', 'ADV-POSTED',
    'BTR-POSTED', 'EX-1/PAY-1', 'EX-1/REIMB-1', 'PTM-POSTED', 'PTM-SPLIT', 'REC-POSTED', 'REIMB-BATCH',
    'TR-CORRECTED', 'TR-POSTED']);
  const opening = calculateReconciliationOpening(octoberOptions({ movements: [
    { id: 'TR-POSTED', date: '2026-10-06', credit: 7000 },
    { id: 'EX-00733/PAY-001', date: '2026-10-06', debit: 500 },
  ], coveredMovementIds: covered }));
  assert.equal(opening.currentOpening, 97.67, 'previous finalized incoming money is not counted again');
});

test('another account book or a pending draft cannot mark a movement covered in this account', () => {
  const book = { imports: [{ id: 'BST-INDUS', draftId: 'BRD-INDUS' }] };
  const store = {
    transfers: [{ id: 'TR-AXIS', reconciliationDraft: 'BRD-AXIS' }, { id: 'TR-PENDING', reconciliationDraft: 'BRD-PENDING' }],
    receipts: [{ id: 'REC-AXIS', bankReconciliationEvidence: { recordId: 'BST-AXIS' } }],
    bankDateOverrides: { 'EX-AXIS/PAY-1': { reconciliationDraft: 'BRD-AXIS' } },
    bankReconciliationLinks: { 'GROUP-PENDING': { reconciliationDraft: 'BRD-PENDING' } },
    bankStatements: { Axis: { imports: [{ id: 'BST-AXIS', draftId: 'BRD-AXIS' }] } },
  };
  assert.deepEqual(Array.from(finalizedMovementCoverageIds(store, book)), []);
});

test('coverage uses finalized same-book overrides and grouped links, not provisional markers', () => {
  const book = { imports: [{ id: 'BST-INDUS', draftId: 'BRD-INDUS' }] };
  const store = {
    bankDateOverrides: { 'EX-1/PAY-1': { reconciliationDraft: 'BRD-INDUS' },
      'EX-2/PAY-1': { reconciliationDraft: 'BST-INDUS' },
      'EX-3/PAY-1': { reconciliationDraft: 'BRD-INDUS', provisional: true } },
    bankReconciliationLinks: { 'GROUP-1': { reconciliationDraft: 'BRD-INDUS' },
      'GROUP-2': { reconciliationDraft: 'BRD-INDUS', provisional: true } },
  };
  assert.deepEqual(Array.from(finalizedMovementCoverageIds(store, book)).sort(), ['EX-1/PAY-1', 'EX-2/PAY-1', 'GROUP-1']);
});

test('combined finalized imports and latest finalized draft provenance retain generated-entry coverage', () => {
  const book = { imports: [{ id: 'BST-COMBINED', draftId: 'BRD-COMBINED', combinedReconciliationIds: ['BST-OLD'] }],
    lastReconciliation: { draftId: 'BRD-LATEST' } };
  const store = { transfers: [{ id: 'TR-OLD', bankReconciliationEvidence: { recordId: 'BST-OLD' } },
    { id: 'TR-LATEST', reconciliationDraft: 'BRD-LATEST' },
    { id: 'TR-UNRELATED', reconciliationDraft: 'BRD-OTHER' }] };
  assert.deepEqual(Array.from(finalizedMovementCoverageIds(store, book)).sort(), ['TR-LATEST', 'TR-OLD']);
});

test('an expense reconciliation source does not automatically cover later manual payments on that expense', () => {
  const book = { imports: [{ id: 'BST-INDUS', draftId: 'BRD-INDUS' }] };
  const store = { expenses: { 'EX-1': { id: 'EX-1', reconciliationSource: { draftId: 'BRD-INDUS' },
    payments: [{ id: 'PAY-1', bankReconciliationDraft: 'BRD-INDUS' }, { id: 'PAY-2' }] } } };
  const covered = finalizedMovementCoverageIds(store, book);
  assert.equal(covered.has('EX-1/PAY-1'), true);
  assert.equal(covered.has('EX-1/PAY-2'), false);
});

test('finalized coverage extraction does not mutate the store or statement book', () => {
  const store = { transfers: [{ id: 'TR-1', reconciliationDraft: 'BRD-1' }] };
  const book = { imports: [{ id: 'BST-1', draftId: 'BRD-1', reconciliationRows: [{ ledger: { id: 'EX-1/PAY-1' } }] }] };
  const before = JSON.stringify({ store, book });
  const first = finalizedMovementCoverageIds(store, book);
  const second = finalizedMovementCoverageIds(store, book);
  assert.deepEqual(first, second);
  assert.equal(JSON.stringify({ store, book }), before);
});

function carriedBook(extra = {}) {
  return Object.assign({ imports: [{ id: 'BST-CARRIED', draftId: 'BRD-CARRIED', from: '2026-10-07', to: '2026-10-07',
    carryForwardMovements: [{ id: 'EX-00733/PAY-001', date: '2026-10-06', debit: 500, credit: 0 }] }] }, extra);
}

test('an unconfirmed carried identity remains an exact later candidate outside ordinary date windows', () => {
  const movement = { id: 'EX-00733/PAY-001', date: '2026-10-06', debit: 500, credit: 0, description: 'Maheshwari fresh mart' };
  const pending = pendingCarryForwardCandidates({}, carriedBook(), [movement]);
  assert.equal(pending.candidates.length, 1);
  assert.equal(pending.candidates[0].priorPeriodCarryForward, true);
  assert.equal(pending.candidates[0].carryForwardOriginalDate, '2026-10-06');
  assert.deepEqual(pending.candidates[0].carryForwardSourcePeriods, [{ reconciliationId: 'BST-CARRIED',
    from: '2026-10-07', to: '2026-10-07', ledgerDate: '2026-10-06' }]);
  assert.deepEqual(pending.conflicts, []);
  assert.equal(movement.reconciled, undefined);
  assert.equal(movement.priorPeriodCarryForward, undefined);
});

test('same-book final bank evidence removes a pending carried candidate, but other-book evidence does not', () => {
  const movement = { id: 'EX-00733/PAY-001', date: '2026-10-06', debit: 500 };
  const book = carriedBook();
  const outsideStore = { bankDateOverrides: { [movement.id]: { reconciliationDraft: 'BRD-AXIS' } } };
  assert.equal(pendingCarryForwardCandidates(outsideStore, book, [movement]).candidates.length, 1);
  book.imports.push({ id: 'BST-CONFIRMED', draftId: 'BRD-CONFIRMED', reconciliationRows: [{ linkedRecordIds: [movement.id] }] });
  assert.equal(pendingCarryForwardCandidates(outsideStore, book, [movement]).candidates.length, 0);
});

test('repeated carried snapshots provide one candidate and preserve each source period', () => {
  const book = carriedBook();
  book.imports.push({ id: 'BST-CARRIED-2', from: '2026-10-08', to: '2026-10-08', carryForwardMovements: book.imports[0].carryForwardMovements });
  const pending = pendingCarryForwardCandidates({}, book, [{ id: 'EX-00733/PAY-001', date: '2026-10-06', debit: 500 }]);
  assert.equal(pending.candidates.length, 1);
  assert.equal(pending.candidates[0].carryForwardSourcePeriods.length, 2);
});

test('a changed historical carry amount is a conflict, not a replacement bank-match candidate', () => {
  const pending = pendingCarryForwardCandidates({}, carriedBook(), [{ id: 'EX-00733/PAY-001', date: '2026-10-06', debit: 700 }]);
  assert.deepEqual(pending.candidates, []);
  assert.equal(pending.conflicts[0].type, 'changed_carry_forward_amount');
});

test('a deleted historical carry entry is not recreated from its finalized snapshot', () => {
  const pending = pendingCarryForwardCandidates({}, carriedBook(), []);
  assert.deepEqual(pending.candidates, []);
  assert.equal(pending.conflicts[0].type, 'missing_carry_forward_movement');
});

test('multiple source-identity matches require review instead of an invented aggregate', () => {
  const book = carriedBook({ imports: [{ id: 'BST-BATCH', carryForwardMovements: [{ id: 'OLD-BATCH',
    date: '2026-10-06', debit: 500, sourceIds: ['EX-1/REIMB-1', 'EX-2/REIMB-1'] }] }] });
  const pending = pendingCarryForwardCandidates({}, book, [
    { id: 'EX-1/REIMB-1', date: '2026-10-06', debit: 200 },
    { id: 'EX-2/REIMB-1', date: '2026-10-06', debit: 300 },
  ]);
  assert.deepEqual(pending.candidates, []);
  assert.equal(pending.conflicts[0].type, 'ambiguous_carry_forward_identity');
});

test('pending-candidate calculation does not change ledger dates or count money again', () => {
  const book = carriedBook();
  const movements = [{ id: 'EX-00733/PAY-001', date: '2026-10-06', debit: 500 }];
  const before = JSON.stringify({ book, movements });
  const pending = pendingCarryForwardCandidates({}, book, movements);
  const futureOpening = calculateReconciliationOpening({ previousClosing: 361.67, cutoffDate: '2026-10-07',
    statementFrom: '2026-10-20', statementOpening: 361.67, movements: pending.candidates });
  assert.equal(futureOpening.currentOpening, 361.67);
  assert.equal(futureOpening.carryForwardNet, 0);
  assert.equal(JSON.stringify({ book, movements }), before);
});

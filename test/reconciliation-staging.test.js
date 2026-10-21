'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { postedBankRowMovements, pendingReconciliationAmount, postedReconciliationConflicts, findTransferCreationConflicts } = require('../modules/reconciliation-staging');

const account = 'Axis Bank 3448', destination = 'IndusInd Bank 7883', day = '2026-10-18';
const bank = { date: day, debit: 10449.9, credit: 0, reference: '629100000001', description: 'IMPS transfer', balance: 165673.07 };
const draft = { id: 'BRD-AXIS-OCT18', nature: 'SANKI', account, transactions: [bank], resolutions: {} };
const transfer = { id: 'TR-00181', date: day, amount: 10444, nature: 'SANKI', fromNature: 'SANKI', toNature: 'PERSONAL', fromAccount: account, toAccount: destination, reconciliationDraft: draft.id, bankRowId: 'bank-0' };
const charge = { id: 'ADJ-FEE', date: day, amount: -5.9, nature: 'SANKI', account, reconciliationDraft: draft.id, bankRowId: 'bank-0', automaticAxisTransferCharge: true };
const movements = [{ id: transfer.id, date: day, debit: 10444, credit: 0 }, { id: charge.id, date: day, debit: 5.9, credit: 0 }];
const store = () => ({ transfers: [{ ...transfer }], adjustments: [{ ...charge }] });
const row = (action = 'create_internal_transfer') => ({ id: 'bank-0', status: 'resolved', bank: { ...bank }, resolution: { action, principalAmount: 10444, chargeAmount: 5.9 } });
const posted = (s = store(), list = movements, current = draft, currentBank = bank) => postedBankRowMovements({ store: s, draft: current, rowId: 'bank-0', bank: currentBank, movements: list });
const candidate = (s = {}, extra = {}) => findTransferCreationConflicts({ store: s, draft, rowId: 'bank-0', bank, fromAccount: account, toAccount: destination, fromNature: 'SANKI', toNature: 'PERSONAL', principalAmount: 10444, ...extra });

test('the screenshot retains both staged transfers and exactly one automatic fee', () => {
  const rows = [
    { id: 'bank-1', status: 'resolved', bank: { date: day, debit: 71123.9, credit: 0 }, resolution: { action: 'create_internal_transfer' } },
    { id: 'bank-2', status: 'resolved', bank: { date: day, debit: 962.95, credit: 0 }, resolution: { action: 'create_internal_transfer' } },
    { id: 'bank-3', status: 'matched', bank, axisTransferCharge: true, chargeAmount: 5.9, app: movements[0] }
  ];
  const staged = Math.round(rows.reduce((sum, current) => sum + pendingReconciliationAmount({ row: current }), 0) * 100) / 100;
  assert.equal(staged, -72092.75);
  assert.equal(237765.82 + staged, 165673.07);
});

test('a posted transfer principal leaves its separately unposted fee pending', () => {
  assert.equal(pendingReconciliationAmount({ row: row(), postedMovements: posted(store(), [movements[0]]) }), -5.9);
  assert.equal(pendingReconciliationAmount({ row: row(), postedMovements: posted(store(), [movements[1]]) }), -10444);
  assert.equal(pendingReconciliationAmount({ row: row(), postedMovements: posted() }), 0);
});

test('a posted automatic fee is never staged again on repeated preview or finalization calculation', () => {
  const automatic = { id: 'bank-0', status: 'matched', bank, axisTransferCharge: true, chargeAmount: 5.9 };
  for (let repeat = 0; repeat < 3; repeat++) assert.equal(pendingReconciliationAmount({ row: automatic, postedMovements: posted() }), 0);
  assert.equal(pendingReconciliationAmount({ row: automatic, postedMovements: posted(store(), [movements[0]]) }), -5.9);
  assert.equal(pendingReconciliationAmount({ row: { ...automatic, status: 'already_reconciled' } }), 0);
  assert.equal(pendingReconciliationAmount({ row: automatic, postedMovements: [{ ...movements[0], postingKind: 'adjustment' }] }), -5.9, 'a posted principal adjustment is not the fee');
});

test('equal unrelated transfer and fee amounts do not suppress pending amounts', () => {
  const unrelated = { transfers: [{ ...transfer, reconciliationDraft: 'BRD-OTHER' }], adjustments: [{ ...charge, bankRowId: 'bank-1' }] };
  assert.deepEqual(posted(unrelated), []);
  assert.equal(pendingReconciliationAmount({ row: row(), postedMovements: posted(unrelated) }), -10449.9);
});

test('a staged projection is not an actual posted movement and excluded entries are not counted', () => {
  assert.deepEqual(posted(store(), movements.map(movement => ({ ...movement, pendingPosting: true }))), []);
  const s = store(); s.transfers[0].accountingExcluded = true; s.adjustments[0].accountingExcluded = true;
  assert.deepEqual(posted(s), []);
});

function finalized(s = store(), bankRow = bank) {
  const tx = { ...bankRow, id: 'BTX-EXACT', reconciliationDraftId: draft.id, reconciliationDraftRowIndex: 0 };
  s.bankStatements = { [account]: { transactions: { exact: tx }, imports: [{ id: 'BST-EXACT', draftId: draft.id, finalizedAt: day + 'T12:00:00Z', reconciliationRows: [{ id: 'bank-0', bank: { ...bankRow, transactionId: tx.id }, linkedRecordIds: [transfer.id, charge.id] }] }] } };
  return s;
}

test('an overlapping same-cutoff statement uses exact physical bank identity for principal and fee', () => {
  const s = finalized(), next = { ...draft, id: 'BRD-OVERLAPPING' };
  assert.equal(posted(s, movements, next).length, 2);
  assert.equal(pendingReconciliationAmount({ row: row(), postedMovements: posted(s, movements, next) }), 0);
  const different = { ...bank, reference: '629100000002', balance: 155223.17 };
  assert.deepEqual(posted(s, movements, next, different), []);
});

test('ambiguous equal physical rows cannot prove cross-draft posting identity', () => {
  const s = finalized(), book = s.bankStatements[account];
  book.transactions.duplicate = { ...book.transactions.exact, id: 'BTX-DUPLICATE' };
  assert.deepEqual(posted(s, movements, { ...draft, id: 'BRD-OVERLAPPING' }), []);
});

test('a legacy split fee is located only through its exact bank transaction companion', () => {
  const s = finalized(); delete s.adjustments[0].bankRowId;
  s.bankStatements[account].imports[0].reconciliationRows[0].linkedRecordIds = [transfer.id];
  s.reconciliationExpenses = [{ adjustmentId: charge.id, account, bankTransactionId: 'BTX-EXACT' }];
  const actual = posted(s, movements, { ...draft, id: 'BRD-OVERLAPPING' });
  assert.equal(actual.length, 2);
  assert.equal(pendingReconciliationAmount({ row: row('split_allocation'), postedMovements: actual }), 0);
  s.reconciliationExpenses[0].bankTransactionId = 'BTX-UNRELATED';
  assert.equal(posted(s, movements, { ...draft, id: 'BRD-OVERLAPPING' }).length, 1);
});

test('duplicated posted records remain ledger discrepancies rather than inventing pending credits', () => {
  const actual = posted();
  assert.equal(pendingReconciliationAmount({ row: row(), postedMovements: actual.concat(actual) }), 0);
});

test('a known partial fee leaves only its remaining amount pending', () => {
  const s = store(); s.adjustments[0].amount = -2.95;
  const partial = posted(s, [movements[0], { ...movements[1], debit: 2.95 }]);
  const automatic = { id: 'bank-0', status: 'matched', bank, axisTransferCharge: true, chargeAmount: 5.9 };
  assert.equal(pendingReconciliationAmount({ row: automatic, postedMovements: partial }), -2.95);
  assert.equal(pendingReconciliationAmount({ row: row('split_allocation'), postedMovements: partial }), -2.95);
  assert.equal(pendingReconciliationAmount({ row: automatic, postedMovements: [{ id: 'UNCLASSIFIED', postingKind: 'adjustment', debit: 2.95 }] }), -5.9, 'an unclassified partial adjustment is not silently classified as a fee');
  assert.deepEqual(postedReconciliationConflicts({ row: automatic, postedMovements: partial }), []);
});

test('legacy known partial fee can be proved by its matching Bank Charges spending record', () => {
  const s = store(); s.adjustments[0].amount = -2.95; delete s.adjustments[0].automaticAxisTransferCharge;
  s.reconciliationExpenses = [{ adjustmentId: charge.id, amount: 2.95, category: 'BANK CHARGES' }];
  const partial = posted(s, [{ ...movements[1], debit: 2.95 }]);
  assert.equal(pendingReconciliationAmount({ row: row('split_allocation'), postedMovements: partial }), -2.95);
});

test('two fee components totaling the known fee do not create a false duplicate blocker', () => {
  const partial = { id: 'FEE-1', postingKind: 'adjustment', bankChargeComponent: true, debit: 2.95 };
  const list = [partial, { ...partial, id: 'FEE-2' }];
  assert.equal(pendingReconciliationAmount({ row: row('split_allocation'), postedMovements: list }), 0);
  assert.deepEqual(postedReconciliationConflicts({ row: row('split_allocation'), postedMovements: list }), []);
});

test('duplicate posted principals and excess fees are explicit blockers despite offsetting balances', () => {
  const actual = posted(), duplicatePrincipal = { ...actual[0], id: 'TR-DUPLICATE' }, duplicateFee = { ...actual[1], id: 'ADJ-DUPLICATE' };
  const conflicts = postedReconciliationConflicts({ row: row(), postedMovements: actual.concat(duplicatePrincipal, duplicateFee) });
  assert.deepEqual(conflicts.map(conflict => conflict.type), ['duplicate_posted_principal', 'excess_posted_bank_fee', 'incomplete_posted_transfer_fee', 'excess_posted_bank_amount']);
  assert.ok(conflicts.every(conflict => conflict.bankRowIds[0] === 'bank-0'));
  assert.deepEqual(postedReconciliationConflicts({ row: { ...row(), resolution: { action: 'link_multiple_existing' } }, postedMovements: actual.concat(duplicatePrincipal) }), [], 'multiple explicitly linked transfers are not a single-principal action');
});

test('a gross split adjustment is staged once and its spending split does not count again', () => {
  const split = row('create_split_adjustment');
  assert.equal(pendingReconciliationAmount({ row: split }), -10449.9);
  const s = { adjustments: [{ ...charge, amount: -10449.9, automaticAxisTransferCharge: false }], reconciliationExpenses: [{ adjustmentId: charge.id, amount: 10444, category: 'OTHER EXPENSE' }, { adjustmentId: charge.id, amount: 5.9, category: 'BANK CHARGES' }] };
  const actual = posted(s, [{ ...movements[1], debit: 10449.9 }]);
  assert.equal(actual[0].bankChargeComponent, false);
  assert.equal(pendingReconciliationAmount({ row: split, postedMovements: actual }), 0);
  assert.deepEqual(postedReconciliationConflicts({ row: split, postedMovements: actual }), []);
  assert.equal(postedReconciliationConflicts({ row: split, postedMovements: actual.concat({ ...actual[0], id: 'ADJ-DUPLICATE' }) })[0].type, 'excess_posted_bank_amount');
});

test('partial or changed posted principal blocks finalization and an internal transfer requires one full fee', () => {
  const actual = posted();
  for (const changed of [{ ...actual[0], debit: 10000 }, { ...actual[0], debit: 0, credit: 10444 }]) {
    assert.ok(postedReconciliationConflicts({ row: row(), postedMovements: [changed] }).some(conflict => conflict.type === 'mismatched_posted_principal'));
  }
  assert.ok(postedReconciliationConflicts({ row: row(), postedMovements: [actual[0], { ...actual[1], debit: 2.95 }] }).some(conflict => conflict.type === 'incomplete_posted_transfer_fee'));
  assert.deepEqual(postedReconciliationConflicts({ row: row(), postedMovements: actual }), []);
  assert.deepEqual(postedReconciliationConflicts({ row: row(), postedMovements: [actual[0]] }), []);
});

test('internal transfer split blocks two partial charges even when their sum equals the full fee', () => {
  const actual = posted(), partial = { ...actual[1], debit: 2.95 };
  const conflicts = postedReconciliationConflicts({ row: row(), postedMovements: [actual[0], partial, { ...partial, id: 'FEE-SECOND' }] });
  assert.deepEqual(conflicts.map(conflict => conflict.type), ['incomplete_posted_transfer_fee']);
});

test('a fee owned by another transfer cannot be silently reassigned', () => {
  const actual = posted(); actual[1].record = { ...actual[1].record, transferChargeFor: 'TR-OTHER' };
  assert.ok(postedReconciliationConflicts({ row: row(), postedMovements: actual }).some(conflict => conflict.type === 'conflicting_posted_transfer_fee'));
  actual[1].record.transferChargeFor = transfer.id;
  assert.deepEqual(postedReconciliationConflicts({ row: row(), postedMovements: actual }), []);
});

test('partial, reversed or net-zero gross adjustments require correction before finalization', () => {
  for (const action of ['create_adjustment', 'create_split_adjustment']) {
    for (const list of [
      [{ id: 'ADJ-PARTIAL', postingKind: 'adjustment', debit: 10000, credit: 0 }],
      [{ id: 'ADJ-REVERSED', postingKind: 'adjustment', debit: 0, credit: 10449.9 }],
      [{ id: 'ADJ-DEBIT', postingKind: 'adjustment', debit: 10449.9 }, { id: 'ADJ-CREDIT', postingKind: 'adjustment', credit: 10449.9 }]
    ]) assert.ok(postedReconciliationConflicts({ row: row(action), postedMovements: list }).some(conflict => conflict.type === 'mismatched_posted_adjustment'));
  }
});

test('an existing transfer principal blocks another creation including recognized Axis fee differences', () => {
  assert.equal(candidate({ transfers: [{ ...transfer }] }).length, 1);
  assert.equal(candidate({ transfers: [{ ...transfer, amount: 10446.95 }] }, { principalAmount: 10449.9 }).length, 1);
  assert.equal(candidate({ transfers: [{ ...transfer, amount: 10439 }] }).length, 0);
});

test('different amounts, dates, accounts and directions are separate transfer candidates', () => {
  for (const change of [{ amount: 10430 }, { date: '2026-10-19' }, { fromAccount: 'Axis Bank 3645' }, { fromAccount: destination, toAccount: account }, { fromNature: 'SAMAST' }]) {
    assert.equal(candidate({ transfers: [{ ...transfer, ...change }] }).length, 0, JSON.stringify(change));
  }
});

test('same amount repeated transfer with a different verified source-bank reference is permitted', () => {
  const s = finalized();
  const nextBank = { ...bank, reference: '629100000002', balance: 155223.17 };
  assert.deepEqual(candidate(s, { draft: { ...draft, id: 'BRD-NEXT' }, bank: nextBank }), []);
  assert.equal(candidate(s, { draft: { ...draft, id: 'BRD-NEXT' } }).length, 1);
});

test('raw conflicting references and opposite-bank references do not prove a distinct payment', () => {
  const s = { transfers: [{ ...transfer, bankReference: '629100000002', reconciliationDraft: 'BRD-OLD' }] };
  assert.equal(candidate(s).length, 1);
  s.bankStatements = { [destination]: { imports: [{ finalizedAt: day, reconciliationRows: [{ bank: { ...bank, debit: 0, credit: 10444, reference: '629100000002' }, linkedRecordIds: [transfer.id] }] }] } };
  assert.equal(candidate(s).length, 1);
});

test('contradictory or repeated finalized bank evidence cannot permit a new equal transfer', () => {
  for (const corrupt of [
    book => { book.transactions.exact.reference = '629100000003'; },
    book => { book.transactions.duplicate = { ...book.transactions.exact, id: 'BTX-DUPLICATE', balance: 1000 }; },
    book => { const copy = JSON.parse(JSON.stringify(book.imports[0])); copy.id = 'BST-CONTRADICTORY'; copy.reconciliationRows[0].bank.reference = '629100000004'; book.imports.push(copy); }
  ]) {
    const s = finalized(); corrupt(s.bankStatements[account]);
    assert.equal(candidate(s, { draft: { ...draft, id: 'BRD-NEXT' }, bank: { ...bank, reference: '629100000002', balance: 155223.17 } }).length, 1);
  }
});

test('the same row already posted is identified explicitly for an idempotent caller', () => {
  const actual = candidate(store());
  assert.equal(actual[0].sameOrigin, true);
  assert.equal(actual[0].id, transfer.id);
});

test('equal pending decisions with the same bank identity block a second transfer', () => {
  const pending = { ...draft, id: 'BRD-OTHER', resolutions: { 'bank-0': { action: 'create_internal_transfer', otherNature: 'PERSONAL', otherAccount: destination, postingNature: 'SANKI', principalAmount: 10444, chargeAmount: 5.9 } } };
  const s = { bankReconciliationDrafts: { [pending.id]: pending } };
  const actual = candidate(s); assert.equal(actual.length, 1); assert.equal(actual[0].type, 'pending');
  pending.transactions = [{ ...bank, reference: '629100000002', balance: 155223.17 }];
  assert.deepEqual(candidate(s), []);
});

test('the helper leaves managed PERSONAL duplicate policy unchanged and never mutates sources', () => {
  const s = finalized(), before = JSON.stringify(s);
  assert.deepEqual(candidate(s, { fromNature: 'PERSONAL', toNature: 'PERSONAL' }), []);
  candidate(s); posted(s); pendingReconciliationAmount({ row: row(), postedMovements: posted(s) });
  assert.equal(JSON.stringify(s), before);
});

'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fees = require('../modules/tenant-reconciliation-fees');
const clone = value => JSON.parse(JSON.stringify(value));
function fixture(kind = 'dealer_commission', direction = 'payment', nature = 'SAMAST') {
  const bank = { date: '2098-06-02', description: 'Synthetic tenant fee', reference: 'SYNTHETIC-REF', debit: direction === 'payment' ? 1234.56 : 0, credit: direction === 'receipt' ? 1234.56 : 0, balance: 2000 };
  const draft = { id: 'BRD-SYNTHETIC-FEE', account: fees.ACCOUNT, nature: 'SAMAST', transactions: [bank], resolutions: {}, transactionOwnership: { 'bank-0': { nature, reason: 'Existing reviewed ownership' } } };
  const register = { revision: 7, tenancies: [{ id: 'TEN-A', tenant: 'Synthetic tenant', floor: 'First', property: 'Synthetic property', securityOpening: 10000 }, { id: 'TEN-B', tenant: 'Other tenant' }], invoices: [{ id: 'RENT-A', tenancyId: 'TEN-A', bankDue: 500 }], deposits: [{ id: 'SEC-A', amount: 100 }], credits: [{ id: 'CREDIT-A', amount: 20 }], allocations: [{ id: 'RA-A', amount: 10 }], activity: [] };
  const store = { seq: 4, receiptSeq: 5, expenses: { 'EX-KEEP': { amount: 17 } }, receipts: [{ id: 'REC-KEEP', amount: 29 }], transfers: [{ id: 'TR-KEEP', amount: 12 }], rentals: register, bankReconciliationDrafts: { [draft.id]: draft }, bankStatements: {}, auditLog: [{ action: 'KEEP' }], openingBalances: { [fees.ACCOUNT]: 900 } };
  return { store, context: { draft, row: { id: 'bank-0', status: 'missing_in_app', bank, entityTag: { nature } }, register, body: { draftId: draft.id, rowId: 'bank-0', tenancyId: 'TEN-A', kind, note: 'Verified synthetic tenant fee' }, nature, actor: 'owner-test', now: '2098-06-03T10:00:00.000Z', canApproveExpense: true } };
}
for (const kind of Object.keys(fees.SPECS)) for (const direction of ['payment', 'receipt']) for (const nature of ['SAMAST', 'PERSONAL']) {
  test(kind + ' ' + direction + ' ' + nature + ' keeps authoritative money and tenant link separate from rent/security', () => {
    const { store, context } = fixture(kind, direction, nature), before = clone(store), result = fees.record(store, context);
    assert.equal(result.fee.tenancyId, 'TEN-A'); assert.equal(result.fee.kind, kind); assert.equal(result.direction, direction); assert.equal(result.fee.amount, 1234.56); assert.equal(result.fee.nature, nature);
    const source = result.expense || result.receipt;
    assert.equal(source.date, context.row.bank.date); assert.equal(source.account, fees.ACCOUNT); assert.equal(source.tenancyId, 'TEN-A'); assert.equal(source.rentalFeeId, result.fee.id); assert.equal(source.rentalKind, kind);
    assert.equal(source.bankReconciliationEvidence.bankSignature, fees.bankIdentity(context.row.bank));
    assert.equal(store.rentals.revision, 8); assert.equal(store.rentals.fees.length, 1);
    for (const key of ['tenancies', 'invoices', 'deposits', 'credits', 'allocations']) assert.deepEqual(store.rentals[key], before.rentals[key]);
    for (const key of ['transfers', 'openingBalances', 'bankStatements', 'auditLog']) assert.deepEqual(store[key], before[key]);
    assert.deepEqual(store.expenses['EX-KEEP'], before.expenses['EX-KEEP']); assert.deepEqual(store.receipts.find(x => x.id === 'REC-KEEP'), before.receipts[0]);
    assert.deepEqual(context.draft.transactionOwnership, before.bankReconciliationDrafts[context.draft.id].transactionOwnership);
    if (direction === 'payment') {
      assert.equal(result.expense.status, 'paid'); assert.equal(result.expense.approvedBy, 'owner-test'); assert.equal(result.expense.payments[0].amount, 1234.56); assert.equal(result.expense.payments[0].tenancyId, 'TEN-A'); assert.equal(result.expense.ledger, fees.SPECS[kind].category);
    } else assert.equal(result.receipt.receiptType, fees.SPECS[kind].receiptType);
    const saved = JSON.stringify(store); assert.throws(() => fees.record(store, context), /already has/); assert.equal(JSON.stringify(store), saved);
    const undone = fees.undo(store, { draft: context.draft, rowId: 'bank-0', actor: 'owner-test', now: '2098-06-03T11:00:00.000Z', reason: 'Discard synthetic review' });
    assert.equal(undone.before.source.id, source.id); assert.equal(store.rentals.fees[0].reversedBy, 'owner-test'); assert.equal(store.rentals.fees[0].reversalReason, 'Discard synthetic review');
    assert.equal(context.draft.resolutions['bank-0'], undefined); assert.equal(store.expenses[source.id], undefined); assert.equal(store.receipts.some(x => x.id === source.id), false);
    assert.deepEqual(store.expenses['EX-KEEP'], before.expenses['EX-KEEP']); assert.deepEqual(store.receipts, before.receipts);
    assert.throws(() => fees.undo(store, { draft: context.draft, rowId: 'bank-0' }), /saved tenant fee/);
  });
}
const blockedCases = [
  ['other bank', ({ context }) => { context.draft.account = 'Axis Bank 3448'; }, /only in IndusInd/],
  ['missing tenant', ({ context }) => { context.body.tenancyId = ''; }, /tenant/],
  ['ambiguous tenant', ({ context }) => { context.register.tenancies.push(clone(context.register.tenancies[0])); }, /tenant/],
  ['caller amount override', ({ context }) => { context.body.amount = 1; }, /amount, date/],
  ['caller date override', ({ context }) => { context.body.date = '2098-01-01'; }, /amount, date/],
  ['missing expense authority', ({ context }) => { context.canApproveExpense = false; }, /approval and payment authority/],
  ['unknown entity', ({ context }) => { context.nature = ''; }, /Tag this 7883/],
  ['other entity', ({ context }) => { context.nature = 'SANKI'; }, /Tag this 7883/],
  ['source conflict', ({ context }) => { context.row.entityTag.sourceConflict = true; }, /conflicting source/],
  ['ambiguous source', ({ context }) => { context.row.entityTag.source = 'ambiguous_candidates'; }, /conflicting source/],
  ['bank reversal', ({ context }) => { context.row.reversalReviewRequired = true; }, /bank reversal/],
  ['reversal candidate', ({ context }) => { context.row.reversalPairCandidate = {}; }, /bank reversal/],
  ['matched row', ({ context }) => { context.row.status = 'matched'; }, /unmatched/],
  ['already resolved row', ({ context }) => { context.row.resolution = { action: 'exclude' }; }, /unmatched/],
  ['mixed direction', ({ context }) => { context.row.bank.credit = 1; }, /exactly one/],
  ['negative amount', ({ context }) => { context.row.bank.debit = -1; }, /amount is invalid/],
  ['fractional paise', ({ context }) => { context.row.bank.debit = 1.001; }, /amount is invalid/],
  ['boolean amount', ({ context }) => { context.row.bank.debit = true; }, /amount is invalid/],
  ['prototype kind', ({ context }) => { context.body.kind = '__proto__'; }, /Choose dealer commission/],
  ['empty remark', ({ context }) => { context.body.note = ''; }, /remark/],
  ['pending Owner approval', ({ store, context }) => { store.bankReconciliationApprovals = { A: { draftId: context.draft.id, status: 'pending' } }; }, /awaiting Owner approval/],
  ['other overlapping pending approval', ({ store }) => { store.bankReconciliationApprovals = { A: { draftId: 'OTHER', account: fees.ACCOUNT, status: 'pending', from: '2098-06-01', to: '2098-06-10' } }; }, /awaiting Owner approval/],
  ['retained statement recovery lock', ({ context }) => { context.draft.originalStatementPeriod = {}; }, /locked/],
  ['finalized reconstruction', ({ context }) => { context.draft.reconstructedFromFinalized = {}; }, /finalized history/],
  ['finalized bank period', ({ store }) => { store.bankStatements[fees.ACCOUNT] = { reconciledThrough: '2098-06-03' }; }, /finalized bank period/],
  ['existing receipt source', ({ store, context }) => { store.receipts.push({ id: 'REC-EXISTING', bankReconciliationEvidence: { draftId: context.draft.id, rowId: 'bank-0' } }); }, /already has/],
  ['existing expense source', ({ store, context }) => { store.expenses.EXISTING = { reconciliationSource: { draftId: context.draft.id, rowId: 'bank-0' } }; }, /already has/],
  ['malformed fee register', ({ context }) => { context.register.fees = {}; }, /cannot be verified/]
];
for (const [name, change, message] of blockedCases) test('fee creation rejects ' + name + ' without changing any data', () => {
  const f = fixture(); change(f); const before = JSON.stringify(f.store);
  assert.throws(() => fees.record(f.store, f.context), message); assert.equal(JSON.stringify(f.store), before);
});
test('a fee cannot be posted twice through a new draft for identical bank evidence', () => {
  const f = fixture(); fees.record(f.store, f.context); const next = clone(f.context.draft); next.id = 'BRD-SECOND'; next.resolutions = {};
  const context = { ...f.context, draft: next, body: { ...f.context.body, draftId: next.id } }, before = JSON.stringify(f.store);
  assert.throws(() => fees.record(f.store, context), /already has/); assert.equal(JSON.stringify(f.store), before);
});
for (const action of ['accept_match', 'link_existing', 'create_receipt', 'create_expense', 'create_internal_transfer', 'exclude', 'timing_difference', 'split_allocation', 'bank_reversal_pair']) test('same physical bank row claimed by another draft ' + action + ' cannot become a tenant fee', () => {
  const f = fixture(), other = clone(f.context.draft); other.id = 'BRD-OTHER'; other.nature = 'PERSONAL'; other.resolutions = { 'bank-0': { action, appId: 'EX-EXISTING/PAY-001' } }; f.store.bankReconciliationDrafts[other.id] = other;
  const before = JSON.stringify(f.store); assert.throws(() => fees.record(f.store, f.context), /saved review in another reconciliation/); assert.equal(JSON.stringify(f.store), before);
});
for (const [name, resolution] of [
  ['counterpart', { action: 'linked_existing_counterpart', linkedRowId: 'bank-0', appId: 'REC-EXISTING' }],
  ['grouped', { action: 'link_multiple_bank_entries', bankRowIds: ['bank-0'], appId: 'REC-EXISTING' }],
  ['retained group reference', { action: 'link_multiple_bank_entries', bankReferences: [{ rowId: 'bank-0' }], appId: 'REC-EXISTING' }],
  ['invalid bank index', { action: 'link_multiple_bank_entries', bankRowIds: ['bank-99'], appId: 'REC-EXISTING' }]
]) test('foreign ' + name + ' bank claims fail closed without mutating reviews', () => {
  const f = fixture(), other = clone(f.context.draft); other.id = 'BRD-OTHER'; other.resolutions = { 'app-0': resolution }; f.store.bankReconciliationDrafts[other.id] = other;
  const before = JSON.stringify(f.store); assert.throws(() => fees.record(f.store, f.context), /saved review|unverifiable bank-row claim/); assert.equal(JSON.stringify(f.store), before);
});
for (const [name, change] of [
  ['normalized reference', bank => { bank.reference = '  synthetic ref  '; bank.description = 'Different imported narration'; bank.balance = 999; }],
  ['missing reference', bank => { delete bank.reference; }],
  ['missing bank balance and reference', bank => { delete bank.reference; delete bank.balance; bank.description = 'Ambiguous imported narration'; }],
  ['duplicate physical candidates', (bank, other) => { other.transactions.push(clone(bank)); other.resolutions = { 'bank-1': { action: 'exclude' } }; }]
]) test('foreign bank review identity is fail-closed with ' + name, () => {
  const f = fixture(), other = clone(f.context.draft); other.id = 'BRD-OTHER'; other.resolutions = { 'bank-0': { action: 'exclude' } }; change(other.transactions[0], other); f.store.bankReconciliationDrafts[other.id] = other;
  const before = JSON.stringify(f.store); assert.throws(() => fees.record(f.store, f.context), /saved review/); assert.equal(JSON.stringify(f.store), before);
});
for (const [name, change] of [
  ['different bank', other => { other.account = 'Axis Bank 3448'; }],
  ['different reference', other => { other.transactions[0].reference = 'DISTINCT-BANK-REF'; }],
  ['different amount', other => { other.transactions[0].debit = 99; }],
  ['different date', other => { other.transactions[0].date = '2098-06-03'; }],
  ['unreviewed duplicate upload', other => { other.resolutions = {}; }],
  ['unmatched suggestion only', other => { other.resolutions = {}; other.matchingExclusions = { 'bank-0': { appId: 'EX-KEEP' } }; }]
]) test('a ' + name + ' does not falsely claim this bank transaction', () => {
  const f = fixture(), other = clone(f.context.draft); other.id = 'BRD-OTHER'; other.resolutions = { 'bank-0': { action: 'exclude' } }; change(other); f.store.bankReconciliationDrafts[other.id] = other;
  const preserved = clone(other); assert.ok(fees.record(f.store, f.context).fee.id); assert.deepEqual(other, preserved);
});
test('fee finalization evidence rejects a foreign claim added after fee creation', () => {
  const f = fixture(); fees.record(f.store, f.context); const other = clone(f.context.draft); other.id = 'BRD-OTHER'; other.resolutions = { 'bank-0': { action: 'exclude' } }; f.store.bankReconciliationDrafts[other.id] = other;
  const before = JSON.stringify(f.store); assert.match(fees.evidenceError(f.store, f.context.draft), /saved review/); assert.equal(JSON.stringify(f.store), before);
});
for (const [label, dependent] of [
  ['refund', (store, result) => { store.expenseRefunds = [{ id: 'RF-1', status: 'received', sources: [{ expenseId: result.expense.id }] }]; }],
  ['another draft', (store, result) => { store.bankReconciliationDrafts.OTHER = { id: 'OTHER', resolutions: { 'bank-1': { appId: result.expense.id + '/PAY-001' } } }; }],
  ['finalized report', (store, result) => { store.bankStatements[fees.ACCOUNT] = { imports: [{ reconciliationRows: [{ linkedRecordIds: [result.expense.id + '/PAY-001'] }] }] }; }],
  ['changed payment', (store, result) => { result.expense.payments[0].amount = 1000; }],
  ['pending approval', (store, result, context) => { store.bankReconciliationApprovals = { A: { draftId: context.draft.id, status: 'pending' } }; }]
]) test('fee undo preserves source when it has ' + label, () => {
  const f = fixture(), result = fees.record(f.store, f.context); dependent(f.store, result, f.context); const before = JSON.stringify(f.store);
  assert.throws(() => fees.undo(f.store, { draft: f.context.draft, rowId: 'bank-0', actor: 'test', now: '2098-06-03', reason: 'Test blocked undo' }), /changed|linked|approval/);
  assert.equal(JSON.stringify(f.store), before);
});
test('undo a fee receipt cannot orphan a later rental allocation', () => {
  const f = fixture('documentation_fee', 'receipt'), result = fees.record(f.store, f.context);
  f.store.rentals.credits.push({ id: 'CREDIT-LATER', receiptId: result.receipt.id }); const before = JSON.stringify(f.store);
  assert.throws(() => fees.undo(f.store, { draft: f.context.draft, rowId: 'bank-0', actor: 'test', now: '2098-06-03', reason: 'Test' }), /dependent allocation/);
  assert.equal(JSON.stringify(f.store), before);
});

'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { assertUnlocked, assertPreserved, assertAudit } = require('../modules/source-correction-guards');
const clone = value => JSON.parse(JSON.stringify(value));
const account = 'Synthetic Security Bank 6543', id = 'TR-SECURITY-SAMPLE', date = '2098-07-13';
function fixture() {
  const movement = { id, nature: 'SAMAST', fromNature: 'SAMAST', fromAccount: account, date, amount: 212.13 };
  return { store: { transfers: [movement], auditLog: [] }, options: { ids: [id], scopes: [{ nature: 'SAMAST', account }], dates: [date], records: [movement] } };
}
function rejectsUnchanged(change) {
  const f = fixture(); change(f);
  const before = clone(f.store), optionsBefore = clone(f.options);
  assert.throws(() => assertUnlocked(f.store, f.options), { status: 409 });
  assert.deepEqual(f.store, before, 'guard must not repair or rewrite stored evidence');
  assert.deepEqual(f.options, optionsBefore, 'guard must not alter source objects');
}

for (const [label, change] of [
  ['GoKwik reverse transfer link', f => { f.store.gokwikPostings = { G: { transferId: id } }; }],
  ['receipt settled by this transfer', f => { f.store.receipts = [{ id: 'R-OTHER', settledByTransferId: id }]; }],
  ['expense reimbursement reverse link', f => { f.store.expenses = { 'EX-OTHER': { reimbursementPayments: [{ id: 'RP-OTHER', transferId: id }] } }; }],
  ['another transfer settlement source', f => { f.store.transfers.push({ id: 'TR-OTHER', settlementSourceId: id }); }],
  ['rental receipt dependency', f => { f.store.rentals = { allocations: [{ paymentId: id }] }; }],
  ['unselected card ownership allocation', f => { f.store.creditCardSettlementAllocations = [{ id: 'CCA-OTHER', settledByTransferId: id }]; }],
  ['refund external movement', f => { f.store.expenseRefunds = [{ id: 'RF-OTHER', components: [{ externalMovementId: id }] }]; }],
  ['vendor advance source', f => { f.store.vendorAdvances = [{ id: 'VA-OTHER', transferId: id }]; }]
]) test('unrelated same-store dependency blocks correction: ' + label, () => rejectsUnchanged(change));

test('owned source records and immutable audit snapshots are not foreign dependencies', () => {
  const f = fixture(), allocation = { id: 'CCA-SECURITY-SAMPLE', transferId: id, amount: 212.13 };
  f.store.creditCardSettlementAllocations = [allocation]; f.options.records.push(allocation); f.options.ids.push(allocation.id);
  f.store.auditLog.push({ id: 'AUD-OLD', before: clone(f.store.transfers[0]), after: { id: allocation.id } });
  const before = clone(f.store);
  assert.doesNotThrow(() => assertUnlocked(f.store, f.options));
  assert.deepEqual(f.store, before);
});

for (const [label, change] of [
  ['matching book is text', f => { f.store.bankStatements = { [account]: 'corrupted' }; }],
  ['matching book is null', f => { f.store.bankStatements = { [account]: null }; }],
  ['matching book is array', f => { f.store.bankStatements = { [account]: [] }; }],
  ['matching book imports are not an array', f => { f.store.bankStatements = { [account]: { imports: 'corrupted' } }; }],
  ['pending approval entry is text', f => { f.store.bankReconciliationApprovals = { A: 'corrupted' }; }],
  ['pending approval entry is null', f => { f.store.bankReconciliationApprovals = { A: null }; }],
  ['pending draft is text', f => { f.store.bankReconciliationApprovals = { A: { status: 'pending', account, draftId: 'D', from: '2099-01-01', to: '2099-01-31' } }; f.store.bankReconciliationDrafts = { D: 'corrupted' }; }],
  ['pending draft summary is text', f => { f.store.bankReconciliationApprovals = { A: { status: 'pending', account, draftId: 'D', from: '2099-01-01', to: '2099-01-31' } }; f.store.bankReconciliationDrafts = { D: { account, summary: 'corrupted' } }; }]
]) test('malformed nested protection evidence fails closed: ' + label, () => rejectsUnchanged(change));

for (const [label, change] of [
  ['approval boundaries exclude date but draft includes it', f => {
    f.store.bankReconciliationApprovals = { A: { status: 'pending', account, draftId: 'D', from: '2099-01-01', to: '2099-01-31' } };
    f.store.bankReconciliationDrafts = { D: { account, summary: { from: '2098-07-01', to: '2098-07-31' } } };
  }],
  ['approval account differs from protected draft account', f => {
    f.store.bankReconciliationApprovals = { A: { status: 'pending', account: 'Different Bank', draftId: 'D', from: '2099-01-01', to: '2099-01-31' } };
    f.store.bankReconciliationDrafts = { D: { account, summary: { from: '2098-07-01', to: '2098-07-31' } } };
  }],
  ['approval claims protected account but its draft disagrees', f => {
    f.store.bankReconciliationApprovals = { A: { status: 'pending', account, draftId: 'D', from: '2099-01-01', to: '2099-01-31' } };
    f.store.bankReconciliationDrafts = { D: { account: 'Different Bank', summary: { from: '2099-01-01', to: '2099-01-31' } } };
  }]
]) test('inconsistent pending approval cannot hide a protected period: ' + label, () => rejectsUnchanged(change));

for (const field of ['reconciled', 'reconciledAt', 'reconciliationId', 'finalizedAt', 'locked', 'bankTransactionId', 'bankStatementEvidence', 'settlementId', 'reversedAt', 'supersededBy']) {
  test('persisted direct lock remains authoritative: ' + field, () => rejectsUnchanged(f => { f.options.records[0][field] = true; }));
}
for (const status of ['reversed', 'canceled']) test('persisted inactive status remains authoritative: ' + status, () => rejectsUnchanged(f => { f.options.records[0].status = status; }));

test('conservation guard rejects altering reconciliations while allowing only the declared source collection', () => {
  const before = { transfers: [{ id, amount: 212.13 }], bankReconciliationApprovals: { A: { status: 'pending' } }, bankDateOverrides: { OTHER: { date } }, auditLog: [{ id: 'AUD-OLD' }] };
  const after = clone(before); after.transfers[0].amount = 211.99;
  assert.doesNotThrow(() => assertPreserved(before, after, ['transfers', 'auditLog', 'auditSeq']));
  for (const mutate of [s => { s.bankReconciliationApprovals.A.status = 'approved'; }, s => { delete s.bankDateOverrides.OTHER; }, s => { s.receipts = []; }]) {
    const broken = clone(after); mutate(broken);
    assert.throws(() => assertPreserved(before, broken, ['transfers', 'auditLog', 'auditSeq']), { status: 409 });
  }
});

for (const [label, store] of [
  ['object history', { auditLog: { old: { id: 'AUD-000001' } }, auditSeq: 1 }],
  ['text history', { auditLog: 'retained history', auditSeq: 1 }],
  ['history without sequence', { auditLog: [{ id: 'LEGACY-ENTRY' }] }],
  ['string sequence', { auditLog: [], auditSeq: '12' }],
  ['negative sequence', { auditLog: [], auditSeq: -1 }],
  ['fractional sequence', { auditLog: [], auditSeq: 1.5 }],
  ['infinite sequence', { auditLog: [], auditSeq: Infinity }],
  ['NaN sequence', { auditLog: [], auditSeq: NaN }],
  ['unsafe sequence', { auditLog: [], auditSeq: Number.MAX_SAFE_INTEGER + 1 }],
  ['exhausted sequence', { auditLog: [], auditSeq: Number.MAX_SAFE_INTEGER }],
  ['null retained entry', { auditLog: [null], auditSeq: 1 }],
  ['text retained entry', { auditLog: ['retained history'], auditSeq: 1 }],
  ['array retained entry', { auditLog: [[]], auditSeq: 1 }],
  ['duplicate standard IDs', { auditLog: [{ id: 'AUD-000001' }, { id: 'AUD-000001' }], auditSeq: 2 }],
  ['duplicate legacy IDs', { auditLog: [{ id: 'LEGACY-ENTRY' }, { id: 'LEGACY-ENTRY' }], auditSeq: 2 }],
  ['whitespace duplicate IDs', { auditLog: [{ id: 'AUD-000001' }, { id: ' AUD-000001 ' }], auditSeq: 2 }],
  ['next identity already retained', { auditLog: [{ id: 'AUD-000002' }], auditSeq: 1 }],
  ['retained identity beyond sequence', { auditLog: [{ id: 'AUD-000999' }], auditSeq: 3 }]
]) test('audit preservation rejects invalid retained evidence: ' + label, () => {
  const before = structuredClone(store);
  assert.throws(() => assertAudit(store), { status: 409 });
  assert.deepEqual(store, before, 'the audit guard must never normalize, renumber or discard retained history');
});

test('audit preservation accepts empty or valid legacy history without mutation', () => {
  for (const store of [{}, { auditLog: [] }, { auditLog: [], auditSeq: 0 },
    { auditLog: [{ id: 'AUD-000001' }, { id: 'LEGACY-ENTRY', note: 'Retain verbatim' }], auditSeq: 3 },
    { auditLog: [{ note: 'Historical entry without ID' }], auditSeq: 4 }]) {
    const before = clone(store);
    assert.doesNotThrow(() => assertAudit(store));
    assert.deepEqual(store, before);
  }
});

test('card audit uses its own history field and CCA identity namespace', () => {
  const store = { audit: [{ id: 'CCA-000004' }], auditSeq: 4 }, before = clone(store);
  assert.doesNotThrow(() => assertAudit(store, 'audit', 'CCA-'));
  assert.deepEqual(store, before);
  for (const broken of [{ audit: [{ id: 'CCA-000005' }], auditSeq: 4 },
    { audit: [{ id: 'CCA-000004' }, { id: 'CCA-000004' }], auditSeq: 4 },
    { audit: { old: { id: 'CCA-000004' } }, auditSeq: 4 }]) {
    const retained = clone(broken);
    assert.throws(() => assertAudit(broken, 'audit', 'CCA-'), { status: 409 });
    assert.deepEqual(broken, retained);
  }
});

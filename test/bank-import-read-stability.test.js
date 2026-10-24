'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { planBankImportSourceRepair, applyBankImportSourceRepair } = require('../modules/bank-import-source-repair');
const { stage: stageGoKwik } = require('../modules/gokwik-api-accounting');

const clone = value => JSON.parse(JSON.stringify(value));
const migrationKey = 'owner-confirmed-axis-3645-cases-2026-08-22-to-2026-08-27-v1';
const cases = ['haryanaPersonal', 'kaluAdvance', 'alokCombined', 'cashPaytmRoute', 'duplicateTransfer', 'shivamReimbursement', 'rounding'];
const firstTime = '2026-10-23T08:00:00.000Z', nextTime = '2026-10-23T08:00:10.000Z';

// Execute the actual migration in isolation. Requiring expenses.js would boot
// unrelated services and migrations; this harness has no disk/store/network I/O.
function migrationHarness() {
  const source = fs.readFileSync(path.join(__dirname, '../modules/expenses.js'), 'utf8');
  const start = source.indexOf('function applyOwnerConfirmedAxis3645Cases(');
  const end = source.indexOf('function vendorBalanceForNames(', start);
  assert.ok(start >= 0 && end > start, 'extract the real migration, not a copied implementation');
  let now = firstTime;
  class ClockDate extends Date {
    constructor(...args) { super(...(args.length ? args : [now])); }
    static now() { return Date.parse(now); }
  }
  const migrate = vm.runInNewContext(source.slice(start, end) + '\napplyOwnerConfirmedAxis3645Cases;', {
    Date: ClockDate,
    normalizedNature: value => String(value || 'SANKI').toUpperCase(),
    num: value => Number(value) || 0,
    DEFAULT_COUNTER_CASH: 'Counter Cash', DEFAULT_SALES_BANK: 'Axis Bank 3448', PAYTM_CLEARING_ACCOUNT: 'Paytm Clearing',
    appBankMovements: () => [],
    audit(store, _req, action, type, id, details) { store.auditLog.push(clone({ action, type, id, details })); }
  }, { filename: 'expenses-axis-3645-isolated.js' });
  return { migrate, at(value) { now = value; } };
}

function migrationState() {
  return { appliedAt: '2026-09-01T01:00:00.000Z', account: 'Prashant Axis 3645',
    completed: Object.fromEntries(cases.map(name => [name, true])), results: {},
    preservedUnrelatedResolutions: true, complete: true, updatedAt: '2026-09-01T02:00:00.000Z' };
}

function fixture() {
  const fileHash = 'a'.repeat(64), account = 'IndusInd Bank 8181';
  const transactions = [
    { date: '2026-10-20', reference: '123456789013', description: 'UPI/123456789013/DR/SYNTHETIC SHOP', bankReference: 'S123456', debit: 200, credit: 0, balance: 800 },
    { date: '2026-10-19', reference: '123456789012', description: 'UPI/123456789012/DR/SYNTHETIC VENDOR', bankReference: 'S123457', debit: 90, credit: 0, balance: 1000 }
  ];
  const summary = { format: 'IndusInd mobile PDF', accountLast4: '8181', from: '2026-10-19', to: '2026-10-20',
    openingBalance: 1090, closingBalance: 800, totalDebits: 290, totalCredits: 0, validated: true };
  const draft = { id: 'BRD-READ-STABILITY-SYNTHETIC', account, nature: 'SANKI', fileHash,
    temporaryFile: 'synthetic-retained.pdf', transactions, summary,
    resolutions: { 'bank-0': { action: 'accept_match', by: 'owner', at: '2026-10-21T01:00:00Z' } }, matchingExclusions: {} };
  const parsedRows = clone(transactions);
  parsedRows[1].debit = 490;
  parsedRows.statementSummary = { ...summary, parserVersion: 'positioned-cells-v1', openingBalance: 1490, totalDebits: 690 };
  const store = { oneTimeMigrations: { [migrationKey]: migrationState() },
    bankReconciliationDrafts: { [draft.id]: draft }, bankStatements: {}, bankReconciliationApprovals: {},
    expenses: { 'EX-KEEP': { id: 'EX-KEEP', amount: 490, paidAmount: 490,
      payments: [{ id: 'PAY-001', amount: 490, account, date: '2026-10-19', proof: '/synthetic-proof.jpg' }] } },
    transfers: [{ id: 'TR-EXCLUDED', amount: 2300, nature: 'PERSONAL', fromAccount: 'Synthetic Bank Source',
      toAccount: 'Synthetic Bank Destination', date: '2026-08-03', accountingExcluded: true, excludedBy: 'owner' }],
    vendorAdvances: [], adjustments: [], salesRefunds: [], bankDateOverrides: {},
    openingBalances: { [account]: 0.01 }, auditLog: [] };
  return { store, draft, parsedRows, sourceHash: fileHash, expectedSourceHash: fileHash };
}

function readAt(h, persisted, at) {
  h.at(at);
  const store = clone(persisted);
  assert.equal(h.migrate(store), false, 'read-time migration must report no change');
  return store;
}

test('completed owner migration is byte-stable across reads at different wall-clock times', () => {
  const h = migrationHarness(), { store } = fixture(), before = JSON.stringify(store);
  assert.equal(h.migrate(store), false);
  assert.equal(JSON.stringify(store), before, 'a no-op must not refresh migration.updatedAt');
  h.at(nextTime);
  assert.equal(h.migrate(store), false);
  assert.equal(JSON.stringify(store), before, 'repeated reads preserve timestamps and all accounting evidence');
});

test('pending migration with unchanged missing evidence is also a byte-stable no-op', () => {
  const h = migrationHarness(), { store } = fixture(), state = store.oneTimeMigrations[migrationKey];
  state.completed = {};
  state.complete = false;
  state.results = { haryanaPersonal: { result: 'not_found' }, kaluAdvance: { result: 'required_exact_entries_not_found' } };
  const before = JSON.stringify(store);
  assert.equal(h.migrate(store), false);
  h.at(nextTime);
  assert.equal(h.migrate(store), false);
  assert.equal(JSON.stringify(store), before, 'pending cases must not keep invalidating unrelated statement previews');
});

test('cold migration metadata and collection normalization are persisted once, then reads stabilize', () => {
  const h = migrationHarness(), store = { expenses: {}, bankReconciliationDrafts: {}, auditLog: [] };
  assert.equal(h.migrate(store), true, 'initial metadata is a real store change that must be saved');
  const state = store.oneTimeMigrations[migrationKey];
  assert.equal(state.appliedAt, firstTime); assert.equal(state.updatedAt, firstTime);
  assert.equal(state.complete, false);
  assert.equal(state.results.haryanaPersonal.result, 'not_found');
  assert.equal(state.results.kaluAdvance.result, 'required_exact_entries_not_found');
  for (const key of ['vendorAdvances', 'transfers', 'adjustments', 'salesRefunds']) {
    assert.ok(Array.isArray(store[key])); assert.equal(store[key].length, 0);
  }
  assert.equal(JSON.stringify(store.bankDateOverrides), '{}');
  assert.equal(store.auditLog.length, 0, 'metadata initialization invents no accounting event');
  const saved = JSON.stringify(store);
  h.at(nextTime);
  assert.equal(h.migrate(store), false);
  assert.equal(JSON.stringify(store), saved, 'initialized metadata is not restamped on every read');
});

test('a changed pending diagnostic is persisted once without changing accounting evidence', () => {
  const h = migrationHarness(), { store } = fixture(), state = store.oneTimeMigrations[migrationKey];
  state.completed.haryanaPersonal = false; state.complete = false;
  state.results.haryanaPersonal = { result: 'not_found' };
  for (const id of ['EX-SYNTHETIC-A', 'EX-SYNTHETIC-B']) store.expenses[id] = {
    id, nature: 'PERSONAL', date: '2026-08-22', amount: 6560, vendor: 'Haryana Trading Company',
    payments: [{ id: 'PAY-001', amount: 6560, account: 'Synthetic Personal Bank' }]
  };
  const expenses = clone(store.expenses), transfers = clone(store.transfers), drafts = clone(store.bankReconciliationDrafts);
  assert.equal(h.migrate(store), true, 'not_found -> ambiguous is changed migration evidence');
  assert.equal(store.oneTimeMigrations[migrationKey].results.haryanaPersonal.result, 'ambiguous');
  assert.equal(store.oneTimeMigrations[migrationKey].updatedAt, firstTime);
  assert.equal(store.oneTimeMigrations[migrationKey].completed.haryanaPersonal, false);
  assert.deepEqual(store.expenses, expenses); assert.deepEqual(store.transfers, transfers);
  assert.deepEqual(store.bankReconciliationDrafts, drafts); assert.equal(store.auditLog.length, 0);
  const saved = JSON.stringify(store);
  h.at(nextTime);
  assert.equal(h.migrate(store), false);
  assert.equal(JSON.stringify(store), saved, 'the same unresolved diagnostic does not trigger another save');
});

test('real migration progress retains its timestamp, audit and exactly-once accounting change', () => {
  const h = migrationHarness(), { store } = fixture(), state = store.oneTimeMigrations[migrationKey];
  state.completed.duplicateTransfer = false; state.complete = false;
  store.transfers.push({ id: 'TR-00008', amount: 1203, fromAccount: 'Axis Bank 3448', toAccount: 'Prashant Axis 3645' });
  store.transferSeq = 50;
  const excluded = clone(store.transfers[0]), expenses = clone(store.expenses);
  assert.equal(h.migrate(store), true);
  assert.equal(store.oneTimeMigrations[migrationKey].updatedAt, firstTime);
  assert.equal(store.oneTimeMigrations[migrationKey].completed.duplicateTransfer, true);
  assert.equal(store.transfers.length, 3);
  assert.equal(store.transfers[2].id, 'TR-00051');
  assert.equal(store.transfers[2].createdAt, firstTime);
  assert.equal(store.auditLog.length, 1);
  assert.equal(store.auditLog[0].action, 'BANK_CONFIRMED_TRANSFER_CREATED');
  assert.deepEqual(store.transfers[0], excluded); assert.deepEqual(store.expenses, expenses);
  const saved = JSON.stringify(store);
  h.at(nextTime);
  assert.equal(h.migrate(store), false);
  assert.equal(JSON.stringify(store), saved, 'a later read must neither repeat the migration nor rewrite its audit time');
});

test('real source-repair preview and apply agree after independent no-op store reads', () => {
  const h = migrationHarness(), input = fixture(), persisted = clone(input.store);
  const first = readAt(h, persisted, firstTime), second = readAt(h, persisted, nextTime);
  const preview = planBankImportSourceRepair({ ...input, store: first, draft: first.bankReconciliationDrafts[input.draft.id] });
  const applyInput = { ...input, store: second, draft: second.bankReconciliationDrafts[input.draft.id] };
  const repeated = planBankImportSourceRepair(applyInput);
  assert.equal(repeated.storeFingerprint, preview.storeFingerprint);
  assert.equal(repeated.fingerprint, preview.fingerprint, 'only the wall clock advanced; evidence did not');
  const before = JSON.stringify(second);
  const applied = applyBankImportSourceRepair({ ...applyInput, expectedFingerprint: preview.fingerprint,
    actor: 'synthetic-owner', reason: 'Verified retained synthetic PDF column extraction', at: nextTime });
  assert.equal(applied.nextDraft.transactions[1].debit, 490);
  assert.deepEqual(applied.nextDraft.transactions[0], input.draft.transactions[0]);
  assert.deepEqual(applied.nextDraft.resolutions, input.draft.resolutions);
  assert.equal(applied.nextDraft.sourceAmountRepairHistory.length, 1);
  assert.equal(JSON.stringify(second), before, 'planner returns an audited draft and never edits source accounting');
  assert.deepEqual(second, persisted, 'no-op reads preserve every source and exclusion');
});

test('read-only GoKwik staging and owner migration together produce stable repair evidence', () => {
  const h = migrationHarness(), input = fixture(), persisted = clone(input.store);
  const order = { id: '123456', name: '#100', orderNumber: '100', total: 150, channel: 'Website', paymentTransactions: [] };
  const row = { type: 'Payment', paymentId: 'P-SYNTHETIC', orderId: order.id, orderNumber: order.name, providerOrderId: 'GK-SYNTHETIC',
    date: '2026-08-25', transactionDate: '2026-08-24T10:00:00Z', utr: 'UTR-SYNTHETIC', provider: 'easebuzz',
    gross: 150, fees: 4.43, fee: 3.75, tax: 0.68, credit: 145.57, debit: 0,
    deductions: { gokwik: 75 }, reportedGokwikDeduction: 75, net: 70.57 };
  const project = at => {
    const store = readAt(h, persisted, at);
    const result = stageGoKwik(store, [clone(row)], [clone(order)], { readOnly: true });
    assert.equal(result.added, 1); assert.equal(result.linked, 1); assert.equal(result.issues.length, 0);
    for (const record of [...Object.values(store.gokwikImports), ...Object.values(store.gokwikOrderLinks)]) {
      assert.equal(record.at, undefined, 'a read-side projection cannot invent a fresh saved audit timestamp');
    }
    assert.deepEqual(store.expenses, persisted.expenses); assert.deepEqual(store.transfers, persisted.transfers);
    assert.deepEqual(store.openingBalances, persisted.openingBalances); assert.deepEqual(store.auditLog, persisted.auditLog);
    return store;
  };
  const first = project(firstTime), second = project(nextTime);
  assert.equal(JSON.stringify(first), JSON.stringify(second), 'independent read-side projections retain identical source evidence');
  const preview = planBankImportSourceRepair({ ...input, store: first, draft: first.bankReconciliationDrafts[input.draft.id] });
  const applyInput = { ...input, store: second, draft: second.bankReconciliationDrafts[input.draft.id] };
  const before = JSON.stringify(second);
  const applied = applyBankImportSourceRepair({ ...applyInput, expectedFingerprint: preview.fingerprint,
    actor: 'synthetic-owner', reason: 'Retained synthetic PDF verification with read-only provider evidence', at: nextTime });
  assert.equal(applied.nextDraft.transactions[1].debit, 490);
  assert.equal(JSON.stringify(second), before);
  second.gokwikTransactions['P-SYNTHETIC'].net = 71.57;
  assert.throws(() => applyBankImportSourceRepair({ ...applyInput, expectedFingerprint: preview.fingerprint,
    actor: 'synthetic-owner', reason: 'Real changed provider evidence must invalidate the prior preview', at: nextTime }),
  /accounting evidence changed/);
});

for (const [name, mutate] of [
  ['retained PDF hash', input => { input.sourceHash = 'b'.repeat(64); }],
  ['draft remark', input => { input.draft.periodRemark = 'Changed by another reviewer'; }],
  ['expense payment amount', input => { input.store.expenses['EX-KEEP'].payments[0].amount = 491; }],
  ['expense payment proof', input => { input.store.expenses['EX-KEEP'].payments[0].proof = '/different-proof.jpg'; }],
  ['Owner exclusion', input => { input.store.transfers[0].accountingExcluded = false; }],
  ['account opening', input => { input.store.openingBalances[input.draft.account] = 1.01; }],
  ['affected row decision', input => { input.draft.resolutions['bank-1'] = { action: 'accept_match' }; }],
  ['affected matching exclusion', input => { input.draft.matchingExclusions['bank-1'] = { reason: 'Owner excluded' }; }],
  ['finalized history', input => { input.store.bankStatements[input.draft.account] = { imports: [{ draftId: input.draft.id }] }; }],
  ['pending Owner approval', input => { input.store.bankReconciliationApprovals.PENDING = { draftId: input.draft.id, status: 'pending' }; }]
]) test('read stability never weakens stale-evidence rejection: ' + name, () => {
  const h = migrationHarness(), input = fixture(), persisted = clone(input.store);
  const first = readAt(h, persisted, firstTime);
  const preview = planBankImportSourceRepair({ ...input, store: first, draft: first.bankReconciliationDrafts[input.draft.id] });
  const second = readAt(h, persisted, nextTime);
  const applyInput = { ...input, store: second, draft: second.bankReconciliationDrafts[input.draft.id] };
  mutate(applyInput);
  const before = JSON.stringify(second);
  assert.throws(() => applyBankImportSourceRepair({ ...applyInput, expectedFingerprint: preview.fingerprint,
    actor: 'synthetic-owner', reason: 'Do not bypass changed source evidence', at: nextTime }),
  error => error.statusCode === 409 && /Statement source repair:/.test(error.message));
  assert.equal(JSON.stringify(second), before, 'a rejected repair never mutates the fresh evidence');
});

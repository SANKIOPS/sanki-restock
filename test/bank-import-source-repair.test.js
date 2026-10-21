'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { planBankImportSourceRepair, applyBankImportSourceRepair, draftFingerprint } = require('../modules/bank-import-source-repair');
const clone = value => JSON.parse(JSON.stringify(value));
const hash = 'a'.repeat(64), account = 'IndusInd Bank 8181', ref = '123456789012';

function fixture() {
  const transactions = [
    { date: '2026-10-20', reference: '123456789013', description: 'UPI/123456789013/DR/SHOP', bankReference: 'S123456', debit: 200, credit: 0, balance: 800, row: 1 },
    { date: '2026-10-19', reference: ref, description: 'IMPS/P2A/' + ref + '/RECIPIENT', bankReference: 'M12345674', debit: 90, credit: 0, balance: 1000, row: 2 }
  ];
  const summary = { format: 'IndusInd mobile PDF', accountLast4: '8181', from: '2026-10-19', to: '2026-10-20', openingBalance: 1090, closingBalance: 800, totalDebits: 290, totalCredits: 0, validated: true };
  const draft = { id: 'BRD-SOURCE', account, nature: 'SANKI', originalName: 'statement.pdf', temporaryFile: '/retained/statement.pdf', fileHash: hash, summary, transactions,
    resolutions: { 'bank-0': { action: 'create_internal_transfer', amount: 200, otherAccount: 'Arshpreet 1919' } },
    matchingExclusions: { 'bank-0': { reason: 'Reviewed unrelated row' } }, transactionOwnership: { 'bank-1': { nature: 'SANKI' } }, periodRemark: 'Keep this review', decisionAudit: [{ rowId: 'bank-0', action: 'review' }] };
  const parsedRows = clone(transactions);
  parsedRows[1].debit = 490; parsedRows[1].bankReference = 'M1234567';
  parsedRows.statementSummary = { ...summary, parserVersion: 'positioned-cells-v1', openingBalance: 1490, totalDebits: 690, openingBalanceInferred: true };
  const store = { bankReconciliationDrafts: { [draft.id]: draft }, transfers: [{ id: 'TR-UNCHANGED', reconciliationDraft: draft.id, bankRowId: 'bank-0', amount: 200 }], expenses: { 'EX-KEEP': { amount: 490, payments: [] } }, adjustments: [{ id: 'ADJ-KEEP', amount: -5.9 }], bankStatements: {} };
  return { store, draft, parsedRows, sourceHash: hash, expectedSourceHash: hash };
}

test('repairs 90 to 490 from positioned source without changing accounting or other review decisions', () => {
  const input = fixture(), before = clone(input.store), originalDraft = clone(input.draft), plan = planBankImportSourceRepair(input);
  assert.equal(plan.changed, true);
  assert.equal(plan.changes.length, 1);
  assert.equal(plan.changes[0].rowId, 'bank-1');
  assert.equal(plan.nextDraft.transactions[1].debit, 490);
  assert.equal(plan.nextDraft.transactions[1].bankReference, 'M1234567');
  assert.equal(plan.afterSummary.openingBalance, 1490);
  assert.equal(plan.afterSummary.totalDebits, 690);
  assert.equal(plan.afterSummary.closingBalance, 800);
  assert.deepEqual(plan.nextDraft.transactions[0], originalDraft.transactions[0]);
  for (const key of ['resolutions', 'matchingExclusions', 'transactionOwnership', 'periodRemark', 'decisionAudit']) assert.deepEqual(plan.nextDraft[key], originalDraft[key]);
  assert.deepEqual(input.store, before);
  assert.deepEqual(input.draft, originalDraft);
});

test('apply requires reviewed fingerprint and returns an audited draft without mutating any store', () => {
  const input = fixture(), before = clone(input.store), plan = planBankImportSourceRepair(input);
  const applied = applyBankImportSourceRepair({ ...input, expectedFingerprint: plan.fingerprint, actor: 'owner', reason: 'Correct retained original PDF column extraction', at: '2026-10-21T10:00:00Z' });
  assert.equal(applied.nextDraft.sourceAmountRepairHistory.length, 1);
  assert.equal(applied.nextDraft.sourceAmountRepair.ledgerRecordsUnchanged, true);
  assert.equal(applied.nextDraft.sourceAmountRepair.sourceHash, hash);
  assert.equal(applied.nextDraft.sourceAmountRepair.changes[0].before.debit, 90);
  assert.deepEqual(input.store, before);
  assert.deepEqual(input.store.expenses, before.expenses);
  assert.deepEqual(input.store.adjustments, before.adjustments);
});

test('repeating a completed source correction is an idempotent no-op', () => {
  const input = fixture(), first = planBankImportSourceRepair(input), applied = applyBankImportSourceRepair({ ...input, expectedFingerprint: first.fingerprint, actor: 'owner', reason: 'Source parser repair' });
  input.draft = applied.nextDraft; input.store.bankReconciliationDrafts[input.draft.id] = input.draft;
  const second = planBankImportSourceRepair(input);
  assert.equal(second.changed, false);
  assert.equal(second.changes.length, 0);
  const replay = applyBankImportSourceRepair({ ...input, expectedFingerprint: second.fingerprint });
  assert.deepEqual(replay.nextDraft, applied.nextDraft);
  assert.equal(replay.nextDraft.sourceAmountRepairHistory.length, 1);
});

test('preserves draft indexes when its existing transaction order differs from positioned source order', () => {
  const input = fixture();
  input.draft.transactions.reverse(); input.draft.resolutions = {}; input.draft.matchingExclusions = {}; input.store.transfers = [];
  const plan = planBankImportSourceRepair(input);
  assert.equal(plan.changes[0].rowId, 'bank-0');
  assert.equal(plan.nextDraft.transactions[0].reference, ref);
  assert.equal(plan.nextDraft.transactions[0].debit, 490);
});

test('unreferenced unchanged bank rows can map only by their exact full financial identity', () => {
  const input = fixture();
  for (const rows of [input.draft.transactions, input.parsedRows]) { rows[0].reference = ''; rows[0].description = 'Monthly maintenance'; }
  assert.equal(planBankImportSourceRepair(input).changes.length, 1);
  input.parsedRows[0].description = 'Different maintenance';
  assert.throws(() => planBankImportSourceRepair(input), /mapped uniquely/);
});

test('repair is generic to verified amounts rather than a hard-coded 490 correction', () => {
  const input = fixture();
  input.parsedRows[1].debit = 2345.67;
  input.parsedRows.statementSummary.openingBalance = 3345.67;
  input.parsedRows.statementSummary.totalDebits = 2545.67;
  assert.equal(planBankImportSourceRepair(input).nextDraft.transactions[1].debit, 2345.67);
});

test('keeps unrelated reviewed rows byte-for-byte when positioned parsing removes a legacy page-header prefix', () => {
  const input = fixture();
  input.draft.transactions[0].description = 'Statement Period: - Branch IFSC Code: INDB0000275 ' + input.draft.transactions[0].description;
  const before = clone(input.draft.transactions[0]), plan = planBankImportSourceRepair(input);
  assert.equal(plan.changes.length, 1);
  assert.deepEqual(plan.nextDraft.transactions[0], before);
  assert.deepEqual(plan.nextDraft.resolutions, input.draft.resolutions);
});

test('a balanced source with duplicate physical row keys is rejected as ambiguous', () => {
  const input = fixture();
  const debit = { date: '2026-10-20', reference: '123456789012', description: 'UPI/123456789012/DR/SHOP', debit: 200, credit: 0, balance: 800 };
  const credit = { date: '2026-10-20', reference: '123456789013', description: 'UPI/123456789013/CR/SHOP', debit: 0, credit: 200, balance: 1000 };
  input.parsedRows = [clone(debit), clone(credit), clone(debit)];
  const summary = { ...input.draft.summary, from: '2026-10-20', to: '2026-10-20', openingBalance: 1000, closingBalance: 800, totalDebits: 400, totalCredits: 200 };
  input.parsedRows.statementSummary = { ...summary, parserVersion: 'positioned-cells-v1' };
  input.draft.transactions = clone(input.parsedRows); input.draft.summary = summary;
  assert.throws(() => planBankImportSourceRepair(input), /source rows are ambiguous/);
});

test('an old detached draft object is rejected when the currently saved draft differs', () => {
  const input = fixture();
  input.store.bankReconciliationDrafts[input.draft.id] = { ...clone(input.draft), periodRemark: 'Edited concurrently' };
  assert.throws(() => planBankImportSourceRepair(input), /supplied draft is stale/);
});

for (const [name, change, pattern] of [
  ['wrong retained hash', input => { input.sourceHash = 'b'.repeat(64); }, /hash/],
  ['missing hash', input => { delete input.expectedSourceHash; }, /hash/],
  ['missing positioned parser proof', input => { delete input.parsedRows.statementSummary.parserVersion; }, /column positions/],
  ['unvalidated source', input => { input.parsedRows.statementSummary.validated = false; }, /unvalidated/],
  ['wrong bank account', input => { input.parsedRows.statementSummary.accountLast4 = '7883'; }, /account/],
  ['changed period', input => { input.parsedRows.statementSummary.from = '2026-10-18'; }, /period/],
  ['different row count', input => { input.parsedRows.pop(); }, /row count/],
  ['inconsistent source totals', input => { input.parsedRows.statementSummary.totalDebits = 290; }, /totals/],
  ['changed closing balance', input => { input.parsedRows.statementSummary.closingBalance = 900; input.parsedRows.statementSummary.openingBalance += 100; }, /closing balance/],
  ['changed running balance', input => { input.parsedRows[0].balance = 801; }, /running balance/],
  ['ambiguous source rows', input => { input.parsedRows[0] = clone(input.parsedRows[1]); input.parsedRows.statementSummary = { ...input.parsedRows.statementSummary, openingBalance: 1980, closingBalance: 1000, totalDebits: 980 }; input.draft.summary.closingBalance = 1000; input.draft.summary.openingBalance = 1290; }, /running balance|ambiguous/],
  ['conflicting transaction references', input => { input.parsedRows[1].reference = '123456789999'; }, /conflicting transaction references/],
  ['changed narration', input => { input.parsedRows[1].description += ' DIFFERENT RECIPIENT'; }, /narration/],
  ['merged source', input => { input.draft.sourceStatements = [{ fileHash: hash }, { fileHash: hash }]; }, /merged/],
  ['locked restoration', input => { input.draft.originalStatementPeriod = {}; }, /locked/],
  ['reconstructed finalized history', input => { input.draft.reconstructedFromFinalized = {}; }, /reconstructed/],
  ['reviewed opening correction', input => { input.draft.openingResolution = { amount: 1490 }; }, /opening-balance/],
  ['pending Owner approval', input => { input.store.bankReconciliationApprovals = { A: { draftId: input.draft.id, status: 'pending' } }; }, /pending Owner approval/],
  ['affected saved resolution', input => { input.draft.resolutions['bank-1'] = { action: 'accept_match' }; }, /saved or grouped/],
  ['affected grouped decision from another row', input => { input.draft.resolutions['bank-0'] = { action: 'link_multiple_bank_entries', bankRowIds: ['bank-0', 'bank-1'] }; }, /saved or grouped/],
  ['matching exclusion', input => { input.draft.matchingExclusions['bank-1'] = {}; }, /matching exclusion/],
  ['automatically matched current view', input => { input.reviewRows = [{ id: 'bank-1', status: 'matched' }]; }, /not unresolved/],
  ['unknown current view', input => { input.reviewRows = []; }, /not unresolved/]
]) test('fails closed for ' + name, () => {
  const input = fixture(); change(input); const before = clone(input.store);
  assert.throws(() => planBankImportSourceRepair(input), pattern);
  assert.deepEqual(input.store, before);
});

test('a fresh unresolved view is allowed but a saved resolution on it is not', () => {
  const input = fixture(); input.reviewRows = [{ id: 'bank-1', status: 'amount_mismatch' }];
  assert.equal(planBankImportSourceRepair(input).changed, true);
  input.reviewRows[0].resolution = { action: 'accept_match' };
  assert.throws(() => planBankImportSourceRepair(input), /not unresolved/);
});

test('stale draft and changed accounting state invalidate preview before apply', () => {
  const input = fixture(), fingerprint = draftFingerprint(input.draft), plan = planBankImportSourceRepair(input);
  input.draft.periodRemark = 'New user comment';
  assert.throws(() => planBankImportSourceRepair({ ...input, expectedDraftFingerprint: fingerprint }), /draft changed/);
  assert.throws(() => applyBankImportSourceRepair({ ...input, expectedFingerprint: plan.fingerprint, actor: 'owner', reason: 'Fix' }), /changed/);
  const second = fixture(), secondPlan = planBankImportSourceRepair(second);
  second.store.adjustments.push({ id: 'NEW', amount: 1 });
  assert.throws(() => applyBankImportSourceRepair({ ...second, expectedFingerprint: secondPlan.fingerprint, actor: 'owner', reason: 'Fix' }), /changed/);
});

test('finalized transaction identity blocks repair even when its amount and balance differ', () => {
  const input = fixture();
  input.store.bankStatements[account] = { transactions: { existing: { date: '2026-10-19', reference: ref, debit: 999, credit: 0, balance: 42 } } };
  assert.throws(() => planBankImportSourceRepair(input), /finalized history/);
});

test('finalized carried rows and shared PERSONAL bank books cannot disguise changed amounts', () => {
  const input = fixture();
  input.store.bankStatements['PERSONAL|' + account] = { imports: [{ finalizedAt: '2026-10-20', carriedReconciliationRows: [{ id: 'bank-99', bank: { date: '2026-10-19', reference: ref, debit: 90, credit: 0 } }] }] };
  assert.throws(() => planBankImportSourceRepair(input), /finalized reconciliation evidence/);
});

for (const [name, prepare] of [
  ['transfer', input => { input.store.transfers.push({ id: 'TR-TARGET', reconciliationDraft: input.draft.id, bankRowId: 'bank-1', amount: 90 }); }],
  ['excluded adjustment', input => { input.store.adjustments.push({ id: 'ADJ-TARGET', reconciliationDraft: input.draft.id, bankRowId: 'bank-1', amount: -90, accountingExcluded: true }); }],
  ['expense source', input => { input.store.expenses['EX-TARGET'] = { amount: 90, reconciliationSource: { draftId: input.draft.id, rowId: 'bank-1' } }; }],
  ['expense payment', input => { input.store.expenses['EX-KEEP'].payments.push({ amount: 90, bankReconciliationDraft: input.draft.id, bankReconciliationRow: 'bank-1' }); }],
  ['receipt evidence', input => { input.store.receipts = [{ amount: 90, bankReconciliationEvidence: { draftId: input.draft.id, rowId: 'bank-1', reference: ref } }]; }],
  ['provisional date override', input => { input.store.bankDateOverrides = { T: { provisional: true, reconciliationDraft: input.draft.id, bankReference: ref } }; }],
  ['grouped link', input => { input.store.bankReconciliationLinks = { T: { reconciliationDraft: input.draft.id, bankTransactions: [{ bankRowId: 'bank-1', reference: ref }] } }; }],
  ['unattributed posting', input => { input.store.adjustments.push({ amount: -90, reconciliationDraft: input.draft.id }); }],
  ['other draft posting with same physical bank reference', input => { input.store.receipts = [{ amount: 490, account, reconciliationDraft: 'BRD-OTHER', bankReference: ref }]; }]
]) test('rejects affected ' + name + ' without deleting or rewriting it', () => {
  const input = fixture(); prepare(input); const before = clone(input.store);
  assert.throws(() => planBankImportSourceRepair(input), /source|link/);
  assert.deepEqual(input.store, before);
});

test('same reference in a different bank account does not block this physical source repair', () => {
  const input = fixture();
  input.store.bankStatements['Axis Bank 3448'] = { transactions: { other: { date: '2026-10-19', reference: ref, debit: 490, credit: 0, balance: 42 } } };
  assert.equal(planBankImportSourceRepair(input).changed, true);
});

test('a source repair needs a fresh reviewed fingerprint, authenticated actor and audit reason', () => {
  const input = fixture(), plan = planBankImportSourceRepair(input);
  assert.throws(() => applyBankImportSourceRepair(input), /reviewed source/);
  assert.throws(() => applyBankImportSourceRepair({ ...input, expectedFingerprint: plan.fingerprint }), /actor and repair reason/);
  assert.throws(() => applyBankImportSourceRepair({ ...input, expectedFingerprint: plan.fingerprint, actor: 'owner', reason: 'Fix', at: 'yesterday' }), /timestamp/);
});

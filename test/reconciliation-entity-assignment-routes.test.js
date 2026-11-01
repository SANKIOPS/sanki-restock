'use strict';
const test = require('node:test'), assert = require('node:assert/strict'), fs = require('node:fs'), os = require('node:os'), path = require('node:path');
const source = require.resolve('../modules/rental-source.json'); require.cache[source] = { id: source, filename: source, loaded: true, exports: { id: 'SYNTHETIC-ENTITY', rows: [] } };
const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'recon-entity-assignment-')); process.env.DATA_PATH = path.join(temp, 'data.json');
const file = path.join(temp, 'expenses.json'); fs.writeFileSync(file, '{}');
const { router } = require('../modules/expenses'), clone = value => JSON.parse(JSON.stringify(value)), read = () => JSON.parse(fs.readFileSync(file, 'utf8')), write = value => fs.writeFileSync(file, JSON.stringify(value));
const entityAssignment = require('../modules/reconciliation-entity-assignment');
const date = '2098-08-01', draftId = 'BRD-BULK-ENTITY'; let baseline;
function invoke(route, body = {}, options = {}) {
  return new Promise((resolve, reject) => {
    let status = 200, done = false; const finish = body => { if (!done) { done = true; resolve({ status, body }); } };
    const role = options.role || 'owner', req = { method: options.method || 'POST', path: route, url: route, originalUrl: route, baseUrl: '', body, query: options.query || {}, headers: {}, get: () => '', user: { role, roles: [role], username: role === 'admin' ? 'prashant' : 'synthetic-' + role, permissions: options.permissions || {} } };
    const res = { statusCode: 200, headersSent: false, status(n) { status = n; this.statusCode = n; return this; }, setHeader() {}, getHeader() {}, removeHeader() {}, json(b) { this.headersSent = true; finish(b); return this; }, send(b) { finish(b); return this; }, end: finish };
    router.handle(req, res, error => error ? reject(error) : finish(undefined));
  });
}
test.before(async () => { assert.equal((await invoke('/api/expenses/config', {}, { method: 'GET' })).status, 200); baseline = read(); });
test.after(() => fs.rmSync(temp, { recursive: true, force: true }));
function seed({ shared = false, incoming = false } = {}) {
  const account = shared ? 'IndusInd Bank 7883' : 'Axis Bank 3448', nature = shared ? 'SAMAST' : 'SANKI', store = clone(baseline);
  Object.assign(store, { expenses: {}, receipts: [], transfers: [], adjustments: [], vendorAdvances: [], vendorOpeningPayables: [], salesRefunds: [], expenseRefunds: [], receivables: {}, loanRepayments: [], reconciliationExpenses: [], bankTruthMovements: [], paytmSettlements: [], paytmPayoutPostings: [], procurementAccounting: { paymentsByPo: {} }, bankStatements: {}, bankDateOverrides: {}, bankReconciliationLinks: {}, bankReconciliationApprovals: {}, bankMovementAuthority: {}, bankAmountAuthorityApplications: {}, bankAmountAuthorityProposals: {}, openingBalances: { [account]: 10000 }, openingBalancesByNature: {}, physicalBankOpeningBalances: { [account]: 10000 }, auditLog: [], auditSeq: 0 });
  const transactions = [100, 200, 300].map((amount, i) => ({ date, debit: incoming ? 0 : amount, credit: incoming ? amount : 0, balance: 10000 + (incoming ? 1 : -1) * [100, 300, 600][i], reference: 'BULK-ENTITY-' + i, description: 'Unmatched synthetic bank row ' + i }));
  store.bankReconciliationDrafts = { [draftId]: { id: draftId, account, nature, transactions, summary: { from: date, to: date, openingBalance: 10000, closingBalance: transactions[2].balance, totalDebits: incoming ? 0 : 600, totalCredits: incoming ? 600 : 0, validated: true }, resolutions: {}, transactionOwnership: {}, createdAt: date + 'T10:00:00Z', createdBy: 'synthetic-owner', expiresAt: '2099-01-01T00:00:00Z', matchingPolicy: 'balanced_date_amount_v5' } };
  for (const [name, contents] of Object.entries({ 'salary.json': { salaryPayments: [], advances: {}, employees: {} }, 'salary-samast.json': {}, 'incentives.json': {}, 'procurement.json': { pos: {} }, 'credit-cards.json': { cards: {}, payments: [], statements: {} } })) fs.writeFileSync(path.join(temp, name), JSON.stringify(contents));
  write(store); return store;
}
function bulk(patch = {}, actor = {}) { const draft = read().bankReconciliationDrafts[draftId]; return invoke('/api/expenses/bank-statements/assign-entity-bulk', { draftId, account: draft.account, nature: draft.nature, targetNature: 'SAMAST', reason: 'These selected expenses belong to SAMAST Results', rowIds: ['bank-0', 'bank-1'], dryRun: true, ...patch }, actor); }
async function assign(patch = {}, actor = {}) { const preview = await bulk(patch, actor); assert.equal(preview.status, 200, JSON.stringify(preview.body)); return bulk({ ...patch, dryRun: false, fingerprint: preview.body.fingerprint }, actor); }
async function unchanged(action, expected) { const before = fs.readFileSync(file, 'utf8'), result = await action(); assert.equal(result.status, expected, JSON.stringify(result.body)); assert.equal(fs.readFileSync(file, 'utf8'), before); return result; }

test('bulk preview and GET are read-only; apply changes only selected draft metadata and explicit audit', async () => {
  const before = seed(); const raw = fs.readFileSync(file, 'utf8'), preview = await bulk(); assert.equal(preview.status, 200, JSON.stringify(preview.body)); assert.equal(fs.readFileSync(file, 'utf8'), raw);
  const got = await invoke('/api/expenses/bank-statements', {}, { method: 'GET', query: { account: 'Axis Bank 3448', nature: 'SANKI' } }); assert.equal(got.status, 200); assert.equal(fs.readFileSync(file, 'utf8'), raw); assert.equal(got.body.draft.rows.filter(row => row.entityAssignmentEditable).length, 3);
  const applied = await bulk({ dryRun: false, fingerprint: preview.body.fingerprint }); assert.equal(applied.status, 200, JSON.stringify(applied.body)); assert.equal(applied.body.updated, 2);
  const after = read(), draft = after.bankReconciliationDrafts[draftId]; assert.deepEqual(Object.keys(draft.entityAssignments).sort(), ['bank-0', 'bank-1']); assert.equal(draft.entityAssignments['bank-0'].nature, 'SAMAST'); assert.equal(draft.entityAssignments['bank-2'], undefined); assert.deepEqual(draft.transactions, before.bankReconciliationDrafts[draftId].transactions); assert.deepEqual(draft.resolutions, {}); assert.deepEqual(draft.transactionOwnership, {});
  for (const key of ['expenses', 'receipts', 'transfers', 'adjustments', 'bankStatements', 'openingBalances', 'bankReconciliationLinks', 'bankDateOverrides']) assert.deepEqual(after[key], before[key], key + ' preserved');
  const preservedBefore = clone(before), preservedAfter = clone(after); for (const key of ['bankReconciliationDrafts', 'auditLog', 'auditSeq']) { delete preservedBefore[key]; delete preservedAfter[key]; } assert.deepEqual(preservedAfter, preservedBefore, 'all non-draft, non-audit store fields remain exact');
  assert.equal(after.auditLog.filter(row => row.action === 'BANK_RECONCILIATION_ENTITIES_ASSIGNED').length, 1);
  await unchanged(() => bulk({ dryRun: false, fingerprint: preview.body.fingerprint }), 409);
});

test('new expense uses selected Results entity but preserves physical paying bank and unrelated sources', async () => {
  seed(); assert.equal((await assign()).status, 200);
  const result = await invoke('/api/expenses/bank-statements/create-expense', { draftId, rowId: 'bank-0', ledger: 'FOOD EXPENSE', vendor: 'Synthetic vendor', particulars: 'Selected SAMAST expense', billPhoto: '/synthetic-proof.jpg', remark: 'Verified bank expense' });
  assert.equal(result.status, 200, JSON.stringify(result.body)); assert.equal(result.body.expense.nature, 'SAMAST'); assert.equal(result.body.expense.payments[0].account, 'Axis Bank 3448'); assert.equal(result.body.expense.payments[0].amount, 100); assert.deepEqual(read().bankReconciliationDrafts[draftId].transactionOwnership, {}); assert.deepEqual(read().bankStatements, {});
});

test('category expense uses bulk choice; untagged shared expenses still require an entity', async () => {
  seed(); assert.equal((await assign()).status, 200);
  const result = await invoke('/api/expenses/bank-statements/resolve', { draftId, rowId: 'bank-0', action: 'create_adjustment', category: 'FOOD EXPENSE', reason: 'Verified expense' });
  assert.equal(result.status, 200, JSON.stringify(result.body)); assert.equal(read().bankReconciliationDrafts[draftId].resolutions['bank-0'].postingNature, 'SAMAST'); assert.deepEqual(read().adjustments, []);
  seed({ shared: true }); await unchanged(() => invoke('/api/expenses/bank-statements/resolve', { draftId, rowId: 'bank-0', action: 'create_adjustment', category: 'FOOD EXPENSE', reason: 'Unclassified expense' }), 409);
});

test('shared bulk choices require authorization and never change a prior manual tag', async () => {
  seed({ shared: true }); await unchanged(() => bulk({ targetNature: 'PERSONAL' }, { role: 'admin' }), 403);
  assert.equal((await assign({ targetNature: 'PERSONAL' })).status, 200); const draft = read().bankReconciliationDrafts[draftId]; assert.equal(draft.transactionOwnership['bank-0'].nature, 'PERSONAL'); assert.equal(draft.transactionOwnership['bank-1'].nature, 'PERSONAL'); assert.equal(draft.transactionOwnership['bank-2'], undefined);
  await unchanged(() => bulk({ targetNature: 'SAMAST' }), 409);
  seed({ shared: true }); const sanki = await assign({ targetNature: 'SANKI' }); assert.equal(sanki.status, 200, JSON.stringify(sanki.body));
  assert.equal(read().bankReconciliationDrafts[draftId].transactionOwnership['bank-0'].nature, 'SANKI');
});

test('a protected member rejects the entire batch; prior reviews and history cannot be overwritten', async () => {
  for (const protect of [
    s => { s.bankReconciliationDrafts[draftId].resolutions['bank-1'] = { action: 'exclude', reason: 'Owner exclusion remains' }; },
    s => { s.bankReconciliationDrafts[draftId].matchingExclusions = { 'bank-1': { reason: 'Reviewed mismatch' } }; },
    s => { s.bankReconciliationDrafts[draftId].originalStatementPeriod = { from: date, to: date }; },
    s => { s.bankReconciliationDrafts[draftId].reconstructedFromFinalized = true; },
    s => { s.bankReconciliationApprovals.A = { draftId, status: 'pending' }; },
    s => { s.bankReconciliationDrafts[draftId].transactions[1].description = 'UPI REVERSAL failed payment'; },
    s => { s.adjustments = [{ id: 'ADJ-EXISTING', nature: 'SANKI', account: 'Axis Bank 3448', date, amount: -200, note: 'Prior source' }]; },
    s => { s.bankStatements['Axis Bank 3448'] = { transactions: { final: { ...s.bankReconciliationDrafts[draftId].transactions[1], id: 'BTX-OLD' } }, imports: [{ id: 'BST-PRESERVE', finalizedAt: date }] }; }
  ]) { const s = seed(); protect(s); write(s); await unchanged(() => bulk(), 409); }
});

test('bulk rejects forged scope, duplicate identifiers, stale evidence and unauthorized actors atomically', async () => {
  for (const [patch, status] of [[{ targetNature: {} }, 400], [{ rowIds: ['bank-0', 'bank-0'] }, 400], [{ rowIds: [] }, 400], [{ rowIds: ['app-0'] }, 400], [{ rowIds: ['bank-99'] }, 409], [{ reason: '' }, 400], [{ nature: 'PERSONAL' }, 409], [{ account: 'ICICI Bank 0993' }, 409], [{ dryRun: false }, 409]]) { seed(); await unchanged(() => bulk(patch), status); }
  for (const role of ['claimant', 'accounting', 'samast_accounting']) { seed(); await unchanged(() => bulk({}, { role }), 403); }
  seed(); const review = await bulk(), changed = read(); changed.bankReconciliationDrafts[draftId].transactions[1].debit = 201; write(changed); await unchanged(() => bulk({ dryRun: false, fingerprint: review.body.fingerprint }), 409);
  seed(); assert.equal((await assign({}, { role: 'admin' })).status, 200);
});

function incoming(patch = {}, actor = {}) { return invoke('/api/expenses/bank-statements/create-incoming', { draftId, rowId: 'bank-0', sourceKind: 'external', source: 'Synthetic refund sender', receiptType: 'refund', category: 'Refund Received', note: 'Verified incoming refund without expense classification', ...patch }, actor); }

test('a matched draft row with missing ownership remains assignable without reopening its bank match', () => {
  const draft = { transactions: [{ date, debit: 100, credit: 0, reference: 'MATCHED-ENTITY' }], entityAssignments: {}, resolutions: {} };
  const row = { id: 'bank-0', status: 'matched', bank: draft.transactions[0], app: { id: 'EX-MATCHED/PAY-001' }, ownershipUnresolved: true, entityTag: { nature: '', source: 'none', sourceConflict: false }, reconciliationConflicts: [] };
  assert.equal(entityAssignment.editable(draft, row), true);
  assert.equal(row.status, 'matched');
  assert.equal(row.app.id, 'EX-MATCHED/PAY-001');
});

test('shared incoming receipt derives unique physical owner without adding an expense tag or modifying existing records', async () => {
  const before = seed({ shared: true, incoming: true }), made = await incoming(); assert.equal(made.status, 200, JSON.stringify(made.body)); assert.equal(made.body.receipt.nature, 'PERSONAL'); assert.equal(made.body.receipt.amount, 100); assert.ok(made.body.receipt.receiptAccount); assert.deepEqual(read().bankReconciliationDrafts[draftId].transactionOwnership, {});
  assert.deepEqual(read().expenses, before.expenses); assert.deepEqual(read().transfers, before.transfers); assert.deepEqual(read().bankStatements, before.bankStatements); assert.equal(made.body.rows.find(row => row.id === 'bank-0').ownershipUnresolved, false);
  await unchanged(() => incoming(), 400);
});

test('incoming scope inference preserves account permission, Owner income gate, ambiguity and existing manual attribution', async () => {
  seed({ shared: true, incoming: true }); await unchanged(() => incoming({}, { role: 'admin' }), 403);
  seed({ shared: true, incoming: true }); await unchanged(() => incoming({}, { role: 'admin', permissions: { personalAccounting: true } }), 403);
  seed({ incoming: true }); await unchanged(() => incoming({}, { role: 'admin' }), 403);
  seed({ shared: true, incoming: true }); const s = read(); s.bankReconciliationDrafts[draftId].transactionOwnership['bank-0'] = { nature: 'SAMAST', source: 'manual', reason: 'Existing verified ownership', by: 'owner' }; write(s);
  const made = await incoming(); assert.equal(made.status, 200, JSON.stringify(made.body)); assert.equal(made.body.receipt.nature, 'SAMAST'); assert.equal(made.body.receipt.receiptAccount, undefined); assert.deepEqual(read().bankReconciliationDrafts[draftId].transactionOwnership, s.bankReconciliationDrafts[draftId].transactionOwnership);
  seed({ shared: true, incoming: true }); const ambiguous = read(); ambiguous.receipts = ['SAMAST', 'PERSONAL'].map((nature, i) => ({ id: 'REC-CANDIDATE-' + i, nature, account: 'IndusInd Bank 7883', date, amount: 100, receiptType: 'refund', category: 'Refund Received', source: 'Uncertain source ' + i })); write(ambiguous); const raw = fs.readFileSync(file, 'utf8'), denied = await incoming(); assert.ok(denied.status >= 400, JSON.stringify(denied.body)); assert.equal(fs.readFileSync(file, 'utf8'), raw);
});

test('new incoming account provenance is rechecked at finalization without changing protected sources', async () => {
  seed({ shared: true, incoming: true }); assert.equal((await incoming()).status, 200); const s = read(); s.receipts[0].receiptAccount.nature = 'SAMAST'; write(s);
  await unchanged(() => invoke('/api/expenses/bank-statements/finalize', { draftId }), 409);
});

test('unclassified shared receipts finalize once without any preliminary entity assignment', async () => {
  seed({ shared: true, incoming: true });
  for (const rowId of ['bank-0', 'bank-1', 'bank-2']) { const made = await incoming({ rowId }); assert.equal(made.status, 200, JSON.stringify(made.body)); }
  const before = read(), finalized = await invoke('/api/expenses/bank-statements/finalize', { draftId });
  assert.equal(finalized.status, 200, JSON.stringify(finalized.body)); assert.deepEqual(read().receipts, before.receipts); assert.equal(read().receipts.length, 3); assert.deepEqual(read().expenses, {}); assert.deepEqual(read().adjustments, []); assert.deepEqual(read().transfers, []);
  const repeat = await invoke('/api/expenses/bank-statements/finalize', { draftId }); assert.ok(repeat.status >= 400); assert.equal(read().receipts.length, 3);
});

test('removed provenance, changed manual attribution, and newly linked sources block inferred receipt finalization', async () => {
  for (const change of [
    s => { delete s.bankReconciliationDrafts[draftId].resolutions['bank-0'].receiptAccount; },
    s => { s.bankReconciliationDrafts[draftId].transactionOwnership['bank-0'] = { nature: 'SAMAST', source: 'manual', reason: 'Different reviewed ownership' }; },
    s => { s.receipts.push({ id: 'REC-NEW-SOURCE', nature: 'SAMAST', account: 'IndusInd Bank 7883', date, amount: 100, receiptType: 'refund', category: 'Refund Received', source: 'Existing bank refund', bankReference: 'BULK-ENTITY-0' }); }
  ]) { seed({ shared: true, incoming: true }); assert.equal((await incoming()).status, 200); const s = read(); change(s); write(s); await unchanged(() => invoke('/api/expenses/bank-statements/finalize', { draftId }), 409); }
});

test('bulk assignments bind exact bank evidence and cannot leak to a replaced row or an unselected identical row', async () => {
  const s = seed(); s.bankReconciliationDrafts[draftId].transactions[2] = { ...s.bankReconciliationDrafts[draftId].transactions[0] }; write(s);
  assert.equal((await assign({ rowIds: ['bank-0'] })).status, 200); assert.equal(read().bankReconciliationDrafts[draftId].entityAssignments['bank-2'], undefined);
  const changed = read(); changed.bankReconciliationDrafts[draftId].transactions[0].reference = 'DIFFERENT-PHYSICAL-ROW'; write(changed);
  await unchanged(() => bulk({ rowIds: ['bank-0'] }), 409);
  await unchanged(() => invoke('/api/expenses/bank-statements/create-expense', { draftId, rowId: 'bank-0', nature: 'SAMAST', ledger: 'FOOD EXPENSE', vendor: 'Synthetic vendor', particulars: 'New source', billPhoto: '/synthetic.jpg', remark: 'Reviewed' }), 409);
  await unchanged(() => invoke('/api/expenses/bank-statements/resolve', { draftId, rowId: 'bank-0', nature: 'SAMAST', action: 'create_adjustment', category: 'FOOD EXPENSE', reason: 'Reviewed' }), 409);
});

test('PERSONAL bulk choices cannot be bypassed by explicit expense or category entity overrides', async () => {
  for (const request of [
    () => invoke('/api/expenses/bank-statements/create-expense', { draftId, rowId: 'bank-0', nature: 'SAMAST', ledger: 'FOOD EXPENSE', vendor: 'Synthetic vendor', particulars: 'Attempted personal override', billPhoto: '/synthetic.jpg', remark: 'Admin attempts override' }, { role: 'admin' }),
    ...['create_adjustment', 'create_split_adjustment'].map(action => () => invoke('/api/expenses/bank-statements/resolve', { draftId, rowId: 'bank-0', nature: 'SAMAST', action, category: 'FOOD EXPENSE', reason: 'Admin attempts override', splits: [{ category: 'FOOD EXPENSE', amount: 100 }] }, { role: 'admin' }))
  ]) {
    seed(); assert.equal((await assign({ targetNature: 'PERSONAL' })).status, 200);
    await unchanged(request, 403);
    const saved = read(); assert.deepEqual(saved.expenses, {}); assert.deepEqual(saved.adjustments, []); assert.deepEqual(saved.bankReconciliationDrafts[draftId].resolutions, {});
    assert.equal(saved.bankReconciliationDrafts[draftId].entityAssignments['bank-0'].nature, 'PERSONAL');
  }
});

async function stageSelectedCategoryExpenses() {
  seed(); assert.equal((await assign({ rowIds: ['bank-0', 'bank-1', 'bank-2'] })).status, 200);
  for (const rowId of ['bank-0', 'bank-1', 'bank-2']) {
    const result = await invoke('/api/expenses/bank-statements/resolve', { draftId, rowId, action: 'create_adjustment', category: 'FOOD EXPENSE', reason: 'Synthetic selected entity expense' });
    assert.equal(result.status, 200, JSON.stringify(result.body));
  }
  assert.deepEqual(read().adjustments, []);
}

test('finalization rejects stale bulk assignments before posting, approval or any other store write', async () => {
  for (const mutate of [
    draft => { draft.transactions[0].reference = 'CHANGED-AFTER-REVIEW'; },
    draft => { draft.transactions[0].debit = 101; },
    draft => { draft.transactions[0].description = 'Changed bank source description'; },
    draft => { draft.transactions[0].source = { file: 'different-statement.pdf', page: 2 }; },
    draft => { draft.transactions.splice(0, 1); }
  ]) {
    await stageSelectedCategoryExpenses(); const changed = read(); mutate(changed.bankReconciliationDrafts[draftId]); write(changed);
    for (const role of ['owner', 'admin']) {
      const rejected = await unchanged(() => invoke('/api/expenses/bank-statements/finalize', { draftId, deferClosingBalance: true }, { role }), 409);
      assert.match(rejected.body.error, /changed after its bulk entity assignment/);
    }
    const saved = read(); assert.deepEqual(saved.adjustments, []); assert.deepEqual(saved.expenses, {}); assert.deepEqual(saved.receipts, []); assert.deepEqual(saved.transfers, []); assert.deepEqual(saved.bankStatements, {}); assert.deepEqual(saved.bankReconciliationApprovals, {});
  }
});

test('unchanged selected category expenses still finalize once with the verified Results entity', async () => {
  await stageSelectedCategoryExpenses(); const result = await invoke('/api/expenses/bank-statements/finalize', { draftId });
  assert.equal(result.status, 200, JSON.stringify(result.body));
  const saved = read(); assert.equal(saved.adjustments.length, 3); assert.ok(saved.adjustments.every(row => row.nature === 'SAMAST' && row.account === 'Axis Bank 3448')); assert.deepEqual(saved.expenses, {}); assert.deepEqual(saved.receipts, []); assert.deepEqual(saved.transfers, []);
  await unchanged(() => invoke('/api/expenses/bank-statements/finalize', { draftId }), 404);
});

test('receipt reporting entity preserves physical bank ownership and cannot bypass PERSONAL authorization', async () => {
  seed({ incoming: true }); assert.equal((await assign({ targetNature: 'PERSONAL', rowIds: ['bank-0'] })).status, 200);
  await unchanged(() => incoming({ receiptType: 'bank_interest', category: 'Bank Interest Received' }, { role: 'admin' }), 403);
  const made = await incoming({ receiptType: 'bank_interest', category: 'Bank Interest Received' }); assert.equal(made.status, 200, JSON.stringify(made.body));
  assert.equal(made.body.receipt.nature, 'SANKI'); assert.equal(made.body.receipt.account, 'Axis Bank 3448'); assert.equal(made.body.receipt.reportingNature, 'PERSONAL'); assert.equal(read().bankReconciliationDrafts[draftId].resolutions['bank-0'].reportingNature, 'PERSONAL');
  await unchanged(() => invoke('/api/expenses/bank-statements/finalize', { draftId }, { role: 'admin' }), 409);
  for (const rowId of ['bank-1', 'bank-2']) assert.equal((await incoming({ rowId })).status, 200);
  const before = read(), finalized = await invoke('/api/expenses/bank-statements/finalize', { draftId }); assert.equal(finalized.status, 200, JSON.stringify(finalized.body)); assert.deepEqual(read().receipts, before.receipts);
});

test('receipt reporting assignment and its linked source are rechecked before any finalization write', async () => {
  for (const change of [
    s => { s.receipts[0].reportingNature = 'PERSONAL'; },
    s => { delete s.receipts[0].reportingNature; },
    s => { delete s.bankReconciliationDrafts[draftId].resolutions['bank-0'].reportingNature; },
    s => { delete s.bankReconciliationDrafts[draftId].entityAssignments['bank-0']; },
    s => { s.receipts[0].bankReconciliationEvidence.reference = 'DIFFERENT-RECEIPT-SOURCE'; },
    s => { s.receipts[0].amount = 101; },
    s => { s.receipts[0].nature = 'SAMAST'; },
    s => { s.receipts.push({ id: 'REC-NEW-SOURCE', nature: 'SANKI', account: 'Axis Bank 3448', date, amount: 100, receiptType: 'refund', category: 'Refund Received', source: 'Existing bank refund', bankReference: 'BULK-ENTITY-0' }); }
  ]) {
    seed({ incoming: true }); assert.equal((await assign({ rowIds: ['bank-0'] })).status, 200); assert.equal((await incoming()).status, 200);
    const s = read(); change(s); write(s); const rejected = await unchanged(() => invoke('/api/expenses/bank-statements/finalize', { draftId }), 409);
    assert.match(rejected.body.error, /reporting entity|changed source evidence/); assert.deepEqual(read().bankStatements, {}); assert.deepEqual(read().bankReconciliationApprovals, {});
  }
});

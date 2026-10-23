'use strict';

// Real router/middleware dispatch, synthetic data only. These tests must never
// read a live accounting snapshot or the rental source supplied with the app.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const rentalSource = require.resolve('../modules/rental-source.json');
require.cache[rentalSource] = { id: rentalSource, filename: rentalSource, loaded: true, exports: { id: 'OWN-TRANSFER-SYNTHETIC', rows: [] } };
const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'own-account-transfer-adversarial-'));
process.env.DATA_PATH = path.join(temp, 'data.json');
const file = path.join(temp, 'expenses.json');
fs.writeFileSync(file, JSON.stringify({ expenses: {}, receipts: [], receivables: {}, oneTimeMigrations: {} }));
const { router } = require('../modules/expenses');
const clone = x => JSON.parse(JSON.stringify(x));
const read = () => JSON.parse(fs.readFileSync(file, 'utf8'));
const write = x => fs.writeFileSync(file, JSON.stringify(x));
const shared = 'IndusInd Bank 7883', counterpart = 'ICICI Bank 0993', date = '2098-10-20';
const owner = { role: 'owner', username: 'owner-user' };
const personalAdmin = { role: 'admin', username: 'prashant', permissions: { personalAccounting: true } };
let seq = 0;

function dispatch(route, options = {}) {
  const method = options.method || 'POST', role = options.role || 'owner';
  const req = { method, path: route, url: route, originalUrl: route, baseUrl: '', body: options.body || {}, query: options.query || {}, params: {}, headers: { 'user-agent': 'Synthetic own-account safety test' }, get(name) { return this.headers[String(name).toLowerCase()] || ''; }, user: { role, roles: [role], username: options.username || 'owner-user', permissions: options.permissions || {} } };
  return new Promise((resolve, reject) => {
    let status = 200, finished = false;
    const headers = {}, finish = body => { if (!finished) { finished = true; resolve({ status, body }); } };
    const res = { statusCode: 200, headersSent: false, status(code) { this.statusCode = status = code; return this; }, setHeader(name, value) { headers[String(name).toLowerCase()] = value; }, getHeader(name) { return headers[String(name).toLowerCase()]; }, removeHeader(name) { delete headers[String(name).toLowerCase()]; }, json(body) { this.headersSent = true; finish(body); return this; }, send(body) { this.headersSent = true; finish(body); return this; }, end(body) { this.headersSent = true; finish(body); return this; } };
    router.handle(req, res, error => error ? reject(error) : finish(undefined));
  });
}
let baseline;
test.before(async () => { assert.equal((await dispatch('/api/expenses/config', { method: 'GET', ...owner })).status, 200); baseline = read(); });
test.after(() => fs.rmSync(temp, { recursive: true, force: true }));

function draft(extra = {}) {
  seq++;
  return { id: 'BRD-OWN-ADVERSARIAL-' + seq, nature: 'SAMAST', account: shared,
    transactions: [{ date, description: 'Transfer between registered own accounts', reference: '900000' + String(seq).padStart(6, '0'), debit: 1000, credit: 0, balance: 9000 }],
    summary: { from: date, to: date, openingBalance: 10000, closingBalance: 9000, totalDebits: 1000, totalCredits: 0, validated: true },
    resolutions: {}, matchingPolicy: 'balanced_date_amount_v5', createdBy: 'owner-user', createdAt: date + 'T10:00:00.000Z', expiresAt: '2099-01-01T00:00:00Z', ...extra };
}
async function seed(current = draft(), extra = {}) {
  const store = Object.assign(clone(baseline), { expenses: {}, receipts: [], receivables: {}, transfers: [], adjustments: [], reconciliationExpenses: [], vendorAdvances: [], vendorOpeningPayables: [], loanRepayments: [], salesRefunds: [], bankTruthMovements: [], bankDateOverrides: {}, bankReconciliationLinks: {}, bankReconciliationDrafts: { [current.id]: clone(current) }, bankReconciliationApprovals: {}, bankStatements: {}, openingBalances: {}, openingBalancesByNature: { PERSONAL: { [shared]: 10000, [counterpart]: 0, 'ICICI Bank 0992': 0 } }, physicalBankOpeningBalances: { [shared]: 10000 }, auditLog: [], auditSeq: 0, transferSeq: 0, adjSeq: 0 }, extra);
  write(store);
  assert.equal((await dispatch('/api/expenses/config', { method: 'GET', ...owner })).status, 200);
  return read().bankReconciliationDrafts[current.id];
}
function body(current, extra = {}) { return { draftId: current.id, rowId: 'bank-0', action: 'create_internal_transfer', ownAccountTransfer: true, otherNature: 'PERSONAL', otherAccount: counterpart, principalAmount: 1000, chargeAmount: 0, reason: 'Verified movement between registered own bank accounts', ...extra }; }
function resolve(current, extra = {}, who = owner) { return dispatch('/api/expenses/bank-statements/resolve', { ...who, body: body(current, extra) }); }
async function view(current, who = owner) { const result = await dispatch('/api/expenses/bank-statements', { method: 'GET', ...who, query: { nature: current.nature, account: current.account, draftId: current.id } }); assert.equal(result.status, 200, JSON.stringify(result.body)); return result.body.draft; }
function financialSnapshot() { const s = read(); return clone(Object.fromEntries(['expenses', 'transfers', 'receipts', 'adjustments', 'reconciliationExpenses', 'bankTruthMovements', 'bankStatements', 'bankDateOverrides', 'bankReconciliationLinks'].map(k => [k, s[k]]))); }

test('GET and account configuration never infer or persist a transaction entity before an explicit transfer decision', async () => {
  const current = await seed(), before = fs.readFileSync(file, 'utf8');
  const initial = await view(current);
  assert.equal(initial.rows[0].entityTag.nature || '', '');
  assert.equal(initial.ownershipUnresolved, 1);
  const config = await dispatch('/api/expenses/config', { method: 'GET', ...owner });
  assert.equal(config.status, 200);
  assert.equal(fs.readFileSync(file, 'utf8'), before);
  assert.deepEqual(read().bankReconciliationDrafts[current.id].resolutions, {});
  assert.equal(read().bankReconciliationDrafts[current.id].transactionOwnership && read().bankReconciliationDrafts[current.id].transactionOwnership['bank-0'], undefined);
});

test('an explicit valid own-bank transfer derives PERSONAL without posting or using the SAMAST container as ownership', async () => {
  const current = await seed(), before = financialSnapshot(), saved = await resolve(current);
  assert.equal(saved.status, 200, JSON.stringify(saved.body));
  assert.equal(saved.body.rows[0].entityTag.nature, 'PERSONAL');
  assert.equal(saved.body.ownershipUnresolved, 0);
  assert.equal(read().bankReconciliationDrafts[current.id].resolutions['bank-0'].postingNature, 'PERSONAL');
  assert.deepEqual(financialSnapshot(), before, 'review must not create a transfer, bank fee or expense');
});

test('unknown accounts and forged client ownership cannot acquire a trusted own-transfer decision', async () => {
  for (const patch of [
    { otherAccount: 'Unregistered Owner Bank 9999' },
    { otherAccount: 'Paytm Settlement Clearing', otherNature: 'SANKI' },
    { otherAccount: 'Arshpreet 1919', otherNature: 'SANKI' },
    { otherAccount: 'Unregistered Personal Cash' },
    { otherAccount: shared },
    { otherNature: 'SANKI', postingNature: 'SANKI', nature: 'SANKI' }
  ]) {
    const current = await seed(), before = fs.readFileSync(file, 'utf8'), rejected = await resolve(current, patch);
    assert.ok(rejected.status >= 400, JSON.stringify({ patch, response: rejected }));
    assert.equal(fs.readFileSync(file, 'utf8'), before);
  }
});

test('an authorized own-transfer preserves manual SAMAST ownership instead of overriding it with PERSONAL', async () => {
  const current = draft({ transactionOwnership: { 'bank-0': { nature: 'SAMAST', source: 'manual', reason: 'Confirmed original business ownership', by: 'owner-user', at: date + 'T09:00:00Z' } } });
  await seed(current); const before = financialSnapshot(), saved = await resolve(current);
  assert.equal(saved.status, 200, JSON.stringify(saved.body));
  const stored = read().bankReconciliationDrafts[current.id];
  assert.deepEqual(stored.transactionOwnership, current.transactionOwnership);
  assert.equal(stored.resolutions['bank-0'].postingNature, 'SAMAST');
  assert.equal(stored.resolutions['bank-0'].classification, 'owner_withdrawal');
  assert.equal(stored.resolutions['bank-0'].ownAccountTransfer, undefined, 'no inferred PERSONAL provenance is created over the manual tag');
  assert.equal((await view(current)).rows[0].entityTag.nature, 'SAMAST');
  assert.deepEqual(financialSnapshot(), before);
  await seed(current);
  const deniedBefore = fs.readFileSync(file, 'utf8'), denied = await resolve(current, {}, { role: 'admin', username: 'prashant' });
  assert.equal(denied.status, 403);
  assert.equal(fs.readFileSync(file, 'utf8'), deniedBefore);
});

test('a reference-verified SAMAST source cannot be replaced through an own-transfer flag', async () => {
  const current = draft(), expense = { id: 'EX-REFERENCE-AUTHORITY', nature: 'SAMAST', date, vendor: 'Verified merchant', particulars: 'Already paid source transaction', ledger: 'Flowers', type: 'variable', channel: 'Shared', amount: 1000, paidAmount: 1000, status: 'paid', approvedAt: date, payments: [{ id: 'PAY-001', amount: 1000, date, account: shared, bankReference: current.transactions[0].reference }], reimbursementPayments: [] };
  await seed(current, { expenses: { [expense.id]: expense } });
  const initial = await view(current);
  assert.equal(initial.rows[0].entityTag.nature, 'SAMAST');
  assert.equal(initial.rows[0].referenceVerified, true);
  const before = fs.readFileSync(file, 'utf8'), rejected = await resolve(current);
  assert.ok(rejected.status >= 400, JSON.stringify(rejected.body));
  assert.equal(fs.readFileSync(file, 'utf8'), before);
});

test('forged own-transfer flag does not grant private-book or private-account privileges', async () => {
  for (const who of [{ role: 'admin', username: 'unrelated-admin' }, { role: 'admin', username: 'prashant' }, { role: 'accounting', username: 'ordinary-accountant' }, { role: 'claimant', username: 'prashant' }]) {
    const current = await seed(), before = fs.readFileSync(file, 'utf8'), rejected = await resolve(current, {}, who);
    assert.equal(rejected.status, 403, JSON.stringify({ who, response: rejected.body }));
    assert.equal(fs.readFileSync(file, 'utf8'), before);
  }
});

test('the own-account picker does not turn counterpart selection into private-ledger access', async () => {
  await seed();
  for (const who of [{ role: 'admin', username: 'unrelated-admin' }, { role: 'accounting', username: 'ordinary-accountant' }, { role: 'claimant', username: 'prashant' }]) {
    const result = await dispatch('/api/expenses/config', { method: 'GET', ...who });
    assert.equal(result.status, 200);
    assert.deepEqual(result.body.ownTransferAccounts, []);
  }
  const who = { role: 'admin', username: 'prashant' }, config = await dispatch('/api/expenses/config', { method: 'GET', ...who });
  assert.equal(config.status, 200);
  assert.deepEqual(config.body.accountsByNature.PERSONAL, []);
  for (const route of ['/api/expenses/account-ledger', '/api/expenses/bank-statements']) {
    const denied = await dispatch(route, { method: 'GET', ...who, query: { nature: 'PERSONAL', account: 'ICICI Bank 0992' } });
    assert.equal(denied.status, 403);
  }
});

test('undo revokes only the inferred transfer attribution; a later unrelated expense still needs its own entity', async () => {
  const current = await seed(); assert.equal((await resolve(current)).status, 200);
  const undone = await dispatch('/api/expenses/bank-statements/undo', { ...owner, body: { draftId: current.id, rowId: 'bank-0', reason: 'This was not a transfer after checking the source' } });
  assert.equal(undone.status, 200, JSON.stringify(undone.body));
  const after = read().bankReconciliationDrafts[current.id], currentView = await view(current);
  assert.equal(after.resolutions['bank-0'], undefined);
  assert.equal(currentView.rows[0].entityTag.nature || '', '');
  assert.equal(currentView.ownershipUnresolved, 1);
  const before = fs.readFileSync(file, 'utf8'), expense = await dispatch('/api/expenses/bank-statements/resolve', { ...owner, body: body(current, { action: 'create_adjustment', ownAccountTransfer: undefined, otherNature: undefined, otherAccount: undefined, category: 'Flowers' }) });
  assert.ok(expense.status >= 400);
  assert.equal(fs.readFileSync(file, 'utf8'), before);
});

test('directly replacing an own-transfer decision cannot inherit its inferred PERSONAL expense entity', async () => {
  const current = await seed(); assert.equal((await resolve(current)).status, 200);
  const before = fs.readFileSync(file, 'utf8');
  const rejected = await dispatch('/api/expenses/bank-statements/resolve', { ...owner, body: { draftId: current.id, rowId: 'bank-0', action: 'create_adjustment', category: 'Flowers', reason: 'Trying a different accounting treatment without an expense entity' } });
  assert.ok(rejected.status >= 400, JSON.stringify(rejected.body));
  assert.equal(fs.readFileSync(file, 'utf8'), before);
});

test('same-entity own-transfer evidence participates in pending Owner approval fingerprints', async () => {
  const current = await seed(); assert.equal((await resolve(current, {}, personalAdmin)).status, 200);
  const requested = await dispatch('/api/expenses/bank-statements/finalize', { ...personalAdmin, body: { draftId: current.id, deferClosingBalance: true } });
  assert.equal(requested.status, 202, JSON.stringify(requested.body));
  assert.ok(requested.body.approval.ownerPersonalTransferFingerprint, 'PERSONAL-to-PERSONAL inference must be fingerprinted too');
  const store = read(); store.bankReconciliationDrafts[current.id].resolutions['bank-0'].otherAccount = 'ICICI Bank 0992'; write(store);
  const before = financialSnapshot(), rejected = await dispatch('/api/expenses/bank-statements/finalize', { ...owner, body: { draftId: current.id, approveRequestId: requested.body.approval.id, deferClosingBalance: true } });
  assert.equal(rejected.status, 409, JSON.stringify(rejected.body));
  assert.deepEqual(financialSnapshot(), before);
  assert.equal(read().bankReconciliationApprovals[requested.body.approval.id].status, 'pending');
});

test('registry-derived staged transfers cannot be finalized after their retained provenance is removed or forged', async () => {
  for (const tamper of [
    resolution => { resolution.postingNature = 'SAMAST'; },
    resolution => { resolution.ownAccountSelection.version = 'obsolete-registry'; },
    resolution => { resolution.ownAccountTransfer.bankSignature = 'forged-bank-evidence'; },
    resolution => { delete resolution.ownAccountTransfer; }
  ]) {
    const current = await seed(); assert.equal((await resolve(current)).status, 200);
    const store = read(), resolution = store.bankReconciliationDrafts[current.id].resolutions['bank-0'];
    assert.ok(resolution.ownAccountSelection && resolution.ownAccountTransfer, 'the new decision retains exact registry and bank-side provenance');
    // A damaged new decision must not fall back to the SAMAST container.
    tamper(resolution); write(store);
    const before = financialSnapshot(), rejected = await dispatch('/api/expenses/bank-statements/finalize', { ...owner, body: { draftId: current.id, deferClosingBalance: true } });
    assert.equal(rejected.status, 409, JSON.stringify(rejected.body));
    assert.deepEqual(financialSnapshot(), before);
  }
});

test('a self-created incoming transfer cannot replace missing bank-side inference by authenticating itself', async () => {
  const current = draft();
  current.transactions[0] = { ...current.transactions[0], debit: 0, credit: 1000, balance: 11000 };
  current.summary = { ...current.summary, closingBalance: 11000, totalDebits: 0, totalCredits: 1000 };
  await seed(current);
  const created = await dispatch('/api/expenses/bank-statements/create-incoming', { ...owner, body: { draftId: current.id, rowId: 'bank-0', sourceKind: 'internal', ownAccountTransfer: true, fromNature: 'PERSONAL', fromAccount: counterpart, note: 'Verified incoming own-account transfer with bank evidence' } });
  assert.equal(created.status, 200, JSON.stringify(created.body));
  const store = read(), resolution = store.bankReconciliationDrafts[current.id].resolutions['bank-0'], transfer = store.transfers.find(t => t.id === resolution.transferId);
  assert.ok(resolution.ownAccountTransfer && transfer.ownAccountTransfer);
  delete resolution.ownAccountTransfer;
  delete transfer.ownAccountTransfer;
  write(store);
  const before = fs.readFileSync(file, 'utf8'), finalized = await dispatch('/api/expenses/bank-statements/finalize', { ...owner, body: { draftId: current.id, deferClosingBalance: true } });
  assert.equal(finalized.status, 409, JSON.stringify(finalized.body));
  assert.equal(fs.readFileSync(file, 'utf8'), before);
});

test('incoming selection without fallback inference still binds its authoritative manual bank-side evidence', async () => {
  const current = draft({ transactionOwnership: { 'bank-0': { nature: 'SAMAST', source: 'manual', reason: 'Verified business bank receipt', by: 'owner-user', at: date + 'T09:00:00Z' } } });
  current.transactions[0] = { ...current.transactions[0], debit: 0, credit: 1000, balance: 11000 };
  current.summary = { ...current.summary, closingBalance: 11000, totalDebits: 0, totalCredits: 1000 };
  await seed(current);
  const created = await dispatch('/api/expenses/bank-statements/create-incoming', { ...owner, body: { draftId: current.id, rowId: 'bank-0', sourceKind: 'internal', ownAccountTransfer: true, fromNature: 'PERSONAL', fromAccount: counterpart, note: 'Verified owner contribution into business-owned receipt' } });
  assert.equal(created.status, 200, JSON.stringify(created.body));
  const store = read(), resolution = store.bankReconciliationDrafts[current.id].resolutions['bank-0'], transfer = store.transfers.find(t => t.id === resolution.transferId);
  assert.equal(resolution.ownAccountTransfer, undefined);
  assert.equal(resolution.postingNature, 'SAMAST');
  assert.equal(transfer.toNature, 'SAMAST');
  assert.ok(resolution.ownAccountSelection.bankOwnership);
  transfer.fromAccount = 'ICICI Bank 0992';
  write(store);
  const before = fs.readFileSync(file, 'utf8'), finalized = await dispatch('/api/expenses/bank-statements/finalize', { ...owner, body: { draftId: current.id, deferClosingBalance: true } });
  assert.equal(finalized.status, 409, JSON.stringify(finalized.body));
  assert.equal(fs.readFileSync(file, 'utf8'), before);
});

test('a genuine manual PERSONAL incoming transfer needs no fallback inference and can finalize exactly once', async () => {
  const ownership = { nature: 'PERSONAL', source: 'manual', reason: 'Verified original personal bank receipt', by: 'owner-user', at: date + 'T09:00:00Z' };
  const current = draft({ transactionOwnership: { 'bank-0': ownership } });
  current.transactions[0] = { ...current.transactions[0], debit: 0, credit: 1000, balance: 11000 };
  current.summary = { ...current.summary, closingBalance: 11000, totalDebits: 0, totalCredits: 1000 };
  await seed(current);
  const created = await dispatch('/api/expenses/bank-statements/create-incoming', { ...owner, body: { draftId: current.id, rowId: 'bank-0', sourceKind: 'internal', ownAccountTransfer: true, fromNature: 'PERSONAL', fromAccount: counterpart, note: 'Verified incoming own transfer under original personal ownership' } });
  assert.equal(created.status, 200, JSON.stringify(created.body));
  const before = read(), resolution = before.bankReconciliationDrafts[current.id].resolutions['bank-0'];
  assert.ok(resolution.ownAccountSelection);
  assert.equal(resolution.ownAccountTransfer, undefined);
  assert.equal(resolution.ownAccountSelection.bankOwnership, undefined);
  assert.deepEqual(before.bankReconciliationDrafts[current.id].transactionOwnership['bank-0'], ownership);
  const finalized = await dispatch('/api/expenses/bank-statements/finalize', { ...owner, body: { draftId: current.id } });
  assert.equal(finalized.status, 200, JSON.stringify(finalized.body));
  const after = read();
  assert.deepEqual(after.transfers, before.transfers, 'finalization must link the already created transfer, never create another');
  assert.deepEqual(after.expenses, before.expenses);
  assert.deepEqual(after.reconciliationExpenses, before.reconciliationExpenses);
  assert.deepEqual(after.receipts, before.receipts);
});

'use strict';
// Isolated synthetic accounting store only. No application server, production
// records, retained bank statement files, or external services are contacted.
const test = require('node:test'), assert = require('node:assert/strict');
const fs = require('node:fs'), os = require('node:os'), path = require('node:path');
const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'sanki-tenant-fees-'));
process.env.DATA_PATH = path.join(tempDir, 'data.json');
fs.writeFileSync(path.join(tempDir, 'expenses.json'), '{}');
const { router } = require('../modules/expenses');
const source = require('../modules/tenant-reconciliation-fees');
const file = path.join(tempDir, 'expenses.json');
test.after(() => fs.rmSync(tempDir, { recursive: true, force: true }));
function invoke(method, routePath, { body = {}, params = {}, query = {}, role = 'owner', username = '', permissions = {} } = {}) {
  const layer = router.stack.find(item => item.route && item.route.path === routePath && item.route.methods[method.toLowerCase()]);
  assert.ok(layer, 'route exists: ' + routePath);
  const req = { body, params, query, headers: { 'user-agent': 'Synthetic tenant-fee test' }, get(name) { return this.headers[name] || ''; }, ip: '127.0.0.1', user: { username: username || (role === 'admin' ? 'prashant' : role + '-test'), role, roles: [role], permissions } };
  let status = 200, result;
  const res = { status(value) { status = value; return this; }, json(value) { result = value; return this; }, end() { return this; } };
  layer.route.stack[0].handle(req, res, () => { throw Error('Unexpected next'); });
  return { status, body: result };
}
function invokeRouter(method, routePath, { body = {}, role = 'owner', permissions = {} } = {}) {
  return new Promise((resolve, reject) => {
    const req = { method, path: routePath, url: routePath, originalUrl: routePath, baseUrl: '', body, query: {}, headers: {}, get() { return ''; }, ip: '127.0.0.1', user: { username: role === 'admin' ? 'prashant' : 'owner-test', role, roles: [role], permissions } };
    let status = 200; const headers = {}, res = { statusCode: 200, headersSent: false, status(code) { status = code; this.statusCode = code; return this; }, setHeader(name, value) { headers[name] = value; }, getHeader(name) { return headers[name]; }, removeHeader(name) { delete headers[name]; }, json(value) { this.headersSent = true; resolve({ status, body: value }); return this; }, send(value) { return this.json(value); }, end(value) { return this.json(value); } };
    router.handle(req, res, error => error ? reject(error) : resolve({ status, body: undefined }));
  });
}
invoke('GET', '/api/expenses/config');
const baseline = fs.readFileSync(file, 'utf8');
function setup(kind, direction, nature = 'SAMAST') {
  const store = JSON.parse(baseline), id = 'BRD-SYNTHETIC-FEE', date = '2098-06-02', amount = 1234.56;
  store.receipts = []; store.expenses = {}; store.transfers = []; store.adjustments = []; store.bankStatements = {}; store.bankDateOverrides = {}; store.bankReconciliationLinks = {}; store.bankMovementAuthority = {}; store.bankReconciliationApprovals = {};
  store.rentals = { revision: 4, tenancies: [{ id: 'TEN-A', tenant: 'Synthetic tenant', property: 'Test', floor: 'Test floor', confirmedAt: '2098-01-01', baseRent: 1000, dueDay: 1, startMonth: '2098-06', trackingFrom: '2098-06', securityOpening: 5000, securityOpeningDate: '2098-06-01' }], invoices: [{ id: 'INV-KEEP', tenancyId: 'TEN-A', month: '2098-06', bankDue: 1000 }], allocations: [], credits: [], deposits: [], activity: [], reminders: [], automation: {} };
  const bank = { date, description: 'Synthetic tenant fee ' + kind, reference: 'FEE-' + direction, debit: direction === 'payment' ? amount : 0, credit: direction === 'receipt' ? amount : 0, balance: direction === 'payment' ? 10000 - amount : 10000 + amount };
  store.bankReconciliationDrafts = { [id]: { id, nature: 'SAMAST', account: source.ACCOUNT, transactions: [bank], summary: { from: date, to: date, openingBalance: 10000, closingBalance: bank.balance, totalDebits: bank.debit, totalCredits: bank.credit, validated: true }, resolutions: {}, transactionOwnership: { 'bank-0': { nature, reason: 'Synthetic reviewed source' } }, createdAt: '2098-06-03T10:00:00.000Z', createdBy: 'prashant', expiresAt: '2099-01-01T00:00:00.000Z' } };
  fs.writeFileSync(file, JSON.stringify(store));
  return { store, id, body: { draftId: id, rowId: 'bank-0', tenancyId: 'TEN-A', kind, note: 'Verified synthetic fee source' } };
}
for (const kind of Object.keys(source.SPECS)) for (const direction of ['payment', 'receipt']) for (const nature of ['SAMAST', 'PERSONAL']) test('route records and safely undoes ' + kind + ' ' + direction + ' ' + nature, () => {
  const f = setup(kind, direction, nature), before = JSON.parse(fs.readFileSync(file));
  const result = invoke('POST', '/api/expenses/bank-statements/create-rental', { body: f.body, role: 'admin', permissions: { personalAccounting: true } });
  assert.equal(result.status, 200, JSON.stringify(result.body));
  const after = JSON.parse(fs.readFileSync(file)), record = result.body.expense || result.body.receipt;
  assert.equal(record.nature, nature); assert.equal(record.amount, 1234.56); assert.equal(record.tenancyId, 'TEN-A');
  if (direction === 'receipt') {
    const ledger = invoke('GET', '/api/expenses/account-ledger', { query: { nature, account: source.ACCOUNT, from: '2098-06-02', to: '2098-06-02' } });
    assert.equal(ledger.status, 200, JSON.stringify(ledger.body));
    assert.ok(ledger.body.entries.some(entry => entry.id === record.id && entry.description.startsWith(kind === 'dealer_commission' ? 'Dealer commission received' : 'Documentation / agreement fee received')));
  }
  assert.equal(after.rentals.fees.length, 1); assert.ok(after.auditLog.some(event => event.action === 'TENANT_FEE_FROM_BANK_RECONCILIATION' && event.subjectId === record.id));
  assert.deepEqual(after.bankReconciliationDrafts[f.id].transactions, before.bankReconciliationDrafts[f.id].transactions);
  assert.deepEqual(after.bankReconciliationDrafts[f.id].transactionOwnership, before.bankReconciliationDrafts[f.id].transactionOwnership);
  assert.equal(source.evidenceError(after, after.bankReconciliationDrafts[f.id]), '');
  for (const key of ['invoices', 'allocations', 'credits', 'deposits', 'tenancies']) assert.deepEqual(after.rentals[key], before.rentals[key]);
  const duplicate = invoke('POST', '/api/expenses/bank-statements/create-rental', { body: f.body }); assert.ok(duplicate.status >= 400, JSON.stringify(duplicate.body));
  assert.equal(JSON.parse(fs.readFileSync(file)).rentals.fees.length, 1);
  if (direction === 'payment') {
    assert.equal(invoke('DELETE', '/api/expenses/:id', { params: { id: record.id }, body: { reason: 'Must not orphan tenant fee' } }).status, 409);
    assert.equal(invoke('DELETE', '/api/expenses/:id/payments/:paymentId', { params: { id: record.id, paymentId: 'PAY-001' }, body: { reason: 'Must not orphan tenant fee' } }).status, 409);
    assert.deepEqual(JSON.parse(fs.readFileSync(file)).expenses[record.id], record);
  }
  const undo = invoke('POST', '/api/expenses/bank-statements/undo', { body: { draftId: f.id, rowId: 'bank-0', reason: 'Wrong synthetic fee selection' }, role: 'admin', permissions: { personalAccounting: true } });
  assert.equal(undo.status, 200, JSON.stringify(undo.body));
  const undone = JSON.parse(fs.readFileSync(file)); assert.equal(undone.rentals.fees.length, 1); assert.ok(undone.rentals.fees[0].reversedAt); assert.equal(undone.expenses[record.id], undefined); assert.equal(undone.receipts.some(receipt => receipt.id === record.id), false);
  assert.ok(undone.auditLog.some(event => event.action === 'TENANT_FEE_REVIEW_UNDONE' && event.before.source.id === record.id));
  assert.deepEqual(undone.bankStatements, before.bankStatements);
});
for (const direction of ['payment', 'receipt']) test('tenant fee ' + direction + ' keeps Owner approval and finalization exactly once', async () => {
  const f = setup('dealer_commission', direction), result = invoke('POST', '/api/expenses/bank-statements/create-rental', { body: f.body, role: 'admin' });
  assert.equal(result.status, 200, JSON.stringify(result.body));
  const requested = await invokeRouter('POST', '/api/expenses/bank-statements/finalize', { body: { draftId: f.id, deferClosingBalance: true }, role: 'admin' });
  assert.equal(requested.status, 202, JSON.stringify(requested.body)); assert.equal(requested.body.approvalPending, true);
  const awaiting = fs.readFileSync(file, 'utf8');
  const blocked = invoke('POST', '/api/expenses/bank-statements/undo', { body: { draftId: f.id, rowId: 'bank-0' } });
  assert.equal(blocked.status, 409); assert.match(blocked.body.error, /approval/); assert.equal(fs.readFileSync(file, 'utf8'), awaiting);
  const finalized = await invokeRouter('POST', '/api/expenses/bank-statements/finalize', { body: { draftId: f.id, deferClosingBalance: true, approveRequestId: requested.body.approval.id } });
  assert.equal(finalized.status, 200, JSON.stringify(finalized.body));
  const after = JSON.parse(fs.readFileSync(file)); assert.equal(after.rentals.fees.length, 1); assert.equal(after.bankReconciliationDrafts[f.id], undefined);
  assert.equal(direction === 'payment' ? Object.keys(after.expenses).length : after.receipts.length, 1);
  assert.equal(after.bankStatements[source.ACCOUNT].imports.at(-1).reconciliationRows.length, 1);
  const repeated = invoke('POST', '/api/expenses/bank-statements/create-rental', { body: f.body }); assert.equal(repeated.status, 404);
});
test('finalization fails closed if a tenant fee link changes after review', async () => {
  const f = setup('documentation_fee', 'receipt'), result = invoke('POST', '/api/expenses/bank-statements/create-rental', { body: f.body });
  assert.equal(result.status, 200); const changed = JSON.parse(fs.readFileSync(file)); changed.receipts[0].tenancyId = 'TEN-OTHER'; fs.writeFileSync(file, JSON.stringify(changed)); const before = fs.readFileSync(file, 'utf8');
  const blocked = await invokeRouter('POST', '/api/expenses/bank-statements/finalize', { body: { draftId: f.id, deferClosingBalance: true } });
  assert.equal(blocked.status, 409); assert.match(blocked.body.error, /tenant fee|linked source/); assert.equal(fs.readFileSync(file, 'utf8'), before);
});
test('fee routes preserve existing rental permissions and do not grant PERSONAL access to business admins', () => {
  for (const direction of ['payment', 'receipt']) {
    const f = setup('dealer_commission', direction, 'PERSONAL'), before = fs.readFileSync(file, 'utf8');
    assert.equal(invoke('POST', '/api/expenses/bank-statements/create-rental', { body: f.body, role: 'admin' }).status, 403);
    assert.equal(invoke('POST', '/api/expenses/bank-statements/create-rental', { body: f.body, role: 'admin', username: 'other-admin', permissions: { personalAccounting: true } }).status, 403);
    assert.equal(invoke('POST', '/api/expenses/bank-statements/create-rental', { body: f.body, role: 'claimant' }).status, 403);
    assert.equal(fs.readFileSync(file, 'utf8'), before);
  }
});
test('fee route rejects unknown entity, pending approval and finalized periods without a saved fee', () => {
  const variants = [
    ['unknown', store => { store.bankReconciliationDrafts['BRD-SYNTHETIC-FEE'].transactionOwnership = {}; }],
    ['pending', store => { store.bankReconciliationApprovals = { A: { draftId: 'BRD-SYNTHETIC-FEE', status: 'pending' } }; }],
    ['finalized', store => { store.bankStatements[source.ACCOUNT] = { reconciledThrough: '2098-06-03', imports: [], transactions: {} }; }]
  ];
  for (const [name, change] of variants) {
    const f = setup('documentation_fee', 'receipt'); change(f.store); fs.writeFileSync(file, JSON.stringify(f.store)); const before = fs.readFileSync(file, 'utf8');
    const result = invoke('POST', '/api/expenses/bank-statements/create-rental', { body: f.body });
    assert.ok(result.status >= 400, name + ': ' + JSON.stringify(result.body)); assert.equal(fs.readFileSync(file, 'utf8'), before, name);
  }
});
for (const kind of Object.keys(source.SPECS)) test('fee route cannot duplicate an existing receipt claimed by another draft: ' + kind, () => {
  const f = setup(kind, 'receipt'), other = JSON.parse(JSON.stringify(f.store.bankReconciliationDrafts[f.id])); other.id = 'BRD-EXISTING-REVIEW';
  f.store.receipts = [{ id: 'REC-EXISTING', date: '2098-06-02', amount: 1234.56, nature: 'SAMAST', account: source.ACCOUNT, receiptType: 'other_income', category: 'Other Income', source: 'Synthetic tenant fee ' + kind, proof: '', proofs: [], createdBy: 'owner-test', createdAt: '2098-06-02T12:00:00.000Z' }];
  other.resolutions = { 'bank-0': { action: 'accept_match', appId: 'REC-EXISTING', by: 'owner-test', at: '2098-06-03T09:00:00.000Z' } }; f.store.bankReconciliationDrafts[other.id] = other;
  fs.writeFileSync(file, JSON.stringify(f.store));
  const viewed = invoke('GET', '/api/expenses/bank-statements', { query: { account: source.ACCOUNT, nature: 'SAMAST', draftId: f.id } });
  assert.equal(viewed.status, 200, JSON.stringify(viewed.body)); assert.equal(viewed.body.draft.rows.find(row => row.id === 'bank-0').status, 'missing_in_app', 'another draft hides its claimed receipt from this draft matching');
  const before = fs.readFileSync(file, 'utf8'), blocked = invoke('POST', '/api/expenses/bank-statements/create-rental', { body: f.body });
  assert.equal(blocked.status, 409, JSON.stringify(blocked.body)); assert.match(blocked.body.error, /saved review in another reconciliation/); assert.equal(fs.readFileSync(file, 'utf8'), before, 'existing receipt, both drafts and all register data remain unchanged');
});
test('Owner finalization rejects a cross-draft physical claim saved after the fee review', async () => {
  const f = setup('dealer_commission', 'receipt'), created = invoke('POST', '/api/expenses/bank-statements/create-rental', { body: f.body }); assert.equal(created.status, 200);
  const store = JSON.parse(fs.readFileSync(file)), other = JSON.parse(JSON.stringify(store.bankReconciliationDrafts[f.id])); other.id = 'BRD-LATER-REVIEW'; other.resolutions = { 'bank-0': { action: 'exclude', reason: 'Synthetic existing reviewed exclusion', by: 'owner-test' } }; store.bankReconciliationDrafts[other.id] = other; fs.writeFileSync(file, JSON.stringify(store));
  const before = fs.readFileSync(file, 'utf8'), blocked = await invokeRouter('POST', '/api/expenses/bank-statements/finalize', { body: { draftId: f.id, deferClosingBalance: true } });
  assert.equal(blocked.status, 409, JSON.stringify(blocked.body)); assert.match(blocked.body.error, /saved review/); assert.equal(fs.readFileSync(file, 'utf8'), before);
});

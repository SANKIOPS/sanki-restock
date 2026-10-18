'use strict';

// Independent integration checks: changing organizational labels is never a
// monetary event and must not run historical repairs or weaken existing locks.
const test = require('node:test'), assert = require('node:assert/strict');
const fs = require('node:fs'), os = require('node:os'), path = require('node:path');
const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'manage-financial-safety-'));
for (const [key, file] of Object.entries({ DATA_PATH: 'data.json', ORDERS_PATH: 'orders.json', SALES_PATH: 'sales.json', PROCUREMENT_PATH: 'procurement.json', INCENTIVES_PATH: 'incentives.json', VELOCITY_REMITTANCE_PATH: 'velocity.json' })) process.env[key] = path.join(dir, file);
for (const file of ['expenses.json', 'orders.json', 'sales.json', 'procurement.json', 'incentives.json', 'velocity.json', 'salary.json', 'credit-cards.json']) fs.writeFileSync(path.join(dir, file), '{}');
const file = path.join(dir, 'expenses.json');
const { router } = require('../modules/expenses');
const { plan } = require('../modules/sanki-categories');
const { expenseFinancialLock } = require('../modules/expense-financial-lock');
const { buildReport } = require('../modules/pnl-report');
const read = () => JSON.parse(fs.readFileSync(file, 'utf8'));
test.after(() => fs.rmSync(dir, { recursive: true, force: true }));

function invoke(method, route, { body = {}, role = 'owner', roles = [role], username = role === 'admin' ? 'prashant' : 'synthetic-' + role } = {}) {
  let status = 200, output, complete = false, failure;
  const headers = {}, req = { method, path: route, url: route, originalUrl: route, baseUrl: '', body, query: {}, params: {}, headers: {}, get() { return ''; }, user: { username, role, roles } };
  const res = { statusCode: 200, headersSent: false, status(code) { status = code; this.statusCode = code; return this; }, setHeader(name, value) { headers[name.toLowerCase()] = value; }, getHeader(name) { return headers[name.toLowerCase()]; }, removeHeader(name) { delete headers[name.toLowerCase()]; }, json(value) { output = value; complete = true; this.headersSent = true; return this; }, end(value) { output = value; complete = true; this.headersSent = true; return this; } };
  router.handle(req, res, error => { failure = error || new Error('Route did not complete: ' + route); complete = true; });
  if (failure) throw failure;
  assert.equal(complete, true, method + ' ' + route + ' completed');
  return { status, body: output };
}

function seed() {
  const payment = (id, amount, account = 'Axis Bank 3448') => ({ id, amount, date: '2026-10-02', account, paymentType: 'UPI', reference: 'REF-' + id, bankReference: 'BANK-' + id, proof: '/proof/' + id + '.png', proofs: ['/proof/' + id + '.png'], paidBy: 'prashant', paidAt: '2026-10-02T10:00:00Z', batchPaymentId: 'BATCH-ORIGINAL', arbitraryPreservedField: { owner: 'source', value: [1, 2, 3] } });
  const expense = (id, ledger, group, nature = 'SANKI', status = 'paid') => ({ id, ledger, categoryGroup: group, nature, status, date: '2026-10-01', amount: 500, requestedAmount: 500, paidAmount: status === 'paid' ? 500 : 0, vendor: 'Original Vendor', particulars: 'Preserve particulars', type: 'variable', channel: 'Shared', billPhoto: '/bill.png', billPhotos: ['/bill.png'], qrPhoto: '/qr.png', approvedBy: 'owner', approvedAt: '2026-10-01T10:00:00Z', paidAlready: false, account: 'Axis Bank 3448', paymentType: 'UPI', createdBy: 'claimant', claimant: 'claimant', payments: status === 'paid' ? [payment('PAY-001', 500)] : [], reimbursementPayments: [], auditHistory: [{ at: '2026-10-01', changes: [{ field: 'particulars', before: '', after: 'Preserve particulars' }] }], arbitraryPreservedField: { value: id } });
  const s = {
    expenses: {
      'EX-99101': expense('EX-99101', 'Flowers Expense', 'Old group'),
      'EX-99102': expense('EX-99102', 'Supplies', 'Second group', 'SAMAST'),
      'EX-99103': expense('EX-99103', 'Personal Category', 'Second group', 'PERSONAL'),
      'EX-99104': expense('EX-99104', 'Flowers', 'Old group'),
      'EX-99105': expense('EX-99105', 'Supplies', 'Second group', 'SANKI', 'approved'),
      'EX-99106': expense('EX-99106', 'Existing Category', 'Final group', 'SANKI', 'pending')
    },
    reconciliationExpenses: [{ id: 'BEXP-99101', nature: 'SANKI', category: 'Supplies', categoryGroup: 'Second group', date: '2026-10-02', amount: 18.9, account: 'Axis Bank 3448', status: 'paid', proof: '/bank.png', bankReference: 'BANK-ORIGINAL', reconciliationDraft: 'DRAFT-ORIGINAL', reconciliationRow: 'bank-0', arbitraryPreservedField: { original: true } }],
    customLedgers: { Supplies: { name: 'Supplies', group: 'Second group', type: 'running', addedBy: 'owner' }, 'Personal Category': { name: 'Personal Category', group: 'Second group', type: 'fixed', custom: ['preserved'] } },
    sankiCategoryCatalog: { version: plan.version, categories: [{ name: 'Flowers Expense', group: 'Old group', type: 'variable' }, { name: 'Supplies', group: 'Second group', type: 'running' }, { name: 'Existing Category', group: 'Final group', type: 'fixed' }], activatedAt: '2026-09-24T00:00:00Z' },
    customCategoryGroups: ['Empty group', 'Old group', 'Second group', 'Final group'],
    bankStatements: { 'SANKI|Axis Bank 3448': { reconciledThrough: '2026-10-02', transactions: { 'bank-0': { id: 'bank-0', date: '2026-10-02', debit: 500, credit: 0, balance: 9500, reference: 'BANK-PAY-001' } }, imports: [{ id: 'BST-ORIGINAL', reconciliationRows: [{ bankRowId: 'bank-0', linkedRecordIds: ['EX-99101/PAY-001'], ledger: { id: 'GROUP-PAYMENT', sourceIds: ['EX-99101/PAY-001'], debit: 500, credit: 0 } }], summary: { openingBalance: 10000, closingBalance: 9500 } }], lastReconciliation: { closingBalance: 9500, ledgerClosingBalance: 9500, balanceDifference: 0, reconciled: true, approvedBy: 'owner' } } },
    bankReconciliationDrafts: { 'DRAFT-ORIGINAL': { id: 'DRAFT-ORIGINAL', account: 'Axis Bank 3448', nature: 'SANKI', summary: { from: '2026-10-01', to: '2026-10-02', openingBalance: 9500, closingBalance: 9000, validated: true }, transactions: [{ date: '2026-10-02', debit: 500, credit: 0, balance: 9000, reference: 'ORIGINAL-DRAFT-REF' }], resolutions: { 'bank-0': { action: 'link_existing', appId: 'EX-99102/PAY-001', remark: 'Original decision' } }, approvalRequestedBy: 'prashant', approvalRequestedAt: '2026-10-03T00:00:00Z' } },
    bankDateOverrides: { 'EX-99101/PAY-001': { bankDate: '2026-10-02', originalDate: '2026-10-01', bankReference: 'ORIGINAL', reconciliationDraft: 'FINALIZED-ORIGINAL' } },
    bankReconciliationLinks: { 'EX-99101/PAY-001': { recordId: 'BST-ORIGINAL', rowId: 'bank-0', reconciliationDraft: 'FINALIZED-ORIGINAL' } },
    cashReconciliations: [{ id: 'CASH-ORIGINAL', account: 'Counter Cash', status: 'approved', verifiedThrough: '2026-10-02', openingBalance: 1000, expectedBalance: 500, countedBalance: 500, approvedBy: 'owner' }],
    vendorAdvances: [{ id: 'VA-ORIGINAL', vendor: 'Original Vendor', amount: 300, date: '2026-10-02', account: 'Axis Bank 3448', proof: '/advance.png', applications: [{ expenseId: 'EX-99101', amount: 100 }] }],
    vendorOpeningPayables: [{ id: 'OPEN-ORIGINAL', vendor: 'Original Vendor', amount: 200 }],
    transfers: [{ id: 'TR-ORIGINAL', fromAccount: 'Axis Bank 3448', toAccount: 'Counter Cash', amount: 100, date: '2026-10-02', bankReference: 'TRANSFER-REF', proof: '/transfer.png' }],
    adjustments: [{ id: 'ADJ-ORIGINAL', amount: -5.9, account: 'Axis Bank 3448', date: '2026-10-02', note: 'Bank charge' }],
    receipts: [{ id: 'REC-ORIGINAL', amount: 300, account: 'Counter Cash', date: '2026-10-02', note: 'Original receipt' }],
    expenseRefunds: [], procurementAccounting: { paymentsByPo: { 'PO-ORIGINAL': { payments: [payment('PO-PAY-001', 200)] } } },
    openingBalances: { 'Axis Bank 3448': 10000, 'Counter Cash': 1000 }, openingBalancesByNature: { PERSONAL: { 'Personal Cash': 1000 } },
    vendors: ['Original Vendor'], vendorsByNature: { SANKI: ['Original Vendor'] }, vendorDirectory: { original: { name: 'Original Vendor', entities: ['SANKI', 'SAMAST', 'PERSONAL'] } },
    ledgerOverrides: { Supplies: { type: 'running', preserve: true } }, oneTimeMigrations: {},
    auditLog: [{ id: 'AUD-000001', action: 'ORIGINAL', subjectId: 'EX-99101', original: true }], auditSeq: 1,
    arbitraryFutureFinancialState: { amount: 312.12, status: 'finalized', references: ['DO-NOT-TOUCH'], proof: '/future.png' }
  };
  s.expenses['EX-99103'].paidAlready = true;
  s.expenses['EX-99103'].payments = [{ ...payment('PAY-001', 500, 'Claimant Bank'), personalFunds: true }];
  s.expenses['EX-99103'].reimbursementPayments = [payment('REIM-001', 500)];
  s.expenses['EX-99103'].reimbursementStatus = 'reimbursed';
  s.expenses['EX-99103'].reimbursementAmount = 500;
  fs.writeFileSync(file, JSON.stringify(s));
  return s;
}

function withoutGroup(value) {
  const copy = structuredClone(value);
  if (Array.isArray(copy)) copy.forEach(item => { if (item && typeof item === 'object') delete item.categoryGroup; });
  else Object.values(copy).forEach(item => { if (item && typeof item === 'object') delete item.categoryGroup; });
  return copy;
}
const protectedRoots = ['expenses', 'reconciliationExpenses', 'bankStatements', 'bankReconciliationDrafts', 'bankDateOverrides', 'bankReconciliationLinks', 'cashReconciliations', 'vendorAdvances', 'vendorOpeningPayables', 'transfers', 'adjustments', 'receipts', 'expenseRefunds', 'procurementAccounting', 'openingBalances', 'openingBalancesByNature', 'vendors', 'vendorsByNature', 'vendorDirectory', 'ledgerOverrides', 'oneTimeMigrations', 'arbitraryFutureFinancialState'];
function assertFinanciallyIdentical(before, after) {
  for (const key of protectedRoots) {
    const strip = ['expenses', 'reconciliationExpenses'].includes(key) ? withoutGroup : value => value;
    assert.equal(JSON.stringify(strip(after[key])), JSON.stringify(strip(before[key])), 'exact preserved financial/root bytes: ' + key);
  }
  // Unknown future store fields are protected too, not just this fixture's
  // current financial field list. Only documented grouping/audit paths vary.
  const everythingExceptApprovedGroupMetadata = value => {
    const copy = structuredClone(value);
    for (const key of ['customCategoryGroups', 'categoryGroupAliases', 'categoryGroupOperations', 'auditLog', 'auditSeq']) delete copy[key];
    copy.expenses = withoutGroup(copy.expenses);
    copy.reconciliationExpenses = withoutGroup(copy.reconciliationExpenses);
    for (const category of Object.values(copy.customLedgers)) delete category.group;
    for (const category of copy.sankiCategoryCatalog.categories) delete category.group;
    return copy;
  };
  assert.deepEqual(everythingExceptApprovedGroupMetadata(after), everythingExceptApprovedGroupMetadata(before), 'all known and future fields are immutable apart from group metadata');
  assert.deepEqual(after.auditLog.slice(0, before.auditLog.length), before.auditLog, 'existing audit evidence is append-only and exact');
  for (const id of Object.keys(before.expenses)) assert.deepEqual(expenseFinancialLock(after, after.expenses[id]), expenseFinancialLock(before, before.expenses[id]), 'existing immutable payment lock: ' + id);
  const report = store => buildReport({ expenses: store, orders: {}, purchases: {}, opening: {}, salary: {}, manualSales: {}, feeds: [] }, { from: '2026-10-01', to: '2026-10-08' });
  assert.deepEqual(report(after), report(before), 'group management has no P&L, payment-date, tax, COGS or reconciliation effect');
}
function prepare(action, sources, target, extra = {}) {
  const catalog = invoke('GET', '/api/expenses/category-groups');
  assert.equal(catalog.status, 200, JSON.stringify(catalog.body));
  const body = { action, sources, target, reason: 'Verified organizational correction only', revision: catalog.body.revision, ...extra };
  const preview = invoke('POST', '/api/expenses/category-groups/preview', { body });
  assert.equal(preview.status, 200, JSON.stringify(preview.body));
  assert.ok(preview.body.previewToken, 'the server authorizes this exact preview');
  return { body, preview: preview.body };
}
function apply(prepared, operationId, extra = {}) {
  return invoke('POST', '/api/expenses/category-groups/apply', { body: { ...prepared.body, previewToken: prepared.preview.previewToken, operationId, ...extra } });
}

test('group inventory and preview do not run historical repairs or write any accounting data', () => {
  seed(); const before = fs.readFileSync(file, 'utf8');
  const catalog = invoke('GET', '/api/expenses/category-groups');
  assert.equal(catalog.status, 200); assert.ok(catalog.body.revision); assert.ok(catalog.body.groups.some(group => group.name === 'Empty group'));
  prepare('rename', ['Old group'], 'Renamed group');
  assert.equal(fs.readFileSync(file, 'utf8'), before, 'inventory/preview are exact byte-read-only operations');
});

test('adding an empty group also preserves unrepaired financial data instead of running general store migrations', () => {
  const before = seed();
  const result = invoke('POST', '/api/expenses/category-groups', { body: { name: 'Additional empty group' }, role: 'admin' });
  assert.equal(result.status, 200, JSON.stringify(result.body));
  const after = read(); assertFinanciallyIdentical(before, after);
  assert.ok(after.customCategoryGroups.includes('Additional empty group'));
  assert.equal(after.expenses['EX-99104'].ledger, 'Flowers', 'creating a group must not normalize historical category aliases');
});

test('renaming a group preserves all financial records, immutable bank links and the exact P&L', () => {
  const before = seed(), prepared = prepare('rename', ['Old group'], 'Renamed group');
  const result = apply(prepared, 'safety-rename-original');
  assert.equal(result.status, 200, JSON.stringify(result.body));
  const after = read(); assertFinanciallyIdentical(before, after);
  assert.equal(after.sankiCategoryCatalog.categories.find(category => category.name === 'Flowers Expense').group, 'Renamed group');
  assert.equal(after.expenses['EX-99101'].categoryGroup, 'Renamed group');
  assert.equal(after.expenses['EX-99104'].ledger, 'Flowers', 'an unrelated canonicalization must not run during a group rename');
  assert.equal(after.auditLog[0].action, 'ORIGINAL', 'existing audit history is append-only');
});

test('merging multiple groups changes labels, never categories, payments, refunds, advances or reconciliations', () => {
  const before = seed(), prepared = prepare('merge', ['Old group', 'Second group'], 'Final group');
  const result = apply(prepared, 'safety-merge-original');
  assert.equal(result.status, 200, JSON.stringify(result.body));
  const after = read(); assertFinanciallyIdentical(before, after);
  assert.deepEqual(after.sankiCategoryCatalog.categories.map(category => category.name), before.sankiCategoryCatalog.categories.map(category => category.name), 'merging groups does not merge or rename categories');
  assert.ok(after.sankiCategoryCatalog.categories.every(category => category.group === 'Final group'));
  assert.equal(after.customLedgers.Supplies.group, 'Final group');
  assert.equal(after.customLedgers['Personal Category'].group, 'Final group');
  for (const id of ['EX-99101', 'EX-99102', 'EX-99103']) assert.equal(after.expenses[id].categoryGroup, 'Final group', 'all linked businesses: ' + id);
  assert.equal(after.reconciliationExpenses[0].categoryGroup, 'Final group');
  assert.deepEqual(invoke('GET', '/api/expenses/category-groups').body.groups.map(group => group.name).sort(), ['Empty group', 'Final group']);
});

test('concurrent payment, reconciliation or category changes reject a stale group preview without touching the newer records', () => {
  for (const mutate of [s => { s.expenses['EX-99101'].payments[0].amount = 501; }, s => { s.bankReconciliationDrafts['DRAFT-ORIGINAL'].resolutions['bank-0'].remark = 'Newer reviewed decision'; }, s => { s.sankiCategoryCatalog.categories.push({ name: 'Concurrent Category', group: 'Second group' }); }]) {
    seed(); const prepared = prepare('merge', ['Old group', 'Second group'], 'Final group'), latest = read();
    mutate(latest); fs.writeFileSync(file, JSON.stringify(latest)); const before = fs.readFileSync(file, 'utf8');
    const result = apply(prepared, 'safety-stale-operation');
    assert.equal(result.status, 409, JSON.stringify(result.body));
    assert.equal(fs.readFileSync(file, 'utf8'), before, 'failed stale preview must preserve the concurrent write exactly');
  }
});

test('the signed preview cannot authorize a different name or reason and rejects malformed/reused operations safely', () => {
  seed(); const prepared = prepare('rename', ['Old group'], 'Renamed group');
  for (const extra of [{ target: 'Injected target' }, { reason: 'A changed reason' }, { previewToken: 'invalid-token' }, { operationId: '' }]) {
    const before = fs.readFileSync(file, 'utf8'), result = apply(prepared, 'safety-tampered-original', extra);
    assert.ok([400, 409].includes(result.status), JSON.stringify(result.body));
    assert.equal(fs.readFileSync(file, 'utf8'), before, 'invalid request must not change the store');
  }
  assert.equal(apply(prepared, 'safety-idempotent-original').status, 200);
  const saved = fs.readFileSync(file, 'utf8'), retry = apply(prepared, 'safety-idempotent-original');
  assert.equal(retry.status, 200, JSON.stringify(retry.body)); assert.equal(retry.body.already, true);
  assert.equal(fs.readFileSync(file, 'utf8'), saved, 'network retries do not duplicate the operation or audit');
  const reused = apply(prepared, 'safety-idempotent-original', { target: 'Other target' });
  assert.equal(reused.status, 409, JSON.stringify(reused.body)); assert.equal(fs.readFileSync(file, 'utf8'), saved);
});

test('permission checks reject every group maintenance route before accessing the accounting store', () => {
  seed(); const original = fs.readFileSync;
  fs.readFileSync = () => { throw new Error('Permission denial must not read accounting data'); };
  try {
    for (const actor of [{ role: 'admin', username: 'ordinary-admin' }, { role: 'accounting' }, { role: 'samast_accounting' }, { role: 'claimant', username: 'prashant' }, { role: 'personal_claimant', username: 'prashant' }, { role: 'viewer' }]) {
      for (const [method, route] of [['GET', '/api/expenses/category-groups'], ['POST', '/api/expenses/category-groups/preview'], ['POST', '/api/expenses/category-groups/apply']]) {
        const result = invoke(method, route, { ...actor, body: { action: 'merge', sources: ['Old group'], target: 'Final group', reason: 'Not authorized' } });
        assert.equal(result.status, 403, actor.role + ' ' + route + ' ' + JSON.stringify(result.body));
      }
    }
  } finally { fs.readFileSync = original; }
});

test('an atomic persistence failure leaves the original financial file intact and allows a safe retry', () => {
  const before = seed(), prepared = prepare('rename', ['Old group'], 'Renamed group'), bytes = fs.readFileSync(file, 'utf8'), original = fs.renameSync;
  fs.renameSync = () => { throw Object.assign(new Error('Synthetic atomic replace failure'), { code: 'EACCES' }); };
  let result;
  try { result = apply(prepared, 'safety-atomic-original'); } finally { fs.renameSync = original; }
  assert.equal(result.status, 503, JSON.stringify(result.body));
  assert.equal(fs.readFileSync(file, 'utf8'), bytes, 'failed atomic save never persists half a group change');
  assert.equal(apply(prepared, 'safety-atomic-original').status, 200, 'the same validated operation can retry after a failed write');
  assertFinanciallyIdentical(before, read());
});

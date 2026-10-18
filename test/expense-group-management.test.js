'use strict';
const test = require('node:test'), assert = require('node:assert/strict'), fs = require('node:fs'), os = require('node:os'), path = require('node:path');
const groups = require('../modules/expense-category-groups');
const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'sanki-group-management-'));
process.env.DATA_PATH = path.join(temp, 'data.json');
process.env.PROCUREMENT_PATH = path.join(temp, 'procurement.json');
process.env.ORDERS_PATH = path.join(temp, 'orders.json');
process.env.SALES_PATH = path.join(temp, 'sales.json');
const filename = path.join(temp, 'expenses.json');
fs.writeFileSync(filename, '{}');
const { router } = require('../modules/expenses');
const { plan } = require('../modules/sanki-categories');
test.after(() => fs.rmSync(temp, { recursive: true, force: true }));

function invoke(method, suffix, { body = {}, role = 'owner', username = role + '-test', throughRouter = true } = {}) {
  const routePath = '/api/expenses/category-groups' + suffix;
  const req = { method, path: routePath, url: routePath, originalUrl: routePath, baseUrl: '', body, query: {}, params: {}, headers: { 'user-agent': 'Isolated group-management regression' }, get(name) { return this.headers[String(name).toLowerCase()] || ''; }, user: { username, role, roles: role ? [role] : [] } };
  let status = 200, result, completed = false, failure;
  const headers = {}, res = { statusCode: 200, headersSent: false, status(code) { status = code; this.statusCode = code; return this; }, setHeader(name, value) { headers[String(name).toLowerCase()] = value; }, getHeader(name) { return headers[String(name).toLowerCase()]; }, removeHeader(name) { delete headers[String(name).toLowerCase()]; }, json(value) { result = value; completed = true; this.headersSent = true; return this; }, end(value) { result = value; completed = true; this.headersSent = true; return this; } };
  if (throughRouter) router.handle(req, res, error => { failure = error; completed = true; });
  else {
    const layer = router.stack.find(item => item.route && item.route.path === routePath && item.route.methods[method.toLowerCase()]);
    assert.ok(layer, 'route exists: ' + routePath); layer.route.stack[0].handle(req, res);
  }
  if (failure) throw failure;
  assert.equal(completed, true, 'route completed: ' + routePath);
  return { status, body: result, headers };
}
function seed(extra = {}) {
  const store = {
    expenses: {
      'EX-G1': { id: 'EX-G1', nature: 'SANKI', ledger: 'Flowers', categoryGroup: 'Old Group', amount: 100, date: '2026-10-01', status: 'paid', paidAmount: 100, vendor: 'Vendor', payments: [{ id: 'PAY-001', amount: 100, date: '2026-10-02', account: 'Axis Bank 3448', proof: '/proof.png', bankReconciliationDraft: 'BST-1' }], reimbursementPayments: [{ id: 'R-1', amount: 20, date: '2026-10-03' }] },
      'EX-G2': { id: 'EX-G2', nature: 'SAMAST', ledger: 'Office', categoryGroup: 'Second Group', amount: 50, date: '2026-10-02', status: 'approved', payments: [] },
      'EX-G3': { id: 'EX-G3', nature: 'PERSONAL', ledger: 'Flowers', categoryGroup: 'Old Group', amount: 25, date: '2026-10-03', status: 'pending' }
    },
    reconciliationExpenses: [{ id: 'BRE-G1', nature: 'SANKI', category: 'Flowers', categoryGroup: 'Old Group', amount: 5.9, date: '2026-10-01', account: 'Axis Bank 3448', reconciliationDraft: 'BST-1', bankTransactionId: 'BANK-1', proof: '/bank.pdf' }],
    customLedgers: { Flowers: { name: 'Flowers', group: 'Old Group', type: 'variable', opaque: 'preserve' }, Office: { name: 'Office', group: 'Second Group', type: 'fixed' }, Final: { name: 'Final', group: 'Final Group', type: 'running' } },
    customCategoryGroups: ['Old Group', 'Second Group', 'Final Group', 'Empty Group'],
    sankiCategoryCatalog: { version: plan.version, categories: [{ name: 'Flowers', group: 'Old Group', retained: 1 }, { name: 'Office', group: 'Second Group' }, { name: 'Final', group: 'Final Group' }], activatedAt: '2026-09-01' },
    bankStatements: { 'SANKI|Axis Bank 3448': { closingBalance: 503.92, reconciledThrough: '2026-10-10', transactions: { B1: { id: 'BANK-1', date: '2026-10-01', debit: 100, reference: 'EX-G1/PAY-001', balance: 503.92 } }, imports: [{ id: 'BST-1', file: '/bank.pdf', approvedBy: 'owner', closingBalance: 503.92 }] } },
    bankReconciliationDrafts: { 'BST-2': { resolutions: { 'bank-1': { action: 'match_existing', appId: 'EX-G1/PAY-001' } }, approvalRequest: { by: 'prashant' } } },
    bankDateOverrides: { 'EX-G1/PAY-001': { date: '2026-10-02', originalDate: '2026-10-01', reconciliationDraft: 'BST-1' } },
    transfers: [{ id: 'TR-1', amount: 10, date: '2026-10-01', fromAccount: 'Axis Bank 3448', toAccount: 'Counter Cash', proof: 'cash.jpg' }],
    adjustments: [{ id: 'ADJ-1', amount: -5.9, date: '2026-10-01', note: 'Charge', account: 'Axis Bank 3448' }],
    openingBalances: { 'Axis Bank 3448': 5090 }, openingBalancesByNature: { SANKI: { 'Axis Bank 3448': 5090 } },
    receipts: [{ id: 'REC-1', amount: 1000 }], cashReconciliations: [{ id: 'CASH-1', status: 'approved', actualClosing: 100 }],
    paytmSettlements: [{ id: 'PAYTM-1', amount: 1000, status: 'finalized' }], unknownFutureRecords: { untouched: [1, 2, 3] },
    auditLog: [], auditSeq: 0, oneTimeMigrations: {}
  };
  Object.assign(store, extra); fs.writeFileSync(filename, JSON.stringify(store)); return store;
}
function read() { return JSON.parse(fs.readFileSync(filename, 'utf8')); }
function request(action = 'rename', sources = ['Old Group'], target = 'Renamed Group') {
  return { action, sources, target, reason: 'Organize groups without touching money movements', revision: invoke('GET', '').body.revision };
}
function previewAndApply(body, operationId = 'operation-001', actor = {}) {
  const preview = invoke('POST', '/preview', { body, ...actor });
  assert.equal(preview.status, 200, preview.body.error);
  const appliedBody = { ...body, previewToken: preview.body.previewToken, operationId };
  const applied = invoke('POST', '/apply', { body: appliedBody, ...actor });
  assert.equal(applied.status, 200, applied.body.error);
  return { preview: preview.body, applied: applied.body, appliedBody };
}
function denyIO(callback) {
  const original = Object.fromEntries(['readFileSync', 'writeFileSync', 'renameSync'].map(key => [key, fs[key]]));
  for (const key of Object.keys(original)) fs[key] = () => { throw new Error('Unexpected store access ' + key); };
  try { callback(); } finally { for (const [key, value] of Object.entries(original)) fs[key] = value; }
}

test('group catalog and preview are byte-read-only and report category, entry and business counts', () => {
  seed(); const before = fs.readFileSync(filename, 'utf8'), result = invoke('GET', '');
  assert.equal(result.status, 200); assert.equal(result.headers['cache-control'], 'no-store');
  const old = result.body.groups.find(group => group.name === 'Old Group');
  assert.deepEqual(old, { name: 'Old Group', categoryCount: 1, entryCount: 3, businesses: ['PERSONAL', 'SANKI'], categories: [{ name: 'Flowers', entryCount: 3, businesses: ['PERSONAL', 'SANKI'] }] });
  const body = request(), preview = invoke('POST', '/preview', { body });
  assert.equal(preview.status, 200); assert.equal(preview.body.entryCount, 3); assert.equal(preview.body.categoryCount, 1); assert.match(preview.body.previewToken, /^[a-f0-9]{64}$/);
  assert.equal(fs.readFileSync(filename, 'utf8'), before, 'no migration bookkeeping or hidden save');
});

test('GET, preview and confirm are restricted before store access, including claimant-only Prashant', () => {
  seed(); denyIO(() => {
    for (const actor of [{ role: 'admin', username: 'another-admin' }, { role: 'accounts' }, { role: 'claimant', username: 'prashant' }, { role: 'personal_claimant', username: 'prashant' }, { role: '' }]) {
      for (const [method, suffix] of [['GET', ''], ['POST', '/preview'], ['POST', '/apply']]) assert.equal(invoke(method, suffix, actor).status, 403);
    }
  });
});

test('invalid actions, sources, target and reasons are rejected without opening store', () => {
  seed(); const valid = request();
  denyIO(() => {
    for (const patch of [{ action: 'delete' }, { sources: [] }, { sources: ['Old Group', ' old   GROUP '] }, { sources: ['A', 'B'] }, { sources: [{}] }, { target: '' }, { target: 'x'.repeat(121) }, { target: 'bad\nname' }, { reason: '' }, { reason: 'two\nlines' }, { reason: 'x'.repeat(1001) }]) {
      assert.equal(invoke('POST', '/preview', { body: { ...valid, ...patch } }).status, 400, JSON.stringify(patch));
      assert.equal(invoke('POST', '/apply', { body: { ...valid, ...patch } }).status, 400, JSON.stringify(patch));
    }
  });
});

test('rename updates all catalog labels and cross-business expense groups only, preserving financial bytes', () => {
  const before = seed(), beforeInvariant = groups.financialInvariant(before), body = request();
  const { applied } = previewAndApply(body);
  const after = read();
  assert.equal(applied.group, 'Renamed Group'); assert.equal(applied.affected.entryCount, 3);
  assert.equal(after.customLedgers.Flowers.group, 'Renamed Group'); assert.equal(after.sankiCategoryCatalog.categories[0].group, 'Renamed Group');
  for (const id of ['EX-G1', 'EX-G3']) assert.equal(after.expenses[id].categoryGroup, 'Renamed Group');
  assert.equal(after.reconciliationExpenses[0].categoryGroup, 'Renamed Group');
  assert.equal(after.expenses['EX-G2'].categoryGroup, 'Second Group');
  assert.equal(after.categoryGroupAliases['old group'], 'Renamed Group');
  assert.equal(after.auditLog.length, 1); assert.equal(after.auditLog[0].action, 'CATEGORY_GROUP_RENAMED');
  assert.deepEqual(after.auditLog[0].before.groups, ['Old Group']);
  assert.equal(groups.financialInvariant(after), beforeInvariant);
  assert.ok(!applied.groups.some(group => group.name === 'Old Group'));
});

test('merge several groups into an existing final group without merging category identities', () => {
  const before = seed(), body = request('merge', ['Old Group', 'Second Group', 'Empty Group'], ' final  GROUP ');
  const { preview, applied } = previewAndApply(body);
  assert.equal(preview.target, 'Final Group'); assert.equal(preview.entryCount, 4); assert.equal(preview.categoryCount, 2); assert.deepEqual(preview.businesses, ['PERSONAL', 'SAMAST', 'SANKI']);
  const after = read(); assert.equal(groups.financialInvariant(after), groups.financialInvariant(before));
  assert.deepEqual(after.sankiCategoryCatalog.categories.map(category => category.name), ['Flowers', 'Office', 'Final']);
  assert.ok(after.sankiCategoryCatalog.categories.every(category => category.group === 'Final Group'));
  assert.deepEqual(applied.groups.map(group => group.name), ['Final Group']);
  assert.equal(after.auditLog[0].action, 'CATEGORY_GROUPS_MERGED');
  assert.deepEqual(after.categoryGroupAliases, { 'old group': 'Final Group', 'second group': 'Final Group', 'empty group': 'Final Group' });
});

test('merge audit preserves precise recoverable source classification without copying financial fields', () => {
  const store = seed(); delete store.expenses['EX-G3'].categoryGroup; store.categoryGroupAliases = { historical: 'Old Group' };
  fs.writeFileSync(filename, JSON.stringify(store));
  previewAndApply(request('merge', ['Old Group', 'Second Group'], 'Final Group'));
  const classification = read().auditLog[0].before.classification;
  assert.deepEqual(classification.customCategoryGroups, store.customCategoryGroups);
  assert.equal(classification.customCategoryGroupsPresent, true);
  assert.equal(classification.categoryGroupAliasesPresent, true);
  assert.deepEqual(classification.categoryGroupAliases, { historical: 'Old Group' });
  assert.deepEqual(classification.categoryAssignments, [{ name: 'Flowers', group: 'Old Group' }, { name: 'Office', group: 'Second Group' }]);
  assert.deepEqual(classification.categories, [
    { collection: 'customLedgers', key: 'Flowers', name: 'Flowers', hasGroup: true, group: 'Old Group' },
    { collection: 'customLedgers', key: 'Office', name: 'Office', hasGroup: true, group: 'Second Group' },
    { collection: 'sankiCategoryCatalog.categories', key: 0, name: 'Flowers', hasGroup: true, group: 'Old Group' },
    { collection: 'sankiCategoryCatalog.categories', key: 1, name: 'Office', hasGroup: true, group: 'Second Group' }
  ]);
  assert.deepEqual(classification.entries, [
    { collection: 'expenses', id: 'EX-G1', hasCategoryGroup: true, categoryGroup: 'Old Group' },
    { collection: 'expenses', id: 'EX-G2', hasCategoryGroup: true, categoryGroup: 'Second Group' },
    { collection: 'expenses', id: 'EX-G3', hasCategoryGroup: false, categoryGroup: null },
    { collection: 'reconciliationExpenses', id: 'BRE-G1', hasCategoryGroup: true, categoryGroup: 'Old Group' }
  ]);
  assert.doesNotMatch(JSON.stringify(classification), /payments|amount|date|proof|\/proof\.png|bankReconciliationDraft|reimbursementPayments/);
  assert.equal(read().expenses['EX-G3'].categoryGroup, 'Final Group', 'audit records absence before assigning the new label');
});

test('case-only rename is allowed while exact no-op does not write or audit', () => {
  seed(); previewAndApply(request('rename', ['Old Group'], 'OLD GROUP'));
  assert.equal(read().customLedgers.Flowers.group, 'OLD GROUP'); assert.equal(read().categoryGroupAliases['old group'], 'OLD GROUP');
  const noOp = request('rename', ['OLD GROUP'], 'OLD GROUP'), before = fs.readFileSync(filename, 'utf8');
  const { applied } = previewAndApply(noOp, 'operation-noop'); assert.equal(applied.already, true); assert.equal(fs.readFileSync(filename, 'utf8'), before);
});

test('duplicate target and invalid merge are rejected without changing records', () => {
  seed(); const before = fs.readFileSync(filename, 'utf8');
  for (const [action, sources, target, status, code] of [['rename', ['Old Group'], 'Second Group', 409, 'TARGET_EXISTS'], ['merge', ['Old Group'], 'Missing Group', 400, 'TARGET_MISSING'], ['merge', ['Old Group'], ' old  group ', 400, 'TARGET_IS_SOURCE'], ['rename', ['Missing Group'], 'New', 409, 'SOURCE_CHANGED']]) {
    const result = invoke('POST', '/preview', { body: request(action, sources, target) }); assert.equal(result.status, status); assert.equal(result.body.code, code);
  }
  assert.equal(fs.readFileSync(filename, 'utf8'), before);
});

test('any intervening record change invalidates preview, including same-total financial changes', () => {
  seed(); const body = request(), preview = invoke('POST', '/preview', { body });
  const changed = read(); changed.expenses['EX-G1'].payments[0].proof = '/revised-proof.png'; fs.writeFileSync(filename, JSON.stringify(changed));
  const before = fs.readFileSync(filename, 'utf8');
  const result = invoke('POST', '/apply', { body: { ...body, previewToken: preview.body.previewToken, operationId: 'stale-operation' } });
  assert.equal(result.status, 409); assert.equal(result.body.code, 'STALE_PREVIEW'); assert.equal(fs.readFileSync(filename, 'utf8'), before);
});

test('missing or tampered preview token cannot confirm even with current revision', () => {
  seed(); const body = request(), preview = invoke('POST', '/preview', { body }), before = fs.readFileSync(filename, 'utf8');
  for (const patch of [{ previewToken: undefined }, { previewToken: 'a'.repeat(64) }, { previewToken: preview.body.previewToken, target: 'Different Group' }, { previewToken: preview.body.previewToken, reason: 'Changed reason' }]) {
    const result = invoke('POST', '/apply', { body: { ...body, operationId: 'tampered-operation', ...patch } }); assert.equal(result.status, 409); assert.equal(result.body.code, 'PREVIEW_REQUIRED');
  }
  assert.equal(fs.readFileSync(filename, 'utf8'), before);
});

test('confirm retry is idempotent, but operation ID cannot authorize another intent or another actor', () => {
  seed(); const { appliedBody } = previewAndApply(request());
  const before = fs.readFileSync(filename, 'utf8');
  const retry = invoke('POST', '/apply', { body: appliedBody }); assert.equal(retry.status, 200); assert.equal(retry.body.already, true);
  const normalizedRetry = invoke('POST', '/apply', { body: { ...appliedBody, sources: [' old   GROUP '] } }); assert.equal(normalizedRetry.status, 200); assert.equal(normalizedRetry.body.already, true);
  assert.equal(fs.readFileSync(filename, 'utf8'), before);
  for (const change of [{ body: { ...appliedBody, target: 'Different' } }, { body: appliedBody, username: 'prashant', role: 'admin' }]) {
    const result = invoke('POST', '/apply', change); assert.equal(result.status, 409); assert.equal(result.body.code, 'OPERATION_REUSED');
  }
});

test('retired labels resolve to final group for stale add forms and aliases flatten through later renames', () => {
  seed(); previewAndApply(request('merge', ['Old Group'], 'Final Group'));
  previewAndApply(request('rename', ['Final Group'], 'Organized Group'), 'operation-002');
  const before = fs.readFileSync(filename, 'utf8');
  for (const name of [' old   GROUP ', 'final group']) {
    const result = invoke('POST', '', { body: { name } }); assert.equal(result.status, 200); assert.equal(result.body.already, true); assert.equal(result.body.group, 'Organized Group'); assert.ok(!result.body.groups.includes('Old Group'));
  }
  assert.equal(fs.readFileSync(filename, 'utf8'), before);
  assert.deepEqual(read().categoryGroupAliases, { 'old group': 'Organized Group', 'final group': 'Organized Group' });
  const retired = invoke('POST', '/preview', { body: request('rename', ['Organized Group'], 'Old Group') }); assert.equal(retired.status, 409); assert.equal(retired.body.code, 'TARGET_RETIRED');
});

test('Owner and explicit Prashant manage standalone and legacy groups without activating catalogs', () => {
  for (const actor of [{ role: 'owner' }, { role: 'admin', username: ' Prashant ' }]) {
    seed({ sankiCategoryCatalog: undefined });
    const before = read(); previewAndApply(request('rename', ['Empty Group'], 'Empty Renamed'), 'standalone-operation', actor);
    const after = read(); assert.equal(after.sankiCategoryCatalog, undefined); assert.equal(groups.financialInvariant(after), groups.financialInvariant(before));
    assert.ok(after.customCategoryGroups.includes('Empty Renamed'));
  }
});

test('case-only legacy/default renames survive a static category provider and later rename chains', () => {
  const store = { customCategoryGroups: [], expenses: { X: { ledger: 'Built-in', categoryGroup: 'Default Group', nature: 'SANKI', amount: 100 } } }, provider = [{ name: 'Built-in', group: 'Default Group', type: 'fixed' }];
  const first = groups.planChange(store, { action: 'rename', sources: ['Default Group'], target: 'DEFAULT GROUP', reason: 'Use final casing', revision: groups.revision(store) }, provider);
  groups.applyChange(store, first, provider);
  assert.deepEqual(groups.categoryGroupNames(store, provider), ['DEFAULT GROUP']);
  assert.equal(groups.catalog(store, provider).groups[0].name, 'DEFAULT GROUP');
  const second = groups.planChange(store, { action: 'rename', sources: ['DEFAULT GROUP'], target: 'Organized', reason: 'Use final group name', revision: groups.revision(store) }, provider);
  groups.applyChange(store, second, provider);
  assert.deepEqual(groups.categoryGroupNames(store, provider), ['Organized']);
  assert.equal(groups.canonicalGroup(store, 'Default Group', provider), 'Organized');
  assert.equal(store.expenses.X.categoryGroup, 'Organized');
});

test('active definitions do not duplicate a category through stale custom-ledger shadow groups', () => {
  const store = seed(); store.customLedgers.Flowers.group = 'Stale Shadow'; fs.writeFileSync(filename, JSON.stringify(store));
  let view = invoke('GET', '').body;
  assert.equal(view.groups.find(group => group.name === 'Stale Shadow'), undefined);
  assert.equal(view.groups.find(group => group.name === 'Old Group').categoryCount, 1);
  previewAndApply(request());
  assert.equal(read().customLedgers.Flowers.group, 'Renamed Group', 'affected custom cache follows the active catalog');
  view = invoke('GET', '').body;
  assert.equal(view.groups.find(group => group.name === 'Stale Shadow'), undefined);
});

test('corrupt and missing stores fail closed and are never replaced with blank accounting records', () => {
  for (const contents of ['{broken', 'null', '[]']) {
    fs.writeFileSync(filename, contents);
    assert.equal(invoke('GET', '').status, 503);
    assert.equal(invoke('POST', '', { body: { name: 'New' } }).status, 503);
    assert.equal(fs.readFileSync(filename, 'utf8'), contents);
  }
  fs.unlinkSync(filename); assert.equal(invoke('GET', '').status, 503); assert.equal(fs.existsSync(filename), false);
});

test('failed atomic group save leaves the original store intact and can be retried once', () => {
  seed(); const body = request(), preview = invoke('POST', '/preview', { body }), before = fs.readFileSync(filename, 'utf8');
  const appliedBody = { ...body, previewToken: preview.body.previewToken, operationId: 'atomic-operation' }, originalRename = fs.renameSync;
  fs.renameSync = () => { throw new Error('Simulated atomic rename failure'); };
  try { assert.equal(invoke('POST', '/apply', { body: appliedBody }).status, 503); } finally { fs.renameSync = originalRename; }
  assert.equal(fs.readFileSync(filename, 'utf8'), before);
  const retry = invoke('POST', '/apply', { body: appliedBody }); assert.equal(retry.status, 200); assert.equal(read().auditLog.length, 1);
});

test('each successful group mutation remains synchronous between strict read and atomic write', () => {
  const source = fs.readFileSync(require.resolve('../modules/expenses'), 'utf8');
  for (const route of ['/api/expenses/category-groups/preview', '/api/expenses/category-groups/apply']) {
    const start = source.indexOf("router.post('" + route + "'");
    const end = source.indexOf('\n});', start);
    assert.ok(start > 0 && end > start); assert.doesNotMatch(source.slice(start, end), /\bawait\b|async\s*\(/);
  }
});

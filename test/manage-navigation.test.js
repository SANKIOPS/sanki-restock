'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { buildCatalog } = require('../public/manage-navigation');
const { MODULES, visibleFor } = require('../modules/module-registry');
const { userCanAccessPath, allowedPagesForUser } = require('../modules/auth-users');
const { apiAllowedForUser } = require('../auth');

const person = (...roles) => ({ username: 'test-user', role: roles[0], roles });
const owner = person('owner');
const prashant = { username: 'prashant', role: 'admin', roles: ['admin'] };
const config = { claimantOnly: false, canManageCategories: true, canManageVendors: true, canApprove: true, isAdmin: true, isOwner: true };
function catalog(user, expenseConfig = config) { return buildCatalog({ modules: visibleFor(user), user, expenseConfig }); }
function items(heads) { return heads.flatMap(head => head.items); }
function keys(heads) { return items(heads).map(item => item.key); }

test('browser and Node use the same pure navigation catalogue', () => {
  const browser = { window: {} };
  vm.runInNewContext(fs.readFileSync(path.join(__dirname, '../public/manage-navigation.js'), 'utf8'), browser);
  assert.equal(typeof browser.window.SankiManageNavigation.buildCatalog, 'function');
  assert.deepEqual(JSON.parse(JSON.stringify(browser.window.SankiManageNavigation.buildCatalog({ modules: visibleFor(owner), user: owner, expenseConfig: config }))), catalog(owner));
});

test('universal Manage is registry driven and every heading starts collapsed', () => {
  assert.equal(MODULES.filter(item => item.key === 'manage').length, 1);
  assert.ok(visibleFor(owner).some(item => item.href === '/manage.html'));
  assert.ok(visibleFor(prashant).some(item => item.href === '/manage.html'));
  const heads = catalog(owner);
  assert.deepEqual(heads.map(head => head.key), ['classification', 'money', 'sales', 'inventory', 'operations', 'administration']);
  assert.ok(heads.every(head => head.expanded === false && head.items.length));
  assert.ok(items(heads).every(item => item.actions.length && item.href.startsWith('/') && !item.href.startsWith('//')));
  assert.equal(new Set(keys(heads)).size, keys(heads).length);
});

test('category and group tools are inline while existing financial workflows retain their original destinations', () => {
  const byKey = Object.fromEntries(items(catalog(owner)).map(item => [item.key, item]));
  assert.equal(byKey.groups.href, '/manage.html?section=groups');
  assert.deepEqual(byKey.groups.actions, ['Add group', 'Rename group', 'Merge groups']);
  assert.equal(byKey.categories.href, '/manage.html?section=categories');
  assert.equal(byKey.vendors.href, '/expenses.html?tab=vendors');
  assert.equal(byKey.transactions.href, '/expenses.html?tab=cash');
  assert.equal(byKey.payables.href, '/expenses.html?tab=pending');
  assert.equal(byKey.reimbursements.href, '/expenses.html?tab=reimbursements');
  assert.equal(byKey.creditcards.href, '/expenses.html?tab=creditcards');
});

test('financial maintenance fails closed when expense config is absent, failed or claimant-only', () => {
  for (const expenseConfig of [undefined, null, {}, { success: false }, { ...config, claimantOnly: true }]) {
    const heads = buildCatalog({ modules: visibleFor(owner), user: owner, expenseConfig });
    const actual = keys(heads);
    for (const key of ['vendors', 'categories', 'groups', 'expenses', 'transactions', 'payables', 'reimbursements', 'creditcards', 'approvals', 'audit']) assert.equal(actual.includes(key), false, key);
  }
});

test('non-manager accounting sees its existing expense screen, not unavailable finance tabs or maintenance', () => {
  const accounting = person('accounting');
  const actual = keys(catalog(accounting, { claimantOnly: false, canApprove: true, canManageCategories: false, canManageVendors: false, isOwner: false }));
  assert.ok(actual.includes('expenses'));
  for (const key of ['groups', 'categories', 'vendors', 'creditcards', 'transactions', 'payables', 'reimbursements', 'approvals', 'audit', 'receivables', 'users', 'meta-ads', 'rentals']) assert.equal(actual.includes(key), false, key);
});

test('only existing Admin finance tabs appear and retired approvals navigation is never revived', () => {
  const actual = keys(catalog(owner));
  for (const key of ['transactions', 'payables', 'reimbursements', 'audit', 'receivables']) assert.ok(actual.includes(key), key);
  assert.equal(actual.includes('approvals'), false);
  const cashVerifier = keys(catalog(prashant, { ...config, isAdmin: false, canVerifyCounterCash: true, isOwner: false }));
  assert.ok(cashVerifier.includes('transactions'));
  for (const key of ['payables', 'reimbursements', 'audit', 'receivables', 'creditcards', 'approvals']) assert.equal(cashVerifier.includes(key), false, key);
});

test('Manage transaction edit actions advertise only the explicit server capability without changing destinations', () => {
  const allowed = items(catalog(prashant, { ...config, isOwner: false, canEditLedgerTransactions: true })).find(item => item.key === 'transactions');
  assert.equal(allowed.href, '/expenses.html?tab=cash');
  assert.ok(allowed.actions.includes('Edit permitted payments, transfers and receipts'));
  assert.match(allowed.description, /Protected entries remain locked/);
  for (const value of [undefined, false, 'true']) {
    const denied = items(catalog(owner, { ...config, canEditLedgerTransactions: value })).find(item => item.key === 'transactions');
    assert.deepEqual(denied.actions, ['Account ledgers', 'Review transactions', 'Reconcile account']);
    assert.equal(denied.href, allowed.href);
  }
});

test('an edit capability alone cannot grant Manage transactions or expand verifier and claimant access', () => {
  assert.equal(keys(catalog(person('accounting'), { claimantOnly: false, isAdmin: false, canEditLedgerTransactions: true })).includes('transactions'), false);
  assert.equal(keys(catalog(owner, { ...config, claimantOnly: true, canEditLedgerTransactions: true })).includes('transactions'), false);
  const verifier = items(catalog(prashant, { ...config, isAdmin: false, isOwner: false, canVerifyCounterCash: true, canEditLedgerTransactions: true })).find(item => item.key === 'transactions');
  assert.deepEqual(verifier.actions, ['Verify Counter Cash']);
  assert.equal(verifier.title, 'Counter Cash verification');
});

test('Prashant catalog retains fixed live modules and excludes private or unrelated tools', () => {
  const actual = keys(catalog(prashant, { ...config, isOwner: false }));
  for (const key of ['vendors', 'categories', 'groups', 'expenses', 'transactions', 'purchases', 'salary', 'incentives']) assert.ok(actual.includes(key), key);
  for (const key of ['users', 'seo', 'meta-ads', 'catalogue-images', 'sales-orders', 'orders', 'creditcards', 'rentals']) assert.equal(actual.includes(key), false, key);
});

test('rental management follows the current Owner-only page UI, not its broader legacy registry entry', () => {
  assert.ok(visibleFor(prashant).some(item => item.key === 'rentals'));
  assert.equal(keys(catalog(prashant)).includes('rentals'), false);
  assert.ok(keys(catalog(owner)).includes('rentals'));
});

test('accounting purchase review does not advertise new POs or stock/cost corrections', () => {
  const byKey = Object.fromEntries(items(catalog(person('accounting'), null)).map(item => [item.key, item]));
  assert.equal(byKey.purchases, undefined);
  assert.equal(byKey['received-stock'].href, '/procurement.html?tab=audit');
  assert.deepEqual(byKey['received-stock'].actions, ['Review received stock']);
  assert.equal(byKey['purchase-history'].href, '/procurement.html?tab=summary');
  for (const role of ['admin', 'owner', 'procurement', 'inventory']) assert.ok(keys(catalog(person(role), null)).includes('purchases'), role);
});

test('pure claimants and view-only Stylists do not acquire a Manage page or entry', () => {
  for (const user of [person('claimant'), person('personal_claimant'), person('stocksearch'), { username: 'prashant', role: 'claimant', roles: ['claimant'] }]) {
    assert.equal(userCanAccessPath(user, '/manage.html'), false);
    assert.equal(visibleFor(user).some(item => item.key === 'manage'), false);
  }
  assert.deepEqual(catalog(person('claimant'), { ...config, claimantOnly: true }), []);
  assert.deepEqual(catalog(person('stocksearch'), null), []);
});

test('claimant plus an operational role can navigate its tools but not finance maintenance', () => {
  const user = person('claimant', 'inventory');
  assert.equal(userCanAccessPath(user, '/manage.html'), true);
  const actual = keys(catalog(user, { ...config, claimantOnly: true }));
  assert.ok(actual.includes('inventory'));
  assert.ok(actual.includes('purchases'));
  for (const key of ['vendors', 'categories', 'groups', 'expenses', 'transactions']) assert.equal(actual.includes(key), false, key);
});

test('module permissions are always intersected and hidden, arbitrary and retired links are never exposed', () => {
  const heads = buildCatalog({
    user: owner,
    expenseConfig: config,
    modules: [
      { key: 'expenses', href: '/expenses.html', status: 'hidden' },
      { key: 'orders', href: 'https://untrusted.example/', status: 'live' },
      { key: 'sales', href: '/sales.html', status: 'live' },
      { key: 'velocity', href: '/velocity.html', status: 'coming' },
      { key: 'procurement', href: '/procurement.html', status: 'live' }
    ]
  });
  assert.deepEqual(keys(heads), ['purchases', 'received-stock', 'purchase-history']);
  assert.deepEqual(buildCatalog({ user: owner, expenseConfig: config, modules: [] }), []);
});

test('module-only personas get functioning permitted destinations and no private maintenance links', () => {
  for (const role of ['sales', 'procurement', 'warehouse', 'model_calendar', 'revenue']) {
    const user = person(role);
    const heads = catalog(user, null);
    assert.ok(heads.length, role);
    assert.ok(visibleFor(user).some(item => item.key === 'manage'), role);
    for (const item of items(heads)) {
      const destination = item.href.split('?')[0];
      assert.ok(userCanAccessPath(user, destination), role + ': ' + item.href);
    }
    assert.equal(keys(heads).includes('groups'), false, role);
  }
});

test('Manage does not expand existing page allowlists or grant payment/reconciliation APIs', () => {
  assert.equal(allowedPagesForUser(person('sales')).includes('/manage.html'), false);
  assert.equal(allowedPagesForUser(prashant).includes('/manage.html'), false);
  for (const user of [person('sales'), person('procurement'), person('warehouse'), person('stocksearch')]) {
    for (const url of ['/api/expenses/EX-1/pay', '/api/expenses/category-groups', '/api/expenses/bank-reconcile/finalize']) assert.equal(apiAllowedForUser(user, url, 'POST'), false, url);
  }
  assert.equal(userCanAccessPath(prashant, '/admin-users.html'), false);
  assert.equal(userCanAccessPath(person('accounting'), '/admin-users.html'), false);
});

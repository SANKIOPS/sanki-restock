'use strict';
const test = require('node:test'), assert = require('node:assert/strict'), fs = require('node:fs'), os = require('node:os'), path = require('node:path');
const { cleanGroupName, categoryGroupNames } = require('../modules/expense-category-groups');

test('group labels reject malformed/non-printable input and normalize visible spacing', () => {
  for (const value of [undefined, null, 0, 42, false, {}, [], ['New group'], '', '   ', 'x'.repeat(121), 'New\nGroup', 'New\tGroup', 'New\u0000Group', 'New\u007fGroup', 'New\u0085Group', 'New\u200bGroup', 'New\u2028Group', 'New\u2029Group', '\ud800']) assert.equal(cleanGroupName(value), '', String(value));
  assert.equal(cleanGroupName('  New   Group\u00a0 '), 'New Group');
  assert.equal(cleanGroupName('नया समूह'), 'नया समूह');
  assert.equal(cleanGroupName('x'.repeat(120)), 'x'.repeat(120));
});

test('group catalog merges standalone and category-backed groups once without rewriting records', () => {
  const store = { customCategoryGroups: ['  Empty   Group ', 'BANKING & FINANCE', 'empty group', null, '\u0000'] }, categories = [{ name: 'A', group: 'Banking & Finance' }, { name: 'B', group: ' Banking  & Finance ' }, { name: 'C', group: 'Custom existing' }];
  const before = JSON.stringify({ store, categories });
  assert.deepEqual(categoryGroupNames(store, categories), ['Banking & Finance', 'Custom existing', 'Empty Group']);
  assert.equal(JSON.stringify({ store, categories }), before);
  assert.deepEqual(categoryGroupNames({ customCategoryGroups: {} }, []), []);
});

const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'sanki-category-groups-'));
process.env.DATA_PATH = path.join(temp, 'data.json');
process.env.PROCUREMENT_PATH = path.join(temp, 'procurement.json');
process.env.ORDERS_PATH = path.join(temp, 'orders.json');
process.env.SALES_PATH = path.join(temp, 'sales.json');
const filename = path.join(temp, 'expenses.json');
fs.writeFileSync(filename, '{}');
const { router } = require('../modules/expenses');
const { plan } = require('../modules/sanki-categories');
const { KEY: categoryFinalizationKey } = require('../modules/sanki-category-finalization');
test.after(() => fs.rmSync(temp, { recursive: true, force: true }));

function invoke(method, routePath, { body = {}, role = 'owner', username = role + '-test', throughRouter = false } = {}) {
  const req = { method, path: routePath, url: routePath, originalUrl: routePath, baseUrl: '', body, query: {}, params: {}, headers: { 'user-agent': 'Isolated category-group regression' }, get(name) { return this.headers[String(name).toLowerCase()] || ''; }, user: { username, role, roles: role ? [role] : [] } };
  let status = 200, result, completed = false, failure;
  const headers = {}, res = { statusCode: 200, headersSent: false, status(code) { status = code; this.statusCode = code; return this; }, setHeader(name, value) { headers[String(name).toLowerCase()] = value; }, getHeader(name) { return headers[String(name).toLowerCase()]; }, removeHeader(name) { delete headers[String(name).toLowerCase()]; }, json(value) { result = value; completed = true; this.headersSent = true; return this; }, end(value) { result = value; completed = true; this.headersSent = true; return this; } };
  if (throughRouter) router.handle(req, res, error => { failure = error; completed = true; });
  else {
    const layer = router.stack.find(item => item.route && item.route.path === routePath && item.route.methods[method.toLowerCase()]);
    assert.ok(layer, 'route exists: ' + routePath);
    layer.route.stack[0].handle(req, res, () => { throw new Error('Unexpected next'); });
  }
  if (failure) throw failure;
  assert.equal(completed, true, 'router completed: ' + routePath);
  return { status, body: result };
}
invoke('GET', '/api/expenses/config');
const baseline = JSON.parse(fs.readFileSync(filename, 'utf8'));
function seed(extra = {}) {
  const store = Object.assign(structuredClone(baseline), { expenses: {}, customLedgers: {}, customCategoryGroups: [], sankiCategoryCatalog: { version: plan.version, categories: [{ name: 'Flowers', group: 'Devotional & Religious' }] }, auditLog: [], auditSeq: 0 }, extra);
  // Keep synthetic fixtures out of unrelated historical correction workflows.
  store.oneTimeMigrations[categoryFinalizationKey] = { appliedAt: '2097-01-01' };
  fs.writeFileSync(filename, JSON.stringify(store));
  return store;
}
function denyStoreAccess(callback) {
  const original = Object.fromEntries(['readFileSync', 'writeFileSync', 'renameSync'].map(key => [key, fs[key]]));
  for (const key of Object.keys(original)) fs[key] = () => { throw new Error('Unexpected store ' + key); };
  try { callback(); } finally { for (const key of Object.keys(original)) fs[key] = original[key]; }
}

test('group permission is enforced before reading or writing the store, including real routing', () => {
  seed();
  denyStoreAccess(() => {
    for (const role of ['admin', 'accounts', 'claimant', '']) {
      const result = invoke('POST', '/api/expenses/category-groups', { role, username: 'ordinary-' + role, body: { name: 'Forbidden' }, throughRouter: true });
      assert.equal(result.status, 403); assert.match(result.body.error, /Owner or Prashant|Claimants may only/);
    }
  });
});

test('claimant-only Prashant cannot bypass the current category capability', () => {
  seed();
  denyStoreAccess(() => {
    for (const role of ['claimant', 'personal_claimant']) {
      for (const throughRouter of [true, false]) {
        const result = invoke('POST', '/api/expenses/category-groups', { role, username: ' Prashant ', body: { name: 'Forbidden' }, throughRouter });
        assert.equal(result.status, 403);
      }
    }
  });
  assert.equal(invoke('GET', '/api/expenses/config', { role: 'claimant', username: 'prashant', throughRouter: true }).body.canManageCategories, false);
});

test('invalid group names are rejected before store access and never become category requests', () => {
  seed();
  denyStoreAccess(() => {
    for (const name of [undefined, null, 1, {}, [], '', ' ', 'x'.repeat(121), 'Invisible\u200b', 'Line\nBreak']) assert.equal(invoke('POST', '/api/expenses/category-groups', { body: { name } }).status, 400);
  });
});

test('Owner or explicit Prashant can add an empty group, persist it and audit only the group', () => {
  for (const actor of [{ role: 'owner' }, { role: 'admin', username: 'prashant' }, { role: 'admin', username: ' Prashant ' }]) {
    seed();
    const before = JSON.parse(fs.readFileSync(filename, 'utf8'));
    const result = invoke('POST', '/api/expenses/category-groups', { ...actor, body: { name: '  Fresh   Group ' }, throughRouter: true });
    assert.equal(result.status, 200); assert.equal(result.body.success, true); assert.equal(result.body.group, 'Fresh Group'); assert.deepEqual(result.body.groups, ['Devotional & Religious', 'Fresh Group']);
    const after = JSON.parse(fs.readFileSync(filename, 'utf8'));
    assert.deepEqual(after.customCategoryGroups, ['Fresh Group']);
    assert.equal(after.auditLog.length, 1); assert.equal(after.auditLog[0].action, 'CATEGORY_GROUP_CREATED'); assert.equal(after.auditLog[0].subjectType, 'category_group'); assert.equal(after.auditLog[0].subjectId, 'Fresh Group');
    // The legacy loader refreshes a historical migration's bookkeeping timestamp
    // on every read. All financial/master records must otherwise be identical.
    for (const key of Object.keys(before).filter(key => !['customCategoryGroups', 'auditLog', 'auditSeq', 'oneTimeMigrations'].includes(key))) assert.deepEqual(after[key], before[key], 'must preserve ' + key);
    const config = invoke('GET', '/api/expenses/config').body;
    assert.ok(config.categoryGroups.includes('Fresh Group')); assert.equal(config.ledgers.some(ledger => ledger.group === 'Fresh Group'), false, 'group creation must not create a category');
  }
});

test('case/spacing duplicate groups reuse the canonical label with no audit or store write', () => {
  seed({ customCategoryGroups: ['Fresh Group'] });
  const before = fs.readFileSync(filename, 'utf8');
  const originalWrite = fs.writeFileSync, originalRename = fs.renameSync;
  fs.writeFileSync = () => { throw new Error('Duplicate must not write'); }; fs.renameSync = () => { throw new Error('Duplicate must not replace store'); };
  try {
    for (const [name, expected] of [[' fresh   GROUP ', 'Fresh Group'], [' DEVOTIONAL  & religious ', 'Devotional & Religious']]) {
      const result = invoke('POST', '/api/expenses/category-groups', { body: { name } });
      assert.equal(result.status, 200); assert.equal(result.body.already, true); assert.equal(result.body.group, expected);
      assert.equal(result.body.groups.filter(group => group === expected).length, 1);
    }
  } finally { fs.writeFileSync = originalWrite; fs.renameSync = originalRename; }
  assert.equal(fs.readFileSync(filename, 'utf8'), before);
});

test('a new standalone group accepts a category with canonical spelling after reload', () => {
  seed();
  assert.equal(invoke('POST', '/api/expenses/category-groups', { body: { name: 'Fresh Group' } }).status, 200);
  for (const group of ['', 'Unknown Group', {}, '\u0000']) {
    const rejected = invoke('POST', '/api/expenses/custom-ledgers', { body: { name: 'Rejected Category', type: 'variable', group } });
    assert.equal(rejected.status, 400); assert.match(rejected.body.error, /existing SANKI group/);
  }
  const created = invoke('POST', '/api/expenses/custom-ledgers', { role: 'admin', username: 'prashant', body: { name: 'New Category', type: 'running', group: ' fresh   GROUP ' } });
  assert.equal(created.status, 200); assert.equal(created.body.ledger.group, 'Fresh Group');
  const config = invoke('GET', '/api/expenses/config').body;
  assert.equal(config.categoryGroups.filter(group => group === 'Fresh Group').length, 1);
  assert.ok(config.ledgers.some(ledger => ledger.name === 'New Category' && ledger.group === 'Fresh Group'));
  const store = JSON.parse(fs.readFileSync(filename, 'utf8'));
  assert.equal(store.sankiCategoryCatalog.categories.filter(category => category.name === 'New Category').length, 1);
  assert.deepEqual(store.auditLog.map(entry => entry.action), ['CATEGORY_GROUP_CREATED', 'CATEGORY_CREATED']);
});

test('legacy custom category groups remain available without activating or changing a reviewed catalog', () => {
  seed({ sankiCategoryCatalog: undefined, customLedgers: { Legacy: { name: 'Legacy', group: 'Legacy Group', type: 'variable' } }, customCategoryGroups: ['Standalone'] });
  const config = invoke('GET', '/api/expenses/config').body;
  assert.ok(config.categoryGroups.includes('Legacy Group')); assert.ok(config.categoryGroups.includes('Standalone'));
  const result = invoke('POST', '/api/expenses/category-groups', { body: { name: ' legacy group ' } });
  assert.equal(result.body.already, true); assert.equal(result.body.group, 'Legacy Group');
  assert.equal(JSON.parse(fs.readFileSync(filename, 'utf8')).sankiCategoryCatalog, undefined);
});

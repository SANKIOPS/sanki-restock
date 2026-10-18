'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const html = fs.readFileSync(path.join(__dirname, '../public/expenses.html'), 'utf8');
const start = html.indexOf('    function refreshSpendingCategoryList(){');
const end = html.indexOf('    function spendingPaymentCard(', start);
assert.ok(start >= 0 && end > start, 'Spending category UI source must be present');
const source = html.slice(start, end);
const flush = () => new Promise(resolve => setImmediate(resolve));

function harness({ owner = false, manager = false, response } = {}) {
  const nodes = {}, calls = [], messages = [], loads = [];
  const node = id => nodes[id] || (nodes[id] = {
    value: '', textContent: '', innerHTML: '', className: '', disabled: false,
    open: false, listeners: {}, focusCount: 0,
    addEventListener(event, listener) { this.listeners[event] = listener; },
    setAttribute(name, value) { this[name] = value; },
    querySelectorAll() { return []; },
    showModal() { this.open = true; },
    close() { this.open = false; },
    focus() { this.focusCount += 1; }
  });
  const esc = value => String(value == null ? '' : value)
    .replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;').replaceAll("'", '&#39;');
  const context = {
    window: {},
    cfg: {
      isOwner: owner, isAdmin: true, canManageCategories: manager,
      categoryGroups: ['Operating expenses'], types: ['fixed', 'variable'],
      natures: ['SANKI'], accounts: ['Counter Cash'],
      ledgers: [{ name: 'Existing category', group: 'Operating expenses' }]
    },
    el: node, esc,
    opts: values => (values || []).map(value => '<option>' + esc(value) + '</option>').join(''),
    monthStart: () => '2026-10-01', today: () => '2026-10-15',
    setTimeout: callback => callback(),
    setMsg: (...args) => messages.push(args),
    loadSpendingDashboard: () => loads.push('dashboard'),
    loadConfig: () => {
      loads.push('config');
      context.cfg.ledgers.push({ name: 'New category', group: 'Operating expenses' });
      return Promise.resolve();
    },
    api: (url, options) => {
      calls.push({ url, ...options });
      return Promise.resolve(response || { success: true, ledger: { name: 'New category' } });
    }
  };
  vm.createContext(context);
  vm.runInContext(source, context);
  const save = () => node('spendingCategorySave').listeners.click.call(node('spendingCategorySave'));
  return { context, node, nodes, calls, messages, loads, save };
}

test('authorized non-Owner sees Add category and can open the existing dialog', () => {
  const h = harness({ manager: true });
  h.context.renderSpendingDashboard();
  assert.match(h.node('panel').innerHTML, /id="sd_add_category"/);
  assert.match(h.node('panel').innerHTML, /onclick="openManageCategories\(\)"/);
  h.node('spendingCategoryName').value = 'Stale name';
  h.context.window.openSpendingCategoryDialog();
  assert.equal(h.node('spendingCategoryDlg').open, true);
  assert.equal(h.node('spendingCategoryName').value, '');
  assert.equal(h.node('spendingCategoryName').focusCount, 1);
  assert.match(h.node('spendingCategoryGroup').innerHTML, /Operating expenses/);
});

test('authorized non-Owner saves through the existing endpoint then refreshes and selects the category', async () => {
  const h = harness({ manager: true });
  h.context.renderSpendingDashboard();
  h.loads.length = 0;
  h.context.window.openSpendingCategoryDialog();
  h.node('spendingCategoryName').value = '  New category  ';
  h.node('spendingCategoryType').value = 'variable';
  h.node('spendingCategoryGroup').value = 'Operating expenses';
  h.save();
  assert.equal(h.node('spendingCategorySave').disabled, true);
  await flush();
  assert.equal(h.calls.length, 1);
  assert.equal(h.calls[0].url, '/api/expenses/custom-ledgers');
  assert.equal(h.calls[0].method, 'POST');
  assert.deepEqual(JSON.parse(h.calls[0].body), {
    name: 'New category', type: 'variable', group: 'Operating expenses'
  });
  assert.deepEqual(h.loads, ['config', 'dashboard']);
  assert.match(h.node('sd_category_list').innerHTML, /New category/);
  assert.equal(h.node('sd_category').value, 'New category');
  assert.equal(h.node('spendingCategoryDlg').open, false);
  assert.equal(h.node('spendingCategorySave').disabled, false);
  assert.deepEqual(h.messages.at(-1), ['Category added and selected in the dashboard.', true]);
});

test('Owner with category-management capability retains Add category access', async () => {
  const h = harness({ owner: true, manager: true });
  h.context.renderSpendingDashboard();
  assert.match(h.node('panel').innerHTML, /id="sd_add_category"/);
  h.context.window.openSpendingCategoryDialog();
  assert.equal(h.node('spendingCategoryDlg').open, true);
  h.node('spendingCategoryName').value = 'New category';
  h.save();
  await flush();
  assert.equal(h.calls.length, 1);
  assert.equal(h.node('spendingCategoryDlg').open, false);
});

test('ordinary Admin cannot see, open, or submit category creation by invoking global UI actions', async () => {
  const h = harness();
  h.context.renderSpendingDashboard();
  assert.doesNotMatch(h.node('panel').innerHTML, /id="sd_add_category"/);
  h.context.window.openSpendingCategoryDialog();
  assert.equal(h.node('spendingCategoryDlg').open, false);
  h.node('spendingCategoryName').value = 'New category';
  h.save();
  await flush();
  assert.equal(h.calls.length, 0);
  assert.equal(h.node('spendingCategorySave').disabled, false);
});

test('the explicit capability is authoritative rather than the Owner flag alone', async () => {
  const h = harness({ owner: true, manager: false });
  h.context.renderSpendingDashboard();
  assert.doesNotMatch(h.node('panel').innerHTML, /id="sd_add_category"/);
  h.context.window.openSpendingCategoryDialog();
  assert.equal(h.node('spendingCategoryDlg').open, false);
  h.node('spendingCategoryName').value = 'New category';
  h.save();
  await flush();
  assert.equal(h.calls.length, 0);
});

test('revoking category capability after opening prevents a subsequent save', async () => {
  const h = harness({ manager: true });
  h.context.window.openSpendingCategoryDialog();
  assert.equal(h.node('spendingCategoryDlg').open, true);
  h.context.cfg.canManageCategories = false;
  h.node('spendingCategoryName').value = 'New category';
  h.save();
  await flush();
  assert.equal(h.calls.length, 0);
  assert.deepEqual(h.loads, []);
});

test('validation and server rejection do not close the dialog or refresh category data', async () => {
  const h = harness({ manager: true, response: { success: false, error: 'Category permission denied.' } });
  h.context.window.openSpendingCategoryDialog();
  h.node('spendingCategoryName').value = '   ';
  h.save();
  assert.equal(h.calls.length, 0);
  assert.equal(h.node('spendingCategoryMsg').textContent, 'Enter a category name.');
  h.node('spendingCategoryName').value = 'New category';
  h.save();
  await flush();
  assert.equal(h.node('spendingCategoryDlg').open, true);
  assert.equal(h.node('spendingCategoryMsg').textContent, 'Category permission denied.');
  assert.equal(h.node('spendingCategorySave').disabled, false);
  assert.deepEqual(h.loads, []);
});

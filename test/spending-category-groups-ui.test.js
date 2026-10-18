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
const deferred = () => {
  let resolve, reject;
  const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
};

function harness({ manager = true, owner = false, apiResult, configResult } = {}) {
  const nodes = {}, calls = [], loads = [], messages = [];
  const node = id => {
    if (nodes[id]) return nodes[id];
    const element = {
      value: '', textContent: '', className: '', disabled: false, hidden: true,
      style: { display: 'none' }, open: false, focusCount: 0, listeners: {},
      addEventListener(event, listener) { this.listeners[event] = listener; },
      setAttribute(name, value) { this[name] = String(value); },
      querySelectorAll() { return []; },
      focus() { this.focusCount += 1; },
      click() { if (!this.disabled && this.listeners.click) this.listeners.click.call(this, {}); },
      showModal() { this.open = true; },
      close() {
        this.open = false;
        if (this.listeners.close) this.listeners.close.call(this, {});
      }
    };
    let markup = '';
    Object.defineProperty(element, 'innerHTML', {
      get() { return markup; },
      set(value) {
        markup = value;
        if (id === 'spendingCategoryGroup' || id === 'spendingCategoryType') {
          const options = [...markup.matchAll(/<option(?:\s+value="([^"]*)")?([^>]*)>([^<]*)<\/option>/g)];
          const chosen = options.find(option => /\bselected\b/.test(option[2])) || options[0];
          this.value = chosen ? (chosen[1] == null ? chosen[3] : chosen[1]) : '';
        }
      }
    });
    nodes[id] = element;
    return element;
  };
  const esc = value => String(value == null ? '' : value)
    .replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;').replaceAll("'", '&#39;');
  const context = {
    window: {},
    cfg: {
      isOwner: owner, isAdmin: true, canManageCategories: manager,
      categoryGroups: ['Operating expenses', 'Marketing'], types: ['fixed', 'variable'],
      natures: ['SANKI'], accounts: ['Counter Cash'],
      ledgers: [{ name: 'Existing category', group: 'Operating expenses' }]
    },
    el: node, esc,
    opts: (values, selected) => (values || []).map(value => '<option value="' + esc(value) + '"' +
      (value === selected ? ' selected' : '') + '>' + esc(value) + '</option>').join(''),
    monthStart: () => '2026-10-01', today: () => '2026-10-15',
    setTimeout: callback => callback(),
    setMsg: (...args) => messages.push(args),
    loadSpendingDashboard: () => loads.push('dashboard'),
    loadConfig: () => {
      loads.push('config');
      if (configResult) return configResult(context);
      context.cfg.categoryGroups = ['Operating expenses', 'Marketing', 'New group'];
      return Promise.resolve();
    },
    api: (url, options) => {
      calls.push({ url, ...options });
      if (typeof apiResult === 'function') return apiResult(url, options);
      return Promise.resolve(apiResult || { success: true, group: 'New group' });
    }
  };
  vm.createContext(context);
  vm.runInContext(source, context);
  const click = id => {
    assert.equal(typeof node(id).listeners.click, 'function', id + ' must have a click handler');
    return node(id).listeners.click.call(node(id), {});
  };
  const visible = id => node(id).hidden === false || node(id).style.display !== 'none';
  const open = () => context.window.openSpendingCategoryDialog();
  const draft = () => {
    node('spendingCategoryName').value = 'Category draft';
    node('spendingCategoryType').value = 'fixed';
    node('spendingCategoryGroup').value = 'Marketing';
  };
  const checkDraft = (group = 'Marketing') => {
    assert.equal(node('spendingCategoryName').value, 'Category draft');
    assert.equal(node('spendingCategoryType').value, 'fixed');
    assert.equal(node('spendingCategoryGroup').value, group);
  };
  return { context, node, calls, loads, messages, click, visible, open, draft, checkDraft };
}

test('Add group is an accessible inline action in the existing category dialog', () => {
  assert.match(html, /id="spendingCategoryAddGroup"/);
  assert.match(html, /id="spendingGroupFields"/);
  assert.match(html, /id="spendingGroupName"[^>]*maxlength="120"/);
  assert.match(html, /id="spendingGroupMsg"[^>]*role="status"/);
  const h = harness();
  h.open();
  assert.equal(h.visible('spendingGroupFields'), false);
  h.draft();
  h.click('spendingCategoryAddGroup');
  assert.equal(h.visible('spendingGroupFields'), true);
  assert.equal(h.node('spendingGroupName').focusCount, 1);
  h.checkDraft();
  h.node('spendingGroupName').value = 'Discard this';
  h.click('spendingGroupCancel');
  assert.equal(h.visible('spendingGroupFields'), false);
  assert.equal(h.node('spendingGroupName').value, '');
  assert.equal(h.node('spendingCategoryDlg').open, true);
  h.checkDraft();
});

test('adding a group refreshes and selects it without submitting or clearing the category draft', async () => {
  const h = harness();
  h.open();
  h.draft();
  h.click('spendingCategoryAddGroup');
  h.node('spendingGroupName').value = '  New group  ';
  h.click('spendingGroupSave');
  assert.equal(h.node('spendingGroupSave').disabled, true);
  assert.equal(h.node('spendingGroupCancel').disabled, true);
  assert.equal(h.node('spendingCategorySave').disabled, true);
  assert.equal(h.node('spendingCategoryCancel').disabled, true);
  assert.equal(h.node('spendingCategoryAddGroup').disabled, true);
  await flush();
  assert.equal(h.calls.length, 1);
  assert.equal(h.calls[0].url, '/api/expenses/category-groups');
  assert.equal(h.calls[0].method, 'POST');
  assert.equal(h.calls[0].headers['Content-Type'], 'application/json');
  assert.deepEqual(JSON.parse(h.calls[0].body), { name: 'New group' });
  assert.deepEqual(h.loads, ['config']);
  h.checkDraft('New group');
  assert.match(h.node('spendingCategoryGroup').innerHTML, /New group/);
  assert.equal(h.node('spendingCategoryDlg').open, true);
  assert.equal(h.visible('spendingGroupFields'), false);
  assert.equal(h.node('spendingGroupName').value, '');
  assert.match(h.node('spendingCategoryMsg').textContent, /group/i);
  for (const id of ['spendingGroupSave', 'spendingGroupCancel', 'spendingCategorySave', 'spendingCategoryCancel', 'spendingCategoryAddGroup']) {
    assert.equal(h.node(id).disabled, false, id + ' is re-enabled');
  }
});

test('an already existing group is selected using the canonical server name', async () => {
  const h = harness({ apiResult: { success: true, already: true, group: 'Marketing' } });
  h.open();
  h.draft();
  h.click('spendingCategoryAddGroup');
  h.node('spendingGroupName').value = 'marketing';
  h.click('spendingGroupSave');
  await flush();
  h.checkDraft();
  assert.equal(h.visible('spendingGroupFields'), false);
  assert.match(h.node('spendingCategoryMsg').textContent, /exist/i);
  assert.equal(h.calls.length, 1);
});

test('blank group validation stays inline and does not submit or change the category draft', async () => {
  const h = harness();
  h.open();
  h.draft();
  h.click('spendingCategoryAddGroup');
  h.node('spendingGroupName').value = '   ';
  h.click('spendingGroupSave');
  await flush();
  assert.equal(h.calls.length, 0);
  assert.deepEqual(h.loads, []);
  assert.match(h.node('spendingGroupMsg').textContent, /group name/i);
  assert.equal(h.visible('spendingGroupFields'), true);
  h.checkDraft();
});

test('overlong names are rejected even when entered outside the HTML input limit', async () => {
  const h = harness();
  h.open();
  h.draft();
  h.click('spendingCategoryAddGroup');
  h.node('spendingGroupName').value = 'x'.repeat(121);
  h.click('spendingGroupSave');
  await flush();
  assert.equal(h.calls.length, 0);
  assert.match(h.node('spendingGroupMsg').textContent, /120/);
  h.checkDraft();
});

test('Enter in the new group input submits only the group', async () => {
  const h = harness();
  h.open();
  h.draft();
  h.click('spendingCategoryAddGroup');
  h.node('spendingGroupName').value = 'New group';
  let prevented = false;
  h.node('spendingGroupName').listeners.keydown({ key: 'Enter', preventDefault() { prevented = true; } });
  await flush();
  assert.equal(prevented, true);
  assert.equal(h.calls.length, 1);
  assert.equal(h.calls[0].url, '/api/expenses/category-groups');
  h.checkDraft('New group');
});

test('new group can be used immediately in the subsequent category save', async () => {
  const h = harness({ apiResult: url => Promise.resolve(url.endsWith('/category-groups')
    ? { success: true, group: 'New group' }
    : { success: true, ledger: { name: 'Category draft' } }) });
  h.open();
  h.draft();
  h.click('spendingCategoryAddGroup');
  h.node('spendingGroupName').value = 'New group';
  h.click('spendingGroupSave');
  await flush();
  h.click('spendingCategorySave');
  await flush();
  assert.equal(h.calls.length, 2);
  assert.equal(h.calls[1].url, '/api/expenses/custom-ledgers');
  assert.deepEqual(JSON.parse(h.calls[1].body), { name: 'Category draft', type: 'fixed', group: 'New group' });
  assert.equal(h.node('spendingCategoryDlg').open, false);
});

test('refreshed group option names are HTML escaped', () => {
  const h = harness();
  h.context.cfg.categoryGroups = ['<img src=x onerror=alert(1)>'];
  h.context.refreshSpendingGroupChoices();
  assert.doesNotMatch(h.node('spendingCategoryGroup').innerHTML, /<img/);
  assert.match(h.node('spendingCategoryGroup').innerHTML, /&lt;img/);
});

for (const [label, options, error] of [
  ['server rejection', { apiResult: { success: false, error: 'Not allowed to create this group.' } }, 'Not allowed to create this group.'],
  ['network failure', { apiResult: () => Promise.reject(new Error('Network unavailable')) }, 'Network unavailable'],
  ['configuration refresh failure', { configResult: () => Promise.reject(new Error('Refresh failed')) }, 'The group was saved, but the list could not refresh. Retry to select the existing group.']
]) {
  test(label + ' preserves the category and group drafts and restores controls', async () => {
    const h = harness(options);
    h.open();
    h.draft();
    h.click('spendingCategoryAddGroup');
    h.node('spendingGroupName').value = 'New group';
    h.click('spendingGroupSave');
    await flush();
    assert.equal(h.node('spendingGroupMsg').textContent, error);
    assert.equal(h.node('spendingGroupName').value, 'New group');
    assert.equal(h.node('spendingCategoryDlg').open, true);
    assert.equal(h.visible('spendingGroupFields'), true);
    h.checkDraft();
    assert.equal(h.node('spendingGroupSave').disabled, false);
    assert.equal(h.node('spendingCategorySave').disabled, false);
    assert.equal(h.node('spendingCategoryCancel').disabled, false);
    assert.equal(h.node('spendingGroupCancel').disabled, false);
  });
}

test('group creation obeys category-management capability, not the Owner label', async () => {
  for (const owner of [false, true]) {
    const h = harness({ manager: false, owner });
    h.open();
    h.node('spendingGroupName').value = 'New group';
    h.click('spendingCategoryAddGroup');
    h.click('spendingGroupSave');
    await flush();
    assert.equal(h.node('spendingCategoryDlg').open, false);
    assert.equal(h.calls.length, 0);
    assert.deepEqual(h.loads, []);
  }
  const h = harness();
  h.open();
  h.click('spendingCategoryAddGroup');
  h.context.cfg.canManageCategories = false;
  h.node('spendingGroupName').value = 'New group';
  h.click('spendingGroupSave');
  await flush();
  assert.equal(h.calls.length, 0);
  assert.deepEqual(h.loads, []);
});

test('repeated group saves and category saves cannot race while a group request is pending', async () => {
  const waiting = deferred();
  const h = harness({ apiResult: () => waiting.promise });
  h.open();
  h.draft();
  h.click('spendingCategoryAddGroup');
  h.node('spendingGroupName').value = 'New group';
  h.click('spendingGroupSave');
  h.click('spendingGroupSave');
  h.click('spendingCategorySave');
  assert.equal(h.calls.length, 1);
  let prevented = false;
  h.node('spendingCategoryDlg').listeners.cancel({ preventDefault() { prevented = true; } });
  assert.equal(prevented, true);
  waiting.resolve({ success: true, group: 'New group' });
  await flush();
  assert.equal(h.calls.length, 1);
  h.checkDraft('New group');
});

test('closing and reopening the parent dialog resets only the new session, ignoring a stale group response', async () => {
  const waiting = deferred();
  const h = harness({ apiResult: () => waiting.promise });
  h.open();
  h.click('spendingCategoryAddGroup');
  h.node('spendingGroupName').value = 'New group';
  h.click('spendingGroupSave');
  h.node('spendingCategoryDlg').close();
  h.open();
  h.draft();
  h.click('spendingCategoryAddGroup');
  h.node('spendingGroupName').value = 'Unrelated new draft';
  waiting.resolve({ success: true, group: 'New group' });
  await flush();
  h.checkDraft();
  assert.equal(h.node('spendingCategoryDlg').open, true);
  assert.equal(h.visible('spendingGroupFields'), true);
  assert.equal(h.node('spendingGroupName').value, 'Unrelated new draft');
  assert.equal(h.node('spendingCategoryMsg').textContent, '');
  assert.equal(h.node('spendingGroupSave').disabled, false);
});

test('a stale configuration refresh cannot overwrite a reopened category dialog', async () => {
  const waiting = deferred();
  const h = harness({ configResult: () => waiting.promise });
  h.open();
  h.click('spendingCategoryAddGroup');
  h.node('spendingGroupName').value = 'New group';
  h.click('spendingGroupSave');
  await flush();
  assert.deepEqual(h.loads, ['config']);
  h.node('spendingCategoryDlg').close();
  h.open();
  h.draft();
  h.click('spendingCategoryAddGroup');
  h.node('spendingGroupName').value = 'New session group';
  waiting.resolve();
  await flush();
  h.checkDraft();
  assert.equal(h.node('spendingGroupName').value, 'New session group');
  assert.equal(h.visible('spendingGroupFields'), true);
  assert.equal(h.node('spendingCategoryMsg').textContent, '');
});

test('a stale group rejection does not put an error into a new dialog session', async () => {
  const waiting = deferred();
  const h = harness({ apiResult: () => waiting.promise });
  h.open();
  h.click('spendingCategoryAddGroup');
  h.node('spendingGroupName').value = 'New group';
  h.click('spendingGroupSave');
  h.node('spendingCategoryDlg').close();
  h.open();
  h.draft();
  waiting.reject(new Error('Old request failure'));
  await flush();
  h.checkDraft();
  assert.equal(h.node('spendingGroupMsg').textContent, '');
  assert.equal(h.node('spendingCategoryMsg').textContent, '');
  assert.equal(h.node('spendingGroupSave').disabled, false);
});

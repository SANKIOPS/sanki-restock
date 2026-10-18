'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const expenses = fs.readFileSync(path.join(__dirname, '../public/expenses.html'), 'utf8');
const dashboard = fs.readFileSync(path.join(__dirname, '../public/dashboard.html'), 'utf8');
const navigation = fs.readFileSync(path.join(__dirname, '../public/navigation-state.js'), 'utf8');
function section(source, start, end) {
  const offset = source.indexOf(start);
  assert.ok(offset >= 0, start);
  const finish = source.indexOf(end, offset);
  assert.ok(finish > offset, end);
  return source.slice(offset, finish);
}
const escape = value => String(value == null ? '' : value).replace(/[&<>"']/g, x => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[x]));
async function render(kind, reminders) {
  const host = { className: '', innerHTML: '' };
  const data = { success: true, reminders };
  const context = { cfg: { isAdmin: true }, esc: escape, el: () => host, document: { getElementById: () => host }, api: () => Promise.resolve(data) };
  vm.createContext(context);
  if (kind === 'dashboard') {
    vm.runInContext(section(dashboard, 'function renderReconciliationAttention(', "      fetch('/api/auth/me'"), context);
    context.renderReconciliationAttention(data);
  } else {
    vm.runInContext(section(expenses, 'function loadReconciliationAttention(', '// Same-page proof viewer'), context);
    context.loadReconciliationAttention();
    await Promise.resolve();
  }
  return host;
}
const awaiting = attentionRequired => ({ account: 'Example Bank 0123', status: 'approval_pending', attentionRequired, pendingDraft: true, draftTo: '2026-10-13', actionLabel: 'Approve / finalize reconciliation', href: '/expenses.html?tab=cash' });
for (const kind of ['dashboard', 'ledger']) {
  test(kind + ' hides a submitted approval-only reconciliation from the admin warning panel', async () => {
    const host = await render(kind, [awaiting(false)]);
    assert.equal(host.innerHTML, '');
    assert.ok(!host.className.includes('show'));
  });
  test(kind + ' retains the owner approval reminder', async () => {
    const host = await render(kind, [awaiting(true)]);
    assert.ok(host.className.includes('show'));
    assert.match(host.innerHTML, /Approve \/ finalize reconciliation/);
    assert.match(host.innerHTML, /Example Bank 0123/);
  });
  test(kind + ' retains unresolved, overdue and legacy warnings alongside a hidden approval', async () => {
    const host = await render(kind, [awaiting(false), { account: 'Unresolved account', status: 'review_pending', attentionRequired: true, href: '/expenses.html' }, { account: 'New statement due', status: 'statement_required', attentionRequired: true, href: '/expenses.html' }, { account: 'Legacy server reminder', status: 'review_pending', href: '/expenses.html' }, { account: 'Finished account', status: 'up_to_date', href: '/expenses.html' }]);
    assert.doesNotMatch(host.innerHTML, /Example Bank 0123|Finished account/);
    assert.match(host.innerHTML, /Unresolved account/);
    assert.match(host.innerHTML, /New statement due/);
    assert.match(host.innerHTML, /Legacy server reminder/);
  });
}
async function finalize(response) {
  const elements = { bs_finalize: { dataset: {} }, bs_msg: {} }, calls = { attention: 0, bank: 0, cash: 0, account: 0, navigation: [] };
  const context = { URL, location: new URL('https://example.test/expenses.html?tab=cash&draft=SYNTHETIC-DRAFT'), cfg: { isOwner: false }, bankDraftId: 'SYNTHETIC-DRAFT', bankReconView: { balanceResolved: true }, el: id => elements[id], confirm: () => true, fmt: String, api: () => Promise.resolve(response), loadBankStatement: () => calls.bank++, loadCash: () => calls.cash++, loadAccountLedger: () => calls.account++, loadReconciliationAttention: () => calls.attention++ };
  // Match the actual browser load order, rather than stubbing the navigation
  // helper used to clear only the finalized preview's URL context.
  context.window = context;
  context.history = { state: null, replaceState(state, title, target) { calls.navigation.push(target); context.location = new URL(target, context.location.href); } };
  vm.createContext(context);
  vm.runInContext(navigation, context);
  vm.runInContext(section(expenses, 'function finalizeBankStatement(', 'function bankHistoryRows('), context);
  context.finalizeBankStatement();
  await Promise.resolve();
  return { calls, context, elements };
}
test('successful submission refreshes reminders immediately and retains the awaiting-owner preview', async () => {
  const result = await finalize({ success: true, approvalPending: true });
  assert.equal(result.calls.attention, 1);
  assert.equal(result.calls.bank, 1);
  assert.equal(result.context.bankDraftId, 'SYNTHETIC-DRAFT');
  assert.equal(result.elements.bs_msg.className, 'msg ok');
  assert.equal(result.calls.navigation.length, 0);
  assert.equal(result.context.location.searchParams.get('draft'), 'SYNTHETIC-DRAFT');
});
test('successful finalization refreshes reminders and failed submission never hides an alert', async () => {
  const completed = await finalize({ success: true, balanceReconciled: true, reconciledThrough: '2026-10-13' });
  assert.equal(completed.calls.attention, 1);
  assert.equal(completed.calls.account, 1);
  assert.equal(completed.calls.navigation.length, 1);
  assert.equal(completed.context.bankDraftId, '');
  assert.equal(completed.context.location.searchParams.get('draft'), null);
  assert.equal(completed.context.location.searchParams.get('tab'), 'cash');
  const failed = await finalize({ success: false, error: 'Unresolved entries remain' });
  assert.equal(failed.calls.attention, 0);
  assert.equal(failed.calls.bank, 0);
  assert.equal(failed.context.bankDraftId, 'SYNTHETIC-DRAFT');
  assert.equal(failed.calls.navigation.length, 0);
  assert.equal(failed.context.location.searchParams.get('draft'), 'SYNTHETIC-DRAFT');
});

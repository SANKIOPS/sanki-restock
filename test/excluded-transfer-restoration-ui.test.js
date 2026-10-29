'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const html = fs.readFileSync(path.join(__dirname, '../public/expenses.html'), 'utf8');
const start = html.indexOf('    // Restoring an excluded transfer');
const end = html.indexOf('    var openGoKwikBankReview=', start);
assert.ok(start >= 0 && end > start, 'the actual protected restoration UI is present');
const source = html.slice(start, end);
assert.match(html, /Restore excluded ['"]\+excluded\.transferId/);
assert.match(html, /openExcludedTransferRestore\(rowId,excluded\.transferId\)/);
const flush = () => new Promise(resolve => setImmediate(resolve));
const copy = value => JSON.parse(JSON.stringify(value));
const selectors = { id: '#excludedRestoreId', reason: '#excludedRestoreReason', confirm: '[data-restore-confirm]', preview: '[data-restore-preview]', save: '[data-restore-save]', close: '[data-restore-close]', summary: '[data-restore-summary]', message: '[data-restore-message]', bank: '[data-restore-bank]' };
function verifiedPlan(overrides = {}) {
  return { version: 1, intent: { draftId: 'SYNTHETIC-DRAFT', rowId: 'bank-0', transferId: 'TR-00042' }, fingerprint: 'a'.repeat(64), alreadyRestored: false,
    transfer: { id: 'TR-00042', date: '2098-04-17', amount: 1750.25, fromAccount: 'Namita 5464', toAccount: 'ICICI Bank 0992' }, ...overrides };
}
function deferred() { let resolve, reject; const promise = new Promise((yes, no) => { resolve = yes; reject = no; }); return { promise, resolve, reject }; }
function harness(options = {}) {
  const nodes = {}, created = [], dialogs = [], calls = [], refreshes = [], messages = [], baseReviews = [];
  function element(tag = 'div') {
    const children = [], listeners = {}, queried = {};
    let inner = '';
    return { tagName: tag.toUpperCase(), children, style: {}, value: '', checked: false, disabled: false, textContent: '', className: '', open: false, removed: false,
      get innerHTML() { return inner; }, set innerHTML(value) { inner = value; children.length = 0; },
      appendChild(child) { children.push(child); child.parent = this; return child; },
      querySelector(selector) { if (!queried[selector]) { queried[selector] = element(selector === selectors.reason ? 'textarea' : selector === selectors.id || selector === selectors.confirm ? 'input' : 'div'); if (selector === selectors.save) queried[selector].disabled = true; } return queried[selector]; },
      addEventListener(type, fn) { listeners[type] = fn; },
      dispatch(type, event = {}) { if (listeners[type]) listeners[type](event); },
      showModal() { this.open = true; }, close() { this.open = false; this.dispatch('close'); },
      remove() { this.removed = true; if (this.parent) { const i = this.parent.children.indexOf(this); if (i >= 0) this.parent.children.splice(i, 1); } },
      focus() { this.focused = true; }
    };
  }
  const row = { id: 'bank-0', status: 'missing_in_app', bank: { date: '2098-04-17', debit: 1750.25, credit: 0, reference: '900000005321', description: 'Synthetic retained UPI bank evidence' }, ...options.row };
  const context = { cfg: { isOwner: true, canManagePersonalAccounting: true, ...options.cfg }, bankNature: options.bankNature || 'PERSONAL', bankDraftId: 'SYNTHETIC-DRAFT', Promise,
    el(id) { return nodes[id] ||= element(); },
    bankReconRow(id) { return id === row.id ? row : null; }, bankEntityTaggingActive() { return !!options.shared; },
    bankReconAmount(value) { return Number(value.bank.debit || value.bank.credit || 0); }, fmt(value) { return '₹' + Number(value).toFixed(2); },
    openBankReview(id) { baseReviews.push(id); context.el('bankReviewActions').innerHTML = ''; }, closeBankReview() { context.reviewClosed = true; },
    api(url, request) { const call = { url, method: request.method, headers: request.headers, body: JSON.parse(request.body) }; calls.push(call); if (options.respond) return options.respond(call, calls.length); return Promise.resolve(url.endsWith('/preview') ? { success: true, plan: verifiedPlan(options.plan) } : { success: true }); },
    setMsg(value, success) { messages.push({ value, success }); },
    loadBankStatement(id) { refreshes.push(['statement', id]); }, loadCash() { refreshes.push(['cash']); }, loadAccountLedger() { refreshes.push(['ledger']); },
    document: { body: element('body'), createElement(tag) { const node = element(tag); created.push(node); if (tag === 'dialog') dialogs.push(node); return node; } }
  };
  context.window = context;
  vm.createContext(context); vm.runInContext(source, context);
  function open() { context.openExcludedTransferRestore('bank-0'); const dialog = dialogs.at(-1); assert.ok(dialog); return { dialog, ...Object.fromEntries(Object.entries(selectors).map(([key, selector]) => [key, dialog.querySelector(selector)])) }; }
  async function preview(form = open()) { form.id.value = 'TR-00042'; form.preview.onclick(); await flush(); return form; }
  function confirm(form) { form.reason.value = 'Owner verified this was an accidental exclusion.'; form.reason.oninput(); form.confirm.checked = true; form.confirm.onchange(); }
  return { context, row, nodes, dialogs, created, calls, refreshes, messages, baseReviews, open, preview, confirm };
}

test('Owner gets one restore action on an unmatched PERSONAL bank row without automatic requests', () => {
  const ui = harness(); ui.context.openBankReview('bank-0');
  assert.deepEqual(ui.baseReviews, ['bank-0']);
  assert.equal(ui.nodes.bankReviewActions.children.length, 1);
  assert.equal(ui.nodes.bankReviewActions.children[0].textContent, 'Restore excluded transfer');
  assert.equal(ui.calls.length, 0);
  ui.nodes.bankReviewActions.children[0].onclick();
  assert.equal(ui.context.reviewClosed, true); assert.equal(ui.dialogs.length, 1); assert.equal(ui.calls.length, 0);
});

test('delegated personal-account administrators see an Owner-or-Prashant hint, not a restore action', () => {
  const ui = harness({ cfg: { isOwner: false } }); ui.context.openBankReview('bank-0');
  assert.equal(ui.nodes.bankReviewActions.children.length, 1);
  assert.equal(ui.nodes.bankReviewActions.children[0].tagName, 'P');
  assert.match(ui.nodes.bankReviewActions.children[0].textContent, /Owner or Prashant.*original ID.*Do not create a replacement/);
  ui.context.openExcludedTransferRestore('bank-0'); assert.equal(ui.dialogs.length, 0); assert.equal(ui.calls.length, 0);
});

test('Prashant gets the same exact-transfer restoration action as the Owner', () => {
  const ui = harness({ cfg: { isOwner: false, isPrashant: true } }); ui.context.openBankReview('bank-0');
  assert.equal(ui.nodes.bankReviewActions.children.length, 1);
  assert.equal(ui.nodes.bankReviewActions.children[0].textContent, 'Restore excluded transfer');
  ui.nodes.bankReviewActions.children[0].onclick(); assert.equal(ui.dialogs.length, 1); assert.equal(ui.calls.length, 0);
});

test('unprivileged users, business-only rows and already matched rows do not get a restore button', () => {
  for (const options of [{ cfg: { isOwner: false, canManagePersonalAccounting: false } }, { bankNature: 'SANKI' }, { row: { status: 'matched' } }, { row: { status: 'resolved' } }]) {
    const ui = harness(options); ui.context.openBankReview('bank-0'); assert.equal(ui.nodes.bankReviewActions.children.length, 0); assert.equal(ui.calls.length, 0);
  }
  const shared = harness({ bankNature: 'SAMAST', shared: true }); shared.context.openBankReview('bank-0'); assert.equal(shared.nodes.bankReviewActions.children[0].tagName, 'BUTTON');
});

test('opening requires a current draft and bank row and never changes money', () => {
  const ui = harness(); ui.context.bankDraftId = ''; ui.context.openExcludedTransferRestore('bank-0'); assert.equal(ui.dialogs.length, 0);
  ui.context.bankDraftId = 'SYNTHETIC-DRAFT'; ui.context.openExcludedTransferRestore('bank-999'); assert.equal(ui.dialogs.length, 0);
  const form = ui.open(); assert.equal(form.dialog.open, true); assert.equal(form.id.focused, true); assert.equal(form.save.disabled, true);
  assert.match(form.dialog.innerHTML, /does not send money, create a transfer or add an adjustment/);
  assert.doesNotMatch(form.dialog.innerHTML, /type=["']number|<select|name=["'](?:amount|date|account)/i);
  assert.match(form.bank.textContent, /2098-04-17.*₹1750.25/); assert.equal(ui.calls.length, 0);
});

test('preview sends only normalized exact ID and selected bank identity and displays immutable bank details', async () => {
  const ui = harness(), form = ui.open(); form.id.value = '  tr-00042  '; form.preview.onclick(); await flush();
  assert.equal(ui.calls.length, 1); assert.equal(ui.calls[0].url, '/api/expenses/bank-statements/restore-excluded-transfer/preview');
  assert.equal(ui.calls[0].method, 'POST'); assert.deepEqual(ui.calls[0].body, { draftId: 'SYNTHETIC-DRAFT', rowId: 'bank-0', transferId: 'TR-00042' });
  assert.match(form.summary.textContent, /TR-00042.*2098-04-17.*₹1750.25.*Namita 5464 → ICICI Bank 0992/);
  assert.equal(form.save.disabled, true); assert.equal(form.confirm.checked, false); assert.deepEqual(ui.refreshes, []);
});

test('invalid original-ID inputs never call the API', () => {
  for (const value of ['', 'TR-42<script>', 'TR-', 'EX-00042', '42', 'TR-00042,TR-00043']) {
    const ui = harness(), form = ui.open(); form.id.value = value; form.preview.onclick(); assert.equal(ui.calls.length, 0); assert.match(form.message.textContent, /original TR reference/); assert.equal(form.save.disabled, true);
  }
});

test('mismatched or malformed server preview cannot authorize another transfer or statement', async () => {
  const invalid = [
    { transfer: { ...verifiedPlan().transfer, id: 'TR-00043' } },
    { intent: { ...verifiedPlan().intent, draftId: 'OTHER-DRAFT' } },
    { intent: { ...verifiedPlan().intent, rowId: 'bank-1' } },
    { intent: { ...verifiedPlan().intent, transferId: 'TR-00043' } },
    { fingerprint: '' }, { fingerprint: 'not-a-verified-fingerprint' }, { version: 0 }
  ];
  for (const plan of invalid) {
    const ui = harness({ plan }), form = await ui.preview(); ui.confirm(form);
    assert.equal(form.save.disabled, true, JSON.stringify(plan)); form.save.onclick(); assert.equal(ui.calls.length, 1);
    assert.equal(form.dialog.removed, false); assert.deepEqual(ui.refreshes, []);
  }
});

test('a successful preview still requires detailed reason and explicit checkbox confirmation', async () => {
  const ui = harness(), form = await ui.preview();
  form.save.onclick(); assert.equal(ui.calls.length, 1);
  form.confirm.checked = true; form.confirm.onchange(); assert.equal(form.save.disabled, true);
  form.reason.value = 'yes'; form.reason.oninput(); assert.equal(form.save.disabled, true);
  form.reason.value = 'The Owner verified the original transfer.'; form.reason.oninput(); assert.equal(form.save.disabled, false);
  form.confirm.checked = false; form.confirm.onchange(); assert.equal(form.save.disabled, true);
});

test('apply sends no editable money fields and never calls creation, adjustment or finalization APIs', async () => {
  const ui = harness(), form = await ui.preview(); ui.confirm(form); form.save.onclick(); await flush();
  assert.equal(ui.calls.length, 2); assert.equal(ui.calls[1].url, '/api/expenses/bank-statements/restore-excluded-transfer/apply');
  assert.deepEqual(ui.calls[1].body, { draftId: 'SYNTHETIC-DRAFT', rowId: 'bank-0', transferId: 'TR-00042', fingerprint: 'a'.repeat(64), reason: 'Owner verified this was an accidental exclusion.', confirm: true });
  assert.ok(ui.calls.every(call => !/\/(?:resolve|finalize|adjustments|transfers|payments)$/.test(call.url)));
  assert.equal(form.dialog.removed, true); assert.equal(form.dialog.open, false);
  assert.deepEqual(ui.refreshes, [['statement', 'SYNTHETIC-DRAFT'], ['cash'], ['ledger']]);
  assert.match(ui.messages[0].value, /No duplicate, adjustment or money movement.*finalization remains separate/);
});

test('changing original ID invalidates preview and checkbox and cannot apply the previous ID', async () => {
  const ui = harness(), form = await ui.preview(); ui.confirm(form); assert.equal(form.save.disabled, false);
  form.id.value = 'TR-00043'; form.id.oninput(); assert.equal(form.save.disabled, true); assert.equal(form.confirm.checked, false); assert.equal(form.summary.textContent, '');
  form.save.onclick(); assert.equal(ui.calls.length, 1);
});

test('changing the selected draft blocks both preview and apply', async () => {
  const ui = harness(), form = await ui.preview(); ui.confirm(form); ui.context.bankDraftId = 'OTHER-DRAFT';
  form.save.onclick(); form.preview.onclick(); assert.equal(ui.calls.length, 1); assert.match(form.message.textContent, /selected statement changed/); assert.equal(form.dialog.removed, false); assert.deepEqual(ui.refreshes, []);
});

test('a pending preview disables identity and close controls and prevents Escape dismissal', async () => {
  const pending = deferred(), ui = harness({ respond: () => pending.promise }), form = ui.open();
  form.id.value = 'TR-00042'; form.preview.onclick();
  assert.equal(form.id.disabled, true); assert.equal(form.preview.disabled, true); assert.equal(form.close.disabled, true); assert.equal(form.save.disabled, true);
  let prevented = false; form.dialog.dispatch('cancel', { preventDefault() { prevented = true; } }); form.close.onclick();
  assert.equal(prevented, true); assert.equal(form.dialog.removed, false);
  pending.resolve({ success: true, plan: verifiedPlan() }); await flush();
  assert.equal(form.id.disabled, false); assert.equal(form.preview.disabled, false); assert.equal(form.close.disabled, false);
});

test('a preview completing after the statement changes cannot enable restoration', async () => {
  const pending = deferred(), ui = harness({ respond: () => pending.promise }), form = ui.open();
  form.id.value = 'TR-00042'; form.preview.onclick(); ui.context.bankDraftId = 'OTHER-DRAFT';
  pending.resolve({ success: true, plan: verifiedPlan() }); await flush(); ui.confirm(form);
  assert.equal(form.save.disabled, true); assert.match(form.message.textContent, /statement changed/); form.save.onclick(); assert.equal(ui.calls.length, 1); assert.deepEqual(ui.refreshes, []);
});

test('double-clicking save submits only one correction while the first request is pending', async () => {
  const pending = deferred(), ui = harness({ respond(call) { return call.url.endsWith('/preview') ? Promise.resolve({ success: true, plan: verifiedPlan() }) : pending.promise; } }), form = await ui.preview();
  ui.confirm(form); form.save.onclick(); form.save.onclick(); assert.equal(ui.calls.length, 2); assert.equal(form.save.disabled, true); assert.equal(form.id.disabled, true);
  pending.resolve({ success: true }); await flush(); assert.equal(ui.calls.length, 2); assert.equal(form.dialog.removed, true);
});

for (const failure of [{ success: false, error: 'Conflicting retained bank evidence' }, new Error('Network unavailable')]) {
  test('preview failure preserves editable form and no ledger refresh: ' + (failure.error || failure.message), async () => {
    const ui = harness({ respond: () => failure instanceof Error ? Promise.reject(failure) : Promise.resolve(failure) }), form = await ui.preview();
    assert.equal(form.dialog.removed, false); assert.equal(form.dialog.open, true); assert.equal(form.save.disabled, true); assert.equal(form.preview.disabled, false);
    assert.equal(form.id.value, 'TR-00042'); assert.match(form.message.textContent, /Conflicting|Network/); assert.deepEqual(ui.refreshes, []); assert.deepEqual(ui.messages, []);
  });
}

test('stale apply failure preserves reason and ID, clears authority to retry, and does not refresh money views', async () => {
  const ui = harness({ respond(call) { return Promise.resolve(call.url.endsWith('/preview') ? { success: true, plan: verifiedPlan() } : { success: false, error: 'Evidence changed after preview. Refresh and review it again.' }); } }), form = await ui.preview();
  ui.confirm(form); form.save.onclick(); await flush();
  assert.equal(form.dialog.open, true); assert.equal(form.dialog.removed, false); assert.equal(form.id.value, 'TR-00042'); assert.match(form.reason.value, /accidental exclusion/);
  assert.equal(form.confirm.checked, false); assert.equal(form.save.disabled, true); assert.equal(form.preview.disabled, false); assert.match(form.message.textContent, /changed after preview/); assert.deepEqual(ui.refreshes, []);
  form.save.onclick(); assert.equal(ui.calls.length, 2, 'failed plan cannot be replayed without a new preview');
});

test('already-restored preview is explanatory and cannot perform another correction', async () => {
  const ui = harness({ plan: { alreadyRestored: true } }), form = await ui.preview(); ui.confirm(form);
  assert.equal(form.save.disabled, true); assert.match(form.message.textContent, /already recorded.*Nothing else/); form.save.onclick(); assert.equal(ui.calls.length, 1); assert.deepEqual(ui.refreshes, []);
});

test('idempotent apply acknowledgement closes once and does not create another request', async () => {
  const ui = harness({ respond(call) { return Promise.resolve(call.url.endsWith('/preview') ? { success: true, plan: verifiedPlan() } : { success: true, restored: false, alreadyRestored: true }); } }), form = await ui.preview();
  ui.confirm(form); form.save.onclick(); await flush(); assert.equal(ui.calls.length, 2); assert.equal(form.dialog.removed, true); assert.equal(ui.messages.length, 1);
});

test('an in-flight successful apply cannot replace another currently selected statement', async () => {
  const pending = deferred(), ui = harness({ respond(call) { return call.url.endsWith('/preview') ? Promise.resolve({ success: true, plan: verifiedPlan() }) : pending.promise; } }), form = await ui.preview();
  ui.confirm(form); form.save.onclick(); ui.context.bankDraftId = 'NEWLY-SELECTED-DRAFT';
  pending.resolve({ success: true }); await flush(); assert.equal(form.dialog.removed, true);
  assert.deepEqual(ui.refreshes, [['cash'], ['ledger']], 'cash may refresh but another statement is never replaced');
});

test('server and statement prose are rendered as text rather than injected executable HTML', async () => {
  const malicious = '<img src=x onerror=alert(1)>', plan = verifiedPlan(); plan.transfer.fromAccount = malicious;
  const ui = harness({ row: { bank: { date: '2098-04-17', debit: 1750.25, description: malicious } }, respond: () => Promise.resolve({ success: true, plan: copy(plan) }) }), form = await ui.preview();
  assert.ok(form.bank.textContent.includes(malicious)); assert.equal(form.bank.innerHTML, ''); assert.ok(form.summary.textContent.includes(malicious)); assert.equal(form.summary.innerHTML, '');
});

test('ordinary close removes only the correction form without any API or account mutation', () => {
  const ui = harness(), form = ui.open(); form.close.onclick(); assert.equal(form.dialog.removed, true); assert.equal(form.dialog.open, false); assert.equal(ui.calls.length, 0); assert.deepEqual(ui.refreshes, []);
});

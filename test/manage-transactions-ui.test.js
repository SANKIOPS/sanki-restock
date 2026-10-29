'use strict';
const test = require('node:test'), assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path');
const ui = require('../public/manage-transactions');
const action = { type: 'ledger_edit', id: 'TR-SYNTHETIC', kind: 'transfer', nature: 'SANKI', account: 'Synthetic bank' };
const entry = { id: action.id, kind: action.kind, amount: 100, date: '2098-01-14', source: '', note: 'Original synthetic note' };
const result = { id: entry.id, kind: 'transfer', date: entry.date, amount: entry.amount, nature: action.nature, account: action.account, title: '<img src=x onerror=evil()>', editable: true, action };
const clone = value => JSON.parse(JSON.stringify(value));
class Element {
  constructor(tag) { this.tagName = tag; this.children = []; this.attributes = {}; this.listeners = {}; this.style = {}; this.textContent = ''; this.disabled = false; this.value = ''; }
  appendChild(child) { this.children.push(child); child.parentNode = this; return child; }
  setAttribute(key, value) { this.attributes[key] = value; }
  addEventListener(type, handler) { (this.listeners[type] ||= []).push(handler); }
  dispatch(type, extra = {}) { const event = { ...extra, preventDefault() { this.prevented = true; } }; for (const listener of this.listeners[type] || []) listener(event); return event; }
  remove() { if (this.parentNode) this.parentNode.children = this.parentNode.children.filter(child => child !== this); this.parentNode = null; }
  close() { this.closed = true; this.dispatch('close'); }
  showModal() { this.shown = true; }
  focus() { this.focused = true; }
  replaceChildren(...children) { for (const child of this.children) child.parentNode = null; this.children = []; children.forEach(child => this.appendChild(child)); }
  querySelectorAll(selector) { const tags = selector.split(',').map(value => value.trim()); return this.children.flatMap(child => [child, ...child.querySelectorAll('*')]).filter(child => selector === '*' || tags.includes(child.tagName)); }
}
const flush = () => new Promise(resolve => setImmediate(resolve));
function setup(options = {}) {
  const body = new Element('body'), elements = {}, calls = [], busyEvents = [];
  ['transactionReference', 'findTransaction', 'transactionResults', 'transactionMessage'].forEach(id => { elements[id] = new Element(id === 'transactionReference' ? 'input' : id === 'findTransaction' ? 'button' : 'div'); body.appendChild(elements[id]); });
  const doc = { body, createElement: tag => new Element(tag), getElementById: id => elements[id] };
  const request = async (url, requestOptions) => {
    const call = { url, options: requestOptions, payload: requestOptions?.body && JSON.parse(requestOptions.body) }; calls.push(call);
    if (options.handler) return options.handler(call, calls);
    if (url.startsWith('/api/expenses/transactions?')) return { success: true, entries: clone(options.entries || [result]) };
    if (url.startsWith('/api/expenses/ledger-entry/edit-preview?')) {
      if (options.previewError) return { success: false, error: options.previewError };
      if (options.previewDelay) await options.previewDelay;
      return options.preview || { success: true, editable: true, fingerprint: 'synthetic-source-fingerprint', scope: { nature: action.nature, account: action.account }, entry: clone(entry) };
    }
    if (requestOptions.method === 'PATCH') {
      if (options.applyDelay) await options.applyDelay;
      return options.applyError ? { success: false, error: options.applyError } : { success: true, entry: { ...entry, ...call.payload } };
    }
    throw Error('Unexpected endpoint: ' + url);
  };
  const controller = ui.create(doc, {}, { claimantOnly: false, isAdmin: true, canEditLedgerTransactions: true }, request, value => busyEvents.push(value));
  controller.load();
  const find = text => body.querySelectorAll('*').find(el => el.textContent === text), field = label => body.querySelectorAll('input,textarea').find(el => el.attributes['aria-label'] === label);
  const search = async (query = 'TR-SYNTHETIC') => { elements.transactionReference.value = query; return controller.search(); };
  const dialog = () => body.querySelectorAll('dialog')[0];
  return { body, elements, controller, calls, busyEvents, find, field, search, dialog };
}
async function open(h) { await h.search(); h.find('Review and modify transaction').dispatch('click'); await flush(); }
function fill(h) { h.field('Amount ₹').value = '99.63'; h.field('Amount ₹').dispatch('input'); h.field('Reason for correction (required)').value = 'Verified synthetic source amount'; h.field('Reason for correction (required)').dispatch('input'); }

test('reference, date and money validation reject unsafe or ambiguous input', () => {
  for (const value of ['', '12', '*', '<script>', 'TR\n123']) assert.equal(ui.reference(value), '');
  for (const value of ['832', 'TR-00123', 'EX-00832/PAY-001']) assert.equal(ui.reference(' ' + value + ' '), value);
  for (const value of ['', '0', '-1', '1.001', '1e3', '1,000', 'Infinity']) assert.throws(() => ui.amount(value));
  assert.equal(ui.amount('99.63'), 99.63); assert.equal(ui.amount('0.01'), 0.01);
  assert.equal(ui.dateValid('2098-02-30'), false); assert.equal(ui.dateValid('2096-02-29'), true);
});
test('dedicated links are constructed locally and cannot use returned URLs or extra query parameters', () => {
  assert.equal(ui.actionLink({ type: 'expense', id: 'EX-00001&evil=1', url: 'https://evil.example' }), '/expenses.html?tab=list&focus=EX-00001%26evil%3D1');
  assert.equal(ui.actionLink({ type: 'credit_card', cardId: 'CC-001', sourceId: 'payment:CCP-001' }), '/expenses.html?tab=creditcards&manageCard=CC-001&managePayment=payment%3ACCP-001');
  assert.equal(ui.actionLink({ type: 'refund', id: 'ERF-001', nature: 'SAMAST' }), '/expenses.html?tab=receivables&nature=SAMAST&manageRefund=ERF-001');
  assert.equal(ui.actionLink({ type: 'refund', id: 'ERF-001', nature: 'OTHER' }), '');
  assert.equal(ui.actionLink({ type: 'source_workflow', url: '/api/delete-everything' }), '');
});
test('protected source destinations whitelist exact source tabs, account scopes and valid dates', () => {
  assert.equal(ui.sourceWorkflowLink({ tab: 'cash', nature: 'SANKI', account: 'Bank & special', date: '2098-01-14', url: 'https://evil.example', draft: 'FORGED' }), '/expenses.html?tab=cash&nature=SANKI&account=Bank%20%26%20special&ledgerFrom=2098-01-14&ledgerTo=2098-01-14');
  assert.equal(ui.sourceWorkflowLink({ tab: 'cash', nature: 'PERSONAL', account: 'Synthetic' }), '/expenses.html?tab=cash&nature=PERSONAL&account=Synthetic');
  assert.equal(ui.sourceWorkflowLink({ tab: 'creditcards', cardId: 'CC-001', sourceId: 'payment:CCP-001' }), '/expenses.html?tab=creditcards&manageCard=CC-001&managePayment=payment%3ACCP-001');
  assert.equal(ui.sourceWorkflowLink({ tab: 'creditcards', cardId: 'CC-001' }), '/expenses.html?tab=creditcards&manageCard=CC-001');
  assert.equal(ui.sourceWorkflowLink({ tab: 'list', focus: 'EX-00001' }), '/expenses.html?tab=list&focus=EX-00001');
  assert.equal(ui.sourceWorkflowLink({ tab: 'refunds', nature: 'SAMAST', refundId: 'RF-00001' }), '/expenses.html?tab=receivables&nature=SAMAST&manageRefund=RF-00001');
  assert.equal(ui.sourceWorkflowLink({ tab: 'receivables', nature: 'SANKI', id: 'RF-00002' }), '/expenses.html?tab=receivables&nature=SANKI&manageRefund=RF-00002');
  for (const destination of [{ tab: 'cash', nature: 'OTHER', account: 'Synthetic' }, { tab: 'cash', nature: 'SANKI', account: 'Synthetic', date: '2098-02-30' }, { tab: 'delete', id: '1', url: 'https://evil.example' }, { tab: 'creditcards', cardId: 'CC-1', sourceId: 'x\n' }, { tab: 'receivables', nature: 'OTHER', id: 'RF-00002' }]) assert.equal(ui.sourceWorkflowLink(destination), '');
});
test('changed-field selection excludes identity, accounts and all unsupported inputs', () => {
  assert.deepEqual(ui.changesFor(entry, { ...entry, amount: '99.63', account: 'Forged', id: 'Forged', nature: 'PERSONAL', source: 'Ignored for transfer' }), { amount: 99.63 });
  assert.deepEqual(ui.changesFor(entry, { ...entry, amount: '100.00' }), {});
  assert.throws(() => ui.changesFor({ ...entry, kind: 'receipt', source: 'Vendor' }, { ...entry, source: '' }), /receipt source/);
});
test('opening the Manage tool and typing do not perform requests; search requires explicit valid submit', async () => {
  const h = setup(); await h.controller.load(); assert.equal(h.calls.length, 0);
  h.elements.transactionReference.value = 'TR-SYNTHETIC'; h.elements.transactionReference.dispatch('input'); assert.equal(h.calls.length, 0);
  h.elements.transactionReference.value = '12'; h.elements.findTransaction.dispatch('click'); await flush(); assert.equal(h.calls.length, 0);
  h.elements.transactionReference.value = '832'; h.elements.transactionReference.dispatch('keydown', { key: 'Enter' }); await flush(); assert.equal(h.calls.length, 1); assert.match(h.calls[0].url, /reference=832$/); assert.equal(h.calls[0].options.cache, 'no-store');
});
test('search results are text-only summaries with explicit edit buttons', async () => {
  const h = setup(); await h.search(); assert.ok(h.find(result.title)); assert.ok(h.find('Review and modify transaction')); assert.equal(h.body.querySelectorAll('img').length, 0); assert.equal(h.dialog(), undefined); assert.equal(h.calls.length, 1);
});
test('blocked entries expose the reason without an independent edit, retaining approved dedicated routes', async () => {
  const h = setup({ entries: [{ ...result, blockedReason: 'Pending Owner approval' }, { ...result, id: 'TR-CARD', blockedReason: 'Use card workflow', action: { type: 'credit_card', cardId: 'CC-001', sourceId: 'payment:CCP-001' } }] });
  await h.search(); assert.ok(h.find('Protected: Pending Owner approval')); assert.equal(h.find('Review and modify transaction'), undefined); assert.ok(h.find('Open audited card-payment correction')); assert.equal(h.calls.length, 1);
});
test('entries require explicit editable eligibility and source workflow destinations are not invented', async () => {
  const h = setup({ entries: [{ ...result, editable: false }, { ...result, id: 'TR-SOURCE', editable: false, action: { type: 'source_workflow', reason: 'Linked evidence requires source review', destination: { tab: 'cash', account: 'Synthetic' } } }] });
  await h.search(); assert.equal(h.find('Review and modify transaction'), undefined); assert.ok(h.find('Protected: Linked evidence requires source review')); assert.equal(h.body.querySelectorAll('a').length, 0);
});
test('protected entries link only to the verified whitelisted source destination without an edit button', async () => {
  const h = setup({ entries: [{ ...result, editable: false, blockedReason: 'Finalized source is locked', action: { type: 'source_workflow', destination: { tab: 'cash', nature: 'SANKI', account: 'Synthetic bank', date: '2098-01-14' } } }] });
  await h.search(); assert.equal(h.find('Review and modify transaction'), undefined); const link = h.find('Open protected source workflow'); assert.equal(link.className, 'button'); assert.equal(link.href, '/expenses.html?tab=cash&nature=SANKI&account=Synthetic%20bank&ledgerFrom=2098-01-14&ledgerTo=2098-01-14'); assert.equal(h.calls.length, 1);
});
test('truncated searches explicitly ask for a fuller reference instead of presenting partial results as complete', async () => {
  const h = setup({ handler: () => ({ success: true, entries: [result], truncated: true, message: '<script>not trusted</script>' }) }); await h.search(); assert.match(h.elements.transactionMessage.textContent, /Showing the first 1/); assert.match(h.elements.transactionMessage.textContent, /More matches are available\. Enter the complete reference/); assert.doesNotMatch(h.elements.transactionMessage.textContent, /script/); assert.equal(h.calls.length, 1);
});
test('empty and failed search responses never expose guessed or prior records', async () => {
  const h = setup({ entries: [] }); await h.search(); assert.match(h.elements.transactionMessage.textContent, /No accessible/); assert.equal(h.elements.transactionResults.children.length, 0);
  const denied = setup({ handler: () => ({ success: false, error: 'Your permission does not allow this source' }) }); await denied.search(); assert.equal(denied.elements.transactionMessage.textContent, 'Your permission does not allow this source'); assert.equal(denied.elements.transactionResults.children.length, 0);
});
test('a late search response cannot replace the results for a newer reference', async () => {
  let finishFirst; const first = new Promise(resolve => { finishFirst = resolve; });
  const h = setup({ handler: call => call.url.includes('FIRST') ? first : { success: true, entries: [{ ...result, id: 'TR-SECOND' }] } });
  const oldSearch = h.search('TR-FIRST'); await flush(); await h.search('TR-SECOND'); finishFirst({ success: true, entries: [{ ...result, id: 'TR-FIRST' }] }); await oldSearch;
  assert.ok(h.find('TR-SECOND · transfer')); assert.equal(h.find('TR-FIRST · transfer'), undefined);
});
test('typing after a search invalidates a pending response without running a new request', async () => {
  let finish; const pending = new Promise(resolve => { finish = resolve; }), h = setup({ handler: () => pending });
  const searching = h.search(); await flush(); h.elements.transactionReference.value = 'NEW'; h.elements.transactionReference.dispatch('input'); finish({ success: true, entries: [result] }); await searching;
  assert.equal(h.elements.transactionResults.children.length, 0); assert.equal(h.calls.length, 1); assert.equal(h.elements.findTransaction.disabled, false);
});
test('editing requires current read-only preview, explicit before/after review and a separate save', async () => {
  const h = setup(); await open(h); assert.equal(h.dialog().shown, true); assert.equal(h.calls.length, 2); assert.match(h.calls[1].url, /^\/api\/expenses\/ledger-entry\/edit-preview\?/); assert.equal(h.find('Save reviewed correction'), undefined);
  fill(h); h.find('Review changes — no records changed').dispatch('click'); assert.ok(h.find('100')); assert.ok(h.find('99.63')); assert.equal(h.calls.length, 2);
  h.find('Save reviewed correction').dispatch('click'); await flush(); assert.equal(h.calls.length, 3); const save = h.calls[2];
  assert.equal(save.options.method, 'PATCH'); assert.equal(save.url, '/api/expenses/ledger-entry'); assert.deepEqual(save.payload, { id: action.id, kind: action.kind, nature: action.nature, account: action.account, amount: 99.63, fingerprint: 'synthetic-source-fingerprint', reason: 'Verified synthetic source amount' });
  assert.ok(h.find('Correction saved with its audit trail. No reconciliation was opened or finalized.'));
  h.find('Close without saving').dispatch('click'); await flush(); assert.equal(h.dialog(), undefined); assert.equal(h.calls.length, 4); assert.match(h.calls[3].url, /^\/api\/expenses\/transactions\?/);
});
test('changing any field after review invalidates Save; no stale write is possible', async () => {
  const h = setup(); await open(h); fill(h); h.find('Review changes — no records changed').dispatch('click'); assert.ok(h.find('Save reviewed correction'));
  h.field('Transaction date').value = '2098-01-15'; h.field('Transaction date').dispatch('change'); assert.equal(h.find('Save reviewed correction'), undefined); assert.equal(h.calls.length, 2);
});
test('no changes, missing reason, invalid amount and impossible dates prevent review/save', async () => {
  const h = setup(); await open(h); h.find('Review changes — no records changed').dispatch('click'); assert.equal(h.find('Save reviewed correction'), undefined);
  h.field('Amount ₹').value = '99.63'; h.find('Review changes — no records changed').dispatch('click'); assert.equal(h.find('Save reviewed correction'), undefined);
  fill(h); h.field('Amount ₹').value = '99.631'; h.find('Review changes — no records changed').dispatch('click'); assert.equal(h.find('Save reviewed correction'), undefined);
  fill(h); h.field('Transaction date').value = '2098-02-30'; h.find('Review changes — no records changed').dispatch('click'); assert.equal(h.find('Save reviewed correction'), undefined); assert.equal(h.calls.length, 2);
});
test('receipt source is editable only for receipts and only changed fields are included', async () => {
  const receiptAction = { ...action, id: 'RC-SYNTHETIC', kind: 'receipt' }, receipt = { ...entry, id: receiptAction.id, kind: 'receipt', source: 'Original vendor' };
  const h = setup({ entries: [{ ...result, id: receipt.id, action: receiptAction }], preview: { success: true, editable: true, fingerprint: 'synthetic', entry: receipt, scope: { nature: action.nature, account: action.account } } });
  await open(h); h.field('Receipt source').value = 'Corrected vendor'; h.field('Reason for correction (required)').value = 'Corrected synthetic spelling'; h.find('Review changes — no records changed').dispatch('click'); h.find('Save reviewed correction').dispatch('click'); await flush();
  assert.equal(h.calls[2].payload.source, 'Corrected vendor'); assert.equal(Object.hasOwn(h.calls[2].payload, 'amount'), false);
  const transfer = setup(); await open(transfer); assert.equal(transfer.field('Receipt source'), undefined);
});
test('permission/lock preview denial has no editable controls and no write fallback', async () => {
  const h = setup({ previewError: 'This transaction touches a finalized bank period.' }); await open(h); assert.ok(h.find('This transaction touches a finalized bank period.')); assert.equal(h.field('Amount ₹'), undefined); assert.equal(h.find('Save reviewed correction'), undefined); h.find('Close without saving').dispatch('click'); assert.equal(h.calls.length, 2);
});
test('mismatched identity, scope or missing fingerprint fail closed before rendering inputs', async () => {
  const good = { success: true, editable: true, fingerprint: 'synthetic', scope: { nature: action.nature, account: action.account }, entry };
  for (const preview of [{ ...good, entry: { ...entry, id: 'OTHER' } }, { ...good, scope: { ...good.scope, account: 'Other bank' } }, { ...good, fingerprint: '' }, { ...good, editable: false }]) {
    const h = setup({ preview }); await open(h); assert.equal(h.field('Amount ₹'), undefined); assert.equal(h.calls.length, 2); assert.match(h.body.querySelectorAll('p').map(el => el.textContent).join(' '), /identity or accounting scope changed/);
  }
});
test('closing an unsaved reviewed edit makes no write and no reconciliation operation', async () => {
  const h = setup(); await open(h); fill(h); h.find('Review changes — no records changed').dispatch('click'); h.find('Close without saving').dispatch('click'); assert.equal(h.dialog(), undefined); assert.equal(h.calls.length, 2); assert.deepEqual(h.busyEvents, [true, false]);
});
test('a denied/stale save removes all editable state and requires a fresh search', async () => {
  const h = setup({ applyError: 'Source fingerprint changed; reload before editing.' }); await open(h); fill(h); h.find('Review changes — no records changed').dispatch('click'); h.find('Save reviewed correction').dispatch('click'); await flush();
  assert.equal(h.find('Save reviewed correction'), undefined); assert.equal(h.field('Amount ₹'), undefined); assert.match(h.body.querySelectorAll('p').map(el => el.textContent).join(' '), /Source fingerprint changed/); assert.equal(h.calls.length, 3);
});
test('double save and close/Escape during a pending write cannot duplicate or interrupt the request', async () => {
  let finish; const delay = new Promise(resolve => { finish = resolve; }), h = setup({ applyDelay: delay }); await open(h); fill(h); h.find('Review changes — no records changed').dispatch('click');
  const save = h.find('Save reviewed correction'); save.dispatch('click'); save.dispatch('click'); await flush(); assert.equal(h.calls.filter(call => call.options.method === 'PATCH').length, 1); assert.equal(h.dialog().dispatch('cancel').prevented, true); h.find('Close without saving').dispatch('click'); assert.notEqual(h.dialog().closed, true); finish(); await flush(); assert.ok(h.find('Correction saved with its audit trail. No reconciliation was opened or finalized.'));
});
test('preview loading blocks duplicate editors and closing until the read completes', async () => {
  let finish; const delay = new Promise(resolve => { finish = resolve; }), h = setup({ previewDelay: delay }); await h.search(); h.find('Review and modify transaction').dispatch('click'); h.find('Review and modify transaction').dispatch('click'); await flush(); assert.equal(h.calls.length, 2); assert.equal(h.dialog().dispatch('cancel').prevented, true); finish(); await flush(); assert.ok(h.field('Amount ₹'));
});
test('Manage transaction module never injects result HTML or adds alternative financial mutation endpoints', () => {
  const script = fs.readFileSync(path.join(__dirname, '../public/manage-transactions.js'), 'utf8'); assert.doesNotMatch(script, /\.innerHTML\s*=|insertAdjacentHTML|eval\(/); assert.doesNotMatch(script, /method:\s*['"](?:POST|DELETE)['"]/); assert.match(script, /Object\.freeze\(\{ id: action\.id/); assert.match(script, /fingerprint: snapshot\.fingerprint/);
});

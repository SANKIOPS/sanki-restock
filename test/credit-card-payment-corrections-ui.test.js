'use strict';
const test = require('node:test'), assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path');
const ui = require('../public/credit-card-payment-corrections');
const clone = value => JSON.parse(JSON.stringify(value));
const source = { id: 'payment:CCP-SYNTHETIC', amount: 100, date: '2098-04-17', reference: 'SYNTHETIC-REF', note: 'Original note', nature: 'SANKI', account: 'Synthetic bank', allocations: [{ nature: 'SANKI', amount: 100 }], editable: true };
class Element {
  constructor(tag) { this.tagName = tag; this.children = []; this.attributes = {}; this.listeners = {}; this.style = {}; this.textContent = ''; this.disabled = false; this.value = ''; }
  appendChild(child) { this.children.push(child); child.parentNode = this; return child; }
  setAttribute(key, value) { this.attributes[key] = value; }
  addEventListener(type, handler) { (this.listeners[type] ||= []).push(handler); }
  dispatch(type) { const event = { preventDefault() { this.prevented = true; } }; for (const listener of this.listeners[type] || []) listener(event); return event; }
  remove() { if (this.parentNode) this.parentNode.children = this.parentNode.children.filter(child => child !== this); this.parentNode = null; }
  close() { this.closed = true; this.dispatch('close'); }
  showModal() { this.shown = true; }
  replaceChildren(...children) { for (const child of this.children) child.parentNode = null; this.children = []; children.forEach(child => this.appendChild(child)); }
  querySelectorAll(selector) { const tags = selector.split(',').map(value => value.trim()); return this.children.flatMap(child => [child, ...child.querySelectorAll('*')]).filter(child => selector === '*' || tags.includes(child.tagName)); }
}
const flush = () => new Promise(resolve => setImmediate(resolve));
function setup(options = {}) {
  const body = new Element('body'), document = { body, createElement: tag => new Element(tag) }, calls = [], events = [];
  const api = async (url, request) => {
    const payload = request?.body && JSON.parse(request.body); calls.push({ url, request, payload });
    if (!url.endsWith('/preview') && !url.endsWith('/apply')) return { success: true, card: { name: 'Synthetic <img> Card' }, entries: options.entries || [clone(source)] };
    if (url.endsWith('/preview')) {
      if (options.previewError) return { success: false, error: options.previewError };
      if (options.delay) await options.delay;
      const payment = { ...source, id: 'CCP-SYNTHETIC' }, transfer = { ...source, id: 'TR-SYNTHETIC' }, afterPayment = { ...payment, ...payload.changes }, afterTransfer = { ...transfer, ...payload.changes };
      return { success: true, fingerprint: 'synthetic-fingerprint', changes: payload.changes, reason: payload.reason, before: { payment, transfer, allocations: [] }, after: { payment: afterPayment, transfer: afterTransfer, allocations: [] } };
    }
    if (options.applyError) return { success: false, error: options.applyError };
    return { success: true, audit: { id: 'AUD-SYNTHETIC' } };
  };
  const dialog = ui.open({ document, api, cardId: 'CC-SYNTHETIC', sourceId: options.sourceId, onApplied: event => events.push(event) });
  const find = text => dialog.querySelectorAll('*').find(el => el.textContent === text), field = label => dialog.querySelectorAll('input,textarea').find(el => el.attributes['aria-label'] === label);
  return { dialog, body, calls, events, find, field };
}
function fill(h) { h.field('Amount ₹').value = '99.63'; h.field('Amount ₹').dispatch('input'); h.field('Correction reason and source evidence (required)').value = 'Verified the synthetic source amount'; h.field('Correction reason and source evidence (required)').dispatch('input'); }

test('Manage exact card-payment link opens only its permitted editor without writing', async () => {
  const h=setup({sourceId:source.id});await flush();assert.ok(h.field('Amount ₹'));assert.equal(h.calls.length,1);assert.equal(h.find('Apply this audited correction'),undefined);
});
test('Manage missing, duplicate or locked card-payment targets never fall back to another payment', async () => {
  for(const options of [{sourceId:'payment:CCP-MISSING'},{sourceId:source.id,entries:[clone(source),clone(source)]},{sourceId:source.id,entries:[{id:source.id,editable:false,blockedReason:'Pending Owner approval'}]}]){
    const h=setup(options);await flush();assert.equal(h.field('Amount ₹'),undefined);assert.equal(h.find('Review correction'),undefined);assert.equal(h.calls.length,1);
  }
});
test('change selection emits only editable changed fields, never identity or account data', () => { assert.deepEqual(ui.changesFor(source, { ...source, amount: '99.63', account: 'Forged', cardId: 'Forged' }), { amount: '99.63' }); assert.deepEqual(ui.changesFor(source, { ...source, amount: '100' }), {}); });
test('list is read-only and correction requires preview followed by a separate explicit Apply', async () => {
  const h = setup(); await flush(); assert.equal(h.dialog.shown, true); assert.equal(h.calls.length, 1); assert.equal(h.calls[0].request.cache, 'no-store'); assert.equal(h.find('Apply this audited correction'), undefined);
  h.find('Review correction').dispatch('click'); fill(h); h.find('Preview correction — no records changed').dispatch('click'); await flush(); assert.equal(h.calls.length, 2); assert.ok(h.calls[1].url.endsWith('/preview')); assert.equal(h.events.length, 0); assert.ok(h.find('Apply this audited correction'));
  h.find('Apply this audited correction').dispatch('click'); await flush(); assert.equal(h.calls.length, 3); assert.ok(h.calls[2].url.endsWith('/apply')); assert.equal(h.calls[2].payload.fingerprint, 'synthetic-fingerprint'); assert.deepEqual(h.calls[2].payload.changes, { amount: '99.63' }); assert.equal(h.events.length, 1); assert.equal(h.find('Apply this audited correction'), undefined);
});
test('editing any field after preview discards the stale Apply action', async () => { const h = setup(); await flush(); h.find('Review correction').dispatch('click'); fill(h); h.find('Preview correction — no records changed').dispatch('click'); await flush(); assert.ok(h.find('Apply this audited correction')); h.field('Reference').value = 'NEW'; h.field('Reference').dispatch('input'); assert.equal(h.find('Apply this audited correction'), undefined); assert.equal(h.calls.filter(call => call.url.endsWith('/apply')).length, 0); });
test('locked payments show server reason and never offer a correction form', async () => { const h = setup({ entries: [{ id: source.id, editable: false, blockedReason: 'Awaiting Owner approval' }] }); await flush(); assert.ok(h.find('Locked: Awaiting Owner approval')); assert.equal(h.find('Review correction'), undefined); assert.equal(h.calls.length, 1); });
test('closing without applying after preview never changes a record', async () => { const h = setup(); await flush(); h.find('Review correction').dispatch('click'); fill(h); h.find('Preview correction — no records changed').dispatch('click'); await flush(); h.find('Close without applying').dispatch('click'); assert.equal(h.dialog.closed, true); assert.equal(h.body.children.length, 0); assert.equal(h.calls.filter(call => call.url.endsWith('/apply')).length, 0); });
test('preview errors are displayed with no Apply action', async () => { const h = setup({ previewError: 'Reconciliation lock: no changes allowed' }); await flush(); h.find('Review correction').dispatch('click'); fill(h); h.find('Preview correction — no records changed').dispatch('click'); await flush(); assert.ok(h.find('Reconciliation lock: no changes allowed')); assert.equal(h.find('Apply this audited correction'), undefined); assert.equal(h.events.length, 0); });
test('failed stale apply clears the preview and requires a fresh review', async () => { const h = setup({ applyError: 'Accounting evidence changed; preview again' }); await flush(); h.find('Review correction').dispatch('click'); fill(h); h.find('Preview correction — no records changed').dispatch('click'); await flush(); h.find('Apply this audited correction').dispatch('click'); await flush(); assert.ok(h.find('Accounting evidence changed; preview again')); assert.equal(h.find('Apply this audited correction'), undefined); assert.equal(h.events.length, 0); });
test('preview cannot be double-submitted and close/Escape cannot interrupt an in-flight correction', async () => {
  let finish; const delay = new Promise(resolve => { finish = resolve; }), h = setup({ delay }); await flush(); h.find('Review correction').dispatch('click'); fill(h); const button = h.find('Preview correction — no records changed'); button.dispatch('click'); button.dispatch('click'); assert.equal(h.calls.filter(call => call.url.endsWith('/preview')).length, 1); assert.equal(h.dialog.dispatch('cancel').prevented, true); h.find('Close without applying').dispatch('click'); assert.notEqual(h.dialog.closed, true); finish(); await flush(); assert.ok(h.find('Apply this audited correction'));
});
test('required reason and at least one change are checked before preview requests', async () => { const h = setup(); await flush(); h.find('Review correction').dispatch('click'); h.find('Preview correction — no records changed').dispatch('click'); assert.equal(h.calls.length, 1); h.field('Amount ₹').value = '99'; h.find('Preview correction — no records changed').dispatch('click'); assert.equal(h.calls.length, 1); });
test('card integration uses existing Manage capability, unsaved-review gate and safe DOM text', () => {
  const html = fs.readFileSync(path.join(__dirname, '../public/expenses.html'), 'utf8'), script = fs.readFileSync(path.join(__dirname, '../public/credit-card-payment-corrections.js'), 'utf8');
  assert.match(html, /credit-card-payment-corrections\.js\?v=/); assert.match(html, /function attachCardCorrectionButtons\(\)\{if\(!cfg\.canEditLedgerTransactions\)return;/); assert.match(html, /dataset\.reconciliationDirty==='true'[\s\S]*?CreditCardPaymentCorrections\.open/); assert.doesNotMatch(script, /\.innerHTML\s*=/); assert.doesNotMatch(script, /\.insertAdjacentHTML\(/);
});

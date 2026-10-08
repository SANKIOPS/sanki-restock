'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { buildReport } = require('../modules/pnl-report');
const source = fs.readFileSync(path.join(__dirname, '..', 'public', 'pnl.js'), 'utf8');
function sample(missing = false) {
  return buildReport({ orders: { orders: { O: { id: 'O', name: '<unsafe invoice>', channel: 'POS', financialStatus: 'paid', taxesIncluded: true, total: 2999, processedAt: '2026-10-01', lineItems: [{ id: 'L', sku: 'SKU', qty: 1, price: 2999 }] } } }, opening: missing ? {} : { lots: [{ sku: 'SKU', qty: 10, unitCost: 999, verified: true }] } }, { from: '2026-10-01', to: '2026-10-08' });
}
function harness(fetcher) {
  const elements = new Map(), listeners = {}, tabs = ['statement', 'tax', 'collections', 'trends', 'policy'].map(id => ({ dataset: { tab: id }, listeners: {}, setAttribute() {}, addEventListener(name, fn) { this.listeners[name] = fn; } }));
  function element(id) {
    if (!elements.has(id)) elements.set(id, { id, value: '', innerHTML: '', textContent: '', hidden: false, disabled: false, open: false, listeners: {}, addEventListener(name, fn) { this.listeners[name] = fn; }, showModal() { this.open = true; }, close() { this.open = false; } });
    return elements.get(id);
  }
  element('channel').value = 'All';
  const document = { getElementById: element, body: { classList: { add() {}, remove() {} } }, querySelectorAll(selector) { return selector === '[data-tab]' ? tabs : selector === '.view' ? tabs.map(t => element(t.dataset.tab)) : []; }, addEventListener(name, fn) { listeners[name] = fn; } };
  vm.runInNewContext(source, { document, fetch: fetcher, AbortController, URLSearchParams, Date, location: {}, window: { print() {} } });
  return { element, tabs, clickFilter(filter) { listeners.click({ target: { closest: () => ({ dataset: { filter } }) } }); }, refresh() { element('filters').listeners.submit({ preventDefault() {} }); } };
}
const settle = () => new Promise(resolve => setImmediate(resolve));
test('P&L UI renders typed totals, escaped references, tabs and SKU drill-down', async () => {
  const ui = harness(async () => ({ ok: true, status: 200, json: async () => sample() })); await settle();
  assert.match(ui.element('cards').innerHTML, /1,542\.53/); assert.equal(ui.element('export').disabled, false);
  ui.tabs[1].listeners.click(); assert.equal(ui.element('statement').hidden, true); assert.equal(ui.element('tax').hidden, false);
  assert.match(ui.element('tax').innerHTML, /&lt;unsafe invoice&gt;/); assert.doesNotMatch(ui.element('tax').innerHTML, /<unsafe invoice>/);
  ui.clickFilter('sales'); assert.equal(ui.element('details').open, true); assert.match(ui.element('detailBody').innerHTML, /SKU \/ purchase allocations/);
});
test('incomplete results display unavailable instead of fabricated profit', async () => {
  const ui = harness(async () => ({ ok: true, status: 200, json: async () => sample(true) })); await settle();
  assert.match(ui.element('cards').innerHTML, /Unavailable/); assert.equal(ui.element('warnings').hidden, false);
  assert.match(ui.element('warnings').innerHTML, /Verified stock\/cost missing/);
});
test('a failed refresh clears stale results and disables exports', async () => {
  let fail = false;
  const ui = harness(async () => fail ? { ok: false, status: 500, json: async () => ({ error: 'Test failure' }) } : { ok: true, status: 200, json: async () => sample() }); await settle();
  fail = true; ui.refresh(); await settle();
  assert.equal(ui.element('cards').innerHTML, ''); assert.equal(ui.element('statement').innerHTML, ''); assert.equal(ui.element('export').disabled, true); assert.equal(ui.element('status').textContent, 'Test failure');
});
test('an older response cannot replace a newer selected report', async () => {
  const pending = [];
  const ui = harness(() => new Promise(resolve => pending.push(resolve)));
  ui.refresh();
  pending[1]({ ok: true, status: 200, json: async () => sample(true) }); await settle();
  pending[0]({ ok: true, status: 200, json: async () => sample() }); await settle();
  assert.match(ui.element('cards').innerHTML, /Unavailable/); assert.equal(ui.element('warnings').hidden, false);
});

test('shared Till date control uses accounting start rather than silently defaulting to this month', async () => {
  let requested;
  const ui = harness(async url => { requested = url; return { ok: true, status: 200, json: async () => sample() }; }); await settle();
  ui.element('from').disabled = true; ui.element('from').value = ''; ui.refresh(); await settle();
  assert.equal(new URL(requested, 'http://localhost').searchParams.get('from'), '2026-08-22');
});

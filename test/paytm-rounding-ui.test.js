'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const html = fs.readFileSync(path.join(__dirname, '../public/expenses.html'), 'utf8');
const start = html.indexOf("      host.querySelectorAll('[data-pt-rounding]')");
const end = html.indexOf("      host.querySelectorAll('[data-pt-sync]')", start);
const handler = html.slice(start, end);
function harness(reason, result) {
  const error = { textContent: '' }, input = { value: reason }, message = { textContent: '' }, calls = [];
  const button = { disabled: false, getAttribute: name => name === 'data-pt-rounding' ? 'SYNTHETIC-TX' : '0.01' };
  const host = { querySelectorAll: () => [button], querySelector: selector => selector.includes('rounding-reason') ? input : error };
  let refreshed = 0, rendered = 0;
  vm.runInNewContext(handler, { host, el: id => id === 'ptMessage' ? message : {}, fmt: value => '₹' + value,
    api: async (...args) => { calls.push(args); if (result instanceof Error) throw result; return result; },
    showPaytmSaved: () => { rendered++; }, loadAccountLedger: () => { refreshed++; } });
  return { button, error, message, calls, refreshed: () => refreshed, rendered: () => rendered };
}
test('all expenses inline scripts compile after adding rounding controls', () => {
  for (const [, script] of html.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/g)) if (script.trim()) assert.doesNotThrow(() => new Function(script));
  assert.match(html, /Rounding reason for Paytm/);
  assert.match(html, /Recorded Rounding Off:/);
});
test('short rounding reason makes no accounting request', async () => {
  const ui = harness('short', {}); await ui.button.onclick();
  assert.equal(ui.calls.length, 0); assert.equal(ui.button.disabled, false); assert.match(ui.error.textContent, /10 characters/);
});
test('rounding action sends only one explicit transaction and never finalizes or posts a payout', async () => {
  const ui = harness('Confirmed receipt rounding', { success: true, review: { difference: .01, orderNumber: '2824' }, view: {} });
  await ui.button.onclick(); assert.equal(ui.calls.length, 1);
  assert.equal(ui.calls[0][0], '/api/expenses/paytm-reports/rounding-review');
  assert.deepEqual(JSON.parse(ui.calls[0][1].body), { transactionId: 'SYNTHETIC-TX', difference: .01, reason: 'Confirmed receipt rounding' });
  assert.match(ui.message.textContent, /no settlement was finalized or bank-linked/);
  assert.equal(ui.rendered(), 1); assert.equal(ui.refreshed(), 1);
});
test('failed or stale rounding save is visible and permits a safe retry', async () => {
  for (const result of [{ success: false, error: 'The rounding difference changed.' }, new Error('Network unavailable')]) {
    const ui = harness('Confirmed receipt rounding', result); await ui.button.onclick();
    assert.equal(ui.button.disabled, false); assert.ok(ui.error.textContent); assert.equal(ui.rendered(), 0); assert.equal(ui.refreshed(), 0);
  }
});

'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const html = fs.readFileSync(path.join(__dirname, '../public/expenses.html'), 'utf8');
const esc = value => String(value == null ? '' : value).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
const money = value => '₹' + Number(value).toFixed(2);
function section(from, to) { const start = html.indexOf(from), end = html.indexOf(to, start); assert.ok(start >= 0 && end > start, from); return html.slice(start, end); }
const flush = () => new Promise(resolve => setImmediate(resolve));
function plan(overrides = {}) { return { eligible: true, mode: 'manual', originalAmount: 1000, bankAmount: 999, fingerprint: 'verified-universal-plan', sourceType: 'credit_card_payment', sourceLabel: 'Credit-card payment', effect: 'linked_sources', sourceEffects: 'The bank transfer and authorized credit-card payment ownership rows change together.', linkedCount: 3, ...overrides }; }
function harness(options = {}) {
  const nodes = {}, calls = [], confirms = [], rendered = [], previews = (options.previews || [{ success: true, bankAmountAuthority: plan(options.plan), bankDate: '2099-10-19' }]).slice();
  function node(id) {
    if (!nodes[id]) {
      const value = { value: '', textContent: '', disabled: false, open: false, style: {}, inputs: [], getAttribute(name) { return this[name]; }, querySelectorAll() { return this.inputs; }, showModal() { this.open = true; }, close() { this.open = false; if (this.onclose) this.onclose(); } };
      Object.defineProperty(value, 'innerHTML', { get() { return this.content || ''; }, set(content) { this.content = content; this.inputs = [...content.matchAll(/<input\b[^>]*data-bank-authority-nature="([^"]+)"[^>]*value="([^"]*)"[^>]*>/g)].map(match => ({ value: match[2], getAttribute: name => name === 'data-bank-authority-nature' ? match[1] : null })); } });
      nodes[id] = value;
    }
    return nodes[id];
  }
  const row = { id: 'bank-1', status: 'amount_mismatch', bank: { debit: 999, date: '2099-10-19' }, app: { id: 'SOURCE-1', debit: 1000 }, bankAmountAuthority: plan(options.plan) };
  const context = vm.createContext({ cfg: { isOwner: options.owner === true }, bankNature: 'SANKI', bankDraftId: 'TEST-DRAFT', bankReconView: { rows: [row], linkCandidates: [] }, el: node, esc, fmt: money, bankReconRow: id => id === row.id ? row : null, Promise,
    api(url, request) { calls.push({ url, body: JSON.parse(request.body) }); return Promise.resolve(url.endsWith('amount-authority-preview') ? previews.shift() : { success: true }); },
    confirm(message) { confirms.push(message); return options.confirm !== false; }, renderBankReconciliation(view) { rendered.push(view); }, showBankReconForm() {} });
  context.window = context;
  vm.runInContext(section('    function bankAmountAuthorityText(', '    function renderBankReconciliation('), context);
  vm.runInContext(section('    window.resolveBankRow=function', '    window.resolveBankBalance='), context);
  return { context, nodes, calls, confirms, rendered, node, row };
}
function allocationRequired() { return { success: false, allocationRequired: true, allocationChoices: [{ nature: 'SAMAST', amount: 600 }, { nature: 'PERSONAL', amount: 400 }], bankAmount: 999, bankDate: '2099-10-19', sourceLabel: 'Credit-card payment' }; }

test('source-specific linked-source confirmation does not invent an expense correction for credit-card payments', async () => {
  const ui = harness(); ui.context.resolveBankRow('bank-1', 'accept_match'); await flush();
  assert.match(ui.confirms[0], /Credit-card payment: ₹1000.00 → ₹999.00/);
  assert.match(ui.confirms[0], /authorized credit-card payment ownership rows change together/);
  assert.match(ui.confirms[0], /Linked records reviewed: 3/);
  assert.doesNotMatch(ui.confirms[0], /Expense:/);
  assert.match(ui.nodes.bs_msg.textContent, /Only the reviewed source and linked effects will change/);
  assert.doesNotMatch(ui.nodes.bs_msg.textContent, /Both payment and supported expense/);
  assert.equal(ui.calls.at(-1).body.confirmBankAmount, true);
});

test('bank-cash-only review preserves invoice/order/earned-pay/report meaning and does not expose unused private source fields', async () => {
  const ui = harness({ plan: { sourceType: 'bank_movement', sourceLabel: 'Salary bank movement', effect: 'bank_cash_only', sourceEffects: 'Actual outgoing bank movement corrected.', linkedCount: 0, privateInvoice: 'PRIVATE-INVOICE-NAME', expenseAmountBefore: 1000, expenseAmountAfter: 999 } });
  const note = ui.context.bankAmountAuthorityHtml(ui.row);
  assert.match(note, /Only the actual bank cash movement is corrected/);
  assert.match(note, /underlying invoices, orders, earned pay and source reports remain unchanged/);
  assert.doesNotMatch(note, /PRIVATE-INVOICE-NAME|Expense ₹/);
  ui.context.resolveBankRow('bank-1', 'accept_match'); await flush();
  assert.match(ui.confirms[0], /Salary bank movement: ₹1000.00 → ₹999.00/);
  assert.match(ui.confirms[0], /This is not an extra entry/);
  assert.doesNotMatch(ui.confirms[0], /Expense:|PRIVATE-INVOICE-NAME/);
  assert.ok(ui.calls.every(call => !/finalize|salary|orders/.test(call.url)), 'review never calls source mutation or finalization APIs');
});

test('source labels and effect strings are escaped in rendered notes; cancelling generic review creates no decision', async () => {
  const ui = harness({ confirm: false, plan: { sourceLabel: '<img src=x onerror=bad>', sourceEffects: '<script>bad</script>', effect: 'bank_cash_only' } });
  const note = ui.context.bankAmountAuthorityHtml(ui.row);
  assert.doesNotMatch(note, /<img|<script>/);
  assert.match(note, /&lt;img/); assert.match(note, /&lt;script/);
  ui.context.resolveBankRow('bank-1', 'accept_match'); await flush();
  assert.equal(ui.calls.length, 1); assert.equal(ui.rendered.length, 0);
});

test('mixed ownership requires explicit exact allocations, a reason, a fresh preview and final confirmation', async () => {
  const ui = harness({ previews: [allocationRequired(), { success: true, bankAmountAuthority: plan(), bankDate: '2099-10-19' }] });
  ui.context.resolveBankRow('bank-1', 'accept_match'); await flush();
  assert.equal(ui.calls.length, 1);
  assert.equal(ui.node('bankAmountAllocationDlg').open, true);
  const inputs = ui.node('bankAmountAllocationRows').inputs;
  assert.deepEqual(inputs.map(input => input.value), ['600', '400'], 'original ownership is not proportionally rescaled');
  assert.equal(ui.node('bankAmountAllocationGo').disabled, true);
  assert.match(ui.node('bankAmountAllocationRemaining').textContent, /Remaining ₹-1.00/);
  inputs[0].value = '599'; ui.node('bankAmountAllocationRows').oninput();
  assert.equal(ui.node('bankAmountAllocationGo').disabled, false);
  ui.node('bankAmountAllocationGo').onclick();
  assert.match(ui.node('bankAmountAllocationMsg').textContent, /10 characters/);
  assert.equal(ui.calls.length, 1);
  ui.node('bankAmountAllocationReason').value = 'Verified the mixed ownership allocation';
  ui.node('bankAmountAllocationGo').onclick(); await flush();
  const allocation = [{ nature: 'SAMAST', amount: 599 }, { nature: 'PERSONAL', amount: 400 }];
  assert.equal(ui.calls.length, 3, 'initial preview, allocated preview and explicit decision only');
  assert.deepEqual(ui.calls[1].body.amountAuthorityAllocations, allocation);
  assert.deepEqual(ui.calls[2].body.amountAuthorityAllocations, allocation);
  assert.equal(ui.calls[2].body.confirmBankAmount, true);
  assert.match(ui.calls[2].body.reason, /Ownership allocation: Verified the mixed ownership allocation/);
  assert.match(ui.confirms[0], /Ownership allocations: SAMAST ₹599.00 · PERSONAL ₹400.00/);
  assert.match(ui.confirms[0], /posted only at Owner finalization/);
  assert.equal(ui.node('bankAmountAllocationDlg').open, false);
});

test('invalid ownership decimals, negative or incomplete allocations cannot leave the review dialog', async () => {
  const ui = harness({ previews: [allocationRequired()] });
  ui.context.resolveBankRow('bank-1', 'accept_match'); await flush();
  const inputs = ui.node('bankAmountAllocationRows').inputs;
  ui.node('bankAmountAllocationReason').value = 'Verified the mixed ownership allocation';
  for (const amount of ['599.001', '-1', '', 'Infinity']) {
    inputs[0].value = amount; ui.node('bankAmountAllocationRows').oninput();
    ui.node('bankAmountAllocationGo').onclick();
    assert.equal(ui.calls.length, 1);
    assert.equal(ui.node('bankAmountAllocationDlg').open, true);
    assert.match(ui.node('bankAmountAllocationMsg').textContent, /valid allocation amounts/);
  }
  ui.node('bankAmountAllocationCancel').onclick(); await flush();
  assert.equal(ui.calls.length, 1); assert.equal(ui.rendered.length, 0);
});

test('ownership allocation cancellation and final-confirmation cancellation never stage a decision', async () => {
  const cancelled = harness({ previews: [allocationRequired()] });
  cancelled.context.resolveBankRow('bank-1', 'accept_match'); await flush();
  cancelled.node('bankAmountAllocationDlg').oncancel({ preventDefault() {} }); await flush();
  assert.equal(cancelled.calls.length, 1); assert.equal(cancelled.confirms.length, 0);
  const declined = harness({ confirm: false, previews: [allocationRequired(), { success: true, bankAmountAuthority: plan(), bankDate: '2099-10-19' }] });
  declined.context.resolveBankRow('bank-1', 'accept_match'); await flush();
  declined.node('bankAmountAllocationRows').inputs[0].value = '599';
  declined.node('bankAmountAllocationReason').value = 'Verified the mixed ownership allocation';
  declined.node('bankAmountAllocationGo').onclick(); await flush();
  assert.equal(declined.calls.length, 2); assert.equal(declined.rendered.length, 0);
});

test('a private-allocation denial exposes no ownership choices or card data fetches', async () => {
  const ui = harness({ previews: [{ success: false, error: 'Owner review required for private ownership allocation.' }] });
  ui.context.resolveBankRow('bank-1', 'accept_match'); await flush();
  assert.equal(ui.nodes.bankAmountAllocationDlg, undefined);
  assert.match(ui.nodes.bs_msg.textContent, /Owner review required/);
  assert.equal(ui.calls.length, 1);
  assert.equal(ui.calls[0].url, '/api/expenses/bank-statements/amount-authority-preview');
  assert.equal(ui.confirms.length, 0);
});

test('existing exact multiple-bank workflows and known Axis-fee review remain intact and inline scripts compile', () => {
  const ui = harness();
  ui.row.axisTransferCharge = true;
  assert.equal(ui.context.bankKnownAxisChargePair({ rowId: 'bank-1', appId: 'SOURCE-1', bankRowId: 'bank-1' }), true);
  const multi = html.split('\n').find(value => value.includes("bankReconForm.mode==='multi_bank'))return"));
  assert.match(multi, /Math.abs\(target-total\)>\.01/);
  assert.match(multi, /action:'link_multiple_bank_entries'/);
  assert.ok(html.includes('id="bankAmountAllocationDlg"'));
  for (const [, script] of html.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/g)) if (script.trim()) assert.doesNotThrow(() => new vm.Script(script));
});

test('source drift warning retains finalized reconciliation status and never offers a repost', () => {
  const context = vm.createContext({ esc, ReconciliationStatus: { paytmLedgerRow: () => '<b>Paytm bank confirmed</b>' } });
  vm.runInContext(section('    function ledgerReconciliationLabel(', '    function loadCounterCashReconciliation('), context);
  const source = { bankMovementAuthority: { sourceChanged: true, planId: 'PRIVATE-PLAN-SNAPSHOT' }, reconciliation: { bankDate: '2098-06-02', reconciliationId: 'BST-VERIFIED', bankReference: '<original-bank-reference>' } };
  const note = context.ledgerReconciliationLabel(source);
  assert.match(note, /✓ Reconciled/); assert.match(note, /BST-VERIFIED/);
  assert.match(note, /Source changed after reconciliation/);
  assert.match(note, /Verified bank amount\/date retained/);
  assert.match(note, /&lt;original-bank-reference&gt;/);
  assert.doesNotMatch(note, /Not reconciled|<button|onclick|PRIVATE-PLAN-SNAPSHOT/);
  assert.doesNotMatch(context.ledgerReconciliationLabel({ ...source, bankMovementAuthority: { sourceChanged: false } }), /Source changed/);
  assert.match(context.ledgerReconciliationLabel({ ...source, paytmReconciliation: {} }), /Paytm bank confirmed[\s\S]*Source changed/);
});

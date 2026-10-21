'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const html = fs.readFileSync(path.join(__dirname, '../public/expenses.html'), 'utf8');
const esc = value => String(value == null ? '' : value).replace(/[&<>"']/g, char => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[char]));
const decode = value => String(value).replace(/&quot;|&#39;|&lt;|&gt;|&amp;/g, char => ({ '&quot;': '"', '&#39;': "'", '&lt;': '<', '&gt;': '>', '&amp;': '&' }[char]));
function line(marker) { const value = html.split('\n').find(item => item.includes(marker)); assert.ok(value, marker); return value; }
const pickerCode = html.slice(html.indexOf('    var vendorPickers='), html.indexOf('    var telegramLinkedUsers='));
const flush = () => new Promise(resolve => setImmediate(resolve));
function ui(overrides = {}) {
  const nodes = {}, calls = [], documentListeners = {};
  let context;
  function node(id) {
    const classes = new Set();
    const item = { id, value: '', textContent: '', className: '', style: {}, attrs: {}, dataset: {}, listeners: {}, options: [], disabled: false, files: [],
      classList: { add(name) { classes.add(name); }, remove(name) { classes.delete(name); }, contains(name) { return classes.has(name); } },
      setAttribute(key, value) { this.attrs[key] = value; },
      addEventListener(type, fn) { (this.listeners[type] ||= []).push(fn); },
      fire(type, extra = {}) { const event = { target: this, key: '', preventDefault() { this.prevented = true; }, ...extra }; (this.listeners[type] || []).forEach(fn => fn.call(this, event)); return event; },
      focus() { context.document.activeElement = this; this.fire('focus'); },
      querySelectorAll() { return this.options; }, querySelector() { return this.options[0] || null; },
      contains(child) { return child === this || this.options.includes(child); },
      showModal() { this.open = true; }, close() { this.open = false; this.fire('close'); }
    };
    Object.defineProperty(item, 'innerHTML', { get() { return this.html || ''; }, set(value) {
      this.html = String(value);
      if (/Suggestions$/.test(this.id || '')) {
        this.options = [...this.html.matchAll(/data-vendor="([^"]*)"/g)].map(match => { const option = node(); option.dataset.vendor = decode(match[1]); return option; });
      } else if (this.id === 'beNature') {
        this.options = [...this.html.matchAll(/<option\b([^>]*)>([^<]*)/g)].map(match => ({ value: (match[1].match(/value="([^"]*)"/) || [null, match[2]])[1] }));
        if (this.options.length) this.value = this.options[0].value;
      }
    } });
    if (id) nodes[id] = item;
    return item;
  }
  ['beNature', 'beVendor', 'beVendorSuggestions', 'beVendorDropdown', 'bankExpenseDlg', 'editNature', 'editDlg', 'approveDlg', 'beLedger', 'beLedgerList', 'beMsg', 'bankExpenseSummary', 'beAccount', 'beDate', 'beAmount', 'beParticulars', 'beBillPhoto', 'beRemark', 'beSave', 'beCancel', 'beBill', 'bePaymentType', 'bs_msg'].forEach(node);
  context = {
    cfg: { isAdmin: true, isOwner: false, approvalNatures: ['SANKI', 'SAMAST', 'PERSONAL'], vendors: ['Sanki Legacy Vendor'], vendorsByNature: { SANKI: ['Sanki Supplier'], SAMAST: ['Meta', 'Samast Supplier', '<Vendor "quoted">'], PERSONAL: ['Private Supplier'] } },
    selectedNature: 'SANKI', bankNature: 'PERSONAL', bankDraftId: 'DRAFT-1', bankExpenseRowId: '', bankExpenseNature: '',
    bankReconView: { account: 'IndusInd Bank 7883', rows: [{ id: 'bank-1', bank: { date: '2026-09-17', debit: 12467, reference: 'SOURCE-REF', description: 'Bank payment' } }] },
    bankEntityTaggingActive: () => true,
    bankRowNature: row => row && row.entityTag && row.entityTag.nature || 'PERSONAL',
    bankReconRow(id) { return context.bankReconView.rows.find(row => row.id === id); },
    bankDisplayParts: () => ({ vendor: 'Prefilled Vendor', particulars: 'Verified payment' }),
    categorySuggestions: nature => '<option value="' + nature + ' Category">',
    categoryChoices: nature => [{ name: nature + ' Category' }],
    document: { activeElement: null, addEventListener(type, fn) { documentListeners[type] = fn; } },
    el: id => nodes[id] || null, esc, fmt: String,
    uploadFile: () => Promise.resolve('/synthetic-bill.jpg'),
    api(url, options) { calls.push({ url, body: JSON.parse(options.body) }); return Promise.resolve({ success: false, error: 'Synthetic only' }); },
    loadAccountLedger() {}, renderBankReconciliation() {},
    ...overrides
  };
  context.window = context;
  vm.createContext(context);
  const run = code => vm.runInContext(code, context);
  run(line('function bankExpenseNatures('));
  run(line('function syncBankExpenseNature('));
  run(pickerCode);
  run(line('window.openBankExpense=function'));
  run(line("el('beNature').addEventListener('change',syncBankExpenseNature)"));
  run(line("el('beSave').addEventListener('click'"));
  return { context, nodes, calls, documentListeners, node };
}

test('bank-expense vendor markup uses the application combobox rather than browser history', () => {
  assert.match(html, /id="beVendor"[^>]*autocomplete="off"[^>]*role="combobox"[^>]*aria-controls="beVendorSuggestions"/);
  assert.match(html, /id="beVendorDropdown"[^>]*aria-label="Show vendors for bank expense"/);
  assert.match(html, /id="beVendorSuggestions"[^>]*role="listbox"/);
  const { nodes } = ui();
  assert.equal(nodes.beVendor.dataset.vendorPickerBound, 'true');
  assert.equal(nodes.beVendor.attrs.autocomplete, 'off');
  assert.equal(nodes.beVendor.attrs['aria-controls'], 'beVendorSuggestions');
});

test('bank vendor suggestions use the selected expense entity, not the bank or logging-form entity', () => {
  const { context, nodes } = ui();
  nodes.beNature.value = 'SAMAST'; nodes.beVendor.value = 'META'; nodes.beVendor.fire('input');
  assert.deepEqual(nodes.beVendorSuggestions.options.map(option => option.dataset.vendor), ['Meta']);
  nodes.beVendorDropdown.fire('click');
  assert.deepEqual(nodes.beVendorSuggestions.options.map(option => option.dataset.vendor), ['Meta', 'Samast Supplier', '<Vendor "quoted">']);
  assert.doesNotMatch(nodes.beVendorSuggestions.innerHTML, /Private Supplier|Sanki Supplier|Sanki Legacy Vendor/);
  assert.equal(context.vendorPickerNature('beVendor'), 'SAMAST');
});

test('blank or unauthorized expense entity cannot leak another entity vendor list', () => {
  const { context, nodes } = ui();
  for (const nature of ['', 'SANKI']) {
    nodes.beNature.value = nature; nodes.beVendorDropdown.fire('click');
    assert.equal(nodes.beVendorSuggestions.options.length, 0);
    assert.match(nodes.beVendorSuggestions.innerHTML, /Choose an allowed expense entity/);
    assert.doesNotMatch(nodes.beVendorSuggestions.innerHTML, /Private Supplier|Sanki Supplier|Samast Supplier/);
  }
  context.cfg.approvalNatures = ['SAMAST']; nodes.beNature.value = 'PERSONAL'; nodes.beVendorDropdown.fire('click');
  assert.equal(nodes.beVendorSuggestions.options.length, 0);
  assert.doesNotMatch(nodes.beVendorSuggestions.innerHTML, /Private Supplier/);
});

test('authorized PERSONAL list is supported, without fallback when that entity has no vendors', () => {
  const { context, nodes } = ui();
  nodes.beNature.value = 'PERSONAL'; nodes.beVendorDropdown.fire('click');
  assert.deepEqual(nodes.beVendorSuggestions.options.map(option => option.dataset.vendor), ['Private Supplier']);
  context.cfg.vendorsByNature.PERSONAL = []; nodes.beVendorDropdown.fire('click');
  assert.equal(nodes.beVendorSuggestions.options.length, 0);
  delete context.cfg.vendorsByNature; nodes.beVendorDropdown.fire('click');
  assert.equal(nodes.beVendorSuggestions.options.length, 0);
  assert.doesNotMatch(nodes.beVendorSuggestions.innerHTML, /Sanki Legacy Vendor/);
});

test('approved vendors can be chosen by click or keyboard and dropdown lists alternatives for prefilled text', () => {
  for (const method of ['click', 'keyboard']) {
    const { context, nodes } = ui();
    nodes.beNature.value = 'SAMAST'; nodes.beVendor.value = 'Different prefill'; nodes.beVendorDropdown.fire('click');
    assert.equal(nodes.beVendorSuggestions.options.length, 3);
    const option = nodes.beVendorSuggestions.options[1];
    if (method === 'click') option.fire('click'); else option.fire('keydown', { key: 'Enter' });
    assert.equal(nodes.beVendor.value, 'Samast Supplier');
    assert.equal(nodes.beVendorSuggestions.classList.contains('open'), false);
    assert.equal(nodes.beVendor.attrs['aria-expanded'], 'false');
    assert.equal(context.document.activeElement, nodes.beVendor);
  }
});

test('changing expense entity preserves typed vendor text but closes stale options and refreshes its next list', () => {
  const { nodes } = ui();
  nodes.beNature.value = 'SAMAST'; nodes.beVendor.value = 'Typed vendor'; nodes.beVendorDropdown.fire('click');
  assert.equal(nodes.beVendorSuggestions.classList.contains('open'), true);
  nodes.beNature.value = 'PERSONAL'; nodes.beNature.fire('change');
  assert.equal(nodes.beVendor.value, 'Typed vendor');
  assert.equal(nodes.beVendorSuggestions.classList.contains('open'), false);
  assert.equal(nodes.beLedger.value, '');
  nodes.beVendorDropdown.fire('click');
  assert.deepEqual(nodes.beVendorSuggestions.options.map(option => option.dataset.vendor), ['Private Supplier']);
});

test('new vendor names remain allowed under the existing submission policy and are safely escaped', () => {
  const { nodes } = ui();
  nodes.beNature.value = 'SAMAST'; nodes.beVendor.value = '<New "Vendor">'; nodes.beVendor.fire('input');
  assert.equal(nodes.beVendor.value, '<New "Vendor">');
  assert.equal(nodes.beVendorSuggestions.options.length, 0);
  assert.match(nodes.beVendorSuggestions.innerHTML, /will be submitted as a new vendor/);
  assert.match(nodes.beVendorSuggestions.innerHTML, /&lt;New &quot;Vendor&quot;&gt;/);
  assert.doesNotMatch(nodes.beVendorSuggestions.innerHTML, /<New/);
  nodes.beVendorDropdown.fire('click');
  assert.match(nodes.beVendorSuggestions.innerHTML, /&lt;Vendor &quot;quoted&quot;&gt;/);
});

test('vendor menu supports keyboard focus, Escape, outside click, dialog close, and idempotent binding', () => {
  const { context, nodes, documentListeners, node } = ui();
  const listenerCount = nodes.beVendor.listeners.input.length;
  context.bindVendorPicker('beVendor'); assert.equal(nodes.beVendor.listeners.input.length, listenerCount);
  nodes.beNature.value = 'SAMAST'; nodes.beVendor.value = 'Meta'; nodes.beVendor.fire('keydown', { key: 'ArrowDown' });
  assert.equal(context.document.activeElement, nodes.beVendorSuggestions.options[0]);
  nodes.beVendorSuggestions.options[0].fire('keydown', { key: 'Escape' });
  assert.equal(nodes.beVendorSuggestions.classList.contains('open'), false);
  nodes.beVendorDropdown.fire('click'); documentListeners.click({ target: node() });
  assert.equal(nodes.beVendorSuggestions.classList.contains('open'), false);
  nodes.beVendorDropdown.fire('click'); nodes.bankExpenseDlg.close();
  assert.equal(nodes.beVendorSuggestions.classList.contains('open'), false);
});

test('opening a bank expense remains read-only and records source bank figures unchanged', () => {
  const { context, nodes, calls } = ui();
  context.openBankExpense('bank-1');
  assert.equal(calls.length, 0);
  assert.equal(nodes.beNature.value, '', 'untagged shared ownership still requires a deliberate choice');
  assert.equal(nodes.beVendor.value, 'Prefilled Vendor');
  assert.equal(nodes.beAccount.value, 'IndusInd Bank 7883');
  assert.equal(nodes.beDate.value, '2026-09-17');
  assert.equal(nodes.beAmount.value, 12467);
});

test('choosing an approved vendor retains the existing create-expense payload and financial safeguards', async () => {
  const { context, nodes, calls } = ui();
  context.openBankExpense('bank-1');
  nodes.beNature.value = 'SAMAST'; nodes.beNature.fire('change');
  nodes.beVendor.value = 'meta'; nodes.beVendor.fire('input'); nodes.beVendorSuggestions.options[0].fire('click');
  nodes.beLedger.value = 'SAMAST Category'; nodes.beBillPhoto.files = [{}]; nodes.beBill.value = 'printed'; nodes.bePaymentType.value = 'UPI';
  nodes.beSave.fire('click'); await flush();
  assert.equal(calls.length, 1);
  assert.equal(calls[0].url, '/api/expenses/bank-statements/create-expense');
  assert.equal(calls[0].body.vendor, 'Meta');
  assert.equal(calls[0].body.nature, 'SAMAST');
  assert.equal(calls[0].body.ledger, 'SAMAST Category');
  assert.equal(calls[0].body.rowId, 'bank-1');
  assert.equal(calls[0].body.draftId, 'DRAFT-1');
  assert.equal(Object.hasOwn(calls[0].body, 'vendorId'), false, 'no stale cross-entity vendor identifier is introduced');
  assert.equal(Object.hasOwn(calls[0].body, 'amount'), false);
  assert.equal(Object.hasOwn(calls[0].body, 'date'), false);
  assert.equal(context.bankReconView.rows[0].bank.debit, 12467);
});

test('the full expense page inline scripts compile after wiring the bank vendor picker', () => {
  for (const script of html.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/gi)) assert.doesNotThrow(() => new vm.Script(script[1]));
});

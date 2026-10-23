'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const crypto = require('node:crypto');
const ExpenseLoans = require('../public/expense-loans.js');

const page = fs.readFileSync(path.join(__dirname, '../public/expenses.html'), 'utf8');
const esc = value => String(value == null ? '' : value)
  .replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;')
  .replaceAll('"', '&quot;').replaceAll("'", '&#39;');
const fmt = value => {
  const number = Number(value) || 0;
  return '₹' + number.toLocaleString('en-IN', {
    minimumFractionDigits: Number.isInteger(number) ? 0 : 1,
    maximumFractionDigits: 2
  });
};

function section(from, to) {
  const start = page.indexOf(from), end = page.indexOf(to, start);
  assert.notEqual(start, -1, 'missing source marker: ' + from);
  assert.notEqual(end, -1, 'missing source marker: ' + to);
  return page.slice(start, end);
}

function ui() {
  const document = fakeDocument();
  const nodes = {
    payableLedgerToolbar: document.createElement('div'),
    payableLedgerSelectionSummary: document.createElement('strong'),
    payableLedgerSelectAll: document.createElement('button'),
    payableLedgerDeselectAll: document.createElement('button')
  };
  Object.entries(nodes).forEach(([id, node]) => { node.id = id; document.body.appendChild(node); });
  const calls = { api: 0 };
  const sandbox = {
    window: {}, document,
    cfg: { isAdmin: true, claimantOnly: false },
    esc, fmt, el: id => nodes[id] || null,
    expenseOperatorAllowed: () => true,
    canApproveExpense: () => true,
    ashpreetPaymentAccounts: () => [],
    refundExpenseHistory: () => '',
    procurementCostHtml: () => '',
    procurementPayableCard: payable => '<article data-sourcing-bill="' + esc(payable.id) + '"></article>',
    setTimeout: fn => fn(),
    api: () => { calls.api++; throw Error('selection must not call an API'); },
    fetch: () => { calls.api++; throw Error('selection must not send a request'); }
  };
  vm.createContext(sandbox);
  vm.runInContext(
    section('    function expenseActions(e,options){', '    var lgPayablesById=') +
    section('    function payableLedgerChoice(', '    function procurementPayableCard('),
    sandbox
  );
  return { sandbox, nodes, document, calls };
}

// A deliberately small DOM: enough to exercise the real selection binder's node
// movement, sibling anchors, identity and focus, without adding a dependency or
// replacing its behavior with a test-only implementation.
function fakeDocument() {
  const document = { activeElement: null };
  class Node {
    constructor(tag, type = 1) {
      this.tagName = tag.toUpperCase(); this.nodeType = type; this.ownerDocument = document;
      this.parentNode = null; this.childNodes = []; this.style = {}; this.dataset = {};
      this.className = ''; this.id = ''; this.hidden = false; this.disabled = false;
      this.checked = false; this.listeners = {}; this.textContent = '';
      this.classList = {
        contains: name => this.className.split(/\s+/).includes(name),
        add: name => { if (!this.classList.contains(name)) this.className = (this.className + ' ' + name).trim(); },
        remove: name => { this.className = this.className.split(/\s+/).filter(value => value !== name).join(' '); },
        toggle: (name, force) => {
          const on = force === undefined ? !this.classList.contains(name) : force;
          this.classList[on ? 'add' : 'remove'](name); return on;
        }
      };
    }
    get children() { return this.childNodes.filter(node => node.nodeType === 1); }
    get firstChild() { return this.childNodes[0] || null; }
    get firstElementChild() { return this.children[0] || null; }
    get parentElement() { return this.parentNode && this.parentNode.nodeType === 1 ? this.parentNode : null; }
    get nextSibling() { return this.parentNode ? this.parentNode.childNodes[this.parentNode.childNodes.indexOf(this) + 1] || null : null; }
    get previousSibling() { return this.parentNode ? this.parentNode.childNodes[this.parentNode.childNodes.indexOf(this) - 1] || null : null; }
    get isConnected() { return this === document.body || !!(this.parentNode && this.parentNode.isConnected); }
    set innerHTML(value) { throw Error('selection must not redraw existing rows: ' + value); }
    cloneNode() { throw Error('selection must move existing rows, not clone them'); }
    contains(node) { return this === node || this.childNodes.some(child => child.contains(node)); }
    matches(selector) {
      if (selector.startsWith('.')) return this.nodeType === 1 && this.classList.contains(selector.slice(1));
      if (selector.startsWith('#')) return this.nodeType === 1 && this.id === selector.slice(1);
      return this.nodeType === 1 && this.tagName.toLowerCase() === selector.toLowerCase();
    }
    closest(selector) { return this.matches(selector) ? this : this.parentElement && this.parentElement.closest(selector); }
    querySelectorAll(selector) {
      const matches = [];
      this.childNodes.forEach(child => { if (child.matches(selector)) matches.push(child); matches.push(...child.querySelectorAll(selector)); });
      return matches;
    }
    querySelector(selector) { return this.querySelectorAll(selector)[0] || null; }
    appendChild(node) { return this.insertBefore(node, null); }
    append(...nodes) { nodes.forEach(node => this.appendChild(node)); }
    prepend(node) { return this.insertBefore(node, this.firstChild); }
    insertBefore(node, before) {
      if (node === before) return node;
      if (before !== null && before.parentNode !== this) throw Error('anchor belongs to another parent');
      if (node.parentNode) node.parentNode.removeChild(node);
      const index = before === null ? this.childNodes.length : this.childNodes.indexOf(before);
      this.childNodes.splice(index, 0, node); node.parentNode = this; return node;
    }
    removeChild(node) {
      const index = this.childNodes.indexOf(node); assert.notEqual(index, -1);
      if (node.isConnected && node.contains(document.activeElement)) document.activeElement = document.body;
      this.childNodes.splice(index, 1); node.parentNode = null; return node;
    }
    remove() { if (this.parentNode) this.parentNode.removeChild(this); }
    after(node) { this.parentNode.insertBefore(node, this.nextSibling); }
    addEventListener(event, listener) { (this.listeners[event] ||= []).push(listener); }
    change() { (this.listeners.change || []).forEach(listener => listener.call(this, { target: this })); }
    click() { if (!this.disabled && this.onclick) this.onclick.call(this); }
    focus() { document.activeElement = this; }
    setAttribute(name, value) { this[name] = String(value); }
    getAttribute(name) { return this[name] === undefined ? null : String(this[name]); }
  }
  document.createElement = tag => new Node(tag);
  document.createComment = value => { const node = new Node('#comment', 8); node.textContent = value; return node; };
  document.body = new Node('body'); document.activeElement = document.body;
  document.querySelectorAll = selector => document.body.querySelectorAll(selector);
  document.querySelector = selector => document.body.querySelector(selector);
  document.getElementById = id => document.querySelector('#' + id);
  return document;
}

function ledgerHost(ui, rows) {
  const { document } = ui;
  const host = document.createElement('div');
  document.body.appendChild(host);
  const ledgers = rows.map((row, index) => {
    const group = document.createElement('section'); group.className = 'payable-ledger-group'; group.id = row.key || 'group-' + index;
    const label = document.createElement('label'); label.className = 'payable-ledger-choice';
    const box = document.createElement('input'); box.className = 'payable-ledger-select';
    box.dataset = { due: String(row.due), key: row.key || 'expense|' + index }; box.disabled = !!row.disabled;
    box.checked = !!row.checked; label.appendChild(box); group.appendChild(label);
    const content = document.createElement('div'); content.className = 'payable-ledger-content'; group.appendChild(content);
    const detail = document.createElement('details'); detail.open = !!row.open; content.appendChild(detail);
    const action = document.createElement('button'); action.dataset.originalAction = row.key; detail.appendChild(action);
    const parent = row.parent || host; parent.appendChild(group);
    return { group, label, box, content, detail, action, parent };
  });
  return { host, ledgers, boxes: ledgers.map(row => row.box) };
}

const selectedIds = host => host.querySelector('#payableLedgerSelected').children.map(group => group.id);
const directGroupIds = host => host.children.filter(node => node.classList.contains('payable-ledger-group')).map(group => group.id);

function expense(overrides = {}) {
  return {
    id: 'EX-1', nature: 'SANKI', date: '2026-10-01', status: 'approved',
    vendor: 'Same Vendor', particulars: 'Bill', ledger: 'General', type: 'variable',
    paymentType: 'UPI', amount: 100, paidAmount: 0, balanceDue: 100,
    paidAlready: false, payments: [], reimbursementPayments: [],
    vendorAdvanceApplications: [], billPhotos: [], paymentProofs: [], auditHistory: [],
    ...overrides
  };
}

test('one expense-vendor ledger selector covers all of that vendor\'s bills and totals remaining balances', () => {
  const { sandbox } = ui();
  const first = expense({ id: 'EX-1', amount: 100, paidAmount: 40, balanceDue: 60, status: 'partially_paid' });
  const second = expense({ id: 'EX-2', amount: 75, paidAmount: 25, balanceDue: 50 });
  const output = sandbox.vendorPayableGroups([first, second]);

  assert.equal((output.match(/class="payable-ledger-select"/g) || []).length, 1);
  assert.match(output, /data-key="expense\|same vendor"/);
  assert.match(output, /data-due="110"/);
  assert.match(output, /2 approved bills grouped under this vendor/);
  assert.match(output, /row_EX-1/);
  assert.match(output, /row_EX-2/);
});

test('an expense vendor and sourcing vendor with the same name remain separate selectable ledgers', () => {
  const { sandbox } = ui();
  const ordinary = sandbox.vendorPayableGroups([expense({ balanceDue: 60 })]);
  const sourcing = sandbox.procurementPayableGroups([
    { id: 'PO-1', date: '2026-10-01', vendor: 'Same Vendor', balanceDue: 45 }
  ]);
  const output = ordinary + sourcing;

  assert.equal((output.match(/class="payable-ledger-select"/g) || []).length, 2);
  assert.match(output, /data-key="expense\|same vendor"/);
  assert.match(output, /data-key="sourcing\|same vendor"/);
  assert.match(output, /data-due="60"/);
  assert.match(output, /data-due="45"/);
});

test('payable expense cards display only remaining due and omit the unrelated payment-combine selector', () => {
  const { sandbox } = ui();
  const output = sandbox.payableExpenseCard(expense({
    amount: 7000, paidAmount: 6600, balanceDue: 400, status: 'partially_paid'
  }));

  assert.match(output, /class="claim-amount">₹400 due/);
  assert.match(output, /Paid: ₹6,600 · Due now: ₹400/);
  assert.doesNotMatch(output, /combined-pay-select|Select to combine/);
});

test('selection toolbar totals checked ledger balances and select-all/deselect-all update count and amount', () => {
  const context = ui(), { sandbox, nodes } = context;
  const { host, boxes } = ledgerHost(context, [{ due: 60 }, { due: 25.5 }, { due: 14.5 }]);
  sandbox.bindPayableLedgerSelection(host);

  assert.equal(nodes.payableLedgerToolbar.style.display, 'flex');
  assert.equal(nodes.payableLedgerSelectionSummary.textContent, '0 ledgers selected · ₹0');
  assert.equal(nodes.payableLedgerDeselectAll.disabled, true);

  boxes[0].checked = true; boxes[0].change();
  assert.equal(nodes.payableLedgerSelectionSummary.textContent, '1 ledger selected · ₹60');

  nodes.payableLedgerSelectAll.onclick();
  assert.equal(nodes.payableLedgerSelectionSummary.textContent, '3 ledgers selected · ₹100');
  assert.equal(nodes.payableLedgerSelectAll.disabled, true);
  assert.equal(nodes.payableLedgerDeselectAll.disabled, false);

  nodes.payableLedgerDeselectAll.onclick();
  assert.equal(nodes.payableLedgerSelectionSummary.textContent, '0 ledgers selected · ₹0');
  assert.ok(boxes.every(box => box.checked === false));
});

test('a filtered reload binds fresh unchecked ledger controls and cannot retain the prior total', () => {
  const context = ui(), { sandbox, nodes } = context;
  const { host: oldHost } = ledgerHost(context, [{ due: 90 }, { due: 10 }]);
  sandbox.bindPayableLedgerSelection(oldHost);
  nodes.payableLedgerSelectAll.onclick();
  assert.equal(nodes.payableLedgerSelectionSummary.textContent, '2 ledgers selected · ₹100');

  oldHost.remove();
  const { host: freshHost, boxes: freshBoxes } = ledgerHost(context, [{ due: 35 }]);
  sandbox.bindPayableLedgerSelection(freshHost);
  assert.equal(nodes.payableLedgerSelectionSummary.textContent, '0 ledgers selected · ₹0');
  assert.equal(freshBoxes[0].checked, false);
  assert.equal(nodes.payableLedgerDeselectAll.disabled, true);

  nodes.payableLedgerSelectAll.onclick();
  assert.equal(nodes.payableLedgerSelectionSummary.textContent, '1 ledger selected · ₹35');
  assert.equal(freshBoxes[0].checked, true);
});

test('all inline expense-page scripts remain valid JavaScript', () => {
  for (const script of page.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/gi)) {
    if (script[1].trim()) assert.doesNotThrow(() => new vm.Script(script[1]));
  }
});

test('the lender selection helper uses its current content-specific cache key', () => {
  const source = fs.readFileSync(path.join(__dirname, '../public/expense-loans.js'), 'utf8').replace(/\r\n/g, '\n');
  const version = crypto.createHash('sha256').update(source).digest('hex').slice(0, 12);
  const scripts = [...page.matchAll(/<script\b[^>]*\bsrc=["']\/expense-loans\.js\?v=([^"']+)["'][^>]*>/gi)];
  assert.equal(scripts.length, 1, 'expense page must load the lender helper exactly once');
  assert.equal(scripts[0][1], version, 'expense page must load the exact current lender helper content version');
});

test('selectors are compact labels beside original cards, not separate visible selection rows', () => {
  const { sandbox } = ui();
  const card = '<details class="claim-card" open><summary>Original card</summary><button data-pay="EX-9">Pay</button></details>';
  const output = sandbox.payableLedgerChoice('expense|vendor', 'Vendor', 20, card, 'vendor');
  assert.match(output, /^<section class="payable-ledger-group"><label class="payable-ledger-choice"[^>]*><input\b/);
  assert.match(output, /aria-label="Select Vendor vendor ledger"/);
  assert.match(output, new RegExp('</label><div class="payable-ledger-content">' + card.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + '</div></section>$'));
  assert.match(output, /<label class="payable-ledger-choice"[^>]*><input[^>]*><\/label>/);
  assert.doesNotMatch(output, /> Select|outstanding<\/label>/);
});

test('vendor, sourcing and lender HTML have one accessible selector per ledger and preserve escaped names', () => {
  const { sandbox } = ui();
  const name = 'A & B <Vendor> "Quoted"';
  const ordinary = sandbox.vendorPayableGroups([expense({ vendor: name })]);
  const sourcing = sandbox.procurementPayableGroups([{ id: 'PO-1', vendor: name, balanceDue: 40 }]);
  const lenders = ExpenseLoans.renderParties({ lenders: [{ name, nature: 'SANKI', outstanding: 30, loans: [], borrowed: 30, repaid: 0 }] }, '', { selectable: true });
  for (const html of [ordinary, sourcing, lenders]) {
    assert.equal((html.match(/class="payable-ledger-select"/g) || []).length, 1);
    assert.equal((html.match(/class="payable-ledger-group"/g) || []).length, 1);
    assert.equal((html.match(/class="payable-ledger-content"/g) || []).length, 1);
    assert.match(html, /aria-label="Select A &amp; B &lt;Vendor&gt; &quot;Quoted&quot;/);
    assert.match(html, /<label class="payable-ledger-choice"[^>]*><input[^>]*><\/label>/);
    assert.doesNotMatch(html, /<Vendor>/);
  }
  assert.match(lenders, /data-key="loan\|SANKI\|A &amp; B &lt;Vendor&gt; &quot;Quoted&quot;"/);
});

test('nonselectable lender register keeps its original cards and has no selection wrapper', () => {
  const data = { lenders: [{ name: 'Lender', nature: 'SANKI', outstanding: 30, loans: [], borrowed: 30, repaid: 0 }] };
  for (const options of [undefined, { selectable: false }]) {
    const output = ExpenseLoans.renderParties(data, '', options);
    assert.match(output, /<details class="claim-card">/);
    assert.match(output, /₹30\.00 owed/);
    assert.doesNotMatch(output, /payable-ledger-group|payable-ledger-select|payable-ledger-content/);
  }
});

test('selected ledgers move immediately under the total and keep original relative order rather than click order', () => {
  const context = ui(), { sandbox } = context;
  const { host, ledgers } = ledgerHost(context, [
    { key: 'sourcing|A', due: 18 }, { key: 'expense|B', due: 12 }, { key: 'loan|C', due: 30 }
  ]);
  sandbox.bindPayableLedgerSelection(host);
  const selected = host.querySelector('#payableLedgerSelected');
  assert.equal(host.firstElementChild, selected);
  assert.equal(selected.hidden, true);
  assert.equal(selected.className, 'claim-list payable-ledger-selected');
  ledgers[2].box.checked = true; ledgers[2].box.change();
  ledgers[0].box.checked = true; ledgers[0].box.change();
  assert.deepEqual(selectedIds(host), ['sourcing|A', 'loan|C']);
  assert.deepEqual(directGroupIds(host), ['expense|B']);
  assert.equal(selected.hidden, false);
  assert.equal(selected.children[0], ledgers[0].group);
  assert.equal(selected.children[1], ledgers[2].group);
  assert.ok(ledgers[0].group.classList.contains('is-selected'));
  assert.equal(host.querySelectorAll('.payable-ledger-select').length, 3);
});

test('deselect returns each moved vendor and nested lender to its original anchor and select-all is reversible', () => {
  const context = ui(), { sandbox, nodes, document } = context;
  const { host, ledgers } = ledgerHost(context, [
    { key: 'expense|A', due: 20 }, { key: 'sourcing|B', due: 40 }
  ]);
  const note = document.createElement('p'); note.textContent = 'Lender information';
  const lenderHost = document.createElement('div'); host.appendChild(lenderHost); lenderHost.appendChild(note);
  const nested = ledgerHost(context, [{ key: 'loan|C', due: 50, parent: lenderHost }, { key: 'loan|D', due: 70, parent: lenderHost }]);
  nested.host.remove();
  const all = ledgers.concat(nested.ledgers);
  sandbox.bindPayableLedgerSelection(host);
  nodes.payableLedgerSelectAll.click();
  assert.deepEqual(selectedIds(host), all.map(row => row.group.id));
  assert.equal(nodes.payableLedgerSelectionSummary.textContent, '4 ledgers selected · ₹180');
  nested.ledgers[0].box.checked = false; nested.ledgers[0].box.change();
  assert.deepEqual(directGroupIds(lenderHost), ['loan|C']);
  nodes.payableLedgerDeselectAll.click();
  assert.deepEqual(directGroupIds(host), ['expense|A', 'sourcing|B']);
  assert.deepEqual(directGroupIds(lenderHost), ['loan|C', 'loan|D']);
  assert.equal(lenderHost.firstElementChild, note);
  assert.ok(all.every(row => row.group.parentNode === row.parent && !row.group.classList.contains('is-selected')));
  assert.equal(host.querySelector('#payableLedgerSelected').hidden, true);
  assert.equal(host.querySelectorAll('.payable-ledger-select').length, 4);
});

test('moving selected rows preserves actual details, action listeners, input values and keyboard focus', () => {
  const context = ui(), { sandbox, document, calls } = context;
  const { host, ledgers } = ledgerHost(context, [{ key: 'expense|A', due: 100, open: true }]);
  const row = ledgers[0]; let clicked = 0;
  row.action.onclick = () => { clicked++; }; row.action.value = 'Unsaved local value';
  sandbox.bindPayableLedgerSelection(host);
  row.box.focus(); row.box.checked = true; row.box.change();
  assert.equal(document.activeElement, row.box);
  assert.equal(row.detail.open, true);
  assert.equal(row.group.querySelector('details'), row.detail);
  assert.equal(row.action.value, 'Unsaved local value');
  row.action.click(); assert.equal(clicked, 1);
  row.box.checked = false; row.box.change();
  assert.equal(document.activeElement, row.box);
  assert.equal(row.detail.open, true);
  assert.equal(row.action.value, 'Unsaved local value');
  row.action.click(); assert.equal(clicked, 2);
  assert.equal(calls.api, 0);
});

test('select-all leaves disabled rows untouched and handles zero outstanding without losing a selector', () => {
  const context = ui(), { sandbox, nodes } = context;
  const { host, ledgers } = ledgerHost(context, [
    { key: 'expense|zero', due: 0 }, { key: 'sourcing|disabled', due: 500, disabled: true }, { key: 'loan|active', due: 12.5 }
  ]);
  sandbox.bindPayableLedgerSelection(host); nodes.payableLedgerSelectAll.click();
  assert.equal(nodes.payableLedgerSelectionSummary.textContent, '2 ledgers selected · ₹12.5');
  assert.equal(ledgers[1].box.checked, false);
  assert.deepEqual(selectedIds(host), ['expense|zero', 'loan|active']);
  assert.deepEqual(directGroupIds(host), ['sourcing|disabled']);
  nodes.payableLedgerDeselectAll.click();
  assert.deepEqual(directGroupIds(host), ['expense|zero', 'sourcing|disabled', 'loan|active']);
});

test('no rows hides selection controls and leaves no stale total after a reload', () => {
  const context = ui(), { sandbox, nodes } = context;
  const previous = ledgerHost(context, [{ due: 45 }]);
  sandbox.bindPayableLedgerSelection(previous.host); nodes.payableLedgerSelectAll.click(); previous.host.remove();
  const empty = ledgerHost(context, []); sandbox.bindPayableLedgerSelection(empty.host);
  assert.equal(nodes.payableLedgerToolbar.style.display, 'none');
  assert.equal(nodes.payableLedgerSelectionSummary.textContent, '0 ledgers selected · ₹0');
  assert.equal(nodes.payableLedgerSelectAll.disabled, true);
  assert.equal(nodes.payableLedgerDeselectAll.disabled, true);
  assert.equal(empty.host.querySelector('#payableLedgerSelected').hidden, true);
});

test('a prechecked row is pinned initially, while a disabled checked row contributes nothing', () => {
  const context = ui(), { sandbox, nodes } = context;
  const { host, ledgers } = ledgerHost(context, [
    { key: 'expense|disabled', due: 500, disabled: true, checked: true },
    { key: 'expense|checked', due: 15, checked: true }
  ]);
  sandbox.bindPayableLedgerSelection(host);
  assert.deepEqual(selectedIds(host), ['expense|checked']);
  assert.equal(nodes.payableLedgerSelectionSummary.textContent, '1 ledger selected · ₹15');
  assert.equal(nodes.payableLedgerSelectAll.disabled, true);
  assert.ok(!ledgers[0].group.classList.contains('is-selected'));
  nodes.payableLedgerDeselectAll.click();
  assert.ok(ledgers.every(row => row.box.checked === false));
  assert.deepEqual(directGroupIds(host), ['expense|disabled', 'expense|checked']);
});

test('repeated selection changes retain one selector per ledger and return to original homes', () => {
  const context = ui(), { sandbox, nodes, document } = context;
  const { host, ledgers } = ledgerHost(context, [
    { key: 'expense|A', due: 10 }, { key: 'sourcing|B', due: 20 }, { key: 'loan|C', due: 30 }
  ]);
  sandbox.bindPayableLedgerSelection(host);
  for (let round = 0; round < 4; round++) {
    ledgers[2].box.checked = true; ledgers[2].box.change();
    ledgers[0].box.checked = true; ledgers[0].box.change();
    assert.deepEqual(selectedIds(host), ['expense|A', 'loan|C']);
    ledgers[2].box.checked = false; ledgers[2].box.change();
    assert.deepEqual(directGroupIds(host), ['sourcing|B', 'loan|C']);
    nodes.payableLedgerSelectAll.focus(); nodes.payableLedgerSelectAll.click();
    assert.equal(document.activeElement, nodes.payableLedgerSelectAll);
    assert.deepEqual(selectedIds(host), ['expense|A', 'sourcing|B', 'loan|C']);
    nodes.payableLedgerDeselectAll.focus(); nodes.payableLedgerDeselectAll.click();
    assert.equal(document.activeElement, nodes.payableLedgerDeselectAll);
    assert.deepEqual(directGroupIds(host), ['expense|A', 'sourcing|B', 'loan|C']);
    assert.equal(host.querySelectorAll('.payable-ledger-select').length, 3);
    assert.equal(host.querySelectorAll('#payableLedgerSelected').length, 1);
  }
});

test('an all-disabled ledger set cannot be selected and has no payable total', () => {
  const context = ui(), { sandbox, nodes } = context;
  const { host, boxes } = ledgerHost(context, [{ due: 40, disabled: true }]);
  sandbox.bindPayableLedgerSelection(host);
  assert.equal(nodes.payableLedgerSelectAll.disabled, true);
  assert.equal(nodes.payableLedgerDeselectAll.disabled, true);
  assert.equal(nodes.payableLedgerSelectionSummary.textContent, '0 ledgers selected · ₹0');
  nodes.payableLedgerSelectAll.click();
  assert.equal(boxes[0].checked, false);
  assert.equal(host.querySelector('#payableLedgerSelected').hidden, true);
});

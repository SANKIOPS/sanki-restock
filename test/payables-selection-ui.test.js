'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

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
  const nodes = {
    payableLedgerToolbar: { style: {} },
    payableLedgerSelectionSummary: { textContent: '' },
    payableLedgerSelectAll: { disabled: false, onclick: null },
    payableLedgerDeselectAll: { disabled: false, onclick: null }
  };
  const sandbox = {
    window: {}, document: { querySelectorAll: () => [] },
    cfg: { isAdmin: true, claimantOnly: false },
    esc, fmt, el: id => nodes[id] || null,
    expenseOperatorAllowed: () => true,
    canApproveExpense: () => true,
    ashpreetPaymentAccounts: () => [],
    refundExpenseHistory: () => '',
    procurementCostHtml: () => '',
    procurementPayableCard: payable => '<article data-sourcing-bill="' + esc(payable.id) + '"></article>',
    setTimeout: fn => fn()
  };
  vm.createContext(sandbox);
  vm.runInContext(
    section('    function expenseActions(e,options){', '    var lgPayablesById=') +
    section('    function payableLedgerChoice(', '    function procurementPayableCard('),
    sandbox
  );
  return { sandbox, nodes };
}

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

function checkbox(due) {
  return {
    checked: false, disabled: false, dataset: { due: String(due) }, listeners: {},
    addEventListener(event, listener) { this.listeners[event] = listener; },
    change() { this.listeners.change(); }
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
  const { sandbox, nodes } = ui();
  const boxes = [checkbox(60), checkbox(25.5), checkbox(14.5)];
  sandbox.bindPayableLedgerSelection({ querySelectorAll: () => boxes });

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
  const { sandbox, nodes } = ui();
  const oldBoxes = [checkbox(90), checkbox(10)];
  sandbox.bindPayableLedgerSelection({ querySelectorAll: () => oldBoxes });
  nodes.payableLedgerSelectAll.onclick();
  assert.equal(nodes.payableLedgerSelectionSummary.textContent, '2 ledgers selected · ₹100');

  const freshBoxes = [checkbox(35)];
  sandbox.bindPayableLedgerSelection({ querySelectorAll: () => freshBoxes });
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

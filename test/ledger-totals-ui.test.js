'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const page = fs.readFileSync(path.join(__dirname, '../public/expenses.html'), 'utf8');
const section = (from, to) => {
  const start = page.indexOf(from);
  const end = page.indexOf(to, start);
  assert.notEqual(start, -1, `start marker exists: ${from}`);
  assert.notEqual(end, -1, `end marker exists: ${to}`);
  return page.slice(start, end);
};
const flush = () => new Promise(resolve => setImmediate(resolve));
const money = value => `₹${Number(value || 0).toLocaleString('en-IN')}`;
const esc = value => String(value == null ? '' : value)
  .replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;')
  .replaceAll('"', '&quot;').replaceAll("'", '&#39;');
const footer = html => {
  const match = String(html || '').match(/<tfoot\b[^>]*>([\s\S]*?)<\/tfoot>/i);
  assert.ok(match, 'ledger table renders a totals footer');
  return match[1];
};

test('vendor ledger footer totals selected-period In and Out and uses the supplied closing balance', () => {
  const code = section('    function vendorLedgerTable(v){', '    function renderVendors(){');
  const context = {
    cfg: { isAdmin: false }, expensesById: {}, vendorPaymentProofRows: {},
    vendorPaymentProofSequence: 0, esc, fmt: money,
    el: id => id === 'vf_from' ? { value: '2099-04-01' } : null,
    vendorBalanceHtml: money, procurementCostHtml: () => ''
  };
  vm.createContext(context);
  vm.runInContext(code, context);
  const html = context.vendorLedgerTable({
    ledgerOpeningBalance: 40,
    ledgerClosingBalance: 275,
    entities: ['SANKI'],
    ledgerRows: [
      { date: '2099-04-01', kind: 'payment', particulars: 'Part payment', type: 'Payment', reference: 'PAY-1', in: 100, out: 0, balance: -60 },
      { date: '2099-04-02', kind: 'advance', particulars: 'Advance', type: 'Advance', reference: 'ADV-1', in: 25, out: 0, balance: -85 },
      { date: '2099-04-03', kind: 'expense', particulars: 'Bill', type: 'Expense', reference: 'EX-1', in: 0, out: 300, balance: 215 }
    ]
  });
  const totals = footer(html);
  assert.match(totals, /Selected-period totals/);
  assert.match(totals, /₹125/);
  assert.match(totals, /₹300/);
  assert.match(totals, /₹275/);
});

test('salary employee ledger footer totals debits and credits and uses the employee closing balance', async () => {
  const code = section('    function renderSalaryLedgers(){', '    function renderCounterCashOnly(){');
  const nodes = {
    panel: { innerHTML: '' },
    salaryLedgerList: { innerHTML: '', textContent: '' }
  };
  const context = {
    el: id => nodes[id], esc, fmt: money, proofGallery: () => '',
    api: () => Promise.resolve({
      success: true,
      ledgers: [{
        ledgerName: 'Test Employee — Salary', outstandingAdvance: 20,
        lastPostingDate: '2099-04-30', balance: 777, status: 'Company owes',
        entries: [
          { id: 'ADV-1', date: '2099-04-01', description: 'Advance', debit: 40, credit: 0 },
          { id: 'PAY-1', date: '2099-04-20', description: 'Payment', debit: 10, credit: 0 },
          { id: 'EARNED', date: '2099-04-30', description: 'Salary earned', debit: 0, credit: 100 }
        ]
      }]
    })
  };
  vm.createContext(context);
  vm.runInContext(code, context);
  context.renderSalaryLedgers();
  await flush();
  const totals = footer(nodes.salaryLedgerList.innerHTML);
  assert.match(totals, /Ledger totals/);
  assert.match(totals, /₹50/);
  assert.match(totals, /₹100/);
  assert.match(totals, /₹777/);
});

test('detailed account footer excludes opening from selected-period In and Out and uses the API closing balance', async () => {
  const code = section('    function loadAccountLedger(){', '    var bankDraftId=');
  const nodes = {
    lg_account: { value: 'Test Account' },
    lg_expense_nature: { value: '' },
    lg_from: { value: '2099-04-01' },
    lg_to: { value: '2099-04-30' },
    ledgerRecon: { style: {}, classList: { toggle() {} } },
    bs_nature: { value: '' }, bs_account: { value: '' },
    ledgerTable: { innerHTML: '', textContent: '' }
  };
  const context = {
    ledgerLoadedAccount: '', selectedNature: 'SANKI', bankNature: '',
    cfg: { isAdmin: false, isOwner: false }, esc, fmt: money, fmtBank: money,
    el: id => nodes[id] || null,
    SankiNavigation: { date: () => '', set() {} },
    bankAccountsForNature: () => [], reconciliationAccountsForNature: () => [],
    ledgerDisplayParts: row => ({ entity: 'SANKI', vendor: 'Test', particulars: row.description }),
    entryDetails: () => '', entryEditButton: () => '',
    loadBankStatement() {}, loadCounterCashReconciliation() {},
    GoKwikReconciliation: { mount() {} }, renderPaytmReports() {},
    api: () => Promise.resolve({
      success: true, account: 'Test Account', balance: 975,
      entries: [
        { id: 'OPENING', kind: 'opening', date: '', description: 'Opening balance', credit: 900, debit: 0, balance: 900 },
        { id: 'REC-1', kind: 'receipt', date: '2099-04-10', description: 'Receipt', credit: 100, debit: 0, balance: 1000 },
        { id: 'PAY-1', kind: 'expense', date: '2099-04-12', description: 'Expense', credit: 0, debit: 25, balance: 975 }
      ]
    })
  };
  vm.createContext(context);
  vm.runInContext(code, context);
  context.loadAccountLedger();
  await flush();
  const totals = footer(nodes.ledgerTable.innerHTML);
  assert.match(totals, /Selected-period totals/);
  assert.match(totals, /₹100/);
  assert.match(totals, /₹25/);
  assert.match(totals, /₹975/);
  assert.doesNotMatch(totals, /₹1,000/);
});

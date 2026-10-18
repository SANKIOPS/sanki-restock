'use strict';
const test = require('node:test'), assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const page = fs.readFileSync(path.join(__dirname, '../public/expenses.html'), 'utf8');
const pendingCode = page.slice(page.indexOf('    function renderPendingPayments(){'), page.indexOf('    function populatePaymentSource('));
const vendorsCode = page.slice(page.indexOf('    function renderVendors(){'), page.indexOf('    function openVendorManager('));
const flush = () => new Promise(resolve => setImmediate(resolve));
function fixture(tab) {
  const nodes = {}, requests = [], bindings = [];
  function parse(source) {
    for (const m of source.matchAll(/<([a-z]+)\b[^>]*\bid="([^"]+)"[^>]*>/g)) {
      const n = node(m[2]), value = m[0].match(/\bvalue="([^"]*)"/);
      if (value) n.value = value[1];
      if (m[1] === 'select') {
        const start = m.index + m[0].length, end = source.indexOf('</select>', start);
        n.options = Array.from(source.slice(start, end).matchAll(/<option\b[^>]*\bvalue="([^"]*)"[^>]*>([^<]*)/g), o => ({ value: o[1], textContent: o[2], disabled: false }));
        if (n.options.length) n.value = n.options[0].value;
      }
    }
  }
  function node(id) {
    if (nodes[id]) return nodes[id];
    const n = { id, value: '', dataset: {}, style: {}, options: [], textContent: '', disabled: false, listeners: {},
      field: { style: {}, insertAdjacentHTML: (_, source) => parse(source) },
      closest() { return n.field; }, insertAdjacentHTML(_, source) { parse(source); },
      addEventListener(event, fn) { n.listeners[event] = fn; },
      classList: { toggle() {} },
      querySelector(selector) {
        const value = selector.match(/^\[value="([^"]*)"\]$/); if (value) return n.options.find(o => o.value === value[1]);
        if (selector === '.vendor-ledger-legend') return node('legend');
        const kind = selector.match(/^\[data-vsource="([^"]+)"\]$/); if (kind) return node('vsource_' + kind[1]);
        return null;
      },
      querySelectorAll(selector) {
        if (selector === '[data-vsource]') return ['expense', 'sourcing', 'loan'].map(kind => { const button = node('vsource_' + kind); button.dataset.vsource = kind; return button; });
        return [];
      }
    };
    Object.defineProperty(n, 'innerHTML', { get() { return n.html || ''; }, set(value) { n.html = value; parse(value); } });
    nodes[id] = n; return n;
  }
  node('panel');
  const sandbox = { tab, cfg: { isOwner: false, ledgers: [], approvalNatures: ['SANKI', 'SAMAST'] }, selectedNature: 'SANKI', expensesById: {},
    el: id => nodes[id] || null, opts: () => '', monthStart: () => '2026-10-01', today: () => '2026-10-12', dateDaysAgo: () => '2026-10-01',
    esc: v => String(v), fmt: v => '₹' + Number(v), uploadFiles() {}, accountsForNature: () => ['Axis Bank 3448'], loadCash() {}, loadAccountLedger() {},
    procurementPayableCard: e => 'SOURCING:' + e.id, procurementPayableGroups: e => 'SOURCING:' + e.map(x => x.id).join(','), vendorPayableGroups: e => 'VENDORS:' + e.map(x => x.id).join(','), bindPayableLedgerSelection() {},
    vendorBalanceHtml: n => '₹' + n, vendorLedgerTable: () => '',
    api(url) { return new Promise((resolve, reject) => requests.push({ url, resolve, reject })); },
    ExpenseLoans: { renderParties: data => 'LENDERS:' + (data.lenders || []).map(l => l.name).join(','), bindParties(host, data, options) { bindings.push({ host, data, options }); } }
  };
  vm.createContext(sandbox); vm.runInContext(pendingCode + vendorsCode, sandbox);
  return { s: sandbox, nodes, requests, bindings };
}
const lenderResponse = { success: true, source: 'loan', asOf: '2026-10-12', loans: [{ id: 'REC-1', outstanding: 400000, repaid: 100000 }],
  lenders: [{ name: 'Example Lender' }], expenses: [], purchases: [], loanOutstanding: 400000, totals: { borrowed: 500000, repaid: 100000, outstanding: 400000 } };

test('payables expose all three party types and reset vendor-only status rules for lenders', async () => {
  const { s, nodes, requests, bindings } = fixture('pending'); s.renderPendingPayments();
  assert.deepEqual(nodes.pf_source.options.map(o => o.value), ['', 'expense', 'sourcing', 'loan']);
  nodes.pf_bucket.value = 'credit'; nodes.pf_source.value = 'loan'; nodes.pf_source.onchange.call(nodes.pf_source);
  assert.equal(nodes.pf_bucket.value, ''); assert.equal(nodes.pf_total_bucket.value, ''); assert.equal(nodes.pf_from.disabled, true);
  assert.equal(nodes.pf_bucket.options.find(o => o.value === 'credit').disabled, true); assert.match(requests[1].url, /source=loan/);
  requests[1].resolve(lenderResponse); await flush(); assert.match(nodes.ptable.innerHTML, /LENDERS:Example Lender/); assert.match(nodes.pstats.innerHTML, /400000/);
  assert.equal(bindings.length, 1); bindings[0].options.refreshAccounts(); assert.equal(requests.length, 3, 'repayment refreshes this filtered screen');
  nodes.pf_source.value = 'expense'; nodes.pf_source.onchange.call(nodes.pf_source); assert.equal(nodes.pf_from.disabled, false);
  assert.equal(nodes.pf_bucket.options.find(o => o.value === 'credit').disabled, false); assert.match(requests.at(-1).url, /source=expense/);
});

test('a slow payable response cannot overwrite a later source selection or another screen', async () => {
  const { s, nodes, requests } = fixture('pending'); s.renderPendingPayments(); nodes.pf_source.value = 'loan'; nodes.pf_source.onchange.call(nodes.pf_source);
  requests[1].resolve(lenderResponse); await flush(); const displayed = nodes.ptable.innerHTML;
  requests[0].resolve({ success: true, expenses: [{ id: 'OLD', balanceDue: 99 }], purchases: [], lenders: [], loans: [] }); await flush(); assert.equal(nodes.ptable.innerHTML, displayed);
  s.loadPendingPayments(); delete nodes.ptable; requests[2].resolve(lenderResponse); await flush();
});

test('vendor ledgers hide expense controls and legends in the lender group and use loan totals', async () => {
  const { s, nodes, requests, bindings } = fixture('vendors'); s.renderVendors();
  const button = nodes.vsource_loan; button.onclick.call(button);
  assert.equal(nodes.panel.dataset.vsource, 'loan'); assert.equal(nodes.vendorManageRow.style.display, 'none');
  assert.equal(nodes.vf_category.field.style.display, 'none'); assert.equal(nodes.legend.style.display, 'none'); assert.match(requests[1].url, /source=loan/);
  requests[1].resolve(lenderResponse); await flush(); assert.match(nodes.vtable.innerHTML, /Outstanding loan liability/); assert.match(nodes.vtable.innerHTML, /LENDERS:Example Lender/); assert.equal(bindings.length, 1);
  nodes.vsource_expense.onclick.call(nodes.vsource_expense); assert.equal(nodes.vendorManageRow.style.display, ''); assert.equal(nodes.legend.style.display, '');
});

test('vendor responses are race-safe and show readable API or network errors', async () => {
  const { s, nodes, requests } = fixture('vendors'); s.renderVendors(); nodes.vsource_loan.onclick.call(nodes.vsource_loan);
  requests[1].resolve(lenderResponse); await flush(); const displayed = nodes.vtable.innerHTML;
  requests[0].resolve({ success: true, vendors: [] }); await flush(); assert.equal(nodes.vtable.innerHTML, displayed);
  s.loadVendors(); requests[2].resolve({ success: false, error: 'You cannot view this entity.' }); await flush(); assert.equal(nodes.vtable.textContent, 'You cannot view this entity.');
  s.loadVendors(); requests[3].reject(new Error('Connection unavailable')); await flush(); assert.equal(nodes.vtable.textContent, 'Connection unavailable');
});

test('both modified inline and shared UI scripts remain valid JavaScript', () => {
  for (const script of page.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/gi)) if (script[1].trim()) assert.doesNotThrow(() => new vm.Script(script[1]));
  assert.doesNotThrow(() => new vm.Script(fs.readFileSync(path.join(__dirname, '../public/expense-loans.js'), 'utf8')));
});

test('a lender repayment opened directly from either filtered screen uses the linked loan endpoint', async () => {
  const nodes = {}, calls = []; let refreshes = 0;
  function node(tag) {
    const n = { style: {}, files: [], value: '', open: false, showModal() { this.open = true; }, close() { this.open = false; } };
    Object.defineProperty(n, 'innerHTML', { set(source) {
      for (const m of source.matchAll(/<(?:input|select|textarea|button|p|div)\b[^>]*\bid="([^"]+)"[^>]*>/g)) {
        const child = { value: (m[0].match(/\bvalue="([^"]*)"/) || [])[1] || '', files: [], disabled: false }; nodes[m[1]] = child;
        if (m[0].startsWith('<select')) child.value = 'Axis Bank 3448';
      }
    } }); return n;
  }
  const sandbox = { module: { exports: {} }, Date, crypto: { randomUUID: () => 'demo-repayment-request' },
    document: { getElementById: id => nodes[id] || null, createElement: node, body: { appendChild(n) { nodes[n.id] = n; } } } };
  vm.runInNewContext(fs.readFileSync(path.join(__dirname, '../public/expense-loans.js'), 'utf8'), sandbox);
  const ui = sandbox.module.exports, loan = { id: 'REC-LINKED', source: 'Example Lender', nature: 'SANKI', date: '2026-09-01', outstanding: 400000 };
  const button = { dataset: { loanRepay: loan.id } }, host = { querySelectorAll: () => [button] };
  ui.bindParties(host, { loans: [loan] }, { nature: () => 'SANKI', accountsForNature: () => ['Axis Bank 3448'],
    uploadFiles: async () => ['/proof.jpg'], api: async (url, options) => { calls.push({ url, body: JSON.parse(options.body) }); return { success: true }; }, refreshAccounts() { refreshes++; } });
  button.onclick(); assert.equal(nodes.loanRepaymentDlg.open, true); assert.equal(nodes.loan_pay_amount.value, '400000');
  nodes.loan_pay_amount.value = '100000'; nodes.loan_pay_note.value = 'Partial principal repayment'; nodes.loan_pay_reference.value = 'REF-LOAN-PAY'; nodes.loan_pay_proofs.files = [{}];
  await nodes.loan_pay_save.onclick.call(nodes.loan_pay_save);
  assert.equal(calls.length, 1); assert.equal(calls[0].url, '/api/expenses/loans/REC-LINKED/repay'); assert.equal(calls[0].body.amount, 100000);
  assert.equal(calls[0].body.account, 'Axis Bank 3448'); assert.equal(calls[0].body.reference, 'REF-LOAN-PAY'); assert.equal(nodes.loanRepaymentDlg.open, false); assert.equal(refreshes, 1);
});

'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const ui = require('../public/reconciliation-status');
test('detailed statements render the same completed and pending Paytm row labels for Owner and Admin', () => {
  const html = fs.readFileSync(path.join(__dirname, '../public/expenses.html'), 'utf8');
  const source = html.split('\n').find(line => line.includes('summary=x.paytmSummary') && line.includes('rows=d.entries.map'));
  assert.ok(source, 'Detailed statement row builder is present');
  const entries = ['posted', 'report_pending'].map((code, index) => ({ id: 'TEST-' + index, kind: 'paytm_customer_receipt', date: '2026-10-13', credit: 100, balance: 100, paytmReconciliation: { code, label: code === 'posted' ? 'Verified report and bank posted' : 'Awaiting verified report', reasons: code === 'posted' ? [] : ['Original report evidence is required'] } }));
  const before = JSON.stringify(entries);
  const render = isOwner => {
    const context = { d: { entries }, cfg: { isOwner, isAdmin: true }, account: 'Paytm Settlement Clearing', ledgerDisplayParts: () => ({ entity: 'SANKI', vendor: 'Sale', particulars: 'Payment' }), entryDetails: () => '', entryEditButton: () => '', esc: String, fmt: String, ledgerReconciliationLabel: row => ui.paytmLedgerRow(row.paytmReconciliation) };
    vm.runInNewContext(source, context);
    return context.rows;
  };
  const admin = render(false);
  assert.equal(render(true), admin);
  assert.match(admin, /data-paytm-status="posted"/);
  assert.match(admin, /data-paytm-status="report_pending"/);
  assert.match(admin, /Original report evidence is required/);
  assert.equal(JSON.stringify(entries), before);
});

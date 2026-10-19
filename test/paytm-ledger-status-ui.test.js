'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const crypto = require('node:crypto');
const ui = require('../public/reconciliation-status');
const root = path.join(__dirname, '..');
const expenses = fs.readFileSync(path.join(root, 'public/expenses.html'), 'utf8');
const dashboard = fs.readFileSync(path.join(root, 'public/dashboard.html'), 'utf8');
const esc = value => String(value == null ? '' : value).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
const coverage = () => ({ reportPaymentsThrough: '2026-10-13', reportSettlementsThrough: '2026-10-14', importedPaymentsThrough: '2026-10-13', importedSettlementsThrough: '2026-10-14', bankSettlementsThrough: '2026-10-18', pendingBankOnlySettlements: 2, pendingReceipts: 4, pendingSettlements: 0, firstPendingSettlementDate: '2026-10-17', lastVerified: { at: '2026-10-19T09:14:00Z', by: 'prashant' }, asOf: '2026-10-19' });
function between(source, first, next) {
  const start = source.indexOf(first), end = source.indexOf(next, start);
  assert.ok(start >= 0 && end > start, first);
  return source.slice(start, end);
}

test('coverage distinguishes imported, fully verified and bank-only dates without claiming blanket reconciliation', () => {
  const c = coverage(), before = JSON.stringify(c), html = ui.paytmCoverage(c);
  assert.match(html, /Imported payment report through <b>2026-10-13/);
  assert.match(html, /Imported report settlements through <b>2026-10-14/);
  assert.match(html, /Payments reconciled through <b>2026-10-13/);
  assert.match(html, /Report settlements verified and bank-posted through <b>2026-10-14/);
  assert.match(html, /Bank settlements seen through <b>2026-10-18/);
  assert.match(html, /2 bank settlement\(s\) awaiting Paytm report details/);
  assert.match(html, /4 receipt\(s\) awaiting verification/);
  assert.match(html, /Earliest pending settlement: 2026-10-17/);
  assert.doesNotMatch(html, /Reconciled through:.*2026-10-18|Last finalized/);
  assert.equal(JSON.stringify(c), before, 'rendering is read-only');
  assert.equal(ui.paytmCoverage({ ...c, isOwner: true }), ui.paytmCoverage({ ...c, isOwner: false }));
});

test('unknown coverage stays unavailable rather than falling back to a misleading generic bank cutoff', () => {
  assert.match(ui.paytmCoverage(), /coverage is unavailable/);
  const html = ui.paytmCoverage({});
  assert.match(html, /Payments reconciled through <b>Not yet/);
  assert.doesNotMatch(html, /Reconciled through:|Last finalized/);
});

test('overall clearing gaps are explicit and do not erase completed imported report dates', () => {
  const html = ui.paytmCoverage({ ...coverage(), overallReconciledThrough: '', firstPendingReceiptDate: '2026-08-31', firstPendingBankOnlyDate: '2026-08-22' });
  assert.match(html, /Report settlements verified and bank-posted through <b>2026-10-14/);
  assert.match(html, /Overall clearing reconciliation through Not yet/);
  assert.match(html, /Receipt allocation review starts 2026-08-31/);
  assert.match(html, /Bank-only history awaiting report details starts 2026-08-22/);
  assert.equal(ui.paytmCoverage({ ...coverage(), overallReconciledThrough: '', isOwner: true }), ui.paytmCoverage({ ...coverage(), overallReconciledThrough: '', isOwner: false }));
});

test('Paytm row states preserve pending reasons, identifiers and role parity', () => {
  for (const code of ['posted', 'finalized', 'ready', 'incomplete', 'report_pending', 'bank_confirmed_report_pending', 'summary']) {
    const s = { code, label: 'Evidence ' + code, reasons: ['Original report reason'], bankReasons: ['Exact bank evidence reason'], transactionIds: ['TX-1'], settlementIds: ['PB-1'], settledDate: '2026-10-14', finalizedAt: '2026-10-19T09:14:00Z', finalizedBy: 'prashant' };
    const before = JSON.stringify(s), html = ui.paytmLedgerRow(s);
    assert.match(html, new RegExp('data-paytm-status="' + code + '"'));
    assert.match(html, /Original report reason/);
    assert.match(html, /Bank link: Exact bank evidence reason/);
    assert.match(html, /Settlement evidence: PB-1/);
    assert.match(html, /Paytm transaction: TX-1/);
    assert.doesNotMatch(html, /Not reconciled/);
    assert.equal(ui.paytmLedgerRow({ ...s, isOwner: true }), ui.paytmLedgerRow({ ...s, isOwner: false }));
    assert.equal(JSON.stringify(s), before);
  }
});

test('coverage and row rendering escape every source-provided field', () => {
  const bad = '<img src=x onerror=alert(1)>', c = coverage();
  for (const name of ['reportPaymentsThrough', 'reportSettlementsThrough', 'importedPaymentsThrough', 'importedSettlementsThrough', 'bankSettlementsThrough', 'firstPendingSettlementDate', 'asOf']) c[name] = bad;
  c.lastVerified = { by: bad, at: bad };
  const row = { code: bad, label: bad, reasons: [bad], bankReasons: [bad], transactionIds: [bad], settlementIds: [bad], settledDate: bad, finalizedAt: bad, finalizedBy: bad };
  for (const html of [ui.paytmCoverage(c), ui.paytmLedgerRow(row)]) {
    assert.doesNotMatch(html, /<img|<script/);
    assert.match(html, /&lt;img/);
  }
});

test('Paytm report panel uses the same coverage projection as both account ledger views', () => {
  const html = ui.paytm({ paytmCoverage: coverage(), reconciliationStatus: { counts: { total: 39, posted: 39, incomplete: 0 }, incompleteSettlements: [] } });
  assert.ok(html.includes(ui.paytmCoverage(coverage())));
  assert.match(html, /No incomplete settlements remain within the imported Paytm report/);
  assert.match(html, /Bank-only settlements awaiting report details are shown separately/);
  assert.equal((expenses.match(/ReconciliationStatus\.paytmCoverage\(d\.paytmCoverage\)/g) || []).length, 2, 'Money Trail and Detailed Statement');
});

test('Paytm row status takes precedence while unrelated generic bank statuses remain unchanged', () => {
  const source = expenses.split('\n').find(line => line.includes('function ledgerReconciliationLabel('));
  const context = vm.createContext({ ReconciliationStatus: ui, esc });
  vm.runInContext(source, context);
  const s = { code: 'bank_confirmed_report_pending', label: 'Bank received; Paytm report details pending', reasons: ['Gross and fees not supplied'] };
  assert.equal(context.ledgerReconciliationLabel({ paytmReconciliation: s, reconciliation: { bankDate: '2026-10-18', reconciliationId: 'BANK' } }), ui.paytmLedgerRow(s));
  assert.equal(context.ledgerReconciliationLabel({}), '<div class="muted">Not reconciled</div>');
  const bank = context.ledgerReconciliationLabel({ reconciliation: { sourceAccountConfirmed: true, sourceAccount: 'Axis 3448', bankDate: '2026-10-18', reconciliationId: 'BST-1', bankReference: 'REF-1' } });
  assert.match(bank, /✓ Source-bank confirmed/);
  assert.match(bank, /Axis 3448 · BST-1 · REF-1/);
});

test('Money Trail displays report coverage instead of the generic latest-bank header', async () => {
  const box = { innerHTML: '' }, elements = { trail: box, cf_from: { value: '' }, cf_to: { value: '2026-10-19' } };
  const context = { selectedNature: 'SANKI', cfg: { isOwner: false }, ReconciliationStatus: ui, el: id => elements[id], api: () => Promise.resolve({ success: true, entries: [], balance: 9474.39, reconciledThrough: '2026-10-18', lastReconciliation: { at: '2026-10-19', by: 'prashant' }, paytmCoverage: coverage() }), reconciliationAccountsForNature: () => ['Paytm Settlement Clearing'], bankAccountsForNature: () => [], fmt: String, fmtBank: String, esc, encodeURIComponent };
  context.window = context;
  context.reconciliationNatureForAccount = (_account, nature) => nature;
  vm.createContext(context);
  vm.runInContext(expenses.split('\n').find(line => line.includes('window.loadMoneyTrail=function(')), context);
  context.loadMoneyTrail('Paytm Settlement Clearing', 'trail');
  await new Promise(resolve => setImmediate(resolve));
  assert.ok(box.innerHTML.includes(ui.paytmCoverage(coverage())));
  assert.doesNotMatch(box.innerHTML, /Reconciled through:|Last finalized by/);
  assert.match(box.innerHTML, /Upload or reconcile statement/);
});

async function attention(kind) {
  const host = { innerHTML: '', className: '' }, detail = '2 bank-only settlements await report <details>; 39 imported settlements already posted.';
  const data = { success: true, reminders: [{ type: 'paytm', account: 'Paytm Settlement Clearing', status: 'review_pending', attentionRequired: true, settlementCounts: { incomplete: 0 }, detail, href: '/expenses.html?tab=cash', actionLabel: 'Paytm report details required' }] };
  const context = { cfg: { isAdmin: true }, esc, el: () => host, document: { getElementById: () => host }, api: () => Promise.resolve(data) };
  vm.createContext(context);
  if (kind === 'ledger') {
    vm.runInContext(between(expenses, 'function loadReconciliationAttention(', '// Same-page proof viewer'), context);
    context.loadReconciliationAttention();
    await Promise.resolve();
  } else {
    vm.runInContext(between(dashboard, 'function renderReconciliationAttention(', "      fetch('/api/auth/me'"), context);
    context.renderReconciliationAttention(data);
  }
  return host.innerHTML;
}
for (const kind of ['ledger', 'dashboard']) test(kind + ' attention preserves exact Paytm backend reason instead of saying zero items', async () => {
  const html = await attention(kind);
  assert.match(html, /2 bank-only settlements await report &lt;details&gt;/);
  assert.match(html, /39 imported settlements already posted/);
  assert.doesNotMatch(html, /0 item\(s\) need review|<details>/);
});

test('shared status script is content-versioned and all inline script changes parse', () => {
  const content = fs.readFileSync(path.join(root, 'public/reconciliation-status.js'), 'utf8').replace(/\r\n/g, '\n');
  const expected = crypto.createHash('sha256').update(content).digest('hex').slice(0, 12);
  const references = [...expenses.matchAll(/src="\/reconciliation-status\.js\?v=([^"]+)"/g)];
  assert.equal(references.length, 1);
  assert.equal(references[0][1], expected);
  for (const html of [expenses, dashboard]) for (const script of html.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/gi)) if (script[1].trim()) assert.doesNotThrow(() => new vm.Script(script[1]));
});

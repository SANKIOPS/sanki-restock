'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const status = require('../public/reconciliation-status');
const html = fs.readFileSync(path.join(__dirname, '../public/expenses.html'), 'utf8');
const start = html.indexOf("    var paytmReviewFilter="), end = html.indexOf('    function renderSalaryLedgers(', start);
assert.ok(start > 0 && end > start);
const code = html.slice(start, end);
const esc = value => String(value == null ? '' : value).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
const decode = value => String(value || '').replace(/&(amp|lt|gt|quot|#39);/g, (_, c) => ({ amp: '&', lt: '<', gt: '>', quot: '"', '#39': "'" }[c]));
const flush = () => new Promise(resolve => setImmediate(resolve));
function fixture() {
  const receipt = { transactionId: 'RECEIPT-123456', utr: 'UTR-REVIEW', payoutId: 'UTR-REVIEW', amount: 200, commission: 0, platformFee: 0, gst: 0, date: '2099-10-16', time: '2099-10-16 20:15', isCustomerPayment: true, transactionType: 'PAYMENT' };
  const refund = { ...receipt, transactionId: 'REFUND-654321', time: '2099-10-16 21:36', isCustomerPayment: false, transactionType: 'REFUND' };
  const payout = { payoutId: 'UTR-REVIEW', settlementId: 'SET-REVIEW', utr: 'UTR-REVIEW', settledDate: '2099-10-17', customerPaymentCount: 1, customerGross: 200, gross: 200, commission: 1, platformFee: 2, gst: 3, nonCustomerAmount: 4, refundAmount: 200, net: 8116, transactionIds: [receipt.transactionId, refund.transactionId], bankCandidates: [], reconciliationStatus: { code: 'incomplete', label: 'Incomplete — review required', reasons: ['Unresolved refund requires evidence review.'], bankReasons: ['This bank credit was previously finalized.'] } };
  return { from: '2099-08-22', through: '2099-10-19', imports: [], transactions: [receipt, refund], orderMatches: [{ transactionId: receipt.transactionId, exclusion: { reason: 'Money received accidentally and returned in full' } }], payouts: [payout], reportPreviews: [{ id: 'PREVIEW-OLD', actionable: false, reasons: ['OLD-TECHNICAL-WARNING'], sources: [] }], reconciliationStatus: { counts: { total: 1, incomplete: 1, posted: 0 }, incompleteSettlements: [{ ...payout.reconciliationStatus, ...payout, reasons: payout.reconciliationStatus.reasons, bankReasons: [], blockedTransactions: [] }] }, returnReviews: { candidates: [{ receiptTransactionId: receipt.transactionId, refundTransactionId: refund.transactionId, settlementId: 'SET-REVIEW', amount: 200, receiptDate: receipt.date, receiptTime: receipt.time, refundDate: refund.date, refundTime: refund.time, exclusionReason: 'Accidental payment returned' }], resolutions: [] } };
}
function harness(data = fixture(), options = {}) {
  const calls = [], confirmations = [], globals = { ptMessage: { textContent: '' }, ptFinalizeAll: { disabled: false } };
  let context, attention = 0, response = options.response || { success: true, link: { historicalSettlementId: 'BANK-EXISTING' }, view: { ...data, returnReviews: { candidates: [], resolutions: [] } } };
  const host = {
    dataset: {}, className: '', nodes: [], value: '',
    set innerHTML(value) {
      this.value = value; this.nodes = [];
      for (const match of value.matchAll(/<([a-z][\w-]*)([^>]*)>/gi)) {
        const attrs = {};
        for (const attr of match[2].matchAll(/([:\w-]+)(?:="([^"]*)")?/g)) attrs[attr[1]] = decode(attr[2] == null ? '' : attr[2]);
        const node = { tagName: match[1].toUpperCase(), attrs, value: attrs.value || '', checked: Object.hasOwn(attrs, 'checked'), hidden: Object.hasOwn(attrs, 'hidden'), open: Object.hasOwn(attrs, 'open'), disabled: false, textContent: '', parentNode: { insertBefore() {} }, focus() { this.focused = true; }, getAttribute(name) { return Object.hasOwn(attrs, name) ? attrs[name] : null; }, querySelector: selector => host.querySelector(selector) };
        if (attrs.id === 'ptReviewFilter') node.value = context.paytmReviewFilter;
        if (attrs.id === 'ptReviewSearch') node.value = context.paytmReviewSearch;
        this.nodes.push(node);
      }
    },
    get innerHTML() { return this.value; },
    querySelectorAll(selector) {
      if (selector.startsWith('#')) return this.nodes.filter(n => n.attrs.id === selector.slice(1));
      const match = selector.match(/^\[([^=\]]+)(?:="([^"]*)")?\](:checked)?$/);
      if (!match) return [];
      return this.nodes.filter(n => Object.hasOwn(n.attrs, match[1]) && (match[2] === undefined || n.attrs[match[1]] === match[2]) && (!match[3] || n.checked));
    },
    querySelector(selector) { return this.querySelectorAll(selector)[0] || null; }
  };
  context = vm.createContext({ cfg: { isOwner: options.owner !== false }, paytmStatusGeneration: 0, el: id => id === 'ptSaved' ? host : globals[id] || null, esc, fmt: value => '₹' + Number(value || 0), ReconciliationStatus: status, document: { createElement: () => ({ innerHTML: '', className: '' }) }, prompt: () => options.prompt === undefined ? 'Verified corrected pairing' : options.prompt,
    confirmPaytmReview(message, callback) { confirmations.push({ message, callback }); },
    api(url, request) { calls.push({ url, body: JSON.parse(request.body) }); return response instanceof Error ? Promise.reject(response) : Promise.resolve(response); },
    loadReconciliationAttention() { attention++; }, loadAccountLedger() {}, showPaytmUploadPreview() {} });
  vm.runInContext(code, context);
  context.showPaytmSaved(data);
  return { context, host, globals, calls, confirmations, setResponse(value) { response = value; }, attention: () => attention };
}

test('default review view keeps customer, technical and old-preview evidence collapsed, with completed payouts hidden not deleted', () => {
  const data = fixture();
  for (let i = 1; i <= 41; i++) data.payouts.push({ ...data.payouts[0], payoutId: 'OLD-' + i, settlementId: 'OLD-' + i, utr: 'OLD-' + i, transactionIds: [], posted: true, reconciliationStatus: { code: 'posted', reasons: [], bankReasons: [] } });
  const ui = harness(data), rows = ui.host.querySelectorAll('[data-pt-summary="true"]');
  assert.equal(rows.length, 42);
  assert.equal(rows.filter(row => !row.hidden).length, 1);
  assert.equal(rows.find(row => !row.hidden).getAttribute('data-pt-payout'), 'UTR-REVIEW');
  assert.ok(ui.host.querySelectorAll('[data-pt-details]').every(node => !node.open));
  assert.equal(ui.host.querySelector('#ptSharedEvidence').open, false);
  assert.match(ui.host.innerHTML, /OLD-TECHNICAL-WARNING/);
  assert.match(ui.host.innerHTML, /Confirm receipt \/ refund pair/);
  assert.match(ui.host.innerHTML, /Confirm the accidental receipt\/refund pair; the existing bank reconciliation will be retained/);
  assert.match(ui.host.innerHTML, /Refunds · not fees/);
  assert.match(ui.host.innerHTML, /VAS \/ other deductions ₹4 · refunds ₹200/);
  assert.equal(ui.globals.ptFinalizeAll.disabled, true);
  assert.equal(ui.calls.length, 0);
  assert.match(html, /#ptSaved \[data-pt-payout\]\[hidden\]\s*\{\s*display:none !important;/, 'mobile display:block table rules cannot unhide completed batches');
});

test('filters and UTR/order searches make no writes and respect server incomplete status over a stale raw posted flag', () => {
  const data = fixture();
  data.payouts[0].posted = true;
  data.payouts.push({ ...data.payouts[0], payoutId: 'DONE', settlementId: 'DONE', utr: 'DONE', transactionIds: [], reconciliationStatus: { code: 'posted', reasons: [], bankReasons: [] } });
  data.orderMatches[0].orderCandidates = [{ number: '9876', customer: 'Example Buyer', total: 200 }];
  const ui = harness(data), filter = ui.host.querySelector('#ptReviewFilter'), search = ui.host.querySelector('#ptReviewSearch');
  assert.equal(ui.host.querySelectorAll('[data-pt-summary="true"]').filter(row => !row.hidden).length, 1);
  filter.value = 'all'; filter.onchange();
  search.value = '9876'; search.oninput();
  assert.equal(ui.host.querySelectorAll('[data-pt-summary="true"]').filter(row => !row.hidden).length, 1);
  search.value = ''; search.oninput(); filter.value = 'completed'; filter.onchange();
  assert.equal(ui.host.querySelectorAll('[data-pt-summary="true"]').find(row => !row.hidden).getAttribute('data-pt-payout'), 'DONE');
  assert.equal(ui.calls.length, 0);
  ui.context.showPaytmSaved(data);
  assert.equal(ui.host.querySelector('#ptReviewFilter').value, 'completed', 'refresh preserves the read-only filter');
});

test('an exact receipt/refund pair is never preselected and requires selection, meaningful reason and explicit confirmation', () => {
  const ui = harness(), button = ui.host.querySelector('[data-pt-return]'), radio = ui.host.querySelector('[data-pt-return-choice]'), reason = ui.host.querySelector('[data-pt-return-reason]'), error = ui.host.querySelector('[data-pt-return-error]');
  assert.equal(radio.checked, false);
  button.onclick(); assert.match(error.textContent, /Select the exact/); assert.equal(ui.calls.length, 0);
  radio.checked = true; reason.value = 'short'; button.onclick();
  assert.match(error.textContent, /10 characters/); assert.equal(ui.confirmations.length, 0);
  reason.value = 'Verified accidental payment returned'; button.onclick();
  assert.equal(ui.calls.length, 0, 'opening confirmation is read-only; cancellation means no request');
  assert.equal(ui.confirmations.length, 1);
  assert.match(ui.confirmations[0].message, /RECEIPT-123456.*REFUND-654321.*₹200 each.*UTR-REVIEW/);
  assert.match(ui.confirmations[0].message, /Attaching finalized bank evidence is a separate reviewed step/);
});

test('confirmed pairing submits one exact evidence decision and never posts or attaches a bank settlement automatically', async () => {
  const ui = harness();
  ui.host.querySelector('[data-pt-return-choice]').checked = true;
  ui.host.querySelector('[data-pt-return-reason]').value = 'Verified accidental payment returned';
  ui.host.querySelector('[data-pt-return]').onclick();
  const confirm = ui.confirmations[0].callback; confirm(); confirm();
  await flush();
  assert.deepEqual(ui.calls, [{ url: '/api/expenses/paytm-reports/resolve-return', body: { receiptTransactionId: 'RECEIPT-123456', refundTransactionId: 'REFUND-654321', reason: 'Verified accidental payment returned', confirmed: true } }]);
  assert.match(ui.globals.ptMessage.textContent, /No bank or accounting posting created/);
  assert.equal(ui.attention(), 1);
});

test('pairing conflicts and network errors remain visible and safely allow retry', async () => {
  for (const response of [{ success: false, error: 'The imported receipt evidence changed.' }, new Error('Network unavailable')]) {
    const ui = harness(fixture(), { response });
    ui.host.querySelector('[data-pt-return-choice]').checked = true;
    ui.host.querySelector('[data-pt-return-reason]').value = 'Verified accidental payment returned';
    const button = ui.host.querySelector('[data-pt-return]'), error = ui.host.querySelector('[data-pt-return-error]');
    button.onclick(); ui.confirmations[0].callback(); await flush();
    assert.equal(button.disabled, false); assert.ok(error.textContent); assert.equal(ui.attention(), 0);
  }
});

test('a resolved pair locks Shopify mutations and offers only independently validated attachment to existing finalized evidence', async () => {
  const data = fixture(); data.returnReviews.candidates = [];
  data.returnReviews.resolutions = [{ receiptTransactionId: 'RECEIPT-123456', refundTransactionId: 'REFUND-654321', settlementId: 'SET-REVIEW', amount: 200, valid: true, canReverse: false, reverseBlockedReason: 'Already attached to finalized evidence.' }];
  data.payouts[0].reconciliationStatus = { code: 'ready', label: 'Ready', reasons: [], bankReasons: [], historicalCandidate: { historicalSettlementId: 'BANK-EXISTING', completePendingDetail: false } };
  const ui = harness(data);
  for (const selector of ['[data-pt-restore]', '[data-pt-link]', '[data-pt-exclude]', '[data-pt-classify]', '[data-pt-return-reverse]', '[data-pt-post]']) assert.equal(ui.host.querySelector(selector), null, selector);
  assert.equal(ui.host.querySelectorAll('[data-pt-history]').length, 1);
  ui.host.querySelector('[data-pt-history-reason]').value = 'Exact original bank evidence verified';
  ui.host.querySelector('[data-pt-history]').onclick();
  assert.equal(ui.calls.length, 0);
  assert.match(ui.confirmations[0].message, /No new bank, fee or income posting/);
  ui.confirmations[0].callback(); await flush();
  assert.equal(ui.calls[0].url, '/api/expenses/paytm-reports/attach-historical');
  assert.deepEqual(ui.calls[0].body, { payoutId: 'UTR-REVIEW', reason: 'Exact original bank evidence verified' });
});

test('a reversible return review correction requires reason and confirmation, preserving exclusion and audit history', async () => {
  const data = fixture(); data.returnReviews.candidates = [];
  data.returnReviews.resolutions = [{ receiptTransactionId: 'RECEIPT-123456', refundTransactionId: 'REFUND-654321', settlementId: 'SET-REVIEW', amount: 200, valid: true, canReverse: true }];
  const ui = harness(data);
  ui.host.querySelector('[data-pt-return-reverse]').onclick();
  assert.equal(ui.calls.length, 0);
  assert.match(ui.confirmations[0].message, /original exclusion stays and no bank reconciliation or accounting entry changes/);
  ui.confirmations[0].callback(); await flush();
  assert.deepEqual(ui.calls[0], { url: '/api/expenses/paytm-reports/reverse-return', body: { receiptTransactionId: 'RECEIPT-123456', reason: 'Verified corrected pairing' } });
});

test('manual classification remains Owner-only and rounding / legacy linking handlers remain available under details', () => {
  const data = fixture(); data.orderMatches[0].exclusion = null; data.returnReviews.candidates = [];
  const owner = harness(data), admin = harness(data, { owner: false });
  assert.ok(owner.host.querySelector('[data-pt-classify]'));
  assert.equal(admin.host.querySelector('[data-pt-classify]'), null);
  assert.ok(admin.host.querySelector('[data-pt-link]'));
  assert.ok(admin.host.querySelector('[data-pt-sync]'));
  assert.match(code, /querySelectorAll\('\[data-pt-rounding\]'\)/);
  assert.match(code, /paytmRow\(p\.reconciliationStatus\)/);
  for (const [, script] of html.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/g)) if (script.trim()) assert.doesNotThrow(() => new Function(script));
});

test('read-only Paytm filters do not mark evidence dirty, while all actual review inputs keep refresh protection', () => {
  const listeners = {}, root = { dataset: {} };
  const from = html.indexOf('    function initializeReconciliationStatusRefresh('), to = html.indexOf('    function renderPaytmReports(', from);
  const context = vm.createContext({ sharedReconciliationRefresh: null, tab: 'cash', ReconciliationStatus: { createRefreshController: () => ({ run() {} }) }, document: { addEventListener(event, fn) { listeners[event] = fn; } }, window: { addEventListener() {}, setInterval() {} }, refreshSharedReconciliationStatus() {}, reconciliationStatusBusy() {}, el: () => null });
  vm.runInContext(html.slice(from, to), context);
  context.initializeReconciliationStatusRefresh();
  for (const event of ['input', 'change']) {
    for (const id of ['ptReviewFilter', 'ptReviewSearch']) listeners[event]({ target: { id, closest: () => root } });
    assert.equal(root.dataset.reconciliationDirty, undefined);
    listeners[event]({ target: { id: 'pairReason', closest: () => root } });
    assert.equal(root.dataset.reconciliationDirty, 'true');
    delete root.dataset.reconciliationDirty;
  }
});

'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const review = require('../public/credit-card-review');
const html = fs.readFileSync(path.join(__dirname, '../public/expenses.html'), 'utf8');
const fmt = amount => '₹' + amount;
const statement = { id: 'CCS-TEST', status: 'review', reviewRevision: 4, refundReceipts: [{ id: 'RCPT-REFUND' }] };
const source = { id: 'payment:CCP-1', label: 'Existing card payment CCP-1', date: '2026-08-27', amount: 500, account: 'Axis Bank 3448', nature: 'SANKI', reference: 'REF-1', fingerprint: 'source-fingerprint', eligible: true, bankMovementRecorded: true, status: 'Existing recorded settlement; bank verification is separate' };
function payment(extra = {}) { return { id: 'ROW-1', date: '2026-08-27', narration: 'PAYMENT RECEIVED THANK YOU', merchant: 'ICICI Bank', credit: 500, debit: 0, classification: 'card_payment', category: 'Must not submit', type: 'variable', nature: 'SANKI', channel: 'Both', settlementCandidates: [source], ...extra }; }
function tr(values = {}, draft) { const fields = Object.fromEntries(Object.entries(values).map(([key, value]) => [key, { value }])); return { dataset: { id: 'ROW-1' }, _ccReviewDraft: draft, querySelector: selector => fields[selector] || null, fields }; }
function render(row, status = 'review') { return review.rowCells(row, 0, { ...statement, status }, ['SANKI', 'SAMAST', 'PERSONAL'], fmt); }
function domRow(row) {
  const node = { dataset: { id: row.id }, fields: {}, querySelector(selector) { return this.fields[selector] || null; } };
  let markup = '';
  const decode = text => String(text).replace(/&(amp|lt|gt|quot|#39);/g, (_, c) => ({ amp: '&', lt: '<', gt: '>', quot: '"', '#39': "'" }[c]));
  Object.defineProperty(node, 'innerHTML', { get() { return markup; }, set(value) {
    markup = value; this.fields = {};
    for (const match of value.matchAll(/<input\b([^>]*)>/g)) { const cls = match[1].match(/class="([^"]+)"/); if (cls) this.fields['.' + cls[1]] = { value: decode((match[1].match(/value="([^"]*)"/) || [])[1] || '') }; }
    for (const match of value.matchAll(/<select\b([^>]*)>([\s\S]*?)<\/select>/g)) { const cls = match[1].match(/class="([^"]+)"/); if (cls) { const options = [...match[2].matchAll(/<option\b([^>]*)>/g)], chosen = options.find(x => /\bselected\b/.test(x[1])) || options[0]; this.fields['.' + cls[1]] = { value: chosen ? decode((chosen[1].match(/value="([^"]*)"/) || [])[1] || '') : '' }; } }
  } });
  node.innerHTML = render(row); return node;
}

test('new credit-card review copy retains the Results label across helper, fields and confirmations', () => {
  const helper = fs.readFileSync(path.join(__dirname, '../public/credit-card-review.js'), 'utf8');
  const from = html.indexOf('// ── CREDIT CARDS:'), to = html.indexOf('    var modelCalendarContractId', from);
  assert.ok(from > 0 && to > from);
  assert.doesNotMatch(helper, /P(?:&|&amp;)L/);
  assert.doesNotMatch(html.slice(from, to), /P(?:&|&amp;)L/);
  assert.match(helper, /does not reduce expenses or enter Results/);
  assert.match(html.slice(from, to), /Genuine expenses\/refunds affect Results/);
});

test('repayment controls show liability treatment, no refund or expense inputs and default pending without selecting a source', () => {
  const output = render(payment());
  assert.match(output, /Liability settlement only/);
  assert.match(output, /does not reduce expenses or enter Results/);
  assert.match(output, /value="pending" selected/);
  assert.doesNotMatch(output, /class="cc_(?:type|category|nature|channel|refund_link|refund_reason)"/);
  assert.doesNotMatch(output, /class="cc_settlement_source"/);
  assert.match(output, /Finalization is blocked/);
});

test('link controls show amount, date, account, entity, reference and exact blocked reasons without auto-selection', () => {
  const blocked = { ...source, id: 'transfer:TR-2', label: 'Transfer TR-2', eligible: false, blockedReason: 'Already allocated to another statement' };
  const output = render(payment({ settlementMode: 'link_existing', settlementCandidates: [source, blocked] }));
  assert.match(output, /payment:CCP-1/);
  assert.match(output, /2026-08-27.*₹500.*Axis Bank 3448/);
  assert.match(output, /value="transfer:TR-2" disabled/);
  assert.match(output, /Already allocated to another statement/);
  assert.doesNotMatch(output, /value="payment:CCP-1" selected/);
  const selected = render(payment({ settlementMode: 'link_existing', settlementSourceId: source.id }));
  assert.match(selected, /Entity: SANKI/);
  assert.match(selected, /Reference: REF-1/);
  assert.match(selected, /no second bank entry or expense offset/);
  assert.match(selected, /Source status: Existing recorded settlement; bank verification is separate/);
  assert.match(selected, /Existing bank movement recorded/);
  assert.match(selected, /bank-statement reconciliation is a separate check/);
  const historical = render(payment({ settlementMode: 'link_existing', settlementSourceId: source.id, settlementCandidates: [{ ...source, bankMovementRecorded: false }] }));
  assert.match(historical, /Bank movement is not verified by this source/);
});

test('link payload binds exact eligible fingerprint and never submits hidden legacy expense attributes', () => {
  const row = payment({ settlementMode: 'link_existing' });
  const fields = tr({ '.cc_class': 'card_payment', '.cc_merchant': 'ICICI', '.cc_settlement_mode': 'link_existing', '.cc_settlement_source': source.id, '.cc_settlement_reason': 'Payment posted the following day', '.cc_category': 'Bogus', '.cc_nature': 'SANKI' });
  assert.deepEqual(review.collectRow(fields, row), { id: 'ROW-1', merchant: 'ICICI', classification: 'card_payment', settlementMode: 'link_existing', settlementSourceId: source.id, settlementSourceFingerprint: source.fingerprint, settlementReason: 'Payment posted the following day' });
  row.settlementCandidates[0] = { ...source, eligible: false };
  assert.equal(review.collectRow(fields, row).settlementSourceFingerprint, '', 'an unavailable source cannot acquire an eligible fingerprint');
});

test('a changed saved source fingerprint is not silently refreshed; explicit re-selection is required', () => {
  const row = payment({ settlementMode: 'link_existing', settlementSourceId: source.id, settlementSourceFingerprint: 'previous-evidence' });
  const current = domRow(row);
  review.bindRow(current, row, 0, statement, ['SANKI'], fmt);
  assert.match(current.innerHTML, /Source evidence changed or was not confirmed/);
  assert.equal(review.collectRow(current, current._ccReviewDraft).settlementSourceFingerprint, 'previous-evidence');
  current.fields['.cc_settlement_source'].value = ''; current.fields['.cc_settlement_source'].onchange();
  current.fields['.cc_settlement_source'].value = source.id; current.fields['.cc_settlement_source'].onchange();
  assert.equal(review.collectRow(current, current._ccReviewDraft).settlementSourceFingerprint, source.fingerprint);
});

test('external historical settlement has explicit untracked warning, no default entity and requires source evidence fields', () => {
  const output = render(payment({ settlementMode: 'external', payingNature: '' }));
  assert.match(output, /External \/ historical source — no bank entry created/);
  assert.match(output, /source not posted in this app/);
  assert.match(output, /Existing matching payments must be linked instead/);
  assert.match(output, /Choose the actual payer/);
  assert.doesNotMatch(output, /value="SANKI" selected/);
  const fields = tr({ '.cc_class': 'card_payment', '.cc_merchant': 'Bank', '.cc_settlement_mode': 'external', '.cc_paying_nature': 'PERSONAL', '.cc_paying_account': 'External family account', '.cc_settlement_reference': 'ABC-123', '.cc_settlement_reason': 'Paid historically from outside this app' });
  assert.deepEqual(review.collectRow(fields, payment()), { id: 'ROW-1', merchant: 'Bank', classification: 'card_payment', settlementMode: 'external', payingNature: 'PERSONAL', payingAccount: 'External family account', settlementReference: 'ABC-123', settlementReason: 'Paid historically from outside this app' });
});

test('only genuine refunds display existing refund links and expense classifications', () => {
  const output = render(payment({ classification: 'refund', expenseRefundReceiptId: 'RCPT-REFUND' }));
  assert.match(output, /Already logged expense refund/);
  assert.match(output, /value="RCPT-REFUND" selected/);
  assert.match(output, /class="cc_category"/);
  assert.doesNotMatch(output, /class="cc_settlement_mode"/);
  const fields = tr({ '.cc_class': 'refund', '.cc_merchant': 'LinkedIn', '.cc_type': 'marketing', '.cc_category': 'Subscriptions', '.cc_nature': 'SANKI', '.cc_channel': 'Both', '.cc_refund_link': 'separate', '.cc_refund_reason': 'This is a different refund' });
  assert.deepEqual(review.collectRow(fields, payment()), { id: 'ROW-1', merchant: 'LinkedIn', classification: 'refund', type: 'marketing', category: 'Subscriptions', nature: 'SANKI', channel: 'Both', expenseRefundReceiptId: '', refundLinkDecision: 'separate', refundLinkReason: 'This is a different refund' });
});

test('unknown credits stay unclassified and have neither expense offset fields nor a silently selected refund', () => {
  const row = payment({ classification: 'mystery_credit' });
  assert.equal(review.classification(row), 'unclassified_credit');
  const output = render(row);
  assert.match(output, /value="unclassified_credit" selected/);
  assert.match(output, /Other credits require a verified supported treatment/);
  assert.doesNotMatch(output, /class="cc_(?:type|category|nature|channel|refund_link|settlement_mode)"/);
  assert.deepEqual(review.collectRow(tr({ '.cc_class': 'unclassified_credit', '.cc_merchant': 'Unknown' }), row), { id: 'ROW-1', merchant: 'Unknown', classification: 'unclassified_credit' });
  assert.match(html, /<option value="unclassified_credit">Other \/ unconfirmed credit — pending/);
});

test('all supported debit choices retain ordinary classification fields and cannot select credit treatments', () => {
  for (const classification of ['expense', 'fee', 'emi_interest', 'emi_principal']) {
    const output = render(payment({ credit: 0, debit: 123, classification }));
    assert.match(output, /class="cc_category"/);
    assert.match(output, new RegExp('value="' + classification + '" selected'));
    assert.doesNotMatch(output, /value="(?:refund|card_payment|unclassified_credit)"/);
    assert.doesNotMatch(output, /class="cc_settlement_mode"/);
  }
});

test('evidence and labels escape untrusted source descriptions, unknown missing sources cannot silently link', () => {
  const output = render(payment({ settlementMode: 'link_existing', settlementSourceId: 'gone', settlementCandidates: [{ ...source, account: '<img src=x onerror=alert(1)>', label: 'Payment <script>bad()</script>' }] }));
  assert.match(output, /Previously selected source is no longer available/);
  assert.doesNotMatch(output, /<script>|<img /);
  assert.match(output, /&lt;script&gt;/);
});

test('finalized controls are read-only and render never saves, links or finalizes', () => {
  const output = render(payment({ settlementMode: 'link_existing', settlementSourceId: source.id }), 'finalized');
  assert.match(output, /class="cc_merchant"[^>]* disabled/);
  assert.match(output, /class="cc_settlement_source" disabled/);
  assert.doesNotMatch(output, /onclick=|fetch\(|api\(/);
});

test('legacy finalized repayments are described as historical settlements rather than falsely pending or unverified', () => {
  const output = render(payment({ settlementMode: undefined }), 'finalized');
  assert.match(output, /Historical statement settlement; no bank link recorded/);
  assert.doesNotMatch(output, /class="cc_settlement_mode"|Unconfirmed source — keep pending|Finalization is blocked/);
});

test('private settlement rows are immutable, preserve Owner review state, and are never submitted by a shared-card reviewer', () => {
  const row = payment({ settlementProtected: true, settlementProtectedMessage: 'Private settlement source — Owner review required', settlementCandidates: [] });
  const output = render(row);
  assert.match(output, /Private settlement source — Owner review required/);
  assert.match(output, /row is protected and unchanged/);
  assert.doesNotMatch(output, /<input|<select|value="pending"|Unconfirmed source/);
  assert.equal(review.collectRow(tr({ '.cc_class': 'refund', '.cc_merchant': 'Attempted change' }), row), null);
});

test('classification switching replaces only the selected row and preserves its unsaved expense fields in the local draft', () => {
  const row = payment({ classification: 'refund' }), current = domRow(row);
  for (const [key, value] of Object.entries({ '.cc_class': 'card_payment', '.cc_merchant': 'Edited merchant', '.cc_type': 'marketing', '.cc_category': 'Edited unsaved category', '.cc_nature': 'SAMAST', '.cc_channel': 'Website' })) current.fields[key].value = value;
  const other = tr({ '.cc_category': 'Other unsaved value' });
  let dirty = 0;
  review.bindRow(current, row, 0, statement, ['SANKI', 'SAMAST'], fmt, () => dirty++);
  current.fields['.cc_class'].onchange();
  assert.equal(current._ccReviewDraft.category, 'Edited unsaved category');
  assert.equal(current._ccReviewDraft.nature, 'SAMAST');
  assert.equal(current._ccReviewDraft.merchant, 'Edited merchant');
  assert.match(current.innerHTML, /Liability settlement only/);
  assert.equal(other.fields['.cc_category'].value, 'Other unsaved value');
  assert.equal(other.innerHTML, undefined);
  assert.equal(dirty, 1);
  assert.equal(current.fields['.cc_category'], undefined);
  current.fields['.cc_class'].value = 'refund'; current.fields['.cc_class'].onchange();
  assert.equal(current.fields['.cc_category'].value, 'Edited unsaved category', 'switching back restores the same row draft, not blank placeholders');
  assert.equal(current.fields['.cc_nature'].value, 'SAMAST');
  assert.equal(dirty, 2);
});

function requestHarness(responses) {
  const row = tr({ '.cc_class': 'card_payment', '.cc_merchant': 'ICICI', '.cc_settlement_mode': 'pending' }, payment());
  const button = { disabled: false, dataset: {} }, calls = [], messages = [];
  const ctx = vm.createContext({ CreditCardReview: review, ccReviewSubmitting: false, ccReviewId: statement.id, ccStatements: [{ ...statement, rows: [payment()] }], document: { querySelectorAll: selector => selector === '.ccrow' ? [row] : [button] }, confirm: () => true, encodeURIComponent, setMsg: (message, ok) => messages.push({ message, ok }), renderCreditCards: () => { ctx.renders++; }, renders: 0,
    api(url, options) { calls.push({ url, body: JSON.parse(options.body) }); const response = responses.shift(); return response instanceof Error ? Promise.reject(response) : response && typeof response.then === 'function' ? response : Promise.resolve(response); } });
  const start = html.indexOf('    function ccReviewRows('), end = html.indexOf('    window.openCreditCardReview=', start);
  vm.runInContext(html.slice(start, end), ctx);
  return { ctx, row, button, calls, messages };
}
const flush = async () => { for (let i = 0; i < 4; i++) await new Promise(resolve => setImmediate(resolve)); };

test('save includes exact review revision, prevents repeated requests, freezes and restores controls, and never finalizes', async () => {
  let resolve;
  const ui = requestHarness([new Promise(done => { resolve = done; })]);
  ui.ctx.saveCreditCardReview(statement.id); ui.ctx.saveCreditCardReview(statement.id);
  assert.equal(ui.calls.length, 1);
  assert.equal(ui.calls[0].body.reviewRevision, 4);
  assert.equal(ui.button.disabled, true);
  resolve({ success: true, statement: { ...statement, reviewRevision: 5 } }); await flush();
  assert.equal(ui.calls.length, 1);
  assert.equal(ui.ctx.renders, 1);
  assert.equal(ui.ctx.ccReviewSubmitting, false);
  assert.equal(ui.button.disabled, false);
});

test('failed save preserves unsaved form and does not redraw or finalize', async () => {
  const ui = requestHarness([{ success: false, error: 'Source changed. Refresh candidates.' }]);
  ui.ctx.saveCreditCardReview(statement.id); await flush();
  assert.equal(ui.calls.length, 1); assert.equal(ui.ctx.renders, 0);
  assert.equal(ui.row.fields['.cc_merchant'].value, 'ICICI');
  assert.match(ui.messages[0].message, /Source changed/);
  assert.equal(ui.button.disabled, false);
});

test('shared-card review payload omits protected settlement rows entirely', async () => {
  const ui = requestHarness([{ success: true, statement: { ...statement, reviewRevision: 5 } }]);
  ui.row._ccReviewDraft.settlementProtected = true;
  ui.ctx.saveCreditCardReview(statement.id); await flush();
  assert.deepEqual(ui.calls[0].body.rows, []);
});

test('explicit finalize sends the freshly saved numeric revision, blocks missing revision and preserves saved revision on finalization failure', async () => {
  const ui = requestHarness([{ success: true, statement: { ...statement, reviewRevision: 5 } }, { success: false, error: 'Unconfirmed source remains pending' }]);
  ui.ctx.finalizeCreditCardStatement(statement.id); await flush();
  assert.equal(ui.calls.length, 2);
  assert.equal(ui.calls[0].body.reviewRevision, 4); assert.equal(ui.calls[1].body.reviewRevision, 5);
  assert.equal(ui.ctx.ccStatements[0].reviewRevision, 5);
  assert.equal(ui.ctx.renders, 0);
  assert.match(ui.messages[0].message, /Unconfirmed source/);
  const missing = requestHarness([{ success: true, statement: {} }]);
  missing.ctx.finalizeCreditCardStatement(statement.id); await flush();
  assert.equal(missing.calls.length, 1);
  assert.match(missing.messages[0].message, /revision was not returned/);
});

test('cancelling final confirmation makes no review or finalization writes', async () => {
  const ui = requestHarness([]); ui.ctx.confirm = () => false;
  ui.ctx.finalizeCreditCardStatement(statement.id); await flush();
  assert.equal(ui.calls.length, 0); assert.equal(ui.button.disabled, false);
});

function paymentHarness(confirm = true) {
  const fields = { cc_pay_card: 'CC-1', cc_pay_nature: 'SANKI', cc_pay_account: 'Axis Bank 3448', cc_pay_kind: 'full', cc_pay_amount: '500', cc_pay_sanki: '500', cc_pay_samast: '0', cc_pay_personal: '0', cc_pay_date: '2026-08-27', cc_pay_ref: 'REF-123' };
  const elements = Object.fromEntries(Object.entries(fields).map(([key, value]) => [key, { value }])); elements.cc_pay_go = { disabled: false };
  const calls = [], confirmations = []; let resolve;
  const ctx = vm.createContext({ ccPaymentSubmitting: false, el: id => elements[id], fmt, confirm(message) { confirmations.push(message); return confirm; }, setMsg() {}, renderCreditCards() {}, api(url, options) { calls.push({ url, body: JSON.parse(options.body) }); return new Promise(done => { resolve = done; }); } });
  const start = html.indexOf('    function recordCreditCardPayment('), end = html.indexOf('    var modelCalendarContractId', start);
  vm.runInContext(html.slice(start, end), ctx);
  return { ctx, calls, confirmations, elements, resolve: result => resolve(result) };
}

test('normal new-payment form clearly warns to link existing payment instead and prevents double-submit', async () => {
  const ui = paymentHarness(); ui.ctx.recordCreditCardPayment(); ui.ctx.recordCreditCardPayment();
  assert.equal(ui.calls.length, 1);
  assert.match(ui.confirmations[0], /NEW bank-to-card payment.*already exists, cancel and link/);
  assert.equal(ui.elements.cc_pay_go.disabled, true);
  ui.resolve({ success: false, error: 'Existing payment found; link it' }); await flush();
  assert.equal(ui.elements.cc_pay_go.disabled, false);
  assert.equal(ui.elements.cc_pay_amount.value, '500');
});

test('cancelling new bank payment confirmation creates no bank or card entry', () => {
  const ui = paymentHarness(false); ui.ctx.recordCreditCardPayment();
  assert.equal(ui.calls.length, 0); assert.equal(ui.elements.cc_pay_go.disabled, false);
});

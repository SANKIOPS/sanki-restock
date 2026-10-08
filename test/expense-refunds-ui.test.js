'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname, '../public/expense-refunds.js'), 'utf8');
test('refund script URL changes with source content to invalidate cached releases', () => {
  const version = crypto.createHash('sha256').update(source.replace(/\r\n/g, '\n').trimEnd()).digest('hex').slice(0, 12);
  const html = fs.readFileSync(path.join(__dirname, '../public/expenses.html'), 'utf8');
  assert.ok(html.includes('src="/expense-refunds.js?v=' + version + '"'), 'Update the refund script content version when its source changes');
});
test('refund dialog overrides the shared narrow dialog maximum width', () => {
  assert.match(source, /#rf_dialog\{width:min\(960px,95vw\);max-width:95vw;/);
});
test('refund mode hidden fields override shared flex-field display rules', () => {
  assert.match(source, /#rf_dialog \[hidden\]\{display:none!important\}/);
  assert.match(source, /\.rf_issuer_field'\)\.hidden = !credit/);
  assert.match(source, /\.rf_existing_field'\)\.hidden = !money \|\| receiver === 'payer'/);
});

function formComponent(mode = 'voucher', extra = {}) {
  const fields = Object.fromEntries(Object.entries({ '.rf_mode': mode, '.rf_receiver': 'company', '.rf_component_amount': '1512',
    '.rf_account': 'Axis Bank 3448', '.rf_issuer': 'Test Merchant', '.rf_reference': '00001234',
    '.rf_expiry': '2026-12-31', '.rf_existing': 'OLD-BANK-RECEIPT', ...extra }).map(([key, value]) => [key, { value }]));
  for (const key of ['.rf_account_field', '.rf_issuer_field', '.rf_expiry_field', '.rf_existing_field']) fields[key] = {};
  return { fields, querySelector: selector => fields[selector] };
}
function uiHarness(component = formComponent()) {
  const nodes = { rf_dialog: { querySelectorAll: () => [] }, rf_proofs: { files: [] }, rf_form_status: { value: 'received' },
    rf_date: { value: '2026-10-05' }, rf_reason_type: { value: 'return' }, rf_reason: { value: 'Returned' },
    rf_message: {}, rf_preview: {}, rf_save: {} };
  let posted;
  const sandbox = { window: {}, document: { getElementById: id => nodes[id], querySelectorAll: selector => selector === '#rf_components .rf-component' ? [component] : [] },
    cfg: { accountsByNature: { SANKI: ['Axis Bank 3448'] } },
    esc: value => String(value || '').replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;'),
    fmtBank: value => '₹' + Number(value).toFixed(2), proofGallery: () => '',
    api: async (url, options) => { posted = JSON.parse(options.body); return { success: true, preview: { amount: 1512,
      components: [{ mode: 'voucher', amount: 1512, receiver: 'company', account: '', issuer: 'Server-approved merchant', reference: '00001234' }] }, impact: { billCredit: 1512 } }; } };
  const instrumented = source.replace(/\}\(\)\);\s*$/, 'window.refundTest = { componentFromForm, componentDestination, previewComponent, accounts, doPreview, recordRow, setup: function () { data = { sources: [], existingMovements: [] }; draft = { sources: [], proofs: [] }; } }; }());');
  vm.runInNewContext(instrumented, sandbox);
  sandbox.window.refundTest.setup();
  return { ...sandbox.window.refundTest, nodes, posted: () => posted };
}

test('non-cash form payloads discard stale hidden bank accounts and receipt links', () => {
  const ui = uiHarness();
  for (const mode of ['voucher', 'store_credit', 'vendor_credit']) {
    const payload = ui.componentFromForm(formComponent(mode));
    assert.equal(payload.account, ''); assert.equal(payload.externalMovementId, '');
    assert.equal(payload.issuer, 'Test Merchant'); assert.equal(payload.reference, '00001234');
    assert.equal(payload.expiryDate, '2026-12-31');
  }
});

test('switching back to a bank refund discards irrelevant voucher issuer and expiry', () => {
  const payload = uiHarness().componentFromForm(formComponent('bank'));
  assert.equal(payload.account, 'Axis Bank 3448'); assert.equal(payload.externalMovementId, 'OLD-BANK-RECEIPT');
  assert.equal(payload.issuer, ''); assert.equal(payload.expiryDate, '');
});

test('credit-note payload has neither bank receipt nor voucher wallet fields', () => {
  const payload = uiHarness().componentFromForm(formComponent('credit_note'));
  for (const field of ['account', 'issuer', 'expiryDate', 'externalMovementId']) assert.equal(payload[field], '');
  assert.equal(uiHarness().componentDestination(payload), 'Unpaid bill');
});

test('mode switching clears bank options for a voucher and restores them for money', () => {
  const component = formComponent('bank'), ui = uiHarness(component);
  ui.accounts(component); assert.match(component.fields['.rf_account'].innerHTML, /Axis Bank 3448/);
  component.fields['.rf_mode'].value = 'voucher'; ui.accounts(component);
  assert.equal(component.fields['.rf_account'].innerHTML, '');
  assert.equal(component.fields['.rf_account_field'].hidden, true);
  assert.equal(component.fields['.rf_issuer_field'].hidden, false);
  component.fields['.rf_mode'].value = 'bank'; ui.accounts(component);
  assert.match(component.fields['.rf_account'].innerHTML, /Axis Bank 3448/);
  assert.equal(component.fields['.rf_account_field'].hidden, false);
  assert.equal(component.fields['.rf_issuer_field'].hidden, true);
});

test('voucher, store and vendor credit preview cannot display a stale bank destination', () => {
  const ui = uiHarness();
  for (const mode of ['voucher', 'store_credit', 'vendor_credit']) {
    const html = ui.previewComponent({ mode, amount: 1512, account: 'Axis Bank 3448', issuer: '<Merchant>', reference: '00001234', externalMovementId: 'OLD-RECEIPT' });
    assert.match(html, /Credit with &lt;Merchant&gt;/); assert.match(html, /no bank\/cash movement/);
    assert.match(html, /Reference: 00001234/); assert.doesNotMatch(html, /Axis Bank|company account credited|existing receipt linked/);
  }
});

test('review uses server-normalized voucher issuer instead of raw hidden form values', async () => {
  const ui = uiHarness(formComponent('voucher', { '.rf_issuer': '' })); await ui.doPreview();
  assert.equal(ui.posted().components[0].account, '');
  assert.match(ui.nodes.rf_preview.innerHTML, /Credit with Server-approved merchant/);
  assert.doesNotMatch(ui.nodes.rf_preview.innerHTML, /Axis Bank 3448/);
  assert.equal(ui.nodes.rf_save.disabled, false);
});

test('monetary refund previews retain bank, card and personal-payer effects', () => {
  const ui = uiHarness();
  assert.match(ui.previewComponent({ mode: 'bank', amount: 10, account: 'Axis Bank 3448' }), /Axis Bank 3448.*company account credited/);
  assert.match(ui.previewComponent({ mode: 'card', amount: 10, account: 'Original Card' }), /card outstanding reduced/);
  assert.match(ui.previewComponent({ mode: 'cash', amount: 10, account: 'Personal Cash', receiver: 'payer' }), /not company cash/);
});

test('saved non-cash refund row prioritizes its merchant even with obsolete bank metadata', () => {
  const html = uiHarness().recordRow({ id: 'RF-TEST', date: '2026-10-05', vendor: 'Supplier', nature: 'SANKI', amount: 1512,
    sources: [], components: [{ mode: 'voucher', amount: 1512, account: 'Axis Bank 3448', issuer: 'Merchant', reference: '00001234' }],
    reasonType: 'return', reason: 'Returned', displayStatus: 'received', status: 'received', proofs: [] });
  assert.match(html, /Credit with Merchant/); assert.match(html, /00001234/); assert.doesNotMatch(html, /Axis Bank 3448/);
});

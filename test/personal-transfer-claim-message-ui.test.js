'use strict';
// Synthetic UI and claim metadata only. No app/accounting stores or requests.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const html = fs.readFileSync(path.join(__dirname, '../public/expenses.html'), 'utf8');
const escape = value => String(value == null ? '' : value).replace(/[&<>"']/g, char => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[char]));
function messageNode() {
  let text = '', markup = '';
  return { className: '', get textContent() { return text; }, set textContent(value) { text = String(value); markup = ''; }, get innerHTML() { return markup; }, set innerHTML(value) { markup = String(value); } };
}
function harness() {
  const elements = { brMsg: messageNode(), biMsg: messageNode(), brSave: { disabled: true }, brPrincipal: { value: '825' }, brCharge: { value: '0' }, brOtherAccount: { value: 'PERSONAL|Synthetic own account' }, brRemark: { value: 'Unsaved synthetic transfer evidence' }, biNote: { value: 'Unsaved incoming source explanation' } };
  let cleared = 0, renders = 0;
  const ctx = vm.createContext({ el: id => elements[id], esc: escape, encodeURIComponent, clearBankReconForm() { cleared++; }, renderBankReconciliation() { renders++; }, api() { throw Error('The message renderer must never send a request'); }, window: {}, location: { href: 'https://synthetic.test/expenses.html?tab=cash' } });
  const start = html.indexOf('    function showBankTransferConflict('), end = html.indexOf("    el('brKind').addEventListener", start);
  assert.ok(start > 0 && end > start); vm.runInContext(html.slice(start, end), ctx);
  return { ctx, elements, get cleared() { return cleared; }, get renders() { return renders; } };
}
function response(extra = {}) {
  return { success: false, error: 'A specific saved transfer decision, not merely an open account reconciliation, claims this bank transaction.', transferClaim: { type: 'pending_internal_transfer', nature: 'PERSONAL', account: 'Synthetic own account', draftId: 'BRD-SYNTHETIC-CLAIM', rowId: 'bank-3', date: '2098-07-19', principalAmount: 825, bankReference: '800000000101', bankReferenceVerified: true, reviewUrl: 'https://untrusted.invalid/ignored', ...extra } };
}

test('pending claim review is an ordinary new-tab deep link and does not navigate, save, discard or rewrite the current form', () => {
  const ui = harness(), before = JSON.stringify(Object.fromEntries(Object.entries(ui.elements).filter(([, value]) => 'value' in value))), href = ui.ctx.location.href;
  ui.ctx.finishBankReconForm(response());
  const output = ui.elements.brMsg.innerHTML;
  assert.match(output, /href="\/expenses\.html\?tab=cash&amp;nature=PERSONAL&amp;account=Synthetic%20own%20account&amp;draft=BRD-SYNTHETIC-CLAIM"/);
  assert.match(output, /target="_blank" rel="noopener noreferrer"/);
  assert.match(output, /Review saved decision · Synthetic own account · bank-3/);
  assert.match(output, /Your current form and unsaved amounts stay open here/);
  assert.match(output, /does not undo or finalize any decision/);
  assert.doesNotMatch(output, /untrusted\.invalid|onclick=|\/resolve|\/finalize|javascript:/);
  assert.equal(ui.elements.brSave.disabled, false); assert.equal(ui.cleared, 0); assert.equal(ui.renders, 0); assert.equal(ui.ctx.location.href, href);
  assert.equal(JSON.stringify(Object.fromEntries(Object.entries(ui.elements).filter(([, value]) => 'value' in value))), before);
});

test('claim labels and error text are escaped and metadata URLs cannot create external or executable links', () => {
  const ui = harness(), data = response({ account: '<img src=x onerror=alert(1)>', reviewUrl: 'javascript:alert(1)' }); data.error = '<script>untrusted error</script>';
  ui.ctx.showBankTransferConflict('biMsg', data, 'Fallback');
  const output = ui.elements.biMsg.innerHTML;
  assert.match(output, /&lt;script&gt;untrusted error&lt;\/script&gt;/);
  assert.match(output, /&lt;img src=x onerror=alert\(1\)&gt;/);
  assert.match(output, /account=%3Cimg%20src%3Dx%20onerror%3Dalert\(1\)%3E/);
  assert.doesNotMatch(output, /<script>|<img |href="javascript:|untrusted\.invalid/);
});

test('missing, unauthorized or malformed claim metadata remains a plain error and never supplies a navigation target', () => {
  for (const mutate of [data => { delete data.transferClaim; }, data => { data.transferClaim.type = 'automatic_suggestion'; }, data => { data.transferClaim.nature = 'UNKNOWN'; }, data => { data.transferClaim.draftId = '../foreign'; }, data => { data.transferClaim.rowId = 'bank-3" onclick="bad()'; }, data => { data.transferClaim.account = ''; }]) {
    const ui = harness(), data = response(); mutate(data); ui.ctx.showBankTransferConflict('brMsg', data, 'Fallback');
    assert.equal(ui.elements.brMsg.innerHTML, ''); assert.equal(ui.elements.brMsg.textContent, data.error); assert.equal(ui.cleared, 0); assert.equal(ui.renders, 0);
  }
});

test('outgoing and incoming error paths share the safe claim renderer without clearing either form', () => {
  assert.match(html, /function finishBankReconForm\(d\)\{el\('brSave'\)\.disabled=false;if\(!d\.success\)\{showBankTransferConflict\('brMsg',d,/);
  assert.match(html, /if\(!d\.success\)\{showBankTransferConflict\('biMsg',d,'Could not record incoming money\.'\);return;\}/);
  assert.match(html, /selectedDraftId=SankiNavigation\.text\('draft',\/\^\[A-Za-z0-9_-\]\+\$\//, 'the new link uses the existing validated draft routing');
});

function bankLoaderHarness(response) {
  const elements = { bs_account: { value: 'Synthetic own account' }, bs_table: messageNode(), bs_history: { innerHTML: 'Original account history' }, bs_finalize: { style: { display: 'inline-flex' } }, bs_discard: { style: { display: 'inline-flex' } } };
  elements.bs_table.innerHTML = 'Original statement overview';
  let rendered = 0, historyRenders = 0, navigationChanges = 0;
  const form = { draftId: 'BRD-ORIGINAL-UNFINISHED', principal: '825', charge: '0', otherAccount: 'PERSONAL|Synthetic destination', remark: 'Unsaved original evidence' };
  const ctx = vm.createContext({ Promise, encodeURIComponent, Error, bankNature: 'PERSONAL', bankStatementLoadGeneration: 0, bankDraftId: 'BRD-ORIGINAL-UNFINISHED', bankReconView: { draftId: 'BRD-ORIGINAL-UNFINISHED' }, bankReconForm: form, bankDraftOptions: [], el: id => elements[id], api: () => Promise.resolve(response), SankiNavigation: { text: () => '', set() { navigationChanges++; } }, reconciliationStatusBusy: () => false, renderBankHistory() { historyRenders++; return 'New history'; }, renderBankReconciliation() { rendered++; }, ReconciliationStatus: { bankSaved: () => 'History' }, bankDraftSwitcher: () => '', fmt: String, esc: escape });
  const start = html.indexOf('    function loadBankStatement('), end = html.indexOf('    function uploadBankStatement(', start); vm.runInContext(html.slice(start, end), ctx);
  return { ctx, elements, form, get rendered() { return rendered; }, get historyRenders() { return historyRenders; }, get navigationChanges() { return navigationChanges; } };
}

test('a stale saved-claim deep link never silently opens a different or missing reconciliation', async () => {
  for (const draft of [{ draftId: 'BRD-DIFFERENT-PREVIEW' }, undefined]) {
    const ui = bankLoaderHarness({ success: true, draft, transactions: [], imports: [] }), before = JSON.stringify(ui.form);
    await ui.ctx.loadBankStatement('BRD-REQUESTED-SAVED-CLAIM', false);
    assert.equal(ui.rendered, 0); assert.equal(ui.historyRenders, 0); assert.equal(ui.navigationChanges, 0);
    assert.match(ui.elements.bs_table.textContent, /requested saved reconciliation BRD-REQUESTED-SAVED-CLAIM is no longer available/);
    assert.match(ui.elements.bs_table.textContent, /different preview has not been opened/);
    assert.equal(ui.elements.bs_finalize.style.display, 'none'); assert.equal(ui.elements.bs_discard.style.display, 'none');
    assert.equal(JSON.stringify(ui.form), before); assert.equal(ui.elements.bs_history.innerHTML, 'Original account history');
  }
});

test('a quiet stale-draft refresh preserves the loaded overview, form and original draft context', async () => {
  const ui = bankLoaderHarness({ success: true, draft: { draftId: 'BRD-DIFFERENT-PREVIEW' }, transactions: [], imports: [] }), before = JSON.stringify(ui.form);
  await assert.rejects(() => ui.ctx.loadBankStatement('BRD-ORIGINAL-UNFINISHED', true), /no longer available/);
  assert.equal(ui.rendered, 0); assert.equal(ui.historyRenders, 0); assert.equal(ui.navigationChanges, 0);
  assert.equal(ui.elements.bs_table.innerHTML, 'Original statement overview'); assert.equal(ui.ctx.bankDraftId, 'BRD-ORIGINAL-UNFINISHED');
  assert.equal(JSON.stringify(ui.form), before);
});

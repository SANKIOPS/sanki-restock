'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const accounts = require('../public/own-account-transfers');
const page = fs.readFileSync(path.join(__dirname, '../public/expenses.html'), 'utf8');
const line = text => { const found = page.split('\n').find(value => value.includes(text)); assert.ok(found, text); return found; };
const section = (from, to) => { const start = page.indexOf(from), end = page.indexOf(to, start); assert.ok(start >= 0 && end > start); return page.slice(start, end); };
const config = () => ({ isAdmin: true, isOwner: false, approvalNatures: ['SANKI', 'SAMAST'], natures: ['SANKI', 'SAMAST'], ownTransferAccounts: [{ nature: 'SANKI', account: 'Axis Bank 3448', label: 'Axis Bank 3448 · SANKI' }, { nature: 'SANKI', account: 'Counter Cash' }, { nature: 'SAMAST', account: 'Samast Bank' }, { nature: 'PERSONAL', account: 'IndusInd Bank 7883', label: 'Gagan Sir Personal · IndusInd Bank 7883' }], accountsByNature: { SANKI: ['Axis Bank 3448'], SAMAST: ['IndusInd Bank 7883'] } });
const esc = value => String(value == null ? '' : value).replace(/[&<>"']/g, char => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[char]));
const flush = () => new Promise(resolve => setImmediate(resolve));
function harness(overrides = {}) {
  const nodes = {}, calls = [], uploads = [], messages = [];
  const ctx = vm.createContext({ cfg: config(), OwnAccountTransfers: accounts, bankNature: 'SAMAST', bankDraftId: 'DRAFT-1', bankIncomingRowId: 'bank-1', bankIncomingNature: 'SAMAST', bankReconView: { account: 'IndusInd Bank 7883', rows: [{ id: 'bank-1', transferAccountNature: 'PERSONAL', bank: { debit: 100, credit: 0, date: '2026-10-23' }, entityTag: { nature: 'UNKNOWN' } }] },
    el(id) { return nodes[id] ||= { value: '', innerHTML: '', textContent: '', disabled: false, style: {}, files: [], listeners: {}, open: false, addEventListener(event, fn) { (this.listeners[event] ||= []).push(fn); } }; },
    bankEntityTaggingActive: () => true,
    bankReconRow(id) { return ctx.bankReconView.rows.find(row => row.id === id); },
    api(url, options) { calls.push({ url, body: JSON.parse(options.body) }); return Promise.resolve({ success: false, error: 'Synthetic validation failure; no records changed' }); },
    uploadFiles(files, nature) { uploads.push(nature); return Promise.resolve(['/synthetic-proof.png']); },
    setMsg(message) { messages.push(message); }, fmt: String, esc, loadCash() {}, loadAccountLedger() {},
    ...overrides });
  vm.runInContext(section('    function opts(', '    function payingAccountsForNature('), ctx);
  vm.runInContext(line('function bankRowNature('), ctx);
  return { ctx, nodes, calls, uploads, messages, run(code) { return vm.runInContext(code, ctx); }, fields(values) { for (const [id, value] of Object.entries(values)) ctx.el(id).value = value; } };
}

test('manual own-account form asks only canonical source/destination accounts, not standalone entities', () => {
  const form = line('Move money between any accounts');
  assert.match(form, /id="tr_from"/); assert.match(form, /id="tr_to"/);
  assert.doesNotMatch(form, /tr_from_nature|tr_to_nature|From entity|To entity/);
  assert.match(form, /no separate entity choice is needed/);
  assert.match(page, /id="beNature"/); assert.match(page, /id="biReceiptType"/);
  assert.match(page, /id="biClassificationWrap"><summary>Optional special transfer treatment/);
});

test('transfer selector uses only the explicit authorized registry, never expense ownership or reporting filter defaults', () => {
  const cfg = config(), original = JSON.stringify(cfg);
  const html = accounts.options(cfg);
  assert.match(html, /value="PERSONAL\|IndusInd Bank 7883"/);
  assert.doesNotMatch(html, /value="SAMAST\|IndusInd Bank 7883"/);
  assert.equal(accounts.find(cfg, 'PERSONAL|IndusInd Bank 7883').nature, 'PERSONAL');
  assert.equal(accounts.find(cfg, 'SAMAST|IndusInd Bank 7883').account, '');
  assert.equal(accounts.find({ accountsByNature: cfg.accountsByNature }, 'IndusInd Bank 7883').account, '');
  assert.equal(accounts.find(cfg, 'PERSONAL|Unlisted private account').account, '');
  assert.equal(JSON.stringify(cfg), original);
});

test('ambiguous registry identities are disabled and cannot be decoded; duplicate same-identity entries do not multiply', () => {
  const cfg = { ownTransferAccounts: [{ nature: 'SANKI', account: 'Shared Bank' }, { nature: 'SAMAST', account: 'Shared Bank' }, { nature: 'PERSONAL', account: 'Known Bank' }, { nature: 'PERSONAL', account: 'Known Bank' }] };
  assert.match(accounts.options(cfg), /value="SANKI\|Shared Bank" disabled/);
  assert.match(accounts.options(cfg), /ownership is ambiguous/);
  assert.equal(accounts.find(cfg, 'Shared Bank').account, '');
  assert.equal(accounts.find(cfg, 'SANKI|Shared Bank').account, '');
  assert.equal(accounts.options(cfg).split('value="PERSONAL|Known Bank"').length - 1, 1);
});

test('account labels escape source text, exclude the same physical account and expose no balance data', () => {
  const cfg = config(); cfg.ownTransferAccounts[0].label = '<img src=x onerror=bad()> Bank';
  const output = accounts.options(cfg, 'IndusInd Bank 7883');
  assert.match(output, /&lt;img/); assert.doesNotMatch(output, /<img|IndusInd Bank 7883|balance|opening|₹/);
});

test('transfer-only 7883 context does not alter expense ownership and real conflicts remain blocked', () => {
  const ui = harness(), row = ui.ctx.bankReconView.rows[0];
  assert.equal(ui.ctx.bankTransferNature(row), 'PERSONAL');
  assert.equal(ui.ctx.bankRowNature(row), 'SAMAST');
  row.transferAccountNature = 'SAMAST'; row.entityTag = { nature: 'SAMAST', source: 'manual' };
  assert.equal(ui.ctx.bankTransferNature(row), 'SAMAST', 'authoritative server context wins over registry hints');
  row.transferAccountNature = 'PERSONAL';
  for (const source of ['manual', 'linked_ledger', 'finalized_bank_record']) { row.entityTag.source = source; assert.equal(ui.ctx.bankTransferNature(row), 'SAMAST', 'reliable actual backend source wins: ' + source); }
  row.entityTag.sourceConflict = true;
  assert.equal(ui.ctx.bankTransferNature(row), '', 'conflicting evidence is never silently overridden');
});

test('unknown generic bank rows show a transaction action, while verified own transfers and conflicts retain distinct explanations', () => {
  const ui = harness();
  ui.run(section('    function bankEntityTagLabel(', '    function bankDraftSwitcher('));
  const row = ui.ctx.bankReconView.rows[0];
  let output = ui.ctx.bankEntityTagMarkup(row);
  assert.match(output, /Choose transaction action/); assert.doesNotMatch(output, /Belongs to: Needs review/);
  row.resolution = { ownAccountSelection: { fromAccount: 'Axis Bank 3448', toAccount: 'IndusInd Bank 7883' } };
  output = ui.ctx.bankEntityTagMarkup(row); assert.match(output, /Account-based transfer · PERSONAL/);
  row.entityTag = { sourceConflict: true, nature: 'SAMAST' };
  output = ui.ctx.bankEntityTagMarkup(row); assert.match(output, /Conflict — review/); assert.doesNotMatch(output, /Account-based transfer/);
  assert.equal(ui.calls.length, 0);
});

test('bookkeeping corrections stay an explicit optional advanced action, real conflicts remain visible and reviewing never writes', () => {
  const ui = harness();
  ui.run(section('    function bankEntityTagLabel(', '    function bankDraftSwitcher('));
  ui.run(section('    function showBankEntityTagEditor(rowId){', '    var openBankReviewBeforeEntityTag='));
  ui.ctx.showBankEntityTagEditor('bank-1');
  assert.equal(ui.nodes.bankEntityTagEditor.open, false);
  assert.match(ui.nodes.bankEntityTagCurrent.innerHTML, /No bookkeeping correction requested/);
  assert.match(ui.nodes.bankEntityTagMsg.textContent, /not use it as a prerequisite/);
  ui.ctx.bankReconView.rows[0].entityTag.sourceConflict = true;
  ui.ctx.showBankEntityTagEditor('bank-1');
  assert.equal(ui.nodes.bankEntityTagEditor.open, true);
  assert.match(ui.nodes.bankEntityTagMsg.textContent, /conflicts with the linked ledger/);
  assert.match(page, />Advanced bookkeeping corrections — optional<\/summary>/);
  assert.equal(ui.calls.length, 0);
});

test('bulk checkboxes explicitly mean optional bookkeeping correction, not transaction reconciliation requirements', () => {
  const ui = harness(); ui.run(line('function bankEntityBulkCheckbox(')); ui.run(line('function bankEntityBulkToolbar('));
  assert.match(ui.ctx.bankEntityBulkCheckbox({ rowId: 'bank-1' }, false), /Select for optional entity correction/);
  assert.doesNotMatch(ui.ctx.bankEntityBulkCheckbox({ rowId: 'bank-1' }, false), /<span>Select entity<\/span>/);
  assert.match(ui.ctx.bankEntityBulkToolbar('draft'), /not a reconciliation prerequisite/);
});

test('changing canonical source updates destination and treatment without discarding other unsaved inputs', () => {
  const ui = harness(); ui.run(line('function syncTransferClass()')); ui.run(line('function syncOwnTransferAccounts()'));
  ui.fields({ tr_from: 'PERSONAL|IndusInd Bank 7883', tr_to: 'SANKI|Axis Bank 3448', tr_class: 'internal_transfer', tr_note: 'Unsaved reason', tr_amount: '100', f_part: 'Unrelated unsaved expense' });
  const proof = {}; ui.ctx.el('tr_proof').files = [proof];
  ui.ctx.syncOwnTransferAccounts();
  assert.equal(ui.nodes.tr_to.value, 'SANKI|Axis Bank 3448');
  assert.doesNotMatch(ui.nodes.tr_to.innerHTML, /Samast Bank|IndusInd Bank 7883/);
  assert.equal(ui.nodes.tr_class.value, 'owner_contribution');
  assert.equal(ui.nodes.tr_note.value, 'Unsaved reason'); assert.equal(ui.nodes.tr_amount.value, '100');
  assert.equal(ui.nodes.f_part.value, 'Unrelated unsaved expense'); assert.equal(ui.nodes.tr_proof.files[0], proof);
  assert.equal(ui.calls.length, 0);
});

test('manual transfer snapshots canonical identities and mode before proof upload; failures preserve all input and double-click sends once', async () => {
  let release;
  const ui = harness({ uploadFiles(files, nature) { ui.uploads.push(nature); return new Promise(resolve => { release = resolve; }); } });
  ui.run(section('    function recordTransfer(', '    function ledgerDisplayParts('));
  ui.fields({ tr_from: 'PERSONAL|IndusInd Bank 7883', tr_to: 'SANKI|Axis Bank 3448', tr_amount: '100', tr_class: 'owner_contribution', tr_date: '2026-10-23', tr_note: 'Verified source account', tr_route: 'direct', tr_intermediary: '' }); ui.ctx.el('tr_proof').files = [{}];
  ui.ctx.recordTransfer(); ui.ctx.recordTransfer();
  assert.deepEqual(ui.uploads, ['SANKI']); assert.equal(ui.calls.length, 0);
  ui.fields({ tr_to: 'SAMAST|Samast Bank', tr_class: 'inter_entity_loan', tr_date: '2026-10-22' });
  release(['/proof.png']); await flush();
  assert.equal(ui.calls.length, 1); const body = ui.calls[0].body;
  assert.equal(body.ownAccountTransfer, true); assert.equal(body.fromNature, 'PERSONAL'); assert.equal(body.fromAccount, 'IndusInd Bank 7883'); assert.equal(body.toNature, 'SANKI'); assert.equal(body.toAccount, 'Axis Bank 3448');
  assert.equal(body.date, '2026-10-23'); assert.equal(body.classification, 'owner_contribution');
  assert.equal(ui.nodes.tr_go.disabled, false); assert.equal(ui.nodes.tr_amount.value, '100'); assert.equal(ui.nodes.tr_note.value, 'Verified source account');
});

test('unknown accounts, self-transfers, missing proof and unsupported personal routes do not begin uploads or writes', () => {
  const ui = harness(); ui.run(section('    function recordTransfer(', '    function ledgerDisplayParts('));
  ui.fields({ tr_from: 'SAMAST|IndusInd Bank 7883', tr_to: 'SANKI|Axis Bank 3448', tr_amount: '100', tr_class: 'owner_contribution', tr_date: '2026-10-23', tr_note: 'Reason', tr_route: 'direct', tr_intermediary: '' }); ui.ctx.el('tr_proof').files = [{}];
  ui.ctx.recordTransfer(); assert.match(ui.messages.at(-1), /Unknown or ambiguous/);
  ui.fields({ tr_from: 'PERSONAL|IndusInd Bank 7883', tr_to: 'SAMAST|Samast Bank' }); ui.ctx.recordTransfer(); assert.match(ui.messages.at(-1), /only with a SANKI/);
  ui.fields({ tr_from: 'SANKI|Axis Bank 3448', tr_to: 'SANKI|Axis Bank 3448' }); ui.ctx.recordTransfer(); assert.match(ui.messages.at(-1), /different accounts/);
  ui.fields({ tr_to: 'SANKI|Counter Cash' }); ui.ctx.el('tr_proof').files = []; ui.ctx.recordTransfer(); assert.match(ui.messages.at(-1), /proof is required/);
  assert.equal(ui.uploads.length, 0); assert.equal(ui.calls.length, 0);
});

test('internal reconciliation bodies carry the account-selector flag; external receipt and expense classification remain separate', () => {
  assert.match(line("other=bankTransferCounterpart(el('brOtherAccount').value"), /action:'create_internal_transfer',ownAccountTransfer:other.own&&bankOwnAccountRegistered\(\)\?true:undefined/);
  assert.match(line("el('biSave').addEventListener"), /sourceKind:internal\?'internal':'external',ownAccountTransfer:internal&&!salaryFunding/);
  assert.match(line("el('beSave').addEventListener"), /Choose an allowed expense entity/);
  assert.doesNotMatch(line("el('beSave').addEventListener"), /ownAccountTransfer/);
  assert.match(line('function bankIncomingSources('), /bankIncomingTransferNature\(\)/);
});

test('advanced legacy transfers retain claimant, clearing and wallet accounts plus existing classification/intermediary/proof controls', () => {
  const ui = harness();
  ui.ctx.selectedNature = 'SANKI'; ui.ctx.today = () => '2026-10-23';
  ui.ctx.cfg.transferAccountsByNature = { SANKI: ['Axis Bank 3448', 'Arshpreet 1919', 'Paytm Settlement Clearing', 'Vendor Credit Wallet'] };
  ui.run(line('function renderOtherTransferForm('));
  const form = ui.ctx.renderOtherTransferForm();
  assert.match(form, /^<details class="card" id="tr_other_form"><summary>/);
  for (const text of ['Arshpreet 1919', 'Paytm Settlement Clearing', 'Vendor Credit Wallet', 'tr_other_from_nature', 'tr_other_to_nature', 'tr_other_class', 'value="reimbursement"', 'tr_other_route', 'tr_other_proof']) assert.ok(form.includes(text), text);
  assert.match(form, /not in the own-account selectors above/);
  assert.match(page, /renderOtherTransferForm\(\)\+/);
});

test('advanced claimant transfers use the original explicitly classified route, never own-account mode, and preserve failures', async () => {
  const ui = harness(); ui.run(section('    function recordTransfer(', '    function ledgerDisplayParts('));
  ui.fields({ tr_other_from_nature: 'SANKI', tr_other_from: 'Axis Bank 3448', tr_other_to_nature: 'SANKI', tr_other_to: 'Arshpreet 1919', tr_other_class: 'reimbursement', tr_other_route: 'intermediary', tr_other_intermediary: 'Forwarder', tr_other_amount: '100', tr_other_date: '2026-10-23', tr_other_note: 'Existing claimant reimbursement route' }); ui.ctx.el('tr_other_proof').files = [{}];
  ui.ctx.recordTransfer(true); await flush();
  assert.equal(ui.calls.length, 1); const body = ui.calls[0].body;
  assert.equal(ui.calls[0].url, '/api/expenses/transfers'); assert.equal(body.toAccount, 'Arshpreet 1919'); assert.equal(body.classification, 'reimbursement');
  assert.equal(Object.hasOwn(body, 'ownAccountTransfer'), false); assert.equal(body.routedThroughIntermediary, true); assert.equal(body.intermediary, 'Forwarder');
  assert.equal(ui.nodes.tr_other_go.disabled, false); assert.equal(ui.nodes.tr_other_note.value, 'Existing claimant reimbursement route');
  assert.deepEqual(ui.uploads, ['SANKI']);
});

test('advanced legacy form cannot bypass the canonical own-account selectors by relabelling a known physical account', () => {
  const ui = harness(); ui.run(section('    function recordTransfer(', '    function ledgerDisplayParts('));
  ui.fields({ tr_other_from_nature: 'SAMAST', tr_other_from: 'IndusInd Bank 7883', tr_other_to_nature: 'SANKI', tr_other_to: 'Axis Bank 3448', tr_other_class: 'inter_entity_loan', tr_other_route: 'direct', tr_other_intermediary: '', tr_other_amount: '100', tr_other_date: '2026-10-23', tr_other_note: 'Must use canonical account identity' }); ui.ctx.el('tr_other_proof').files = [{}];
  ui.ctx.recordTransfer(true);
  assert.match(ui.messages.at(-1), /Both are registered own accounts/);
  assert.equal(ui.uploads.length, 0); assert.equal(ui.calls.length, 0);
});

test('reconciliation counterpart groups preserve approved claimant/clearing routes, with own mode only for registered own accounts', async () => {
  const ui = harness(); ui.ctx.bankReconView.account = 'Axis Bank 3448'; ui.ctx.bankReconView.rows[0].transferAccountNature = 'SANKI';
  ui.ctx.cfg.transferAccountsByNature = { SANKI: ['Axis Bank 3448', 'Counter Cash', 'Arshpreet 1919', 'Paytm Settlement Clearing'] };
  const options = ui.ctx.bankTransferOptions('SANKI', 'Axis Bank 3448');
  assert.match(options, /optgroup label="Registered own accounts"/); assert.match(options, /optgroup label="Other approved accounts — existing route"/);
  assert.match(options, /SANKI\|Arshpreet 1919/); assert.match(options, /SANKI\|Paytm Settlement Clearing/);
  assert.equal(ui.ctx.bankTransferCounterpart('PERSONAL|IndusInd Bank 7883', 'SANKI', 'Axis Bank 3448').own, true);
  assert.equal(ui.ctx.bankTransferCounterpart('SANKI|Arshpreet 1919', 'SANKI', 'Axis Bank 3448').own, false);
  ui.ctx.bankReconForm = { draftId: 'DRAFT-1', rowId: 'bank-1', mode: 'assign' }; ui.ctx.bankReconAmount = () => 100; ui.ctx.finishBankReconForm = () => { ui.ctx.el('brSave').disabled = false; };
  ui.fields({ brKind: 'internal_transfer', brRemark: 'Verified transfer with bank evidence', brPrincipal: '100', brCharge: '0', brOtherAccount: 'PERSONAL|IndusInd Bank 7883' });
  ui.run(line("other=bankTransferCounterpart(el('brOtherAccount').value"));
  const click = () => ui.nodes.brSave.listeners.click[0]({ preventDefault() {}, stopImmediatePropagation() {} });
  click(); await flush(); assert.equal(ui.calls[0].body.ownAccountTransfer, true);
  ui.fields({ brOtherAccount: 'SANKI|Arshpreet 1919' }); click(); await flush();
  assert.equal(ui.calls.length, 2); assert.equal(ui.calls[1].body.otherAccount, 'Arshpreet 1919'); assert.equal(Object.hasOwn(ui.calls[1].body, 'ownAccountTransfer'), false);
  ui.fields({ brPrincipal: '99' }); click(); assert.equal(ui.calls.length, 2, 'bank amount check is unchanged for legacy choices');
  ui.fields({ brPrincipal: '100', brOtherAccount: 'SANKI|Unknown' }); click(); assert.equal(ui.calls.length, 2, 'tampered legacy names never acquire a source identity');
});

test('incoming legacy source remains account-only, uses its existing route without own inference, and external receipts carry no own flag', async () => {
  const ui = harness(); ui.ctx.bankReconView.account = 'Axis Bank 3448'; ui.ctx.bankReconView.rows[0].transferAccountNature = 'SANKI';
  ui.ctx.cfg.transferAccountsByNature = { SANKI: ['Axis Bank 3448', 'Arshpreet 1919', 'Counter Cash'] };
  ['bankIncomingTransferNature', 'bankIncomingSources', 'bankIncomingSource'].forEach(name => ui.run(line('function ' + name + '(')));
  ui.fields({ biSourceKind: 'internal', biFromAccount: 'SANKI|Arshpreet 1919', biClassification: 'internal_transfer', biNote: 'Verified claimant return transfer', biSalaryAdvance: '', biReceiptType: 'other_income', biCategory: 'Other Income', biExternalSource: '' });
  ui.run(line("el('biSave').addEventListener"));
  const click = () => ui.nodes.biSave.listeners.click[0].call(ui.nodes.biSave);
  click(); await flush();
  assert.equal(ui.calls.length, 1); assert.equal(ui.calls[0].body.fromAccount, 'Arshpreet 1919'); assert.equal(Object.hasOwn(ui.calls[0].body, 'ownAccountTransfer'), false);
  ui.fields({ biSourceKind: 'external', biExternalSource: 'External payer', biNote: 'Verified external receipt' }); click(); await flush();
  assert.equal(ui.calls.length, 2); assert.equal(ui.calls[1].body.sourceKind, 'external'); assert.equal(Object.hasOwn(ui.calls[1].body, 'ownAccountTransfer'), false);
  ui.fields({ biSourceKind: 'internal', biFromAccount: 'SANKI|Counter Cash', biClassification: 'internal_transfer', biNote: 'Verified cash funding' }); click(); await flush();
  assert.equal(ui.calls.length, 3); assert.equal(ui.calls[2].body.ownAccountTransfer, true);
  assert.equal(ui.nodes.biSave.disabled, false); assert.equal(ui.nodes.biNote.value, 'Verified cash funding');
});

test('legacy claimant, clearing and wallet bank sides retain existing incoming/outgoing routes without receiving own-account authority', async () => {
  for (const current of ['Arshpreet 1919', 'Paytm Settlement Clearing', 'Vendor Credit Wallet']) {
    const ui = harness({ bankEntityTaggingActive: () => false, bankNature: 'SANKI' });
    ui.ctx.bankReconView.account = current; delete ui.ctx.bankReconView.rows[0].transferAccountNature;
    ui.ctx.cfg.transferAccountsByNature = { SANKI: ['Axis Bank 3448', 'Counter Cash', current] };
    assert.equal(ui.ctx.bankTransferNature(ui.ctx.bankReconView.rows[0]), 'SANKI', current);
    assert.equal(ui.ctx.bankOwnAccountRegistered(), false, current);
    ui.ctx.bankReconForm = { draftId: 'DRAFT-1', rowId: 'bank-1', mode: 'assign' }; ui.ctx.bankReconAmount = () => 100; ui.ctx.finishBankReconForm = () => { ui.ctx.el('brSave').disabled = false; };
    ui.fields({ brKind: 'internal_transfer', brRemark: 'Verified approved legacy bank-side route', brPrincipal: '100', brCharge: '0', brOtherAccount: 'SANKI|Axis Bank 3448' });
    ui.run(line("other=bankTransferCounterpart(el('brOtherAccount').value"));
    ui.nodes.brSave.listeners.click[0]({ preventDefault() {}, stopImmediatePropagation() {} }); await flush();
    assert.equal(ui.calls.length, 1, current); assert.equal(ui.calls[0].body.otherAccount, 'Axis Bank 3448'); assert.equal(Object.hasOwn(ui.calls[0].body, 'ownAccountTransfer'), false, current);
    ['bankIncomingTransferNature', 'bankIncomingSources', 'bankIncomingSource'].forEach(name => ui.run(line('function ' + name + '(')));
    assert.ok(ui.ctx.bankIncomingSources().some(item => item.account === 'Axis Bank 3448'), current);
    ui.fields({ biSourceKind: 'internal', biFromAccount: 'SANKI|Axis Bank 3448', biClassification: 'internal_transfer', biNote: 'Verified incoming legacy bank-side route', biSalaryAdvance: '', biReceiptType: 'other_income', biCategory: 'Other Income', biExternalSource: '' });
    ui.run(line("el('biSave').addEventListener")); ui.nodes.biSave.listeners.click[0].call(ui.nodes.biSave); await flush();
    assert.equal(ui.calls.length, 2, current); assert.equal(ui.calls[1].body.fromAccount, 'Axis Bank 3448'); assert.equal(Object.hasOwn(ui.calls[1].body, 'ownAccountTransfer'), false, current);
  }
});

test('legacy bank-side fallback never supplies expense-container ownership to unresolved shared 7883 or conflicting evidence', () => {
  const ui = harness(); delete ui.ctx.bankReconView.rows[0].transferAccountNature;
  ui.ctx.cfg.ownTransferAccounts = ui.ctx.cfg.ownTransferAccounts.filter(item => item.account !== 'IndusInd Bank 7883');
  assert.equal(ui.ctx.bankTransferNature(ui.ctx.bankReconView.rows[0]), '');
  ui.ctx.bankReconView.account = 'Arshpreet 1919'; ui.ctx.bankEntityTaggingActive = () => false;
  ui.ctx.bankReconView.rows[0].entityTag.sourceConflict = true;
  assert.equal(ui.ctx.bankTransferNature(ui.ctx.bankReconView.rows[0]), '');
});

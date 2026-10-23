'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const OwnAccountTransfers = require('../public/own-account-transfers');
const page = fs.readFileSync(path.join(__dirname, '../public/expenses.html'), 'utf8');
const personal = ['IndusInd Bank 7883', 'ICICI Bank 0992', 'ICICI Bank 0993'];
const descriptors = () => personal.map(account => ({ nature: 'PERSONAL', account, label: 'Gagan Sir Personal · ' + account }));
const esc = value => String(value == null ? '' : value).replace(/[&<>"']/g, x => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[x]));
function section(start, end) {
  const from = page.indexOf(start), to = page.indexOf(end, from);
  assert.ok(from >= 0 && to > from, 'source markers exist: ' + start);
  return page.slice(from, to);
}
function line(marker) {
  const found = page.split('\n').find(text => text.includes(marker));
  assert.ok(found, 'source line exists: ' + marker);
  return found;
}
function harness(overrides = {}) {
  const nodes = {}, storage = new Map(), calls = [];
  const context = {
    cfg: { me: 'prashant', isAdmin: true, isOwner: false, natures: ['SANKI', 'SAMAST'], approvalNatures: ['SANKI', 'SAMAST'], accountsByNature: { SANKI: ['Business Bank', 'ICICI Bank 0992'], SAMAST: ['Samast Bank'] }, transferAccountsByNature: { SANKI: ['Business Bank', 'ICICI Bank 0992'] }, ownerPersonalTransferAccounts: descriptors() },
    bankNature: 'SANKI', bankDraftId: 'TEST-DRAFT', bankIncomingRowId: 'bank-1', bankIncomingNature: '', bankReconForm: null,
    bankReconView: { account: 'Business Bank', rows: [{ id: 'bank-1', bank: { debit: 100, date: '2099-01-01' } }] },
    el(id) { return nodes[id] ||= { value: '', innerHTML: '', style: {}, files: [], listeners: {}, open: false, addEventListener(event, fn, capture) { (this.listeners[event] ||= []).push({ fn, capture }); }, querySelector() { return {}; }, showModal() { this.open = true; }, close() { this.open = false; } }; },
    sessionStorage: { setItem(key, value) { storage.set(key, value); }, getItem(key) { return storage.get(key) || null; } },
    esc, fmt: String, categorySuggestions: () => '', OwnAccountTransfers,
    api(url, options) { calls.push({ url, body: JSON.parse(options.body) }); return Promise.resolve({ success: false, error: 'Synthetic response: no live writes' }); },
    finishBankReconForm() {}, syncBankIncomingClassification() {},
    bankReconRow(id) { return context.bankReconView.rows.find(row => row.id === id); },
    loadCash() {}, loadAccountLedger() {}, renderBankReconciliation() {},
    setMsg() {}, ExpenseLoans: { refresh() {} },
    ...overrides
  };
  Object.defineProperty(context.cfg, 'ownTransferAccounts', { get() {
    const personalRows = context.cfg.ownerPersonalTransferAccounts || [], names = new Set(personalRows.map(item => item.account));
    const byNature = Object.assign({}, context.cfg.accountsByNature, context.cfg.transferAccountsByNature);
    return Object.entries(byNature).flatMap(([nature, accounts]) => accounts.filter(account => !names.has(account)).map(account => ({ nature, account, label: account + ' · ' + nature }))).concat(personalRows);
  } });
  context.window = context;
  vm.createContext(context);
  vm.runInContext(section('    function opts(', '    function payingAccountsForNature('), context);
  vm.runInContext(line('function bankRowNature('), context);
  return { context, nodes, storage, calls, run: code => vm.runInContext(code, context) };
}
const flush = () => new Promise(resolve => setImmediate(resolve));

test('the three Gagan personal accounts are labelled and entity-qualified only in transfer counterpart choices', () => {
  const h = harness(), html = h.context.transferCounterpartOptions('SANKI', 'Business Bank');
  for (const account of personal) {
    assert.equal(html.split('value="PERSONAL|' + account + '"').length - 1, 1);
    assert.ok(html.includes('Gagan Sir Personal · ' + account));
  }
  assert.ok(html.includes('value="ICICI Bank 0992"'), 'a same-name business account remains distinct');
  assert.ok(!html.includes('value="Business Bank"'), 'current entity/account cannot be its own counterpart');
  assert.equal(h.context.transferCounterpart('PERSONAL|ICICI Bank 0992', 'SANKI').nature, 'PERSONAL');
  assert.equal(h.context.transferCounterpart('ICICI Bank 0992', 'SANKI').nature, 'SANKI');
  assert.deepEqual(Array.from(h.context.accountsForNature('SANKI')), ['Business Bank', 'ICICI Bank 0992']);
  assert.deepEqual(Array.from(h.context.accountsForNature('PERSONAL')), [], 'no personal ledger/paying account list is added');
  assert.doesNotMatch(html, /balance|opening|₹/i);
});

test('transfer-only PERSONAL entity is added without changing approval natures; unrelated users receive no choices', () => {
  const h = harness();
  assert.match(h.context.transferNatureOptions('SANKI'), /value="PERSONAL">Gagan Sir Personal/);
  assert.deepEqual(Array.from(h.context.cfg.approvalNatures), ['SANKI', 'SAMAST']);
  assert.deepEqual(Array.from(h.context.transferAccountsForNature('PERSONAL')), personal);
  h.context.cfg.ownerPersonalTransferAccounts = [];
  assert.doesNotMatch(h.context.transferNatureOptions('SANKI'), /PERSONAL/);
  assert.doesNotMatch(h.context.transferCounterpartOptions('SANKI', 'Business Bank'), /Gagan Sir Personal|PERSONAL\|/);
});

test('Owner retains the original full PERSONAL account list and no duplicate personal counterpart', () => {
  const h = harness();
  h.context.cfg.isOwner = true;
  h.context.cfg.approvalNatures.push('PERSONAL');
  h.context.cfg.transferAccountsByNature.PERSONAL = personal.concat('Other Owner Account');
  assert.deepEqual(Array.from(h.context.transferAccountsForNature('PERSONAL')), personal.concat('Other Owner Account'));
  const html = h.context.transferCounterpartOptions('PERSONAL', 'ICICI Bank 0992');
  assert.equal(html.split('value="ICICI Bank 0993"').length - 1, 1);
  assert.doesNotMatch(html, /value="ICICI Bank 0992"|PERSONAL\|/);
  assert.equal(h.context.transferNatureOptions('PERSONAL').split('value="PERSONAL"').length - 1, 1);
});

test('new reconciliation counterpart choices are SANKI-only while existing Owner incoming entities remain available', () => {
  const h = harness();
  for (const isOwner of [false, true]) {
    h.context.cfg.isOwner = isOwner;
    assert.doesNotMatch(h.context.transferCounterpartOptions('SAMAST', 'Samast Bank'), /Gagan Sir Personal|PERSONAL\|/);
    assert.match(h.context.transferCounterpartOptions('SANKI', 'Business Bank'), /PERSONAL\|ICICI Bank 0993/);
  }
  h.context.cfg.isOwner = false;
  assert.doesNotMatch(h.context.transferNatureOptions('SAMAST', 'SAMAST'), /PERSONAL/);
  assert.match(h.context.transferNatureOptions('SANKI', 'SANKI'), /value="PERSONAL"/);
  h.context.cfg.isOwner = true;
  h.context.cfg.approvalNatures.push('PERSONAL');
  assert.match(h.context.transferNatureOptions('SAMAST', 'SAMAST'), /value="PERSONAL"/, 'existing Owner incoming entities are retained');
});

test('personal transfer classifications distinguish owner contribution and owner withdrawal', () => {
  const h = harness();
  assert.equal(h.context.transferClassification('PERSONAL', 'SANKI', 'inter_entity_loan'), 'owner_contribution');
  assert.equal(h.context.transferClassification('SAMAST', 'PERSONAL', 'inter_entity_loan'), 'owner_withdrawal');
  assert.equal(h.context.transferClassification('SANKI', 'SANKI', 'owner_contribution'), 'internal_transfer');
  assert.equal(h.context.transferClassification('SANKI', 'SAMAST', 'inter_entity_loan'), 'inter_entity_loan');
  h.run(line('function syncTransferClass()'));
  h.context.el('tr_from').value = 'PERSONAL|' + personal[0];
  h.context.el('tr_to').value = 'SANKI|Business Bank';
  h.context.el('tr_class').value = 'internal_transfer';
  h.context.syncTransferClass();
  assert.equal(h.nodes.tr_class.value, 'owner_contribution');
});

test('unfinished reconciliation retains each tagged personal choice when saved and reopened', () => {
  const h = harness();
  h.run(section('    function bankReconRow(', '    function clearBankReconForm('));
  h.context.updateBankReconFields = () => h.context.saveBankReconForm();
  h.run(section('    function showBankReconForm(', '    window.openCounterCashBankTransfer='));
  for (const account of personal) {
    h.context.showBankReconForm({ draftId: 'TEST-DRAFT', rowId: 'bank-1', mode: 'assign', kind: 'internal_transfer' });
    h.nodes.brOtherAccount.value = 'PERSONAL|' + account;
    h.nodes.brRemark.value = 'Verified owner transfer';
    h.nodes.brOtherAccount.listeners.change[0].fn();
    const saved = JSON.parse(h.storage.get('sankiBankReconForm'));
    assert.equal(saved.otherAccount, 'PERSONAL|' + account);
    h.nodes.brOtherAccount.value = '';
    h.context.openBankAssignment('bank-1', 'internal_transfer');
    assert.equal(h.nodes.brOtherAccount.value, 'PERSONAL|' + account);
    assert.equal(h.nodes.brRemark.value, 'Verified owner transfer');
  }
});

test('reconciliation transfer save sends explicit PERSONAL entity and canonical account, retaining amount safeguards', async () => {
  const h = harness();
  h.context.bankReconForm = { draftId: 'TEST-DRAFT', rowId: 'bank-1', mode: 'assign' };
  h.context.bankReconRow = () => h.context.bankReconView.rows[0];
  h.context.bankReconAmount = () => 100;
  h.context.el('brKind').value = 'internal_transfer';
  h.context.el('brRemark').value = 'Verified owner withdrawal';
  h.context.el('brPrincipal').value = '100';
  h.context.el('brCharge').value = '0';
  h.run(line("other=bankTransferCounterpart(el('brOtherAccount').value"));
  const listener = h.nodes.brSave.listeners.click[0];
  assert.equal(listener.capture, true);
  const event = { preventDefault() {}, stopImmediatePropagation() {} };
  for (const account of personal) {
    h.context.el('brOtherAccount').value = 'PERSONAL|' + account;
    listener.fn(event);
    await flush();
    assert.equal(h.calls.at(-1).body.otherNature, 'PERSONAL');
    assert.equal(h.calls.at(-1).body.otherAccount, account);
    assert.equal(h.calls.at(-1).body.principalAmount, 100);
  }
  const before = h.calls.length;
  h.context.el('brPrincipal').value = '99';
  listener.fn(event);
  assert.equal(h.calls.length, before, 'amount mismatch cannot be posted');
  h.context.el('brPrincipal').value = '100';
  h.context.el('brOtherAccount').value = '';
  listener.fn(event);
  assert.equal(h.calls.length, before, 'empty counterpart cannot be posted');
});

test('incoming transfer derives PERSONAL source and owner contribution from the single tagged account selector', () => {
  const h = harness();
  ['bankIncomingTransferNature', 'bankIncomingSources', 'bankIncomingSource', 'syncBankIncomingSource', 'syncBankIncomingAccount'].forEach(name => h.run(line('function ' + name + '(')));
  h.context.syncBankIncomingAccount();
  for (const account of personal) assert.ok(h.nodes.biFromAccount.innerHTML.includes(account));
  h.context.el('biFromAccount').value = 'PERSONAL|' + personal[0];
  h.context.syncBankIncomingSource();
  assert.equal(h.nodes.biClassification.value, 'owner_contribution');
  assert.match(line('window.openBankIncoming=function'), /bankIncomingNature=bankRowNature\(row\)/);
  assert.doesNotMatch(line('window.openBankIncoming=function'), /biFromNature/);
  assert.match(line("el('biSave').addEventListener"), /fromNature:ownerSource\?ownerSource\.nature:source\.nature/);
  assert.ok(!h.nodes.biFromNature, 'no separate source entity control is read');
});

test('direct transfer submits selected PERSONAL source with business-scoped proof upload, not PERSONAL access', async () => {
  const h = harness(), uploads = [];
  h.context.uploadFiles = (files, nature) => { uploads.push(nature); return Promise.resolve(['synthetic-proof.jpg']); };
  h.run(section('    function recordTransfer(', '    function entryDetails('));
  const values = { tr_from_nature: 'PERSONAL', tr_to_nature: 'SANKI', tr_from: personal[0], tr_to: 'Business Bank', tr_amount: '100', tr_class: 'owner_contribution', tr_route: 'direct', tr_date: '2099-01-01', tr_note: 'Owner funding', tr_intermediary: '' };
  for (const [id, value] of Object.entries(values)) h.context.el(id).value = value;
  h.context.el('tr_proof').files = [{}];
  h.context.recordTransfer();
  await flush();
  assert.deepEqual(uploads, ['SANKI']);
  assert.equal(h.calls[0].body.fromNature, 'PERSONAL');
  assert.equal(h.calls[0].body.fromAccount, personal[0]);
  assert.equal(h.calls[0].body.toNature, 'SANKI');
  assert.equal(h.calls[0].body.classification, 'owner_contribution');
  assert.match(line('<select id="tr_from">'), /ownTransferOptions\(\)/);
  assert.doesNotMatch(line('<select id="tr_from">'), /tr_from_nature|tr_to_nature/);
});

test('Prashant unsupported personal/entity combinations are explained and rejected before proof upload; Owner rights stay intact', () => {
  const h = harness(), uploads = [], messages = [];
  h.context.uploadFiles = (files, nature) => { uploads.push(nature); return Promise.resolve(['synthetic-proof.jpg']); };
  h.context.setMsg = message => messages.push(message);
  h.run(section('    function recordTransfer(', '    function entryDetails('));
  h.context.el('tr_from').value = personal[0];
  h.context.el('tr_to').value = 'Samast Bank';
  h.context.el('tr_amount').value = '100';
  h.context.el('tr_intermediary').value = '';
  h.context.el('tr_proof').files = [{}];
  for (const [fromNature, toNature] of [['PERSONAL', 'SAMAST'], ['SAMAST', 'PERSONAL'], ['PERSONAL', 'PERSONAL']]) {
    h.context.el('tr_from').value = fromNature + '|' + (fromNature === 'PERSONAL' ? personal[0] : 'Samast Bank');
    h.context.el('tr_to').value = toNature + '|' + (toNature === 'PERSONAL' ? personal[1] : 'Samast Bank');
    h.context.recordTransfer();
    assert.match(messages.at(-1), /only with a SANKI business account/);
  }
  assert.equal(uploads.length, 0);
  assert.equal(h.calls.length, 0);
  h.context.cfg.isOwner = true;
  assert.equal(h.context.personalTransferScopeAllowed('PERSONAL', 'SAMAST'), true);
  assert.equal(h.context.personalTransferScopeAllowed('SAMAST', 'PERSONAL'), true);
  h.context.cfg.isOwner = false;
  h.context.selectedNature = 'SANKI'; h.context.today = () => '2099-01-01';
  const card = line('<select id="tr_from">').trim().replace(/\+$/, '');
  h.run('var transferCard = ' + card + ';');
  assert.match(h.context.transferCard, /only with a SANKI business account/);
  assert.doesNotMatch(h.context.transferCard, /\+\(!cfg/);
  h.context.cfg.isOwner = true;
  h.run('transferCard = ' + card + ';');
  assert.doesNotMatch(h.context.transferCard, /only with a SANKI business account/);
});

test('Owner PERSONAL-to-SANKI transfers use business proof scope while PERSONAL-to-PERSONAL keeps existing private scope', async () => {
  const h = harness(), uploads = [];
  h.context.cfg.isOwner = true;
  h.context.uploadFiles = (files, nature) => { uploads.push(nature); return Promise.resolve(['synthetic-proof.jpg']); };
  h.run(section('    function recordTransfer(', '    function entryDetails('));
  const values = { tr_from_nature: 'PERSONAL', tr_to_nature: 'SANKI', tr_from: personal[0], tr_to: 'Business Bank', tr_amount: '100', tr_class: 'owner_contribution', tr_route: 'direct', tr_date: '2099-01-01', tr_note: 'Owner funding', tr_intermediary: '' };
  for (const [id, value] of Object.entries(values)) h.context.el(id).value = value;
  h.context.el('tr_proof').files = [{}];
  h.context.recordTransfer();
  await flush();
  assert.deepEqual(uploads, ['SANKI']);
  assert.equal(h.calls[0].body.fromNature, 'PERSONAL');
  assert.equal(h.calls[0].body.toNature, 'SANKI');
  h.context.el('tr_to').value = 'PERSONAL|' + personal[1];
  h.context.el('tr_class').value = 'internal_transfer';
  h.context.recordTransfer();
  await flush();
  assert.deepEqual(uploads, ['SANKI', 'PERSONAL']);
  assert.equal(h.calls[1].body.toNature, 'PERSONAL');
});

test('Owner finalized-transfer restoration submits entity-qualified source without changing the statement', async () => {
  const h = harness(), inputs = { '#finalRestoreSource': { value: 'PERSONAL|' + personal[2] }, '#finalLinkReason': { value: 'Original bank evidence verified' }, '#finalLinkError': {} };
  h.context.nature = 'SANKI'; h.context.account = 'Business Bank'; h.context.recordId = 'TEST-RECORD'; h.context.rowId = 'bank-1';
  h.context.restore = { querySelector: selector => inputs[selector] ||= {} };
  h.context.dialog = { querySelector: selector => inputs[selector], close() {}, remove() {} };
  h.run(line("restore.querySelector('#finalRestoreSave').onclick"));
  h.context.restore.querySelector('#finalRestoreSave').onclick();
  await flush();
  assert.equal(h.calls[0].body.fromNature, 'PERSONAL');
  assert.equal(h.calls[0].body.fromAccount, personal[2]);
  assert.equal(h.calls[0].body.nature, 'SANKI');
  assert.match(section('    window.correctFinalizedBankLink=', '    function renderBankCorrectionHistory('), /if\(!cfg\.isOwner\)return/);
});

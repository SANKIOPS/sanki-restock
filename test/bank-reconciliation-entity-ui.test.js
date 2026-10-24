'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const OwnAccountTransfers = require('../public/own-account-transfers');
const page = fs.readFileSync(path.join(__dirname, '../public/expenses.html'), 'utf8');
const esc = value => String(value == null ? '' : value).replace(/[&<>"']/g, char => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[char]));
function line(marker) { const result = page.split('\n').find(value => value.includes(marker)); assert.ok(result, 'source marker exists: ' + marker); return result; }
function section(start, end) { const from = page.indexOf(start), to = page.indexOf(end, from); assert.ok(from >= 0 && to > from, 'source section exists: ' + start); return page.slice(from, to); }
function harness(overrides = {}) {
  const nodes = {}, calls = [], uploads = [], categoryReads = [], vendorReads = [];
  const context = {
    cfg: { isOwner: false, isAdmin: true, me: 'prashant', approvalNatures: ['SANKI', 'SAMAST'], natures: ['SANKI', 'SAMAST', 'PERSONAL'], accountsByNature: { SANKI: ['Business Bank', 'Counter Cash', 'ICICI Bank 0992'], SAMAST: ['Shared Bank', 'Samast Bank'] }, transferAccountsByNature: { SANKI: ['Business Bank', 'Counter Cash', 'ICICI Bank 0992'], SAMAST: ['Shared Bank', 'Samast Bank'] }, ownerPersonalTransferAccounts: ['IndusInd Bank 7883', 'ICICI Bank 0992', 'ICICI Bank 0993'].map(account => ({ nature: 'PERSONAL', account, label: 'Gagan Sir Personal · ' + account })) },
    bankNature: 'SANKI', bankDraftId: 'TEST-DRAFT', bankExpenseRowId: '', bankExpenseNature: '', bankIncomingRowId: 'bank-1', bankIncomingNature: 'SANKI', bankEntityTaggingActive: () => false,
    bankReconView: { account: 'Business Bank', rows: [{ id: 'bank-1', bank: { debit: 100, credit: 0, date: '2098-06-01', reference: 'BANK-REF', description: 'Bank source narration' } }] },
    el(id) {
      if (nodes[id]) return nodes[id];
      const node = { value: '', _html: '', textContent: '', disabled: false, files: [], style: {}, listeners: {}, options: [], addEventListener(type, fn) { (this.listeners[type] ||= []).push(fn); }, showModal() { this.open = true; }, close() { this.open = false; }, focus() {} };
      Object.defineProperty(node, 'innerHTML', { get() { return this._html; }, set(value) { this._html = String(value); this.options = [...this._html.matchAll(/<option\b([^>]*)>([^<]*)/g)].map(match => ({ value: (match[1].match(/value="([^"]*)"/) || [null, match[2]])[1], selected: /\bselected\b/.test(match[1]) })); if (this.options.length) this.value = (this.options.find(item => item.selected) || this.options[0]).value; } });
      return nodes[id] = node;
    },
    esc, fmt: String, encodeURIComponent, OwnAccountTransfers,
    categoryChoices(nature) { categoryReads.push(nature); return [{ name: nature + ' Category', group: nature + ' Group' }]; },
    categorySuggestions(nature) { categoryReads.push(nature); return '<option value="' + nature + ' Category">'; },
    vendorSuggestions(nature) { vendorReads.push(nature); return '<option value="' + nature + ' Vendor">'; },
    vendorChoices(nature) { vendorReads.push(nature); return [nature + ' Vendor']; },
    vendorNamesForNature(nature) { vendorReads.push(nature); return [nature + ' Vendor']; },
    bankDisplayParts: () => ({ vendor: 'Bank Vendor', particulars: 'Bank particulars' }),
    bankReconRow(id) { return context.bankReconView.rows.find(row => row.id === id); },
    uploadFile(input, nature) { uploads.push(nature); return Promise.resolve('/synthetic-proof.jpg'); },
    api(url, options) { calls.push({ url, body: options && options.body ? JSON.parse(options.body) : null }); return Promise.resolve({ success: false, error: 'Synthetic result; no financial write' }); },
    loadAccountLedger() {}, loadCash() {}, renderBankReconciliation() {},
    syncBankIncomingReceiptType() {}, bankOwnerContributionSource: () => null,
    ExpenseLoans: { sync() {}, reset() {}, refresh() {}, receiptFields: () => ({ loanTerms: {} }) },
    ...overrides
  };
  Object.defineProperty(context.cfg, 'ownTransferAccounts', { get() {
    const personal = context.cfg.ownerPersonalTransferAccounts || [], names = new Set(personal.map(item => item.account)), byNature = context.cfg.transferAccountsByNature || {};
    return Object.entries(byNature).flatMap(([nature, accounts]) => accounts.filter(account => !names.has(account)).map(account => ({ nature, account, label: account + ' · ' + nature }))).concat(personal);
  } });
  context.window = context;
  vm.createContext(context);
  const run = code => vm.runInContext(code, context);
  run(section('    function opts(', '    function payingAccountsForNature('));
  run(line('function bankRowNature('));
  return { context, nodes, calls, uploads, categoryReads, vendorReads, run,
    expense() { ['bankExpenseNatures', 'syncBankExpenseNature'].forEach(name => run(line('function ' + name + '('))); run(line('window.openBankExpense=function')); run(line("el('beSave').addEventListener")); },
    incoming() { ['bankIncomingTransferNature', 'bankIncomingSources', 'bankIncomingSource', 'loadSalaryAdvanceCandidates', 'syncBankIncomingClassification', 'syncBankIncomingSource', 'syncBankIncomingAccount', 'syncBankIncoming'].forEach(name => run(line('function ' + name + '('))); run(line("el('biSave').addEventListener")); }
  };
}
const flush = () => new Promise(resolve => setImmediate(resolve));
function fields(h, values) { Object.entries(values).forEach(([id, value]) => { h.context.el(id).value = value; }); }
function click(h, id) { const node = h.context.el(id); return node.listeners.click.at(-1).call(node, { preventDefault() {}, stopImmediatePropagation() {} }); }

test('only reconciliation expense entity becomes an authorized selector; incoming transfer has one account picker', () => {
  assert.match(page, /<select id="beNature"/); assert.doesNotMatch(page, /<(?:input|select)[^>]*id="biFromNature"/);
  assert.match(page, /<select id="biFromAccount"/); assert.match(page, /id="beDate"[^>]*readonly/); assert.match(page, /id="beAmount"[^>]*readonly/);
  assert.doesNotMatch(page, /<select id="tr_(?:from|to)_nature"/); assert.match(page, /<select id="tr_from"/); assert.match(page, /<select id="tr_to"/);
});
test('expense entity choices use approval permissions, not broader submission natures', () => {
  const h = harness(); h.expense();
  assert.deepEqual(Array.from(h.context.bankExpenseNatures()), ['SANKI', 'SAMAST']);
  h.context.openBankExpense('bank-1'); assert.doesNotMatch(h.nodes.beNature.innerHTML, /PERSONAL/);
  h.context.cfg.approvalNatures.push('PERSONAL'); assert.ok(h.context.bankExpenseNatures().includes('PERSONAL'));
});
test('switching expense entity clears its stale category without discarding the typed vendor or bank figures', () => {
  const h = harness(); h.expense(); h.context.openBankExpense('bank-1');
  fields(h, { beNature: 'SAMAST', beLedger: 'SANKI Category', beVendor: 'Stale vendor' });
  h.context.syncBankExpenseNature();
  assert.equal(h.nodes.beLedger.value, ''); assert.equal(h.nodes.beVendor.value, 'Stale vendor');
  assert.equal(h.nodes.beAccount.value, 'Business Bank'); assert.equal(h.nodes.beAmount.value, 100); assert.equal(h.nodes.beDate.value, '2098-06-01');
  assert.match(h.nodes.beLedgerList.innerHTML, /SAMAST Category/); assert.equal(h.context.bankExpenseNature, 'SAMAST');
  assert.ok(h.categoryReads.includes('SAMAST'));
});
test('expense save validates and uploads the selected entity and snapshots its payload during upload', async () => {
  let release; const h = harness({ uploadFile(input, nature) { h.uploads.push(nature); return new Promise(resolve => { release = resolve; }); } });
  h.expense(); h.context.openBankExpense('bank-1');
  fields(h, { f_nature: 'SANKI', beNature: 'SAMAST', beLedger: 'SAMAST Category', beVendor: 'Selected Vendor', beParticulars: 'Selected particulars', beRemark: 'Bank verified expense', beBill: 'printed', bePaymentType: 'UPI' }); h.context.el('beBillPhoto').files = [{}];
  click(h, 'beSave'); assert.deepEqual(h.uploads, ['SAMAST']); assert.equal(h.nodes.beNature.disabled, true);
  click(h, 'beSave'); assert.deepEqual(h.uploads, ['SAMAST'], 'a second click cannot queue another bill/payment');
  fields(h, { beNature: 'SANKI', beVendor: 'Changed after click', beLedger: 'SANKI Category' }); release('/synthetic-proof.jpg'); await flush();
  assert.equal(h.calls.length, 1); assert.equal(h.calls[0].body.nature, 'SAMAST'); assert.equal(h.calls[0].body.vendor, 'Selected Vendor'); assert.equal(h.calls[0].body.ledger, 'SAMAST Category');
  assert.equal(h.calls[0].body.rowId, 'bank-1'); assert.equal(h.calls[0].body.draftId, 'TEST-DRAFT'); assert.equal(h.nodes.beNature.disabled, false);
});
test('an unauthorized or stale category cannot upload a bill or send a create-expense request', () => {
  const h = harness(); h.expense(); h.context.openBankExpense('bank-1'); h.context.el('beBillPhoto').files = [{}];
  fields(h, { beNature: 'PERSONAL', beLedger: 'PERSONAL Category', beVendor: 'Vendor', beParticulars: 'Bill', beRemark: 'Reason' }); click(h, 'beSave');
  assert.equal(h.uploads.length, 0); assert.equal(h.calls.length, 0);
  fields(h, { beNature: 'SAMAST', beLedger: 'SANKI Category' }); click(h, 'beSave'); assert.equal(h.uploads.length, 0); assert.equal(h.calls.length, 0);
});
test('shared 7883 expense forms expose only authorized shared entities and do not guess untagged ownership', () => {
  const h = harness({ bankEntityTaggingActive: () => true }); h.expense();
  assert.deepEqual(Array.from(h.context.bankExpenseNatures()), ['SAMAST']); h.context.openBankExpense('bank-1'); assert.equal(h.nodes.beNature.value, '');
  h.context.cfg.isOwner = true; h.context.cfg.approvalNatures.push('PERSONAL'); assert.deepEqual(Array.from(h.context.bankExpenseNatures()), ['SAMAST', 'PERSONAL']);
  h.context.bankReconView.rows[0].entityTag = { nature: 'PERSONAL' }; h.context.openBankExpense('bank-1'); assert.equal(h.nodes.beNature.value, 'PERSONAL');
});
test('incoming account labels are canonical and ambiguous shared names remain unavailable until the registry is reviewed', () => {
  const h = harness(); h.context.cfg.isOwner = true; h.context.cfg.approvalNatures.push('PERSONAL'); h.context.cfg.transferAccountsByNature.PERSONAL = ['Shared Bank', 'ICICI Bank 0992']; h.incoming(); h.context.syncBankIncomingAccount();
  const sources = h.context.bankIncomingSources();
  assert.ok(!sources.some(row => row.account === 'Shared Bank'));
  assert.doesNotMatch(h.nodes.biFromAccount.innerHTML, /value="(?:SAMAST|PERSONAL)\|Shared Bank"/);
  fields(h, { biFromAccount: 'SAMAST|Shared Bank' }); assert.equal(h.context.bankIncomingSource().account, '');
  fields(h, { biFromAccount: 'PERSONAL|Shared Bank' }); assert.equal(h.context.bankIncomingSource().account, '');
  assert.ok(!sources.some(row => row.account === 'Business Bank'), 'same physical receiving account is excluded');
  fields(h, { biFromAccount: 'PERSONAL|Private Unlisted Bank' }); assert.equal(h.context.bankIncomingSource().account, '');
});
test('Admin PERSONAL source choices remain restricted to the known SANKI-pair exception', () => {
  const h = harness(); h.incoming();
  assert.deepEqual(Array.from(h.context.bankIncomingSources().filter(row => row.nature === 'PERSONAL').map(row => row.account)), ['IndusInd Bank 7883', 'ICICI Bank 0992', 'ICICI Bank 0993']);
  h.context.bankReconView.account = 'Samast Bank'; assert.equal(h.context.bankIncomingSources().filter(row => row.nature === 'PERSONAL').length, 0);
  h.context.bankReconView.account = 'Business Bank'; h.context.cfg.ownerPersonalTransferAccounts = []; assert.equal(h.context.bankIncomingSources().filter(row => row.nature === 'PERSONAL').length, 0);
});
test('incoming transfer submits entity decoded from its selected account, never a removed entity dropdown', async () => {
  const h = harness(); h.incoming(); h.context.syncBankIncomingAccount();
  fields(h, { biSourceKind: 'internal', biFromAccount: 'PERSONAL|ICICI Bank 0993', biClassification: 'owner_contribution', biNote: 'Verified contribution', biExternalSource: '', biSalaryAdvance: '' });
  click(h, 'biSave'); await flush(); assert.equal(h.calls.length, 1);
  assert.equal(h.calls[0].body.fromNature, 'PERSONAL'); assert.equal(h.calls[0].body.fromAccount, 'ICICI Bank 0993'); assert.equal(h.calls[0].body.classification, 'owner_contribution');
  assert.equal(h.calls[0].body.ownAccountTransfer, true);
  assert.ok(!h.nodes.biFromNature, 'no removed entity input was read');
  fields(h, { biFromAccount: '' }); click(h, 'biSave'); await flush(); assert.equal(h.calls.length, 1, 'empty source is not submitted');
  fields(h, { biFromAccount: 'ICICI Bank 0993' }); click(h, 'biSave'); await flush(); assert.equal(h.calls.length, 1, 'an unqualified ambiguous name is not submitted');
});
test('salary advance lookup and transfer submit use canonical decoded source account', async () => {
  const h = harness(); h.incoming(); h.context.syncBankIncomingAccount();
  fields(h, { biSourceKind: 'internal', biFromAccount: 'SANKI|Counter Cash', biClassification: 'salary_advance_funding', biAmount: '100', biNote: 'Salary funding transfer', biSalaryAdvance: 'ADV-001' });
  h.context.syncBankIncomingClassification(); await flush(); assert.match(h.calls[0].url, /sourceAccount=Counter%20Cash/); assert.doesNotMatch(h.calls[0].url, /SANKI%7C/);
  h.context.el('biSalaryAdvance').value = 'ADV-001'; click(h, 'biSave'); await flush(); const saved = h.calls.find(call => call.body);
  assert.equal(saved.body.fromNature, 'SANKI'); assert.equal(saved.body.fromAccount, 'Counter Cash'); assert.equal(saved.body.salaryAdvanceId, 'ADV-001'); assert.equal(saved.body.classification, 'salary_advance_funding');
  assert.equal(Object.hasOwn(saved.body, 'ownAccountTransfer'), false, 'special salary funding retains its own advance-link workflow, not generic ownership treatment');
  assert.match(line('window.openCounterCashBankTransfer=function'), /SANKI\|Counter Cash/); assert.doesNotMatch(line('window.openCounterCashBankTransfer=function'), /biFromNature/);
});
test('changing incoming source clears a stale salary advance and refreshes its classification', () => {
  const h = harness(); h.incoming(); h.context.syncBankIncomingAccount();
  fields(h, { biFromAccount: 'PERSONAL|ICICI Bank 0993', biSalaryAdvance: 'STALE-ADVANCE', biClassification: 'salary_advance_funding' });
  h.context.syncBankIncomingSource(); assert.equal(h.nodes.biSalaryAdvance.value, ''); assert.equal(h.nodes.biClassification.value, 'owner_contribution');
});
test('Counter Cash shortcut still selects the encoded cash source and records no transaction merely by opening', () => {
  const h = harness(); h.incoming(); h.context.bankReconView.rows[0].bank = { date: '2098-06-01', debit: 0, credit: 100, reference: 'CASH-DEPOSIT', description: 'Cash deposit' };
  h.run(line('window.openBankIncoming=function')); h.run(line('window.openCounterCashBankTransfer=function')); h.context.openCounterCashBankTransfer('bank-1');
  assert.equal(h.nodes.biFromAccount.value, 'SANKI|Counter Cash'); assert.equal(h.nodes.biClassification.value, 'internal_transfer'); assert.equal(h.nodes.biSourceKind.value, 'internal'); assert.equal(h.calls.length, 0);
  assert.ok(!h.nodes.biFromNature);
});

test('bulk draft reporting assignment preselects only an authorized expense entity without altering the account', () => {
  const h = harness(); h.expense();
  h.context.bankReconView.rows[0].entityAssignment = { nature: 'SAMAST', source: 'manual_bulk' };
  const before = JSON.stringify(h.context.bankReconView);
  h.context.openBankExpense('bank-1');
  assert.equal(h.nodes.beNature.value, 'SAMAST');
  assert.equal(h.nodes.beAccount.value, 'Business Bank');
  assert.match(h.nodes.beLedgerList.innerHTML, /SAMAST Category/);
  assert.equal(JSON.stringify(h.context.bankReconView), before);
  assert.equal(h.calls.length, 0, 'opening a form does not apply any transaction or bulk assignment');
  h.context.bankReconView.rows[0].entityAssignment = { nature: 'PERSONAL' };
  h.context.openBankExpense('bank-1');
  assert.doesNotMatch(h.nodes.beNature.innerHTML, /PERSONAL/);
  assert.notEqual(h.nodes.beNature.value, 'PERSONAL');
});

test('incoming external receipt needs no entity input or pre-tag request; source and note are still mandatory', async () => {
  const h = harness({ bankEntityTaggingActive: () => true }); h.incoming();
  h.context.bankReconView.rows[0].bank = { date: '2098-06-01', debit: 0, credit: 100, reference: 'CREDIT-REF' };
  fields(h, { biSourceKind: 'external', biExternalSource: 'Supplier refund', biReceiptType: 'refund', biCategory: 'Refund Received', biNote: 'Verified incoming refund reference', biSalaryAdvance: '' });
  click(h, 'biSave'); await flush();
  assert.equal(h.calls.length, 1);
  assert.equal(h.calls[0].url, '/api/expenses/bank-statements/create-incoming');
  assert.equal(h.calls[0].body.sourceKind, 'external');
  for (const key of ['nature', 'targetNature', 'postingNature', 'ownAccountTransfer']) assert.equal(Object.hasOwn(h.calls[0].body, key), false, key);
  fields(h, { biNote: '' }); click(h, 'biSave'); await flush(); assert.equal(h.calls.length, 1);
  fields(h, { biNote: 'Verified incoming refund', biExternalSource: '' }); click(h, 'biSave'); await flush(); assert.equal(h.calls.length, 1);
  assert.match(page, /Entity classification is optional for incoming receipts and transfers/);
});

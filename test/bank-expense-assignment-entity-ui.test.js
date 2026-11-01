'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const OwnAccountTransfers = require('../public/own-account-transfers');
const page = fs.readFileSync(path.join(__dirname, '../public/expenses.html'), 'utf8');
const esc = value => String(value == null ? '' : value).replace(/[&<>"']/g, char => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[char]));
function line(marker) { const value = page.split('\n').find(item => item.includes(marker)); assert.ok(value, 'source marker exists: ' + marker); return value; }
function section(start, end) { const from = page.indexOf(start), to = page.indexOf(end, from); assert.ok(from >= 0 && to > from); return page.slice(from, to); }
function harness(overrides = {}) {
  const nodes = {}, calls = [], saved = new Map();
  const context = {
    cfg: { isAdmin: true, isOwner: false, approvalNatures: ['SANKI', 'SAMAST', 'PERSONAL'], ownTransferAccounts: [{ nature: 'PERSONAL', account: 'IndusInd Bank 7883' }, { nature: 'SANKI', account: 'Counter Cash' }] },
    bankNature: 'PERSONAL', bankDraftId: 'DRAFT-1', bankReconForm: null,
    bankReconView: { account: 'IndusInd Bank 7883', rows: [{ id: 'bank-1', bank: { date: '2026-08-25', debit: 500, credit: 0, reference: 'REF-1', description: 'Verified bank payment' } }, { id: 'bank-2', bank: { date: '2026-08-26', debit: 600, credit: 0, reference: 'REF-2', description: 'Another payment' } }] },
    bankEntityTaggingActive: () => true,
    el(id) {
      if (nodes[id]) return nodes[id];
      const labels = {};
      const node = { value: '', disabled: false, style: {}, open: false, textContent: '', className: '', listeners: {}, options: [], selectedOptions: [], querySelector(selector) { return labels[selector] ||= { disabled: false, textContent: '' }; }, addEventListener(type, fn, capture) { (this.listeners[type] ||= []).push({ fn, capture: !!capture }); }, showModal() { this.open = true; }, close() { this.open = false; } };
      Object.defineProperty(node, 'innerHTML', { get() { return this.html || ''; }, set(value) { this.html = String(value); this.options = [...this.html.matchAll(/<option\b([^>]*)>([^<]*)/g)].map(match => ({ value: (match[1].match(/value="([^"]*)"/) || [null, match[2]])[1] })); if (this.options.length) this.value = this.options[0].value; } });
      return nodes[id] = node;
    },
    esc, fmt: String, OwnAccountTransfers,
    sessionStorage: { setItem(key, value) { saved.set(key, value); }, getItem(key) { return saved.get(key) || null; }, removeItem(key) { saved.delete(key); } },
    bankReconRow(id) { return context.bankReconView.rows.find(row => row.id === id); },
    bankRowNature(row) { return row && row.entityTag && row.entityTag.nature || context.bankNature; },
    bankReconAmount(row) { return Number(row && row.bank && (row.bank.debit || row.bank.credit) || 0); },
    categorySuggestions: nature => '<option value="' + nature + ' Category">',
    renderBankAppCandidates() {},
    transferCounterpartOptions() { return '<option value="SANKI|Counter Cash">SANKI · Counter Cash</option>'; },
    transferCounterpart(value) { const parts = String(value || '').split('|'); return { nature: parts.length > 1 ? parts[0] : '', account: parts.length > 1 ? parts[1] : '' }; },
    ownTransferOptions() { return OwnAccountTransfers.options(context.cfg, context.bankReconView.account); },
    ownTransferAccount(value) { return OwnAccountTransfers.find(context.cfg, value); },
    bankTransferOptions() { return OwnAccountTransfers.options(context.cfg, context.bankReconView.account); },
    bankTransferCounterpart(value) { return Object.assign({ own: true }, OwnAccountTransfers.find(context.cfg, value)); },
    bankTransferNature() { return 'PERSONAL'; },
    bankOwnAccountRegistered() { return true; },
    api(url, options) { calls.push({ url, body: JSON.parse(options.body) }); return Promise.resolve({ success: false, error: 'Synthetic preview, no write' }); },
    finishBankReconForm() { context.el('brSave').disabled = false; },
    prepareBankAmountAuthority: payload => Promise.resolve(payload),
    bankEntityTagLabel: tag => tag.nature || 'Needs review', bankEntityTagMeta: () => '',
    ...overrides
  };
  context.window = context;
  vm.createContext(context);
  const run = source => vm.runInContext(source, context);
  run(line('function bankExpenseNatures('));
  run(line('function saveBankReconForm('));
  run(section('    function syncBankAssignmentExpenseNature(){', '    function clearBankReconForm('));
  run(line('function updateBankReconFields('));
  run(line('function showBankReconForm('));
  run(line('function defaultPaytmReceiptDate('));
  run(line('window.openBankAssignment=function'));
  run(line('function submitBankReconForm('));
  run(line("el('brSave').addEventListener('click',function(ev){if(!(bankReconForm&&bankReconForm.mode==='assign'"));
  run(section('    function showBankEntityTagEditor(rowId){', '    var openBankReviewBeforeEntityTag='));
  return { context, nodes, calls, saved,
    fields(values) { Object.entries(values).forEach(([id, value]) => { context.el(id).value = value; }); },
    event(id, type) { let stopped = false; const event = { preventDefault() {}, stopImmediatePropagation() { stopped = true; } }; for (const item of [...(context.el(id).listeners[type] || [])].sort((a, b) => Number(b.capture) - Number(a.capture))) { if (stopped) break; item.fn.call(context.el(id), event); } }
  };
}
const flush = () => new Promise(resolve => setImmediate(resolve));

test('bulk draft entity assignment supplies expense category context but never changes transfer account context', () => {
  const h = harness();
  h.context.bankReconView.rows[0].entityAssignment = { nature: 'SAMAST' };
  h.context.openBankAssignment('bank-1', 'category');
  assert.equal(h.nodes.brExpenseNature.value, 'SAMAST');
  assert.match(h.nodes.brLedgerList.innerHTML, /SAMAST Category/);
  h.context.openBankAssignment('bank-1', 'internal_transfer');
  assert.equal(h.nodes.brExpenseNatureWrap.style.display, 'none');
  assert.equal(h.context.bankTransferNature(h.context.bankReconView.rows[0]), 'PERSONAL');
  assert.equal(h.calls.length, 0);
});

test('expense category assignment has its own selector; transfers and links hide it', () => {
  const h = harness();
  h.context.openBankAssignment('bank-1', 'category');
  assert.equal(h.nodes.brExpenseNatureWrap.style.display, 'flex');
  h.context.openBankAssignment('bank-1', 'internal_transfer');
  assert.equal(h.nodes.brExpenseNatureWrap.style.display, 'none');
  h.context.showBankReconForm({ draftId: 'DRAFT-1', rowId: 'bank-1', mode: 'link', side: 'bank' });
  assert.equal(h.nodes.brExpenseNatureWrap.style.display, 'none');
  assert.match(page, /<select id="brExpenseNature"/);
  assert.doesNotMatch(page, /<(?:select|input)\b[^>]*id="(?:brTransferNature|brOtherNature|brFromNature|brToNature)"/);
});

test('own-transfer summary uses account context rather than the reporting-container entity and switches back for expenses', () => {
  const h = harness({ bankNature: 'SAMAST' });
  h.context.openBankAssignment('bank-1', 'internal_transfer');
  assert.match(h.nodes.bankReconDlgSummary.textContent, /IndusInd Bank 7883 · account-based transfer \(PERSONAL\)/);
  assert.doesNotMatch(h.nodes.bankReconDlgSummary.textContent, /SAMAST/);
  h.fields({ brKind: 'category' }); h.context.updateBankReconFields();
  assert.match(h.nodes.bankReconDlgSummary.textContent, /SAMAST/);
  assert.doesNotMatch(h.nodes.bankReconDlgSummary.textContent, /account-based transfer/);
  assert.equal(h.calls.length, 0);
});

test('shared untagged expenses require explicit entity; only authorized choices are offered', () => {
  const h = harness();
  h.context.cfg.approvalNatures = ['SANKI', 'SAMAST'];
  h.context.openBankAssignment('bank-1', 'category');
  assert.equal(h.nodes.brExpenseNature.value, '', 'account context does not guess untagged shared ownership');
  assert.deepEqual(Array.from(h.nodes.brExpenseNature.options, option => option.value), ['', 'SAMAST']);
  assert.equal(h.context.bankReconForm.expenseNature, '');
  assert.equal(h.calls.length, 0);
});

test('known tags may suggest an authorized expense entity, but cannot expose unauthorized tags', () => {
  const h = harness();
  h.context.bankReconView.rows[0].entityTag = { nature: 'SAMAST' };
  h.context.openBankAssignment('bank-1', 'category');
  assert.equal(h.nodes.brExpenseNature.value, 'SAMAST');
  h.context.cfg.approvalNatures = ['PERSONAL'];
  h.context.showBankReconForm({ draftId: 'DRAFT-1', rowId: 'bank-1', mode: 'assign', kind: 'category' });
  assert.equal(h.nodes.brExpenseNature.value, '');
  assert.deepEqual(Array.from(h.nodes.brExpenseNature.options, option => option.value), ['', 'PERSONAL']);
});

test('changing expense entity clears stale category and persists the new choice in the unfinished form', () => {
  const h = harness();
  h.context.openBankAssignment('bank-1', 'category');
  h.fields({ brExpenseNature: 'SAMAST', brLedger: 'PERSONAL Category' });
  h.event('brExpenseNature', 'change');
  assert.equal(h.nodes.brLedger.value, '');
  assert.match(h.nodes.brLedgerList.innerHTML, /SAMAST Category/);
  assert.equal(h.context.bankReconForm.expenseNature, 'SAMAST');
  assert.equal(JSON.parse(h.saved.get('sankiBankReconForm')).expenseNature, 'SAMAST');
  h.fields({ brExpenseNature: '' });
  h.event('brExpenseNature', 'change');
  assert.equal(h.nodes.brLedgerList.innerHTML, '');
});

test('new row forms cannot inherit a previous row entity; same-row unfinished forms restore their deliberate choice', () => {
  const h = harness();
  h.context.openBankAssignment('bank-1', 'category');
  h.fields({ brExpenseNature: 'SAMAST', brLedger: 'SAMAST Category' });
  h.event('brExpenseNature', 'change');
  h.fields({ brLedger: 'SAMAST Category' });
  h.context.saveBankReconForm();
  h.context.openBankAssignment('bank-1', 'category');
  assert.equal(h.nodes.brExpenseNature.value, 'SAMAST');
  assert.equal(h.nodes.brLedger.value, 'SAMAST Category');
  h.context.openBankAssignment('bank-2', 'category');
  assert.equal(h.nodes.brExpenseNature.value, '');
  assert.equal(h.nodes.brLedger.value, '');
  assert.equal(h.context.bankReconForm.rowId, 'bank-2');
  assert.equal(h.context.bankReconForm.expenseNature, '');
  assert.equal(JSON.parse(h.saved.get('sankiBankReconForm')).expenseNature, '');
});

test('category assignment submits the selected entity without requiring or calling a separate pre-tag API', async () => {
  const h = harness();
  h.context.openBankAssignment('bank-1', 'category');
  h.fields({ brExpenseNature: 'SAMAST', brLedger: 'SAMAST Category', brRemark: 'Business expense confirmed from bank' });
  h.context.submitBankReconForm();
  await flush();
  assert.equal(h.calls.length, 1);
  assert.equal(h.calls[0].url, '/api/expenses/bank-statements/resolve');
  assert.equal(h.calls[0].body.nature, 'SAMAST');
  assert.equal(h.calls[0].body.action, 'create_adjustment');
  assert.equal(h.calls[0].body.category, 'SAMAST Category');
  assert.equal(h.calls[0].body.rowId, 'bank-1');
  assert.equal(h.context.bankReconView.rows[0].entityTag, undefined);
  assert.equal(h.calls.some(call => call.url.endsWith('/retag')), false);
});

test('category assignment refuses a missing or unauthorized entity and always requires its reason', () => {
  const h = harness();
  h.context.openBankAssignment('bank-1', 'category');
  h.fields({ brLedger: 'SAMAST Category', brRemark: 'Business expense reason' });
  h.context.submitBankReconForm();
  assert.match(h.nodes.brMsg.textContent, /Choose the expense entity/);
  h.fields({ brExpenseNature: 'SANKI' });
  h.context.submitBankReconForm();
  assert.equal(h.calls.length, 0);
  h.fields({ brExpenseNature: 'SAMAST', brRemark: '' });
  h.context.submitBankReconForm();
  assert.match(h.nodes.brMsg.textContent, /Enter a transaction remark/);
  assert.equal(h.calls.length, 0);
  assert.equal(h.nodes.brSave.disabled, false);
});

test('transfer capture handler ignores any stale expense selector and submits only the entity-qualified counterpart', async () => {
  const h = harness();
  h.context.openBankAssignment('bank-1', 'category');
  h.fields({ brExpenseNature: 'SAMAST' });
  h.event('brExpenseNature', 'change');
  h.context.openBankAssignment('bank-1', 'internal_transfer');
  h.fields({ brOtherAccount: 'SANKI|Counter Cash', brPrincipal: '500', brCharge: '0', brRemark: 'Actual transfer verified in bank' });
  h.event('brSave', 'click');
  await flush();
  assert.equal(h.calls.length, 1);
  const body = h.calls[0].body;
  assert.equal(body.action, 'create_internal_transfer');
  assert.equal(body.otherNature, 'SANKI');
  assert.equal(body.otherAccount, 'Counter Cash');
  assert.equal(Object.hasOwn(body, 'nature'), false, 'no stale expense entity can choose the transfer source entity');
  assert.equal(Object.hasOwn(body, 'expenseNature'), false);
  assert.equal(h.nodes.brExpenseNatureWrap.style.display, 'none');
});

test('optional entity correction panel is a collapsed details element and does not write on review', () => {
  const openingTag = page.match(/<details\b[^>]*id="bankEntityTagEditor"[^>]*>/);
  assert.ok(openingTag);
  assert.doesNotMatch(openingTag[0], /\sopen(?:\s|=|>)/);
  const h = harness();
  h.context.el('bankEntityTagEditor').open = true;
  h.context.showBankEntityTagEditor('bank-1');
  assert.equal(h.nodes.bankEntityTagEditor.open, false);
  assert.equal(h.nodes.bankEntityTagEditor.style.display, 'block');
  assert.equal(h.calls.length, 0);
});
test('app-only missing-bank rows do not expose the bank entity correction editor', () => {
  const h = harness();
  h.context.bankReconView.rows[0].bank = null;h.context.bankReconView.rows[0].app={id:'EX-1/PAY-001'};
  h.context.showBankEntityTagEditor('bank-1');
  assert.equal(h.nodes.bankEntityTagEditor.style.display, 'none');
  assert.equal(h.context.bankEntityTagRowId, '');
  assert.equal(h.calls.length, 0);
});
test('app-only transfer rows expose source-level entity correction', () => {
  const h = harness();
  h.context.bankReconView.rows[0].bank=null;h.context.bankReconView.rows[0].app={id:'TR-00172'};
  h.context.showBankEntityTagEditor('bank-1');
  assert.equal(h.nodes.bankEntityTagEditor.style.display,'block');
  assert.match(h.nodes.bankEntityTagCurrent.innerHTML,/TR-00172/);
  assert.match(h.nodes.bankEntityTagMsg.textContent,/updates its source record/);
});

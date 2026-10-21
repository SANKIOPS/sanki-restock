'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const page = fs.readFileSync(path.join(__dirname, '../public/expenses.html'), 'utf8');
const esc = value => String(value == null ? '' : value).replace(/[&<>"']/g, char => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[char]));
const unesc = value => String(value).replace(/&quot;|&#39;|&lt;|&gt;|&amp;/g, char => ({ '&quot;': '"', '&#39;': "'", '&lt;': '<', '&gt;': '>', '&amp;': '&' }[char]));
function section() {
  const start = page.indexOf('    // Entity-only bulk corrections');
  const end = page.indexOf('    function openPersonalHistoricalBulk', start);
  assert.ok(start >= 0 && end > start, 'bulk entity UI section exists');
  return page.slice(start, end);
}
function node() {
  const item = { value: '', checked: false, disabled: false, style: {}, listeners: {}, textContent: '', className: '', open: false, html: '', options: [], boxes: [] };
  item.addEventListener = (type, fn) => { (item.listeners[type] ||= []).push(fn); };
  item.querySelectorAll = selector => selector === '.bank-entity-bulk-row' ? item.boxes : [];
  item.querySelector = () => null;
  item.showModal = () => { item.open = true; };
  item.close = () => { item.open = false; };
  Object.defineProperty(item, 'innerHTML', {
    get() { return item.html; },
    set(value) {
      item.html = String(value);
      item.options = [...item.html.matchAll(/<option\b([^>]*)>([^<]*)/g)].map(match => ({ value: unesc((match[1].match(/value="([^"]*)"/) || [null, match[2]])[1]), selected: /\bselected\b/.test(match[1]) }));
      if (item.options.length) item.value = (item.options.find(option => option.selected) || item.options[0]).value;
    }
  });
  return item;
}
function checkbox(item, options = {}) {
  const row = { style: { display: options.hidden ? 'none' : '' } };
  return { checked: !!options.checked, closest: () => row, getAttribute(name) { return name === 'data-item' ? JSON.stringify(item) : name === 'data-finalized' ? String(!!options.finalized) : null; }, row };
}
function harness(overrides = {}) {
  const nodes = {}, calls = [], refreshes = [];
  const context = {
    cfg: { isAdmin: true, isOwner: false, me: 'prashant', approvalNatures: ['SANKI', 'SAMAST', 'PERSONAL'], canManagePersonalAccounting: true },
    bankNature: 'PERSONAL', bankDraftId: 'DRAFT-1',
    bankReconView: { account: 'IndusInd Bank 7883', rows: [{ id: 'bank-1', bank: { date: '2026-08-25', credit: 200000, debit: 0, balance: 210000, reference: 'SOURCE-REF' }, linkedRecordIds: ['RECEIPT-1'], ledger: { id: 'RECEIPT-1', amount: 200000 }, status: 'resolved' }] },
    el(id) { return nodes[id] ||= node(); },
    esc, fmt: value => '₹' + Number(value || 0).toFixed(2),
    api(url, options) { calls.push({ url, method: options.method, body: JSON.parse(options.body) }); return Promise.resolve({ success: true, fingerprint: 'verified-fingerprint', updated: 1, changes: [{ rowId: 'bank-1', date: '2026-08-25', amount: 200000, fromNature: 'PERSONAL', toNature: 'SAMAST', linkedRecordIds: ['RECEIPT-1'] }] }); },
    loadBankStatement(id) { refreshes.push(['statement', id]); },
    loadAccountLedger() { refreshes.push(['ledger']); },
    renderBankHistory() { return 'existing finalized history'; },
    bankHistoryRows(log) { return log.reviewedRows || []; },
    ...overrides
  };
  context.window = context;
  vm.createContext(context);
  vm.runInContext(section(), context);
  context.el('bs_account').value = context.bankReconView.account;
  const h = { context, nodes, calls, refreshes };
  h.fields = values => { Object.entries(values).forEach(([id, value]) => { context.el(id).value = value; }); };
  h.event = (id, type, event = {}) => { for (const fn of context.el(id).listeners[type] || []) fn.call(context.el(id), { preventDefault() {}, ...event }); };
  h.select = (items = [{ draftId: 'DRAFT-1', rowId: 'bank-1' }], options = {}) => {
    const scope = options.scope || 'draft';
    context.el(scope === 'history' ? 'bs_history' : 'bs_table').boxes = items.map(item => checkbox(item, { checked: true, ...options }));
    return h;
  };
  h.open = (scope = 'draft') => { context.openBankEntityBulk(scope); return h; };
  h.valid = () => { h.fields({ bankEntityBulkNature: 'SAMAST', bankEntityBulkReason: 'Correct verified entity assignment' }); return h; };
  return h;
}
const flush = () => new Promise(resolve => setImmediate(resolve));

test('all inline expense-page scripts compile, including the bulk UI and history wrapper', () => {
  let count = 0;
  for (const match of page.matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script>/gi)) {
    if (/\bsrc\s*=/.test(match[1]) || !match[2].trim()) continue;
    assert.doesNotThrow(() => new vm.Script(match[2], { filename: 'expenses-inline-' + count + '.js' }));
    count++;
  }
  assert.ok(count > 0);
  assert.match(page, /id="bankEntityBulkDlg"/);
  assert.match(page, /id="bankEntityBulkReason"/);
});

test('only Owner or Admin can see shared 7883 entity correction, and finalized entries stay Owner-only', () => {
  const h = harness();
  for (const account of ['IndusInd Bank 7883', 'Gagan personal 7883']) assert.equal(h.context.bankEntityBulkAllowed(account, false), true);
  for (const account of ['IndusInd Bank 8181', 'Counter Cash', '', '78830']) assert.equal(h.context.bankEntityBulkAllowed(account, false), false);
  assert.equal(h.context.bankEntityBulkAllowed('IndusInd Bank 7883', true), false);
  h.context.cfg.isAdmin = false;
  assert.equal(h.context.bankEntityBulkAllowed('IndusInd Bank 7883', false), false);
  h.context.cfg.isOwner = true;
  assert.equal(h.context.bankEntityBulkAllowed('IndusInd Bank 7883', true), true);
  h.select([{ recordId: 'FINAL-1', rowId: 'row-1' }], { finalized: true, scope: 'history' }).open('history');
  assert.equal(h.nodes.bankEntityBulkDlg.open, true);
  const admin = harness().select([{ recordId: 'FINAL-1', rowId: 'row-1' }], { finalized: true, scope: 'history' }).open('history');
  assert.equal(admin.nodes.bankEntityBulkDlg.open, false, 'Admin cannot open even a manually checked finalized selection');
});

test('target choices follow approval authorization and never expose unrelated entity targets', () => {
  const h = harness();
  assert.deepEqual(Array.from(h.context.bankEntityBulkTargets()), ['SAMAST', 'PERSONAL']);
  h.context.cfg.canManagePersonalAccounting = false;
  assert.deepEqual(Array.from(h.context.bankEntityBulkTargets()), ['SAMAST']);
  h.context.cfg.isOwner = true;
  assert.deepEqual(Array.from(h.context.bankEntityBulkTargets()), ['SAMAST', 'PERSONAL']);
  h.context.cfg.approvalNatures = ['SANKI'];
  assert.deepEqual(Array.from(h.context.bankEntityBulkTargets()), []);
  h.select().open();
  assert.equal(h.nodes.bankEntityBulkDlg.open, false, 'no dialog is offered without an authorized target');
});

test('checkbox attributes safely round-trip quoted identifiers without HTML injection', () => {
  const h = harness();
  const item = { recordId: 'FINAL-"<&', rowId: "row-'\"><script>" };
  const html = h.context.bankEntityBulkCheckbox(item, true);
  assert.match(html, /data-finalized="true"/);
  assert.doesNotMatch(html, /<script>/);
  const encoded = html.match(/data-item="([^"]*)"/)[1];
  assert.deepEqual(JSON.parse(unesc(encoded)), item);
  assert.match(html, /onchange="updateBankEntityBulkToolbar\(\)"/);
});

test('selection deduplicates identical source rows while retaining same row IDs in distinct statements', () => {
  const h = harness().select([
    { draftId: 'DRAFT-1', rowId: 'bank-1' },
    { draftId: 'DRAFT-1', rowId: 'bank-1' },
    { draftId: 'DRAFT-2', rowId: 'bank-1' }
  ]).open();
  assert.equal(h.context.bankEntityBulkState.items.length, 2);
  assert.equal(h.nodes.bankEntityBulkSummary.textContent.startsWith('2 selected'), true);
});

test('select all visible respects current history search and never selects another table scope', () => {
  const h = harness();
  const draft = [checkbox({ draftId: 'DRAFT-1', rowId: 'bank-1' }), checkbox({ draftId: 'DRAFT-1', rowId: 'bank-2' }, { hidden: true })];
  const history = [checkbox({ recordId: 'FINAL-1', rowId: 'bank-3' }), checkbox({ recordId: 'FINAL-1', rowId: 'bank-4' }, { hidden: true })];
  h.context.el('bs_table').boxes = draft;
  h.context.el('bs_history').boxes = history;
  h.context.selectBankEntityBulkRows('history', true);
  assert.deepEqual(history.map(box => box.checked), [true, false]);
  assert.deepEqual(draft.map(box => box.checked), [false, false]);
  assert.equal(h.nodes.bankEntityBulkCount_history.textContent, '1 selected');
  h.context.selectBankEntityBulkRows('draft', true);
  assert.deepEqual(draft.map(box => box.checked), [true, false]);
  history[1].checked = true;
  h.context.selectBankEntityBulkRows('history', false);
  assert.deepEqual(history.map(box => box.checked), [false, false], 'clear also clears search-hidden selection');
  assert.equal(h.nodes.bankEntityBulkOpen_history.disabled, true);
  assert.equal(draft[0].checked, true);
});

test('opening only prepares the dialog; preview uses dryRun and cannot change bank amounts, dates or links', async () => {
  const h = harness().select().open().valid();
  const before = JSON.stringify(h.context.bankReconView);
  assert.equal(h.calls.length, 0);
  assert.equal(h.nodes.bankEntityBulkSave.disabled, true);
  h.event('bankEntityBulkPreview', 'click');
  await flush();
  assert.equal(h.calls.length, 1);
  assert.equal(h.calls[0].url, '/api/expenses/bank-statements/retag-bulk');
  assert.equal(h.calls[0].method, 'POST');
  assert.deepEqual(h.calls[0].body, { account: 'IndusInd Bank 7883', nature: 'PERSONAL', targetNature: 'SAMAST', reason: 'Correct verified entity assignment', items: [{ draftId: 'DRAFT-1', rowId: 'bank-1' }], dryRun: true });
  assert.equal(JSON.stringify(h.context.bankReconView), before);
  assert.equal(h.context.bankEntityBulkState.fingerprint, 'verified-fingerprint');
  assert.equal(h.nodes.bankEntityBulkSave.disabled, false);
  assert.match(h.nodes.bankEntityBulkRows.innerHTML, /RECEIPT-1/);
  assert.equal(h.refreshes.length, 0);
});

test('a meaningful reason and authorized target are required before any request', () => {
  const h = harness().select().open();
  h.context.bankEntityBulkRequest(false);
  h.fields({ bankEntityBulkNature: 'SAMAST', bankEntityBulkReason: 'short' });
  h.context.bankEntityBulkRequest(false);
  h.fields({ bankEntityBulkNature: 'SANKI', bankEntityBulkReason: 'An otherwise sufficient reason' });
  h.context.bankEntityBulkRequest(false);
  assert.equal(h.calls.length, 0);
  assert.match(h.nodes.bankEntityBulkMsg.textContent, /allowed final entity/);
});

test('changing account or source entity after opening blocks preview and clears stale fingerprint', () => {
  for (const mutation of [h => { h.nodes.bs_account.value = 'Other Bank 7883'; }, h => { h.context.bankNature = 'SAMAST'; }]) {
    const h = harness().select().open().valid();
    h.context.bankEntityBulkState.fingerprint = 'stale-fingerprint';
    mutation(h);
    h.context.bankEntityBulkRequest(true);
    assert.equal(h.calls.length, 0);
    assert.equal(h.context.bankEntityBulkState.fingerprint, '');
    assert.equal(h.nodes.bankEntityBulkSave.disabled, true);
  }
});

test('changing final entity or reason invalidates the reviewed fingerprint before saving', async () => {
  for (const [field, event] of [['bankEntityBulkNature', 'change'], ['bankEntityBulkReason', 'input']]) {
    const h = harness().select().open().valid();
    h.context.bankEntityBulkRequest(false);
    await flush();
    assert.equal(h.nodes.bankEntityBulkSave.disabled, false);
    h.event(field, event);
    assert.equal(h.context.bankEntityBulkState.fingerprint, '');
    assert.equal(h.nodes.bankEntityBulkSave.disabled, true);
    h.context.bankEntityBulkRequest(true);
    assert.equal(h.calls.length, 1, 'commit cannot reuse a previous review after editing');
  }
});

test('pending preview disables edit and cancellation controls and ignores double clicks', async () => {
  let release;
  const h = harness({ api(url, options) { h.calls.push({ url, body: JSON.parse(options.body) }); return new Promise(resolve => { release = resolve; }); } }).select().open().valid();
  h.event('bankEntityBulkPreview', 'click');
  h.event('bankEntityBulkPreview', 'click');
  assert.equal(h.calls.length, 1);
  for (const id of ['bankEntityBulkPreview', 'bankEntityBulkSave', 'bankEntityBulkCancel', 'bankEntityBulkNature', 'bankEntityBulkReason']) assert.equal(h.nodes[id].disabled, true, id);
  let prevented = false;
  h.event('bankEntityBulkDlg', 'cancel', { preventDefault() { prevented = true; } });
  h.event('bankEntityBulkCancel', 'click');
  assert.equal(prevented, true);
  assert.equal(h.nodes.bankEntityBulkDlg.open, true);
  release({ success: true, fingerprint: 'verified-fingerprint', updated: 1, changes: [] });
  await flush();
  assert.equal(h.context.bankEntityBulkState.busy, false);
  for (const id of ['bankEntityBulkPreview', 'bankEntityBulkSave', 'bankEntityBulkCancel', 'bankEntityBulkNature', 'bankEntityBulkReason']) assert.equal(h.nodes[id].disabled, false, id);
});

test('commit requires the preview fingerprint, is request-locked, and refreshes without mutating reconciliation figures', async () => {
  let release;
  const h = harness({ api(url, options) { const body = JSON.parse(options.body); h.calls.push({ url, body }); if (body.dryRun) return Promise.resolve({ success: true, fingerprint: 'verified-fingerprint', updated: 1, changes: [] }); return new Promise(resolve => { release = resolve; }); } }).select().open().valid();
  const before = JSON.stringify(h.context.bankReconView);
  h.context.bankEntityBulkRequest(true);
  assert.equal(h.calls.length, 0);
  h.context.bankEntityBulkRequest(false);
  await flush();
  h.event('bankEntityBulkSave', 'click');
  h.event('bankEntityBulkSave', 'click');
  assert.equal(h.calls.length, 2);
  assert.equal(h.calls[1].body.dryRun, false);
  assert.equal(h.calls[1].body.fingerprint, 'verified-fingerprint');
  release({ success: true, updated: 1 });
  await flush();
  assert.equal(h.context.bankEntityBulkState, null);
  assert.equal(h.nodes.bankEntityBulkDlg.open, false);
  assert.deepEqual(h.refreshes, [['statement', 'DRAFT-1'], ['ledger']]);
  assert.equal(JSON.stringify(h.context.bankReconView), before, 'bank dates, figures, ledger IDs and reconciliation links are never edited client-side');
  assert.match(h.nodes.bs_msg.textContent, /Bank figures and reconciliations are unchanged/);
});

test('API rejection restores controls, removes stale review, and never refreshes or silently retries', async () => {
  const h = harness({ api(url, options) { h.calls.push({ url, body: JSON.parse(options.body) }); return Promise.resolve({ success: false, error: 'The reviewed batch is stale.' }); } }).select().open().valid();
  h.context.bankEntityBulkState.fingerprint = 'old-fingerprint';
  h.context.bankEntityBulkRequest(true);
  await flush();
  assert.equal(h.calls.length, 1);
  assert.equal(h.context.bankEntityBulkState.fingerprint, '');
  assert.equal(h.context.bankEntityBulkState.busy, false);
  assert.equal(h.nodes.bankEntityBulkSave.disabled, true);
  for (const id of ['bankEntityBulkPreview', 'bankEntityBulkCancel', 'bankEntityBulkNature', 'bankEntityBulkReason']) assert.equal(h.nodes[id].disabled, false, id);
  assert.equal(h.nodes.bankEntityBulkMsg.textContent, 'The reviewed batch is stale.');
  assert.equal(h.refreshes.length, 0);
  h.context.bankEntityBulkRequest(true);
  assert.equal(h.calls.length, 1, 'another explicit preview is required');
});

test('network failure clears review and restores controls; response from a discarded dialog is ignored', async () => {
  const h = harness({ api() { return Promise.reject(new Error('Synthetic network interruption')); } }).select().open().valid();
  h.context.bankEntityBulkState.fingerprint = 'old-fingerprint';
  h.context.bankEntityBulkRequest(true);
  await flush();
  assert.equal(h.context.bankEntityBulkState.fingerprint, '');
  assert.equal(h.context.bankEntityBulkState.busy, false);
  assert.equal(h.nodes.bankEntityBulkSave.disabled, true);
  assert.equal(h.nodes.bankEntityBulkPreview.disabled, false);
  assert.equal(h.nodes.bankEntityBulkNature.disabled, false);
  assert.match(h.nodes.bankEntityBulkMsg.textContent, /Refresh and review before retrying/);
  let release;
  const stale = harness({ api() { return new Promise(resolve => { release = resolve; }); } }).select().open().valid();
  stale.context.bankEntityBulkRequest(false);
  stale.context.bankEntityBulkState = null;
  release({ success: true, fingerprint: 'stale', changes: [] });
  await flush();
  assert.equal(stale.context.bankEntityBulkState, null);
  assert.equal(stale.refreshes.length, 0);
});

test('history wrapper captures the original declaration even when that declaration appears later in the script', () => {
  const h = harness();
  const declaration = page.split('\n').find(line => line.includes('function renderBankHistory(imports,account,nature)'));
  assert.ok(declaration);
  const context = { ...h.context, cfg: { isAdmin: true }, bankHistoryLogs: null };
  context.window = context;
  vm.createContext(context);
  vm.runInContext(section() + '\n' + declaration, context);
  assert.notEqual(context.renderBankHistoryBeforeEntityBulk, context.renderBankHistory);
  assert.match(context.renderBankHistory([], 'Other Bank 8181', 'SANKI'), /No finalized reconciliation history yet/);
  assert.equal(context.bankHistoryLogs.length, 0);
});

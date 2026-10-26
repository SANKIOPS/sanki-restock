'use strict';
const test = require('node:test'), assert = require('node:assert/strict');
const fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const ReconciliationStatus = require('../public/reconciliation-status');
const page = fs.readFileSync(path.join(__dirname, '../public/expenses.html'), 'utf8');
const navigation = fs.readFileSync(path.join(__dirname, '../public/navigation-state.js'), 'utf8');
const bankAccount = 'ICICI Bank 0993', cashAccount = 'Gagan Personal Cash', draftId = 'BRD-OWNER-TEST';
const flush = () => new Promise(resolve => setImmediate(resolve));
function line(marker) { const source = page.split('\n').find(value => value.includes(marker)); assert.ok(source, marker); return source; }
function section(start, end) { const a = page.indexOf(start), b = page.indexOf(end, a + start.length); assert.ok(a >= 0 && b > a, start); return page.slice(a, b); }
function node(value = '') {
  let html = '';
  return { value, style: {}, dataset: {}, disabled: false, options: [], files: [],
    classList: { toggle() {} }, remove() {}, querySelectorAll() { return []; }, querySelector() { return null; },
    get innerHTML() { return html; }, set innerHTML(value) { html = String(value); },
    get textContent() { return html; }, set textContent(value) { html = String(value); }
  };
}
function pendingDraft() {
  return { draftId, account: bankAccount, nature: 'PERSONAL', rows: [],
    statementSummary: { from: '2026-08-22', to: '2026-10-19', openingBalance: 200.37, totalDebits: 950.04, totalCredits: 2000, closingBalance: 1250.33, validated: true },
    summary: { resolved: 42, matched: 135 }, unresolved: 0,
    ledgerClosing: 1250.33, bankClosing: 1250.33, balanceDifference: 0, balanceResolved: true, canFinalizeTransactions: true,
    finalizationApproval: { status: 'pending', requestedBy: 'prashant', requestedAt: '2026-10-26T08:00:00.000Z' }
  };
}
function bankResponse(account = bankAccount, withDraft = true) {
  return { success: true, account, nature: 'PERSONAL', transactions: [], imports: [],
    closingBalance: account === cashAccount ? 8000 : 1250.33,
    draftOptions: withDraft ? [{ draftId, from: '2026-08-22', to: '2026-10-19', resolved: 42, rows: 177 }] : [],
    draft: withDraft ? pendingDraft() : null };
}
function ledgerResponse(account = bankAccount) { return { success: true, account, entries: [], balance: account === cashAccount ? 8000 : 1250.33 }; }
function harness({ account = bankAccount, owner = false, reconcile = false, url } = {}) {
  const nodes = {}, calls = [];
  for (const id of ['lg_account', 'lg_expense_nature', 'lg_from', 'lg_to', 'ledgerRecon', 'ledgerTable', 'bs_nature', 'bs_account', 'bs_table', 'bs_history', 'bs_finalize', 'bs_discard', 'bs_file', 'bs_password', 'bs_msg']) nodes[id] = node();
  nodes.lg_account.value = nodes.bs_account.value = account;
  nodes.lg_account.options = [cashAccount, bankAccount].map(value => ({ value }));
  nodes.lg_from.value = '2026-10-01'; nodes.lg_to.value = '2026-10-26';
  const context = {
    URL, URLSearchParams, Date, console,
    location: new URL(url || 'https://example.test/expenses.html?tab=cash&nature=PERSONAL&ledgerFrom=2026-10-01&ledgerTo=2026-10-26'),
    selectedNature: 'PERSONAL', bankNature: 'PERSONAL', ledgerLoadedAccount: '',
    bankStatementLoadGeneration: 0, bankStatementPendingLoad: null, bankDraftId: '', bankReconView: null, bankDraftOptions: [],
    cfg: { isOwner: owner, isAdmin: true, canManagePersonalAccounting: true },
    el: id => nodes[id] || null, esc: String, fmt: String, fmtBank: String,
    bankAccountsForNature: () => [bankAccount],
    reconciliationAccountsForNature: () => reconcile ? [cashAccount, bankAccount] : [],
    reconciliationNatureForAccount: (_account, nature) => nature,
    ReconciliationStatus, reconciliationStatusBusy: () => false, renderBankHistory: () => '',
    api: (url, options) => {
      assert.ok(url.startsWith('/api/expenses/account-ledger?') || url.startsWith('/api/expenses/bank-statements?'), 'loading only reads ledger or saved reconciliation');
      assert.ok(!options || !options.method || options.method === 'GET', 'loading never posts or finalizes');
      return new Promise((resolve, reject) => calls.push({ url, options, resolve, reject }));
    }
  };
  context.window = context;
  context.history = { state: {}, replaceState(state, _title, target) { this.state = state; context.location = new URL(target, context.location); } };
  vm.createContext(context); vm.runInContext(navigation, context);
  vm.runInContext([
    section('    function loadAccountLedger(){', "    var bankDraftId='"),
    line('function bankDraftSwitcher(){'),
    section('    function loadBankStatement(', '    function uploadBankStatement('),
    section('    function renderBankReconciliation(d){', '    function bankOpeningEvidenceAmount(')
  ].join('\n'), context);
  return { context, nodes, calls, requests(kind) { return calls.filter(call => call.url.startsWith('/api/expenses/' + kind + '?')); } };
}
test('late cash responses cannot replace 0993 ledger or its pending Owner approval', async () => {
  const ui = harness({ account: cashAccount, reconcile: true });
  ui.context.loadAccountLedger(); ui.nodes.lg_account.value = bankAccount; ui.context.loadAccountLedger();
  const ledgers = ui.requests('account-ledger'), statements = ui.requests('bank-statements');
  statements[1].resolve(bankResponse()); ledgers[1].resolve(ledgerResponse()); await flush();
  assert.match(ui.nodes.ledgerTable.innerHTML, /Usable balance · ICICI Bank 0993/);
  assert.match(ui.nodes.bs_table.innerHTML, /Awaiting Owner approval/);
  const currentLedger = ui.nodes.ledgerTable.innerHTML, currentStatement = ui.nodes.bs_table.innerHTML;
  statements[0].resolve(bankResponse(cashAccount, false)); ledgers[0].resolve(ledgerResponse(cashAccount)); await flush();
  assert.equal(ui.nodes.lg_account.value, bankAccount); assert.equal(ui.nodes.bs_account.value, bankAccount);
  assert.equal(ui.nodes.ledgerTable.innerHTML, currentLedger); assert.equal(ui.nodes.bs_table.innerHTML, currentStatement);
  assert.equal(ui.context.bankDraftId, draftId);
});
const changes = {
  account: ui => { ui.nodes.lg_account.value = cashAccount; },
  entity: ui => { ui.context.selectedNature = 'SAMAST'; },
  'expense entity filter': ui => { ui.nodes.lg_expense_nature.value = 'SAMAST'; },
  'from date': ui => { ui.nodes.lg_from.value = '2026-09-01'; },
  'to date': ui => { ui.nodes.lg_to.value = '2026-10-19'; },
  'replaced page host': ui => { ui.nodes.ledgerTable = node(); ui.nodes.ledgerTable.textContent = 'New page'; }
};
for (const [name, change] of Object.entries(changes)) test('a response for the previous ' + name + ' cannot render or alter dates', async () => {
  const ui = harness(); ui.context.loadAccountLedger(); change(ui);
  const content = ui.nodes.ledgerTable.innerHTML, from = ui.nodes.lg_from.value, count = ui.calls.length;
  ui.calls[0].resolve({ ...ledgerResponse(), lastReconciliation: { from: '2026-08-22' } }); await flush();
  assert.equal(ui.nodes.ledgerTable.innerHTML, content); assert.equal(ui.nodes.lg_from.value, from);
  assert.equal(ui.calls.length, count, 'stale response cannot initiate a load with changed dates');
});
test('latest ledger request wins even when account and dates are unchanged', async () => {
  const ui = harness(); ui.context.loadAccountLedger(); ui.context.loadAccountLedger();
  ui.calls[1].resolve({ ...ledgerResponse(), balance: 200 }); await flush();
  const content = ui.nodes.ledgerTable.innerHTML; ui.calls[0].resolve({ ...ledgerResponse(), balance: 100 }); await flush();
  assert.equal(ui.nodes.ledgerTable.innerHTML, content);
});
for (const failure of ['response error', 'network failure']) test('current ' + failure + ' is shown and an obsolete one is ignored', async () => {
  const fail = (call, message) => failure === 'response error' ? call.resolve({ success: false, error: message }) : call.reject(new Error(message));
  const current = harness(); current.context.loadAccountLedger(); fail(current.calls[0], 'Current ledger unavailable'); await flush();
  assert.match(current.nodes.ledgerTable.textContent, /Current ledger unavailable/);
  const stale = harness({ account: cashAccount }); stale.context.loadAccountLedger(); stale.nodes.lg_account.value = bankAccount; stale.context.loadAccountLedger();
  stale.calls[1].resolve(ledgerResponse()); await flush(); const content = stale.nodes.ledgerTable.innerHTML;
  fail(stale.calls[0], 'Old cash ledger unavailable'); await flush(); assert.equal(stale.nodes.ledgerTable.innerHTML, content);
});
test('visible account changes invalidate an arriving statement response even if hidden input has not caught up', async () => {
  const ui = harness(); const loading = ui.context.loadBankStatement();
  ui.nodes.lg_account.value = cashAccount; const content = ui.nodes.bs_table.innerHTML;
  ui.calls[0].resolve(bankResponse()); await loading;
  assert.equal(ui.nodes.bs_table.innerHTML, content); assert.equal(ui.context.bankDraftId, ''); assert.equal(ui.context.bankReconView, null);
});
test('statement loading syncs a stale hidden account to the visible selection', async () => {
  const ui = harness({ reconcile: true }); ui.nodes.bs_account.value = cashAccount;
  ui.context.loadBankStatement(); assert.equal(ui.nodes.bs_account.value, bankAccount);
  const statement = ui.requests('bank-statements')[0], ledger = ui.requests('account-ledger')[0];
  assert.equal(new URL(statement.url, 'https://example.test').searchParams.get('account'), bankAccount);
  statement.resolve(bankResponse()); ledger.resolve(ledgerResponse()); await flush();
  assert.equal(ui.context.bankDraftId, draftId); assert.match(ui.nodes.bs_table.innerHTML, /Awaiting Owner approval/);
});
test('explicit statement loading clears old cash status and actions before the response arrives', async () => {
  const ui = harness(); ui.nodes.bs_table.innerHTML = 'No finalized reconciliation has been saved for Gagan Personal Cash.';
  ui.nodes.bs_history.innerHTML = 'Old cash history'; ui.nodes.bs_finalize.style.display = ui.nodes.bs_discard.style.display = 'inline-flex';
  ui.context.bankDraftId = 'BRD-OLD-CASH'; ui.context.bankReconView = { account: cashAccount };
  const loading = ui.context.loadBankStatement();
  assert.doesNotMatch(ui.nodes.bs_table.innerHTML, /Gagan Personal Cash/); assert.doesNotMatch(ui.nodes.bs_history.innerHTML, /Old cash history/);
  assert.equal(ui.nodes.bs_finalize.style.display, 'none'); assert.equal(ui.nodes.bs_discard.style.display, 'none');
  assert.equal(ui.context.bankDraftId, ''); assert.equal(ui.context.bankReconView, null);
  ui.calls[0].resolve({ success: false, error: 'Statement service unavailable' }); await loading;
  assert.match(ui.nodes.bs_table.textContent, /Statement service unavailable/); assert.doesNotMatch(ui.nodes.bs_table.innerHTML, /Gagan Personal Cash/);
});
for (const owner of [false, true]) test((owner ? 'Owner' : 'Admin') + ' deep link loads the shared pending draft and correct role action without posting', async () => {
  const url = 'https://example.test/expenses.html?tab=cash&nature=PERSONAL&account=ICICI+Bank+0993&ledgerFrom=2026-09-14&ledgerTo=2026-10-26&draft=' + draftId;
  const ui = harness({ owner, account: cashAccount, reconcile: true, url });
  vm.runInContext([line('var pageParams=new URLSearchParams(location.search),requestedAccount='), line("el('lg_from').value=SankiNavigation.date('ledgerFrom'")].join('\n'), ui.context);
  assert.equal(ui.nodes.lg_account.value, bankAccount, 'URL account restoration precedes the first load');
  ui.context.loadAccountLedger();
  const statement = ui.requests('bank-statements')[0], ledger = ui.requests('account-ledger')[0], request = new URL(statement.url, 'https://example.test');
  assert.equal(request.searchParams.get('account'), bankAccount); assert.equal(request.searchParams.get('nature'), 'PERSONAL'); assert.equal(request.searchParams.get('draftId'), draftId);
  const saved = bankResponse(), before = JSON.stringify(saved);
  statement.resolve(saved); ledger.resolve(ledgerResponse()); await flush();
  assert.equal(JSON.stringify(saved), before, 'read-only rendering preserves statement and approval data');
  assert.equal(ui.context.bankDraftId, draftId); assert.equal(ui.context.bankReconView.finalizationApproval.status, 'pending'); assert.equal(ui.context.bankReconView.balanceDifference, 0);
  assert.match(ui.nodes.bs_table.innerHTML, /Awaiting Owner approval/);
  assert.match(ui.nodes.bs_table.innerHTML, /2026-08-22 to 2026-10-19 · 42 reviewed · 177 bank entries/);
  assert.match(ui.nodes.ledgerTable.innerHTML, /Usable balance · ICICI Bank 0993/);
  assert.equal(ui.nodes.bs_finalize.disabled, !owner); assert.equal(ui.nodes.bs_finalize.textContent, owner ? 'Approve & finalize reconciliation' : 'Awaiting Owner approval');
  assert.equal(ui.nodes.lg_from.value, '2026-09-14'); assert.equal(ui.nodes.lg_to.value, '2026-10-26'); assert.equal(ui.calls.length, 2);
  assert.ok(ui.calls.every(call => !call.options?.method || call.options.method === 'GET'));
});
test('quiet refresh does not supersede a slow pending statement request', async () => {
  const ui = harness(); const initial = ui.context.loadBankStatement();
  const refresh = ui.context.loadBankStatement('', true);
  const again = ui.context.loadBankStatement('', true);
  assert.equal(ui.requests('bank-statements').length, 1, 'timer refresh must not continuously replace the slow saved-draft request');
  ui.calls[0].resolve(bankResponse()); await Promise.all([initial, refresh, again]);
  assert.equal(ui.context.bankDraftId, draftId); assert.match(ui.nodes.bs_table.innerHTML, /Awaiting Owner approval/);
});
test('autofilled PDF password alone does not block shared reads, but selected files and active edits still do', () => {
  const ui = harness();
  Object.assign(ui.context, { ccReviewSubmitting: false, ccCandidateRefreshBusy: () => false, document: { activeElement: null, querySelector: () => null } });
  vm.runInContext(section('    function reconciliationStatusBusy(){', '    function refreshSharedReconciliationStatus('), ui.context);
  ui.nodes.bs_password.value = 'autofilled-unused-password';
  assert.equal(ui.context.reconciliationStatusBusy(), false); assert.equal(ui.nodes.bs_password.value, 'autofilled-unused-password', 'read refresh must not erase the password');
  ui.nodes.bs_file.files = [{ name: 'statement.pdf' }]; assert.equal(ui.context.reconciliationStatusBusy(), true);
  ui.nodes.bs_file.files = [];
  const panel = node(), active = { matches: () => true }; panel.contains = candidate => candidate === active;
  ui.nodes.panel = panel; ui.context.document.activeElement = active;
  assert.equal(ui.context.reconciliationStatusBusy(), true);
});

test('same-account reload preserves upload warnings and action confirmations', async () => {
  const ui = harness();
  let loading = ui.context.loadBankStatement();
  ui.calls[0].resolve(bankResponse()); await loading;
  for (const message of [
    'Statement combined. Review note: A bank row requires attention.',
    'Reconciliation decision saved with its remark.'
  ]) {
    ui.nodes.bs_msg.textContent = message;
    loading = ui.context.loadBankStatement(draftId);
    assert.equal(ui.nodes.bs_msg.textContent, message, 'starting a read must not erase the current account outcome');
    ui.calls.at(-1).resolve(bankResponse()); await loading;
    assert.equal(ui.nodes.bs_msg.textContent, message);
  }
});
test('changing accounts clears the previous account upload or action message', async () => {
  const ui = harness({ account: cashAccount, reconcile: true });
  const initial = ui.context.loadBankStatement();
  ui.calls[0].resolve(bankResponse(cashAccount, false)); await initial;
  ui.nodes.bs_msg.textContent = 'A cash-ledger decision was saved.';
  ui.nodes.lg_account.value = bankAccount;
  ui.context.loadAccountLedger();
  assert.equal(ui.nodes.bs_msg.textContent, '', 'old account message must not appear under 0993');
  ui.requests('bank-statements')[1].resolve(bankResponse());
  ui.requests('account-ledger')[0].resolve(ledgerResponse()); await flush();
  assert.equal(ui.nodes.bs_msg.textContent, '');
  assert.equal(ui.context.bankDraftId, draftId);
});
test('quiet refresh joins manually selected period B while the URL still contains period A', async () => {
  const periodA = draftId, periodB = 'BRD-MANUAL-PERIOD-B';
  const ui = harness({ url: 'https://example.test/expenses.html?tab=cash&nature=PERSONAL&draft=' + periodA });
  const initial = ui.context.loadBankStatement(periodA);
  ui.calls[0].resolve(bankResponse()); await initial;
  const manuallySelected = ui.context.loadBankStatement(periodB);
  assert.equal(ui.context.location.searchParams.get('draft'), periodA, 'new period is saved to URL only after its response renders');
  const quietTimer = ui.context.loadBankStatement('', true);
  assert.equal(quietTimer, manuallySelected, 'timer must join the explicit selection rather than restart stale URL period A');
  assert.equal(ui.requests('bank-statements').length, 2);
  assert.equal(new URL(ui.calls[1].url, 'https://example.test').searchParams.get('draftId'), periodB);
  const saved = bankResponse();
  saved.draft.draftId = periodB; saved.draftOptions[0].draftId = periodB;
  ui.calls[1].resolve(saved); await Promise.all([manuallySelected, quietTimer]);
  assert.equal(ui.context.bankDraftId, periodB);
  assert.equal(ui.context.location.searchParams.get('draft'), periodB);
  assert.match(ui.nodes.bs_table.innerHTML, /BRD-MANUAL-PERIOD-B/);
});

test('selected nonempty expense entity is encoded from its own request snapshot', async () => {
  const ui = harness();
  ui.nodes.lg_expense_nature.value = 'SAMAST';
  ui.context.loadAccountLedger();
  const samast = ui.requests('account-ledger')[0];
  assert.equal(new URL(samast.url, 'https://example.test').searchParams.get('expenseNature'), 'SAMAST');
  assert.ok(samast.url.includes('&expenseNature=' + encodeURIComponent('SAMAST') + '&'));
  ui.nodes.lg_expense_nature.value = 'PERSONAL';
  ui.context.loadAccountLedger();
  const personal = ui.requests('account-ledger')[1];
  assert.equal(new URL(personal.url, 'https://example.test').searchParams.get('expenseNature'), 'PERSONAL');
  assert.ok(personal.url.includes('&expenseNature=' + encodeURIComponent('PERSONAL') + '&'));
  assert.equal(new URL(samast.url, 'https://example.test').searchParams.get('expenseNature'), 'SAMAST', 'the earlier request keeps its original selected entity');
  personal.resolve({ ...ledgerResponse(), balance: 222 }); await flush();
  const content = ui.nodes.ledgerTable.innerHTML;
  samast.resolve({ ...ledgerResponse(), balance: 111 }); await flush();
  assert.equal(ui.nodes.ledgerTable.innerHTML, content, 'an obsolete entity response cannot replace the currently selected entity');
  assert.equal(ui.nodes.lg_expense_nature.value, 'PERSONAL');
});

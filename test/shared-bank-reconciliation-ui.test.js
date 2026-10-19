'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const page = fs.readFileSync(path.join(__dirname, '../public/expenses.html'), 'utf8');
const sharedAccount = 'IndusInd Bank 7883';
const otherAccount = 'ICICI Bank 0992';
const flush = () => new Promise(resolve => setImmediate(resolve));
const line = marker => {
  const source = page.split('\n').find(value => value.includes(marker));
  assert.ok(source, 'source exists: ' + marker);
  return source;
};
const section = (start, end) => {
  const a = page.indexOf(start), b = page.indexOf(end, a + start.length);
  assert.ok(a >= 0 && b > a, 'source section exists: ' + start);
  return page.slice(a, b);
};

function harness({ account = sharedAccount, nature = 'PERSONAL', mapped = true, sharedAllowed = true } = {}) {
  const nodes = {}, calls = [], uploads = [], navigation = [];
  for (const id of ['trail', 'cf_from', 'cf_to', 'lg_account', 'lg_expense_nature', 'lg_from', 'lg_to', 'ledgerRecon', 'ledgerTable', 'bs_nature', 'bs_account', 'bs_table', 'bs_history', 'bs_finalize', 'bs_discard', 'bs_file', 'bs_password', 'bs_msg']) {
    nodes[id] = { value: '', textContent: '', innerHTML: '', style: {}, classList: { toggle() {} }, scrollIntoView() {} };
  }
  nodes.lg_account.value = account;
  nodes.cf_from.value = nodes.lg_from.value = '2026-10-01';
  nodes.cf_to.value = nodes.lg_to.value = '2026-10-19';
  const context = {
    selectedNature: nature, bankNature: nature, ledgerLoadedAccount: '',
    bankStatementLoadGeneration: 0, bankDraftId: '', bankReconView: null, bankDraftOptions: [],
    cfg: {
      isAdmin: true, isOwner: false,
      bankAccountsByNature: { PERSONAL: [sharedAccount, otherAccount], SAMAST: [sharedAccount] },
      reconciliationAccountsByNature: { PERSONAL: [otherAccount], SAMAST: sharedAllowed ? [sharedAccount] : [] },
      reconciliationNatureByAccount: mapped ? { [sharedAccount]: 'SAMAST' } : {}
    },
    el: id => nodes[id] || null, esc: String, fmt: String, fmtBank: String,
    SankiNavigation: { set: value => navigation.push(value), date: () => '', text: () => '' },
    ReconciliationStatus: { bankSaved: () => 'Saved bank status' },
    bankDraftSwitcher: () => '', renderBankHistory: () => '',
    api: async (url, options) => {
      calls.push({ url, options });
      if (url.startsWith('/api/expenses/bank-statements?')) return { success: true, transactions: [], imports: [], draftOptions: [], closingBalance: 0 };
      if (url.startsWith('/api/expenses/account-ledger?')) return { success: true, account: nodes.lg_account.value, entries: [], balance: 123 };
      throw Error('Unexpected API request: ' + url);
    },
    FormData: class { constructor() { this.values = new Map(); } append(key, value) { this.values.set(key, value); } get(key) { return this.values.get(key); } },
    fetch: async (url, options) => {
      uploads.push({ url, options });
      return { json: async () => ({ success: true, draftId: 'BRD-SHARED-7883' }) };
    }
  };
  context.window = context;
  vm.createContext(context);
  vm.runInContext([
    line('function bankAccountsForNature('),
    line('function reconciliationAccountsForNature('),
    line('function reconciliationNatureForAccount('),
    line('window.loadMoneyTrail=function('),
    line('window.selectDetailedAccount=function('),
    section('    function loadAccountLedger(){', "    var bankDraftId='"),
    section('    function loadBankStatement(', '    function uploadBankStatement('),
    line('function uploadBankStatement(')
  ].join('\n'), context);
  return { context, nodes, calls, uploads, navigation };
}

for (const nature of ['PERSONAL', 'SAMAST']) test(`${nature} 7883 card and detailed ledger show reconciliation but use the one SAMAST statement`, async () => {
  const ui = harness({ nature });
  ui.context.loadMoneyTrail(sharedAccount, 'trail');
  await flush();
  assert.match(ui.nodes.trail.innerHTML, /Upload or reconcile statement/);
  assert.match(ui.nodes.trail.innerHTML, /Reconciled through:/);
  assert.match(ui.nodes.trail.innerHTML, /selectDetailedAccount\('IndusInd Bank 7883'\)/);
  ui.context.selectDetailedAccount(sharedAccount);
  await flush();
  assert.equal(ui.nodes.ledgerRecon.style.display, 'block');
  assert.match(ui.nodes.ledgerTable.innerHTML, /Reconciled through:/);
  assert.equal(ui.context.selectedNature, nature, 'opening reconciliation must not switch the reporting entity');
  assert.equal(ui.context.bankNature, 'SAMAST');
  assert.equal(ui.nodes.bs_nature.value, 'SAMAST');
  assert.equal(ui.nodes.bs_account.value, sharedAccount);
  const ledgerCalls = ui.calls.filter(call => call.url.startsWith('/api/expenses/account-ledger?'));
  assert.equal(ledgerCalls.length, 2, 'both ledger views are exercised');
  for (const call of ledgerCalls) assert.equal(new URL(call.url, 'https://example.test').searchParams.get('nature'), nature);
  const statements = ui.calls.filter(call => call.url.startsWith('/api/expenses/bank-statements?'));
  assert.equal(statements.length, 1);
  assert.equal(new URL(statements[0].url, 'https://example.test').searchParams.get('nature'), 'SAMAST');
  assert.equal(ui.navigation.find(value => Object.hasOwn(value, 'nature')).nature, nature);

  ui.nodes.bs_file.files = [{ name: '7883.pdf' }];
  ui.context.uploadBankStatement();
  await flush();
  assert.equal(ui.uploads.length, 1);
  assert.equal(ui.uploads[0].url, '/api/expenses/bank-statements/import');
  assert.equal(ui.uploads[0].options.method, 'POST');
  assert.equal(ui.uploads[0].options.body.get('nature'), 'SAMAST');
  assert.equal(ui.uploads[0].options.body.get('account'), sharedAccount);
  assert.match(ui.calls.at(-1).url, /draftId=BRD-SHARED-7883/);
  assert.equal(new URL(ui.calls.at(-1).url, 'https://example.test').searchParams.get('nature'), 'SAMAST');
});

test('other personal banks retain their PERSONAL statement route when switching away from 7883', async () => {
  const ui = harness();
  ui.context.selectDetailedAccount(sharedAccount);
  await flush();
  assert.equal(ui.context.bankNature, 'SAMAST');
  ui.context.selectDetailedAccount(otherAccount);
  await flush();
  ui.context.loadMoneyTrail(otherAccount, 'trail');
  await flush();
  assert.equal(ui.context.bankNature, 'PERSONAL');
  assert.equal(ui.nodes.bs_nature.value, 'PERSONAL');
  assert.equal(ui.nodes.ledgerRecon.style.display, 'block');
  assert.match(ui.nodes.trail.innerHTML, /Upload or reconcile statement/);
  const otherCalls = ui.calls.filter(call => new URL(call.url, 'https://example.test').searchParams.get('account') === otherAccount);
  assert.equal(otherCalls.filter(call => call.url.startsWith('/api/expenses/bank-statements?')).length, 1);
  for (const call of otherCalls) assert.equal(new URL(call.url, 'https://example.test').searchParams.get('nature'), 'PERSONAL');
});

for (const access of [{ mapped: false, sharedAllowed: true }, { mapped: true, sharedAllowed: false }]) test(`shared routing does not grant reconciliation access (${JSON.stringify(access)})`, async () => {
  const ui = harness(access);
  ui.context.loadMoneyTrail(sharedAccount, 'trail');
  ui.context.selectDetailedAccount(sharedAccount);
  await flush();
  assert.doesNotMatch(ui.nodes.trail.innerHTML, /Upload or reconcile statement/);
  assert.equal(ui.nodes.ledgerRecon.style.display, 'none');
  assert.equal(ui.calls.some(call => call.url.startsWith('/api/expenses/bank-statements?')), false);
});

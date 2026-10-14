'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const testRoot = path.join(__dirname, '..', 'tmp', 'reconciliation-reminder-tests');
fs.mkdirSync(testRoot, { recursive: true });
const tempDir = fs.mkdtempSync(path.join(testRoot, 'sanki-reminder-policy-'));
process.env.DATA_PATH = path.join(tempDir, 'data.json');
const expenseFile = path.join(tempDir, 'expenses.json');
const cardFile = path.join(tempDir, 'credit-cards.json');
fs.writeFileSync(expenseFile, '{}');
const { router } = require('../modules/expenses');
function invoke(role = 'admin', username = role === 'admin' ? 'prashant' : 'owner-user') {
  const route = router.stack.find(item => item.route && item.route.path === '/api/expenses/reconciliation-reminders' && item.route.methods.get);
  let status = 200, body;
  route.route.stack[0].handle({ user: { username, role, roles: [role] } }, {
    status(value) { status = value; return this; },
    json(value) { body = value; return this; }
  });
  assert.equal(status, 200);
  return body;
}
const initial = invoke('owner');
const required = initial.requiredThrough;
const minus = days => {
  const date = new Date(required + 'T00:00:00Z'); date.setUTCDate(date.getUTCDate() - days);
  return date.toISOString().slice(0, 10);
};
const baseline = JSON.parse(fs.readFileSync(expenseFile, 'utf8'));
function fresh() {
  const store = JSON.parse(JSON.stringify(baseline));
  Object.assign(store, { expenses: {}, transfers: [], receipts: [], adjustments: [], vendorAdvances: [],
    bankTruthMovements: [], paytmSettlements: [], paytmPayoutPostings: [], prashantPersonalFundings: [],
    salesRefunds: [], receivables: {}, bankDateOverrides: {}, bankReconciliationLinks: {},
    bankReconciliationDrafts: {}, bankReconciliationApprovals: {}, cashReconciliations: [],
    bankStatements: { 'Axis Bank 3448': { reconciledThrough: minus(1), transactions: {}, imports: [] } },
    openingBalances: { 'Axis Bank 3448': 1000, 'Counter Cash': 0 }, openingBalancesByNature: {} });
  return store;
}
function addPending(store, { account = 'Axis Bank 3448', nature = 'SANKI', id = 'BRD-ATTENTION', date = required,
  balance = 1100, paytm = false, empty = false } = {}) {
  const reference = 'ATTENTION-REF-12345';
  if (!paytm && !empty) store.receipts.push({ id: 'RCP-' + id, nature, account, date, amount: 100, source: 'Verified receipt', bankReference: reference });
  const draft = { id, account, nature, transactions: empty ? [] : [{ date, description: paytm ? 'PAYTM PAYMENTS SERVICES' : 'Verified receipt',
    reference, debit: 0, credit: 100, balance }], summary: { from: date, to: date, openingBalance: empty ? 0 : 1000,
    closingBalance: empty ? 0 : balance, totalDebits: 0, totalCredits: empty ? 0 : 100, validated: true },
    resolutions: {}, matchingPolicy: 'balanced_date_amount_v5', createdBy: 'prashant', createdAt: date + 'T10:00:00Z' };
  store.bankReconciliationDrafts[id] = draft;
  store.bankReconciliationApprovals['BRA-' + id] = { id: 'BRA-' + id, draftId: id, account, nature, from: date,
    to: date, status: 'pending', requestedBy: 'prashant', requestedAt: date + 'T10:30:00Z' };
  return draft;
}
function save(store, cards = { cards: {}, statements: {} }) {
  fs.writeFileSync(expenseFile, JSON.stringify(store)); fs.writeFileSync(cardFile, JSON.stringify(cards));
}
const row = (result, account) => result.reminders.find(item => item.account === account);

test('route suppresses only admin approval-only attention and keeps official books and pendingCount truthful', () => {
  const store = fresh(); addPending(store); save(store);
  const before = fs.readFileSync(expenseFile, 'utf8'), admin = invoke(), owner = invoke('owner');
  assert.equal(row(admin, 'Axis Bank 3448').status, 'approval_pending');
  assert.equal(row(admin, 'Axis Bank 3448').attentionRequired, false);
  assert.equal(row(owner, 'Axis Bank 3448').attentionRequired, true);
  assert.equal(row(admin, 'Axis Bank 3448').reconciledThrough, minus(1));
  assert.match(row(owner, 'Axis Bank 3448').actionLabel, /Approve \/ finalize/);
  assert.equal(owner.pendingCount, admin.pendingCount + 1);
  for (const result of [admin, owner]) assert.equal(result.pendingCount, result.reminders.filter(item => item.attentionRequired).length);
  assert.equal(fs.readFileSync(expenseFile, 'utf8'), before, 'viewing reminders cannot mutate the accounting store');
});

test('route retains admin warnings for balance differences, missing rows, locked drafts and unsubmitted siblings', () => {
  const cases = [
    store => { store.bankReconciliationDrafts['BRD-ATTENTION'].summary.closingBalance = 1101; },
    store => { store.bankReconciliationDrafts['BRD-ATTENTION'].transactions.push({ date: required, debit: 10, credit: 0, balance: 1090, reference: 'UNMATCHED' }); },
    store => { store.bankReconciliationDrafts['BRD-ATTENTION'].originalStatementPeriod = { from: minus(2), to: required }; },
    store => { const sibling = JSON.parse(JSON.stringify(store.bankReconciliationDrafts['BRD-ATTENTION'])); sibling.id = 'BRD-SIBLING'; store.bankReconciliationDrafts[sibling.id] = sibling; }
  ];
  cases.forEach((change, index) => {
    const store = fresh(); addPending(store); change(store); save(store);
    const result = row(invoke(), 'Axis Bank 3448');
    assert.equal(result.attentionRequired, true, 'scenario ' + index);
    assert.equal(result.status, 'review_pending', 'scenario ' + index);
  });
});

test('route shows newer review due even though an older completed period is awaiting Owner approval', () => {
  const store = fresh(); store.bankStatements['Axis Bank 3448'].reconciledThrough = minus(4);
  addPending(store, { date: minus(2) }); save(store);
  const result = row(invoke(), 'Axis Bank 3448');
  assert.equal(result.attentionRequired, true);
  assert.equal(result.status, 'statement_required');
  assert.equal(result.awaitingOwnerApproval, true);
  assert.equal(result.reconciledThrough, minus(4));
  assert.match(result.actionLabel, /Newer statement required/);
});

test('read-only draft validation may stage a Paytm counterpart in memory but never saves it', () => {
  const store = fresh(); addPending(store, { paytm: true }); save(store);
  const before = fs.readFileSync(expenseFile, 'utf8');
  const originalWrite = fs.writeFileSync, originalRename = fs.renameSync;
  let writes = 0;
  fs.writeFileSync = function (...args) { if (String(args[0]).startsWith(tempDir)) writes++; return originalWrite.apply(this, args); };
  fs.renameSync = function (...args) { if (String(args[1]).startsWith(tempDir)) writes++; return originalRename.apply(this, args); };
  let result;
  try { result = invoke(); } finally { fs.writeFileSync = originalWrite; fs.renameSync = originalRename; }
  assert.equal(row(result, 'Axis Bank 3448').attentionRequired, false, 'exact staged counterpart is validated only on the snapshot');
  assert.equal(writes, 0);
  assert.equal(fs.readFileSync(expenseFile, 'utf8'), before);
  assert.equal(JSON.parse(before).paytmSettlements.length, 0);
});

test('hypothetical screenshot-date repair stays read-only and cannot bless the submitted original summary', () => {
  const XLSX = require('xlsx'), file = path.join(tempDir, 'retained-screenshot.xlsx');
  const book = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(book, XLSX.utils.json_to_sheet([
    { Date: required, Narration: 'Reviewed receipt', Reference: 'SCREENSHOT-IN', Debit: '', Credit: 100, Balance: 1100 },
    { Date: required, Narration: 'Reviewed payment', Reference: 'SCREENSHOT-OUT', Debit: 50, Credit: '', Balance: 1050 }
  ]), 'Statement');
  XLSX.writeFile(book, file);
  const store = fresh(), draft = addPending(store, { empty: true, date: '2098-01-01' });
  Object.assign(draft, { originalName: 'Statement screenshots', temporaryFile: file,
    transactions: [{ date: '2098-01-01', credit: 100, debit: 0, balance: 1100, reference: 'SCREENSHOT-IN' },
      { date: '2098-01-01', credit: 0, debit: 50, balance: 1050, reference: 'SCREENSHOT-OUT' }],
    summary: { ...draft.summary, openingBalance: 1000, closingBalance: 1050, totalDebits: 50, totalCredits: 100 },
    resolutions: { 'bank-0': { action: 'create_adjustment', reason: 'Reviewed receipt' },
      'bank-1': { action: 'create_adjustment', reason: 'Reviewed payment' } } });
  save(store);
  const before = fs.readFileSync(expenseFile, 'utf8'), result = row(invoke(), 'Axis Bank 3448');
  assert.equal(result.attentionRequired, true);
  assert.equal(result.status, 'review_pending');
  assert.equal(fs.readFileSync(expenseFile, 'utf8'), before, 'the repaired snapshot must not overwrite the submitted source');
  assert.equal(JSON.parse(before).bankReconciliationDrafts[draft.id].summary.validated, true);
});

test('bank-style card approvals obey the same attention policy and a legacy review stays actionable', () => {
  const account = 'HDFC 7376', cards = { cards: { CC: { id: 'CC', name: 'HDFC', last4: '7376', active: true } }, statements: {} };
  const store = fresh(); addPending(store, { account, empty: true }); save(store, cards);
  assert.equal(row(invoke(), account).attentionRequired, false);
  assert.equal(row(invoke('owner'), account).status, 'approval_pending');
  cards.statements.CCS = { id: 'CCS', cardId: 'CC', status: 'review', storedName: 'legacy.pdf', uploadedAt: required + 'T10:00:00Z', summary: { from: minus(1), to: required } };
  save(store, cards);
  assert.equal(row(invoke(), account).attentionRequired, true);
  assert.equal(row(invoke(), account).status, 'review_pending');
});

test('private PERSONAL card approval periods cannot affect or reveal admin reminder metadata', () => {
  const account = 'HDFC 7376', cards = { cards: { CC: { id: 'CC', name: 'HDFC', last4: '7376', active: true } }, statements: {} };
  const store = fresh();
  // Register the account in the private entity without creating a movement.
  store.adjustments.push({ id: 'PRIVATE-CARD-REGISTER', nature: 'PERSONAL', account, date: minus(2), amount: 0 });
  addPending(store, { account, nature: 'PERSONAL', id: 'BRD-PRIVATE-CARD', empty: true }); save(store, cards);
  const admin = row(invoke(), account), owner = row(invoke('owner'), account);
  assert.equal(admin.status, 'statement_required');
  assert.equal(admin.attentionRequired, true);
  assert.equal(admin.approvalPending, undefined);
  assert.equal(admin.submittedThrough, undefined);
  assert.equal(admin.detail, undefined);
  assert.equal(owner.status, 'approval_pending');
  assert.equal(owner.submittedThrough, required);
  assert.equal(owner.attentionRequired, true);
});

test('clean Counter Cash requests stop admin warnings; changed balances and newer counts remain actionable', () => {
  const store = fresh(); store.cashReconciliations = [{ id: 'CCR-ATTENTION', account: 'Counter Cash', status: 'pending',
    verifiedThrough: required, countedBalance: 0, ledgerBalance: 0, difference: 0, requestedBy: 'prashant', requestedAt: required + 'T10:00:00Z' }];
  save(store);
  assert.equal(row(invoke(), 'Counter Cash').attentionRequired, false);
  assert.equal(row(invoke('owner'), 'Counter Cash').attentionRequired, true);
  assert.match(row(invoke('owner'), 'Counter Cash').actionLabel, /Approve cash verification/);
  store.receipts.push({ id: 'CASH-CHANGED', nature: 'SANKI', account: 'Counter Cash', date: required, amount: 1 }); save(store);
  assert.equal(row(invoke(), 'Counter Cash').attentionRequired, true);
  assert.equal(row(invoke(), 'Counter Cash').status, 'review_pending');
  store.receipts = []; store.cashReconciliations[0].verifiedThrough = minus(1); save(store);
  assert.equal(row(invoke(), 'Counter Cash').attentionRequired, true);
  assert.equal(row(invoke(), 'Counter Cash').status, 'verification_required');
  store.cashReconciliations[0].verifiedThrough = required; store.cashReconciliations[0].countedBalance = ''; save(store);
  assert.equal(row(invoke(), 'Counter Cash').attentionRequired, true, 'a missing count is not a verified zero balance');
  assert.equal(row(invoke(), 'Counter Cash').status, 'review_pending');
});

test.after(() => fs.rmSync(tempDir, { recursive: true, force: true }));

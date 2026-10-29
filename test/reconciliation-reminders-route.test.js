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

test('archived duplicate card reviews do not remain pending while unknown states stay actionable', () => {
  const account='HDFC 7376',cards={cards:{CC:{id:'CC',name:'HDFC',last4:'7376',active:true}},statements:{
    ORIGINAL:{id:'ORIGINAL',cardId:'CC',status:'finalized',storedName:'original.pdf',summary:{from:minus(5),to:required}},
    DUPLICATE:{id:'DUPLICATE',cardId:'CC',status:'archived',storedName:'duplicate.pdf',summary:{from:minus(5),to:required}}
  }};
  save(fresh(),cards);
  for(const role of ['owner','admin']){const result=row(invoke(role),account);assert.equal(result.pendingDraft,false);assert.equal(result.draftId,'');assert.equal(result.status,'up_to_date');}
  cards.statements.DUPLICATE.status='unknown-review-state';save(fresh(),cards);
  for(const role of ['owner','admin']){const result=row(invoke(role),account);assert.equal(result.pendingDraft,true);assert.equal(result.draftId,'DUPLICATE');assert.equal(result.status,'review_pending');}
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

test('posted Paytm batches with covered old previews do not become pending; roles see identical counts and previews', () => {
  const store=fresh(), tx={transactionId:'PT-REMINDER',date:minus(1),amount:100,commission:1,gst:.18,platformFee:0,settledAmount:98.82,utr:'UTR-REMINDER',payoutId:'P-REMINDER',settledDate:required};
  Object.assign(store,{paytmReportTransactions:{[tx.transactionId]:tx},paytmPayoutPostings:[{id:'POST-REMINDER',payoutId:tx.utr,settlementId:tx.utr,utr:tx.utr,date:required,settledDate:required,transactionIds:[tx.transactionId],gross:100,customerGross:100,commission:1,gst:.18,platformFee:0,nonCustomerAmount:0,net:98.82,bankTransactionId:'BANK-REMINDER',postedBy:'synthetic-owner',postedAt:required+'T08:00:00Z'}],paytmManualResolutions:{[tx.transactionId]:{transactionId:tx.transactionId,type:'manual_sale',recordId:'RECEIPT-REMINDER',amount:100}},paytmVerifiedSettlements:[],paytmReportImports:[{at:required+'T08:00:00Z'}],paytmReportDrafts:{OLD:{id:'OLD',transactions:[tx],enrichments:[],warnings:[],sources:[{name:'already-saved.csv'}]}}});
  store.receipts.push({id:'RECEIPT-REMINDER',paytmTransactionId:tx.transactionId,nature:'SANKI',account:'Paytm Settlement Clearing',date:tx.date,amount:100,proof:'/api/expenses/photo/synthetic-paytm-receipt.jpg'});
  store.bankStatements['Axis Bank 3448'].transactions={'BANK-REMINDER':{id:'BANK-REMINDER',date:required,reference:tx.utr,credit:98.82,debit:0}};
  save(store);const before=fs.readFileSync(expenseFile,'utf8'),admin=row(invoke(),'Paytm Settlement Clearing'),owner=row(invoke('owner'),'Paytm Settlement Clearing');
  assert.equal(admin.status,'up_to_date');assert.equal(admin.attentionRequired,false);assert.equal(admin.pendingDraft,false);assert.equal(admin.settlementCounts.posted,1);assert.equal(admin.settlementCounts.incomplete,0);
  assert.deepEqual(admin,owner);assert.equal(admin.reportPreviews[0].code,'already_covered');assert.equal(fs.readFileSync(expenseFile,'utf8'),before);
  store.paytmReportDrafts.NEW={id:'NEW',transactions:[{...tx,transactionId:'NEW-PAYMENT'}],warnings:[]};save(store);
  const pending=row(invoke(),'Paytm Settlement Clearing');assert.equal(pending.attentionRequired,true);assert.equal(pending.actionLabel,'Review Paytm upload evidence');assert.equal(pending.settlementCounts.incomplete,0);assert.match(pending.detail,/NEW.*1 new payment/);
});

test('cash reminder identifies the actual changed request while a newer clean count stays awaiting Owner approval',()=>{
  const store=fresh();store.cashReconciliations=[
    {id:'CCR-OLD',account:'Counter Cash',status:'pending',verifiedThrough:minus(1),countedBalance:10,ledgerBalance:10,difference:0,requestedAt:minus(1)+'T08:00:00Z'},
    {id:'CCR-NEW',account:'Counter Cash',status:'pending',verifiedThrough:required,countedBalance:0,ledgerBalance:0,difference:0,requestedAt:required+'T08:00:00Z'}
  ];save(store);const before=fs.readFileSync(expenseFile,'utf8'),admin=row(invoke(),'Counter Cash'),owner=row(invoke('owner'),'Counter Cash');
  assert.equal(admin.status,'review_pending');assert.equal(admin.attentionRequired,true);assert.deepEqual(admin.cashStatus,owner.cashStatus);
  assert.deepEqual(admin.cashStatus.counts,{pending:2,reviewRequired:1,awaitingOwnerApproval:1});assert.match(admin.detail,/CCR-OLD.*Ledger changed/);assert.match(admin.detail,/CCR-NEW.*agrees.*awaiting Owner/);
  assert.equal(fs.readFileSync(expenseFile,'utf8'),before);
});

test.after(() => fs.rmSync(tempDir, { recursive: true, force: true }));

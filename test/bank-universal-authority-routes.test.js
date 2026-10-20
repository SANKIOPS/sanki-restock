'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

// Every route runs against this process-local store, never production files,
// an HTTP server, Shopify, or a financial API.
const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'sanki-bank-universal-routes-'));
process.env.DATA_PATH = path.join(tempDir, 'data.json');
fs.writeFileSync(path.join(tempDir, 'expenses.json'), '{}');
const { router, appBankMovements } = require('../modules/expenses');
const file = path.join(tempDir, 'expenses.json');
const account = 'IndusInd Bank 8181';
const draftId = 'BRD-UNIVERSAL-AUTHORITY-ROUTE';
const expenseId = 'EX-UNIVERSAL-AUTHORITY-ROUTE';
const appId = expenseId + '/PAY-001';
const ledgerDate = '2098-06-01';
const bankDate = '2098-06-02';
const reference = '992288776655';
const read = () => JSON.parse(fs.readFileSync(file, 'utf8'));
const write = store => fs.writeFileSync(file, JSON.stringify(store));
const clone = value => JSON.parse(JSON.stringify(value));

function invoke(method, route, { body = {}, query = {}, role = 'owner', username = '', permissions = {} } = {}) {
  return new Promise((resolve, reject) => {
    let status = 200, completed = false;
    const finish = value => {
      if (completed) return;
      completed = true;
      clearTimeout(timer);
      resolve({ status, body: value });
    };
    const timer = setTimeout(() => reject(new Error('Route did not complete: ' + route)), 10000);
    const headers = {};
    const req = {
      method, path: route, url: route, originalUrl: route, baseUrl: '', body, query,
      headers: { 'user-agent': 'Bank authority regression test', 'x-forwarded-for': '203.0.113.12' },
      get(name) { return this.headers[String(name).toLowerCase()] || ''; },
      ip: '203.0.113.12',
      user: { role, roles: [role], permissions, username: username || (role === 'admin' ? 'prashant' : role === 'owner' ? 'owner-user' : 'arshpreet') }
    };
    const res = {
      statusCode: 200, headersSent: false,
      status(value) { status = value; this.statusCode = value; return this; },
      setHeader(name, value) { headers[String(name).toLowerCase()] = value; },
      getHeader(name) { return headers[String(name).toLowerCase()]; },
      removeHeader(name) { delete headers[String(name).toLowerCase()]; },
      json(value) { this.headersSent = true; finish(value); return this; },
      send(value) { this.headersSent = true; finish(value); return this; },
      end(value) { this.headersSent = true; finish(value); return this; }
    };
    try { router.handle(req, res, error => { if (error) { clearTimeout(timer); reject(error); } else finish(undefined); }); }
    catch (error) { clearTimeout(timer); reject(error); }
  });
}

let baseline;
test.before(async () => {
  // Warm only local one-time normalization/migration markers. Each scenario
  // then discards all historical financial fixtures before seeding its own.
  fs.writeFileSync(file, '{}');
  const configured = await invoke('GET', '/api/expenses/config');
  assert.equal(configured.status, 200);
  baseline = read();
});
test.after(() => fs.rmSync(tempDir, { recursive: true, force: true }));

function seed({ amount = 500, bankAmount = 499.5, expenseAmount = amount, date = bankDate, bankCredit = false, payment = {}, expense = {}, draft = {} } = {}) {
  const store = clone(baseline);
  Object.assign(store, {
    expenses: {}, transfers: [], receipts: [], adjustments: [], vendorAdvances: [],
    vendorOpeningPayables: [], reconciliationExpenses: [], bankTruthMovements: [],
    paytmSettlements: [], paytmPayoutPostings: [], salesRefunds: [], expenseRefunds: [],
    receivables: {}, loanRepayments: [], bankStatements: {}, bankDateOverrides: {},
    bankReconciliationLinks: {}, bankReconciliationApprovals: {}, bankAmountAuthorityApplications: {}, bankAmountAuthorityProposals: {},
    openingBalances: { [account]: 2000 }, openingBalancesByNature: {}, physicalBankOpeningBalances: {},
    openingBalanceDates: { [account]: '2098-01-01' }, auditLog: [], auditSeq: 0,
    vendors: {}, vendorDirectory: {}, vendorsByNature: {}, bankReconciliationDrafts: {}
  });
  store.expenses[expenseId] = {
    id: expenseId, nature: 'SANKI', vendor: 'Authority Test Vendor',
    particulars: 'Bank amount authority fixture', ledger: 'FOOD EXPENSE', type: 'variable',
    status: amount >= expenseAmount ? 'paid' : 'partially_paid',
    amount: expenseAmount, requestedAmount: expenseAmount, paidAmount: amount,
    date: ledgerDate, approvedAt: ledgerDate + 'T10:00:00Z', approvedBy: 'owner-user',
    billPhoto: '/api/expenses/photo/authority-bill.jpg',
    payments: [{ id: 'PAY-001', amount, date: ledgerDate, account, bankReference: reference,
      paymentType: 'UPI', paidBy: 'prashant', proof: '/api/expenses/photo/authority-payment.jpg', ...payment }],
    ...expense
  };
  const closing = Math.round((2000 + (bankCredit ? bankAmount : -bankAmount)) * 100) / 100;
  store.bankReconciliationDrafts[draftId] = {
    id: draftId, account, nature: 'SANKI',
    transactions: [{ date, description: 'Authority Test Vendor UPI ' + reference, reference,
      debit: bankCredit ? 0 : bankAmount, credit: bankCredit ? bankAmount : 0, balance: closing }],
    summary: { from: ledgerDate, to: date, openingBalance: 2000, closingBalance: closing,
      totalDebits: bankCredit ? 0 : bankAmount, totalCredits: bankCredit ? bankAmount : 0, validated: true },
    resolutions: {}, matchingPolicy: 'balanced_date_amount_v5',
    createdBy: 'prashant', createdAt: '2098-06-02T12:00:00Z', expiresAt: '2099-01-01T00:00:00Z',
    ...draft
  };
  write(store);
  return store;
}

const reconcile = (role = 'owner') => invoke('POST', '/api/expenses/bank-statements/reconcile', { role, body: { draftId, account } });
const preview = (role = 'owner', extra = {}) => invoke('POST', '/api/expenses/bank-statements/amount-authority-preview', { role, body: { draftId, rowId: 'bank-0', bankRowId: 'bank-0', appId, ...extra } });
const resolveMatch = (fingerprint, extra = {}, role = 'owner') => invoke('POST', '/api/expenses/bank-statements/resolve', {
  role, body: { draftId, rowId: 'bank-0', action: 'link_existing', appId,
    reason: 'Verified this exact payment against the original bank statement',
    confirmBankAmount: true, bankAmountAuthorityFingerprint: fingerprint, ...extra }
});
const finalize = (role = 'owner', extra = {}) => invoke('POST', '/api/expenses/bank-statements/finalize', { role, body: { draftId, ...extra } });

async function loadFixtureBaseline() {
  // Persist through an existing non-financial save before reviewing amounts.
  // loadStore already canonicalizes proof arrays and legacy expense statuses;
  // those existing effects must not be mistaken for a bank-authority mutation.
  const result = await invoke('POST', '/api/expenses/bank-statements/remark', {
    body: { draftId, remark: 'Source evidence loaded before bank amount review' }
  });
  assert.equal(result.status, 200, JSON.stringify(result.body));
  assert.deepEqual(read().bankAmountAuthorityApplications, {});
  assert.equal(Object.keys(read().bankStatements).length, 0);
  return read();
}


const cardFile = path.join(tempDir, 'credit-cards.json'), salaryFile = path.join(tempDir, 'salary.json'), procurementFile = path.join(tempDir, 'procurement.json');
const readCards = () => JSON.parse(fs.readFileSync(cardFile, 'utf8'));
const writeCards = cards => fs.writeFileSync(cardFile, JSON.stringify(cards));
function emptySeed({ bankAmount = 999.63, credit = false } = {}) {
  const store = seed({ amount: 1000, bankAmount, bankCredit: credit });
  store.expenses = {}; store.creditCardSettlementAllocations = []; store.bankMovementAuthority = {};
  store.procurementAccounting = { mediator: 'LG', paymentsByPo: {} };
  write(store); writeCards({ cards: {}, payments: [], statements: {}, audit: [] });
  fs.writeFileSync(salaryFile, JSON.stringify({ salaryPayments: [], advances: {}, employees: {} }));
  fs.writeFileSync(procurementFile, JSON.stringify({ pos: {} }));
  return store;
}
function cardSeed({ amount = 26701, bankAmount = 26700.63, allocations = [{ nature: 'SANKI', amount }], finalStatement = false } = {}) {
  const store = emptySeed({ bankAmount });
  const transfer = { id: 'TR-00136', nature: 'SANKI', fromNature: 'SANKI', toNature: 'SANKI', classification: 'credit_card_payment', creditCardPaymentId: 'CCP-00011', fromAccount: account, toAccount: 'HDFC 7376', amount, date: ledgerDate, bankReference: reference, proof: '/api/expenses/photo/card-transfer-proof.jpg', note: 'Linked card payment' };
  store.transfers = [transfer];
  store.creditCardSettlementAllocations = allocations.map(item => ({ id: 'CCA-CCP-00011-' + item.nature, creditCardPaymentId: 'CCP-00011', cardId: 'CC-0002', cardName: 'HDFC 7376', date: ledgerDate, amount: item.amount, payerNature: 'SANKI', beneficiaryNature: item.nature, payingAccount: account, status: item.nature === 'SANKI' ? 'self' : 'due' }));
  store.openingBalances[account] = 100000;
  const draft = store.bankReconciliationDrafts[draftId]; draft.transactions[0].balance = Math.round((100000 - bankAmount) * 100) / 100;
  Object.assign(draft.summary, { openingBalance: 100000, closingBalance: draft.transactions[0].balance });
  store.expenses['EX-CARD-PURCHASE'] = { id: 'EX-CARD-PURCHASE', nature: 'SANKI', amount: 8500, paidAmount: 8500, date: ledgerDate, ledger: 'PURCHASE', particulars: 'Original card purchase must remain unchanged', status: 'paid', payments: [], proof: '/api/expenses/photo/original-purchase.jpg' };
  const cards = { cards: { 'CC-0002': { id: 'CC-0002', name: 'HDFC', last4: '7376', active: true, openingOutstanding: 100000, cardholder: 'PRIVATE-CARD-HOLDER' } }, payments: [{ id: 'CCP-00011', cardId: 'CC-0002', nature: 'SANKI', account, amount, date: ledgerDate, active: true, allocations: clone(allocations), reference, proof: '/api/expenses/photo/card-payment-proof.jpg' }], statements: { PURCHASE: { id: 'PURCHASE', cardId: 'CC-0002', status: 'finalized', rows: [{ id: 'PURCHASE-ROW', classification: 'expense', amount: 8500, debit: 8500, date: ledgerDate, narration: 'Original card purchase' }] } }, audit: [] };
  if (finalStatement) cards.statements.PAYMENT = { id: 'PAYMENT', cardId: 'CC-0002', status: 'finalized', finalizedAt: ledgerDate, rows: [{ id: 'PRINTED-PAYMENT', classification: 'card_payment', creditCardPaymentId: 'CCP-00011', amount, credit: amount, date: ledgerDate, reference, narration: 'PAYMENT THANK YOU' }] };
  write(store); writeCards(cards); return { store, cards };
}
const previewMovement = (id, { role = 'owner', ...extra } = {}) => invoke('POST', '/api/expenses/bank-statements/amount-authority-preview', { role, body: { draftId, rowId: 'bank-0', bankRowId: 'bank-0', appId: id, ...extra } });
const stageMovement = (id, fingerprint, { role = 'owner', ...extra } = {}) => invoke('POST', '/api/expenses/bank-statements/resolve', { role, body: { draftId, rowId: 'bank-0', bankRowId: 'bank-0', appId: id, action: 'link_existing', reason: 'Verified original bank cash movement against the linked source evidence', confirmBankAmount: true, bankAmountAuthorityFingerprint: fingerprint, ...extra } });
async function bankLedger() {
  const result = await invoke('GET', '/api/expenses/account-ledger', { query: { nature: 'SANKI', account, from: ledgerDate, to: bankDate } });
  assert.equal(result.status, 200, JSON.stringify(result.body)); return result.body;
}
function noFakeEntries(store) {
  assert.equal(store.adjustments.length, 0); assert.equal(store.receipts.length, 0);
  assert.equal(store.reconciliationExpenses.length, 0);
}

test('TR-00136 26701 -> 26700.63 previews read-only and commits the linked bank/card/ownership JSON once after Owner approval', async () => {
  const { cards: beforeCards } = cardSeed();
  const before = await loadFixtureBaseline();
  assert.equal(before.transfers[0].amount, 26701); assert.equal(before.transfers[0].date, ledgerDate);
  const candidate = await previewMovement('TR-00136', { role: 'admin' });
  assert.equal(candidate.status, 200, JSON.stringify(candidate.body));
  assert.equal(candidate.body.bankAmountAuthority.sourceType, 'credit_card_payment');
  assert.equal(candidate.body.bankAmountAuthority.effect, 'linked_sources');
  assert.doesNotMatch(JSON.stringify(candidate.body), /PRIVATE-CARD-HOLDER|card-transfer-proof|original-purchase/);
  assert.deepEqual(read().transfers, before.transfers); assert.deepEqual(readCards(), beforeCards);
  const staged = await stageMovement('TR-00136', candidate.body.bankAmountAuthority.fingerprint, { role: 'admin' });
  assert.equal(staged.status, 200, JSON.stringify(staged.body));
  assert.equal(staged.body.ledgerClosing, 73299.37); assert.equal(staged.body.balanceDifference, 0);
  assert.deepEqual(read().transfers, before.transfers); assert.deepEqual(readCards(), beforeCards);
  const submission = await finalize('admin');
  assert.equal(submission.status, 202, JSON.stringify(submission.body));
  assert.deepEqual(readCards(), beforeCards); assert.equal(read().transfers[0].amount, 26701);
  const approved = await finalize('owner', { approveRequestId: submission.body.approval.id });
  assert.equal(approved.status, 200, JSON.stringify(approved.body));
  const after = read(), cards = readCards();
  assert.equal(after.transfers.length, 1); assert.equal(after.transfers[0].amount, 26700.63); assert.equal(after.transfers[0].date, bankDate);
  assert.equal(cards.payments.length, 1); assert.equal(cards.payments[0].amount, 26700.63); assert.equal(cards.payments[0].allocations[0].amount, 26700.63);
  assert.equal(after.creditCardSettlementAllocations[0].amount, 26700.63); assert.equal(after.creditCardSettlementAllocations[0].date, bankDate);
  assert.deepEqual(after.expenses, before.expenses); assert.deepEqual(cards.statements, beforeCards.statements);
  assert.equal(cards.payments[0].proof, beforeCards.payments[0].proof); assert.equal(after.transfers[0].proof, before.transfers[0].proof);
  noFakeEntries(after);
  const ledger = await bankLedger(); const movements = ledger.entries.filter(item => item.id === 'TR-00136');
  assert.equal(movements.length, 1); assert.equal(movements[0].debit, 26700.63); assert.equal(ledger.balance, 73299.37);
  assert.ok(!fs.existsSync(path.join(tempDir, '.bank-authority-transaction.json')), 'atomic commit leaves no unresolved journal');
  const snapshot = JSON.stringify({ expenses: read(), cards: readCards() });
  const retry = await finalize('owner', { approveRequestId: submission.body.approval.id });
  assert.equal(retry.status, 404); assert.equal(JSON.stringify({ expenses: read(), cards: readCards() }), snapshot);
});

test('card manual difference above one rupee remains unrestricted while automatic exact-reference correction retains the inclusive threshold', async () => {
  cardSeed({ bankAmount: 28001.19 });
  const candidate = await previewMovement('TR-00136');
  assert.equal(candidate.status, 200, JSON.stringify(candidate.body)); assert.equal(candidate.body.bankAmountAuthority.bankAmount, 28001.19);
  const staged = await stageMovement('TR-00136', candidate.body.bankAmountAuthority.fingerprint);
  assert.equal(staged.status, 200, JSON.stringify(staged.body));
  const done = await finalize(); assert.equal(done.status, 200, JSON.stringify(done.body)); assert.equal(readCards().payments[0].amount, 28001.19);
  cardSeed({ bankAmount: 26700 });
  const automatic = await reconcile();
  assert.equal(automatic.status, 200, JSON.stringify(automatic.body));
  const matched = automatic.body.rows.find(item => item.id === 'bank-0');
  assert.equal(matched.status, 'matched'); assert.equal(matched.bankAmountAuthority.mode, 'auto');
  assert.equal(matched.bankAmountAuthority.bankAmount, 26700);
  assert.equal(readCards().payments[0].amount, 26701, 'automatic preview does not post');
});

test('card source and ownership changes after review reject staging/finalization without partial cross-file changes', async () => {
  for (const change of [
    cards => { cards.payments[0].proof = '/api/expenses/photo/new-proof.jpg'; },
    cards => { cards.payments[0].allocations[0].amount = 26000; }
  ]) {
    cardSeed(); const candidate = await previewMovement('TR-00136'); assert.equal(candidate.status, 200, JSON.stringify(candidate.body));
    const staged = await stageMovement('TR-00136', candidate.body.bankAmountAuthority.fingerprint); assert.equal(staged.status, 200, JSON.stringify(staged.body));
    const cards = readCards(); change(cards); writeCards(cards); const cardsBefore = fs.readFileSync(cardFile, 'utf8'), transferBefore = clone(read().transfers);
    const denied = await finalize(); assert.equal(denied.status, 409, JSON.stringify(denied.body));
    assert.equal(fs.readFileSync(cardFile, 'utf8'), cardsBefore); assert.deepEqual(read().transfers, transferBefore);
    assert.deepEqual(read().bankAmountAuthorityApplications, {}); assert.equal(Object.keys(read().bankStatements).length, 0);
  }
});

test('mixed ownership requires explicit exact allocations; PERSONAL choices remain private from an unauthorized Admin', async () => {
  cardSeed({ amount: 1000, bankAmount: 999.63, allocations: [{ nature: 'SANKI', amount: 600 }, { nature: 'PERSONAL', amount: 400 }] });
  const denied = await previewMovement('TR-00136', { role: 'admin' });
  assert.equal(denied.status, 403, JSON.stringify(denied.body));
  assert.equal(denied.body.allocationChoices, undefined); assert.doesNotMatch(JSON.stringify(denied.body), /600|400|PRIVATE-CARD-HOLDER/);
  const needed = await previewMovement('TR-00136'); assert.equal(needed.status, 409, JSON.stringify(needed.body));
  assert.equal(needed.body.allocationRequired, true); assert.deepEqual(needed.body.allocationChoices, [{ nature: 'SANKI', amount: 600 }, { nature: 'PERSONAL', amount: 400 }]);
  const bad = await previewMovement('TR-00136', { amountAuthorityAllocations: [{ nature: 'SANKI', amount: 599 }, { nature: 'PERSONAL', amount: 400 }] });
  assert.equal(bad.status, 409);
  assert.equal(readCards().payments[0].amount, 1000);
  const allocations = [{ nature: 'SANKI', amount: 599.63 }, { nature: 'PERSONAL', amount: 400 }];
  const candidate = await previewMovement('TR-00136', { amountAuthorityAllocations: allocations });
  assert.equal(candidate.status, 200, JSON.stringify(candidate.body));
  const staged = await stageMovement('TR-00136', candidate.body.bankAmountAuthority.fingerprint, { amountAuthorityAllocations: allocations });
  assert.equal(staged.status, 200, JSON.stringify(staged.body));
  const done = await finalize(); assert.equal(done.status, 200, JSON.stringify(done.body));
  assert.deepEqual(readCards().payments[0].allocations, allocations);
  assert.deepEqual(read().creditCardSettlementAllocations.map(item => ({ nature: item.beneficiaryNature, amount: item.amount })), allocations);
});

test('PERSONAL ownership request normalization cannot bypass Admin privacy checks in preview or resolve', async () => {
  for (const privateShare of [{ nature: 'personal' }, { nature: ' PERSONAL ' }, { nature: ' PeRsOnAl ' }, { beneficiaryNature: ' PeRsOnAl ' }]) {
    cardSeed({ amount: 1000, bankAmount: 999.63, allocations: [{ nature: 'SANKI', amount: 600 }, { nature: 'PERSONAL', amount: 400 }] });
    const before = JSON.stringify({ expenses: read(), cards: readCards() });
    const allocations = [{ nature: 'SANKI', amount: 599.63 }, { ...privateShare, amount: 400 }];
    const deniedPreview = await previewMovement('TR-00136', { role: 'admin', amountAuthorityAllocations: allocations });
    assert.equal(deniedPreview.status, 403, JSON.stringify(deniedPreview.body));
    assert.equal(deniedPreview.body.allocationChoices, undefined);
    assert.doesNotMatch(JSON.stringify(deniedPreview.body), /600|400|PRIVATE-CARD-HOLDER/);
    const deniedResolve = await stageMovement('TR-00136', 'forged-fingerprint', { role: 'admin', amountAuthorityAllocations: allocations });
    assert.equal(deniedResolve.status, 403, JSON.stringify(deniedResolve.body));
    assert.equal(deniedResolve.body.allocationChoices, undefined);
    assert.doesNotMatch(JSON.stringify(deniedResolve.body), /600|400|PRIVATE-CARD-HOLDER/);
    assert.equal(JSON.stringify({ expenses: read(), cards: readCards() }), before, 'a denied request must not stage or change either store');
  }
});

test('orphaned or missing credit-card payment links are blocked rather than silently falling back to an unrelated cash correction', async () => {
  for (const change of [
    (store, cards) => { store.transfers[0].creditCardPaymentId = ''; },
    (store, cards) => { cards.payments = []; }
  ]) {
    const { store, cards } = cardSeed(); change(store, cards); write(store); writeCards(cards);
    const before = JSON.stringify({ transfers: store.transfers, cards });
    const result = await previewMovement('TR-00136');
    assert.equal(result.status, 409, JSON.stringify(result.body));
    assert.match(result.body.error, /linked.*payment|unique|missing/i);
    assert.equal(JSON.stringify({ transfers: read().transfers, cards: readCards() }), before);
  }
});

test('finalized card statement remains printed evidence while its actual bank leg can be corrected without an extra entry', async () => {
  const { store: before, cards: beforeCards } = cardSeed({ finalStatement: true });
  const candidate = await previewMovement('TR-00136'); assert.equal(candidate.status, 200, JSON.stringify(candidate.body));
  assert.equal(candidate.body.bankAmountAuthority.effect, 'bank_cash_only');
  assert.match(candidate.body.bankAmountAuthority.sourceEffects, /finalized card statement/);
  const staged = await stageMovement('TR-00136', candidate.body.bankAmountAuthority.fingerprint); assert.equal(staged.status, 200, JSON.stringify(staged.body));
  const done = await finalize(); assert.equal(done.status, 200, JSON.stringify(done.body));
  const after = read(); assert.deepEqual(readCards(), beforeCards); assert.deepEqual(after.transfers, before.transfers);
  assert.deepEqual(after.creditCardSettlementAllocations, before.creditCardSettlementAllocations);
  const movements = appBankMovements(after, account, 'SANKI').filter(item => item.id === 'TR-00136');
  assert.equal(movements.length, 1); assert.equal(movements[0].debit, 26700.63);
  assert.equal(Object.keys(after.bankMovementAuthority).length, 1); noFakeEntries(after);
});

test('finalized opposite-account bank evidence protects the card payment and cannot be overridden by a new manual review', async () => {
  const { store, cards } = cardSeed();
  store.bankStatements['HDFC 7376'] = { transactions: {}, imports: [{ id: 'FINAL-CARD-BANK', reconciliationRows: [{ linkedRecordIds: ['CCP-00011'], ledger: { id: 'CCP-00011' } }] }] };
  write(store); const result = await previewMovement('TR-00136');
  assert.equal(result.status, 409, JSON.stringify(result.body));
  assert.deepEqual(readCards(), cards); assert.equal(read().transfers[0].amount, 26701); assert.deepEqual(read().bankAmountAuthorityApplications, {});
});

const familyCases = {
  salary: store => { const source = { id: 'SAL-ROUTE-1', employeeName: 'Salary Test Employee', amount: 1000, earned: 1000, date: ledgerDate, account, reference, proof: '/salary-proof.jpg' }; fs.writeFileSync(salaryFile, JSON.stringify({ salaryPayments: [source], advances: {}, employees: {} })); return { id: source.id, snapshot: () => fs.readFileSync(salaryFile, 'utf8') }; },
  refund: store => { const source = { id: 'SRF-ROUTE-1', nature: 'SANKI', saleReference: 'ORDER-ORIGINAL', amount: 1000, date: ledgerDate, refundAccount: account, proof: '/original-refund-proof.jpg' }; store.salesRefunds = [source]; return { id: source.id, snapshot: () => read().salesRefunds }; },
  loan: store => { store.receipts = [{ id: 'REC-LOAN-PARENT', nature: 'SANKI', account: 'Axis Bank 3448', amount: 3000, date: ledgerDate, receiptType: 'loan_received', source: 'Lender', loanTerms: { lenderKind: 'person' } }]; const source = { id: 'LOANPAY-ROUTE-1', loanId: 'REC-LOAN-PARENT', nature: 'SANKI', account, amount: 1000, date: ledgerDate, source: 'Lender', proof: '/loan-proof.jpg' }; store.loanRepayments = [source]; return { id: source.id, snapshot: () => ({ receipts: read().receipts, repayments: read().loanRepayments }) }; },
  procurement: store => { const po = { id: 'PO-ROUTE-1', origin: 'india', vendor: 'Stock Supplier', datePurchase: ledgerDate, lines: [{ sku: 'TEST-SKU', qty: 1, perPcsYuan: 1000 }], transportTotal: 0 }; fs.writeFileSync(procurementFile, JSON.stringify({ pos: { [po.id]: po }, settings: {} })); store.procurementAccounting.paymentsByPo[po.id] = { payments: [{ id: 'PAY-001', amount: 1000, date: ledgerDate, account, proof: '/po-proof.jpg' }] }; return { id: po.id + '/PAY-001', snapshot: () => ({ procurement: fs.readFileSync(procurementFile, 'utf8'), payments: read().procurementAccounting.paymentsByPo }) }; },
  reimbursement_batch: store => { store.expenses = {}; for (const [index, amount] of [[1, 600], [2, 400]]) { const id = 'EX-REIM-' + index; store.expenses[id] = { id, nature: 'SANKI', date: ledgerDate, claimant: 'pradeep', paidAlready: true, fundedBy: 'claimant', vendor: 'Original Vendor', amount, personalPaidAmount: amount, status: 'paid', approvedAt: ledgerDate, reimbursementPayments: [{ id: 'REIM-001', batchId: 'RB-ROUTE-1', amount, date: ledgerDate, account, proof: '/reimbursement-proof.jpg' }], payments: [] }; } return { id: 'RB-ROUTE-1', snapshot: () => read().expenses }; }
};
for (const [family, make] of Object.entries(familyCases)) test('manual ' + family + ' correction changes only the bank cash leg, retaining entitlement/source proof and one movement', async () => {
  const store = emptySeed(), item = make(store); write(store);
  await loadFixtureBaseline(); const before = clone(item.snapshot());
  const originalMovements = appBankMovements(read(), account, 'SANKI').filter(row => row.id === item.id);
  assert.equal(originalMovements.length, 1, 'fixture exposes exactly one source movement');
  assert.equal(originalMovements[0].debit, 1000); assert.equal(originalMovements[0].date, ledgerDate);
  const candidate = await previewMovement(item.id); assert.equal(candidate.status, 200, JSON.stringify(candidate.body));
  assert.equal(candidate.body.bankAmountAuthority.effect, 'bank_cash_only');
  assert.deepEqual(item.snapshot(), before);
  const staged = await stageMovement(item.id, candidate.body.bankAmountAuthority.fingerprint); assert.equal(staged.status, 200, JSON.stringify(staged.body));
  assert.equal(staged.body.balanceDifference, 0); assert.deepEqual(item.snapshot(), before);
  const done = await finalize(); assert.equal(done.status, 200, JSON.stringify(done.body));
  const after = read(); assert.deepEqual(item.snapshot(), before);
  const movements = appBankMovements(after, account, 'SANKI').filter(row => row.id === item.id);
  assert.equal(movements.length, 1); assert.equal(movements[0].debit, 999.63); assert.equal(movements[0].date, bankDate);
  assert.equal(Object.keys(after.bankMovementAuthority).length, 1);
  assert.equal(after.adjustments.length, 0); assert.equal(after.reconciliationExpenses.length, 0);
});

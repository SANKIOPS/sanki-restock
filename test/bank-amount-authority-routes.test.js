'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

// Every route runs against this process-local store, never production files,
// an HTTP server, Shopify, or a financial API.
const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'sanki-bank-authority-routes-'));
process.env.DATA_PATH = path.join(tempDir, 'data.json');
fs.writeFileSync(path.join(tempDir, 'expenses.json'), '{}');
const { router, appBankMovements } = require('../modules/expenses');
const file = path.join(tempDir, 'expenses.json');
const account = 'IndusInd Bank 8181';
const draftId = 'BRD-AMOUNT-AUTHORITY-ROUTE';
const expenseId = 'EX-AMOUNT-AUTHORITY-ROUTE';
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

for (const [amount, bankAmount] of [[500, 499], [500, 501], [500, 499.99]]) {
  test('verified automatic bank difference is inclusive and cent-precise: ' + amount + ' -> ' + bankAmount, async () => {
    seed({ amount, bankAmount });
    const result = await reconcile();
    assert.equal(result.status, 200, JSON.stringify(result.body));
    const row = result.body.rows.find(item => item.id === 'bank-0');
    assert.equal(row.status, 'matched');
    assert.equal(row.app.id, appId);
    assert.equal(row.bankAmountAuthority.mode, 'auto');
    assert.equal(row.bankAmountAuthority.originalAmount, amount);
    assert.equal(row.bankAmountAuthority.bankAmount, bankAmount);
    assert.equal(row.bankAmountAuthority.expenseAmountAfter, bankAmount);
    assert.equal(result.body.ledgerClosing, Math.round((2000 - bankAmount) * 100) / 100);
    assert.equal(result.body.balanceDifference, 0);
    assert.equal(result.body.canFinalize, true);
    assert.equal(read().expenses[expenseId].amount, amount, 'preview must not alter the expense');
    assert.equal(read().expenses[expenseId].payments[0].amount, amount);
  });
}

test('₹1.01 difference is never automatically corrected, even with the exact reference', async () => {
  seed({ bankAmount: 498.99 });
  const result = await reconcile();
  assert.equal(result.status, 200, JSON.stringify(result.body));
  const row = result.body.rows.find(item => item.id === 'bank-0');
  assert.notEqual(row.status, 'matched');
  assert.notEqual(row.bankAmountAuthority && row.bankAmountAuthority.mode, 'auto');
  assert.equal(result.body.canFinalizeTransactions, false);
  assert.equal(read().expenses[expenseId].amount, 500);
});

test('small amount proximity without party or reference evidence never authorizes an automatic correction', async () => {
  const store = seed();
  store.expenses[expenseId].payments[0].bankReference = '';
  store.bankReconciliationDrafts[draftId].transactions[0].reference = 'OTHERPAYMENT';
  store.bankReconciliationDrafts[draftId].transactions[0].description = 'Completely unrelated customer';
  write(store);
  const result = await reconcile();
  assert.equal(result.status, 200);
  assert.equal(result.body.rows.some(row => row.bankAmountAuthority && row.bankAmountAuthority.mode === 'auto'), false);
  assert.equal(result.body.canFinalizeTransactions, false);
});

test('manual preview authorizes any reviewed difference but changes no source or ledger', async () => {
  seed({ bankAmount: 1499.37 });
  const before = clone(read().expenses[expenseId]);
  const result = await preview('admin');
  assert.equal(result.status, 200, JSON.stringify(result.body));
  const plan = result.body.bankAmountAuthority;
  assert.equal(plan.eligible, true);
  assert.equal(plan.mode, 'manual');
  assert.equal(plan.originalAmount, 500);
  assert.equal(plan.bankAmount, 1499.37);
  assert.equal(plan.expenseAmountBefore, 500);
  assert.equal(plan.expenseAmountAfter, 1499.37);
  assert.equal(plan.bankDate, bankDate);
  assert.ok(plan.fingerprint);
  assert.deepEqual(read().expenses[expenseId], before);
  assert.deepEqual(read().bankReconciliationDrafts[draftId].resolutions, {});
  assert.deepEqual(read().bankAmountAuthorityApplications, {});
});

test('manual unequal amount requires explicit confirmation and the exact reviewed fingerprint', async () => {
  seed({ bankAmount: 713.28 });
  const candidate = await preview();
  assert.equal(candidate.status, 200, JSON.stringify(candidate.body));
  const fingerprint = candidate.body.bankAmountAuthority.fingerprint;
  for (const patch of [{ confirmBankAmount: false }, { bankAmountAuthorityFingerprint: '' }, { bankAmountAuthorityFingerprint: 'stale-plan' }]) {
    const result = await resolveMatch(fingerprint, patch);
    assert.equal(result.status, patch.confirmBankAmount === false ? 400 : 409, JSON.stringify(result.body));
    assert.equal(read().expenses[expenseId].amount, 500);
    assert.deepEqual(read().bankReconciliationDrafts[draftId].resolutions, {});
  }
});

test('manual correction is staged by Admin, submitted without posting, and applied once by Owner', async () => {
  seed({ bankAmount: 499.5 });
  const candidate = await preview('admin');
  assert.equal(candidate.status, 200, JSON.stringify(candidate.body));
  const fingerprint = candidate.body.bankAmountAuthority.fingerprint;
  const staged = await resolveMatch(fingerprint, {}, 'admin');
  assert.equal(staged.status, 200, JSON.stringify(staged.body));
  let store = read();
  assert.equal(store.expenses[expenseId].amount, 500);
  assert.equal(store.expenses[expenseId].payments[0].amount, 500);
  assert.equal(store.expenses[expenseId].payments[0].date, ledgerDate);
  const pointer = store.bankReconciliationDrafts[draftId].resolutions['bank-0'].bankAmountAuthority;
  assert.deepEqual(Object.keys(pointer), ['planId']);
  assert.equal(store.bankAmountAuthorityProposals[pointer.planId].mode, 'manual');
  const submission = await finalize('admin');
  assert.equal(submission.status, 202, JSON.stringify(submission.body));
  assert.equal(submission.body.approvalPending, true);
  store = read();
  assert.equal(store.expenses[expenseId].amount, 500);
  assert.equal(store.expenses[expenseId].payments[0].amount, 500);
  assert.deepEqual(store.bankAmountAuthorityApplications, {});
  assert.equal((store.bankStatements[account] && store.bankStatements[account].imports || []).length, 0);
  const pending = submission.body.approval;
  const missingApproval = await finalize();
  assert.equal(missingApproval.status, 409);
  assert.equal(read().expenses[expenseId].amount, 500);
  const approved = await finalize('owner', { approveRequestId: pending.id });
  assert.equal(approved.status, 200, JSON.stringify(approved.body));
  store = read();
  const expense = store.expenses[expenseId], payment = expense.payments[0];
  assert.equal(expense.amount, 499.5);
  assert.equal(expense.requestedAmount, 499.5);
  assert.equal(expense.paidAmount, 499.5);
  assert.equal(expense.status, 'paid');
  assert.equal(payment.amount, 499.5);
  assert.equal(payment.date, bankDate);
  assert.equal(expense.date, ledgerDate, 'bill date remains the original expense date');
  assert.equal(expense.payments.length, 1, 'no duplicate payment');
  assert.equal(store.adjustments.length, 0, 'no rounding adjustment or fee');
  assert.equal(store.vendorAdvances.length, 0, 'no artificial vendor advance');
  assert.equal(Object.keys(store.bankAmountAuthorityApplications).length, 1);
  const journal = Object.values(store.bankAmountAuthorityApplications)[0];
  assert.equal(journal.before.amount, 500);
  assert.equal(journal.before.date, ledgerDate);
  assert.equal(journal.after.amount, 499.5);
  assert.equal(journal.after.date, bankDate);
  assert.equal(store.bankReconciliationApprovals[pending.id].status, 'approved');
  assert.equal(store.bankStatements[account].imports.length, 1);
  assert.equal(Object.values(store.bankStatements[account].transactions).length, 1);
  const again = await finalize('owner', { approveRequestId: pending.id });
  assert.ok([404, 409].includes(again.status), JSON.stringify(again.body));
  assert.equal(read().bankStatements[account].imports.length, 1);
  assert.equal(Object.keys(read().bankAmountAuthorityApplications).length, 1);
});

test('manual large correction takes exact bank amount/date and preserves cents in downstream reports', async () => {
  seed({ bankAmount: 1499.37 });
  const candidate = await preview();
  assert.equal(candidate.status, 200, JSON.stringify(candidate.body));
  const staged = await resolveMatch(candidate.body.bankAmountAuthority.fingerprint);
  assert.equal(staged.status, 200, JSON.stringify(staged.body));
  const result = await finalize();
  assert.equal(result.status, 200, JSON.stringify(result.body));
  const expense = read().expenses[expenseId];
  assert.equal(expense.amount, 1499.37);
  assert.equal(expense.paidAmount, 1499.37);
  assert.equal(expense.payments[0].amount, 1499.37);
  assert.equal(expense.payments[0].date, bankDate);
  const movements = appBankMovements(read(), account, 'SANKI');
  assert.equal(movements.find(row => row.id === appId).debit, 1499.37);
  const spending = await invoke('GET', '/api/expenses/spending-dashboard', { query: { nature: 'SANKI', account, from: ledgerDate, to: bankDate } });
  assert.equal(spending.status, 200);
  assert.equal(spending.body.payments.find(row => row.reference === appId).amount, 1499.37);
  assert.equal(spending.body.totalPaid, 1499.37);
  const vendors = await invoke('GET', '/api/expenses/vendors', { query: { nature: 'SANKI', search: 'Authority Test Vendor', to: bankDate } });
  assert.equal(vendors.status, 200);
  const vendor = vendors.body.vendors.find(row => row.name === 'Authority Test Vendor');
  assert.ok(vendor);
  assert.equal(vendor.ledgerRows.find(row => row.type === 'Expense').out, 1499.37);
  assert.equal(vendor.ledgerRows.find(row => row.type === 'Payment').in, 1499.37);
  assert.equal(vendor.ledgerClosingBalance, 0);
  assert.equal(vendors.body.totalDue, 0);
  assert.equal(vendors.body.totalAdvance, 0);
});

test('partial-payment correction changes expense and payment by the same delta, preserving genuine vendor dues', async () => {
  seed({ expenseAmount: 800, bankAmount: 499.5 });
  const candidate = await preview();
  assert.equal(candidate.status, 200, JSON.stringify(candidate.body));
  assert.equal(candidate.body.bankAmountAuthority.expenseAmountAfter, 799.5);
  assert.equal((await resolveMatch(candidate.body.bankAmountAuthority.fingerprint)).status, 200);
  const result = await finalize();
  assert.equal(result.status, 200, JSON.stringify(result.body));
  const expense = read().expenses[expenseId];
  assert.equal(expense.amount, 799.5);
  assert.equal(expense.paidAmount, 499.5);
  assert.equal(expense.status, 'partially_paid');
  assert.equal(expense.amount - expense.paidAmount, 300);
});

test('stale manual bank or source fingerprints fail closed before staging', async () => {
  for (const mutate of [
    store => { store.bankReconciliationDrafts[draftId].transactions[0].debit = 700.01; },
    store => { store.expenses[expenseId].particulars = 'Changed after reviewer preview'; }
  ]) {
    seed({ bankAmount: 700 });
    const candidate = await preview();
    assert.equal(candidate.status, 200, JSON.stringify(candidate.body));
    const store = read(); mutate(store); write(store);
    const result = await resolveMatch(candidate.body.bankAmountAuthority.fingerprint);
    assert.equal(result.status, 409, JSON.stringify(result.body));
    assert.equal(read().expenses[expenseId].amount, 500);
    assert.deepEqual(read().bankReconciliationDrafts[draftId].resolutions, {});
  }
});

test('changing an automatic correction after Admin submission invalidates Owner approval', async () => {
  seed();
  const initial = await reconcile('admin');
  assert.equal(initial.body.rows.find(row => row.id === 'bank-0').bankAmountAuthority.mode, 'auto');
  const submission = await finalize('admin');
  assert.equal(submission.status, 202, JSON.stringify(submission.body));
  const store = read();
  store.expenses[expenseId].particulars = 'Source changed since Admin submitted';
  write(store);
  const result = await finalize('owner', { approveRequestId: submission.body.approval.id });
  assert.equal(result.status, 409, JSON.stringify(result.body));
  assert.equal(read().expenses[expenseId].amount, 500);
  assert.deepEqual(read().bankAmountAuthorityApplications, {});
  assert.equal(read().bankReconciliationApprovals[submission.body.approval.id].status, 'pending');
});

test('manual match never changes money direction, regardless of amount authority', async () => {
  seed({ bankCredit: true, bankAmount: 700 });
  const result = await preview();
  assert.ok([400, 409].includes(result.status), JSON.stringify(result.body));
  assert.match(result.body.error, /direction|money.in|money.out|same/i);
  assert.equal(read().expenses[expenseId].amount, 500);
});

test('finalized expense evidence and foreign draft claims cannot be unlocked by manual amount matching', async () => {
  for (const protect of [
    store => { store.bankStatements['Axis Bank 3448'] = { transactions: {}, imports: [{ id: 'BST-OTHER', reconciliationRows: [{ linkedRecordIds: [appId] }] }] }; },
    store => { store.bankReconciliationDrafts.OTHER = { ...clone(store.bankReconciliationDrafts[draftId]), id: 'OTHER', resolutions: { 'bank-0': { action: 'link_existing', appId, reason: 'Already claimed elsewhere' } } }; },
    store => { store.bankDateOverrides[appId] = { bankDate: ledgerDate, reconciliationDraft: 'FINALIZED-OTHER', bankTransactionId: 'BTX-OTHER' }; }
  ]) {
    const store = seed({ bankAmount: 700 }); protect(store); write(store);
    const result = await preview();
    assert.ok([400, 409].includes(result.status), JSON.stringify(result.body));
    assert.equal(read().expenses[expenseId].amount, 500);
    assert.deepEqual(read().bankAmountAuthorityApplications, {});
  }
});

test('batched, refunded, provisional and settlement-backed payments retain their dedicated safeguards', async () => {
  for (const payment of [
    { batchPaymentId: 'BATCH-OTHER' }, { provisional: true },
    { paytmSettlementId: 'PTM-OTHER' }, { creditCardStatementId: 'CC-OTHER' }
  ]) {
    seed({ bankAmount: 700, payment });
    const result = await preview();
    assert.ok([400, 409].includes(result.status), JSON.stringify(result.body));
    assert.equal(read().expenses[expenseId].payments[0].amount, 500);
  }
  seed({ bankAmount: 700, expense: { refundedAmount: 5, refundStatus: 'partially_refunded' } });
  const result = await preview();
  assert.ok([400, 409].includes(result.status), JSON.stringify(result.body));
  assert.equal(read().expenses[expenseId].amount, 500);
});

test('ordinary claimants cannot preview or submit bank amount corrections', async () => {
  seed({ bankAmount: 700 });
  const result = await preview('claimant');
  assert.equal(result.status, 403, JSON.stringify(result.body));
  assert.equal(read().expenses[expenseId].amount, 500);
  assert.deepEqual(read().bankReconciliationDrafts[draftId].resolutions, {});
});

test('manual accept_match also adopts a difference above ₹1 after explicit review', async () => {
  // accept_match operates on a displayed suggestion; arbitrarily distant
  // candidates use the explicit link_existing picker covered above.
  seed({ bankAmount: 503.28 });
  const candidate = await preview();
  assert.equal(candidate.status, 200, JSON.stringify(candidate.body));
  const accepted = await resolveMatch(candidate.body.bankAmountAuthority.fingerprint, { action: 'accept_match' });
  assert.equal(accepted.status, 200, JSON.stringify(accepted.body));
  assert.equal(read().expenses[expenseId].amount, 500);
  const result = await finalize();
  assert.equal(result.status, 200, JSON.stringify(result.body));
  const expense = read().expenses[expenseId];
  assert.equal(expense.amount, 503.28);
  assert.equal(expense.payments[0].amount, 503.28);
  assert.equal(expense.payments[0].date, bankDate);
  assert.equal(Object.values(read().bankAmountAuthorityApplications)[0].after.date, bankDate);
});

test('an exact-amount manual match keeps normal bank-date behavior without inventing an amount correction', async () => {
  seed({ bankAmount: 500 });
  const candidate = await preview();
  assert.equal(candidate.status, 200, JSON.stringify(candidate.body));
  assert.equal(candidate.body.bankAmountAuthority, null);
  assert.equal(candidate.body.bankDate, bankDate);
  const accepted = await resolveMatch('', { confirmBankAmount: false, action: 'accept_match' });
  assert.equal(accepted.status, 200, JSON.stringify(accepted.body));
  assert.equal(read().expenses[expenseId].payments[0].amount, 500);
  const result = await finalize();
  assert.equal(result.status, 200, JSON.stringify(result.body));
  const store = read();
  assert.equal(store.expenses[expenseId].amount, 500);
  assert.equal(store.expenses[expenseId].payments[0].amount, 500);
  assert.equal(store.bankDateOverrides[appId].bankDate, bankDate);
  assert.equal(Object.keys(store.bankAmountAuthorityApplications).length, 0);
});

test('automatic bank amount authority posts exactly once only after the Owner approves the submitted preview', async () => {
  seed({ bankAmount: 499.99 });
  const submission = await finalize('admin');
  assert.equal(submission.status, 202, JSON.stringify(submission.body));
  assert.ok(submission.body.approval.bankAmountAuthorityFingerprint);
  assert.equal(read().expenses[expenseId].amount, 500);
  assert.deepEqual(read().bankAmountAuthorityApplications, {});
  const result = await finalize('owner', { approveRequestId: submission.body.approval.id });
  assert.equal(result.status, 200, JSON.stringify(result.body));
  const store = read();
  assert.equal(store.expenses[expenseId].amount, 499.99);
  assert.equal(store.expenses[expenseId].paidAmount, 499.99);
  assert.equal(store.expenses[expenseId].payments[0].date, bankDate);
  assert.equal(store.expenses[expenseId].payments.length, 1);
  const application = Object.values(store.bankAmountAuthorityApplications)[0];
  assert.equal(application.before.amount, 500);
  assert.equal(application.after.amount, 499.99);
  const expenseSnapshot = clone(store.expenses[expenseId]);
  const retry = await finalize('owner', { approveRequestId: submission.body.approval.id });
  assert.ok([404, 409].includes(retry.status), JSON.stringify(retry.body));
  assert.deepEqual(read().expenses[expenseId], expenseSnapshot);
  assert.equal(Object.keys(read().bankAmountAuthorityApplications).length, 1);
  assert.equal(read().bankStatements[account].imports.length, 1);
});

test('ordinary receipt and transfer matches adopt large bank differences without duplicate movements', async () => {
  for (const kind of ['receipt', 'transfer']) {
    const store = seed({ bankAmount: 1273.25, bankCredit: kind === 'receipt' });
    store.expenses = {};
    const id = kind === 'receipt' ? 'RCPT-AUTHORITY' : 'TR-AUTHORITY';
    if (kind === 'receipt') store.receipts = [{ id, nature: 'SANKI', account, date: ledgerDate,
      amount: 500, receiptType: 'other_income', bankReference: reference, description: 'Authority receipt' }];
    else store.transfers = [{ id, nature: 'SANKI', fromNature: 'SANKI', toNature: 'SANKI',
      fromAccount: account, toAccount: 'Counter Cash', amount: 500, date: ledgerDate,
      classification: 'internal_transfer', bankReference: reference }];
    write(store);
    const candidate = await preview('owner', { appId: id });
    assert.equal(candidate.status, 200, JSON.stringify(candidate.body));
    const staged = await resolveMatch(candidate.body.bankAmountAuthority.fingerprint, { appId: id });
    assert.equal(staged.status, 200, JSON.stringify(staged.body));
    assert.equal(read()[kind === 'receipt' ? 'receipts' : 'transfers'][0].amount, 500);
    const result = await finalize();
    assert.equal(result.status, 200, JSON.stringify(result.body));
    const finalStore = read(), records = finalStore[kind === 'receipt' ? 'receipts' : 'transfers'];
    assert.equal(records.length, 1);
    assert.equal(records[0].id, id);
    assert.equal(records[0].amount, 1273.25);
    assert.equal(records[0].date, bankDate);
    assert.equal(finalStore.adjustments.length, 0);
    assert.equal(Object.keys(finalStore.bankAmountAuthorityApplications).length, 1);
  }
});

test('manual confirmation of recognized Axis charges preserves principal and posts the separate fee exactly once', async () => {
  for (const fee of [2.95, 5.9]) {
    const axisAccount = 'Axis Bank 3448', transferId = 'TR-AUTHORITY-AXIS-FEE';
    const bankTotal = Math.round((1000 + fee) * 100) / 100;
    const store = seed({ bankAmount: bankTotal });
    store.expenses = {};
    store.openingBalances = { [axisAccount]: 2000 };
    store.openingBalanceDates = { [axisAccount]: '2098-01-01' };
    store.transfers = [{ id: transferId, nature: 'SANKI', fromNature: 'SANKI', toNature: 'SANKI',
      fromAccount: axisAccount, toAccount: 'Counter Cash', amount: 1000, date: ledgerDate,
      classification: 'internal_transfer', bankReference: reference }];
    store.bankReconciliationDrafts[draftId].account = axisAccount;
    write(store);
    const comparison = await invoke('POST', '/api/expenses/bank-statements/reconcile', {
      body: { draftId, account: axisAccount }
    });
    assert.equal(comparison.status, 200, JSON.stringify(comparison.body));
    const row = comparison.body.rows.find(value => value.id === 'bank-0');
    assert.equal(row.status, 'matched');
    assert.equal(row.app.id, transferId);
    assert.equal(row.axisTransferCharge, true);
    assert.equal(row.chargeAmount, fee);
    assert.equal(row.bankAmountAuthority, undefined, 'a known fee must never inflate the transfer principal');
    const blockedPreview = await preview('admin', { appId: transferId });
    assert.equal(blockedPreview.status, 409, JSON.stringify(blockedPreview.body));
    assert.match(blockedPreview.body.error, /recognized Axis transfer charge|separate principal/i);
    const confirmed = await resolveMatch('', { action: 'accept_match', appId: transferId,
      confirmBankAmount: false, reason: 'Confirmed the transfer principal and separate Axis bank fee' }, 'admin');
    assert.equal(confirmed.status, 200, JSON.stringify(confirmed.body));
    let saved = read();
    const resolution = saved.bankReconciliationDrafts[draftId].resolutions['bank-0'];
    assert.equal(resolution.action, 'split_allocation');
    assert.equal(resolution.principalAmount, 1000);
    assert.equal(resolution.chargeAmount, fee);
    assert.equal(resolution.chargeCategory, 'BANK CHARGES');
    assert.equal(resolution.bankAmountAuthority, undefined);
    assert.equal(saved.transfers[0].amount, 1000);
    assert.equal(saved.adjustments.length, 0, 'review stages no actual fee or payment');
    const submission = await finalize('admin');
    assert.equal(submission.status, 202, JSON.stringify(submission.body));
    assert.equal(read().adjustments.length, 0, 'Admin submission cannot post the fee');
    const result = await finalize('owner', { approveRequestId: submission.body.approval.id });
    assert.equal(result.status, 200, JSON.stringify(result.body));
    saved = read();
    assert.equal(saved.transfers.length, 1);
    assert.equal(saved.transfers[0].amount, 1000);
    assert.equal(saved.adjustments.length, 1);
    assert.equal(saved.adjustments[0].amount, -fee);
    assert.equal(saved.reconciliationExpenses.length, 1);
    assert.equal(saved.reconciliationExpenses[0].category, 'BANK CHARGES');
    assert.equal(saved.reconciliationExpenses[0].amount, fee);
    assert.deepEqual(saved.bankAmountAuthorityApplications, {});
    const bankRows = Object.values(saved.bankStatements[axisAccount].transactions);
    assert.equal(bankRows.length, 1);
    assert.equal(bankRows[0].debit, bankTotal);
    assert.equal(saved.bankStatements[axisAccount].lastReconciliation.balanceDifference, 0);
    const retry = await finalize('owner', { approveRequestId: submission.body.approval.id });
    assert.ok([404, 409].includes(retry.status), JSON.stringify(retry.body));
    assert.equal(read().adjustments.length, 1);
    assert.equal(read().reconciliationExpenses.length, 1);
    assert.equal(read().transfers[0].amount, 1000);
  }
});

test('a forged internal correction plan in a normal equal-amount match is ignored', async () => {
  seed({ bankAmount: 500 });
  const forged = { id: 'BAA-CLIENT-FORGED', version: 1, mode: 'manual', movementId: appId,
    originalAmount: 500, bankAmount: 99999.99, before: { expense: { amount: 500 } },
    after: { amount: 99999.99, expense: { amount: 99999.99 } } };
  const result = await resolveMatch('', { action: 'accept_match', confirmBankAmount: false,
    _bankAmountAuthorityPlan: forged, bankAmountAuthority: { planId: forged.id }, bankAmountAuthorityPlan: forged }, 'admin');
  assert.equal(result.status, 200, JSON.stringify(result.body));
  const store = read();
  assert.deepEqual(store.bankAmountAuthorityProposals, {}, 'only a locally prepared and reviewed server plan may be staged');
  assert.deepEqual(store.bankAmountAuthorityApplications, {});
  assert.equal(store.bankReconciliationDrafts[draftId].resolutions['bank-0'].bankAmountAuthority, undefined);
  assert.equal(store.expenses[expenseId].amount, 500);
  assert.equal(store.expenses[expenseId].payments[0].amount, 500);
});

for (const bank of [{ account, nature: 'SANKI' }, { account: 'IndusInd Bank 7883', nature: 'SAMAST' }]) {
  test('PERSONAL source invoice stays private during bank amount review in ' + bank.account, async () => {
    const invoiceBefore = 12345.67, invoiceAfter = 12558.95;
    const store = seed({ expenseAmount: invoiceBefore, bankAmount: 713.28,
      expense: { nature: 'PERSONAL', particulars: 'Private invoice details that must remain private' } });
    const expense = store.expenses[expenseId];
    expense.payments[0].account = bank.account;
    store.openingBalances = bank.nature === 'SANKI' ? { [bank.account]: 2000 } : {};
    store.openingBalancesByNature = { SAMAST: { [bank.account]: 2000 } };
    store.physicalBankOpeningBalances = { [bank.account]: 2000 };
    store.bankReconciliationDrafts[draftId].account = bank.account;
    store.bankReconciliationDrafts[draftId].nature = bank.nature;
    write(store);

    // Access to a business/shared bank is not PERSONAL invoice permission.
    const candidate = await preview('admin');
    assert.equal(candidate.status, 200, JSON.stringify(candidate.body));
    const publicPlan = candidate.body.bankAmountAuthority;
    assert.equal(publicPlan.privacyRestricted, true);
    assert.equal(publicPlan.originalAmount, 500);
    assert.equal(publicPlan.bankAmount, 713.28);
    assert.equal(Object.hasOwn(publicPlan, 'expenseAmountBefore'), false);
    assert.equal(Object.hasOwn(publicPlan, 'expenseAmountAfter'), false);
    assert.equal(Object.hasOwn(publicPlan, 'before'), false);
    assert.equal(Object.hasOwn(publicPlan, 'after'), false);
    assert.equal(JSON.stringify(candidate.body).includes(String(invoiceBefore)), false);
    assert.equal(JSON.stringify(candidate.body).includes(String(invoiceAfter)), false);

    const staged = await resolveMatch(publicPlan.fingerprint, {}, 'admin');
    assert.equal(staged.status, 200, JSON.stringify(staged.body));
    const saved = read(), resolution = saved.bankReconciliationDrafts[draftId].resolutions['bank-0'];
    assert.deepEqual(resolution.bankAmountAuthority, { planId: publicPlan.fingerprint });
    const privatePlan = saved.bankAmountAuthorityProposals[publicPlan.fingerprint];
    assert.equal(privatePlan.sourceNature, 'PERSONAL');
    assert.equal(privatePlan.before.expense.amount, invoiceBefore);
    assert.equal(privatePlan.after.expense.amount, invoiceAfter);
    assert.equal(saved.expenses[expenseId].amount, invoiceBefore, 'Admin review remains staged');
    const publicRow = staged.body.rows.find(row => row.id === 'bank-0');
    assert.deepEqual(publicRow.resolution.bankAmountAuthority, { planId: publicPlan.fingerprint });
    assert.equal(Object.hasOwn(publicRow.bankAmountAuthority, 'expenseAmountBefore'), false);
    assert.equal(Object.hasOwn(publicRow.bankAmountAuthority, 'expenseAmountAfter'), false);
    const publicResponse = JSON.stringify(staged.body);
    assert.equal(publicResponse.includes(String(invoiceBefore)), false);
    assert.equal(publicResponse.includes(String(invoiceAfter)), false);
    // Existing bank movement narration/particulars are an established bank
    // access surface. This rule must add no private invoice totals or snapshots.
    assert.equal(publicResponse.includes('sourceFingerprint'), false);

    const submission = await finalize('admin');
    assert.equal(submission.status, 202, JSON.stringify(submission.body));
    const result = await finalize('owner', { approveRequestId: submission.body.approval.id });
    assert.equal(result.status, 200, JSON.stringify(result.body));
    const finalStore = read();
    assert.equal(finalStore.expenses[expenseId].amount, invoiceAfter);
    assert.equal(finalStore.expenses[expenseId].payments[0].amount, 713.28);
    const applicationAudit = finalStore.auditLog.filter(entry => entry.action === 'BANK_AMOUNT_AUTHORITY_APPLIED');
    assert.equal(applicationAudit.length, 1);
    assert.equal(applicationAudit[0].nature, 'PERSONAL', 'private source audit cannot inherit business-bank visibility');
    assert.equal(applicationAudit[0].before.expense.amount, invoiceBefore);
    assert.equal(applicationAudit[0].after.expense.amount, invoiceAfter);
    const adminAudit = await invoke('GET', '/api/expenses/audit-log', { role: 'admin', query: { subject: expenseId } });
    assert.equal(adminAudit.status, 200);
    assert.equal(adminAudit.body.entries.some(entry => entry.action === 'BANK_AMOUNT_AUTHORITY_APPLIED'), false);
    assert.equal(JSON.stringify(adminAudit.body).includes(String(invoiceBefore)), false);
    assert.equal(JSON.stringify(adminAudit.body).includes(String(invoiceAfter)), false);
    const ownerAudit = await invoke('GET', '/api/expenses/audit-log', { query: { subject: expenseId, nature: 'PERSONAL' } });
    assert.equal(ownerAudit.status, 200);
    assert.equal(ownerAudit.body.entries.some(entry => entry.action === 'BANK_AMOUNT_AUTHORITY_APPLIED'), true);
  });
}

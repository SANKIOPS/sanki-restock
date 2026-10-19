'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'sanki-gagan-transfer-'));
process.env.DATA_PATH = path.join(temp, 'data.json');
const file = path.join(temp, 'expenses.json');
fs.writeFileSync(file, JSON.stringify({ expenses: {}, receipts: [], receivables: {}, oneTimeMigrations: {} }));
const { router, applyFinalizedInternalTransfers, summaryForPL } = require('../modules/expenses');
const { buildReport } = require('../modules/pnl-report');
const accounts = ['IndusInd Bank 7883', 'ICICI Bank 0992', 'ICICI Bank 0993'];
const bank = 'Tiana Traders IndusInd 0437';
const date = '2098-06-14';
const clone = value => JSON.parse(JSON.stringify(value));
const read = () => JSON.parse(fs.readFileSync(file, 'utf8'));
const write = value => fs.writeFileSync(file, JSON.stringify(value));

function request(method, route, { role = 'admin', username = '', body = {}, query = {} } = {}) {
  return {
    method, path: route, url: route, originalUrl: route, baseUrl: '', body, query, params: {},
    headers: { 'user-agent': 'Gagan transfer regression test' },
    get(name) { return this.headers[String(name).toLowerCase()] || ''; },
    user: { role, roles: [role], username: username || (role === 'admin' ? 'prashant' : role === 'claimant' ? 'arshpreet' : 'owner-user') }
  };
}

function invoke(route, options = {}) {
  const method = options.method || 'GET';
  const layer = router.stack.find(item => item.route && item.route.path === route && item.route.methods[method.toLowerCase()]);
  assert.ok(layer, route);
  let status = 200, result;
  layer.route.stack[0].handle(request(method, route, options), {
    status(code) { status = code; return this; },
    json(value) { result = value; return this; }
  }, () => { throw new Error('Unexpected next: ' + route); });
  return { status, body: result };
}

function dispatch(route, options = {}) {
  const method = options.method || 'POST';
  return new Promise((resolve, reject) => {
    let status = 200, finished = false;
    const headers = {};
    const finish = body => { if (!finished) { finished = true; resolve({ status, body }); } };
    const res = {
      statusCode: 200, headersSent: false,
      status(code) { this.statusCode = status = code; return this; },
      setHeader(name, value) { headers[String(name).toLowerCase()] = value; },
      getHeader(name) { return headers[String(name).toLowerCase()]; },
      removeHeader(name) { delete headers[String(name).toLowerCase()]; },
      json(body) { this.headersSent = true; finish(body); return this; },
      send(body) { this.headersSent = true; finish(body); return this; },
      end(body) { this.headersSent = true; finish(body); return this; }
    };
    router.handle(request(method, route, options), res, error => error ? reject(error) : finish(undefined));
  });
}

// Bootstrap ordinary migrations once, then retain their markers in each isolated
// scenario. These tests never load or write the user's actual accounting files.
assert.equal(invoke('/api/expenses/config', { role: 'owner' }).status, 200);
const baseline = read();
const preservedExpense = {
  id: 'EX-PRESERVE', nature: 'SANKI', date: '2098-05-01', status: 'paid', approvedAt: '2098-05-01',
  amount: 119, paidAmount: 119, vendor: 'Preserved vendor', particulars: 'Existing paid bill',
  payments: [{ id: 'PAY-001', date: '2098-05-01', amount: 119, account: 'Prashant Cash', proof: '/existing-proof.jpg', proofs: ['/existing-proof.jpg'] }],
  reimbursementPayments: [], account: '', billPhoto: '', billPhotos: [], purchasePaymentProof: '', purchasePaymentProofs: [],
  paymentProof: '', paymentProofs: [], provisionalPaidAmount: 0
};
const preservedBook = {
  transactions: {}, imports: [{ id: 'BST-PRESERVE', finalizedAt: '2098-05-01T10:00:00Z', reconciliationRows: [] }],
  lastReconciliation: { to: '2098-05-01', closingBalance: 7, balanceReconciled: true }
};

function reset(extra = {}) {
  const store = Object.assign(clone(baseline), {
    expenses: { [preservedExpense.id]: clone(preservedExpense) }, transfers: [], receipts: [], receivables: {},
    adjustments: [], reconciliationExpenses: [], loanRepayments: [], vendorAdvances: [], vendorOpeningPayables: [],
    salesRefunds: [], bankTruthMovements: [], bankDateOverrides: {}, bankReconciliationLinks: {},
    bankReconciliationDrafts: {}, bankReconciliationApprovals: {},
    bankStatements: { 'Tiana 0425': clone(preservedBook) },
    openingBalances: { [bank]: 10000 }, openingBalancesByNature: { PERSONAL: Object.fromEntries(accounts.map(account => [account, 1000])) },
    auditLog: [], transferSeq: 0, adjSeq: 0
  }, extra);
  write(store);
  return store;
}

function assertPreserved() {
  const store = read();
  assert.deepEqual(store.expenses[preservedExpense.id], preservedExpense, 'an existing paid expense and its proof/payment must not change');
  assert.deepEqual(store.bankStatements['Tiana 0425'], preservedBook, 'an unrelated finalized reconciliation must not change');
}

function assertProfitTreatment(expectedFee = 0) {
  const store = read();
  const report = buildReport({ expenses: store, orders: { orders: {} }, purchases: {}, opening: { lots: [] }, salary: {}, manualSales: {}, feeds: [] }, { from: date, to: date });
  assert.equal(report.totals.grossSales, 0, 'owner funding is not another sale');
  assert.equal(report.totals.otherIncome, 0, 'owner funding is not operating income');
  assert.equal(report.totals.expenses, expectedFee, 'only a confirmed business bank fee is an expense');
  assert.equal(summaryForPL(date, date).Shared.running, expectedFee);
  assert.equal(store.receipts.length, 0, 'owner transfers must not create sales/income receipts');
}

function transferBody(account, incoming, extra = {}) {
  return Object.assign({
    fromNature: incoming ? 'PERSONAL' : 'SANKI', toNature: incoming ? 'SANKI' : 'PERSONAL',
    fromAccount: incoming ? account : bank, toAccount: incoming ? bank : account,
    classification: incoming ? 'owner_contribution' : 'owner_withdrawal', amount: 123.45, date,
    proof: '/transfer-proof.jpg', note: 'Verified owner funds transfer'
  }, extra);
}

function draftFixture({ id = 'BRD-PERSONAL-TRANSFER', incoming = false, amount = 1005.9, nature = 'SANKI', account = bank } = {}) {
  const debit = incoming ? 0 : amount, credit = incoming ? amount : 0, closing = Math.round((10000 + credit - debit) * 100) / 100;
  return {
    id, nature, account, transactions: [{ date, debit, credit, balance: closing, reference: id + '-REF', description: 'Verified Gagan owner transfer' }],
    summary: { from: date, to: date, openingBalance: 10000, closingBalance: closing, totalDebits: debit, totalCredits: credit, validated: true },
    resolutions: {}, matchingPolicy: 'balanced_date_amount_v5', createdBy: 'prashant', createdAt: '2098-06-14T10:00:00Z', expiresAt: '2099-01-01T00:00:00Z'
  };
}

test.after(() => fs.rmSync(temp, { recursive: true, force: true }));

test('Owner and Admin Prashant receive only the three explicit PERSONAL transfer counterparts', () => {
  reset();
  for (const role of ['owner', 'admin']) {
    const config = invoke('/api/expenses/config', { role }).body;
    assert.deepEqual(config.ownerPersonalTransferAccounts.map(row => row.account), accounts);
    assert.ok(config.ownerPersonalTransferAccounts.every(row => row.nature === 'PERSONAL' && row.label.includes('Personal')));
  }
  const config = invoke('/api/expenses/config').body;
  assert.deepEqual(config.approvalNatures, ['SANKI', 'SAMAST']);
  assert.deepEqual(config.accountsByNature.PERSONAL, []);
  for (const key of ['transferAccountsByNature', 'bankAccountsByNature', 'reconciliationAccountsByNature', 'ledgerAccountsByNature']) {
    assert.deepEqual(config[key].PERSONAL, [], key + ' must not grant access to the private book');
    assert.ok(!config[key].SANKI.some(name => accounts.includes(name)), key + ' must not relabel personal accounts as SANKI');
  }
  for (const options of [{ role: 'admin', username: 'unrelated-admin' }, { role: 'claimant' }, { role: 'claimant', username: 'prashant' }, { role: 'accounting' }]) {
    assert.deepEqual(invoke('/api/expenses/config', options).body.ownerPersonalTransferAccounts, [], JSON.stringify(options));
  }
  assertPreserved();
});

for (const role of ['owner', 'admin']) for (const account of accounts) for (const incoming of [false, true]) {
  test(role + ' records ' + (incoming ? 'contribution from ' : 'withdrawal to ') + account + ' with explicit entity identity', () => {
    reset();
    const body = transferBody(account, incoming);
    const result = invoke('/api/expenses/transfers', { method: 'POST', role, body });
    assert.equal(result.status, 200, JSON.stringify(result.body));
    const saved = read().transfers;
    assert.equal(saved.length, 1);
    for (const key of ['fromNature', 'toNature', 'fromAccount', 'toAccount', 'classification', 'amount', 'date', 'proof']) assert.equal(saved[0][key], body[key], key);
    const business = invoke('/api/expenses/account-ledger', { role: 'owner', query: { nature: 'SANKI', account: bank } }).body;
    const personal = invoke('/api/expenses/account-ledger', { role: 'owner', query: { nature: 'PERSONAL', account } }).body;
    assert.equal(business.entries.filter(entry => entry.id === saved[0].id).length, 1);
    assert.equal(personal.entries.filter(entry => entry.id === saved[0].id).length, 1);
    const samast = invoke('/api/expenses/account-ledger', { role: 'owner', query: { nature: 'SAMAST', account } }).body;
    assert.equal(samast.entries.some(entry => entry.id === saved[0].id), false, 'the same physical bank account must not receive a SAMAST movement');
    assert.equal(business.balance, incoming ? 10123.45 : 9876.55);
    assert.equal(personal.balance, incoming ? 876.55 : 1123.45);
    assertProfitTreatment();
    assertPreserved();
  });
}

test('new transfer selection does not authorize other users, accounts, books or classifications', () => {
  const cases = [
    { role: 'admin', username: 'unrelated-admin' },
    { role: 'claimant', username: 'prashant' },
    { role: 'claimant', username: 'arshpreet' },
    { role: 'accounting' },
    { body: transferBody('Namita 5464', false) },
    { body: transferBody('Gagan Personal Cash', false) },
    { body: transferBody('Unlisted Bank 6666', false) },
    { body: transferBody(accounts[0], false, { fromNature: 'SAMAST', fromAccount: 'Kirti Nagar Cash' }) },
    { body: transferBody(accounts[0], false, { fromAccount: 'Paytm Settlement Clearing' }) },
    { body: transferBody(accounts[0], false, { fromAccount: 'Arshpreet 1919' }) },
    { body: transferBody(accounts[0], false, { classification: 'owner_contribution' }) },
    { body: transferBody(accounts[0], true, { classification: 'owner_withdrawal' }) },
    { body: transferBody(accounts[0], false, { classification: 'inter_entity_loan' }) },
    { body: transferBody(accounts[0], false, { fromNature: 'PERSONAL', fromAccount: accounts[1] }) }
  ];
  for (const options of cases) {
    reset();
    const result = invoke('/api/expenses/transfers', { method: 'POST', body: transferBody(accounts[0], false), ...options });
    assert.equal(result.status, 403, JSON.stringify({ options, response: result.body }));
    assert.equal(read().transfers.length, 0);
    assertPreserved();
  }
});

test('PERSONAL counterpart transfers retain proof and positive-amount requirements', () => {
  for (const extra of [{ proof: '' }, { amount: 0 }, { amount: -10 }]) {
    reset();
    assert.equal(invoke('/api/expenses/transfers', { method: 'POST', body: transferBody(accounts[0], false, extra) }).status, 400);
    assert.equal(read().transfers.length, 0);
    assertPreserved();
  }
});

for (const account of accounts) test('reviewing a bank withdrawal to ' + account + ' defers posting, preserves full debit and posts once', async () => {
  const draft = draftFixture({ id: 'BRD-WITHDRAW-' + account.slice(-4) });
  reset({ bankReconciliationDrafts: { [draft.id]: draft } });
  const body = { draftId: draft.id, rowId: 'bank-0', action: 'create_internal_transfer', otherNature: 'PERSONAL', otherAccount: account, principalAmount: 1000, chargeAmount: 5.9, reason: 'Statement confirms owner withdrawal and bank charge' };
  const reviewed = invoke('/api/expenses/bank-statements/resolve', { method: 'POST', body });
  assert.equal(reviewed.status, 200, JSON.stringify(reviewed.body));
  assert.equal(reviewed.body.balanceDifference, 0);
  const decision = read().bankReconciliationDrafts[draft.id].resolutions['bank-0'];
  assert.equal(decision.otherNature, 'PERSONAL');
  assert.equal(decision.otherAccount, account);
  assert.equal(decision.classification, 'owner_withdrawal');
  assert.equal(read().transfers.length, 0, 'review itself must not post a payment or transfer');
  assert.equal(read().adjustments.length, 0);
  const requested = await dispatch('/api/expenses/bank-statements/finalize', { body: { draftId: draft.id } });
  assert.equal(requested.status, 202, JSON.stringify(requested.body));
  assert.equal(read().transfers.length, 0, 'Admin submission still requires Owner approval');
  assert.equal(read().adjustments.length, 0);
  const approved = await dispatch('/api/expenses/bank-statements/finalize', { role: 'owner', body: { draftId: draft.id, approveRequestId: requested.body.approval.id } });
  assert.equal(approved.status, 200, JSON.stringify(approved.body));
  assert.equal(approved.body.balanceDifference, 0);
  const posted = read(), transfer = posted.transfers[0];
  assert.equal(posted.transfers.length, 1);
  assert.deepEqual([transfer.fromNature, transfer.fromAccount, transfer.toNature, transfer.toAccount, transfer.classification, transfer.amount], ['SANKI', bank, 'PERSONAL', account, 'owner_withdrawal', 1000]);
  assert.equal(posted.adjustments.length, 1);
  assert.deepEqual([posted.adjustments[0].nature, posted.adjustments[0].account, posted.adjustments[0].amount], ['SANKI', bank, -5.9]);
  assert.equal(posted.reconciliationExpenses.find(expense => expense.adjustmentId === posted.adjustments[0].id).category, 'BANK CHARGES');
  assert.equal(posted.bankStatements[bank].imports.at(-1).reconciliationRows[0].bank.debit, 1005.9);
  assert.equal(invoke('/api/expenses/account-ledger', { role: 'owner', query: { nature: 'SANKI', account: bank } }).body.balance, 8994.1);
  assert.equal(invoke('/api/expenses/account-ledger', { role: 'owner', query: { nature: 'PERSONAL', account } }).body.balance, 2000);
  applyFinalizedInternalTransfers({ ...draft, resolutions: { 'bank-0': decision } }, 'owner-user');
  assert.equal(read().transfers.length, 1, 'replaying the finalized decision cannot duplicate the principal');
  assert.equal(read().adjustments.length, 1, 'replaying cannot duplicate bank charges');
  assertProfitTreatment(5.9);
  assertPreserved();
});

for (const account of accounts) test('staged bank contribution from ' + account + ' remains unposted until Owner approval', async () => {
  const draft = draftFixture({ id: 'BRD-STAGED-CONTRIB-' + account.slice(-4), incoming: true, amount: 1000 });
  reset({ bankReconciliationDrafts: { [draft.id]: draft } });
  const reviewed = invoke('/api/expenses/bank-statements/resolve', { method: 'POST', body: {
    draftId: draft.id, rowId: 'bank-0', action: 'create_internal_transfer', otherNature: 'PERSONAL', otherAccount: account,
    principalAmount: 1000, chargeAmount: 0, reason: 'Statement confirms owner contribution'
  } });
  assert.equal(reviewed.status, 200, JSON.stringify(reviewed.body));
  assert.equal(reviewed.body.balanceDifference, 0);
  const resolution = read().bankReconciliationDrafts[draft.id].resolutions['bank-0'];
  assert.deepEqual([resolution.otherNature, resolution.otherAccount, resolution.classification], ['PERSONAL', account, 'owner_contribution']);
  assert.equal(read().transfers.length, 0);
  const requested = await dispatch('/api/expenses/bank-statements/finalize', { body: { draftId: draft.id } });
  assert.equal(requested.status, 202, JSON.stringify(requested.body));
  assert.equal(read().transfers.length, 0);
  const approved = await dispatch('/api/expenses/bank-statements/finalize', { role: 'owner', body: { draftId: draft.id, approveRequestId: requested.body.approval.id } });
  assert.equal(approved.status, 200, JSON.stringify(approved.body));
  assert.equal(approved.body.balanceDifference, 0);
  const transfer = read().transfers[0];
  assert.equal(read().transfers.length, 1);
  assert.deepEqual([transfer.fromNature, transfer.fromAccount, transfer.toNature, transfer.toAccount, transfer.classification, transfer.amount], ['PERSONAL', account, 'SANKI', bank, 'owner_contribution', 1000]);
  applyFinalizedInternalTransfers({ ...draft, resolutions: { 'bank-0': resolution } }, 'owner-user');
  assert.equal(read().transfers.length, 1);
  assert.equal(read().adjustments.length, 0);
  assertProfitTreatment();
  assertPreserved();
});

test('corrupt staged PERSONAL counterpart decisions fail before finalized bank or financial writes', async () => {
  for (const scenario of [
    { otherNature: 'PERSONAL', otherAccount: 'Namita 5464', classification: 'owner_withdrawal' },
    { otherNature: 'SAMAST', otherAccount: 'ICICI Bank 0993', classification: 'owner_withdrawal' },
    { otherNature: 'PERSONAL', otherAccount: 'Unlisted Bank 6666', classification: 'owner_withdrawal' },
    { incoming: true, otherNature: 'PERSONAL', otherAccount: accounts[0], classification: 'owner_withdrawal' }
  ]) {
    const draft = draftFixture({ incoming: scenario.incoming, amount: 1000 });
    draft.resolutions['bank-0'] = {
      action: 'create_internal_transfer', otherNature: scenario.otherNature, otherAccount: scenario.otherAccount,
      classification: scenario.classification, principalAmount: 1000, chargeAmount: 0, reason: 'Malformed retained decision must not post'
    };
    reset({ bankReconciliationDrafts: { [draft.id]: draft } });
    // Complete the ordinary load-time historical-fixture normalization before
    // taking the no-write snapshot; it is unrelated to this transfer decision.
    assert.equal(invoke('/api/expenses/config', { role: 'owner' }).status, 200);
    const before = read();
    const rejected = await dispatch('/api/expenses/bank-statements/finalize', { role: 'owner', body: { draftId: draft.id } });
    assert.equal(rejected.status, 409, JSON.stringify({ scenario, response: rejected.body }));
    const after = read();
    for (const key of ['transfers', 'receipts', 'adjustments', 'reconciliationExpenses', 'bankTruthMovements', 'bankStatements', 'bankDateOverrides', 'bankReconciliationLinks']) {
      assert.deepEqual(after[key], before[key], key + ' must remain unchanged when retained authorization is invalid');
    }
    assert.deepEqual(after.bankReconciliationDrafts[draft.id].resolutions, before.bankReconciliationDrafts[draft.id].resolutions);
    assertPreserved();
  }
});

for (const scenario of [
  { name: 'account suffix alias', otherNature: 'PERSONAL', otherAccount: 'ICICI 0992' },
  { name: 'spaced entity', otherNature: ' PERSONAL ', otherAccount: 'ICICI Bank 0992' },
  { name: 'mixed-case entity and account', otherNature: ' personal ', otherAccount: ' icici bank 0992 ' }
]) test('retained ' + scenario.name + ' is canonicalized before both preflight and posting', async () => {
  const draft = draftFixture({ id: 'BRD-CANONICAL-' + scenario.name.replace(/ /g, '-') });
  draft.resolutions['bank-0'] = {
    action: 'create_internal_transfer', otherNature: scenario.otherNature, otherAccount: scenario.otherAccount,
    classification: 'owner_withdrawal', principalAmount: 1000, chargeAmount: 5.9, reason: 'Retained verified owner bank transfer'
  };
  reset({ bankReconciliationDrafts: { [draft.id]: draft } });
  const approved = await dispatch('/api/expenses/bank-statements/finalize', { role: 'owner', body: { draftId: draft.id } });
  assert.equal(approved.status, 200, JSON.stringify(approved.body));
  assert.equal(approved.body.balanceDifference, 0);
  assert.equal(invoke('/api/expenses/account-ledger', { role: 'owner', query: { nature: 'SANKI', account: bank } }).status, 200);
  const saved = read(), transfer = saved.transfers[0];
  assert.equal(saved.transfers.length, 1, 'a canonical alias must not cause a late failure or duplicate principal');
  assert.deepEqual([transfer.fromNature, transfer.fromAccount, transfer.toNature, transfer.toAccount, transfer.classification, transfer.amount], ['SANKI', bank, 'PERSONAL', 'ICICI Bank 0992', 'owner_withdrawal', 1000]);
  assert.equal(saved.adjustments.length, 1);
  assert.equal(saved.adjustments[0].amount, -5.9);
  const row = saved.bankStatements[bank].imports.at(-1).reconciliationRows[0];
  assert.equal(row.bank.debit, 1005.9);
  assert.deepEqual(new Set(row.linkedRecordIds), new Set([transfer.id, saved.adjustments[0].id]), 'an ordinary ledger load must retain exact finalized principal and fee evidence links');
  applyFinalizedInternalTransfers(draft, 'owner-user');
  assert.equal(read().transfers.length, 1);
  assert.equal(read().adjustments.length, 1);
  assertProfitTreatment(5.9);
  assertPreserved();
});

for (const changedPart of ['counterpart', 'bank reference']) test('Owner approval rejects a changed ' + changedPart + ' until Admin resubmits the current personal-transfer review', async () => {
  const draft = draftFixture({ id: 'BRD-CHANGED-' + changedPart.replace(/ /g, '-'), amount: 1000 });
  reset({ bankReconciliationDrafts: { [draft.id]: draft } });
  const reviewed = invoke('/api/expenses/bank-statements/resolve', { method: 'POST', body: {
    draftId: draft.id, rowId: 'bank-0', action: 'create_internal_transfer', otherNature: 'PERSONAL', otherAccount: accounts[0],
    principalAmount: 1000, chargeAmount: 0, reason: 'Owner personal withdrawal reviewed by Admin'
  } });
  assert.equal(reviewed.status, 200, JSON.stringify(reviewed.body));
  const requested = await dispatch('/api/expenses/bank-statements/finalize', { body: { draftId: draft.id } });
  assert.equal(requested.status, 202, JSON.stringify(requested.body));
  const changed = read();
  if (changedPart === 'counterpart') changed.bankReconciliationDrafts[draft.id].resolutions['bank-0'].otherAccount = accounts[1];
  else changed.bankReconciliationDrafts[draft.id].transactions[0].reference = 'CHANGED-BANK-REFERENCE';
  write(changed);
  const rejected = await dispatch('/api/expenses/bank-statements/finalize', { role: 'owner', body: { draftId: draft.id, approveRequestId: requested.body.approval.id } });
  assert.equal(rejected.status, 409, JSON.stringify(rejected.body));
  assert.match(rejected.body.error, /changed since review|resubmit/i);
  const unchanged = read();
  for (const key of ['transfers', 'receipts', 'adjustments', 'reconciliationExpenses', 'bankTruthMovements', 'bankStatements', 'bankReconciliationApprovals']) {
    assert.deepEqual(unchanged[key], changed[key], key + ' must remain unchanged when submitted evidence is stale');
  }
  const resubmitted = await dispatch('/api/expenses/bank-statements/finalize', { body: { draftId: draft.id } });
  assert.equal(resubmitted.status, 202, JSON.stringify(resubmitted.body));
  assert.notEqual(resubmitted.body.approval.ownerPersonalTransferFingerprint, requested.body.approval.ownerPersonalTransferFingerprint);
  assert.equal(read().transfers.length, 0);
  const approved = await dispatch('/api/expenses/bank-statements/finalize', { role: 'owner', body: { draftId: draft.id, approveRequestId: resubmitted.body.approval.id } });
  assert.equal(approved.status, 200, JSON.stringify(approved.body));
  assert.equal(approved.body.balanceDifference, 0);
  const transfer = read().transfers[0];
  assert.equal(read().transfers.length, 1);
  assert.equal(transfer.toAccount, changedPart === 'counterpart' ? accounts[1] : accounts[0]);
  assert.equal(transfer.bankReference, changedPart === 'bank reference' ? 'CHANGED-BANK-REFERENCE' : draft.transactions[0].reference);
  assertProfitTreatment();
  assertPreserved();
});

for (const account of accounts) test('incoming bank creation from ' + account + ' keeps PERSONAL identity and cannot post twice', () => {
  const draft = draftFixture({ id: 'BRD-CONTRIB-' + account.slice(-4), incoming: true, amount: 1000 });
  reset({ bankReconciliationDrafts: { [draft.id]: draft } });
  const body = { draftId: draft.id, rowId: 'bank-0', sourceKind: 'internal', fromNature: 'PERSONAL', fromAccount: account, classification: 'owner_contribution', note: 'Owner personal funds received by company' };
  const made = invoke('/api/expenses/bank-statements/create-incoming', { method: 'POST', body });
  assert.equal(made.status, 200, JSON.stringify(made.body));
  assert.equal(made.body.balanceDifference, 0);
  const saved = read().transfers;
  assert.equal(saved.length, 1);
  assert.deepEqual([saved[0].fromNature, saved[0].fromAccount, saved[0].toNature, saved[0].toAccount, saved[0].classification, saved[0].amount], ['PERSONAL', account, 'SANKI', bank, 'owner_contribution', 1000]);
  assert.ok(invoke('/api/expenses/bank-statements/create-incoming', { method: 'POST', body }).status >= 400);
  assert.equal(read().transfers.length, 1);
  assertProfitTreatment();
  assertPreserved();
});

test('bank-created PERSONAL transfers reject invalid routes, mismatched charges and incoming fees without posting', () => {
  const cases = [
    { incoming: false, extra: { otherAccount: 'Namita 5464' }, status: 403 },
    { incoming: false, extra: { otherAccount: 'Gagan Personal Cash' }, status: 403 },
    { incoming: false, extra: { principalAmount: 500, chargeAmount: 5.9 }, status: 400 },
    { incoming: false, extra: { principalAmount: 1000, chargeAmount: -5.9 }, status: 400 },
    { incoming: true, extra: { principalAmount: 1000, chargeAmount: 5.9 }, status: 400 },
    { incoming: false, username: 'unrelated-admin', status: 403 },
    { incoming: false, role: 'claimant', username: 'prashant', status: 403 }
  ];
  for (const scenario of cases) {
    const draft = draftFixture({ incoming: scenario.incoming });
    reset({ bankReconciliationDrafts: { [draft.id]: draft } });
    const body = { draftId: draft.id, rowId: 'bank-0', action: 'create_internal_transfer', otherNature: 'PERSONAL', otherAccount: accounts[0], principalAmount: 1000, chargeAmount: 5.9, reason: 'Validate exact bank transfer', ...scenario.extra };
    const result = invoke('/api/expenses/bank-statements/resolve', { method: 'POST', role: scenario.role, username: scenario.username, body });
    assert.equal(result.status, scenario.status, JSON.stringify({ scenario, response: result.body }));
    assert.deepEqual(read().bankReconciliationDrafts[draft.id].resolutions, {});
    assert.equal(read().transfers.length, 0);
    assert.equal(read().adjustments.length, 0);
    assertPreserved();
  }
});

test('cross-entity incoming creation remains restricted to the exact Admin Prashant owner-contribution route', () => {
  for (const scenario of [
    { username: 'unrelated-admin' }, { role: 'claimant', username: 'prashant' },
    { extra: { fromAccount: 'Namita 5464' } }, { extra: { fromAccount: 'Gagan Personal Cash' } },
    { extra: { classification: 'owner_withdrawal' } }, { extra: { classification: 'inter_entity_loan' } }
  ]) {
    const draft = draftFixture({ incoming: true, amount: 1000 });
    reset({ bankReconciliationDrafts: { [draft.id]: draft } });
    const result = invoke('/api/expenses/bank-statements/create-incoming', { method: 'POST', role: scenario.role, username: scenario.username, body: {
      draftId: draft.id, rowId: 'bank-0', sourceKind: 'internal', fromNature: 'PERSONAL', fromAccount: accounts[0], classification: 'owner_contribution', note: 'Cannot bypass transfer access', ...scenario.extra
    } });
    assert.equal(result.status, 403, JSON.stringify({ scenario, response: result.body }));
    assert.equal(read().transfers.length, 0);
    assert.deepEqual(read().bankReconciliationDrafts[draft.id].resolutions, {});
    assertPreserved();
  }
});

test('restoring a finalized incoming PERSONAL contribution remains Owner-only, explicit and duplicate-safe', () => {
  for (const account of accounts) {
    const recordId = 'BST-RESTORE-' + account.slice(-4), rowId = 'bank-restore';
    reset();
    const store = read();
    store.bankStatements[bank] = { transactions: {}, imports: [{ id: recordId, to: date, statementSummary: { closingBalance: 11000 }, carriedReconciliationRows: [{ id: rowId, bank: { date, reference: recordId, debit: 0, credit: 1000 }, linkedRecordIds: [] }] }], lastReconciliation: {} };
    write(store);
    const body = { nature: 'SANKI', account: bank, recordId, rowId, fromNature: 'PERSONAL', fromAccount: account, classification: 'owner_contribution', reason: 'Restore confirmed owner contribution' };
    assert.equal(invoke('/api/expenses/bank-statements/restore-finalized-transfer', { method: 'POST', body }).status, 403);
    assert.equal(read().transfers.length, 0);
    const restored = invoke('/api/expenses/bank-statements/restore-finalized-transfer', { method: 'POST', role: 'owner', body });
    assert.equal(restored.status, 200, JSON.stringify(restored.body));
    assert.equal(restored.body.balanceDifference, 0);
    const transfer = read().transfers[0];
    assert.deepEqual([transfer.fromNature, transfer.fromAccount, transfer.toNature, transfer.toAccount, transfer.classification, transfer.amount], ['PERSONAL', account, 'SANKI', bank, 'owner_contribution', 1000]);
    assert.equal(invoke('/api/expenses/bank-statements/restore-finalized-transfer', { method: 'POST', role: 'owner', body }).status, 409);
    assert.equal(read().transfers.length, 1);
    assertProfitTreatment();
    assertPreserved();
  }
});

test('counterpart selection does not expose PERSONAL expenses, balances, bank history or ledgers to Admin Prashant', () => {
  reset();
  const store = read();
  store.expenses['EX-PRIVATE-SECRET'] = { id: 'EX-PRIVATE-SECRET', nature: 'PERSONAL', date, amount: 456, status: 'paid', paidAmount: 456, approvedAt: date, vendor: 'SECRET PRIVATE VENDOR', particulars: 'SECRET PRIVATE PARTICULARS', createdBy: 'owner-user', payments: [{ id: 'PAY-001', date, amount: 456, account: accounts[0], proof: '/secret-personal-proof.jpg' }] };
  store.receipts.push({ id: 'REC-PRIVATE-SECRET', nature: 'PERSONAL', account: accounts[0], date, amount: 789, source: 'SECRET PRIVATE INCOME', receiptType: 'other_income' });
  write(store);
  const config = invoke('/api/expenses/config').body;
  assert.doesNotMatch(JSON.stringify(config), /SECRET PRIVATE|secret-personal-proof/);
  for (const account of accounts) {
    assert.equal(invoke('/api/expenses/account-ledger', { query: { nature: 'PERSONAL', account } }).status, 403);
    assert.equal(invoke('/api/expenses/bank-statements', { query: { nature: 'PERSONAL', account } }).status, 403);
  }
  const personalBalance = invoke('/api/expenses/balances', { query: { nature: 'PERSONAL' } });
  assert.equal(personalBalance.status, 403);
  for (const route of ['/api/expenses/list', '/api/expenses/balances']) {
    const result = invoke(route, { query: { nature: 'SANKI' } });
    assert.equal(result.status, 200);
    assert.doesNotMatch(JSON.stringify(result.body), /EX-PRIVATE-SECRET|SECRET PRIVATE|secret-personal-proof/);
  }
  assertPreserved();
});

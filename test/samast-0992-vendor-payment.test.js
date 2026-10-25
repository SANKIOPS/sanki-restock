'use strict';

// Real routes, isolated fictional records, and no live services.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const directory = fs.mkdtempSync(path.join(__dirname, '.tmp-samast-0992-payment-'));
process.env.DATA_PATH = path.join(directory, 'data.json');
process.env.PROCUREMENT_PATH = path.join(directory, 'procurement.json');
process.env.ORDERS_PATH = path.join(directory, 'orders.json');
process.env.SALES_PATH = path.join(directory, 'sales.json');
process.env.TEMP = directory;
process.env.TMP = directory;
process.env.TELEGRAM_BOT_TOKEN = '';
const file = path.join(directory, 'expenses.json');
const rentalSource = require.resolve('../modules/rental-source.json');
require.cache[rentalSource] = { id: rentalSource, filename: rentalSource, loaded: true,
  exports: { id: 'SYNTHETIC-0992-PAYMENT', asOf: '2098-01-01', rows: [] } };
fs.writeFileSync(file, '{}');
const { router, appBankMovements, summaryForPL } = require('../modules/expenses');
const account = 'ICICI Bank 0992', date = '2098-04-02', expenseId = 'EX-SYNTHETIC-SAMAST-0992';
const owner = { role: 'owner', username: 'synthetic-owner' };
const read = () => JSON.parse(fs.readFileSync(file, 'utf8'));
const write = value => fs.writeFileSync(file, JSON.stringify(value));
function invoke(method, route, { body = {}, query = {}, role = 'admin', username = 'prashant', permissions = {} } = {}) {
  return new Promise((resolve, reject) => {
    let status = 200, complete = false;
    const timer = setTimeout(() => reject(new Error('Route did not complete: ' + route)), 10000);
    const finish = value => { if (complete) return; complete = true; clearTimeout(timer); resolve({ status, body: value }); };
    const req = { method, path: route, url: route, originalUrl: route, baseUrl: '', body, query,
      ip: '203.0.113.71', headers: {}, get: () => '', user: { role, roles: [role], username, permissions } };
    const res = { statusCode: 200, headersSent: false, status(value) { status = value; this.statusCode = value; return this; },
      setHeader() {}, getHeader() {}, removeHeader() {}, json(value) { this.headersSent = true; finish(value); return this; },
      send(value) { this.headersSent = true; finish(value); return this; }, end: finish };
    try { router.handle(req, res, error => { if (error) { clearTimeout(timer); reject(error); } else finish(undefined); }); }
    catch (error) { clearTimeout(timer); reject(error); }
  });
}
const bill = (id = expenseId, amount = 1650, nature = 'SAMAST', vendor = 'Synthetic Building Supplier') => ({
  id, nature, vendor, date, amount, paidAmount: 0, status: 'approved', ledger: 'FLOWERS', type: 'variable', channel: 'Shared',
  particulars: 'Fictional building expense', claimant: 'synthetic-claimant', createdBy: 'synthetic-claimant',
  approvedBy: owner.username, approvedAt: date + 'T08:00:00.000Z', billPhoto: '/api/expenses/photo/synthetic-bill.jpg', payments: []
});
const payment = patch => ({ expenseIds: [expenseId], amount: 1650, account, date, paymentType: 'UPI',
  paymentProof: '/api/expenses/photo/synthetic-0992-proof.jpg', note: 'Verified fictional SAMAST payment from Sir personal account',
  requestId: 'synthetic-0992-payment-0001', ...patch });
const pay = (body = payment(), actor = {}) => invoke('POST', '/api/expenses/vendor-payments/batch', { ...actor, body });
let baseline;
test.before(async () => { assert.equal((await invoke('GET', '/api/expenses/config', owner)).status, 200); baseline = read(); });
test.after(() => fs.rmSync(directory, { recursive: true, force: true }));
function seed(extra = {}) {
  const store = structuredClone(baseline);
  Object.assign(store, { expenses: { [expenseId]: bill() }, transfers: [], receipts: [], adjustments: [],
    vendors: {}, vendorsByNature: { SAMAST: { 'synthetic building supplier': { name: 'Synthetic Building Supplier' } }, PERSONAL: {} },
    vendorDirectory: {}, vendorAdvances: [], vendorOpeningPayables: [], vendorPaymentRequests: {},
    bankTruthMovements: [], paytmSettlements: [], paytmPayoutPostings: [], reconciliationExpenses: [],
    expenseRefunds: [], salesRefunds: [], receivables: {}, loanRepayments: [], procurementAccounting: { paymentsByPo: {} },
    bankStatements: {}, bankReconciliationDrafts: {}, bankReconciliationApprovals: {}, bankReconciliationLinks: {}, bankDateOverrides: {},
    bankAmountAuthorityApplications: {}, bankAmountAuthorityProposals: {}, bankMovementAuthority: {},
    openingBalances: {}, openingBalancesByNature: { SAMAST: { [account]: 10000 }, PERSONAL: { [account]: 10000 } },
    physicalBankOpeningBalances: {}, openingBalanceDates: {}, openingBalanceDatesByNature: {},
    cashReconciliations: [], auditLog: [], auditSeq: 0 }, extra);
  for (const [name, value] of Object.entries({ 'salary.json': { employees: {}, advances: {}, salaryPayments: [] },
    'salary-samast.json': { employees: {}, advances: {}, salaryPayments: [] }, 'incentives.json': {},
    'procurement.json': { pos: {} }, 'orders.json': { orders: {} }, 'sales.json': {},
    'credit-cards.json': { cards: {}, payments: [], statements: {} } })) fs.writeFileSync(path.join(directory, name), JSON.stringify(value));
  write(store); return store;
}
function assertNoInventedFunding(store) {
  assert.deepEqual(store.transfers, []); assert.deepEqual(store.adjustments, []); assert.deepEqual(store.bankTruthMovements, []);
  for (const expense of Object.values(store.expenses)) {
    assert.equal(expense.nature, 'SAMAST');
    assert.ok(!expense.personalPaidAmount); assert.ok(!expense.reimbursementStatus);
    assert.equal((expense.reimbursementPayments || []).length, 0);
    for (const p of expense.payments || []) { assert.equal(p.account, account); assert.equal(p.personalFunds, false); }
  }
}
async function assertBankDebit(expected, expectedRows = 1) {
  for (const nature of ['SAMAST', 'PERSONAL']) {
    const rows = appBankMovements(read(), account, nature).filter(row => row.debit > 0);
    assert.equal(rows.length, expectedRows, nature + ' bank preview must not duplicate a vendor payment');
    assert.equal(rows.reduce((sum, row) => sum + row.debit, 0), expected, nature + ' bank preview gross debit');
    assert.ok(rows.every(row => row.entity === 'SAMAST'), 'funding account must not reclassify business expenses');
    const ledger = await invoke('GET', '/api/expenses/account-ledger', { ...owner, query: { nature, account, from: date, to: date } });
    assert.equal(ledger.status, 200, JSON.stringify(ledger.body));
    const debitRows = ledger.body.entries.filter(row => row.debit > 0);
    assert.equal(debitRows.length, expectedRows, nature + ' ledger debit count');
    assert.equal(debitRows.reduce((sum, row) => sum + row.debit, 0), expected, nature + ' ledger gross debit');
    const balances = await invoke('GET', '/api/expenses/balances', { ...owner, query: { nature, from: date, to: date } });
    assert.equal(balances.status, 200, JSON.stringify(balances.body));
    const source = balances.body.accounts.find(row => row.name === account);
    assert.ok(source, nature + ' source bank exists'); assert.equal(source.balance, 10000 - expected, nature + ' recorded bank balance');
    assert.equal(source.spent, expected, nature + ' cash totals include actual gross payment once');
  }
}

test('Admin and Owner get0992 for SAMAST vendor payments without generic or personal-history escalation', async () => {
  seed();
  for (const actor of [{}, { username: 'synthetic-other-admin' }, owner]) {
    const config = await invoke('GET', '/api/expenses/config', actor);
    assert.equal(config.status, 200);
    assert.equal(config.body.vendorPaymentAccountsByNature.SAMAST.filter(value => value === account).length, 1);
    assert.ok(!config.body.vendorPaymentAccountsByNature.SANKI.includes(account), 'not a SANKI vendor grant');
    if (actor.role !== 'owner') {
      assert.ok(!config.body.payingAccountsByNature.SAMAST.includes(account), 'not generic payment access');
      assert.ok(!config.body.accountsByNature.SAMAST.includes(account), 'not general account visibility');
      assert.ok(!config.body.ledgerAccountsByNature.SAMAST.includes(account));
      assert.ok(!config.body.reconciliationAccountsByNature.SAMAST.includes(account));
      assert.equal(config.body.canManagePersonalAccounting, false);
      for (const nature of ['SAMAST', 'PERSONAL']) {
        for (const route of ['/api/expenses/account-ledger', '/api/expenses/bank-statements']) {
          assert.equal((await invoke('GET', route, { ...actor, query: { nature, account } })).status, 403, route + ' stays private');
        }
      }
    }
  }
});

test('claimant and accounting-only roles cannot choose or post0992 payments', async () => {
  for (const role of ['claimant', 'accounting', 'samast_accounting']) {
    seed(); const actor = { role, username: 'synthetic-' + role };
    const config = await invoke('GET', '/api/expenses/config', actor);
    assert.ok(!config.body.vendorPaymentAccountsByNature.SAMAST.includes(account));
    const before = fs.readFileSync(file, 'utf8');
    assert.ok([400, 403].includes((await pay(payment(), actor)).status));
    assert.equal(fs.readFileSync(file, 'utf8'), before);
  }
});

test('SAMAST0992 normal and partial payments keep audit, proof, vendor balance and reporting entity', async () => {
  for (const amount of [600, 1650]) {
    seed(); const result = await pay(payment({ amount }));
    assert.equal(result.status, 200, JSON.stringify(result.body));
    const store = read(), expense = store.expenses[expenseId], p = expense.payments[0]; assertNoInventedFunding(store);
    assert.equal(expense.status, amount === 1650 ? 'paid' : 'partially_paid'); assert.equal(expense.paidAmount, amount);
    assert.equal(p.proof, payment().paymentProof); assert.equal(p.date, date); assert.equal(p.paidBy, 'prashant');
    assert.ok(store.auditLog.some(row => row.action === 'PAYMENT_ALLOCATED' && row.subjectId === expenseId && row.nature === 'SAMAST' && row.account === account && row.after.amount === amount));
    assert.equal(summaryForPL(date, date).Shared.variable, 0, 'SAMAST expense cannot leak into SANKI P&L');
    const spending = await invoke('GET', '/api/expenses/spending-dashboard', { ...owner, query: { nature: 'SAMAST', from: date, to: date } });
    assert.equal(spending.body.payments.filter(row => row.id === expenseId).reduce((sum, row) => sum + row.amount, 0), amount);
    const personal = await invoke('GET', '/api/expenses/spending-dashboard', { ...owner, query: { nature: 'PERSONAL', from: date, to: date } });
    assert.ok(!personal.body.payments.some(row => row.id === expenseId));
    const vendors = await invoke('GET', '/api/expenses/vendors', { ...owner, query: { nature: 'SAMAST', search: 'Synthetic Building Supplier', from: date, to: date } });
    assert.equal(vendors.body.vendors[0].ledgerClosingBalance, 1650 - amount);
    await assertBankDebit(amount);
  }
});

test('all Admin users can record0992 SAMAST vendor payment, not just one username', async () => {
  seed(); assert.equal((await pay(payment(), { username: 'synthetic-other-admin' })).status, 200);
  assert.equal(read().expenses[expenseId].payments[0].paidBy, 'synthetic-other-admin'); assertNoInventedFunding(read());
});

test('0992 overpayment creates one gross debit and applying vendor credit adds no bank movement', async () => {
  seed(); const paid = await pay(payment({ amount: 2000 }));
  assert.equal(paid.status, 200, JSON.stringify(paid.body)); assert.equal(paid.body.vendorAdvanceCreated, 350);
  let store = read(); assertNoInventedFunding(store);
  assert.equal(store.vendorAdvances.length, 1);
  const advance = store.vendorAdvances[0];
  assert.deepEqual([advance.nature, advance.account, advance.amount, advance.remainingAmount, advance.grossPaymentAmount], ['SAMAST', account, 350, 350, 2000]);
  await assertBankDebit(2000);
  const nextId = 'EX-SYNTHETIC-SAMAST-NEXT-0992'; store = read(); store.expenses[nextId] = bill(nextId, 350); write(store);
  const credit = await pay(payment({ expenseIds: [nextId], amount: 0, account: '', paymentProof: '', applyVendorCredit: true, requestId: 'synthetic-0992-payment-credit' }));
  assert.equal(credit.status, 200, JSON.stringify(credit.body)); assert.equal(credit.body.vendorCreditApplied, 350);
  store = read(); assertNoInventedFunding(store); assert.equal(store.vendorAdvances[0].remainingAmount, 0);
  assert.equal(store.expenses[nextId].status, 'paid'); assert.equal(store.expenses[nextId].payments.length, 0);
  assert.equal(store.expenses[nextId].vendorAdvanceApplications.length, 1);
  await assertBankDebit(2000);
});

test('0992 payment retries are idempotent and changed replays cannot post another debit', async () => {
  seed(); const body = payment({ amount: 2000 }); assert.equal((await pay(body)).status, 200);
  const before = fs.readFileSync(file, 'utf8');
  const repeat = await pay(body); assert.equal(repeat.status, 200); assert.equal(repeat.body.already, true);
  assert.equal(fs.readFileSync(file, 'utf8'), before);
  assert.equal((await pay({ ...body, amount: 2001 })).status, 409); assert.equal(fs.readFileSync(file, 'utf8'), before);
  await assertBankDebit(2000);
});

test('0992 permission does not bypass proof, approval, same vendor/entity, or SANKI source rules', async () => {
  for (const kind of ['proof', 'pending', 'mixed-vendor', 'mixed-entity', 'sanki']) {
    const store = seed(), body = payment();
    if (kind === 'proof') body.paymentProof = '';
    if (kind === 'pending') { store.expenses[expenseId].status = 'pending'; delete store.expenses[expenseId].approvedAt; }
    if (kind === 'sanki') store.expenses[expenseId].nature = 'SANKI';
    if (kind === 'mixed-vendor' || kind === 'mixed-entity') {
      const second = bill('EX-SYNTHETIC-0992-OTHER', 50, kind === 'mixed-entity' ? 'SANKI' : 'SAMAST', kind === 'mixed-vendor' ? 'Other Supplier' : 'Synthetic Building Supplier');
      store.expenses[second.id] = second; body.expenseIds.push(second.id);
    }
    write(store); const before = fs.readFileSync(file, 'utf8');
    assert.equal((await pay(body)).status, 400, kind); assert.equal(fs.readFileSync(file, 'utf8'), before, kind + ' cannot partially save');
  }
});

test('SAMAST0992 payments retain bank warning overrides without exposing private transfer details', async () => {
  const transfers = [
    { id: 'TR-SYNTHETIC-SAMAST-0992-WARNING', nature: 'SAMAST', fromNature: 'SAMAST', toNature: 'SAMAST', fromAccount: account, toAccount: 'Kirti Nagar Cash', date, amount: 120, classification: 'internal_transfer' },
    { id: 'TR-SYNTHETIC-PERSONAL-0992-WARNING', nature: 'PERSONAL', fromNature: 'PERSONAL', toNature: 'PERSONAL', fromAccount: account, toAccount: 'ICICI Bank 0993', date, amount: 130, classification: 'internal_transfer' }
  ];
  seed({ transfers }); const before = fs.readFileSync(file, 'utf8'), denied = await pay();
  assert.equal(denied.status, 409, JSON.stringify(denied.body)); assert.equal(denied.body.requiresOverride, true);
  assert.ok(denied.body.issues.length >= 2, 'both SAMAST and PERSONAL source-bank warnings remain effective');
  assert.ok(!JSON.stringify(denied.body).includes(transfers[1].id), 'private reference is not disclosed');
  assert.equal(fs.readFileSync(file, 'utf8'), before);
  const reason = 'Verified urgent SAMAST payment from original0992 proof despite open transfer warnings';
  const paid = await pay(payment({ reconciliationOverrideReason: reason })); assert.equal(paid.status, 200, JSON.stringify(paid.body));
  const store = read(), recorded = store.expenses[expenseId].payments[0];
  assert.equal(recorded.reconciliationOverrideReason, reason); assert.ok(recorded.reconciliationIssuesAtPayment.length >= 2);
  assert.ok(!JSON.stringify(paid.body).includes(transfers[1].id), 'successful response also preserves private history');
  assert.deepEqual(store.transfers, transfers); assert.deepEqual(store.adjustments, []); assert.equal(store.expenses[expenseId].nature, 'SAMAST');
});

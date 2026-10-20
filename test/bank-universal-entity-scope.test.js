'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'sanki-bank-entity-scope-'));
process.env.DATA_PATH = path.join(tempDir, 'data.json');
const file = path.join(tempDir, 'expenses.json');
fs.writeFileSync(file, '{}');
const { router, appBankMovements } = require('../modules/expenses');
const authority = require('../modules/bank-movement-authority');
const account = 'IndusInd Bank 7883', oldDate = '2098-06-01', bankDate = '2098-06-02';
const read = () => JSON.parse(fs.readFileSync(file, 'utf8'));
const write = value => fs.writeFileSync(file, JSON.stringify(value));
const clone = value => JSON.parse(JSON.stringify(value));

function invoke(route, query = {}) {
  return new Promise((resolve, reject) => {
    let status = 200, complete = false;
    const timer = setTimeout(() => reject(Error('Route did not complete: ' + route)), 10000);
    const finish = body => { if (complete) return; complete = true; clearTimeout(timer); resolve({ status, body }); };
    const req = { method: 'GET', path: route, url: route, originalUrl: route, baseUrl: '', body: {}, query,
      headers: {}, get: () => '', user: { role: 'owner', roles: ['owner'], username: 'owner-user', permissions: {} } };
    const res = { statusCode: 200, headersSent: false, status(value) { status = value; this.statusCode = value; return this; },
      setHeader() {}, getHeader() {}, removeHeader() {}, json(body) { this.headersSent = true; finish(body); return this; },
      send(body) { this.headersSent = true; finish(body); return this; }, end: finish };
    try { router.handle(req, res, error => { if (error) { clearTimeout(timer); reject(error); } else finish(undefined); }); }
    catch (error) { clearTimeout(timer); reject(error); }
  });
}

let baseline;
test.before(async () => {
  const configured = await invoke('/api/expenses/config'); assert.equal(configured.status, 200);
  baseline = read();
});
test.after(() => fs.rmSync(tempDir, { recursive: true, force: true }));

function seed() {
  const store = clone(baseline);
  Object.assign(store, { expenses: {}, adjustments: [], receipts: [], transfers: [], vendorAdvances: [], vendorOpeningPayables: [],
    salesRefunds: [], expenseRefunds: [], receivables: {}, loanRepayments: [], bankTruthMovements: [], reconciliationExpenses: [],
    paytmSettlements: [], paytmPayoutPostings: [], creditCardSettlementAllocations: [], procurementAccounting: { paymentsByPo: {} },
    bankReconciliationDrafts: {}, bankStatements: {}, bankDateOverrides: {}, bankReconciliationLinks: {}, bankMovementAuthority: {},
    openingBalances: {}, openingBalancesByNature: { SAMAST: { [account]: 10000 }, PERSONAL: { [account]: 10000 } },
    physicalBankOpeningBalances: {}, openingBalanceDates: {}, auditLog: [], auditSeq: 0 });
  for (const [name, contents] of Object.entries({ 'salary.json': { salaryPayments: [], advances: {}, employees: {} },
    'salary-samast.json': {}, 'incentives.json': {}, 'procurement.json': { pos: {} }, 'credit-cards.json': { cards: {}, payments: [], statements: {} } })) {
    fs.writeFileSync(path.join(tempDir, name), JSON.stringify(contents));
  }
  return store;
}
function correction(store, id, nature, credit = false, sourceFamily) {
  store.bankMovementAuthority[authority.key(account, nature, id)] = { movementId: id, account, nature: 'SAMAST', sourceNature: nature,
    original: { date: oldDate, debit: credit ? 0 : 1000, credit: credit ? 1000 : 0 },
    after: { date: bankDate, debit: credit ? 0 : 999.63, credit: credit ? 999.63 : 0 },
    planId: 'PLAN-' + id, effect: 'bank_cash_only', description: 'Original ' + nature + ' source', ...(sourceFamily ? { sourceFamily } : {}) };
}
async function views(nature) {
  const ledger = await invoke('/api/expenses/account-ledger', { nature, account, to: bankDate });
  assert.equal(ledger.status, 200, JSON.stringify(ledger.body));
  const balance = await invoke('/api/expenses/balance-sheet', { nature, asOf: bankDate });
  assert.equal(balance.status, 200, JSON.stringify(balance.body));
  const row = balance.body.assets.accounts.find(row => row.nature === nature && row.name === account);
  assert.ok(row, JSON.stringify(balance.body));
  return { ledger: ledger.body, balance: row.balance };
}

const families = {
  adjustment(store, id, nature) { store.adjustments.push({ id, nature, account, date: oldDate, amount: -1000, note: 'Original adjustment' }); },
  receipt(store, id, nature) { store.receipts.push({ id, nature, account, date: oldDate, amount: 1000, receiptType: 'other_income', source: 'Original source' }); },
  transfer(store, id, nature) { store.transfers.push({ id, nature, fromNature: nature, toNature: 'SANKI', fromAccount: account,
    toAccount: 'Tiana 0425', date: oldDate, amount: 1000, classification: 'internal_transfer' }); }
};
for (const [family, make] of Object.entries(families)) for (const sourceNature of ['PERSONAL', 'SAMAST']) {
  const otherNature = sourceNature === 'PERSONAL' ? 'SAMAST' : 'PERSONAL';
  test('shared-bank ' + sourceNature + ' ' + family + ' correction must not inject cash into ' + otherNature + ' entity views', async () => {
    const store = seed(), id = family.toUpperCase() + '-SCOPE-1', credit = family === 'receipt';
    make(store, id, sourceNature); correction(store, id, sourceNature, credit, family === 'transfer' ? 'transfer' : 'bank_movement'); write(store);
    const selected = await views(otherNature);
    assert.equal(selected.ledger.entries.filter(row => row.id === id).length, 0, 'other entity has no original source leg');
    assert.equal(selected.ledger.balance, 10000); assert.equal(selected.balance, 10000);
    const own = await views(sourceNature), expected = credit ? 10999.63 : 9000.37;
    assert.equal(own.ledger.entries.filter(row => row.id === id).length, 1);
    assert.equal(own.ledger.balance, expected); assert.equal(own.balance, expected);
    const physical = appBankMovements(read(), account, 'SAMAST').filter(row => row.id === id);
    assert.equal(physical.length, 1); assert.equal(physical[0][credit ? 'credit' : 'debit'], 999.63, 'one combined physical reconciliation retains actual bank amount');
    // A deleted source must retain its verified amount only in its original
    // entity, not become a new cash movement in the other entity's ledger.
    store.adjustments = []; store.receipts = []; store.transfers = []; write(store);
    const missingOther = await views(otherNature), missingOwn = await views(sourceNature);
    assert.equal(missingOther.ledger.entries.filter(row => row.id === id).length, 0);
    assert.equal(missingOther.ledger.balance, 10000); assert.equal(missingOther.balance, 10000);
    assert.equal(missingOwn.ledger.entries.filter(row => row.id === id).length, 1);
    assert.equal(missingOwn.ledger.balance, expected); assert.equal(missingOwn.balance, expected);
  });
}

test('cross-entity expense payment retains the existing physical-account ledger and balance behavior', async () => {
  const store = seed(), id = 'EX-CROSS-SCOPE/PAY-001';
  store.expenses['EX-CROSS-SCOPE'] = { id: 'EX-CROSS-SCOPE', nature: 'PERSONAL', date: oldDate, amount: 1000, paidAmount: 1000,
    status: 'paid', approvedAt: oldDate, vendor: 'Original expense', particulars: 'Paid from the shared physical bank',
    payments: [{ id: 'PAY-001', amount: 1000, date: oldDate, account }] };
  correction(store, id, 'PERSONAL', false, 'expense_payment'); write(store);
  for (const nature of ['SAMAST', 'PERSONAL']) {
    const selected = await views(nature);
    assert.equal(selected.ledger.entries.filter(row => row.id === id).length, 1);
    assert.equal(selected.ledger.balance, 9000.37); assert.equal(selected.balance, 9000.37);
  }
});

test('a deleted vendor gross payment with an expense-looking reference retains its actual source family', async () => {
  const store = seed(), id = 'EX-GROSS-SCOPE/PAY-001';
  correction(store, id, 'PERSONAL', false, 'vendor_advance'); write(store);
  const other = await views('SAMAST'), own = await views('PERSONAL');
  assert.equal(other.ledger.entries.filter(row => row.id === id).length, 0);
  assert.equal(other.ledger.balance, 10000); assert.equal(other.balance, 10000);
  assert.equal(own.ledger.entries.filter(row => row.id === id).length, 1);
  assert.equal(own.ledger.balance, 9000.37); assert.equal(own.balance, 9000.37);
});

test('orphaned cross-entity reimbursement family remains physical-account cash even with a nonstandard batch ID', async () => {
  const store = seed(), id = 'BATCH-REIMBURSEMENT-SCOPE';
  correction(store, id, 'PERSONAL', false, 'reimbursement'); write(store);
  const selected = await views('SAMAST');
  assert.equal(selected.ledger.entries.filter(row => row.id === id).length, 1);
  assert.equal(selected.ledger.balance, 9000.37); assert.equal(selected.balance, 9000.37);
});

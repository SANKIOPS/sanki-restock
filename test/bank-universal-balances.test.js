'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

// Exercise the real ledger/balance-sheet routes against an isolated local
// store. No live bank, provider, HTTP server or production file is involved.
const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'sanki-universal-balances-'));
process.env.DATA_PATH = path.join(tempDir, 'data.json');
const file = path.join(tempDir, 'expenses.json');
fs.writeFileSync(file, '{}');
const { router, appBankMovements, telegramAccountingSummary } = require('../modules/expenses');
const authority = require('../modules/bank-movement-authority');
const account = 'IndusInd Bank 8181';
const employeeAccount = 'Prashant Axis 3645';
const oldDate = '2098-06-01', overrideDate = '2098-06-02', bankDate = '2098-06-03';
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
      setHeader() {}, getHeader() {}, removeHeader() {},
      json(body) { this.headersSent = true; finish(body); return this; },
      send(body) { this.headersSent = true; finish(body); return this; }, end: finish };
    try { router.handle(req, res, error => { if (error) { clearTimeout(timer); reject(error); } else finish(undefined); }); }
    catch (error) { clearTimeout(timer); reject(error); }
  });
}

let baseline;
test.before(async () => {
  const response = await invoke('/api/expenses/config');
  assert.equal(response.status, 200);
  baseline = read();
});
test.after(() => fs.rmSync(tempDir, { recursive: true, force: true }));

function seed(selectedAccount = account) {
  const store = clone(baseline);
  Object.assign(store, { expenses: {}, adjustments: [], receipts: [], transfers: [], vendorAdvances: [],
    vendorOpeningPayables: [], salesRefunds: [], expenseRefunds: [], receivables: {}, loanRepayments: [],
    bankTruthMovements: [], reconciliationExpenses: [], paytmSettlements: [], paytmPayoutPostings: [],
    creditCardSettlementAllocations: [], procurementAccounting: { paymentsByPo: {} },
    bankReconciliationDrafts: {}, bankStatements: {}, bankDateOverrides: {}, bankReconciliationLinks: {},
    bankMovementAuthority: {}, bankAmountAuthorityApplications: {}, bankAmountAuthorityProposals: {},
    openingBalances: { [selectedAccount]: 10000 }, openingBalancesByNature: {}, physicalBankOpeningBalances: {},
    openingBalanceDates: { [selectedAccount]: '2098-01-01' }, auditLog: [], auditSeq: 0 });
  for (const [name, contents] of Object.entries({ 'salary.json': { salaryPayments: [], advances: {}, employees: {} },
    'salary-samast.json': {}, 'incentives.json': {}, 'procurement.json': { pos: {} },
    'credit-cards.json': { cards: {}, payments: [], statements: {} } })) {
    fs.writeFileSync(path.join(tempDir, name), JSON.stringify(contents));
  }
  write(store);
  return store;
}
function correction(store, id, originalAmount, bankAmount, { selectedAccount = account, credit = false, sourceIds } = {}) {
  const direction = credit ? 'credit' : 'debit';
  store.bankDateOverrides[id] = { originalDate: oldDate, bankDate: overrideDate,
    reconciliationDraft: 'FINAL-PRIOR-DATE', bankReference: 'PRIOR-BANK-REF' };
  store.bankMovementAuthority[authority.key(selectedAccount, 'SANKI', id)] = {
    movementId: id, account: selectedAccount, nature: 'SANKI', sourceNature: 'SANKI',
    original: { debit: 0, credit: 0, [direction]: originalAmount, date: oldDate },
    after: { debit: 0, credit: 0, [direction]: bankAmount, date: bankDate },
    sourceIds, planId: 'PLAN-' + id, effect: 'bank_cash_only', description: 'Verified bank movement' };
  write(store);
}
function expense(store, id, amount = 1000, extra = {}, selectedAccount = account) {
  store.expenses[id] = { id, nature: 'SANKI', date: oldDate, amount, requestedAmount: amount, paidAmount: amount,
    status: 'paid', approvedAt: oldDate, vendor: 'Original Vendor', particulars: 'Original bill',
    ledger: 'FOOD EXPENSE', payments: [{ id: 'PAY-001', amount, date: oldDate, account: selectedAccount,
      proof: '/original-payment.jpg', bankReference: '123456789012' }], ...extra };
  return store.expenses[id];
}
async function views(asOf, selectedAccount = account) {
  const ledger = await invoke('/api/expenses/account-ledger', { nature: 'SANKI', account: selectedAccount, to: asOf });
  assert.equal(ledger.status, 200, JSON.stringify(ledger.body));
  const sheet = await invoke('/api/expenses/balance-sheet', { nature: 'SANKI', asOf });
  assert.equal(sheet.status, 200, JSON.stringify(sheet.body));
  const bank = sheet.body.assets.accounts.find(row => row.name === selectedAccount && row.nature === 'SANKI');
  assert.ok(bank, JSON.stringify(sheet.body));
  return { ledger: ledger.body, sheet: bank.balance };
}
async function assertDates(id, bankAmount, { selectedAccount = account, credit = false } = {}) {
  for (const asOf of ['2098-05-31', oldDate, overrideDate]) {
    const result = await views(asOf, selectedAccount);
    assert.equal(result.ledger.balance, 10000, 'bank ledger before actual bank day: ' + asOf);
    assert.equal(result.sheet, 10000, 'recorded balance before actual bank day: ' + asOf);
    assert.equal(result.ledger.entries.filter(row => row.id === id).length, 0);
  }
  for (const asOf of [bankDate, '2098-06-04']) {
    const result = await views(asOf, selectedAccount), expected = Math.round((10000 + (credit ? bankAmount : -bankAmount)) * 100) / 100;
    assert.equal(result.ledger.balance, expected, 'bank ledger actual bank amount: ' + asOf);
    assert.equal(result.sheet, expected, 'recorded balance agrees with real bank ledger: ' + asOf);
    const movements = result.ledger.entries.filter(row => row.id === id);
    assert.equal(movements.length, 1, 'one canonical physical cash movement');
    assert.equal(movements[0][credit ? 'credit' : 'debit'], bankAmount);
    assert.equal(movements[0].date, bankDate);
  }
}

for (const bankAmount of [999.63, 1000.51]) test('real salary cash balance uses the raw salary baseline, not its prior bank-date override; amount ' + bankAmount, async () => {
  const store = seed(), id = 'SALB-BALANCE-001';
  const salary = { salaryPayments: [{ id, account, employeeName: 'Test Employee', amount: 1000, earned: 1000,
    date: oldDate, reference: 'SALARY-REF', proof: '/salary-proof.jpg' }], advances: {}, employees: {} };
  fs.writeFileSync(path.join(tempDir, 'salary.json'), JSON.stringify(salary));
  correction(store, id, 1000, bankAmount);
  await assertDates(id, bankAmount);
  assert.deepEqual(JSON.parse(fs.readFileSync(path.join(tempDir, 'salary.json'), 'utf8')), salary, 'salary entitlement and payment evidence stay unchanged');
});

test('real expense cash balance cancels its existing effective-bankdate baseline, without moving the original invoice or payment', async () => {
  const store = seed(), id = 'EX-BALANCE/PAY-001'; expense(store, 'EX-BALANCE');
  const before = clone(store.expenses);
  correction(store, id, 1000, 999.63);
  await assertDates(id, 999.63);
  assert.deepEqual(read().expenses, before);
});

test('vendor gross payment with an EX/PAY reference uses its raw advance date and suppresses covered expense allocations exactly once', async () => {
  const store = seed(), id = 'EX-GROSS/PAY-001';
  expense(store, 'EX-GROSS', 600, { payments: [{ id: 'PAY-001', amount: 600, date: oldDate,
    account, batchPaymentId: 'VPAY-BALANCE-1' }] });
  store.vendorAdvances = [{ id: 'VADV-BALANCE-1', nature: 'SANKI', vendor: 'Original Vendor', date: oldDate,
    account, amount: 400, remainingAmount: 400, grossPaymentAmount: 1000,
    batchPaymentId: 'VPAY-BALANCE-1', paymentReference: id, applications: [], proof: '/gross-proof.jpg' }];
  correction(store, id, 1000, 1000.51);
  await assertDates(id, 1000.51);
  const movements = appBankMovements(read(), account, 'SANKI');
  assert.equal(movements.filter(row => row.id === id).length, 1);
  assert.equal(movements.reduce((sum, row) => sum + row.debit, 0), 1000.51);
  assert.equal(read().expenses['EX-GROSS'].payments[0].amount, 600);
  assert.equal(read().vendorAdvances[0].grossPaymentAmount, 1000);
});

test('incoming receipt correction respects bank day and positive delta while keeping the original receipt', async () => {
  const store = seed(), id = 'REC-BALANCE-1';
  store.receipts = [{ id, nature: 'SANKI', account, date: oldDate, amount: 1000,
    receiptType: 'other_income', source: 'Original source', proof: '/receipt-proof.jpg' }];
  correction(store, id, 1000, 1250.25, { credit: true });
  await assertDates(id, 1250.25, { credit: true });
  assert.equal(read().receipts[0].amount, 1000);
});

test('later source amount drift cannot change finalized bank cash or duplicate the one corrected movement', async () => {
  const store = seed(), id = 'REC-DRIFT-1';
  store.receipts = [{ id, nature: 'SANKI', account, date: oldDate, amount: 1000, receiptType: 'other_income', source: 'Source' }];
  correction(store, id, 1000, 999.63, { credit: true });
  store.receipts[0].amount = 1200; store.receipts[0].date = '2098-06-04'; write(store);
  await assertDates(id, 999.63, { credit: true });
  const result = await views(bankDate);
  assert.equal(result.ledger.entries.find(row => row.id === id).bankMovementAuthority.sourceChanged, true);
  assert.ok(result.ledger.bankMovementAuthorityConflicts.some(item => item.movementId === id && item.sourceChanged));
  assert.equal(read().receipts[0].amount, 1200, 'read-only projection never rewrites the later source');
});

test('the opposite physical account retains its source evidence while the selected bank leg uses the bank amount', async () => {
  const store = seed(), id = 'TR-OPPOSITE-1', other = 'Tiana 0425';
  store.openingBalances[other] = 10000;
  store.transfers = [{ id, nature: 'SANKI', fromNature: 'SANKI', toNature: 'SANKI', fromAccount: account,
    toAccount: other, amount: 1000, date: oldDate, classification: 'internal_transfer', proof: '/transfer.jpg' }];
  correction(store, id, 1000, 999.63);
  await assertDates(id, 999.63);
  const source = read().transfers[0]; assert.equal(source.amount, 1000); assert.equal(source.date, oldDate);
  const opposite = await views(bankDate, other);
  assert.equal(opposite.ledger.balance, 11000); assert.equal(opposite.sheet, 11000);
  assert.equal(opposite.ledger.entries.filter(row => row.id === id).length, 1);
  assert.equal(opposite.ledger.entries.find(row => row.id === id).credit, 1000);
});

test('reimbursement batch account ledger and as-of balance collapse its known source allocations once', async () => {
  const store = seed(), id = 'RB-BALANCE-1', sourceIds = [];
  for (const [index, amount] of [[1, 600], [2, 400]]) {
    const expenseId = 'EX-REIMBALANCE-' + index; sourceIds.push(expenseId + '/REIM-001');
    expense(store, expenseId, amount, { claimant: 'pradeep', paidAlready: true, fundedBy: 'claimant',
      personalPaidAmount: amount, payments: [], reimbursementPayments: [{ id: 'REIM-001', amount,
        batchId: id, date: oldDate, account, proof: '/reimbursement.jpg' }] });
  }
  correction(store, id, 1000, 999.63, { sourceIds });
  await assertDates(id, 999.63);
  const result = await views(bankDate);
  assert.equal(result.ledger.entries.filter(row => sourceIds.includes(row.id)).length, 0);
  assert.equal(result.ledger.entries.find(row => row.id === id).items.length, 2);
});

test('company-funds Telegram cash matches the recorded physical bank after an expense with an existing bank-date override', async () => {
  const store = seed(employeeAccount), id = 'EX-EMPLOYEE/PAY-001';
  expense(store, 'EX-EMPLOYEE', 1000, {}, employeeAccount);
  correction(store, id, 1000, 999.63, { selectedAccount: employeeAccount });
  for (const asOf of [oldDate, overrideDate, bankDate]) {
    const result = await views(asOf, employeeAccount);
    const summary = telegramAccountingSummary(asOf, asOf, read());
    const funds = summary.employeeFunds.find(item => item.account === employeeAccount);
    assert.equal(funds.balance, result.sheet, 'employee cash must agree with recorded bank as of ' + asOf);
  }
});

test('company-funds Telegram cash cannot apply a correction for salary outside its existing company-funds definition', async () => {
  const store = seed(employeeAccount), id = 'SALB-EMPLOYEE-1';
  fs.writeFileSync(path.join(tempDir, 'salary.json'), JSON.stringify({ salaryPayments: [{ id, account: employeeAccount,
    employeeName: 'Test Employee', amount: 1000, date: oldDate, reference: 'SALARY-REF' }], advances: {}, employees: {} }));
  correction(store, id, 1000, 999.63, { selectedAccount: employeeAccount });
  for (const asOf of [oldDate, overrideDate, bankDate]) {
    const summary = telegramAccountingSummary(asOf, asOf, read());
    assert.equal(summary.employeeFunds.find(item => item.account === employeeAccount).balance, 10000,
      'salary is not an existing employee company-funds source, so its correction must not create a new delta as of ' + asOf);
  }
});

test('company-funds Telegram privacy filter excludes PERSONAL transfer corrections together with their source transfers', () => {
  const store = seed(employeeAccount), id = 'TR-PERSONAL-EMPLOYEE';
  store.transfers = [{ id, nature: 'SANKI', fromNature: 'PERSONAL', toNature: 'SANKI', fromAccount: 'ICICI Bank 0993',
    toAccount: employeeAccount, amount: 1000, date: oldDate, classification: 'internal_transfer' }];
  correction(store, id, 1000, 999.63, { selectedAccount: employeeAccount, credit: true });
  for (const asOf of [oldDate, overrideDate, bankDate]) {
    const summary = telegramAccountingSummary(asOf, asOf, read(), { includePersonal: false });
    assert.equal(summary.employeeFunds.find(item => item.account === employeeAccount).balance, 10000,
      'excluded PERSONAL transfer cannot leak back in as a cash correction as of ' + asOf);
  }
});

for (const [family, credit] of [['receipt', true], ['adjustment', false], ['transfer', false]]) test('company-funds Telegram uses raw-date canonical cash for an existing ' + family + ' source', () => {
  const store = seed(employeeAccount), id = 'FUNDS-' + family;
  if (family === 'receipt') store.receipts = [{ id, nature: 'SANKI', account: employeeAccount, date: oldDate, amount: 1000, receiptType: 'other_income', source: 'Source' }];
  if (family === 'adjustment') store.adjustments = [{ id, nature: 'SANKI', account: employeeAccount, date: oldDate, amount: -1000, note: 'Adjustment' }];
  if (family === 'transfer') store.transfers = [{ id, nature: 'SANKI', fromNature: 'SANKI', toNature: 'SANKI', fromAccount: employeeAccount, toAccount: account, amount: 1000, date: oldDate, classification: 'internal_transfer' }];
  correction(store, id, 1000, 999.63, { selectedAccount: employeeAccount, credit });
  for (const asOf of [oldDate, overrideDate, bankDate]) {
    const summary = telegramAccountingSummary(asOf, asOf, read());
    const expected = asOf < bankDate ? 10000 : credit ? 10999.63 : 9000.37;
    assert.equal(summary.employeeFunds.find(item => item.account === employeeAccount).balance, expected,
      family + ' cash uses one physical bank movement as of ' + asOf);
  }
});

test('company-funds Telegram aggregate reimbursement correction cancels raw allocations on their raw dates', () => {
  const store = seed(employeeAccount), id = 'RB-EMPLOYEE-1', sourceIds = [];
  for (const [index, amount] of [[1, 600], [2, 400]]) {
    const expenseId = 'EX-EMPLOYEEREIM-' + index; sourceIds.push(expenseId + '/REIM-001');
    expense(store, expenseId, amount, { claimant: 'pradeep', paidAlready: true, fundedBy: 'claimant', personalPaidAmount: amount,
      payments: [], reimbursementPayments: [{ id: 'REIM-001', amount, batchId: id, date: oldDate, account: employeeAccount }] }, employeeAccount);
  }
  correction(store, id, 1000, 999.63, { selectedAccount: employeeAccount, sourceIds });
  for (const asOf of [oldDate, overrideDate, bankDate]) {
    const summary = telegramAccountingSummary(asOf, asOf, read());
    assert.equal(summary.employeeFunds.find(item => item.account === employeeAccount).balance, asOf < bankDate ? 10000 : 9000.37);
  }
});

test('company-funds Telegram does not reinterpret a covered expense ID as its different gross vendor payment', () => {
  const store = seed(employeeAccount), id = 'EX-EMPLOYEEGROSS/PAY-001';
  expense(store, 'EX-EMPLOYEEGROSS', 600, { payments: [{ id: 'PAY-001', amount: 600, date: oldDate,
    account: employeeAccount, batchPaymentId: 'VPAY-EMPLOYEE-1' }] }, employeeAccount);
  store.vendorAdvances = [{ id: 'VADV-EMPLOYEE-1', nature: 'SANKI', vendor: 'Vendor', date: oldDate,
    account: employeeAccount, amount: 400, remainingAmount: 400, grossPaymentAmount: 1000,
    batchPaymentId: 'VPAY-EMPLOYEE-1', paymentReference: id, applications: [] }];
  correction(store, id, 1000, 999.63, { selectedAccount: employeeAccount });
  for (const asOf of [oldDate, overrideDate, bankDate]) {
    const summary = telegramAccountingSummary(asOf, asOf, read());
    assert.equal(summary.employeeFunds.find(item => item.account === employeeAccount).balance, 9400,
      'legacy funds definition counts the allocated expense but not the distinct gross vendor advance');
  }
});

test('company-adjusted Telegram funds keep excluded personal bank-truth corrections audit-only', () => {
  const store = seed(employeeAccount), id = 'BT-EMPLOYEE-PRIVATE';
  store.bankTruthMovements = [{ id, nature: 'SANKI', account: employeeAccount, date: oldDate,
    credit: 1000, debit: 0, accountingExcluded: true, reason: 'Excluded personal bank movement' }];
  correction(store, id, 1000, 999.63, { selectedAccount: employeeAccount, credit: true });
  for (const asOf of [oldDate, overrideDate, bankDate]) {
    const summary = telegramAccountingSummary(asOf, asOf, read());
    assert.equal(summary.employeeFunds.find(item => item.account === employeeAccount).balance, 10000,
      'personal bank-truth evidence cannot become company funds through an orphaned projection');
  }
});

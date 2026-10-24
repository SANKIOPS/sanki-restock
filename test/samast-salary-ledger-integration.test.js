'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const tempDir = fs.mkdtempSync(path.join(__dirname, '.tmp-samast-salary-ledger-'));
process.env.DATA_PATH = path.join(tempDir, 'data.json');
const file = path.join(tempDir, 'expenses.json');
fs.writeFileSync(file, '{}');
const expenses = require('../modules/expenses');
const salary = require('../modules/salary');
const owner = { username: 'owner', roles: ['owner'] };
const account = 'IndusInd Bank 7883';
const date = '2098-10-03';
const sourceId = 'SALB-00001-001';
const samastId = 'SAMAST/' + sourceId;
const read = () => JSON.parse(fs.readFileSync(file, 'utf8'));
const write = store => fs.writeFileSync(file, JSON.stringify(store));
const invoke = (method, route, input = {}) => expenses.telegramApi(method, route, owner, input);
const source = (entity, payments) => fs.writeFileSync(path.join(tempDir, entity === 'SAMAST' ? 'salary-samast.json' : 'salary.json'), JSON.stringify({ employees: {}, advances: {}, salaryPayments: payments }));
const payment = (extra = {}) => ({ id: sourceId, batchId: 'SALB-00001', empId: 'E001', employeeName: 'Samast Guard', ym: '2098-09', date, amount: 12500, account, proof: '/salary-proof.jpg', reference: 'SAMAST-SALARY-7883', active: true, ...extra });

invoke('GET', '/api/expenses/config');
const baseline = read();
function reset() {
  const store = JSON.parse(JSON.stringify(baseline));
  Object.assign(store, {
    expenses: {}, receipts: [], transfers: [], adjustments: [], vendorAdvances: [], receivables: {},
    bankStatements: {}, bankReconciliationDrafts: {}, bankDateOverrides: {}, bankReconciliationLinks: {},
    bankMovementAuthority: {}, bankTruthMovements: [], salesRefunds: [], expenseRefunds: [], reconciliationExpenses: [],
    openingBalances: {}, openingBalancesByNature: { SAMAST: { [account]: 50000 }, PERSONAL: { [account]: 50000 } },
    physicalBankOpeningBalances: {}
  });
  write(store);
  source('SANKI', []);
  source('SAMAST', []);
  return store;
}
const balance = (nature, selected = account) => {
  const result = invoke('GET', '/api/expenses/balances', { query: { nature } });
  assert.equal(result.status, 200, JSON.stringify(result));
  return result.accounts.find(row => row.name === selected);
};
const ledger = (nature, selected = account) => {
  const result = invoke('GET', '/api/expenses/account-ledger', { query: { nature, account: selected } });
  assert.equal(result.status, 200, JSON.stringify(result));
  return result;
};
test.after(() => fs.rmSync(tempDir, { recursive: true, force: true }));

test('recording a SAMAST salary through the salary API reaches 7883 ledger and reconciliation once', () => {
  reset();
  const create = salary.telegramApi('POST', '/api/salary/employees', owner, { entity: 'SAMAST', body: { name: 'SAMAST Salary Route Guard', salary: 12000, joiningDate: '2098-09-01' } });
  assert.equal(create.status, 200, JSON.stringify(create));
  const empId = create.employee.id;
  const payroll = salary.telegramApi('POST', '/api/salary/row/:ym', owner, { entity: 'SAMAST', params: { ym: '2098-09' }, body: { empId, paidDays: 30 } });
  assert.equal(payroll.status, 200, JSON.stringify(payroll));
  const before = balance('SAMAST').balance;
  const paid = salary.telegramApi('POST', '/api/salary/payments/batch', owner, { entity: 'SAMAST', body: {
    ym: '2098-09', date, account, proof: '/salary-api-proof.jpg', reference: 'SAMAST-SALARY-ROUTE',
    items: [{ empId, amount: 12000 }]
  } });
  assert.equal(paid.status, 200, JSON.stringify(paid));
  const id = 'SAMAST/' + paid.batchId + '-001';
  const saved = JSON.parse(fs.readFileSync(path.join(tempDir, 'salary-samast.json'), 'utf8')).salaryPayments[0];
  assert.deepEqual([saved.entity, saved.payingNature, saved.account], ['SAMAST', 'SAMAST', account]);
  assert.equal(balance('SAMAST').balance, before - 12000);
  const rows = ledger('SAMAST').entries.filter(row => row.id === id);
  assert.equal(rows.length, 1);
  assert.deepEqual([rows[0].entity, rows[0].debit, rows[0].proof], ['SAMAST', 12000, '/salary-api-proof.jpg']);
  const movements = expenses.appBankMovements(read(), account, 'SAMAST').filter(row => row.id === id);
  assert.equal(movements.length, 1);
  assert.deepEqual([movements[0].entity, movements[0].debit, movements[0].reference], ['SAMAST', 12000, 'SAMAST-SALARY-ROUTE']);
  const updated = salary.telegramApi('GET', '/api/salary/month/:ym', owner, { entity: 'SAMAST', params: { ym: '2098-09' } });
  assert.equal(updated.rows.find(row => row.id === empId).balance, 0);
});

test('SAMAST salary in 7883 debits the SAMAST bank once, not the PERSONAL or SANKI book', () => {
  reset();
  const samastBefore = balance('SAMAST').balance, personalBefore = balance('PERSONAL').balance;
  source('SAMAST', [payment({ entity: 'SAMAST', payingNature: 'SAMAST' })]);
  const after = balance('SAMAST');
  assert.equal(after.balance, samastBefore - 12500);
  assert.equal(after.spent, 12500);
  assert.equal(balance('PERSONAL').balance, personalBefore);
  const entries = ledger('SAMAST').entries.filter(row => row.id === samastId);
  assert.equal(entries.length, 1);
  assert.deepEqual([entries[0].entity, entries[0].debit, entries[0].kind], ['SAMAST', 12500, 'salary_payment']);
  assert.equal(ledger('PERSONAL').entries.some(row => row.id === samastId), false);
  assert.equal(ledger('SANKI', 'Axis Bank 3448').entries.some(row => row.id === samastId), false);
  const movements = expenses.appBankMovements(read(), account, 'SAMAST').filter(row => row.id === samastId);
  assert.equal(movements.length, 1);
  assert.deepEqual([movements[0].entity, movements[0].debit, movements[0].reference], ['SAMAST', 12500, 'SAMAST-SALARY-7883']);
  assert.deepEqual(expenses.appBankMovements(read(), account, 'PERSONAL').filter(row => row.id === samastId), movements, 'both shared-bank entry contexts use the same physical salary movement');
});

test('legacy SAMAST salary paid by a SANKI source keeps its expense entity and debits only the actual bank', () => {
  reset();
  const before = balance('SANKI', 'Axis Bank 3448').balance, samastBefore = balance('SAMAST').balance;
  source('SAMAST', [payment({ account: 'Axis Bank 3448' })]);
  assert.equal(balance('SANKI', 'Axis Bank 3448').balance, before - 12500);
  assert.equal(balance('SAMAST').balance, samastBefore);
  const rows = ledger('SANKI', 'Axis Bank 3448').entries.filter(row => row.id === samastId);
  assert.equal(rows.length, 1);
  assert.equal(rows[0].entity, 'SAMAST');
  assert.equal(expenses.appBankMovements(read(), 'Axis Bank 3448', 'SANKI').find(row => row.id === samastId).entity, 'SAMAST');
});

test('SAMAST salary reconciliation tags and date overrides stay separate from identical SANKI payment IDs', () => {
  const store = reset(), draftId = 'BRD-SAMAST-SALARY', bankDate = '2098-10-04';
  source('SAMAST', [payment()]);
  source('SANKI', [payment({ employeeName: 'SANKI Staff', account: 'Axis Bank 3448', amount: 4000, reference: 'SANKI-SALARY' })]);
  store.bankReconciliationDrafts[draftId] = {
    id: draftId, nature: 'SAMAST', account,
    transactions: [{ date: bankDate, description: 'SAMAST GUARD SALARY', reference: 'SAMAST-SALARY-7883', debit: 12500, credit: 0, balance: 37500 }],
    summary: { from: date, to: bankDate, openingBalance: 50000, closingBalance: 37500, totalDebits: 12500, totalCredits: 0, validated: true },
    resolutions: {}, createdBy: 'owner', createdAt: '2098-10-05T00:00:00.000Z', expiresAt: '2099-12-31T00:00:00.000Z'
  };
  write(store);
  const view = invoke('GET', '/api/expenses/bank-statements', { query: { nature: 'SAMAST', account, draftId } });
  assert.equal(view.status, 200, JSON.stringify(view));
  const row = view.draft.rows.find(item => item.id === 'bank-0');
  assert.equal(row.app.id, samastId);
  assert.equal(row.entityTag.nature, 'SAMAST');
  const result = invoke('POST', '/api/expenses/bank-statements/resolve', { body: { draftId, rowId: 'bank-0', action: 'accept_match', reason: 'Verified salary debit and actual payment date' } });
  assert.equal(result.status, 200, JSON.stringify(result));
  assert.equal(read().bankDateOverrides[samastId].bankDate, bankDate);
  assert.equal(read().bankDateOverrides[sourceId], undefined);
  assert.equal(ledger('SAMAST').entries.find(item => item.id === samastId).date, bankDate);
  assert.equal(ledger('SANKI', 'Axis Bank 3448').entries.find(item => item.id === sourceId).date, date);
  assert.equal(JSON.parse(fs.readFileSync(path.join(tempDir, 'salary-samast.json'), 'utf8')).salaryPayments[0].date, date, 'reconciliation date projection must not overwrite payroll evidence');
});

test('inactive, excluded and already-linked SAMAST salary payments add no extra bank debit', () => {
  reset();
  const before = balance('SAMAST').balance;
  source('SAMAST', [
    payment({ active: false }),
    payment({ id: 'SALB-00002-001', accountingExcluded: true }),
    payment({ id: 'SALB-00003-001', linkedLedgerEntryId: 'EX-EXISTING/PAY-001' })
  ]);
  assert.equal(balance('SAMAST').balance, before);
  assert.equal(ledger('SAMAST').entries.filter(row => row.kind === 'salary_payment').length, 0);
  assert.equal(expenses.appBankMovements(read(), account, 'SAMAST').filter(row => row.category === 'Salary payment').length, 0);
});

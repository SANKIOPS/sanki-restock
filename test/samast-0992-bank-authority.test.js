'use strict';

// Evaluate the actual balance/projection helpers with fictional data only.
// Other route regressions cover access and persistence. This isolates the
// cross-entity gross-payment authority edge without loading a real store.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const authority = require('../modules/bank-movement-authority');
const source = fs.readFileSync(path.join(__dirname, '../modules/expenses.js'), 'utf8');
const helperStart = source.indexOf('function bankAuthorityExpenseMovement(');
const helperEnd = source.indexOf('const TELEGRAM_EMPLOYEE_ACCOUNTS=', helperStart);
assert.ok(helperStart > 0 && helperEnd > helperStart, 'production balance helpers exist');
const scopeStart = source.indexOf('function isSamastOwnerVendorPaymentAccount(');
const scopeEnd = source.indexOf('// Only finalization writes this evidence.', scopeStart);
assert.ok(scopeStart > 0 && scopeEnd > scopeStart, 'production vendor scope helpers exist');
const helpers = source.slice(scopeStart, scopeEnd) + source.slice(helperStart, helperEnd);
const account = 'ICICI Bank 0992', id = 'EX-SYNTHETIC-0992-AUTHORITY/PAY-001';
const ledgerDate = '2098-04-02', bankDate = '2098-04-03';

function fixture(amount, date) {
  const row = { id, entity: 'SAMAST', date: ledgerDate, debit: 2000, credit: 0 };
  const store = {
    expenses: { 'EX-SYNTHETIC-0992-AUTHORITY': {
      id: 'EX-SYNTHETIC-0992-AUTHORITY', nature: 'SAMAST', amount: 1650, paidAmount: 1650,
      payments: [{ id: 'PAY-001', batchPaymentId: 'BPAY-SYNTHETIC', amount: 1650, account, date: ledgerDate }]
    } },
    vendorAdvances: [{ nature: 'SAMAST', account, date: ledgerDate, amount: 350,
      remainingAmount: 350, grossPaymentAmount: 2000, batchPaymentId: 'BPAY-SYNTHETIC', paymentReference: id }],
    transfers: [], receipts: [], adjustments: [], bankTruthMovements: [],
    openingBalancesByNature: { SAMAST: { [account]: 10000 }, PERSONAL: { [account]: 10000 } },
    bankMovementAuthority: { [authority.key(account, 'PERSONAL', id)]: {
      account, nature: 'PERSONAL', sourceNature: 'SAMAST', sourceFamily: 'vendor_payment',
      movementId: id, planId: 'SYNTHETIC-VERIFIED-0992',
      original: { date: ledgerDate, debit: 2000, credit: 0 }, after: { date, debit: amount, credit: 0 }
    } }
  };
  const empty = () => [];
  const context = {
    bankMovementAuthority: authority, appBankMovements: () => [row],
    canonicalAccountName: value => String(value || ''), normalizedNature: value => String(value || 'SANKI').toUpperCase(),
    isSharedBankReconciliationAccount: value => value === 'IndusInd Bank 7883',
    num: value => Number(value) || 0, roundMoney: value => Math.round(value * 100) / 100, round0: Math.round,
    cashEntryIsVisible: () => true, isBankLedgerName: () => true, usesCompanyAdjustedBankTruth: () => false,
    accountVendorChargeInScope: (record, nature) => record.nature === nature,
    expenseLoans: { repayments: empty }, personalFundingRows: empty,
    transferDebitAmount: record => record.amount, transferCreditAmount: record => record.amount,
    transferCreditsCompanyFunds: () => true, paymentIsPosted: () => true,
    accountMovementDate: (s, a, n, movement, date) => date,
    transferReimbursements: { postsMovement: () => true }, ledgerConnections: { claimantAccount: () => '' },
    salaryPaymentEntries: empty, salaryAdvanceEntries: empty
  };
  vm.createContext(context);
  vm.runInContext(helpers, context, { filename: 'expenses-bank-authority-helpers.js' });
  return { store, row, context };
}

for (const scenario of [
  { label: 'amount', amount: 2005.9, date: ledgerDate },
  { label: 'amount and date', amount: 1999.63, date: bankDate }
]) test('SAMAST 0992 gross payment retains verified ' + scenario.label + ' in PERSONAL and SAMAST balances', () => {
  const { store, row, context } = fixture(scenario.amount, scenario.date);
  const original = structuredClone(store);
  for (const nature of ['SAMAST', 'PERSONAL']) {
    const scoped = context.bankAuthorityScopedStore(store, nature, account, [row]);
    const projected = authority.project(scoped, account, nature, [row]);
    assert.equal(projected.length, 1, nature + ' has one gross debit, not allocation plus advance plus gross');
    assert.equal(projected[0].debit, scenario.amount);
    assert.equal(projected[0].date, scenario.date);
    assert.equal(projected[0].entity, 'SAMAST');
    for (const through of [ledgerDate, bankDate]) {
      const expected = through < scenario.date ? 10000 : Math.round((10000 - scenario.amount) * 100) / 100;
      const balance = context.recordedAccountBalance(store, nature, account, through);
      assert.equal(balance, expected, nature + ' verified balance through ' + through);
      const ledgerBalance = 10000 + projected.filter(record => record.date <= through)
        .reduce((sum, record) => sum + record.credit - record.debit, 0);
      assert.equal(Math.round(ledgerBalance * 100) / 100, balance, 'balance agrees with detailed bank ledger');
    }
  }
  assert.deepEqual(store, original, 'read projections leave invoice, payment, advance and authority evidence unchanged');
  assert.equal(store.expenses['EX-SYNTHETIC-0992-AUTHORITY'].nature, 'SAMAST');
  assert.equal(store.expenses['EX-SYNTHETIC-0992-AUTHORITY'].payments[0].amount, 1650);
  assert.equal(store.vendorAdvances[0].remainingAmount, 350);
  assert.deepEqual(store.transfers, []);
  assert.deepEqual(store.adjustments, []);
  assert.deepEqual(store.receipts, []);
});

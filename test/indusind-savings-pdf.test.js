'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { parseIndusIndSavingsStatement } = require('../modules/indusind-savings-pdf');

const columns = [60, 133, 256, 346, 437, 495];
const headings = ['Date', 'Particulars', 'Chq No/Ref No', 'Withdrawal', 'Deposit', 'Balance'];
const item = (str, x, y) => ({ str, transform: [1, 0, 0, 1, x, y] });

function page(rows, leading = []) {
  const items = headings.map((text, index) => item(text, columns[index], 400));
  leading.forEach((text, index) => items.push(item(text, columns[1], 380 - index * 16)));
  rows.forEach((row, index) => {
    const y = 340 - index * 90;
    items.push(item(row.date, columns[0], y));
    row.description.forEach((text, part) => items.push(item(text, columns[1], y - 1.7 - part * 16)));
    items.push(item(row.reference || '-', columns[2] + 30, y));
    if (row.withdrawal) items.push(item(row.withdrawal, columns[3] + 20, y));
    if (row.deposit) items.push(item(row.deposit, columns[4] + 10, y));
    items.push(item(row.balance, columns[5] - 12, y));
  });
  return { items };
}

function fixture(overrides = {}) {
  const newest = { date: '2026-10-18', description: ['UPI/317200712785/DR', '/SYNTH/ICIC/payee'], withdrawal: '10500', deposit: '', balance: '370197.82' };
  const middle = { date: '2026-10-18', description: overrides.middleDescription || ['UPI/616316397607/DR'], withdrawal: '10000', deposit: '', balance: overrides.middleBalance || '380697.82' };
  const oldest = { date: '2026-10-18', description: ['IMPS/P2A/6291184376', '18/UTIB/SYNTHETIC PAYEE'], withdrawal: '', deposit: '71118', balance: '390697.82' };
  const pages = [page([newest, middle]), page([oldest], ['/CHAN/FDRL/wrapped@bank'])];
  const text = `Account Statement
Savings Account Summary
Account No.Account TypeCurrencyLien AmountBalance
100062567883INDUS PRIVILEGE MAX
Transaction history for Savings Account
Statement Period: 22 August 2026 - 19 October 2026
DateParticularsChq No/Ref NoWithdrawalDepositBalance`;
  return { text, pages };
}

test('IndusInd Privilege savings parser reads positioned rows and validates the declared period', () => {
  const rows = parseIndusIndSavingsStatement(fixture());
  assert.equal(rows.length, 3);
  assert.deepEqual(rows.map(row => [row.reference, row.debit, row.credit, row.balance]), [
    ['317200712785', 10500, 0, 370197.82],
    ['616316397607', 10000, 0, 380697.82],
    ['629118437618', 0, 71118, 390697.82]
  ]);
  assert.match(rows[1].description, /UPI\/616316397607\/DR \/CHAN\/FDRL\/wrapped@bank/);
  assert.deepEqual(rows.statementSummary, {
    format: 'IndusInd Privilege savings PDF',
    accountLast4: '7883',
    from: '2026-08-22',
    to: '2026-10-19',
    openingBalance: 319579.82,
    openingBalanceInferred: true,
    closingBalance: 370197.82,
    totalDebits: 20500,
    totalCredits: 71118,
    validated: true
  });
});

test('IndusInd Privilege savings parser rejects any inconsistent intermediate balance', () => {
  assert.throws(() => parseIndusIndSavingsStatement(fixture({ middleBalance: '380697.83' })), /does not match its running balance/);
});

test('IndusInd Privilege savings parser rejects incomplete money instead of guessing split cells', () => {
  const pdf = fixture();
  const amount = pdf.pages[0].items.find(value => value.str === '10500');
  amount.str = '10500.12370197.82';
  assert.throws(() => parseIndusIndSavingsStatement(pdf), /not a complete currency value/);
});

test('IndusInd Privilege savings parser rejoins a wrapped N/R/PG reference segment', () => {
  for (const [middleDescription, expected] of [
    [['N/HDFCH0133425935', '1/HDFC0000240/AMTY GLOBAL'], 'HDFCH01334259351'],
    [['R/HDFCR5202610127', '0528004/HDFC0001309/ONLY SUPPLEMENTS'], 'HDFCR52026101270528004'],
    [['PG/YIDS273212', '6737/BILLDESK/LIFE'], 'YIDS2732126737']
  ]) {
    const rows = parseIndusIndSavingsStatement(fixture({ middleDescription }));
    assert.equal(rows[1].reference, expected);
    assert.match(rows[1].description, new RegExp('^(?:N|R|PG)/' + expected + '/'));
  }
});

test('other IndusInd layouts remain outside the savings profile', () => {
  assert.equal(parseIndusIndSavingsStatement({
    text: 'Account Statement\n159355468181\nINDUS PRIVILEGE MAX\nTransaction History\nDateParticularsChq No/Ref NoWithdrawalDepositBalance',
    pages: []
  }), null);
});

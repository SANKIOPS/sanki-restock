'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { parseIndieBusinessStatement } = require('../modules/indusind-indie-statement');
const { parseIndusIndSavingsStatement } = require('../modules/indusind-savings-pdf');

// Synthetic positions model PDF.js's cell concatenation; no customer statement
// details or attachment are committed as a fixture.
const columns = [40, 125, 370, 462, 531, 624, 716];
const headings = ['Date', 'Particulars', 'Ref No', 'Chq No', 'Withdrawal', 'Deposit', 'Balance'];
const item = (str, x, y) => ({ str, transform: [1, 0, 0, 1, x, y] });
const records = () => [
  ['2026/09/11', 'N/SYNTHETIC004/TESTBANK/ESCROW', 'S10000004', '-', '-', '750.67', '10251.46'],
  ['2026/09/01', 'N/SYNTHETIC003/TESTBANK/ESCROW', 'S10000003', '-', '-', '900.45', '9500.79'],
  ['2026/08/31', 'N/SYNTHETIC002/TESTBANK/ESCROW', 'S10000002', '-', '-', '500.23', '8600.34'],
  ['2026/08/25', 'N/SYNTHETIC001/TESTBANK/ESCROW', 'S10000001', '-', '-', '100.11', '8100.11']
];

function page(rows, header = true) {
  const items = header ? headings.map((text, index) => item(text, columns[index] + 8, 400)) : [];
  rows.forEach((cells, index) => {
    const y = 370 - index * 60;
    cells.forEach((text, column) => { if (text !== '') items.push(item(text, columns[column], y)); });
    items.push(item('WRAPPED NARRATION 123456789012345', columns[1], y - 18));
    items.push(item('SYNTHETIC SETTLEMENT ID', columns[1], y - 36));
  });
  return { items };
}

function fixture(rows = records(), pages = [page(rows)]) {
  const text = 'IndusInd Bank\nStatement Of Account\nAccount Number : 25XXXXXX0123\nPeriod : 22 August 2026 - 14 October 2026\n'
    + pages.map(value => {
      let y, result = '';
      value.items.forEach(cell => { result += y == null || y === cell.transform[5] ? '' : '\n'; result += cell.str; y = cell.transform[5]; });
      return result;
    }).join('\n') + '\nThis is a computer generated statement. Contact indie@indusind.com';
  return { text, pages };
}

test('INDIE positional parser preserves all credits, references and full printed period', () => {
  const pdf = fixture();
  assert.match(pdf.text, /2026\/09\/11N\/SYNTHETIC/);
  assert.match(pdf.text, /S10000004--750\.6710251\.46/);
  const rows = parseIndieBusinessStatement(pdf);
  assert.equal(rows.length, 4);
  assert.deepEqual(rows.map(row => row.reference), ['S10000004', 'S10000003', 'S10000002', 'S10000001']);
  assert.deepEqual(rows.map(row => row.credit), [750.67, 900.45, 500.23, 100.11]);
  assert.ok(rows.every(row => row.debit === 0));
  assert.match(rows[0].description, /WRAPPED NARRATION 123456789012345 SYNTHETIC SETTLEMENT ID/);
  assert.deepEqual(rows.statementSummary, { format: 'IndusInd INDIE business PDF', accountLast4: '0123', from: '2026-08-22', to: '2026-10-14', openingBalance: 8000, openingBalanceInferred: true, closingBalance: 10251.46, totalDebits: 0, totalCredits: 2251.46, validated: true });
});

test('INDIE parses withdrawals and explicit zero sides without changing same-date order', () => {
  const rows = parseIndieBusinessStatement(fixture([
    ['2026/09/11', 'Debit after deposit', 'S10000003', '12345', '50.25', '0.00', '1099.75'],
    ['2026/09/11', 'Earlier deposit', 'S10000002', '-', '0.00', '150.00', '1150.00']
  ]));
  assert.deepEqual(rows.map(row => [row.reference, row.debit, row.credit]), [['S10000003', 50.25, 0], ['S10000002', 0, 150]]);
  assert.equal(rows.statementSummary.openingBalance, 1000);
  assert.equal(rows.statementSummary.closingBalance, 1099.75);
});

test('INDIE reads headerless continuation pages and ignores a terms-only page', () => {
  const data = records();
  const terms = { items: [item('This is a computer generated statement and does not require signature.', 40, 400)] };
  const rows = parseIndieBusinessStatement(fixture(data, [page(data.slice(0, 2)), page(data.slice(2), false), terms]));
  assert.equal(rows.length, 4);
  assert.equal(rows.statementSummary.openingBalance, 8000);
});

test('INDIE repeated table headers across pages do not drop transactions', () => {
  const data = records();
  const rows = parseIndieBusinessStatement(fixture(data, [page(data.slice(0, 2)), page(data.slice(2))]));
  assert.equal(rows.length, 4);
  assert.equal(rows.statementSummary.totalCredits, 2251.46);
});

test('INDIE malformed dates on a headerless continuation page cannot be silently skipped', () => {
  const data = records(), pages = [page(data.slice(0, 2)), page(data.slice(2), false)];
  pages[1].items.find(cell => cell.str === '2026/08/31').str = '2026/08/3';
  assert.throws(() => parseIndieBusinessStatement(fixture(data, pages)), /without a readable transaction date/);
});

test('INDIE headerless monetary rows with all dates missing cannot be treated as a terms page', () => {
  const data = records(), pages = [page(data.slice(0, 2)), page(data.slice(2), false)];
  pages[1].items = pages[1].items.filter(cell => !/^\d{4}\//.test(cell.str));
  assert.throws(() => parseIndieBusinessStatement(fixture(data, pages)), /without a readable transaction date/);
});

test('INDIE rejects an inconsistent intermediate running balance even when sums agree', () => {
  const data = records(); data[1][6] = '9500.80';
  assert.throws(() => parseIndieBusinessStatement(fixture(data)), /does not match its running balance/);
});

test('INDIE rejects an omitted intermediate row instead of importing a partial statement', () => {
  const data = records(); data.splice(1, 1);
  assert.throws(() => parseIndieBusinessStatement(fixture(data)), /does not match its running balance/);
});

test('INDIE rejects malformed or incomplete monetary cells', () => {
  for (const value of ['750.6', '750.6710251.46', '7e2', 'abc', '1,2.34']) {
    const data = records(); data[0][5] = value;
    assert.throws(() => parseIndieBusinessStatement(fixture(data)), /not a complete currency value/);
  }
  const data = records(); data[0][6] = '';
  assert.throws(() => parseIndieBusinessStatement(fixture(data)), /Balance.*not a complete currency value/);
});

test('INDIE rejects both sides positive, both sides zero, and negative sides', () => {
  for (const sides of [['10.00', '750.67'], ['0.00', '0.00'], ['-10.00', '0.00']]) {
    const data = records(); [data[0][4], data[0][5]] = sides;
    assert.throws(() => parseIndieBusinessStatement(fixture(data)), /exactly one positive withdrawal or deposit/);
  }
});

test('INDIE rejects calendar errors, out-of-period dates and ascending ordering', () => {
  for (const date of ['2026/09/31', '2026/08/21', '2026/10/15']) {
    const data = records(); data[0][0] = date;
    assert.throws(() => parseIndieBusinessStatement(fixture(data)), /invalid or outside the statement period/);
  }
  assert.throws(() => parseIndieBusinessStatement(fixture(records().reverse())), /newest-first order/);
});

test('INDIE recognized malformed table never falls back to legacy numeric guessing', () => {
  const pdf = fixture();
  pdf.pages[0].items = pdf.pages[0].items.filter(cell => cell.str !== 'Balance');
  assert.throws(() => parseIndieBusinessStatement(pdf), /seven statement columns/);
  const dated = fixture();
  dated.pages[0].items.find(cell => cell.str === '2026/09/01').str = '2026/09/1';
  assert.throws(() => parseIndieBusinessStatement(dated), /date is missing or malformed/);
});

test('INDIE requires complete positions, consistent account and valid statement period', () => {
  const missing = fixture(); missing.pages = [];
  assert.throws(() => parseIndieBusinessStatement(missing), /original PDF table positions/);
  const wrongAccount = fixture(); wrongAccount.text += '\nAccount Number : 25XXXXXX0999';
  assert.throws(() => parseIndieBusinessStatement(wrongAccount), /account number is missing or inconsistent/);
  const wrongPeriod = fixture(); wrongPeriod.text = wrongPeriod.text.replace('22 August 2026 - 14 October 2026', '22 August 2026 - 31 September 2026');
  assert.throws(() => parseIndieBusinessStatement(wrongPeriod), /statement period is missing or inconsistent/);
});

test('legacy IndusInd mobile and other bank layouts remain outside the INDIE profile', () => {
  assert.equal(parseIndieBusinessStatement({ text: 'Account Statement\n000000000123\nINDUS PRIVILEGE\nDateParticularsChq No/Ref NoWithdrawalDepositBalance', pages: [] }), null);
  assert.equal(parseIndieBusinessStatement({ text: fixture().text.replace('IndusInd Bank', 'Other Bank'), pages: [] }), null);
});

function uploadHarness(pages, legacyRows = [], documentError) {
  const source = fs.readFileSync(path.join(__dirname, '../modules/expenses.js'), 'utf8');
  const code = source.slice(source.indexOf('async function readPdfText('), source.indexOf('function bankRowKey('));
  const state = { fallbackCalls: 0, destroyed: false, legacyText: '' };
  const context = {
    fs: { readFileSync: () => new Uint8Array([1, 2, 3]) }, path, parseIndieBusinessStatement, parseIndusIndSavingsStatement,
    parseBankStatementText: text => { state.fallbackCalls++; state.legacyText = text; return legacyRows; },
    pdfjs: { getDocument: async () => {
      if (documentError) throw documentError;
      return { numPages: pages.length, getPage: async pageNo => ({ getTextContent: async () => pages[pageNo - 1] }), destroy: () => { state.destroyed = true; } };
    } }
  };
  vm.createContext(context); vm.runInContext(code, context);
  return { state, parse: password => context.parseBankStatementUpload('synthetic.pdf', 'synthetic.pdf', password) };
}

function uploadPages() {
  const pdf = fixture();
  pdf.pages[0].items.unshift(item('IndusInd Bank', 40, 560), item('Statement Of Account', 40, 540), item('Account Number : 25XXXXXX0123', 40, 520), item('Period : 22 August 2026 - 14 October 2026', 40, 500));
  return pdf.pages;
}

test('PDF upload uses positions for INDIE and never invokes generic text parsing', async () => {
  const harness = uploadHarness(uploadPages());
  const rows = await harness.parse('');
  assert.equal(rows.length, 4);
  assert.equal(rows.statementSummary.totalCredits, 2251.46);
  assert.equal(harness.state.fallbackCalls, 0);
  assert.equal(harness.state.destroyed, true);
});

test('recognized invalid INDIE upload fails closed before legacy parsing', async () => {
  const pages = uploadPages(); pages[0].items.find(cell => cell.str === '9500.79').str = '9500.80';
  const harness = uploadHarness(pages, [{ credit: 999 }]);
  await assert.rejects(harness.parse(''), /does not match its running balance/);
  assert.equal(harness.state.fallbackCalls, 0);
  assert.equal(harness.state.destroyed, true);
});

test('non-INDIE PDF upload preserves the exact legacy text fallback', async () => {
  const legacy = [{ date: '2026-10-01', debit: 25, credit: 0, balance: 75 }];
  const harness = uploadHarness([{ items: [item('Other Bank', 40, 500), item('Legacy table', 40, 480)] }], legacy);
  assert.equal(await harness.parse(''), legacy);
  assert.equal(harness.state.legacyText, '\n\nOther Bank\nLegacy table');
  assert.equal(harness.state.fallbackCalls, 1);
  assert.equal(harness.state.destroyed, true);
});

test('PDF upload retains existing protected-document password errors', async () => {
  const harness = uploadHarness([], [], { name: 'PasswordException' });
  await assert.rejects(harness.parse(''), /password protected/);
  await assert.rejects(harness.parse('wrong'), /Incorrect statement password/);
  assert.equal(harness.state.fallbackCalls, 0);
});

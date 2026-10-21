'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { parseIndusIndMobileStatement } = require('../modules/indusind-mobile-statement');
const { parseIndieBusinessStatement } = require('../modules/indusind-indie-statement');
const { parseIndusIndSavingsStatement } = require('../modules/indusind-savings-pdf');

const columns = [20, 180, 450, 670, 800, 930];
const headings = ['Date', 'Particulars', 'Chq No/Ref No', 'Withdrawal', 'Deposit', 'Balance'];
const item = (str, x, y) => ({ str, width: str.length * 6, transform: [1, 0, 0, 1, x, y] });
const target = () => ['19 Oct 2026', 'UPI/123456789012/DR/SYN', 'S1234567', '490.00', '0.00', '1,000.00'];
function page(records, { header = true, leading = [] } = {}) {
  const items = header ? headings.map((text, index) => item(text, columns[index], 700)) : [];
  leading.forEach((text, index) => items.push(item(text, columns[1], 670 - index * 18)));
  records.forEach((record, index) => {
    const y = 620 - index * 100;
    record.forEach((text, column) => { if (text !== '') items.push(item(text, column >= 3 ? columns[column] + (headings[column].length - text.length) * 6 : columns[column], y)); });
    if (record[2] === 'S1234567') {
      items.push(item('TH/TEST/12345.bqr@ban', columns[1], y - 18), item('k', columns[1], y - 36));
    }
  });
  return { items };
}
function flattened(pages) {
  return pages.map(page => {
    let y, text = '';
    page.items.forEach(cell => { text += y == null || y === cell.transform[5] ? '' : '\n'; text += cell.str; y = cell.transform[5]; });
    return text;
  }).join('\n');
}
function fixture(records = [target()], options = {}) {
  const pages = options.pages || [page(records)];
  const header = 'Account Statement\n111111118181\nINDUS PRIVILEGE MAX\nTransaction History\nStatement Period: 18 Oct 2026 - 19 Oct 2026\n';
  return { text: header + flattened(pages), pages };
}

test('mobile positional parser preserves seven-digit reference and full 490 on the oldest row', () => {
  const pdf = fixture();
  assert.match(pdf.text, /S1234567490\.000\.001,000\.00/);
  const rows = parseIndusIndMobileStatement(pdf);
  assert.equal(rows.length, 1);
  assert.deepEqual([rows[0].reference, rows[0].bankReference, rows[0].debit, rows[0].credit, rows[0].balance], ['123456789012', 'S1234567', 490, 0, 1000]);
  assert.match(rows[0].description, /SYN TH\/TEST\/12345\.bqr@ban k/);
  assert.equal(rows.statementSummary.openingBalance, 1490);
  assert.equal(rows.statementSummary.totalDebits, 490);
  assert.equal(rows.statementSummary.parserVersion, 'positioned-cells-v1');
});

test('mobile parser does not assume a fixed bank-reference length', () => {
  for (const reference of ['S12345', 'S123456', 'S1234567', 'S12345678', 'M123456789', 'M123456789012']) {
    const record = target(); record[2] = reference;
    const rows = parseIndusIndMobileStatement(fixture([record]));
    assert.equal(rows[0].bankReference, reference);
    assert.equal(rows[0].debit, 490);
    assert.equal(rows.statementSummary.openingBalance, 1490);
  }
});

test('mobile parser keeps same-date ordering, credits, zeros, blanks and Indian currency commas', () => {
  const records = [
    ['19 Oct 2026', 'UPI/111111111111/DR/PAYEE', 'S1234567', '1,23,456.78', '0.00', '3,000.00'],
    ['19 Oct 2026', 'UPI/222222222222/CR/RECEIPT', 'S12345', '', '1,25,456.78', '1,26,456.78']
  ];
  const rows = parseIndusIndMobileStatement(fixture(records));
  assert.deepEqual(rows.map(row => [row.debit, row.credit, row.balance]), [[123456.78, 0, 3000], [0, 125456.78, 126456.78]]);
  assert.equal(rows.statementSummary.openingBalance, 1000);
});

test('mobile parser validates intermediate balances and does not discard omitted rows', () => {
  const records = [['19 Oct 2026', 'UPI/111111111111/DR/PAYEE', 'S12345678', '50.00', '0.00', '950.00'], target()];
  assert.equal(parseIndusIndMobileStatement(fixture(records)).statementSummary.openingBalance, 1490);
  records[0][5] = '950.01';
  assert.throws(() => parseIndusIndMobileStatement(fixture(records)), /does not match its running balance/);
});

test('mobile parser handles repeated headers, headerless continuation and narration across pages', () => {
  const records = [['19 Oct 2026', 'UPI/111111111111/DR/PAYEE', 'S12345678', '50.00', '0.00', '950.00'], target()];
  for (const header of [true, false]) {
    const pages = [page([records[0]]), page([records[1]], { header, leading: ['WRAPPED PREVIOUS NARRATION'] }), { items: [item('This is a computer generated statement.', 20, 700)] }];
    const rows = parseIndusIndMobileStatement(fixture(records, { pages }));
    assert.equal(rows.length, 2);
    assert.match(rows[0].description, /WRAPPED PREVIOUS NARRATION/);
    assert.equal(rows[1].debit, 490);
  }
});

test('mobile parser refuses headerless continuation monetary rows without dates', () => {
  const newer = ['19 Oct 2026', 'UPI/111111111111/DR/PAYEE', 'S12345678', '50.00', '0.00', '950.00'];
  const pages = [page([newer]), page([target()], { header: false })];
  pages[1].items = pages[1].items.filter(cell => cell.str !== '19 Oct 2026');
  assert.throws(() => parseIndusIndMobileStatement(fixture([], { pages })), /more than one numeric line|separated numeric cells/);
});

test('mobile parser rejoins digits only within one positioned money cell', () => {
  const pdf = fixture();
  const amount = pdf.pages[0].items.find(cell => cell.str === '490.00');
  amount.str = '4'; amount.width = 6;
  pdf.pages[0].items.push(item('90.00', amount.transform[4] + 8, amount.transform[5]));
  assert.equal(parseIndusIndMobileStatement(pdf)[0].debit, 490);
  pdf.pages[0].items.at(-1).transform[5] -= 18;
  assert.throws(() => parseIndusIndMobileStatement(pdf), /more than one numeric line/);
});

test('mobile parser cannot steal a separated amount digit into the reference column', () => {
  const pdf = fixture();
  const amount = pdf.pages[0].items.find(cell => cell.str === '490.00');
  amount.str = '90.00'; amount.width = 30; amount.transform[4] += 6;
  // A split leading digit close to the column boundary must fail visibly,
  // never silently become an extra digit of S1234567.
  pdf.pages[0].items.push(item('4', 550, amount.transform[5]));
  assert.throws(() => parseIndusIndMobileStatement(pdf), /separated numeric cells/);
});

test('mobile parser uses right edges for wide monetary cells rather than header midpoints', () => {
  const actualColumns = [59.63, 133.24, 255.93, 345.57, 436.69, 495.1];
  const headingWidths = [19.71, 44.064, 61.587, 49.329, 31.824, 32.778];
  for (const amount of ['5.90', '490.00', '12,34,56,789.00', '1,00,00,00,00,000.00']) {
    const cells = target(); cells[3] = amount;
    const items = headings.map((str, index) => ({ ...item(str, actualColumns[index], 700), width: headingWidths[index] }));
    cells.forEach((str, index) => {
      const width = str.length * 5.4;
      items.push({ ...item(str, index >= 3 ? actualColumns[index] + headingWidths[index] - width : actualColumns[index], 620), width });
    });
    const rows = parseIndusIndMobileStatement(fixture(cells, { pages: [{ items }] }));
    assert.equal(rows[0].debit, Number(amount.replace(/,/g, '')));
    assert.equal(rows[0].bankReference, 'S1234567');
  }
});

test('mobile parser fails closed for concatenated money cells, invalid numbers and missing balances', () => {
  for (const value of ['490.001000.00', '490.0', '4e2', '4,9.00', '-490.00']) {
    const record = target(); record[3] = value;
    assert.throws(() => parseIndusIndMobileStatement(fixture([record])), /complete currency|positive withdrawal/);
  }
  const record = target(); record[5] = '';
  assert.throws(() => parseIndusIndMobileStatement(fixture([record])), /Balance.*complete currency/);
});

test('mobile parser rejects unreadable column boundaries rather than text fallback', () => {
  const pdf = fixture();
  pdf.pages[0].items = pdf.pages[0].items.filter(cell => cell.str !== 'Withdrawal');
  assert.throws(() => parseIndusIndMobileStatement(pdf), /six statement columns/);
  assert.throws(() => parseIndusIndMobileStatement({ ...fixture(), pages: [] }), /original PDF table positions/);
});

test('mobile parser rejects malformed dates, out-of-period rows, ascending dates and contradictory directions', () => {
  for (const date of ['31 Sep 2026', '20 Oct 2026', '19 Octo 2026']) {
    const record = target(); record[0] = date;
    assert.throws(() => parseIndusIndMobileStatement(fixture([record])), /invalid or outside/);
  }
  const record = target(); record[1] = record[1].replace('/DR/', '/CR/');
  assert.throws(() => parseIndusIndMobileStatement(fixture([record])), /printed payment direction/);
  const older = target(); older[0] = '18 Oct 2026';
  assert.throws(() => parseIndusIndMobileStatement(fixture([older, target()])), /newest-first/);
});

test('mobile parser validates independently printed opening and closing balances', () => {
  const pdf = fixture(); pdf.text += '\nOpening Balance: 1490.00\nClosing Balance: 1000.00';
  assert.equal(parseIndusIndMobileStatement(pdf).statementSummary.openingBalanceInferred, false);
  assert.throws(() => parseIndusIndMobileStatement({ ...pdf, text: pdf.text.replace('1490.00', '1090.00') }), /printed opening balance/);
  assert.throws(() => parseIndusIndMobileStatement({ ...pdf, text: pdf.text.replace('Closing Balance: 1000.00', 'Closing Balance: 1001.00') }), /printed closing balance/);
});

test('mobile parser leaves other statement families to their established positional profiles', () => {
  assert.equal(parseIndusIndMobileStatement({ text: 'Other Bank', pages: [] }), null);
  const savings = fixture(); savings.text += '\nSavings Account Summary';
  assert.equal(parseIndusIndMobileStatement(savings), null);
});

function legacyHarness() {
  const source = fs.readFileSync(path.join(__dirname, '../modules/expenses.js'), 'utf8');
  const context = { statementNum: value => Number(String(value || '').replace(/,/g, '')) || 0, roundMoney: value => Math.round(value * 100) / 100 };
  vm.createContext(context);
  vm.runInContext(source.slice(source.indexOf('function selectIndusIndMobileCandidates('), source.indexOf('function parseIndusIndScreenshotText(')), context);
  return context;
}
function legacyText(body, opening = '') {
  return 'Account Statement\n111111118181\nINDUS PRIVILEGE\nTransaction History\nStatement Period: 18 Oct 2026 - 19 Oct 2026\nDateParticularsChq No/Ref NoWithdrawalDepositBalance\n' + (opening ? 'Opening Balance: ' + opening + '\n' : '') + body;
}

test('legacy flattened text rejects the exact 490-to-90 ambiguity on its oldest row', () => {
  const { parseIndusIndMobileStatementText } = legacyHarness();
  const body = '19 Oct 2026UPI/123456789012/DR/SYN TH/TEST/12345.bqr@bank\nS1234567490.000.001000.00';
  assert.throws(() => parseIndusIndMobileStatementText(legacyText(body)), /ambiguous/);
  const newer = '19 Oct 2026UPI/111111111111/DR/PAYEE\nS1234567850.000.00950.00\n';
  assert.throws(() => parseIndusIndMobileStatementText(legacyText(newer + body)), /ambiguous/);
  const rows = parseIndusIndMobileStatementText(legacyText(body, '1490.00'));
  assert.equal(rows[0].debit, 490);
  assert.equal(rows[0].bankReference, 'S1234567');
});

test('legacy text preserves uniquely determined amounts and five-digit references', () => {
  const { parseIndusIndMobileStatementText } = legacyHarness();
  const body = '19 Oct 2026UPI/111111111111/DR/PAYEE\nS123455.000.00995.00\n18 Oct 2026UPI/222222222222/CR/PAYER\nS123456780.00500.001000.00';
  const rows = parseIndusIndMobileStatementText(legacyText(body));
  assert.equal(rows[0].bankReference, 'S12345');
  assert.equal(rows[0].debit, 5);
  assert.equal(rows[1].credit, 500);
  assert.equal(rows.statementSummary.openingBalance, 500);
});

test('legacy text rejects missing amounts instead of validating a partial import', () => {
  const { parseIndusIndMobileStatementText } = legacyHarness();
  const body = '19 Oct 2026UPI/111111111111/DR/PAYEE\nS12345bad amount\n18 Oct 2026UPI/222222222222/CR/PAYER\nS123456780.00500.001000.00';
  assert.throws(() => parseIndusIndMobileStatementText(legacyText(body)), /not every dated row/);
});

function uploadHarness(pdf) {
  const pages = pdf.pages;
  const first = pages[0];
  first.items.unshift(item('Account Statement', 20, 820), item('111111118181', 20, 800), item('INDUS PRIVILEGE MAX', 20, 780), item('Transaction History', 20, 760), item('Statement Period: 18 Oct 2026 - 19 Oct 2026', 20, 740));
  const source = fs.readFileSync(path.join(__dirname, '../modules/expenses.js'), 'utf8');
  const state = { fallbackCalls: 0, destroyed: false };
  const context = {
    fs: { readFileSync: () => new Uint8Array([1]) }, path, parseIndusIndSavingsStatement, parseIndieBusinessStatement, parseIndusIndMobileStatement,
    parseBankStatementText: () => { state.fallbackCalls++; return [{ debit: 90 }]; },
    pdfjs: { getDocument: async () => ({ numPages: pages.length, getPage: async number => ({ getTextContent: async () => pages[number - 1] }), destroy: () => { state.destroyed = true; } }) }
  };
  vm.createContext(context);
  vm.runInContext(source.slice(source.indexOf('async function readPdfText('), source.indexOf('function bankRowKey(')), context);
  return { state, parse: () => context.parseBankStatementUpload('synthetic.pdf', 'synthetic.pdf', '') };
}

test('PDF upload selects positioned mobile cells before the ambiguous legacy parser', async () => {
  const harness = uploadHarness(fixture());
  const rows = await harness.parse();
  assert.equal(rows[0].debit, 490);
  assert.equal(harness.state.fallbackCalls, 0);
  assert.equal(harness.state.destroyed, true);
});

test('recognized invalid mobile PDF upload fails closed without fallback', async () => {
  const pdf = fixture(); pdf.pages[0].items.find(cell => cell.str === '490.00').str = '490.001000.00';
  const harness = uploadHarness(pdf);
  await assert.rejects(harness.parse(), /not a complete currency value/);
  assert.equal(harness.state.fallbackCalls, 0);
  assert.equal(harness.state.destroyed, true);
});

test('mobile PDF with an incomplete table header is recognized and fails before fallback', async () => {
  const pdf = fixture(); pdf.pages[0].items = pdf.pages[0].items.filter(cell => cell.str !== 'Withdrawal');
  const harness = uploadHarness(pdf);
  await assert.rejects(harness.parse(), /six statement columns/);
  assert.equal(harness.state.fallbackCalls, 0);
});

'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { parseIciciCreditCardPdf, isIciciCreditCardText } = require('../modules/icici-credit-card-pdf');

// Synthetic cells model ICICI's positioned PDF text, including its overlaid
// numeric font. No customer statement, card number or transaction is a fixture.
const columns = [40, 112, 195, 390, 460, 550];
const headings = ['Date', 'SerNo.', 'Transaction Details', 'Reward Points', 'Intl.# amount', 'Amount (in`)'];
const item = (str, x, y, fontName = 'plain') => ({ str, transform: [1, 0, 0, 1, x, y], width: String(str).length * 4, fontName });
const records = () => [
  { date: '15/09/2026', ref: '12345678901', description: 'SYNTHETIC SHOP', rewards: '5', amount: '125.25' },
  { date: '20/09/2026', ref: '12345678902', description: 'INFINITY PAYMENT RECEIVED, THANK YOU', rewards: '0', amount: '90.00', credit: true },
  { date: '22/09/2026', ref: '12345678903', description: 'SYNTHETIC FOREIGN REFUND', rewards: '0', foreign: '1.67 USD', amount: '30.50', credit: true },
  { date: '29/09/2026', ref: '12345678904', description: '24 SEVEN SYNTHETIC SHOP', rewards: '3', amount: '80.25' }
];
const zeroBankNotice = (changes = {}) => ({ date: '14/09/2026', ref: '98765432101',
  description: 'Auto Dr.Retn-Insuff. fund', rewards: '0', amount: '0.00', credit: true, ...changes });

function tablePage(rows, header = true) {
  const items = header ? headings.map((value, index) => item(value, columns[index], 700)) : [];
  if (header) items.push(item('123456XXXXXX1234', columns[2], 685));
  rows.forEach((row, index) => {
    const y = 670 - index * 30;
    [row.date, row.ref, row.description, row.rewards, row.foreign || '', row.amount].forEach((value, column) => {
      if (value !== '' && value != null) items.push(item(value, columns[column], y));
    });
    if (row.credit) items.push(item('CR', 620, y));
  });
  return { items, fonts: { plain: 'Arial' } };
}

function refreshText(pdf) {
  pdf.text = pdf.pages.map(page => {
    let y, text = '';
    page.items.forEach(cell => { text += y == null || y === cell.transform[5] ? '' : '\n'; text += cell.str; y = cell.transform[5]; });
    return text;
  }).join('\n\n');
  return pdf;
}

function fixture(options = {}) {
  const rows = options.rows || records();
  const pages = options.pages || [tablePage(rows)];
  const summary = options.summary || { opening: '100.00', charges: '205.50', cash: '0.00', credits: '120.50', due: '185.00' };
  pages[0].items.unshift(
    item('CREDIT CARD STATEMENT', 40, 900), item('STATEMENT DATE', 40, 880), item('October 11, 2026', 40, 860),
    item('CARD NUMBER : 123456XXXXXX1234', 40, 840), item('Total Amount due', 40, 820), item(summary.due, 40, 800),
    item('Previous Balance', 40, 760), item('Purchases / Charges', 190, 760), item('Cash Advances', 340, 760), item('Payments / Credits', 490, 760),
    item(summary.opening, 40, 740), item(summary.charges, 190, 740), item(summary.cash, 340, 740), item(summary.credits, 490, 740)
  );
  pages[0].items.push(item('Statement period : September 12, 2026 to October 11, 2026', 40, 400));
  return refreshText({ pages });
}

const failure = /ICICI credit-card statement validation failed/i;
const cents = rows => [Math.round(rows.reduce((sum, row) => sum + row.debit, 0) * 100), Math.round(rows.reduce((sum, row) => sum + row.credit, 0) * 100)];

test('ICICI positioned cells preserve INR amounts, credit directions and independently validated totals', () => {
  const rows = parseIciciCreditCardPdf(fixture());
  assert.equal(rows.length, 4);
  assert.deepEqual(rows.map(row => [row.date, row.reference, row.debit, row.credit]), [
    ['2026-09-15', '12345678901', 125.25, 0],
    ['2026-09-20', '12345678902', 0, 90],
    ['2026-09-22', '12345678903', 0, 30.5],
    ['2026-09-29', '12345678904', 80.25, 0]
  ]);
  assert.deepEqual(cents(rows), [20550, 12050]);
  assert.equal(rows.at(-1).balance, 185);
  const summary = rows.statementSummary;
  assert.equal(summary.format, 'ICICI Bank credit-card PDF');
  assert.equal(summary.from, '2026-09-12');
  assert.equal(summary.to, '2026-10-11');
  assert.equal(summary.openingBalance, 100);
  assert.equal(summary.closingBalance, 185);
  assert.equal(summary.totalDebits, 205.5);
  assert.equal(summary.totalCredits, 120.5);
  assert.equal(summary.validated, true);
  assert.equal(summary.accountType, 'credit_card');
  assert.ok(summary.cardLast4s.includes('1234'));
});

test('ICICI foreign-currency decimal and reward points cannot become the transaction amount', () => {
  const data = records();
  data[0].foreign = '999.99 USD';
  data[0].rewards = '12345';
  const rows = parseIciciCreditCardPdf(fixture({ rows: data }));
  assert.equal(rows[0].debit, 125.25);
  assert.equal(rows[2].credit, 30.5);
  assert.deepEqual(cents(rows), [20550, 12050]);
});

test('ICICI separate CR cell marks both payments and refunds as credits', () => {
  const rows = parseIciciCreditCardPdf(fixture());
  assert.deepEqual(rows.filter(row => row.credit).map(row => row.credit), [90, 30.5]);
  assert.match(rows.find(row => row.reference === '12345678904').description, /24 SEVEN SYNTHETIC SHOP/);
});

test('ICICI zero-value auto-debit return notice is retained as evidence, never a financial transaction', () => {
  const rows = parseIciciCreditCardPdf(fixture({ rows: [zeroBankNotice(), ...records()] }));
  assert.equal(rows.length, 4);
  assert.deepEqual(cents(rows), [20550, 12050]);
  assert.equal(rows.at(-1).balance, 185);
  assert.deepEqual(rows.map(row => row.row), [2, 3, 4, 5]);
  assert.ok(rows.every(row => row.debit > 0 || row.credit > 0));
  assert.ok(rows.every(row => !Object.hasOwn(row, 'side')));
  assert.deepEqual(rows.statementSummary.nonPostingEntries, [{
    date: '2026-09-14', reference: '98765432101', description: 'Auto Dr.Retn-Insuff. fund',
    cardLast4: '1234', amount: 0, debit: 0, credit: 0, sourceRow: 1,
    reason: 'zero_value_bank_notice',
    explanation: 'ICICI auto-debit return notice: insufficient funds; no money moved and no ledger entry is posted.'
  }]);
  assert.equal(rows.statementSummary.totalDebits, 205.5);
  assert.equal(rows.statementSummary.totalCredits, 120.5);
  assert.equal(rows.statementSummary.validated, true);
});

test('ICICI known zero notice accepts ordinary case and spacing without changing its retained narration', () => {
  const description = 'AUTO   DR. RETN - INSUFF. FUND';
  const rows = parseIciciCreditCardPdf(fixture({ rows: [zeroBankNotice({ description }), ...records()] }));
  assert.equal(rows.length, 4);
  assert.equal(rows.statementSummary.nonPostingEntries[0].description, description);
});

test('ICICI exact zero-notice narration can wrap across positioned detail lines', () => {
  const pdf = fixture({ rows: [zeroBankNotice({ description: 'Auto Dr.Retn-' }), ...records()] });
  pdf.pages[0].items.push(item('Insuff. fund', columns[2], 657));
  const rows = parseIciciCreditCardPdf(refreshText(pdf));
  assert.equal(rows.length, 4);
  assert.equal(rows.statementSummary.nonPostingEntries[0].description, 'Auto Dr.Retn- Insuff. fund');
});

test('ICICI zero notice requires the complete exact narration, including wrapped suffixes', () => {
  for (const description of ['SYNTHETIC MERCHANT', 'Auto Dr.Retn-Insuff. funds',
    'Auto Dr.Retn-Insuff. fund REVERSAL', 'Auto Dr.Retn-Other reason']) {
    assert.throws(() => parseIciciCreditCardPdf(fixture({ rows: [zeroBankNotice({ description }), ...records()] })),
      /transaction 1 has a zero INR amount/i, description);
  }
  const pdf = fixture({ rows: [zeroBankNotice(), ...records()] });
  pdf.pages[0].items.push(item('ADDITIONAL UNKNOWN BANK EVENT', columns[2], 657));
  assert.throws(() => parseIciciCreditCardPdf(refreshText(pdf)), /transaction 1 has a zero INR amount/i);
});

test('ICICI recognized zero notice without explicit CR direction or with DR fails closed', () => {
  for (const marker of ['', 'DR']) {
    const pdf = fixture({ rows: [zeroBankNotice({ credit: false }), ...records()] });
    if (marker) pdf.pages[0].items.push(item(marker, 620, 670));
    assert.throws(() => parseIciciCreditCardPdf(refreshText(pdf)), /transaction 1 has a zero INR amount/i, marker || 'no marker');
  }
});

test('ICICI zero notice still rejects competing CR and DR markers', () => {
  const pdf = fixture({ rows: [zeroBankNotice(), ...records()] });
  pdf.pages[0].items.push(item('DR', 635, 670));
  assert.throws(() => parseIciciCreditCardPdf(refreshText(pdf)), /conflicting debit \/ credit markers/i);
});

test('ICICI verified zero VartaSet2 overlay is retained only with its own explicit CR marker', () => {
  const pdf = fixture({ rows: [zeroBankNotice({ amount: '199.99', credit: false }), ...records()] });
  pdf.pages[0].fonts.numeric = 'VartaSet2';
  pdf.pages[0].items.push(item('Q.QQ', columns[5], 670, 'numeric'), item('CR', 620, 670));
  const rows = parseIciciCreditCardPdf(refreshText(pdf));
  assert.equal(rows.length, 4);
  assert.equal(rows.statementSummary.nonPostingEntries[0].amount, 0);
  assert.deepEqual(cents(rows), [20550, 12050]);
});

test('ICICI zero overlay cannot inherit CR from a stale underlayer', () => {
  const pdf = fixture({ rows: [zeroBankNotice({ amount: '199.99' }), ...records()] });
  pdf.pages[0].fonts.numeric = 'VartaSet2';
  pdf.pages[0].items.push(item('Q.QQ', columns[5], 670, 'numeric'));
  assert.throws(() => parseIciciCreditCardPdf(refreshText(pdf)), /transaction 1 has a zero INR amount/i);
});

test('ICICI zero-notice handling never relaxes conflicting amount or replacement-layer checks', () => {
  const ordinary = fixture({ rows: [zeroBankNotice(), ...records()] });
  ordinary.pages[0].items.push(item('0.01', columns[5] + 1, 670));
  assert.throws(() => parseIciciCreditCardPdf(refreshText(ordinary)), /conflicting INR amounts/i);
  const overlay = fixture({ rows: [zeroBankNotice({ credit: false }), ...records()] });
  overlay.pages[0].fonts.numeric = 'VartaSet2';
  overlay.pages[0].items.push(item('Q.QQ', columns[5], 670, 'numeric'), item('0.00', columns[5] + 1, 670), item('CR', 620, 670));
  assert.throws(() => parseIciciCreditCardPdf(refreshText(overlay)), /ambiguous replacement layer/i);
});

test('ICICI zero notice still requires valid complete date, reference and INR currency cell', () => {
  for (const changes of [{ date: '31/09/2026' }, { date: '12/10/2026' }, { date: '14/09/202' },
    { ref: '9876543210' }, { ref: '' }, { amount: '' }, { amount: '0.0' }, { amount: 'Q.QQ' }]) {
    assert.throws(() => parseIciciCreditCardPdf(fixture({ rows: [zeroBankNotice(changes), ...records()] })), failure, JSON.stringify(changes));
  }
});

test('ICICI non-zero credit with the same notice text remains a financial transaction', () => {
  const data = records(); data[1].description = zeroBankNotice().description;
  const rows = parseIciciCreditCardPdf(fixture({ rows: data }));
  assert.equal(rows.length, 4);
  assert.equal(rows.find(row => row.reference === data[1].ref).credit, 90);
  assert.deepEqual(rows.statementSummary.nonPostingEntries, []);
});

test('ICICI retained notices cannot conceal missing transactions or mismatched declared totals', () => {
  assert.throws(() => parseIciciCreditCardPdf(fixture({ rows: [zeroBankNotice(), ...records().slice(1)] })), /do not match declared totals/i);
  assert.throws(() => parseIciciCreditCardPdf(fixture({ rows: [zeroBankNotice()],
    summary: { opening: '100.00', charges: '0.00', cash: '0.00', credits: '0.00', due: '100.00' } })), /no complete dated transactions/i);
});

test('ICICI distinct notice positions preserve all notice evidence even when references repeat', () => {
  const rows = parseIciciCreditCardPdf(fixture({ rows: [zeroBankNotice(), zeroBankNotice(), ...records()] }));
  assert.equal(rows.length, 4);
  assert.deepEqual(rows.statementSummary.nonPostingEntries.map(entry => entry.sourceRow), [1, 2]);
});

test('ICICI ordinary CR cells retain their visual direction when painted before the amount', () => {
  const pdf = fixture(), items = pdf.pages[0].items;
  const marks = items.filter(cell => cell.str === 'CR');
  marks.forEach(mark => {
    items.splice(items.indexOf(mark), 1);
    const amountIndex = items.findIndex(cell => cell.transform[4] === columns[5] && cell.transform[5] === mark.transform[5]);
    items.splice(amountIndex, 0, mark);
  });
  const rows = parseIciciCreditCardPdf(refreshText(pdf));
  assert.deepEqual(cents(rows), [20550, 12050]);
});

test('ICICI conflicting CR and DR markers cannot be reconciled by choosing one side', () => {
  const pdf = fixture();
  const credit = pdf.pages[0].items.find(cell => cell.str === 'CR');
  pdf.pages[0].items.push(item('DR', 635, credit.transform[5]));
  assert.throws(() => parseIciciCreditCardPdf(refreshText(pdf)), failure);
});

test('ICICI repeated identical PDF fields at the same position are one visual row', () => {
  const pdf = fixture();
  const duplicates = pdf.pages[0].items.filter(cell => cell.transform[5] === 670).map(cell => ({ ...cell, transform: cell.transform.slice() }));
  pdf.pages[0].items.push(...duplicates);
  const rows = parseIciciCreditCardPdf(refreshText(pdf));
  assert.equal(rows.length, 4);
  assert.equal(rows[0].debit, 125.25);
});

test('ICICI verified VartaSet2 digit overlay wins over its stale numeric underlayer', () => {
  const pdf = fixture();
  const amount = pdf.pages[0].items.find(cell => cell.str === '125.25');
  amount.str = '129.99';
  pdf.pages[0].fonts.numeric = 'VartaSet2';
  pdf.pages[0].items.push(item('HIL.IL', amount.transform[4], amount.transform[5], 'numeric'));
  const rows = parseIciciCreditCardPdf(refreshText(pdf));
  assert.equal(rows[0].debit, 125.25);
  assert.deepEqual(cents(rows), [20550, 12050]);
});

test('ICICI known replacement amount does not inherit a credit marker from the earlier stale layer', () => {
  const pdf = fixture();
  const amount = pdf.pages[0].items.find(cell => cell.str === '125.25');
  amount.str = '129.99';
  pdf.pages[0].fonts.numeric = 'VartaSet2';
  pdf.pages[0].items.push(item('CR', 620, amount.transform[5]));
  pdf.pages[0].items.push(item('HIL.IL', amount.transform[4], amount.transform[5], 'numeric'));
  const rows = parseIciciCreditCardPdf(refreshText(pdf));
  assert.equal(rows[0].debit, 125.25);
  assert.equal(rows[0].credit, 0);
});

test('ICICI unknown pseudo-numeric font is never decoded by an unverified alphabet guess', () => {
  const pdf = fixture();
  const amount = pdf.pages[0].items.find(cell => cell.str === '125.25');
  amount.str = 'HIL.IL'; amount.fontName = 'unknown';
  pdf.pages[0].fonts.unknown = 'UnverifiedNumericFont';
  assert.throws(() => parseIciciCreditCardPdf(refreshText(pdf)), failure);
});

test('ICICI conflicting ordinary amount overlays are rejected rather than selected to fit totals', () => {
  const pdf = fixture();
  const amount = pdf.pages[0].items.find(cell => cell.str === '125.25');
  pdf.pages[0].items.push(item('125.26', amount.transform[4], amount.transform[5]));
  assert.throws(() => parseIciciCreditCardPdf(refreshText(pdf)), failure);
});

test('ICICI an encoded layer cannot overrule a later-painted competing ordinary amount', () => {
  const pdf = fixture();
  const amount = pdf.pages[0].items.find(cell => cell.str === '125.25');
  pdf.pages[0].fonts.numeric = 'VartaSet2';
  const at = pdf.pages[0].items.indexOf(amount);
  pdf.pages[0].items.splice(at, 0, item('HIL.IL', amount.transform[4], amount.transform[5], 'numeric'));
  amount.str = '129.99';
  assert.throws(() => parseIciciCreditCardPdf(refreshText(pdf)), failure);
});

test('ICICI does not deduplicate distinct transactions merely because their serial references repeat', () => {
  const data = records(); data[3].ref = data[0].ref;
  const rows = parseIciciCreditCardPdf(fixture({ rows: data }));
  assert.equal(rows.length, 4);
  assert.equal(rows.filter(row => row.reference === data[0].ref).length, 2);
  assert.deepEqual(cents(rows), [20550, 12050]);
});

test('ICICI headerless transaction pages fail closed instead of using guessed columns', () => {
  const data = records();
  const pages = [tablePage(data.slice(0, 2)), tablePage(data.slice(2), false)];
  assert.throws(() => parseIciciCreditCardPdf(fixture({ pages })), failure);
});

test('ICICI ignores a terms-only final page without losing transactions', () => {
  const pdf = fixture();
  pdf.pages.push({ items: [item('Terms and conditions: This is a computer generated statement.', 40, 700)], fonts: { plain: 'Arial' } });
  const rows = parseIciciCreditCardPdf(refreshText(pdf));
  assert.equal(rows.length, 4);
  assert.deepEqual(cents(rows), [20550, 12050]);
});

test('ICICI repeated table headings on continuation pages preserve every transaction', () => {
  const data = records();
  const rows = parseIciciCreditCardPdf(fixture({ pages: [tablePage(data.slice(0, 2)), tablePage(data.slice(2))] }));
  assert.equal(rows.length, 4);
  assert.deepEqual(cents(rows), [20550, 12050]);
});

test('ICICI a dated transaction without its INR cell fails closed', () => {
  const data = records(); data[0].amount = '';
  assert.throws(() => parseIciciCreditCardPdf(fixture({ rows: data })), failure);
});

test('ICICI rejects incomplete and malformed amount cells', () => {
  for (const value of ['125.2', '125.250', '1,2.50', '1e2', '125.25 80.25']) {
    const data = records(); data[0].amount = value;
    assert.throws(() => parseIciciCreditCardPdf(fixture({ rows: data })), failure, value);
  }
});

test('ICICI rejects invalid calendar dates and transactions later than the declared period', () => {
  for (const date of ['31/09/2026', '12/10/2026']) {
    const data = records(); data[0].date = date;
    assert.throws(() => parseIciciCreditCardPdf(fixture({ rows: data })), failure, date);
  }
});

test('ICICI retains a prior-cycle transaction posted into this statement', () => {
  const data = records(); data[0].date = '11/09/2026';
  const rows = parseIciciCreditCardPdf(fixture({ rows: data }));
  assert.equal(rows[0].date, '2026-09-11');
  assert.equal(rows[0].debit, 125.25);
  assert.deepEqual(cents(rows), [20550, 12050]);
});

test('ICICI malformed transaction dates are not silently omitted', () => {
  const data = records(); data[0].date = '15/09/202';
  assert.throws(() => parseIciciCreditCardPdf(fixture({ rows: data })), failure);
});

test('ICICI requires the complete independent statement summary', () => {
  for (const field of ['Previous Balance', 'Purchases / Charges', 'Cash Advances', 'Payments / Credits']) {
    const pdf = fixture();
    pdf.pages[0].items = pdf.pages[0].items.filter(cell => cell.str !== field);
    assert.throws(() => parseIciciCreditCardPdf(refreshText(pdf)), failure, field);
  }
});

test('ICICI requires a separately readable Total Amount due', () => {
  for (const removeLabel of [false, true]) {
    const pdf = fixture();
    pdf.pages[0].items = pdf.pages[0].items.filter(cell => cell.transform[5] !== 800 && (!removeLabel || cell.str !== 'Total Amount due'));
    assert.throws(() => parseIciciCreditCardPdf(refreshText(pdf)), failure);
  }
});

test('ICICI uses the printed total due instead of a minimum due painted earlier in the PDF stream', () => {
  const pdf = fixture(), items = pdf.pages[0].items;
  const labelIndex = items.findIndex(cell => cell.str === 'Total Amount due');
  items.splice(labelIndex, 0, item('Minimum Amount due', 40, 790));
  const valueIndex = items.findIndex(cell => cell.transform[5] === 800);
  items.splice(valueIndex, 0, item('25.00', 40, 770));
  const rows = parseIciciCreditCardPdf(refreshText(pdf));
  assert.equal(rows.statementSummary.closingBalance, 185);
  assert.equal(rows.at(-1).balance, 185);
});

test('ICICI checks purchases, credits and due independently down to a cent', () => {
  const defaults = { opening: '100.00', charges: '205.50', cash: '0.00', credits: '120.50', due: '185.00' };
  for (const [key, value] of [['charges', '205.51'], ['credits', '120.51'], ['due', '185.01']]) {
    assert.throws(() => parseIciciCreditCardPdf(fixture({ summary: { ...defaults, [key]: value } })), failure, key);
  }
});

test('ICICI includes declared cash advances in liability movement without dropping the summary column', () => {
  const rows = parseIciciCreditCardPdf(fixture({ summary: { opening: '100.00', charges: '125.25', cash: '80.25', credits: '120.50', due: '185.00' } }));
  assert.deepEqual(cents(rows), [20550, 12050]);
  assert.equal(rows.statementSummary.closingBalance, 185);
});

test('ICICI an omitted transaction cannot be hidden by the reconstructed closing balance', () => {
  assert.throws(() => parseIciciCreditCardPdf(fixture({ rows: records().slice(1) })), failure);
});

test('ICICI requires original positioned cells and stays out of unrelated statement formats', () => {
  const pdf = fixture();
  assert.throws(() => parseIciciCreditCardPdf({ text: pdf.text, pages: [] }), failure);
  assert.equal(parseIciciCreditCardPdf({ text: 'Other Bank Statement\nDate Debit Credit Balance', pages: pdf.pages }), null);
});

function uploadHarness(options = {}) {
  const pdf = fixture(), amount = pdf.pages[0].items.find(cell => cell.str === '125.25');
  amount.str = '129.99';
  const numericFontName = options.literalFontName || 'numeric';
  pdf.pages[0].items.push(item('HIL.IL', amount.transform[4], amount.transform[5], numericFontName));
  const source = fs.readFileSync(path.join(__dirname, '../modules/expenses.js'), 'utf8');
  const code = source.slice(source.indexOf('async function readPdfText('), source.indexOf('function bankRowKey('));
  const state = { operators: 0, fonts: [], destroyed: false, fallbacks: 0 };
  // Exercise the real upload/extraction functions with the PDF.js transport
  // boundary stubbed. The private proprietary font is never a fixture asset.
  const context = {
    fs: { readFileSync: () => new Uint8Array([1, 2, 3]) }, path,
    setTimeout: options.fastTimeout ? callback => setTimeout(callback, 0) : setTimeout, clearTimeout,
    pdfjs: { PDFJS: {}, OPS: { setFont: 37 }, NativeImageDecoding: { NONE: 'none' }, getDocument: async () => ({
      numPages: 1,
      getPage: async () => ({
        getTextContent: async () => ({ items: pdf.pages[0].items }),
        getOperatorList: async () => { state.operators++; return { fnArray: [37], argsArray: [[options.operatorFontId || 'numeric', 6]] }; },
        commonObjs: { get: (name, resolve) => { state.fonts.push(name); if (!options.unresolvedFont) resolve({ name: name === numericFontName ? (options.fontName || 'VartaSet2') : 'Arial', data: options.fontData || [1], missingFile: !!options.missingFile }); } }
      }),
      destroy: () => { state.destroyed = true; }
    }) },
    parseIciciCreditCardPdf, isIciciCreditCardText,
    parseIndusIndSavingsStatement: () => null, parseIndieBusinessStatement: () => null,
    parseIndusIndMobileStatement: () => null,
    parseBankStatementText: () => { state.fallbacks++; throw new Error('Recognized ICICI PDF must not fall through to legacy text parsing.'); }
  };
  vm.createContext(context); vm.runInContext(code, context);
  return { state, parse: () => context.parseBankStatementUpload('synthetic.pdf', 'synthetic.pdf', '') };
}

test('ICICI PDF upload resolves custom-font metadata before parsing the visible layer', async () => {
  const { state, parse } = uploadHarness();
  const rows = await parse();
  assert.equal(rows.length, 4);
  assert.equal(rows[0].debit, 125.25);
  assert.deepEqual(cents(rows), [20550, 12050]);
  assert.equal(rows.statementSummary.validated, true);
  assert.equal(state.operators, 1);
  assert.deepEqual(state.fonts, ['numeric']);
  assert.equal(state.destroyed, true);
  assert.equal(state.fallbacks, 0);
});

test('ICICI unresolved font metadata times out safely, releases the document and never falls back', async () => {
  const { state, parse } = uploadHarness({ unresolvedFont: true, fastTimeout: true });
  await assert.rejects(parse(), failure);
  assert.equal(state.operators, 1);
  assert.deepEqual(state.fonts, ['numeric']);
  assert.equal(state.destroyed, true);
  assert.equal(state.fallbacks, 0);
});

test('ICICI an unsupported resolved font fails closed instead of trying legacy numeric guesses', async () => {
  const { state, parse } = uploadHarness({ fontName: 'UnknownCustomDigits' });
  await assert.rejects(parse(), failure);
  assert.equal(state.destroyed, true);
  assert.equal(state.fallbacks, 0);
});

test('ICICI literal font names absent from setFont operator IDs are rejected before an unresolved lookup', async () => {
  const { state, parse } = uploadHarness({ literalFontName: 'VartaSet2', operatorFontId: 'g_d0_f4R' });
  await assert.rejects(parse(), /ICICI credit-card statement validation failed:.*not embedded/i);
  assert.equal(state.operators, 1);
  assert.deepEqual(state.fonts, []);
  assert.equal(state.destroyed, true);
  assert.equal(state.fallbacks, 0);
});

test('ICICI font metadata must contain a real embedded font program, not fallback or missing-file metadata', async () => {
  for (const options of [{ missingFile: true }, { fontData: [] }]) {
    const { state, parse } = uploadHarness(options);
    await assert.rejects(parse(), /ICICI credit-card statement validation failed:.*not embedded/i);
    assert.equal(state.operators, 1);
    assert.deepEqual(state.fonts, ['numeric']);
    assert.equal(state.destroyed, true);
    assert.equal(state.fallbacks, 0);
  }
});



'use strict';

// IndusInd Privilege savings statements use a six-column transaction table.
// PDF text extraction concatenates the amount and balance cells, so use the
// original PDF.js positions instead of guessing where one number ends.
const HEADINGS = ['Date', 'Particulars', 'Chq No/Ref No', 'Withdrawal', 'Deposit', 'Balance'];
const MONTHS = {
  january: 1, february: 2, march: 3, april: 4, may: 5, june: 6,
  july: 7, august: 8, september: 9, october: 10, november: 11, december: 12
};

const compact = value => String(value || '').replace(/\s+/g, '').toLowerCase();
const fail = detail => { throw new Error('IndusInd savings statement validation failed: ' + detail + ' No preview was created.'); };

function calendarDate(year, month, day) {
  const value = `${year}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
  const parsed = new Date(value + 'T00:00:00Z');
  return Number.isFinite(parsed.getTime()) && parsed.toISOString().slice(0, 10) === value ? value : '';
}

function longDate(value) {
  const match = String(value || '').match(/^(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})$/);
  return match && MONTHS[match[2].toLowerCase()]
    ? calendarDate(match[3], MONTHS[match[2].toLowerCase()], Number(match[1]))
    : '';
}

function moneyCents(value, optional, label) {
  const text = String(value || '').trim();
  if (optional && (!text || /^[-\u2013\u2014]$/.test(text))) return 0;
  // This statement family prints whole rupees and one-decimal balances as
  // well as ordinary two-decimal currency values. Keep every comparison in
  // integer paise so a partially parsed row cannot pass by floating rounding.
  if (!/^-?(?:\d+|\d{1,3}(?:,\d{3})+|\d{1,2}(?:,\d{2})*,\d{3})(?:\.\d{1,2})?$/.test(text)) {
    fail(label + ' is not a complete currency value.');
  }
  const cents = Math.round(Number(text.replace(/,/g, '')) * 100);
  if (!Number.isSafeInteger(cents) || Math.abs(cents) > 1e14) fail(label + ' is outside the supported amount range.');
  return cents;
}

function pageLines(page) {
  const items = (page.items || []).filter(item => String(item.str || '').trim()).map(item => ({
    text: String(item.str).trim(),
    x: Number(item.transform && item.transform[4]),
    y: Number(item.transform && item.transform[5])
  }));
  if (items.some(item => !Number.isFinite(item.x) || !Number.isFinite(item.y))) fail('the table positions could not be read.');
  items.sort((a, b) => b.y - a.y || a.x - b.x);
  const lines = [];
  for (const item of items) {
    let line = lines.at(-1);
    if (line && Math.abs(line.y - item.y) > 1) line = null;
    if (!line) { line = { y: item.y, items: [] }; lines.push(line); }
    line.items.push(item);
  }
  lines.forEach(line => line.items.sort((a, b) => a.x - b.x));
  return lines.sort((a, b) => b.y - a.y);
}

function headerColumns(line) {
  if (line.items.length !== HEADINGS.length || line.items.some((item, index) => compact(item.text) !== compact(HEADINGS[index]))) return null;
  const columns = line.items.map(item => item.x);
  return columns.some((value, index) => index && value <= columns[index - 1]) ? null : columns;
}

function columnFor(item, columns, padding) {
  const shifted = columns.map(value => value - padding);
  for (let index = 0; index < shifted.length - 1; index++) {
    if (item.x < (shifted[index] + shifted[index + 1]) / 2) return index;
  }
  return shifted.length - 1;
}

function normalizedDescription(parts) {
  return parts.join(' ').replace(/\s+/g, ' ').trim()
    .replace(/\b(UPI|IMPS\/P2A)\/([\d\s]+)(?=\/)/gi, (full, prefix, digits) => {
      const reference = digits.replace(/\s/g, '');
      return /^\d{10,}$/.test(reference) ? prefix + '/' + reference : full;
    })
    // Long N/R/PG bank references are sometimes wrapped between digits by
    // the PDF renderer. Rejoin only the reference segment immediately before
    // its delimiter; leave beneficiary narration and ordinary words alone.
    .replace(/\b(N|R|PG)\/([A-Z0-9]+(?:\s+\d+)*)(?=\/)/gi, (full, prefix, reference) => {
      return prefix + '/' + reference.replace(/\s/g, '');
    });
}

function referenceFrom(description, printedReference) {
  const printed = String(printedReference || '').trim();
  if (printed && !/^[-\u2013\u2014]$/.test(printed)) return printed;
  const payment = description.match(/\b(?:UPI|IMPS\/P2A)\/([\d\s]{10,})(?=\/)/i);
  if (payment) return payment[1].replace(/\s/g, '');
  const bankReference = description.match(/\b(?:N|R|PG)\/([A-Z0-9]{8,})/i);
  if (bankReference) return bankReference[1];
  const accountReference = description.match(/\b(?:Account|PAY)\/([0-9]{8,})/i);
  return accountReference ? accountReference[1] : '';
}

function parseIndusIndSavingsStatement(pdf) {
  const text = String(pdf && pdf.text || '').replace(/\r/g, '');
  const isFormat = /Account\s+Statement/i.test(text)
    && /Savings\s+Account\s+Summary/i.test(text)
    && /Transaction\s+history\s+for\s+Savings\s+Account/i.test(text)
    && /INDUS\s+PRIVILEGE/i.test(text)
    && /DateParticularsChq\s*No\/Ref\s*NoWithdrawalDepositBalance/i.test(text);
  if (!isFormat) return null;

  const accounts = Array.from(text.matchAll(/(\d{10,14})\s*INDUS\s+PRIVILEGE/gi)).map(match => match[1]);
  const account = accounts[0] || '';
  if (!account || accounts.some(value => value !== account)) fail('the account number is missing or inconsistent.');
  const periodMatches = Array.from(text.matchAll(/Statement\s+Period\s*:\s*(\d{1,2}\s+[A-Za-z]+\s+\d{4})\s*-\s*(\d{1,2}\s+[A-Za-z]+\s+\d{4})/gi));
  const periods = periodMatches.map(match => ({ from: longDate(match[1]), to: longDate(match[2]) }));
  const period = periods[0];
  if (!period || !period.from || !period.to || period.from > period.to || periods.some(value => value.from !== period.from || value.to !== period.to)) {
    fail('the statement period is missing or inconsistent.');
  }
  if (!Array.isArray(pdf.pages) || !pdf.pages.length) fail('the original PDF table positions are required.');

  const records = [];
  let previousColumns, active, padding;
  for (const page of pdf.pages) {
    const lines = pageLines(page);
    const headingIndex = lines.findIndex(line => headerColumns(line));
    const hasDate = lines.some(line => line.items.some(item => /^\d{4}-\d{2}-\d{2}$/.test(item.text)));
    const hasTableContent = previousColumns && lines.some(line => line.items.some(item => item.x >= previousColumns[2] - 30 && (/^[-\u2013\u2014]$/.test(item.text) || /^-?[\d,]+(?:\.\d{1,2})?$/.test(item.text))));
    if (headingIndex < 0 && !hasDate && !hasTableContent) continue;
    const columns = headingIndex >= 0 ? headerColumns(lines[headingIndex]) : previousColumns;
    if (!columns) fail('the six statement columns could not be read.');
    previousColumns = columns;

    for (const line of lines.slice(headingIndex >= 0 ? headingIndex + 1 : 0)) {
      if (headerColumns(line)) continue;
      const lineText = line.items.map(item => item.text).join(' ');
      if (/^(?:This is a computer generated statement|Acronyms\s*:|Page\s+\d+(?:\s+of\s+\d+)?)/i.test(lineText)) break;
      const dateItem = line.items.find(item => /^\d{4}-\d{2}-\d{2}$/.test(item.text));
      if (dateItem) {
        if (padding == null) padding = columns[0] - dateItem.x;
        if (padding < -2 || padding > 30 || Math.abs(columns[0] - dateItem.x - padding) > 2) fail('a dated row is outside the Date column.');
        const parts = dateItem.text.split('-');
        const date = calendarDate(parts[0], Number(parts[1]), Number(parts[2]));
        if (!date || date < period.from || date > period.to) fail('a transaction date is invalid or outside the statement period.');
        active = { date, cells: HEADINGS.map(() => []), row: records.length + 1 };
        records.push(active);
      }
      if (padding == null) {
        if (/Account\s+Statement|Savings\s+Account|Statement\s+Period|Date\s*:/i.test(lineText)) continue;
        fail('table content was found without a readable transaction date.');
      }
      for (const item of line.items) {
        const column = columnFor(item, columns, padding);
        if (!active) fail('a table cell could not be assigned to a dated row.');
        if (column === 0 && item !== dateItem) fail('a transaction date is missing or malformed.');
        active.cells[column].push(item.text);
      }
    }
  }
  if (!records.length) fail('no complete dated transactions were found.');

  const rows = records.map(record => {
    const cells = record.cells.map(parts => parts.join(' ').trim());
    const debit = moneyCents(cells[3], true, 'Withdrawal in transaction ' + record.row);
    const credit = moneyCents(cells[4], true, 'Deposit in transaction ' + record.row);
    const balance = moneyCents(cells[5], false, 'Balance in transaction ' + record.row);
    if (debit < 0 || credit < 0 || !(debit > 0 || credit > 0) || (debit > 0 && credit > 0)) {
      fail('transaction ' + record.row + ' must have exactly one positive withdrawal or deposit.');
    }
    const description = normalizedDescription(record.cells[1]);
    if (!description) fail('transaction ' + record.row + ' has no complete particulars.');
    return {
      date: record.date,
      description,
      reference: referenceFrom(description, cells[2]),
      debit: debit / 100,
      credit: credit / 100,
      balance: balance / 100,
      row: record.row,
      _debit: debit,
      _credit: credit,
      _balance: balance
    };
  });

  if (rows.some((row, index) => index && rows[index - 1].date < row.date)) fail('transactions are not in the expected newest-first order.');
  const oldest = rows.at(-1);
  const opening = oldest._balance + oldest._debit - oldest._credit;
  let running = opening;
  for (const row of rows.slice().reverse()) {
    running += row._credit - row._debit;
    if (running !== row._balance) fail('transaction ' + row.row + ' does not match its running balance.');
  }
  const totalDebits = rows.reduce((sum, row) => sum + row._debit, 0);
  const totalCredits = rows.reduce((sum, row) => sum + row._credit, 0);
  rows.forEach(row => { delete row._debit; delete row._credit; delete row._balance; });
  rows.statementSummary = {
    format: 'IndusInd Privilege savings PDF',
    accountLast4: account.slice(-4),
    from: period.from,
    to: period.to,
    openingBalance: opening / 100,
    openingBalanceInferred: true,
    closingBalance: running / 100,
    totalDebits: totalDebits / 100,
    totalCredits: totalCredits / 100,
    validated: true
  };
  return rows;
}

module.exports = { parseIndusIndSavingsStatement };

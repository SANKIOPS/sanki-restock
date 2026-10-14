'use strict';

// INDIE business statements use separate Ref No and Chq No columns. PDF text
// extraction joins those cells (and the two monetary cells) without spaces, so
// this profile reads their positions instead of guessing numeric substrings.
const HEADINGS = ['Date', 'Particulars', 'Ref No', 'Chq No', 'Withdrawal', 'Deposit', 'Balance'];
const MONTHS = { january: 1, february: 2, march: 3, april: 4, may: 5, june: 6, july: 7, august: 8, september: 9, october: 10, november: 11, december: 12 };
const compact = value => String(value || '').replace(/\s+/g, '').toLowerCase();
const fail = detail => { throw new Error('IndusInd INDIE statement validation failed: ' + detail + ' No preview was created.'); };

function calendarDate(year, month, day) {
  const value = `${year}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
  const parsed = new Date(value + 'T00:00:00Z');
  return Number.isFinite(parsed.getTime()) && parsed.toISOString().slice(0, 10) === value ? value : '';
}

function longDate(value) {
  const match = String(value || '').match(/^(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})$/);
  return match && MONTHS[match[2].toLowerCase()] ? calendarDate(match[3], MONTHS[match[2].toLowerCase()], Number(match[1])) : '';
}

function moneyCents(value, optional, label) {
  const text = String(value || '').trim();
  if (optional && (!text || /^[-\u2013\u2014]$/.test(text))) return 0;
  // Keep cents exact, permit ordinary and Indian thousands separators, and
  // reject concatenated cells, scientific notation and incomplete decimals.
  if (!/^-?(?:\d+|\d{1,3}(?:,\d{3})+|\d{1,2}(?:,\d{2})*,\d{3})\.\d{2}$/.test(text)) fail(label + ' is not a complete currency value.');
  const cents = Math.round(Number(text.replace(/,/g, '')) * 100);
  if (!Number.isSafeInteger(cents) || Math.abs(cents) > 1e12) fail(label + ' is outside the supported amount range.');
  return cents;
}

function pageLines(page) {
  const items = (page.items || []).filter(item => String(item.str || '').trim()).map(item => ({
    text: String(item.str).trim(), x: Number(item.transform && item.transform[4]), y: Number(item.transform && item.transform[5])
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
  return line.items.map(item => item.x);
}

function parseIndieBusinessStatement(pdf) {
  const text = String(pdf && pdf.text || '').replace(/\r/g, '');
  const isFormat = /IndusInd\s+Bank/i.test(text) && /Statement\s+Of\s+Account/i.test(text) && /Account\s+Number\s*:/i.test(text)
    && /Ref\s*No\s*Chq\s*No|indie@indusind\.com/i.test(text);
  if (!isFormat) return null;

  const accounts = Array.from(text.matchAll(/Account\s+Number\s*:\s*([0-9Xx*]+)/gi)).map(match => match[1]);
  const last4 = accounts[0] && (accounts[0].match(/(\d{4})$/) || [])[1];
  if (!last4 || accounts.some(account => account !== accounts[0])) fail('the account number is missing or inconsistent.');
  const periods = Array.from(text.matchAll(/Period\s*:\s*(\d{1,2}\s+[A-Za-z]+\s+\d{4})\s*-\s*(\d{1,2}\s+[A-Za-z]+\s+\d{4})/gi)).map(match => ({ from: longDate(match[1]), to: longDate(match[2]) }));
  const period = periods[0];
  if (!period || !period.from || !period.to || period.from > period.to || periods.some(value => value.from !== period.from || value.to !== period.to)) fail('the statement period is missing or inconsistent.');
  if (!Array.isArray(pdf.pages) || !pdf.pages.length) fail('the original PDF table positions are required.');

  const records = [];
  let previousColumns;
  for (const page of pdf.pages) {
    const lines = pageLines(page), headingIndex = lines.findIndex(line => headerColumns(line));
    const hasDate = lines.some(line => line.items.some(item => /^\d{4}\//.test(item.text)));
    const hasTableContent = previousColumns && lines.some(line => line.items.some(item => item.x >= previousColumns[2] - 32 && (/^[A-Za-z]\d{4,}$/.test(item.text) || /^-?[\d,]+\.[\d.]+$/.test(item.text))));
    if (headingIndex < 0 && !hasDate && !hasTableContent) continue; // A terms-only final page.
    const columns = headingIndex >= 0 ? headerColumns(lines[headingIndex]) : previousColumns;
    if (!columns || columns.some((value, index) => index && value <= columns[index - 1])) fail('the seven statement columns could not be read.');
    previousColumns = columns;
    let active, padding;
    for (const line of lines.slice(headingIndex >= 0 ? headingIndex + 1 : 0)) {
      if (headerColumns(line)) continue;
      const lineText = line.items.map(item => item.text).join(' ');
      if (/^(?:This is a computer generated statement|Acronyms\s*:|Page\s+\d+(?:\s+of\s+\d+)?)/i.test(lineText)) break;
      const dateItem = line.items.find(item => /^\d{4}\/\d{2}\/\d{2}$/.test(item.text));
      if (dateItem) {
        if (padding == null) padding = columns[0] - dateItem.x;
        if (padding < -2 || padding > 30 || Math.abs(columns[0] - dateItem.x - padding) > 2) fail('a dated row is outside the Date column.');
        const parts = dateItem.text.split('/'), date = calendarDate(parts[0], Number(parts[1]), Number(parts[2]));
        if (!date || date < period.from || date > period.to) fail('a transaction date is invalid or outside the statement period.');
        active = { date, cells: HEADINGS.map(() => []), row: records.length + 1 };
        records.push(active);
      }
      if (padding == null) {
        // Headers on continuation pages may precede their first dated row.
        if (/IndusInd\s+Bank|Statement\s+Of\s+Account|Account\s+Number\s*:|Period\s*:|Date\s*:/i.test(lineText)) continue;
        fail('table content was found without a readable transaction date.');
      }
      for (const item of line.items) {
        let column = columns.length - 1;
        while (column >= 0 && item.x < columns[column] - padding - 2) column--;
        if (column < 0 || !active) fail('a table cell could not be assigned to a dated row.');
        if (column === 0 && item !== dateItem) fail('a transaction date is missing or malformed.');
        active.cells[column].push(item.text);
      }
    }
  }
  if (!records.length) fail('no complete dated transactions were found.');

  const rows = records.map(record => {
    const cells = record.cells.map(parts => parts.join(' ').trim());
    const debit = moneyCents(cells[4], true, 'Withdrawal in transaction ' + record.row);
    const credit = moneyCents(cells[5], true, 'Deposit in transaction ' + record.row);
    const balance = moneyCents(cells[6], false, 'Balance in transaction ' + record.row);
    if (debit < 0 || credit < 0 || !(debit > 0 || credit > 0) || (debit > 0 && credit > 0)) fail('transaction ' + record.row + ' must have exactly one positive withdrawal or deposit.');
    if (!cells[1] || !cells[2] || !/^[A-Za-z0-9/-]+$/.test(cells[2])) fail('transaction ' + record.row + ' has no complete particulars or reference.');
    return { date: record.date, description: cells[1], reference: /^[-\u2013\u2014]$/.test(cells[2]) ? '' : cells[2], debit: debit / 100, credit: credit / 100, balance: balance / 100, row: record.row, _debit: debit, _credit: credit, _balance: balance };
  });
  if (rows.some((row, index) => index && rows[index - 1].date < row.date)) fail('transactions are not in the expected newest-first order.');
  const oldest = rows.at(-1), opening = oldest._balance + oldest._debit - oldest._credit;
  let running = opening;
  for (const row of rows.slice().reverse()) {
    running += row._credit - row._debit;
    if (running !== row._balance) fail('transaction ' + row.row + ' does not match its running balance.');
  }
  const totalDebits = rows.reduce((sum, row) => sum + row._debit, 0), totalCredits = rows.reduce((sum, row) => sum + row._credit, 0);
  rows.forEach(row => { delete row._debit; delete row._credit; delete row._balance; });
  rows.statementSummary = { format: 'IndusInd INDIE business PDF', accountLast4: last4, from: period.from, to: period.to, openingBalance: opening / 100, openingBalanceInferred: true, closingBalance: running / 100, totalDebits: totalDebits / 100, totalCredits: totalCredits / 100, validated: true };
  return rows;
}

module.exports = { parseIndieBusinessStatement };

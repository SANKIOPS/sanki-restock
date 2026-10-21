'use strict';

// Mobile statements print the reference and money in adjacent columns. Flattened
// text cannot distinguish S1234567 + 490.00 from S12345674 + 90.00. Read the
// original cells, including the oldest row where no previous balance exists.
const HEADINGS = ['Date', 'Particulars', 'Chq No/Ref No', 'Withdrawal', 'Deposit', 'Balance'];
const MONTHS = { jan: 1, feb: 2, mar: 3, apr: 4, may: 5, jun: 6, jul: 7, aug: 8, sep: 9, oct: 10, nov: 11, dec: 12 };
const compact = value => String(value || '').replace(/\s+/g, '').toLowerCase();
const fail = message => { throw new Error('IndusInd mobile statement validation failed: ' + message + ' No preview was created.'); };

function dateValue(value) {
  const match = String(value || '').match(/^(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})$/);
  if (!match || !MONTHS[match[2].slice(0, 3).toLowerCase()]) return '';
  const month = MONTHS[match[2].slice(0, 3).toLowerCase()];
  const fullMonths = ['january', 'february', 'march', 'april', 'may', 'june', 'july', 'august', 'september', 'october', 'november', 'december'];
  if (match[2].length !== 3 && match[2].toLowerCase() !== fullMonths[month - 1]) return '';
  const date = match[3] + '-' + String(month).padStart(2, '0') + '-' + match[1].padStart(2, '0');
  const parsed = new Date(date + 'T00:00:00Z');
  return Number.isFinite(parsed.getTime()) && parsed.toISOString().slice(0, 10) === date ? date : '';
}

function moneyCents(value, optional, label) {
  const text = String(value || '').trim();
  if (optional && (!text || /^[-\u2013\u2014]$/.test(text))) return 0;
  if (!/^-?(?:\d+|\d{1,3}(?:,\d{3})+|\d{1,2}(?:,\d{2})*,\d{3})\.\d{2}$/.test(text)) fail(label + ' is not a complete currency value.');
  const amount = Math.round(Number(text.replace(/,/g, '')) * 100);
  if (!Number.isSafeInteger(amount) || Math.abs(amount) > 1e14) fail(label + ' is outside the supported amount range.');
  return amount;
}

function pageLines(page) {
  const items = (page.items || []).filter(item => String(item.str || '').trim()).map(item => ({
    text: String(item.str).trim(), x: Number(item.transform && item.transform[4]), y: Number(item.transform && item.transform[5]), width: Number(item.width)
  }));
  if (items.some(item => !Number.isFinite(item.x) || !Number.isFinite(item.y))) fail('the original table positions could not be read.');
  items.sort((a, b) => b.y - a.y || a.x - b.x);
  const lines = [];
  for (const item of items) {
    let line = lines.at(-1);
    if (!line || Math.abs(line.y - item.y) > 1) { line = { y: item.y, items: [] }; lines.push(line); }
    line.items.push(item);
  }
  lines.forEach(line => line.items.sort((a, b) => a.x - b.x));
  return lines;
}

function headerColumns(line) {
  const columns = [];
  const rightEdges = [];
  let index = 0;
  for (const heading of HEADINGS) {
    const expected = compact(heading), first = line.items[index];
    let actual = '';
    while (index < line.items.length && actual.length < expected.length) actual += compact(line.items[index++].text);
    if (!first || actual !== expected) return null;
    columns.push(first.x);
    const last = line.items[index - 1];
    rightEdges.push(last.x + last.width);
  }
  if (index !== line.items.length || !columns.every((x, i) => !i || x > columns[i - 1]) || rightEdges.some(value => !Number.isFinite(value))) return null;
  columns.rightEdges = rightEdges;
  return columns;
}

function columnFor(item, columns) {
  // Monetary cells are right-aligned with their headings in this PDF family.
  // A wide value can start left of the heading midpoint, so classify a whole
  // currency item by its right edge, never its first digit's x-coordinate.
  if (/^-?[\d,]+\.\d{2}$/.test(item.text) && item.x > (columns[1] + columns[2]) / 2) {
    const right = item.x + item.width;
    const matches = [3, 4, 5].filter(index => Number.isFinite(right) && Math.abs(right - columns.rightEdges[index]) <= 2);
    if (matches.length !== 1) fail('a numeric cell is not aligned with a unique monetary column.');
    return matches[0];
  }
  for (let i = 0; i < columns.length - 1; i++) if (item.x < (columns[i] + columns[i + 1]) / 2) return i;
  return columns.length - 1;
}

function joinedCell(parts, numeric, label) {
  if (!parts.length) return '';
  if (!numeric) return parts.map(part => part.text).join(' ').replace(/\s+/g, ' ').trim();
  // A PDF may emit individual digits. They may be joined within one positioned
  // cell on one baseline, but two separate lines/values must never be merged.
  if (parts.some(part => Math.abs(part.y - parts[0].y) > 1)) fail(label + ' has more than one numeric line.');
  const sorted = parts.slice().sort((a, b) => a.x - b.x);
  for (let index = 1; index < sorted.length; index++) {
    const previous = sorted[index - 1], gap = sorted[index].x - previous.x - previous.width;
    if (!Number.isFinite(gap) || gap < -2 || gap > 4) fail(label + ' contains separated numeric cells.');
  }
  return sorted.map(part => part.text).join('');
}

function parseIndusIndMobileStatement(pdf) {
  const text = String(pdf && pdf.text || '').replace(/\r/g, '');
  const isFormat = /Account\s+Statement\s*\n\s*\d{10,14}/i.test(text)
    && /INDUS\s+PRIVILEGE/i.test(text) && /Transaction\s+History/i.test(text)
    && !/Savings\s+Account\s+Summary/i.test(text);
  if (!isFormat) return null;
  const accounts = Array.from(text.matchAll(/Account\s+Statement\s*\n\s*(\d{10,14})(?!\d)/gi)).map(match => match[1]);
  if (!accounts.length || accounts.some(account => account !== accounts[0])) fail('the account number is missing or inconsistent.');
  const periods = Array.from(text.matchAll(/Statement\s+Period\s*:\s*(\d{1,2}\s+[A-Za-z]+\s+\d{4})\s*-\s*(\d{1,2}\s+[A-Za-z]+\s+\d{4})/gi))
    .map(match => ({ from: dateValue(match[1]), to: dateValue(match[2]) }));
  const period = periods[0];
  if (!period || !period.from || !period.to || period.from > period.to || periods.some(value => value.from !== period.from || value.to !== period.to)) fail('the statement period is missing or inconsistent.');
  if (!Array.isArray(pdf.pages) || !pdf.pages.length) fail('the original PDF table positions are required.');

  const records = [];
  let previousColumns, active;
  for (const page of pdf.pages) {
    const lines = pageLines(page), headingIndex = lines.findIndex(line => headerColumns(line));
    const hasDate = lines.some(line => line.items.some(item => /^\d{1,2}\s+[A-Za-z]+\s+\d{4}$/.test(item.text)));
    const hasTableContent = previousColumns && lines.some(line => line.items.some(item => item.x >= previousColumns[2] - 20 && (/^[SM]\d{5,12}$/i.test(item.text) || /^-?[\d,]+\.[\d.]+$/.test(item.text))));
    if (headingIndex < 0 && !hasDate && !hasTableContent) continue;
    const columns = headingIndex >= 0 ? headerColumns(lines[headingIndex]) : previousColumns;
    if (!columns) fail('the six statement columns could not be read.');
    previousColumns = columns;
    for (const line of lines.slice(headingIndex >= 0 ? headingIndex + 1 : 0)) {
      if (headerColumns(line)) continue;
      const lineText = line.items.map(item => item.text).join(' ');
      if (/^(?:This is a computer generated statement|Acronyms\s*:|Page\s+\d+(?:\s+of\s+\d+)?)/i.test(lineText)) break;
      const dateParts = line.items.filter(item => columnFor(item, columns) === 0);
      const rawDate = dateParts.map(item => item.text).join(' ').trim();
      if (rawDate) {
        const date = dateValue(rawDate);
        if (!date || date < period.from || date > period.to) fail('a transaction date is invalid or outside the statement period.');
        active = { date, cells: HEADINGS.map(() => []), row: records.length + 1 };
        records.push(active);
      }
      for (const item of line.items) {
        if (!active) fail('table content was found without a readable transaction date.');
        active.cells[columnFor(item, columns)].push(item);
      }
    }
  }
  if (!records.length) fail('no complete dated transactions were found.');
  const rows = records.map(record => {
    const cells = record.cells.map((parts, index) => joinedCell(parts, index >= 2, HEADINGS[index] + ' in transaction ' + record.row));
    const debit = moneyCents(cells[3], true, 'Withdrawal in transaction ' + record.row);
    const credit = moneyCents(cells[4], true, 'Deposit in transaction ' + record.row);
    const balance = moneyCents(cells[5], false, 'Balance in transaction ' + record.row);
    if (debit < 0 || credit < 0 || !(debit > 0 || credit > 0) || (debit > 0 && credit > 0)) fail('transaction ' + record.row + ' must have exactly one positive withdrawal or deposit.');
    const bankReference = cells[2].replace(/\s/g, '');
    if (!/^[SM]\d{5,12}$/i.test(bankReference)) fail('transaction ' + record.row + ' has no complete bank reference.');
    const description = cells[1].replace(/\b(UPI|IMPS\/P2A)\/([\d\s]+)(?=\/)/gi, (full, prefix, digits) => {
      const reference = digits.replace(/\s/g, '');
      return /^\d{10,}$/.test(reference) ? prefix + '/' + reference : full;
    });
    if (!description) fail('transaction ' + record.row + ' has no particulars.');
    const side = description.match(/\/(DR|CR)\//i), reversal = /(?:REVERSED\s*:|\bREVERSAL\b|^RVS\b)/i.test(description);
    if (!reversal && side && (side[1].toUpperCase() === 'DR' ? !debit : !credit)) fail('transaction ' + record.row + ' contradicts its printed payment direction.');
    const reference = (description.match(/\b(?:UPI|IMPS\/P2A)\/(\d{10,})(?=\/)/i) || [])[1] || bankReference;
    return { date: record.date, description, reference, bankReference, debit: debit / 100, credit: credit / 100, balance: balance / 100, row: record.row, _debit: debit, _credit: credit, _balance: balance };
  });
  if (rows.some((row, index) => index && rows[index - 1].date < row.date)) fail('transactions are not in the expected newest-first order.');
  const oldest = rows.at(-1), opening = oldest._balance + oldest._debit - oldest._credit;
  let running = opening;
  for (const row of rows.slice().reverse()) {
    running += row._credit - row._debit;
    if (running !== row._balance) fail('transaction ' + row.row + ' does not match its running balance.');
  }
  const openingMatch = text.match(/Opening\s+Balance\s*:\s*(-?[\d,]+\.\d{2})/i);
  const closingMatch = text.match(/Closing\s+Balance\s*:\s*(-?[\d,]+\.\d{2})/i);
  if (openingMatch && moneyCents(openingMatch[1], false, 'Opening balance') !== opening) fail('the printed opening balance does not match the transactions.');
  if (closingMatch && moneyCents(closingMatch[1], false, 'Closing balance') !== running) fail('the printed closing balance does not match the transactions.');
  const totalDebits = rows.reduce((sum, row) => sum + row._debit, 0), totalCredits = rows.reduce((sum, row) => sum + row._credit, 0);
  rows.forEach(row => { delete row._debit; delete row._credit; delete row._balance; });
  rows.statementSummary = { format: 'IndusInd mobile PDF', parserVersion: 'positioned-cells-v1', accountLast4: accounts[0].slice(-4), from: period.from, to: period.to, openingBalance: opening / 100, openingBalanceInferred: !openingMatch, closingBalance: running / 100, totalDebits: totalDebits / 100, totalCredits: totalCredits / 100, validated: true };
  return rows;
}

module.exports = { parseIndusIndMobileStatement };

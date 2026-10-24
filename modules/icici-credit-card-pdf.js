'use strict';

// ICICI's generated PDFs can paint the transaction table twice: a stale
// underlying table and a visible replacement using VartaSet2. That font has
// no useful Unicode mapping for its digits. Never concatenate these layers or
// infer INR amounts from reward points / foreign-currency amounts.
const fail = message => { throw new Error('ICICI credit-card statement validation failed: ' + message + ' No preview was created.'); };
const compact = text => String(text || '').replace(/\s+/g, '').toLowerCase();
const MONEY = '(?:\\d+|\\d{1,3}(?:,\\d{3})+|\\d{1,2}(?:,\\d{2})*,\\d{3})\\.\\d{2}';
const AMOUNT = new RegExp('^(' + MONEY + ')\\s*(CR|DR)?$', 'i');
const VARTA_DIGITS = { H: '1', I: '2', J: '3', K: '4', L: '5', M: '6', N: '7', O: '8', P: '9', Q: '0' };

function isIciciCreditCardText(text) {
  return /CREDIT\s+CARD\s+STATEMENT/i.test(text) && /STATEMENT\s+DATE/i.test(text)
    && /Transaction\s+Details\s*Reward/i.test(text);
}

function cents(value, label) {
  const match = String(value || '').trim().match(AMOUNT);
  if (!match) fail(label + ' is not a complete currency value.');
  const amount = Math.round(Number(match[1].replace(/,/g, '')) * 100);
  if (!Number.isSafeInteger(amount) || amount < 0 || amount > 1e12) fail(label + ' is outside the supported amount range.');
  return { amount, side: (match[2] || '').toUpperCase() };
}

function calendar(year, month, day) {
  const date = `${year}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
  const parsed = new Date(date + 'T00:00:00Z');
  return Number.isFinite(parsed.getTime()) && parsed.toISOString().slice(0, 10) === date ? date : '';
}

function longDate(text) {
  const match = String(text || '').match(/^([A-Za-z]+)\s+(\d{1,2}),\s*(\d{4})$/);
  const months = ['january','february','march','april','may','june','july','august','september','october','november','december'];
  return match ? calendar(match[3], months.indexOf(match[1].toLowerCase()) + 1, Number(match[2])) : '';
}

function pageLines(page) {
  const items = (page.items || []).map((item, order) => ({
    text: String(item.str || '').trim(), x: Number(item.transform && item.transform[4]),
    y: Number(item.transform && item.transform[5]), width: Number(item.width || 0),
    font: (page.fonts || {})[item.fontName] || '', order
  })).filter(item => item.text);
  if (items.some(item => !Number.isFinite(item.x) || !Number.isFinite(item.y))) fail('the original table positions are not readable.');
  items.sort((a, b) => b.y - a.y || a.x - b.x || a.order - b.order);
  const lines = [];
  for (const item of items) {
    let line = lines.at(-1);
    if (!line || Math.abs(line.y - item.y) > .75) { line = { y: item.y, items: [] }; lines.push(line); }
    // Duplicate text at the SAME physical position is a second text layer,
    // not a second transaction. Identical references at different positions
    // are deliberately retained and validated against the statement totals.
    const same = line.items.findIndex(old => old.text === item.text && Math.abs(old.x - item.x) < .5);
    if (same >= 0) line.items[same] = item;
    else line.items.push(item);
  }
  lines.forEach(line => line.items.sort((a, b) => a.x - b.x || a.order - b.order));
  return lines;
}

function columns(line) {
  const find = pattern => line.items.find(item => pattern.test(compact(item.text)));
  const date = find(/^date$/), reference = find(/^serno\.?$/), details = find(/^transactiondetails$/),
    reward = find(/^reward(?:points)?$/), international = find(/^intl\.?\#?(?:amount)?$/), amount = find(/^amount\(in/);
  if (![date, reference, details, reward, international, amount].every(Boolean)) return null;
  if (![date, reference, details, reward, international, amount].every((item, i, all) => !i || item.x > all[i - 1].x)) fail('the transaction columns overlap or are out of order.');
  return { date: date.x, reference: (date.x + reference.x) / 2, details: details.x - 3,
    reward: (details.x + details.width + reward.x) / 2,
    international: (reward.x + reward.width + international.x) / 2,
    amount: (international.x + amount.x) / 2 };
}

function rowAmount(items, row) {
  const amounts = [], marks = [];
  for (const item of items) {
    if (/^(CR|DR)$/i.test(item.text)) { marks.push(item); continue; }
    let text = item.text, encoded = false;
    if (/^(?:[A-Z]{6}\+)?VartaSet2$/i.test(item.font)) {
      // This is the documented glyph sequence of VartaSet2, not a guessed
      // substitution for arbitrary alphabetic narration or an unknown font.
      text = text.replace(/[H-Q]/g, char => VARTA_DIGITS[char]); encoded = true;
    }
    const parsed = cents(text, 'INR amount in transaction ' + row);
    amounts.push({ ...parsed, encoded, order: item.order });
  }
  if (!amounts.length) fail('transaction ' + row + ' has no INR amount.');
  const overlays = amounts.filter(item => item.encoded), selected = overlays.length ? overlays : amounts;
  if (new Set(selected.map(item => item.amount + '|' + item.side)).size !== 1) fail('transaction ' + row + ' has conflicting INR amounts.');
  const value = selected.reduce((last, item) => item.order > last.order ? item : last);
  // Only the known, later-painted VartaSet2 replacement may supersede a
  // different numeric underlayer. Two competing ordinary amounts must fail.
  if (overlays.length && amounts.some(item => !item.encoded && item.order > value.order)) fail('transaction ' + row + ' has an ambiguous replacement layer.');
  const sides = new Set([value.side, ...marks.filter(item => !overlays.length || item.order > value.order).map(item => item.text.toUpperCase())].filter(Boolean));
  if (sides.size > 1) fail('transaction ' + row + ' has conflicting debit / credit markers.');
  if (!value.amount) fail('transaction ' + row + ' has a zero INR amount.');
  return { amount: value.amount, credit: sides.has('CR') };
}

function printedAmountDue(pdf) {
  const amounts = [];
  for (const page of pdf.pages) {
    const lines = pageLines(page), items = lines.flatMap(line => line.items);
    for (const label of items.filter(item => /^totalamountdue$/i.test(compact(item.text)))) {
      // The PDF stream can put Minimum Amount Due's value BEFORE the total
      // due's value. Use the printed position, never the next decimal in text.
      const lowerLabel = items.filter(item => item.y < label.y && /^(?:minimumamountdue|creditlimit)/i.test(compact(item.text))).sort((a, b) => b.y - a.y)[0];
      const values = items.filter(item => item.y < label.y && label.y - item.y <= 35
        && (!lowerLabel || item.y > lowerLabel.y) && item.x >= label.x - 5 && item.x <= label.x + label.width + 5
        && AMOUNT.test(item.text.replace(/^[`₹]\s*/, '')));
      if (values.length !== 1) fail('Total Amount Due was not readable or has conflicting values.');
      const parsed = cents(values[0].text.replace(/^[`₹]\s*/, ''), 'Total Amount Due');
      amounts.push(parsed.amount * (parsed.side === 'CR' ? -1 : 1));
    }
  }
  if (!amounts.length || new Set(amounts).size !== 1) fail('Total Amount Due was not readable or has conflicting values.');
  return amounts[0];
}

function declaredTotals(pdf) {
  const names = ['previousbalance', 'purchases/charges', 'cashadvances', 'payments/credits'], summaries = [];
  for (const page of pdf.pages) {
    const lines = pageLines(page), items = lines.flatMap(line => line.items);
    for (const line of lines) {
      const labels = names.map(name => line.items.find(item => compact(item.text) === name));
      if (!labels.every(Boolean)) continue;
      summaries.push(labels.map(label => {
        const values = items.filter(item => item.y < label.y && label.y - item.y <= 30
          && item.x >= label.x - 5 && item.x <= label.x + label.width + 5
          && AMOUNT.test(item.text.replace(/^[`₹]\s*/, '')));
        if (values.length !== 1) fail('the declared ' + label.text + ' total is missing or ambiguous.');
        const value = cents(values[0].text.replace(/^[`₹]\s*/, ''), label.text);
        if (value.side) fail('the declared ' + label.text + ' total has an unsupported direction marker.');
        return value.amount;
      }));
    }
  }
  if (!summaries.length || new Set(summaries.map(values => values.join('|'))).size !== 1) fail('the four declared statement totals are missing or inconsistent.');
  return summaries[0];
}

function parseIciciCreditCardPdf(pdf) {
  const text = String(pdf && pdf.text || '').replace(/\r/g, '');
  if (!isIciciCreditCardText(text)) return null;
  if (!Array.isArray(pdf.pages) || !pdf.pages.length) fail('the original PDF table positions are required.');
  const values = declaredTotals(pdf);
  const due = printedAmountDue(pdf);
  const [opening, purchases, cashAdvances, credits] = values;
  if (opening + purchases + cashAdvances - credits !== due) fail('the declared totals do not reproduce the printed Total Amount Due.');
  const periodMatch = text.match(/Statement\s+period\s*:\s*([A-Za-z]+\s+\d{1,2},\s*\d{4})\s+to\s+([A-Za-z]+\s+\d{1,2},\s*\d{4})/i);
  const from = periodMatch && longDate(periodMatch[1]), to = periodMatch && longDate(periodMatch[2]);
  if (!from || !to || from > to) fail('the statement period is missing or invalid.');

  const records = [], cardLast4s = new Set();
  let cardLast4 = '';
  for (const page of pdf.pages) {
    const lines = pageLines(page), headerIndex = lines.findIndex(line => columns(line));
    if (headerIndex < 0) {
      // A terms page is harmless; a transaction-bearing page without its
      // column headings is not safe to interpret using guessed positions.
      if (lines.some(line => line.items.some(item => /^\d{2}\/\d{2}\/\d{4}$/.test(item.text)))) fail('a transaction page has no readable table headings.');
      continue;
    }
    const cols = columns(lines[headerIndex]);
    let active;
    for (const line of lines.slice(headerIndex + 1)) {
      if (columns(line)) continue;
      const relevant = line.items.filter(item => item.x >= cols.date - 5), lineText = relevant.map(item => item.text).join(' ');
      if (/^(?:The standing instruction|For further information|IMPORTANT MESSAGES|#?\s*International Spends|Page\s+\d)/i.test(lineText)) break;
      const card = relevant.find(item => /^\d{4,6}[Xx*]{4,}\d{4}$/.test(item.text));
      if (card) { cardLast4 = card.text.slice(-4); cardLast4s.add(cardLast4); active = null; continue; }
      const dateItems = relevant.filter(item => item.x < cols.reference && /^\d{1,2}\/\d{1,2}\/\d{4}$/.test(item.text));
      if (dateItems.length) {
        if (dateItems.length !== 1) fail('a transaction has multiple dates in the Date column.');
        const parts = dateItems[0].text.split('/'), date = calendar(parts[2], Number(parts[1]), Number(parts[0]));
        // ICICI can include a prior-cycle transaction posted in this cycle.
        // Do not drop it just because its transaction date precedes 'from'.
        if (!date || date > to) fail('a transaction date is invalid or later than the statement period.');
        const references = relevant.filter(item => item.x >= cols.reference && item.x < cols.details);
        if (references.length !== 1 || !/^\d{11}$/.test(references[0].text)) fail('a transaction reference is missing or malformed.');
        const description = relevant.filter(item => item.x >= cols.details && item.x < cols.reward).map(item => item.text);
        if (!description.length) fail('a transaction has no merchant / particulars.');
        const money = rowAmount(relevant.filter(item => item.x >= cols.amount), records.length + 1);
        active = { date, description, reference: references[0].text, debit: money.credit ? 0 : money.amount,
          credit: money.credit ? money.amount : 0, row: records.length + 1, ...(cardLast4 ? { cardLast4 } : {}) };
        records.push(active);
      } else {
        if (relevant.some(item => item.x < cols.reference && /^[0-9/]+$/.test(item.text))) fail('a transaction date is missing or malformed.');
        if (relevant.some(item => item.x >= cols.amount && (AMOUNT.test(item.text) || /^[H-Q,]+\.[H-Q]{2}$/.test(item.text)))) fail('an INR amount has no complete dated transaction.');
        const continuation = relevant.filter(item => item.x >= cols.details && item.x < cols.reward);
        if (active && continuation.length && relevant.every(item => item.x >= cols.details && item.x < cols.reward)) active.description.push(...continuation.map(item => item.text));
      }
    }
  }
  if (!records.length) fail('no complete dated transactions were found.');
  const debits = records.reduce((sum, row) => sum + row.debit, 0), parsedCredits = records.reduce((sum, row) => sum + row.credit, 0);
  if (debits !== purchases + cashAdvances || parsedCredits !== credits) fail('parsed Purchases / Charges ₹' + debits / 100 + ' and Payments / Credits ₹' + parsedCredits / 100 + ' do not match declared totals ₹' + (purchases + cashAdvances) / 100 + ' and ₹' + credits / 100 + '.');
  let running = opening;
  records.sort((a, b) => a.date.localeCompare(b.date) || a.row - b.row);
  const rows = records.map(record => {
    running += record.debit - record.credit;
    return { ...record, description: record.description.join(' '), debit: record.debit / 100, credit: record.credit / 100, balance: running / 100 };
  });
  if (running !== due) fail('transactions do not reproduce the printed Total Amount Due.');
  rows.statementSummary = { format: 'ICICI Bank credit-card PDF', from, to, openingBalance: opening / 100,
    closingBalance: due / 100, totalDebits: debits / 100, totalCredits: parsedCredits / 100,
    validated: true, accountType: 'credit_card', cardLast4s: [...cardLast4s],
    ...(cardLast4s.size === 1 ? { accountLast4: [...cardLast4s][0] } : {}) };
  return rows;
}

module.exports = { parseIciciCreditCardPdf, isIciciCreditCardText };

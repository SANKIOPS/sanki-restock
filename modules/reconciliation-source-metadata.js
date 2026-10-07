'use strict';

// Retained-source hashes must be checked by the caller before invoking this
// helper. This function only proposes metadata patches, never ledger changes.
function amountInCents(value) {
  if (typeof value !== 'number' && typeof value !== 'string') return null;
  if (typeof value === 'string' && !/^-?\d+(?:\.\d{1,2})?$/.test(value.trim())) return null;
  const amount = Number(value);
  if (!Number.isFinite(amount)) return null;
  const cents = Math.round(amount * 100);
  if (!Number.isSafeInteger(cents) || Math.abs(amount * 100 - cents) > 1e-7) return null;
  return cents;
}

function transactionKey(row) {
  if (!row || typeof row !== 'object' || typeof row.date !== 'string') return null;
  const date = row.date;
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) return null;
  const timestamp = Date.parse(date + 'T00:00:00Z');
  if (!Number.isFinite(timestamp) || new Date(timestamp).toISOString().slice(0, 10) !== date) return null;
  const debit = amountInCents(row.debit), credit = amountInCents(row.credit), balance = amountInCents(row.balance);
  if (debit === null || credit === null || balance === null || debit < 0 || credit < 0 || (debit > 0 && credit > 0)) return null;
  return JSON.stringify([date, debit, credit, balance]);
}

function uniqueTransactions(rows) {
  const keyed = new Map();
  for (let index = 0; index < rows.length; index++) {
    const key = transactionKey(rows[index]);
    if (!key || keyed.has(key)) return null;
    keyed.set(key, { index, row: rows[index] });
  }
  return keyed;
}

function recoverReversalSourceMetadata(input) {
  const options = input || {}, current = options.currentTransactions, parsed = options.parsedTransactions;
  const summary = options.parsedSummary || (parsed && parsed.statementSummary), accountLast4 = String(options.accountLast4 || '');
  if (!Array.isArray(current) || !Array.isArray(parsed) || !current.length || current.length !== parsed.length) return [];
  if (!summary || summary.validated !== true || !/^\d{4}$/.test(accountLast4) || String(summary.accountLast4 || '') !== accountLast4) return [];
  const currentRows = uniqueTransactions(current), sourceRows = uniqueTransactions(parsed);
  if (!currentRows || !sourceRows || currentRows.size !== sourceRows.size) return [];
  // Establish the complete bijection before proposing even one metadata patch.
  // Mobile PDFs may be reverse chronological; keep existing draft indexes.
  for (const key of currentRows.keys()) if (!sourceRows.has(key)) return [];
  const patches = [];
  for (const [key, value] of currentRows) {
    const source = sourceRows.get(key).row;
    const description = typeof source.description === 'string' ? source.description : '';
    if (source.reversal !== true && !/\bREVERSED\b/i.test(description)) continue;
    const reference = typeof source.reference === 'string' ? source.reference.trim() : String(source.reference || '');
    if (!/^\d{12}$/.test(reference) || !description.trim()) return [];
    const metadata = { description, reference, reversal: true };
    if (value.row.description !== metadata.description || value.row.reference !== metadata.reference || value.row.reversal !== true) patches.push({ index: value.index, metadata });
  }
  return patches;
}

module.exports = { recoverReversalSourceMetadata };

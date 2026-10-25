'use strict';

const { bankEvidenceIndex, resolveBankRowEvidence, safeBankEvidenceId } = require('./bank-opening-row-evidence');
const MAX_ROWS = 10000, MAX_CENTS = BigInt(Number.MAX_SAFE_INTEGER);
const object = value => value !== null && typeof value === 'object' && !Array.isArray(value);
const own = (value, key) => Object.prototype.hasOwnProperty.call(value, key);
function date(value) {
  if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(value)) return '';
  const parsed = new Date(value + 'T00:00:00Z');
  return Number.isFinite(parsed.getTime()) && parsed.toISOString().slice(0, 10) === value ? value : '';
}
// Financial evidence must already be exact paise; never round a discrepancy.
function cents(value) {
  if (!['string', 'number'].includes(typeof value) || typeof value === 'number' && !Number.isFinite(value)) return null;
  const text = String(value);
  if (text.length > 24 || !/^-?(?:0|[1-9]\d*)(?:\.\d{1,2})?$/.test(text)) return null;
  const negative = text.startsWith('-'), [whole, fractional = ''] = (negative ? text.slice(1) : text).split('.');
  const result = (BigInt(whole) * 100n + BigInt(fractional.padEnd(2, '0'))) * (negative ? -1n : 1n);
  return result >= -MAX_CENTS && result <= MAX_CENTS ? result : null;
}
const ADJUSTED = ['companyAdjustedClosingBalance', 'calculatedCompanyBalance', 'closingBalanceAdjustment',
  'closingBalanceAdjustmentReason', 'closingBalanceAdjustedAt', 'closingBalanceAdjustedBy',
  'companyAdjustedBankTruth', 'reportedClosingBalance', 'closingBalanceCorrectedFromMergedRows'];
const adjusted = value => ADJUSTED.some(key => own(value, key)
  && value[key] !== undefined && value[key] !== null && value[key] !== false && value[key] !== '');
function uniqueIds(values) {
  if (!Array.isArray(values) || !values.length || values.length > MAX_ROWS) return null;
  const ids = new Set();
  for (const value of values) {
    if (!safeBankEvidenceId(value) || ids.has(value)) return null;
    ids.add(value);
  }
  return ids;
}
const sameIds = (left, right) => left.size === right.size && [...left].every(id => right.has(id));
function reportIds(record) {
  const ids = [];
  for (const key of ['reconciliationRows', 'carriedReconciliationRows']) {
    if (record[key] != null && !Array.isArray(record[key])) return null;
    for (const row of record[key] || []) {
      if (!object(row)) return null;
      if (row.bank == null) continue; // Ledger-only report rows are not bank coverage.
      if (!object(row.bank) || !safeBankEvidenceId(row.bank.transactionId)) return null;
      ids.push(row.bank.transactionId);
    }
  }
  return ids.length ? uniqueIds(ids) : new Set();
}
function summary(record, singleDay) {
  if (!object(record) || !safeBankEvidenceId(record.id) || adjusted(record)
    || !(record.balanceReconciled === true || record.reconciled === true)
    || record.balanceReconciled === false || record.reconciled === false
    || record.transactionsReconciled === false || record.closingBalanceDeferred !== false
    || cents(record.balanceDifference) !== 0n || record.pendingApproval === true) return null;
  const source = record.statementSummary;
  if (!object(source) || source.validated !== true || adjusted(source)
    || !date(record.from) || !date(record.to) || !date(source.from) || !date(source.to)
    || record.to !== source.to || source.from > record.from || record.from > record.to
    || singleDay && (record.from !== record.to || source.from !== record.to)) return null;
  const opening = cents(source.openingBalance), closing = cents(source.closingBalance);
  const debit = cents(source.totalDebits), credit = cents(source.totalCredits);
  if ([opening, closing, debit, credit].some(value => value === null)
    || debit < 0n || credit < 0n || opening + credit - debit !== closing
    || own(record, 'closingBalance') && cents(record.closingBalance) !== closing
    || own(record, 'ledgerClosingBalance') && cents(record.ledgerClosingBalance) !== closing) return null;
  return { from: source.from, to: source.to, opening, closing, debit, credit };
}
function exactRow(id, index, record) {
  // The resolver examines every same-book observation, not merely its bounded
  // public projection. Inspect raw money too, so no normalization can round it.
  const resolved = resolveBankRowEvidence(id, index, record);
  if (!resolved.usableForBalanceChain) return null;
  let result = null;
  for (const observation of index.byId.get(id) || []) {
    const bank = observation.bank;
    const current = { id, date: date(bank.date), debit: cents(bank.debit), credit: cents(bank.credit), balance: cents(bank.balance) };
    if (!current.date || [current.debit, current.credit, current.balance].some(value => value === null)
      || current.debit < 0n || current.credit < 0n || (current.debit > 0n) === (current.credit > 0n)) return null;
    if (result && ['date', 'debit', 'credit', 'balance'].some(key => current[key] !== result[key])) return null;
    result = current;
  }
  return result;
}
function candidateRows(record, index, singleDay) {
  const financial = summary(record, singleDay), reports = reportIds(record);
  if (!financial || !reports || record.reconciliationBankCoverageComplete === false) return null;
  let ids;
  if (record.reconciliationBankCoverageComplete === true) {
    ids = uniqueIds(record.reconciliationBankTransactionIds);
    if (!ids || [...reports].some(id => !ids.has(id))) return null;
    const firstSeen = uniqueIds(index.firstSeenByImport.get(record.id));
    if (!firstSeen || !Number.isSafeInteger(record.rows) || record.rows !== firstSeen.size
      || [...firstSeen].some(id => !ids.has(id) || !reports.has(id))) return null;
  } else {
    if (singleDay) return null;
    // Legacy reports retained newly imported rows, not the complete source PDF.
    // Exact counts/sets prove that persisted subset only. The terminal boundary
    // is proved separately against ALL same-book stored cutoff-day rows below;
    // never invent or persist a full-source coverage flag for this old record.
    const firstSeen = uniqueIds(index.firstSeenByImport.get(record.id));
    if (!firstSeen || !reports.size || !Number.isSafeInteger(record.rows)
      || record.rows !== reports.size || !sameIds(firstSeen, reports)) return null;
    if (record.reconciliationBankTransactionIds != null) {
      const persisted = uniqueIds(record.reconciliationBankTransactionIds);
      if (!persisted || !sameIds(persisted, reports)) return null;
    }
    ids = reports;
  }
  const rows = new Map();
  for (const id of ids) {
    const row = exactRow(id, index, record);
    if (!row || row.date < financial.from || row.date > financial.to
      || singleDay && row.date !== financial.to) return null;
    rows.set(id, row);
  }
  return { ...financial, ids, rows };
}
function uniqueChain(candidate) {
  const next = new Map();
  let debit = 0n, credit = 0n;
  for (const row of candidate.rows.values()) {
    const predecessor = row.balance + row.debit - row.credit;
    if (next.has(predecessor)) return null;
    next.set(predecessor, row); debit += row.debit; credit += row.credit;
  }
  if (debit !== candidate.debit || credit !== candidate.credit) return null;
  const rows = [], used = new Set(), balances = new Set([candidate.opening]);
  let running = candidate.opening;
  while (rows.length < candidate.rows.size) {
    const row = next.get(running);
    if (!row || used.has(row.id) || balances.has(row.balance)) return null;
    rows.push(row); used.add(row.id); balances.add(row.balance); running = row.balance;
  }
  return running === candidate.closing ? rows : null;
}

// Additional proof only: strictly fuller persisted cutoff-day evidence. It
// neither removes historical movement coverage nor asserts absolute end-of-day
// completeness. Every other conflicting import remains independently checked.
// Scope is the supplied bank book; no other accounts, files or stores are read.
function createCutoffDayExtensionProof(book) {
  let index = null;
  const newerCache = new Map(), olderCache = new Map();
  return (candidate, earlier) => {
    try {
      if (!object(book) || !Array.isArray(book.imports) || !object(book.transactions)
        || book.imports.length > MAX_ROWS || Object.keys(book.transactions).length > MAX_ROWS
        || candidate === earlier || !book.imports.includes(candidate) || !book.imports.includes(earlier)
        || !safeBankEvidenceId(candidate.id) || !safeBankEvidenceId(earlier.id) || candidate.id === earlier.id
        || book.imports.filter(record => record && record.id === candidate.id).length !== 1
        || book.imports.filter(record => record && record.id === earlier.id).length !== 1
        || candidate.to !== earlier.to) return false;
      if (!index) {
        let observations = 0;
        for (const record of book.imports) {
          if (!object(record)) return false;
          for (const key of ['reconciliationRows', 'carriedReconciliationRows']) {
            if (record[key] != null && !Array.isArray(record[key])) return false;
            observations += (record[key] || []).length;
          }
        }
        if (observations > MAX_ROWS * 10) return false;
        index = bankEvidenceIndex(book);
      }
      if (!newerCache.has(candidate)) newerCache.set(candidate, candidateRows(candidate, index, true));
      if (!olderCache.has(earlier)) olderCache.set(earlier, candidateRows(earlier, index, false));
      const next = newerCache.get(candidate), old = olderCache.get(earlier);
      if (!next || !old) return false;
      const chain = uniqueChain(next);
      if (!chain) return false;
      // Any unknown or malformed stored cutoff row prevents this narrower proof.
      // Reading the whole same-book set prevents a legacy report's omitted
      // previously finalized same-day row from silently disappearing.
      for (const bank of Object.values(book.transactions)) {
        if (!object(bank) || !date(bank.date)) return false;
        if (bank.date === next.to && (!safeBankEvidenceId(bank.id) || !next.ids.has(bank.id))) return false;
      }
      const terminal = new Set();
      for (const row of old.rows.values()) {
        const shared = next.rows.get(row.id);
        if (shared && ['date', 'debit', 'credit', 'balance'].some(key => row[key] !== shared[key])) return false;
        if (row.date === next.to) {
          if (!shared) return false;
          terminal.add(row.id);
        } else if (row.date >= next.to) return false;
      }
      if (!terminal.size || terminal.size >= chain.length) return false;
      const prefix = chain.slice(0, terminal.size);
      return prefix.every(row => terminal.has(row.id)) && prefix.at(-1).balance === old.closing;
    } catch { return false; }
  };
}
module.exports = { createCutoffDayExtensionProof };

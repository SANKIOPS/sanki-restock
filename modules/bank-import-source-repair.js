'use strict';

const crypto = require('node:crypto');
const copy = value => JSON.parse(JSON.stringify(value));
const text = value => String(value == null ? '' : value).trim();
const digest = value => crypto.createHash('sha256').update(JSON.stringify(value)).digest('hex');
const fail = message => { const error = new Error('Statement source repair: ' + message); error.status = error.statusCode = 409; throw error; };
const draftFingerprint = draft => digest(draft);
const accountName = value => text(value).replace(/^(?:SANKI|SAMAST|PERSONAL)\|/i, '').toLowerCase().replace(/\s+/g, ' ');
const narration = value => text(value).toLowerCase().replace(/\s+/g, ' ');

function cents(value, label) {
  if (!['number', 'string'].includes(typeof value) || typeof value === 'string' && !/^-?\d+(?:\.\d{1,2})?$/.test(value.trim())) fail(label + ' is not a complete amount.');
  const number = Number(value), result = Math.round(number * 100);
  if (!Number.isFinite(number) || !Number.isSafeInteger(result) || Math.abs(number * 100 - result) > 1e-6) fail(label + ' is invalid.');
  return result;
}

function validDate(value) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(text(value))) return false;
  const date = new Date(value + 'T00:00:00Z');
  return Number.isFinite(date.getTime()) && date.toISOString().slice(0, 10) === value;
}

function strongReference(row) {
  if (!row || typeof row !== 'object') return '';
  const found = new Set();
  for (const field of ['reference', 'bankReference', 'transactionReference']) if (/^\d{12}$/.test(text(row[field]))) found.add(text(row[field]));
  for (const match of text(row.description).matchAll(/\b(?:UPI|IMPS\/P2[AM])\/([\d\s]{12,})(?=\/|\s|$)/gi)) {
    const reference = match[1].replace(/\s/g, '');
    if (/^\d{12}$/.test(reference)) found.add(reference);
  }
  if (found.size > 1) fail('conflicting transaction references in a source row.');
  return [...found][0] || '';
}

function rowMoney(row) {
  if (!row || !validDate(row.date)) fail('a transaction date is invalid.');
  const debit = cents(row.debit, 'Debit'), credit = cents(row.credit, 'Credit'), balance = cents(row.balance, 'Balance');
  if (debit < 0 || credit < 0 || !(debit > 0 || credit > 0) || debit > 0 && credit > 0) fail('a row must have one positive debit or credit.');
  return { debit, credit, balance };
}

function financialKey(row, money) {
  return JSON.stringify([row.date, money.debit, money.credit, money.balance, text(row.reference), narration(row.description)]);
}

function verifySummary(rows, summary, source) {
  if (!summary || summary.validated !== true || !validDate(summary.from) || !validDate(summary.to) || summary.from > summary.to) fail(source + ' period is missing or unvalidated.');
  const total = rows.reduce((result, row) => {
    const money = rowMoney(row);
    if (row.date < summary.from || row.date > summary.to) fail(source + ' row is outside its declared period.');
    result.debits += money.debit; result.credits += money.credit; return result;
  }, { debits: 0, credits: 0 });
  const opening = cents(summary.openingBalance, source + ' opening'), closing = cents(summary.closingBalance, source + ' closing');
  if (total.debits !== cents(summary.totalDebits, source + ' debits') || total.credits !== cents(summary.totalCredits, source + ' credits') || opening + total.credits - total.debits !== closing) fail(source + ' totals do not balance.');
  return { ...total, opening, closing };
}

function containsRow(value, rowIds) {
  if (typeof value === 'string') return rowIds.has(value);
  if (Array.isArray(value)) return value.some(item => containsRow(item, rowIds));
  if (value && typeof value === 'object') return Object.entries(value).some(([key, item]) => rowIds.has(key) || containsRow(item, rowIds));
  return false;
}

function blockedEvidence(store, draft, changes) {
  const rowIds = new Set(changes.map(change => change.rowId)), references = new Set(changes.map(change => change.reference));
  const matchesReference = value => {
    if (typeof value === 'string') return [...references].some(reference => new RegExp('(?:^|\\D)' + reference + '(?:\\D|$)').test(value));
    if (Array.isArray(value)) return value.some(matchesReference);
    if (value && typeof value === 'object') return Object.values(value).some(matchesReference);
    return false;
  };
  for (const [id, resolution] of Object.entries(draft.resolutions || {})) if (rowIds.has(id) || containsRow(resolution, rowIds)) fail('an affected row already has a saved or grouped decision; undo it first.');
  for (const [id, exclusion] of Object.entries(draft.matchingExclusions || {})) if (rowIds.has(id) || containsRow(exclusion, rowIds)) fail('an affected row has a matching exclusion; review it first.');
  if (Object.values(store.bankReconciliationApprovals || {}).some(approval => approval && approval.draftId === draft.id && approval.status === 'pending')) fail('withdraw the pending Owner approval before repairing the statement.');

  // Amounts and balances are deliberately absent from this finalized identity
  // guard: a bad imported amount must not disguise an already-finalized row.
  for (const [key, book] of Object.entries(store.bankStatements || {})) {
    if (accountName(key) !== accountName(draft.account) && accountName(book.account) !== accountName(draft.account)) continue;
    if (Object.values(book.transactions || {}).some(bank => references.has(strongReference(bank)))) fail('an affected bank transaction is already stored in finalized history.');
    for (const imported of book.imports || []) {
      if (imported.draftId === draft.id) fail('this statement already has a finalized import.');
      for (const row of [].concat(imported.reconciliationRows || [], imported.carriedReconciliationRows || [])) if (row.bank && references.has(strongReference(row.bank))) fail('an affected transaction is already present in finalized reconciliation evidence.');
    }
  }

  const linked = record => {
    if (!record || typeof record !== 'object') return false;
    const evidence = record.bankReconciliationEvidence || record.reconciliationSource || {};
    const origin = text(record.reconciliationDraft || record.bankReconciliationDraft || record.reconciliationDraftId || evidence.draftId);
    const rowId = text(record.bankRowId || record.bankReconciliationRow || evidence.rowId);
    if (origin === draft.id && (rowIds.has(rowId) || containsRow(record.bankTransactions, rowIds) || containsRow(record.bankReferences, rowIds) || matchesReference(record.bankReference) || matchesReference(record.bankReferences) || matchesReference(evidence))) return true;
    if (origin === draft.id && !rowId && !record.bankTransactions && !record.bankReferences && !record.bankReference && !evidence.reference) return true; // Unattributed provenance cannot safely be assumed unrelated.
    const other = (store.bankReconciliationDrafts || {})[origin];
    const sameAccount = [record.account, record.bankAccount, record.fromAccount, record.toAccount, other && other.account].some(account => accountName(account) === accountName(draft.account));
    return !!origin && sameAccount && matchesReference({ reference: record.bankReference, references: record.bankReferences, evidence, transactions: record.bankTransactions });
  };
  for (const name of ['transfers', 'adjustments', 'receipts', 'vendorAdvances', 'vendorOpeningPayables', 'bankTruthMovements', 'reconciliationExpenses', 'paytmSettlements']) if ((store[name] || []).some(linked)) fail('an affected row has a linked or posted accounting source; no bank amount was changed.');
  for (const expense of Object.values(store.expenses || {})) if (linked(expense) || [].concat(expense.payments || [], expense.reimbursementPayments || []).some(linked)) fail('an affected row has an expense or payment source; no bank amount was changed.');
  for (const name of ['bankDateOverrides', 'bankReconciliationLinks']) if (Object.values(store[name] || {}).some(linked)) fail('an affected row has an existing bank-date or grouped ledger link.');
}

/** Pure planner. The caller must hash the retained bytes and use the positioned
 * parser. No bank/ledger record is edited; nextDraft retains all original indexes. */
function planBankImportSourceRepair(options = {}) {
  const { store = {}, draft, parsedRows, sourceHash, expectedSourceHash, expectedDraftFingerprint, reviewRows } = options;
  if (!draft || !draft.id || !draft.account || !Array.isArray(draft.transactions) || !draft.transactions.length) fail('the saved statement draft is missing.');
  const currentFingerprint = draftFingerprint(draft);
  if (expectedDraftFingerprint && expectedDraftFingerprint !== currentFingerprint) fail('the draft changed while its retained source was being parsed.');
  const saved = (store.bankReconciliationDrafts || {})[draft.id];
  if (saved && draftFingerprint(saved) !== currentFingerprint) fail('the supplied draft is stale.');
  if (draft.originalStatementPeriod || draft.reconstructedFromFinalized || (draft.sourceStatements || []).length > 1) fail('merged, reconstructed or recovery-locked statements require individual review.');
  if (draft.openingResolution) fail('undo the reviewed opening-balance correction before repairing this statement.');
  const retained = (draft.sourceStatements || [])[0] || draft;
  if (![sourceHash, expectedSourceHash, draft.fileHash, retained.fileHash].every(hash => /^[a-f0-9]{64}$/i.test(text(hash))) || new Set([sourceHash, expectedSourceHash, draft.fileHash, retained.fileHash].map(hash => hash.toLowerCase())).size !== 1) fail('the retained statement hash is missing or changed.');
  if (!Array.isArray(parsedRows) || parsedRows.length !== draft.transactions.length) fail('the reparsed statement has a different row count.');
  const summary = parsedRows.statementSummary;
  if (!summary || summary.format !== 'IndusInd mobile PDF' || summary.parserVersion !== 'positioned-cells-v1') fail('verified original PDF column positions are required.');
  const accountLast4 = (text(draft.account).match(/(\d{4})(?!.*\d)/) || [])[1];
  if (!accountLast4 || summary.accountLast4 !== accountLast4 || draft.summary && draft.summary.accountLast4 && draft.summary.accountLast4 !== accountLast4) fail('the source account does not match the selected bank.');
  const beforeFinancial = verifySummary(draft.transactions, draft.summary, 'Saved statement'), afterFinancial = verifySummary(parsedRows, summary, 'Reparsed statement');
  if (summary.from !== draft.summary.from || summary.to !== draft.summary.to || beforeFinancial.closing !== afterFinancial.closing) fail('the statement period or closing balance changed.');

  // Every physical balance must be independently reproduced from positioned
  // amounts. This is not the circular inferred-opening aggregate validation.
  let running = afterFinancial.opening;
  for (const row of parsedRows.slice().reverse()) {
    const amount = rowMoney(row); running += amount.credit - amount.debit;
    if (running !== amount.balance) fail('reparsed transaction amounts do not reproduce each running balance.');
  }
  if (running !== afterFinancial.closing || parsedRows.some((row, index) => index && parsedRows[index - 1].date < row.date)) fail('the reparsed statement is not in verified newest-first order.');

  const sourceMap = new Map();
  const keyFor = row => {
    const amount = rowMoney(row), reference = strongReference(row);
    return reference ? JSON.stringify([row.date, reference, amount.balance]) : financialKey(row, amount);
  };
  for (const row of parsedRows) { const key = keyFor(row); if (sourceMap.has(key)) fail('source rows are ambiguous; no unique row mapping exists.'); sourceMap.set(key, row); }
  const used = new Set(), changes = [], nextDraft = copy(draft);
  draft.transactions.forEach((row, index) => {
    const key = keyFor(row), parsed = sourceMap.get(key);
    if (!parsed || used.has(key)) fail('saved rows cannot be mapped uniquely to the retained statement.');
    used.add(key);
    const before = rowMoney(row), after = rowMoney(parsed);
    if (before.debit === after.debit && before.credit === after.credit) return;
    // Positioned extraction may omit an old page-header prefix from another,
    // financially unchanged row. Keep that saved row completely untouched.
    // A row whose money changes still requires matching narration as well as
    // its strong reference/date/balance identity.
    if (narration(row.description) !== narration(parsed.description)) fail('transaction narration changed; review its physical identity manually.');
    if (!strongReference(row) || (before.debit > 0) !== (after.debit > 0)) fail('a changed row lacks a strong reference or changes debit/credit direction.');
    const rowId = 'bank-' + index;
    if (reviewRows) {
      const view = reviewRows.find(value => value.id === rowId);
      if (!view || !['missing_in_app', 'missing_in_ledger', 'amount_mismatch', 'unmatched'].includes(view.status) || view.resolution) fail('the affected row is not unresolved in the current review.');
    }
    const replacement = { ...row, debit: after.debit / 100, credit: after.credit / 100 };
    if (parsed.bankReference != null) replacement.bankReference = parsed.bankReference;
    nextDraft.transactions[index] = replacement;
    changes.push({ rowId, index, reference: strongReference(row), before: copy(row), after: copy(replacement) });
  });
  if (used.size !== sourceMap.size) fail('the source row mapping is incomplete.');
  if (changes.length) blockedEvidence(store, draft, changes);
  nextDraft.summary = changes.length ? { ...draft.summary, ...copy(summary) } : copy(draft.summary);
  const plan = { changed: changes.length > 0, changes, sourceHash: sourceHash.toLowerCase(), draftFingerprint: currentFingerprint, storeFingerprint: digest(store), beforeSummary: copy(draft.summary), afterSummary: copy(nextDraft.summary), nextDraft };
  plan.fingerprint = digest({ draftFingerprint: plan.draftFingerprint, storeFingerprint: plan.storeFingerprint, sourceHash: plan.sourceHash, changes: plan.changes, afterSummary: plan.afterSummary });
  return plan;
}

/** Re-plan against a fresh store; return a draft only, never mutate accounting. */
function applyBankImportSourceRepair(options = {}) {
  const plan = planBankImportSourceRepair(options);
  if (!options.expectedFingerprint || options.expectedFingerprint !== plan.fingerprint) fail('the reviewed source or accounting evidence changed; preview the repair again.');
  if (!plan.changed) return plan;
  if (!text(options.actor) || !text(options.reason)) fail('an actor and repair reason are required.');
  const at = options.at || new Date().toISOString();
  if (!Number.isFinite(Date.parse(at))) fail('the audit timestamp is invalid.');
  const audit = { type: 'bank_import_source_amount_repair', sourceHash: plan.sourceHash, parserVersion: 'positioned-cells-v1', by: text(options.actor), at, reason: text(options.reason), beforeSummary: plan.beforeSummary, afterSummary: plan.afterSummary, changes: plan.changes, fingerprint: plan.fingerprint, ledgerRecordsUnchanged: true };
  plan.nextDraft.sourceAmountRepairHistory = [].concat(plan.nextDraft.sourceAmountRepairHistory || [], audit);
  plan.nextDraft.sourceAmountRepair = audit;
  return plan;
}

module.exports = { planBankImportSourceRepair, applyBankImportSourceRepair, draftFingerprint };

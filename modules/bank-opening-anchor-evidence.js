'use strict';

// Evidence only: use the existing selector without repairing history, choosing
// a different closing, reading source files, or creating ledger movements.
const { verifiedReconciliationAnchor } = require('./reconciliation-opening');
const LIMITS = Object.freeze({ candidates: 50, coverageIds: 1000, lineageIds: 100, sources: 20 });
const list = value => Array.isArray(value) ? value : [];
const own = (value, key) => !!value && Object.prototype.hasOwnProperty.call(value, key);
const scalar = value => typeof value === 'string' ? value.slice(0, 256)
  : typeof value === 'boolean' || typeof value === 'number' && Number.isFinite(value) || value === null ? value : undefined;
function fields(value, names) {
  return Object.fromEntries(names.filter(key => own(value, key) && scalar(value[key]) !== undefined)
    .map(key => [key, scalar(value[key])]));
}
const SUMMARY_FIELDS = ['from', 'to', 'openingBalance', 'closingBalance', 'totalDebit', 'totalCredit', 'totalDebits', 'totalCredits', 'debit', 'credit', 'count', 'rows', 'validated'];
function date(value) {
  if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(value)) return '';
  const parsed = new Date(value + 'T00:00:00Z');
  return Number.isFinite(parsed.getTime()) && parsed.toISOString().slice(0, 10) === value ? value : '';
}
function period(record) {
  const values = [record.to, record.statementSummary && record.statementSummary.to]
    .filter(value => value != null && String(value).trim() !== ''), dates = values.map(date);
  return { dates: [...new Set(dates.filter(Boolean))], contradictory: dates.some(value => !value) || new Set(dates).size > 1 };
}
function ids(values, limit) {
  const unique = [...new Set(values.filter(value => ['string', 'number'].includes(typeof value))
    .map(String).filter(value => value.trim()))].sort();
  // A clipped identity must never look like a complete coverage proof.
  const tooLong = unique.some(value => value.length > 256);
  return { ids: unique.slice(0, limit).map(value => value.slice(0, 256)), count: unique.length,
    truncated: unique.length > limit || tooLong };
}
function retainedSource(record) {
  const hash = own(record, 'fileHash') ? record.fileHash : record.hash;
  return { retentionMetadataPresent: ['file', 'temporaryFile'].some(key => typeof record[key] === 'string' && !!record[key].trim()),
    hashPresent: hash != null && String(hash).trim() !== '',
    sha256: typeof hash === 'string' && /^[a-f0-9]{64}$/i.test(hash) ? hash.toLowerCase() : null,
    contentVerified: false };
}
function sourceEvidence(record) {
  const sources = list(record.sourceStatements);
  return { ...retainedSource(record), sourceCount: sources.length, sourcesTruncated: sources.length > LIMITS.sources,
    sources: sources.slice(0, LIMITS.sources).map(source => ({ ...fields(source, ['from', 'to']), ...retainedSource(source || {}) })) };
}
function closingEvidence(record) {
  return { ...fields(record, ['companyAdjustedClosingBalance', 'closingBalance', 'ledgerClosingBalance',
    'balanceReconciled', 'reconciled', 'balanceDifference', 'closingBalanceDeferred']),
    statementSummary: fields(record.statementSummary, SUMMARY_FIELDS) };
}
function candidate(record, anchorRecordId) {
  const rows = [...list(record.reconciliationRows), ...list(record.carriedReconciliationRows)];
  const persisted = ids(list(record.reconciliationBankTransactionIds), LIMITS.coverageIds);
  const all = ids([...list(record.reconciliationBankTransactionIds), ...rows.map(row => row && row.bank && row.bank.transactionId)], LIMITS.coverageIds);
  const lineage = ids(list(record.combinedReconciliationIds), LIMITS.lineageIds);
  return { ...fields(record, ['id', 'draftId', 'from', 'to', 'rows', 'uploadedAt', 'finalizedAt']),
    ...closingEvidence(record), period: period(record), selected: !!anchorRecordId && String(record.id || '') === anchorRecordId,
    coverage: { ...fields(record, ['reconciliationBankCoverageComplete']), physicalBankTransactionIds: all.ids,
      physicalBankTransactionCount: all.count, physicalBankTransactionIdsTruncated: all.truncated,
      persistedBankTransactionIds: persisted.ids, persistedBankTransactionCount: persisted.count,
      persistedBankTransactionIdsTruncated: persisted.truncated,
      reconciliationRowCount: list(record.reconciliationRows).length, carriedRowCount: list(record.carriedReconciliationRows).length },
    combinedReconciliationIds: lineage.ids, combinedReconciliationCount: lineage.count,
    combinedReconciliationIdsTruncated: lineage.truncated, retainedSource: sourceEvidence(record) };
}
function snapshot(store, options) {
  const { account, nature, bookKey, draftId } = options;
  if (![account, draftId].every(value => typeof value === 'string' && value.trim() === value && value.length > 0 && value.length <= 256)
    || !['SANKI', 'SAMAST', 'PERSONAL'].includes(nature)) return { error: 'Select an exact account, entity and saved draft.', status: 400 };
  const drafts = store.bankReconciliationDrafts || {}, draft = own(drafts, draftId) && drafts[draftId];
  if (!draft || draft.id !== draftId || draft.account !== account || draft.nature !== nature) {
    return { error: 'The saved draft does not belong to this account and entity.', status: 404 };
  }
  const book = own(store.bankStatements, bookKey) ? store.bankStatements[bookKey] : {}, summary = draft.summary || {};
  if (!book || typeof book !== 'object' || Array.isArray(book) || book.imports != null && !Array.isArray(book.imports)
    || list(book.imports).some(record => !record || typeof record !== 'object' || Array.isArray(record)
      || ['reconciliationBankTransactionIds', 'combinedReconciliationIds', 'reconciliationRows', 'carriedReconciliationRows', 'sourceStatements']
        .some(key => record[key] != null && !Array.isArray(record[key])))) {
    return { error: 'The saved opening-anchor evidence cannot be interpreted safely. Review its retained sources; no records were changed.', status: 409 };
  }
  const continuityCutoff = String(book.reconciledThrough || '');
  const anchorBefore = summary.from && summary.to && continuityCutoff >= summary.from && continuityCutoff < summary.to ? summary.to : summary.from;
  if (!date(anchorBefore)) return { error: 'The saved statement has no valid opening-anchor period. Review its original source; no records were changed.', status: 409 };
  const anchor = verifiedReconciliationAnchor(book, { before: anchorBefore, nature, account });
  const candidateDate = anchor.anchorConflict && anchor.anchorConflict.cutoffDate || anchor.cutoffDate || '';
  const imports = list(book.imports).filter(record => record && typeof record === 'object');
  const candidates = imports.filter(record => period(record).dates.includes(candidateDate))
    .sort((a, b) => String(a.id || '').localeCompare(String(b.id || '')));
  const last = book.lastReconciliation || {}, lastMatches = imports.filter(record => last.draftId != null && String(last.draftId)
    && String(record.draftId || '') === String(last.draftId));
  const matches = ids(lastMatches.map(record => record.id), LIMITS.candidates);
  return { success: true, readOnly: true, account, nature, draftId, statementSummary: fields(summary, SUMMARY_FIELDS),
    retainedSource: sourceEvidence(draft), continuityCutoff: scalar(continuityCutoff), anchorBefore,
    anchor: { ...fields(anchor, ['cutoffDate', 'previousClosing', 'anchorSource', 'anchorRecordId']),
      ...(anchor.anchorConflict ? { anchorConflict: fields(anchor.anchorConflict, ['type', 'cutoffDate', 'message']) } : {}) },
    candidateDate, candidateCount: candidates.length, candidatesTruncated: candidates.length > LIMITS.candidates,
    candidates: candidates.slice(0, LIMITS.candidates).map(record => candidate(record, anchor.anchorRecordId)),
    lastReconciliation: book.lastReconciliation ? { ...fields(last, ['draftId', 'through', 'at', 'transactionsReconciled']),
      ...closingEvidence(last), matchingImportIds: matches.ids, matchingImportCount: matches.count,
      matchingImportIdsTruncated: matches.truncated,
      matchingImportPeriods: lastMatches.slice(0, LIMITS.candidates).map(record => ({ ...fields(record, ['id', 'to']), period: period(record) })) } : null,
    limits: { ...LIMITS },
    explanation: 'Read-only stored evidence. Hashes and retention flags are metadata, not verification of source-file contents. Missing or truncated coverage is not proof of supersession. Existing reconciliation locks remain unchanged.' };
}
module.exports = { snapshot, LIMITS };

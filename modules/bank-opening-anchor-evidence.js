'use strict';

// Evidence only: use the existing selector without repairing history, choosing
// a different closing, reading source files, or creating ledger movements.
const { verifiedReconciliationAnchor } = require('./reconciliation-opening');
const { bankEvidenceIndex, resolveBankRowEvidence, safeBankEvidenceId } = require('./bank-opening-row-evidence');
const LIMITS = Object.freeze({ candidates: 50, coverageIds: 1000, lineageIds: 100, sources: 20, rowObservations: 4, totalRowFacts: 1000 });
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
  // Never project malformed identity text (which could contain a path or PII),
  // or let omitted identities look like complete coverage proof.
  const invalidCount = unique.filter(value => !safeBankEvidenceId(value)).length;
  return { ids: unique.slice(0, limit).filter(safeBankEvidenceId), unclippedIds: unique.slice(0, limit), count: unique.length,
    invalidCount, truncated: unique.length > limit || invalidCount > 0 };
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
function rowEvidence(id, index, candidateRecord, cutoffDate) {
  const row = resolveBankRowEvidence(id, index, candidateRecord, LIMITS.rowObservations);
  return { ...row, dateRelationToCutoff: row.facts ? row.facts.date < cutoffDate ? 'before'
    : row.facts.date === cutoffDate ? 'same_day' : 'after' : 'unverified' };
}
function candidate(record, anchorRecordId, evidenceIndex, cutoffDate, budget) {
  const rows = [...list(record.reconciliationRows), ...list(record.carriedReconciliationRows)];
  const persisted = ids(list(record.reconciliationBankTransactionIds), LIMITS.coverageIds);
  const all = ids([...list(record.reconciliationBankTransactionIds), ...rows.map(row => row && row.bank && row.bank.transactionId)], LIMITS.coverageIds);
  const lineage = ids(list(record.combinedReconciliationIds), LIMITS.lineageIds);
  const projectedIds = all.unclippedIds.slice(0, budget.remaining);
  budget.remaining -= projectedIds.length;
  const rowFacts = projectedIds.map(id => rowEvidence(id, evidenceIndex, record, cutoffDate));
  const rowFactsTruncated = all.truncated || projectedIds.length < all.count;
  const firstSeen = evidenceIndex.firstSeenByImport.get(record.id) || [], firstSeenIds = ids(firstSeen, LIMITS.coverageIds);
  return { ...fields(record, ['id', 'draftId', 'from', 'to', 'rows', 'uploadedAt', 'finalizedAt']),
    ...closingEvidence(record), period: period(record), selected: !!anchorRecordId && String(record.id || '') === anchorRecordId,
    coverage: { ...fields(record, ['reconciliationBankCoverageComplete']), physicalBankTransactionIds: all.ids,
      physicalBankTransactionCount: all.count, physicalBankTransactionIdsTruncated: all.truncated,
      invalidPhysicalBankTransactionIdCount: all.invalidCount,
      persistedBankTransactionIds: persisted.ids, persistedBankTransactionCount: persisted.count,
      persistedBankTransactionIdsTruncated: persisted.truncated,
      reconciliationRowCount: list(record.reconciliationRows).length, carriedRowCount: list(record.carriedReconciliationRows).length,
      firstSeenStoredRowCount: firstSeen.length, firstSeenBankTransactionIds: firstSeenIds.ids,
      firstSeenBankTransactionCount: firstSeenIds.count, firstSeenBankTransactionIdsTruncated: firstSeenIds.truncated,
      invalidFirstSeenBankTransactionIdCount: firstSeenIds.invalidCount,
      firstSeenRowsMissingId: firstSeen.filter(id => typeof id !== 'string' || !id.trim()).length,
      physicalBankRows: rowFacts, rowFactsTruncated,
      allRowFactsUsable: !rowFactsTruncated && rowFacts.length > 0 && rowFacts.every(row => row.usableForBalanceChain && !row.observationsTruncated),
      cutoffDayBankTransactionIds: rowFacts.filter(row => row.dateRelationToCutoff === 'same_day').map(row => row.id),
      earlierDayBankTransactionIds: rowFacts.filter(row => row.dateRelationToCutoff === 'before').map(row => row.id),
      afterCutoffBankTransactionIds: rowFacts.filter(row => row.dateRelationToCutoff === 'after').map(row => row.id),
      unresolvedBankTransactionIds: rowFacts.filter(row => !row.usableForBalanceChain).map(row => row.id),
      ordering: 'Identifier display order is not transaction order. Retained row numbers/indexes may belong to different statements; fact usability and first-seen provenance do not assert complete period coverage, a common source order or a running-balance chain.' },
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
    || book.transactions != null && (typeof book.transactions !== 'object' || Array.isArray(book.transactions))
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
  const evidenceIndex = bankEvidenceIndex(book), budget = { remaining: LIMITS.totalRowFacts };
  const last = book.lastReconciliation || {}, lastMatches = imports.filter(record => last.draftId != null && String(last.draftId)
    && String(record.draftId || '') === String(last.draftId));
  const matches = ids(lastMatches.map(record => record.id), LIMITS.candidates);
  return { success: true, readOnly: true, account, nature, draftId, statementSummary: fields(summary, SUMMARY_FIELDS),
    retainedSource: sourceEvidence(draft), continuityCutoff: scalar(continuityCutoff), anchorBefore,
    anchor: { ...fields(anchor, ['cutoffDate', 'previousClosing', 'anchorSource', 'anchorRecordId']),
      ...(anchor.anchorConflict ? { anchorConflict: fields(anchor.anchorConflict, ['type', 'cutoffDate', 'message']) } : {}) },
    candidateDate, candidateCount: candidates.length, candidatesTruncated: candidates.length > LIMITS.candidates,
    candidates: candidates.slice(0, LIMITS.candidates).map(record => candidate(record, anchor.anchorRecordId, evidenceIndex, candidateDate, budget)),
    lastReconciliation: book.lastReconciliation ? { ...fields(last, ['draftId', 'through', 'at', 'transactionsReconciled']),
      ...closingEvidence(last), matchingImportIds: matches.ids, matchingImportCount: matches.count,
      matchingImportIdsTruncated: matches.truncated,
      matchingImportPeriods: lastMatches.slice(0, LIMITS.candidates).map(record => ({ ...fields(record, ['id', 'to']), period: period(record) })) } : null,
    limits: { ...LIMITS },
    explanation: 'Read-only stored evidence. Hashes, retention flags, balances and source positions are retained metadata, not a fresh verification of source-file contents or row order. Missing, conflicting or truncated evidence is not proof of supersession. Existing reconciliation locks remain unchanged.' };
}
module.exports = { snapshot, LIMITS };

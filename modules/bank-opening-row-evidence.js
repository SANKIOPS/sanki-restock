'use strict';

// Pure, same-book evidence resolution. This does not select an opening balance,
// infer bank order, read retained files, or mutate any source row.
const list = value => Array.isArray(value) ? value : [];
const safeId = value => typeof value === 'string' && /^[A-Za-z0-9_-]{1,256}$/.test(value) ? value : null;
function calendarDate(value) {
  if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(value)) return null;
  const parsed = new Date(value + 'T00:00:00Z');
  return Number.isFinite(parsed.getTime()) && parsed.toISOString().slice(0, 10) === value ? value : null;
}
function money(value) {
  if (!['number', 'string'].includes(typeof value) || typeof value === 'number' && !Number.isFinite(value)) return null;
  const raw = String(value).trim();
  if (raw.length > 32) return null;
  const match = /^(-?)(\d+)(?:\.(\d{1,2}))?$/.exec(raw);
  if (!match) return null;
  const absolutePaise = BigInt(match[2]) * 100n + BigInt((match[3] || '').padEnd(2, '0'));
  if (absolutePaise > BigInt(Number.MAX_SAFE_INTEGER)) return null;
  return Number(match[1] ? -absolutePaise : absolutePaise) / 100;
}
function bankFacts(bank) {
  return { date: calendarDate(bank.date), debit: money(bank.debit), credit: money(bank.credit), balance: money(bank.balance) };
}
function factIssues(facts) {
  const issues = Object.keys(facts).filter(key => facts[key] === null).map(key => 'missing_or_invalid_' + key);
  if (facts.debit !== null && facts.credit !== null && (facts.debit < 0 || facts.credit < 0 || (facts.debit > 0) === (facts.credit > 0))) issues.push('invalid_direction');
  return issues;
}
function bankEvidenceIndex(book) {
  const byId = new Map(), firstSeenByImport = new Map();
  const add = (id, observation) => {
    if (typeof id !== 'string' || !id.trim()) return;
    if (!byId.has(id)) byId.set(id, []);
    byId.get(id).push(observation);
  };
  Object.entries(book.transactions || {}).forEach(([key, bank]) => {
    if (!bank || typeof bank !== 'object' || Array.isArray(bank)) return;
    // Canonical production storage keys are the 24-character hash suffix.
    // Legacy noncanonical keys supply no independent key/identity proof.
    const expectedId = /^[a-f0-9]{24}$/i.test(key) ? 'BTX-' + key : null;
    const observation = { kind: 'stored', bank, keyIdentityConflict: !!expectedId && bank.id !== expectedId };
    add(bank.id, observation);
    if (observation.keyIdentityConflict) add(expectedId, observation);
    if (typeof bank.firstSeenImport === 'string') {
      if (!firstSeenByImport.has(bank.firstSeenImport)) firstSeenByImport.set(bank.firstSeenImport, []);
      firstSeenByImport.get(bank.firstSeenImport).push(bank.id);
    }
  });
  list(book.imports).forEach(record => ['reconciliationRows', 'carriedReconciliationRows'].forEach(kind => {
    list(record && record[kind]).forEach(row => {
      if (row && row.bank && typeof row.bank === 'object' && !Array.isArray(row.bank)) add(row.bank.transactionId, { kind, bank: row.bank, row, record });
    });
  }));
  return { byId, firstSeenByImport };
}
function observationProjection(observation, candidateRecord) {
  const { bank, row, record, kind } = observation, sourcePosition = {}, provenance = {};
  for (const key of ['row', 'sourcePage', 'sourceIndex', 'reconciliationDraftRowIndex']) {
    if (Number.isSafeInteger(bank[key]) && bank[key] >= 0) sourcePosition[key] = bank[key];
  }
  if (safeId(bank.reconciliationDraftId)) {
    sourcePosition.reconciliationDraftId = bank.reconciliationDraftId;
    sourcePosition.matchesCandidateDraft = bank.reconciliationDraftId === candidateRecord.draftId;
  }
  if (row && typeof row.id === 'string' && /^bank-\d+$/.test(row.id) && row.id.length <= 256) sourcePosition.retainedReportRowId = row.id;
  for (const key of ['firstSeenImport', 'lastSeenImport']) if (safeId(bank[key])) provenance[key] = bank[key];
  const facts = bankFacts(bank);
  return { kind, ...(record ? { recordId: safeId(record.id), belongsToCandidate: record === candidateRecord } : {}),
    facts, sourcePosition, provenance, issues: factIssues(facts),
    ...(observation.keyIdentityConflict ? { keyIdentityConflict: true } : {}) };
}
function resolveBankRowEvidence(id, index, candidateRecord, maxObservations = 4) {
  const observations = index.byId.get(id) || [], stored = observations.filter(item => item.kind === 'stored');
  const reports = observations.filter(item => item.kind !== 'stored'), issues = [], conflictingFields = [];
  if (!safeId(id)) issues.push('invalid_identity');
  if (!stored.length) issues.push('missing_stored_row');
  if (stored.length > 1) issues.push('duplicate_stored_id');
  if (stored.some(item => item.keyIdentityConflict)) issues.push('stored_key_identity_mismatch');
  const factSets = observations.map(item => bankFacts(item.bank));
  if (factSets.some(facts => factIssues(facts).length)) issues.push('incomplete_or_invalid_row_facts');
  for (const key of ['date', 'debit', 'credit', 'balance']) {
    if (new Set(factSets.map(facts => facts[key]).filter(value => value !== null)).size > 1) conflictingFields.push(key);
  }
  // Compare physical identity internally without returning bank narration or
  // references in the balance-only evidence DTO.
  const reference = value => String(value || '').toLowerCase().replace(/[^a-z0-9]/g, '');
  const narration = value => String(value || '').toLowerCase().replace(/\s+/g, ' ').trim();
  for (const [key, normalize] of [['reference', reference], ['description', narration]]) {
    if (new Set(observations.map(item => normalize(item.bank[key])).filter(Boolean)).size > 1) conflictingFields.push(key);
  }
  if (conflictingFields.length) issues.push('conflicting_evidence');
  const usableForBalanceChain = issues.length === 0;
  return { id: safeId(id), usableForBalanceChain, facts: usableForBalanceChain ? bankFacts(stored[0].bank) : null,
    issues, conflictingFields, storedMatchCount: stored.length, retainedReportMatchCount: reports.length,
    candidateReportMatchCount: reports.filter(item => item.record === candidateRecord).length,
    observations: observations.slice(0, maxObservations).map(item => observationProjection(item, candidateRecord)),
    observationCount: observations.length, observationsTruncated: observations.length > maxObservations };
}
module.exports = { bankEvidenceIndex, resolveBankRowEvidence, safeBankEvidenceId: safeId };

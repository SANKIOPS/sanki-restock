'use strict';

// An archive is a retained duplicate-review record, never a financial reversal.
const crypto = require('crypto');
const drafts = require('./credit-card-review-drafts');
const clone = value => JSON.parse(JSON.stringify(value));
function fail(message) { const error = new Error(message); error.status = 409; throw error; }
function fingerprint(statement) {
  const value = clone(statement);
  delete value.status; delete value.reviewRevision; delete value.reviewArchive;
  return crypto.createHash('sha256').update(JSON.stringify(value)).digest('hex');
}
function assertArchivable(options) {
  drafts.assertDiscardable(options);
  if (!options.statement.rows.every(row => row.duplicateOf)) fail('Only a wholly duplicated, never-posted review can be archived. Unmatched transactions must stay pending.');
  return true;
}
function assertRestorable(options) {
  const statement = options.statement, entry = statement && statement.reviewArchive;
  const keys = ['version', 'archivedAt', 'archivedBy', 'reason', 'reviewRevision', 'preservedReviewHash'];
  if (!statement || statement.status !== 'archived' || !entry || typeof entry !== 'object' || Array.isArray(entry) || Object.keys(entry).length !== keys.length || Object.keys(entry).some(key => !keys.includes(key)) || entry.version !== 1 || typeof entry.archivedAt !== 'string' || !Number.isFinite(Date.parse(entry.archivedAt)) || typeof entry.archivedBy !== 'string' || !entry.archivedBy.trim() || typeof entry.reason !== 'string' || entry.reason.trim().length < 10 || entry.reason.length > 2000 || !Number.isSafeInteger(entry.reviewRevision) || entry.reviewRevision < 0 || statement.reviewRevision !== entry.reviewRevision + 1 || entry.preservedReviewHash !== fingerprint(statement)) fail('The retained duplicate-review archive cannot be verified. No review was restored.');
  const audit = (options.cards.audit || []).filter(item => item.subjectId === statement.id).at(-1);
  if (!audit || audit.action !== 'STATEMENT_REVIEW_ARCHIVED' || audit.by !== entry.archivedBy || audit.at !== entry.archivedAt || !audit.details || audit.details.reason !== entry.reason || audit.details.reviewRevision !== statement.reviewRevision || !audit.details.after || JSON.stringify(audit.details.after) !== JSON.stringify(statement)) fail('The archive audit and retained review do not agree. No review was restored.');
  // The duplicate-provenance, incoming-dependency, privacy and closed-period
  // checks run again today, using only a detached review-shaped validation copy.
  const review = clone(statement); review.status = 'review';
  const cards = { ...options.cards, statements: { ...options.cards.statements, [statement.id]: review } };
  assertArchivable({ ...options, cards, statement: review });
  return true;
}
function archive(statement, by, reason, at) {
  const next = clone(statement);
  next.status = 'archived'; next.reviewRevision = (statement.reviewRevision || 0) + 1;
  next.reviewArchive = { version: 1, archivedAt: at, archivedBy: by, reason, reviewRevision: statement.reviewRevision || 0, preservedReviewHash: fingerprint(statement) };
  return next;
}
function restore(statement) {
  const next = clone(statement); next.status = 'review';
  next.reviewRevision = statement.reviewRevision + 1;
  // The complete archive record remains in the append-only card audit.
  delete next.reviewArchive;
  return next;
}
module.exports = { assertArchivable, assertRestorable, archive, restore };

'use strict';

// Review-only helpers: never post, reconcile or rebuild merchant rules.
const guards = require('./source-correction-guards');
const FIELDS = ['classification', 'type', 'category', 'nature', 'channel'];
const NATURES = ['SANKI', 'SAMAST', 'PERSONAL'];
const clone = value => JSON.parse(JSON.stringify(value));
const own = (value, key) => Object.prototype.hasOwnProperty.call(value, key);
function fail(message, status = 409) { const error = new Error(message); error.status = status; throw error; }
function object(value) { return !!value && typeof value === 'object' && !Array.isArray(value); }
function metadata(value) {
  if (!object(value) || value.version !== 1 || Object.keys(value).some(key => !['version', 'touchedFields'].includes(key)) || !Array.isArray(value.touchedFields) || value.touchedFields.length > FIELDS.length || value.touchedFields.some(field => !FIELDS.includes(field)) || new Set(value.touchedFields).size !== value.touchedFields.length) fail('The statement-only merchant fill markers are invalid. Refresh the review before saving.', 400);
  return { version: 1, touchedFields: FIELDS.filter(field => value.touchedFields.includes(field)) };
}
function mergeMetadata(row, input) {
  const previous = own(row, 'merchantAutofill') ? metadata(row.merchantAutofill) : null;
  const next = own(input, 'merchantAutofill') ? metadata(input.merchantAutofill) : null;
  if (!previous && !next) return null;
  // A cached older client can change fields without sending markers. Once a
  // statement uses markers, those deliberate edits (including clears) must
  // also become local-only, or a later first fill could overwrite them.
  const changed = previous ? FIELDS.filter(field => own(input, field) && String(input[field] == null ? '' : input[field]).trim() !== String(row[field] == null ? '' : row[field]).trim()) : [];
  // Once used, a field remains local-only until the whole draft is discarded.
  return { version: 1, touchedFields: FIELDS.filter(field => next && next.touchedFields.includes(field) || previous && previous.touchedFields.includes(field) || changed.includes(field)) };
}
const FINANCIAL = ['transfers', 'receipts', 'expenses', 'adjustments', 'reconciliationExpenses', 'expenseRefunds', 'vendorAdvances', 'vendorOpeningPayables', 'receivables', 'refunds', 'loanRepayments', 'creditCardSettlementAllocations', 'gokwikPostings', 'paytmSettlements', 'reimbursements', 'salaryAdvanceTransfers', 'rentals', 'requests'];
function assertDiscardable({ cards, expenses, statement, card, external = {}, canPersonal, visibleSource, visibleExpense, assertUnlocked }) {
  if (!object(cards) || !object(cards.cards) || !object(cards.statements) || !object(expenses) || !object(statement) || !object(card) || cards.statements[statement.id] !== statement || cards.cards[statement.cardId] !== card || !statement.id || !card.id || statement.cardId !== card.id) fail('The statement identity or accounting store cannot be verified.');
  for (const key of ['payments', 'audit']) if (cards[key] != null && !Array.isArray(cards[key])) fail('The card accounting history cannot be verified.');
  for (const key of FINANCIAL) if (expenses[key] != null && typeof expenses[key] !== 'object') fail('A dependent accounting collection cannot be verified.');
  guards.assertAudit(cards, 'audit', 'CCA-'); guards.assertAudit(expenses);
  if (statement.status !== 'review' || statement.finalizedAt || statement.finalizedBy || statement.reopenedAt || statement.reopenedBy || statement.reopenReason || statement.previouslyFinalized || statement.finalizationHistory || statement.locked || statement.reconciled || statement.reconciledAt || statement.reconciliationId) fail('Only a never-finalized, unlocked statement review can be discarded. Finalized or reopened statements retain their correction safeguards.');
  if ((cards.audit || []).some(entry => entry.subjectId === statement.id && ['STATEMENT_FINALIZED', 'STATEMENT_REOPENED'].includes(entry.action))) fail('This statement was previously finalized or reopened. Its accounting review cannot be discarded.');
  if (statement.reviewRevision != null && (!Number.isSafeInteger(statement.reviewRevision) || statement.reviewRevision < 0) || !Number.isSafeInteger((statement.reviewRevision || 0) + 1)) fail('The saved review revision cannot be verified.');
  if (!Array.isArray(statement.rows) || !statement.rows.length) fail('The original statement rows are unavailable.');
  const ids = new Set([statement.id]), rowIds = new Set(), scopes = [], records = [], dates = new Set();
  const account = String(card.name || '').trim() + ' ' + String(card.last4 || '').trim();
  if (!card.name || !/^\d{4}$/.test(String(card.last4 || ''))) fail('The original card identity cannot be verified.');
  for (const row of statement.rows) {
    if (!object(row) || typeof row.id !== 'string' || !row.id || rowIds.has(row.id) || !guards.validDate(row.date) || typeof row.narration !== 'string' || !row.narration.trim()) fail('A statement row identity, date or original narration is invalid.');
    rowIds.add(row.id); ids.add(row.id); ids.add(statement.id + '/' + row.id); ids.add('CCE-' + statement.id + '-' + row.id);
    const values = [row.debit, row.credit, row.amount];
    if (values.some(value => typeof value !== 'number' || !Number.isFinite(value) || value < 0 || !Number.isSafeInteger(Math.round(value * 100)) || Math.abs(value * 100 - Math.round(value * 100)) > 0.00001) || !(row.amount > 0) || !((row.debit > 0 && row.credit === 0) || (row.credit > 0 && row.debit === 0)) || Math.round((row.debit + row.credit) * 100) !== Math.round(row.amount * 100)) fail('The original statement amounts cannot be verified. No review was discarded.');
    if (row.replaces || row.duplicateOf || row.supersededBy || row.linkedStatementId || row.statementId || row.creditCardStatementId) fail('This review contains a linked or overlapping statement row. Use its source correction workflow.');
    if (row.allocations != null && !Array.isArray(row.allocations)) fail('The statement ownership allocation is invalid.');
    if (row.nature === 'PERSONAL' && !canPersonal || (row.allocations || []).some(item => item && item.nature === 'PERSONAL' && !canPersonal)) fail('A private statement allocation is not accessible. Its saved review was preserved.', 403);
    if (row.merchantAutofill != null) metadata(row.merchantAutofill);
    if (row.payingAccount && !visibleSource(row.payingNature, row.payingAccount)) fail('A private or inaccessible settlement source prevents discarding this review.', 403);
    const linkedExpense = row.linkedExpenseId && (expenses.expenses || {})[row.linkedExpenseId];
    if (row.linkedExpenseId && (!linkedExpense || !visibleExpense(linkedExpense))) fail('A linked expense is missing or inaccessible. Its review was preserved.', 403);
    const receipt = row.expenseRefundReceiptId && (expenses.receipts || []).find(item => item.id === row.expenseRefundReceiptId);
    if (row.expenseRefundReceiptId && (!receipt || (receipt.nature === 'PERSONAL' || receipt.ownerOnly) && !canPersonal)) fail('A linked refund receipt is missing or inaccessible. Its review was preserved.', 403);
    records.push(row); dates.add(row.date);
    const natures = NATURES.includes(row.nature) ? [row.nature] : NATURES;
    natures.forEach(nature => scopes.push({ nature, account }));
    if (row.payingAccount) scopes.push({ nature: row.payingNature, account: row.payingAccount });
  }
  // A review referring outward does not own that source. Any independent record
  // pointing back to the draft blocks discard, including retained approvals.
  const dependent = guards.references(FINANCIAL.map(key => expenses[key]));
  if ([...ids].some(id => dependent.has(id))) fail('An accounting record or approval depends on this statement. Its saved review cannot be discarded.');
  const otherCards = { payments: cards.payments || [], statements: Object.fromEntries(Object.entries(cards.statements).filter(([id]) => id !== statement.id)) };
  const otherReferences = guards.references(otherCards);
  if ([...ids].some(id => otherReferences.has(id))) fail('Another card statement or payment depends on this review. Nothing was discarded.');
  if (typeof assertUnlocked !== 'function') fail('The reconciliation protection service is unavailable.');
  assertUnlocked(expenses, { ids: [...ids], scopes, dates: [...dates], records, external });
  // Existing source decisions retain their bank and approval protection even
  // though discard would only clear the referencing draft.
  for (const row of statement.rows) {
    const sourceIds = [row.settlementSourceId, row.expenseRefundReceiptId, row.linkedExpenseId].filter(Boolean);
    if (!sourceIds.length) continue;
    for (const source of sourceIds.slice()) if (/^(payment|transfer|legacy_expense_payment):/.test(source)) sourceIds.push(source.slice(source.indexOf(':') + 1));
    const sourceScopes = row.payingAccount ? [{ nature: row.payingNature, account: row.payingAccount }] : scopes;
    assertUnlocked(expenses, { ids: sourceIds, scopes: sourceScopes, dates: [row.date], records: [], external });
  }
  return true;
}
function reset(statement, card, inferClassification, merchantName) {
  const next = clone(statement);
  for (const row of next.rows) {
    Object.assign(row, { merchant: merchantName(row.narration, card.issuingBank || card.name), classification: inferClassification({ description: row.narration, debit: row.debit, credit: row.credit }), type: '', category: '', suggestedCategory: '', suggestedRule: '', nature: '', channel: '', claimant: '', bill: '', confirmed: false, duplicateResolution: '', linkedExpenseId: '', duplicateWarnings: [], allocations: [], settlementMode: '', settlementSourceId: '', settlementSourceFingerprint: '', settlementReason: '', settlementReference: '', settlementLegacyConfirmed: false, payingNature: '', payingAccount: '', expenseRefundReceiptId: '', refundLinkDecision: '', refundLinkReason: '', merchantAutofill: { version: 1, touchedFields: [] } });
  }
  next.reviewRevision = (statement.reviewRevision || 0) + 1;
  return next;
}
module.exports = { FIELDS, metadata, mergeMetadata, assertDiscardable, reset };

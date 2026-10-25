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
function duplicateTarget(cards, statement, card, row, used, { canPersonal, visibleSource, visibleExpense }, expenses) {
  const pointer = row.duplicateOf;
  if (!object(pointer) || Object.keys(pointer).length !== 2 || Object.keys(pointer).some(key => !['statementId', 'rowId'].includes(key)) || typeof pointer.statementId !== 'string' || !pointer.statementId || typeof pointer.rowId !== 'string' || !pointer.rowId || pointer.statementId === statement.id || row.duplicateResolution) fail('The duplicate statement provenance cannot be verified. Use its source correction workflow.');
  const source = cards.statements[pointer.statementId];
  if (!object(source) || source.id !== pointer.statementId || source.cardId !== card.id || source.status !== 'finalized' || !Array.isArray(source.rows)) fail('The duplicate must refer to an active finalized statement for this same card.');
  const matches = source.rows.filter(item => object(item) && item.id === pointer.rowId), key = pointer.statementId + '/' + pointer.rowId;
  if (matches.length !== 1 || used.has(key)) fail('The duplicate statement row identity is missing, ambiguous or reused.');
  const target = matches[0];
  if (target.replaces || target.duplicateOf || target.supersededBy || target.linkedStatementId || target.statementId || target.creditCardStatementId || target.active === false || target.accountingExcluded || target.deletedAt || target.voidedAt || target.reversedAt || ['duplicate', 'link', 'merge', 'replace'].includes(target.duplicateResolution) || ['voided', 'cancelled', 'canceled', 'reversed', 'rejected', 'deleted'].includes(target.status)) fail('The original duplicate source is inactive or linked to another correction workflow.');
  const reference = value => value == null ? '' : typeof value === 'string' ? value.trim() : null;
  if (row.date !== target.date || row.amount !== target.amount || row.debit !== target.debit || row.credit !== target.credit || row.narration !== target.narration || reference(row.reference) === null || reference(target.reference) === null || reference(row.reference) !== reference(target.reference) || (row.postingDate || '') !== (target.postingDate || '')) fail('The duplicate no longer agrees with its original statement transaction. No review was discarded.');
  if (target.allocations != null && !Array.isArray(target.allocations)) fail('The original statement ownership allocation cannot be verified.');
  if (target.nature === 'PERSONAL' && !canPersonal || (target.allocations || []).some(item => item && item.nature === 'PERSONAL' && !canPersonal)) fail('A private original statement allocation is not accessible. Its duplicate review was preserved.', 403);
  if (target.payingAccount && !visibleSource(target.payingNature, target.payingAccount)) fail('An inaccessible original settlement source prevents discarding this duplicate review.', 403);
  const linkedExpense = target.linkedExpenseId && (expenses.expenses || {})[target.linkedExpenseId];
  if (target.linkedExpenseId && (!linkedExpense || !visibleExpense(linkedExpense))) fail('An original linked expense is missing or inaccessible. Its duplicate review was preserved.', 403);
  const receipt = target.expenseRefundReceiptId && (expenses.receipts || []).find(item => item.id === target.expenseRefundReceiptId);
  if (target.expenseRefundReceiptId && (!receipt || (receipt.nature === 'PERSONAL' || receipt.ownerOnly) && !canPersonal)) fail('An original refund receipt is missing or inaccessible. Its duplicate review was preserved.', 403);
  used.add(key);
  return { statement: source, row: target };
}
function dependencyProjection(expenses, targets, card, account) {
  if (!targets.length) return expenses;
  const projected = clone(expenses);
  // The original finalized posting owns its qualified row identity. Clear only
  // that one raw-id field in a validation copy; unknown or draft-owned pointers
  // elsewhere remain visible to every normal dependent-source protection.
  const records = Object.values(projected.reconciliationExpenses || {});
  for (const record of records) {
    if (!object(record)) continue;
    const source = targets.find(target => record.id === 'CCE-' + target.statement.id + '-' + target.row.id && record.creditCardStatementId === target.statement.id && record.creditCardRowId === target.row.id);
    if (!source || records.filter(item => object(item) && item.id === record.id).length !== 1) continue;
    const row = source.row, amount = row.classification === 'refund' ? -row.amount : row.amount;
    if (['expense', 'fee', 'emi_interest', 'refund'].includes(row.classification) && record.source === 'credit_card_statement' && record.creditCardId === card.id && record.date === row.date && record.account === account && record.amount === amount && record.classification === row.classification && record.nature === row.nature && record.particulars === row.narration) delete record.creditCardRowId;
  }
  return projected;
}
function assertDiscardable({ cards, expenses, statement, card, external = {}, canPersonal, visibleSource, visibleExpense, assertUnlocked }) {
  if (!object(cards) || !object(cards.cards) || !object(cards.statements) || !object(expenses) || !object(statement) || !object(card) || cards.statements[statement.id] !== statement || cards.cards[statement.cardId] !== card || !statement.id || !card.id || statement.cardId !== card.id) fail('The statement identity or accounting store cannot be verified.');
  for (const key of ['payments', 'audit']) if (cards[key] != null && !Array.isArray(cards[key])) fail('The card accounting history cannot be verified.');
  for (const key of FINANCIAL) if (expenses[key] != null && typeof expenses[key] !== 'object') fail('A dependent accounting collection cannot be verified.');
  guards.assertAudit(cards, 'audit', 'CCA-'); guards.assertAudit(expenses);
  if (statement.status !== 'review' || statement.finalizedAt || statement.finalizedBy || statement.reopenedAt || statement.reopenedBy || statement.reopenReason || statement.previouslyFinalized || statement.finalizationHistory || statement.locked || statement.reconciled || statement.reconciledAt || statement.reconciliationId) fail('Only a never-finalized, unlocked statement review can be discarded. Finalized or reopened statements retain their correction safeguards.');
  if ((cards.audit || []).some(entry => entry.subjectId === statement.id && ['STATEMENT_FINALIZED', 'STATEMENT_REOPENED'].includes(entry.action))) fail('This statement was previously finalized or reopened. Its accounting review cannot be discarded.');
  if (statement.reviewRevision != null && (!Number.isSafeInteger(statement.reviewRevision) || statement.reviewRevision < 0) || !Number.isSafeInteger((statement.reviewRevision || 0) + 1)) fail('The saved review revision cannot be verified.');
  if (!Array.isArray(statement.rows) || !statement.rows.length) fail('The original statement rows are unavailable.');
  const ids = new Set([statement.id]), rowIds = new Set(), scopes = [], records = [], dates = new Set(), targets = [], usedTargets = new Set();
  const account = String(card.name || '').trim() + ' ' + String(card.last4 || '').trim();
  if (!card.name || !/^\d{4}$/.test(String(card.last4 || ''))) fail('The original card identity cannot be verified.');
  for (const row of statement.rows) {
    if (!object(row) || typeof row.id !== 'string' || !row.id || rowIds.has(row.id) || !guards.validDate(row.date) || typeof row.narration !== 'string' || !row.narration.trim()) fail('A statement row identity, date or original narration is invalid.');
    rowIds.add(row.id); ids.add(row.id); ids.add(statement.id + '/' + row.id); ids.add('CCE-' + statement.id + '-' + row.id);
    const values = [row.debit, row.credit, row.amount];
    if (values.some(value => typeof value !== 'number' || !Number.isFinite(value) || value < 0 || !Number.isSafeInteger(Math.round(value * 100)) || Math.abs(value * 100 - Math.round(value * 100)) > 0.00001) || !(row.amount > 0) || !((row.debit > 0 && row.credit === 0) || (row.credit > 0 && row.debit === 0)) || Math.round((row.debit + row.credit) * 100) !== Math.round(row.amount * 100)) fail('The original statement amounts cannot be verified. No review was discarded.');
    if (row.replaces || row.supersededBy || row.linkedStatementId || row.statementId || row.creditCardStatementId) fail('This review contains a linked or overlapping statement row. Use its source correction workflow.');
    if (row.duplicateOf) targets.push(duplicateTarget(cards, statement, card, row, usedTargets, { canPersonal, visibleSource, visibleExpense }, expenses));
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
  const projectedExpenses = dependencyProjection(expenses, targets, card, account);
  const dependent = guards.references(FINANCIAL.map(key => projectedExpenses[key]));
  if ([...ids].some(id => dependent.has(id))) fail('An accounting record or approval depends on this statement. Its saved review cannot be discarded.');
  const otherCards = clone({ payments: cards.payments || [], statements: Object.fromEntries(Object.entries(cards.statements).filter(([id]) => id !== statement.id)) });
  // Canonical transaction IDs can repeat on an identical upload. A verified
  // original row definition is not an incoming reference to this review; no
  // nested values, other rows, statements or payment pointers are exempted.
  for (const target of targets) delete otherCards.statements[target.statement.id].rows.find(row => row.id === target.row.id).id;
  const otherReferences = guards.references(otherCards);
  if ([...ids].some(id => otherReferences.has(id))) fail('Another card statement or payment depends on this review. Nothing was discarded.');
  if (typeof assertUnlocked !== 'function') fail('The reconciliation protection service is unavailable.');
  assertUnlocked(projectedExpenses, { ids: [...ids], scopes, dates: [...dates], records, external });
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

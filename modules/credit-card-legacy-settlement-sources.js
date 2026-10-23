'use strict';

// Read-only bridge for an explicitly identified, non-reimbursable PERSONAL
// card repayment captured before the dedicated card-payment workflow existed.
// It never converts an expense, changes bank history, or creates a payment.
const crypto = require('crypto');
const cashAuthority = require('./bank-movement-authority');
const list = value => Array.isArray(value) ? value : [];
const values = value => Array.isArray(value) ? value : Object.values(value || {});
const text = value => String(value == null ? '' : value).trim();
const normalized = value => text(value).toLowerCase().replace(/[^a-z0-9]/g, '');
const accountKey = value => text(value).toLowerCase().replace(/\s+/g, ' ');
const active = record => !!record && record.active !== false && !record.accountingExcluded && !record.deletedAt && !record.voided && !record.voidedAt && !record.reversedAt && !['void', 'voided', 'deleted', 'reversed', 'cancelled', 'canceled', 'rejected'].includes(record.status);
function cents(value) {
  const raw = typeof value === 'number' && Number.isFinite(value) ? String(value) : text(value);
  if (!/^\d+(?:\.\d{1,2})?$/.test(raw)) return null;
  const [whole, fraction = ''] = raw.split('.'), result = Number(whole) * 100 + Number(fraction.padEnd(2, '0'));
  return Number.isSafeInteger(result) ? result : null;
}
function date(value) {
  return /^\d{4}-\d{2}-\d{2}$/.test(text(value)) && Number.isFinite(Date.parse(value + 'T00:00:00Z')) && new Date(value + 'T00:00:00Z').toISOString().slice(0, 10) === value;
}
function stable(value) {
  if (Array.isArray(value)) return value.map(stable);
  return value && typeof value === 'object' ? Object.fromEntries(Object.keys(value).sort().map(key => [key, stable(value[key])])) : value;
}
const fingerprint = value => crypto.createHash('sha256').update(JSON.stringify(stable(value))).digest('hex');
function movementIds(record, includeSelf = true, depth = 0) {
  if (!record || typeof record !== 'object' || depth > 12) return [];
  return [...new Set([...(includeSelf ? [record.id] : []), record.appId, record.movementId, record.linkedEntryId, record.linkedLedgerEntryId, record.expensePaymentId, record.sourcePaymentId, record.sourceId,
    ...list(record.appIds), ...list(record.sourceIds), ...list(record.linkedEntryIds), ...list(record.linkedRecordIds).flatMap(id => text(id).split(/\s*,\s*/)),
    ...list(record.linkedEntries).flatMap(entry => movementIds(entry, true, depth + 1)), ...movementIds(record.ledger, true, depth + 1)].map(text).filter(Boolean))];
}
const linkedIds = row => movementIds(row, false);
function explicitLast4(expense) {
  const fields = [expense.vendor, expense.particulars, expense.telegramNarration].map(text);
  const contexts = fields.filter(field => /\bcredit\s*card\b/i.test(field));
  if (fields.some(field => /\b(?:purchase|membership|fees?|subscription|refund|reversal|reversed|emi|installments?|loans?|salary|advance)\b/i.test(field))) return '';
  const digits = [...new Set(contexts.flatMap(field => field.match(/\b\d{4}\b/g) || []))];
  const repayment = contexts.some(field => /\bcredit\s*card[\s:|#-]+(?:bill[\s:|#-]+)?(?:payment|repayment|settlement)[\s:|#-]*\d{4}\b/i.test(field) || /\bcredit\s*card[\s:|#-]*\d{4}[\s:|#-]+(?:bill[\s:|#-]+)?(?:payment|repayment|settlement)\b/i.test(field));
  return repayment && digits.length === 1 ? digits[0] : '';
}
function forbiddenSource(expense, payment) {
  const fields = ['creditCardId', 'creditCardStatementId', 'salaryAdvanceId', 'fundingTransferId', 'loanId', 'loanRepaymentId', 'receivableId', 'receiptId', 'vendorAdvanceId', 'procurementPoId', 'purchaseOrderId', 'batchPaymentId', 'batchId', 'expenseRefundId', 'refundId', 'settlementId', 'paytmSettlementId', 'gokwikPostingId', 'manualSaleId', 'convertedToTransferId', 'settledByTransferId'];
  return fields.some(field => expense[field] || payment[field]) || expense.isInstallment || expense.inventoryPurchase || expense.nonExpenseReimbursement || payment.provisional || payment.creditOnly || payment.refundCreditId || payment.crossEntityCompanyPayment || /^credit(?:card)?$/.test(normalized(expense.paymentType)) || /^credit(?:card)?$/.test(normalized(payment.paymentType)) || list(expense.vendorAdvanceApplications).length || list(expense.refundCreditApplications).length || list(expense.reimbursementPayments).length || list(expense.settlements).length || list(payment.settlements).length || list(payment.sourceIds).length || list(payment.linkedEntries).length;
}
function linkedElsewhere(exp, expense, payment, movementId) {
  const references = record => movementIds(record).includes(movementId) || record.expenseId === expense.id || record.sourceExpenseId === expense.id;
  if (values(exp.expenseRefunds).some(refund => active(refund) && (references(refund) || [...list(refund.sources), ...list(refund.components)].some(source => source.expenseId === expense.id || source.externalMovementId === movementId || source.receiptId === movementId || list(source.paymentAllocations).some(allocation => source.expenseId === expense.id && allocation.paymentId === payment.id))))) return true;
  for (const field of ['transfers', 'receipts', 'adjustments', 'vendorAdvances', 'vendorOpeningPayables', 'loanRepayments', 'salesRefunds', 'paytmSettlements', 'paytmPayoutPostings', 'creditCardSettlementAllocations']) if (values(exp[field]).some(record => active(record) && (references(record) || list(record.applications).some(references)))) return true;
  return values(exp.bankReconciliationDrafts).some(draft => values(draft.resolutions).some(references));
}
function exactFinalizationOrigin(exp, evidence, account, origin) {
  const { key, record } = evidence, book = (exp.bankStatements || {})[key] || {};
  const instant = value => typeof value === 'string' && /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{3})?Z$/.test(value) && Number.isFinite(Date.parse(value));
  const metadata = value => !!value && value.at === record.finalizedAt && value.by === record.finalizedBy && value.through === record.to && value.transactionsReconciled === true;
  // Old imports sometimes omitted draftId. Never infer it from similar amounts,
  // a nearby timestamp, or the override itself: require the exact completed
  // finalization metadata retained independently in this account's audit/book.
  if (evidence.carried || text(record.draftId) || !instant(record.finalizedAt) || !date(record.to) || !text(record.finalizedBy)) return { error: 'The existing repayment source draft differs from its finalized bank import.' };
  if (list(book.imports).filter(item => item && item.finalizedAt === record.finalizedAt && item.to === record.to && item.finalizedBy === record.finalizedBy).length !== 1) return { error: 'The historical bank source-draft evidence is ambiguous.' };
  const audits = values(exp.auditLog).filter(entry => entry && entry.action === 'BANK_RECONCILIATION_FINALIZED' && entry.subjectType === 'account' && entry.nature === 'PERSONAL' && accountKey(entry.account) === accountKey(account) && accountKey(entry.subjectId) === accountKey(account) && entry.user === record.finalizedBy && instant(entry.at) && metadata(entry.after));
  const last = metadata(book.lastReconciliation) ? book.lastReconciliation : null;
  if (audits.length > 1 || audits.some(entry => text(entry.after.draftId) !== origin) || last && text(last.draftId) !== origin) return { error: 'The historical bank source-draft evidence is conflicting or ambiguous.' };
  if (!audits.length && !last) return { error: 'The historical bank import has no verified source-draft provenance.' };
  if (audits.some(entry => !text(entry.id))) return { error: 'The historical bank import has no verified source-draft provenance.' };
  return { error: '', proof: { origin, ...(audits.length ? { audit: audits[0] } : {}), ...(last ? { lastReconciliation: last } : {}) } };
}
function bankEvidence(exp, expense, payment, movementId) {
  const expectedBook = 'PERSONAL|' + text(payment.account), matching = [], pendingCarry = [];
  for (const [key, book] of Object.entries(exp.bankStatements || {})) for (const record of list(book.imports)) {
    for (const carried of list(record.carryForwardMovements)) if (carried.bankEvidencePending !== false && movementIds(carried).includes(movementId)) pendingCarry.push(carried);
    for (const entry of [...list(record.reconciliationRows).map(row => ({ row, carried: false })), ...list(record.carriedReconciliationRows).map(row => ({ row, carried: true }))]) {
      if (linkedIds(entry.row).includes(movementId)) matching.push({ key, record, row: entry.row, carried: entry.carried });
    }
  }
  const included = matching.filter(evidence => !evidence.row.corrected && active(evidence.record));
  const invalid = 'The existing repayment needs one exact, finalized bank reconciliation link before it can be selected.';
  if (!included.length || pendingCarry.length) return { error: invalid, matching };
  for (const evidence of included) {
    const { key, record, row } = evidence, bank = row.bank, ids = linkedIds(row);
    if (key !== expectedBook || !text(record.id) || !text(record.finalizedAt) || !Number.isFinite(Date.parse(record.finalizedAt)) || !bank || !date(bank.date) || cents(bank.debit) !== cents(payment.amount) || cents(bank.credit || 0) !== 0 || bank.reversal || row.resolutionAction === 'bank_reversal_pair' || /\b(?:reversal|reversed|excluded|timing difference|failed payment)\b/i.test(text(bank.description) + ' ' + text(row.decision)) || ids.length !== 1 || ids[0] !== movementId || row.ledger && (row.ledger.id !== movementId || cents(row.ledger.debit) !== cents(payment.amount) || cents(row.ledger.credit || 0) !== 0)) return { error: invalid, matching };
  }
  const first = included[0], signature = evidence => fingerprint({ key: evidence.key, transactionId: text(evidence.row.bank.transactionId), date: evidence.row.bank.date, debit: evidence.row.bank.debit, credit: evidence.row.bank.credit || 0, reference: normalized(evidence.row.bank.reference), ids: linkedIds(evidence.row) });
  // Carried copies of one identified transaction are evidence, not more money.
  if (included.length > 1 && (!text(first.row.bank.transactionId) || included.filter(evidence => !evidence.carried).length !== 1 || included.some(evidence => signature(evidence) !== signature(first)))) return { error: invalid, matching };
  const override = (exp.bankDateOverrides || {})[movementId], link = (exp.bankReconciliationLinks || {})[movementId], bank = first.row.bank;
  if (override && override.provisional) return { error: 'The existing repayment still has a provisional bank-date decision. Review its bank reconciliation first.', matching };
  const origins = new Set(included.flatMap(evidence => [evidence.record.id, evidence.record.draftId]).filter(Boolean));
  const provenance = [];
  for (const origin of [...new Set([override && override.reconciliationDraft, link && link.reconciliationDraft].filter(Boolean))]) {
    if (origins.has(origin)) continue;
    const primary = included.filter(evidence => !evidence.carried);
    if (primary.length !== 1) return { error: 'The historical bank source-draft evidence is ambiguous.', matching };
    const proof = exactFinalizationOrigin(exp, primary[0], payment.account, origin);
    if (proof.error) return { error: proof.error, matching };
    origins.add(origin); provenance.push(proof.proof);
  }
  if (override && (!date(override.bankDate) || override.bankDate !== bank.date)) return { error: 'The existing repayment saved bank date differs from its finalized bank evidence.', matching };
  if (override && override.originalDate && override.originalDate !== payment.date) return { error: 'The existing repayment original payment date differs from its bank-date audit.', matching };
  if (override && override.bankTransactionId && override.bankTransactionId !== bank.transactionId) return { error: 'The existing repayment bank-transaction identity differs from its finalized bank evidence.', matching };
  if (override && override.bankReference && bank.reference && normalized(override.bankReference) !== normalized(bank.reference)) return { error: 'The existing repayment reference differs from its finalized bank evidence.', matching };
  if (link && (link.provisional || link.bankTransactionId && link.bankTransactionId !== bank.transactionId || link.bankImportId && !origins.has(link.bankImportId))) return { error: invalid, matching };
  const authorities = Object.entries(exp.bankMovementAuthority || {}).filter(([, authority]) => authority && movementIds(authority).includes(movementId));
  if (authorities.some(([key, authority]) => key !== cashAuthority.key(payment.account, 'PERSONAL', movementId) || authority.movementId !== movementId || accountKey(authority.account) !== accountKey(payment.account) || list(authority.sourceIds).length || list(authority.linkedEntryIds).length || list(authority.linkedEntries).length || !authority.original || !authority.after || cents(authority.original.debit) !== cents(payment.amount) || cents(authority.original.credit || 0) !== 0 || authority.original.date !== payment.date || cents(authority.after.debit) !== cents(payment.amount) || cents(authority.after.credit || 0) !== 0 || authority.after.date !== bank.date) || authorities.length > 1) return { error: 'The effective bank amount or source identity changed; review its protected bank workflow first.', matching };
  if (!override && !authorities.length && payment.date !== bank.date) return { error: 'The existing repayment and its finalized bank date no longer agree.', matching };
  for (const reference of [payment.reference, payment.bankReference, expense.bankReference]) if (reference && bank.reference && normalized(reference) !== normalized(bank.reference)) return { error: 'The existing repayment and its finalized bank reference no longer agree.', matching };
  return { error: '', matching, bank, override, link, authorities, provenance, reference: text(bank.reference || payment.bankReference || payment.reference || override && override.bankReference), date: bank.date };
}
function samePhysicalPayment(source, payment, evidence) {
  if (accountKey(source.account || source.fromAccount) !== accountKey(payment.account)) return false;
  const reference = normalized(source.reference || source.bankReference || source.utr), bankReference = normalized(evidence.reference || payment.bankReference || payment.reference);
  if (reference && bankReference && reference === bankReference) return true;
  return cents(source.amount) === cents(payment.amount) && date(source.date) && [payment.date, evidence.date].some(value => date(value) && Math.abs(Date.parse(source.date) - Date.parse(value)) <= 3 * 86400000);
}
function sources(store, exp, statement, recognized = []) {
  const card = (store.cards || {})[statement.cardId];
  if (!card || !/^\d{4}$/.test(text(card.last4))) return [];
  const allExpenses = values(exp.expenses), result = [];
  for (const expense of allExpenses) {
    if (!expense || expense.nature !== 'PERSONAL' || normalized(expense.ledger) !== 'creditcard' || explicitLast4(expense) !== card.last4) continue;
    const payments = list(expense.payments), payment = payments[0];
    if (!payment || !text(expense.id) || !text(payment.id)) continue;
    const movementId = expense.id + '/' + payment.id, bank = bankEvidence(exp, expense, payment, movementId), total = cents(expense.amount);
    let blockedReason = '';
    if (card.id !== statement.cardId || values(store.cards).filter(value => value && text(value.last4) === text(card.last4)).length !== 1 || card.active === false || allExpenses.filter(value => value && value.id === expense.id).length !== 1 || exp.expenses && !Array.isArray(exp.expenses) && exp.expenses[expense.id] !== expense) blockedReason = 'The legacy repayment or selected card identity is ambiguous.';
    else if (!active(expense) || !active(payment) || expense.status !== 'paid' || !text(expense.approvedAt) || !Number.isFinite(Date.parse(expense.approvedAt)) || expense.paidAlready !== true || expense.fundedBy !== 'claimant' || expense.reimbursementStatus !== 'not_applicable' || cents(expense.reimbursementAmount || 0) !== 0 || payments.length !== 1 || payment.personalFunds !== true || total === null || total <= 0 || [expense.paidAmount, expense.personalPaidAmount, payment.amount].some(value => cents(value) !== total) || !date(payment.date) || expense.date !== payment.date || !text(payment.account) || expense.account !== payment.account || !(typeof payment.proof === 'string' && text(payment.proof)) && !list(payment.proofs).some(proof => typeof proof === 'string' && text(proof))) blockedReason = 'Only a fully paid, approved, evidenced PERSONAL card repayment with one non-reimbursable payment can be linked.';
    else if (forbiddenSource(expense, payment) || linkedElsewhere(exp, expense, payment, movementId)) blockedReason = 'The legacy repayment is allocated to another protected accounting workflow.';
    else if (bank.error) blockedReason = bank.error;
    else {
      const allRecorded = [...recognized, ...values(store.payments).filter(active), ...values(exp.transfers).filter(record => active(record) && (record.classification === 'credit_card_payment' || record.creditCardPaymentId))];
      if (allRecorded.some(source => movementIds(source).includes(movementId) || source.sourceExpenseId === expense.id || source.expenseId === expense.id || source.linkedExpenseId === expense.id || samePhysicalPayment(source, payment, bank))) blockedReason = 'A dedicated card payment already represents this bank movement. Review its existing source instead.';
    }
    result.push({ id: 'legacy_expense_payment:' + movementId, sourceKind: 'legacy_expense_payment', legacyConfirmationRequired: true,
      source: payment, expense, evidence: [{ amount: payment.amount, date: payment.date, reference: text(payment.bankReference || payment.reference) }, ...(bank.bank ? [{ amount: bank.bank.debit, date: bank.bank.date, reference: text(bank.bank.reference) }] : [])],
      date: bank.date || payment.date, amount: payment.amount, account: text(payment.account), nature: 'PERSONAL', reference: bank.reference || text(payment.bankReference || payment.reference), blockedReason,
      bankMovementRecorded: !bank.error, status: bank.error ? 'Legacy PERSONAL repayment source; finalized bank evidence needs review. No accounting records will be changed.' : 'Legacy PERSONAL card repayment; original expense and reconciled bank entry stay unchanged. This links liability settlement only, not an expense offset.',
      fingerprint: fingerprint({ card, expense, payment, bankEvidence: bank.matching.map(evidence => ({ key: evidence.key, record: Object.fromEntries(Object.entries(evidence.record).filter(([key]) => !['reconciliationRows', 'carriedReconciliationRows', 'carryForwardMovements'].includes(key))), row: evidence.row })), override: bank.override || null, link: bank.link || null, authorities: bank.authorities || [], ...(bank.provenance && bank.provenance.length ? { finalizationProvenance: bank.provenance } : {}) }) });
  }
  return result;
}
module.exports = { sources };

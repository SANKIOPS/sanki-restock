'use strict';

// Pure adapter for ONE bank-funded credit-card payment, not card purchases.
// The caller authorizes/fingerprints the plan and commits BOTH stores together.
const crypto = require('crypto');
const list = value => Array.isArray(value) ? value : [];
const values = value => value && typeof value === 'object' ? Object.values(value) : [];
const text = value => String(value == null ? '' : value).trim();
const copy = value => JSON.parse(JSON.stringify(value));
const natures = new Set(['SANKI', 'SAMAST', 'PERSONAL']);
function fail(message, code = 'CARD_PAYMENT_AUTHORITY_BLOCKED') { const error = new Error(message); error.status = 409; error.code = code; throw error; }
function precise(value) {
  const number = Number(value), result = Math.round(number * 100);
  if (value === '' || value == null || typeof value === 'boolean' || !Number.isFinite(number) || !Number.isSafeInteger(result) || Math.abs(number * 100 - result) > .00001) fail('Card payment amounts must be finite and precise to two decimal places.');
  return result;
}
function stable(value) {
  if (Array.isArray(value)) return '[' + value.map(stable).join(',') + ']';
  if (value && typeof value === 'object') return '{' + Object.keys(value).sort().filter(key => value[key] !== undefined).map(key => JSON.stringify(key) + ':' + stable(value[key])).join(',') + '}';
  return JSON.stringify(value);
}
const hash = value => crypto.createHash('sha256').update(stable(value)).digest('hex');
const reference = value => text(value).toLowerCase().replace(/[^a-z0-9]/g, '');
function date(value) {
  const result = text(value);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(result) || !Number.isFinite(Date.parse(result + 'T00:00:00Z')) || new Date(result + 'T00:00:00Z').toISOString().slice(0, 10) !== result) fail('A valid bank payment date is required.');
  return result;
}
function financialRefs(row) {
  const ids = [...list(row.linkedRecordIds).flatMap(id => text(id).split(/\s*,\s*/)), row.appId, ...list(row.appIds)];
  function visit(value) { if (!value || typeof value !== 'object') return; ids.push(value.id, ...list(value.sourceIds), ...list(value.linkedEntryIds)); list(value.linkedEntries).forEach(visit); }
  visit(row.ledger); visit(row);
  return ids.filter(Boolean).map(id => text(id).replace(/\/RECEIVED$/, ''));
}
function assertNoBankClaims(store, ids, draftId) {
  const claimed = row => financialRefs(row).some(id => ids.has(id));
  for (const book of values(store.bankStatements)) for (const record of list(book.imports)) if ([...list(record.reconciliationRows), ...list(record.carriedReconciliationRows), ...list(record.carryForwardMovements)].some(claimed)) fail('This card payment already has finalized bank evidence. Preserve it and use its audited correction workflow.');
  for (const draft of values(store.bankReconciliationDrafts)) if (text(draft.id) !== draftId && values(draft.resolutions).some(claimed)) fail('Another bank reconciliation already claims this card payment.');
  for (const collection of [store.bankDateOverrides, store.bankReconciliationLinks]) for (const [id, evidence] of Object.entries(collection || {})) if (ids.has(id) && evidence && !(evidence.provisional && text(evidence.reconciliationDraft) === draftId)) fail('Finalized or foreign bank evidence protects this card payment.');
}
function assertUnsettled(record, draftId) {
  if (record.active === false || record.accountingExcluded || record.reconciled || record.reconciledAt || record.reconciliationId || record.finalizedAt || record.locked || record.bankTransactionId || record.bankStatementEvidence || record.bankReconciliationEvidence || record.settledByTransferId || record.settlementId || record.gokwikPostingId || record.paytmSettlementId || record.confirmedBankSplit || record.creditCardStatementId || record.statementId || record.refundId || record.loanId || record.salaryAdvanceId || list(record.payments).length || list(record.settlements).length || list(record.sourceIds).length || list(record.linkedEntries).length) fail('This card payment is finalized, allocated elsewhere or protected by another accounting workflow.');
  const origin = text(record.reconciliationDraft || record.bankReconciliationDraft);
  if (origin && origin !== draftId) fail('Another statement already protects this card payment.');
  for (const key of ['settledAmount', 'reimbursedAmount', 'paidAmount']) if (record[key] != null && precise(record[key]) !== 0) fail('A settled card ownership allocation cannot be changed independently.');
}
function allocationMap(items, cents) {
  if (!Array.isArray(items) || !items.length) fail('The card payment ownership allocation is missing. Review it in Credit cards.');
  const result = new Map();
  for (const item of items) {
    const nature = text(item.nature || item.beneficiaryNature).toUpperCase(), amount = cents(item.amount);
    if (!natures.has(nature) || amount <= 0 || result.has(nature)) fail('Card ownership allocations must have unique entities and positive precise amounts.');
    result.set(nature, amount);
  }
  return result;
}
function total(map) { const result = [...map.values()].reduce((sum, amount) => sum + amount, 0); if (!Number.isSafeInteger(result)) fail('The card allocation total exceeds the precise monetary range.'); return result; }
function assertNoCardStatement(cardStore, cardId, payment, transfer, bank, cents) {
  const ids = new Set([text(payment.id), text(transfer.id)]), refs = new Set([payment.reference, transfer.bankReference, bank.reference].map(reference).filter(value => value.length >= 5));
  const dates = [payment.date, transfer.date, bank.date].filter(Boolean), amounts = new Set([cents(payment.amount), cents(transfer.amount), cents(bank.debit)]);
  for (const statement of values(cardStore.statements)) {
    if (text(statement.cardId) !== cardId || !(statement.status === 'finalized' || statement.finalizedAt)) continue;
    for (const row of list(statement.rows)) {
      const explicit = [row.creditCardPaymentId, row.paymentId, row.linkedPaymentId, ...financialRefs(row)].some(id => ids.has(text(id))) || list(row.duplicateWarnings).some(warning => warning.kind === 'card_payment' && ids.has(text(warning.id)));
      const paymentRow = row.classification === 'card_payment' || Number(row.credit) > 0 && /payment|autopay|thank\s*you/i.test(text(row.narration));
      if (!explicit && !paymentRow) continue;
      const exactReference = paymentRow && refs.has(reference(row.reference));
      const rowAmount = row.amount != null ? cents(row.amount) : cents(row.credit || 0), rowDates = [row.date, row.postingDate].filter(Boolean);
      const amountDateEvidence = paymentRow && amounts.has(rowAmount) && rowDates.some(rowDate => dates.some(sourceDate => Math.abs((Date.parse(rowDate) - Date.parse(sourceDate)) / 86400000) <= 3));
      if (explicit || exactReference || amountDateEvidence) fail('A finalized card statement contains linked or indistinguishable payment evidence. Keep the printed statement unchanged and review the bank/card difference together.', 'CARD_STATEMENT_LOCKED');
    }
  }
}
function build(store, context = {}) {
  const supplied = context.source && (context.source.record || context.source);
  if (!supplied || supplied.classification !== 'credit_card_payment' && !supplied.creditCardPaymentId) return null;
  const cents = context.cents || precise, fingerprint = context.fingerprint || hash, draft = context.draft || {}, bank = context.bank || {}, movement = context.movement || {};
  if (supplied.classification !== 'credit_card_payment' || !text(supplied.id) || !text(supplied.creditCardPaymentId)) fail('This card transfer has no unique linked card payment. Review its Credit cards source first.');
  const cardStore = context.externalStores && context.externalStores.creditCards;
  if (!cardStore || typeof cardStore !== 'object') fail('The linked credit-card store is unavailable. Nothing can be corrected.');
  const transferIndexes = list(store.transfers).map((record, index) => ({ record, index })).filter(item => text(item.record.id) === text(supplied.id));
  if (transferIndexes.length !== 1 || fingerprint(transferIndexes[0].record) !== fingerprint(supplied)) fail('The original card transfer changed or is duplicated.');
  const { record: transfer, index: transferIndex } = transferIndexes[0], paymentId = text(transfer.creditCardPaymentId);
  if (list(store.transfers).filter(record => !record.accountingExcluded && text(record.creditCardPaymentId) === paymentId).length !== 1) fail('More than one transfer claims this credit-card payment.');
  const paymentIndexes = list(cardStore.payments).map((record, index) => ({ record, index })).filter(item => text(item.record.id) === paymentId);
  if (paymentIndexes.length !== 1) fail('The linked credit-card payment is missing or duplicated.');
  const { record: payment, index: paymentIndex } = paymentIndexes[0], cardId = text(payment.cardId), card = cardStore.cards && cardStore.cards[cardId];
  if (!card || text(card.id) !== cardId || card.active === false || !text(card.name) || !/^\d{4}$/.test(text(card.last4))) fail('The linked card is missing or inactive.');
  const cardName = text(card.name) + ' ' + text(card.last4), nature = text(payment.nature || 'SANKI').toUpperCase(), old = cents(transfer.amount), next = cents(context.bankAmount), bankDate = date(context.bankDate);
  if (!natures.has(nature) || old <= 0 || next <= 0 || cents(bank.debit || 0) !== next || cents(bank.credit || 0) !== 0 || cents(movement.debit || 0) !== old || cents(movement.credit || 0) !== 0 || text(movement.id) !== text(transfer.id)) fail('Only the exact outgoing bank-funded card payment can be corrected.');
  if (context.delta != null && cents(context.delta) !== next - old || bankDate !== date(bank.date)) fail('The reviewed bank amount, difference or date changed.');
  if (transfer.fromAccount !== draft.account || payment.account !== transfer.fromAccount || transfer.toAccount !== cardName || cents(payment.amount) !== old || date(payment.date) !== date(transfer.date) || text(transfer.fromNature || transfer.nature || 'SANKI').toUpperCase() !== nature || text(transfer.toNature || transfer.nature || 'SANKI').toUpperCase() !== nature) fail('The transfer and card payment do not have the same card, payer, amount, date and entity.');
  if (bank.reversal || /(?:REVERSED\s*:|\bREVERSAL\b|^RVS\b)/i.test(text(bank.description))) fail('A failed payment or reversal must use its reversal workflow.');
  assertUnsettled(transfer, text(draft.id)); assertUnsettled(payment, text(draft.id));
  const oldMap = allocationMap(payment.allocations, cents);
  if (total(oldMap) !== old) fail('Existing card payment allocations do not exactly equal its payment amount.');
  let nextMap;
  if (context.allocations != null) {
    nextMap = allocationMap(context.allocations, cents);
    if (nextMap.size !== oldMap.size || [...oldMap.keys()].some(key => !nextMap.has(key)) || total(nextMap) !== next) fail('Explicit allocation amounts must retain the existing entities and exactly equal the bank payment.');
    for (const [key, amount] of oldMap) if ((next - old) * (nextMap.get(key) - amount) < 0 || next === old && nextMap.get(key) !== amount) fail('Do not redistribute old card ownership while correcting a bank difference. Allocate only this difference.');
  } else if (oldMap.size === 1) nextMap = new Map([[oldMap.keys().next().value, next]]);
  else if (next === old) nextMap = new Map(oldMap);
  else fail('This card payment belongs to multiple entities. Explicitly allocate the bank difference before correcting it.', 'CARD_ALLOCATION_REVIEW_REQUIRED');
  const allocationIndexes = list(store.creditCardSettlementAllocations).map((record, index) => ({ record, index })).filter(item => text(item.record.creditCardPaymentId) === paymentId);
  if (allocationIndexes.length !== oldMap.size) fail('The card ownership ledger is missing or duplicated.');
  const seen = new Set(), ids = new Set([text(transfer.id), paymentId]), patches = [];
  const patch = (targetStore, path, before, after) => patches.push({ store: targetStore, path, before: copy(before), after: copy(after) });
  const newReference = text(bank.reference);
  patch('expenses', ['transfers', transferIndex], transfer, { ...transfer, amount: next / 100, date: bankDate, ...(newReference ? { bankReference: newReference } : {}) });
  patch('creditCards', ['payments', paymentIndex], payment, { ...payment, amount: next / 100, date: bankDate, allocations: payment.allocations.map(item => ({ ...item, amount: nextMap.get(text(item.nature || item.beneficiaryNature).toUpperCase()) / 100 })), ...(newReference ? { reference: newReference } : {}) });
  for (const { record, index } of allocationIndexes) {
    const beneficiary = text(record.beneficiaryNature).toUpperCase();
    if (!text(record.id) || seen.has(beneficiary) || !oldMap.has(beneficiary) || cents(record.amount) !== oldMap.get(beneficiary) || text(record.cardId) !== cardId || record.cardName !== cardName || record.payingAccount !== payment.account || text(record.payerNature).toUpperCase() !== nature || date(record.date) !== date(payment.date) || record.status !== (beneficiary === nature ? 'self' : 'due')) fail('The existing card allocation ledger no longer agrees with its payment.');
    assertUnsettled(record, text(draft.id)); seen.add(beneficiary); ids.add(text(record.id));
    patch('expenses', ['creditCardSettlementAllocations', index], record, { ...record, amount: nextMap.get(beneficiary) / 100, date: bankDate });
  }
  assertNoBankClaims(store, ids, text(draft.id)); assertNoCardStatement(cardStore, cardId, payment, transfer, bank, cents);
  const summary = amount => ({ amount: amount / 100, date: amount === old ? transfer.date : bankDate, creditCardPaymentId: paymentId, cardId });
  return { sourceType: 'credit_card_payment', sourceNature: nature, linkedIds: [...ids], patches, guards: [{ store: 'creditCards', path: ['statements'], fingerprint: fingerprint(cardStore.statements || {}) }, { store: 'creditCards', path: ['cards', cardId], fingerprint: fingerprint(card) }], beforeSummary: summary(old), afterSummary: { ...summary(next), date: bankDate } };
}

module.exports = { build };

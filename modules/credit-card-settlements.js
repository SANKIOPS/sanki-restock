'use strict';
// Statement credits are evidence of a liability settlement, never a second
// withdrawal or a negative expense. This module is deliberately read-only.
const crypto = require('crypto');
const { activeRow, rowMatches } = require('./credit-card-accounting');
const NATURES = ['SANKI', 'SAMAST', 'PERSONAL'];
const text = value => String(value == null ? '' : value).trim();
const ref = value => text(value).replace(/[^a-z0-9]/ig, '').toLowerCase();
const values = value => Array.isArray(value) ? value : Object.values(value || {});
function cents(value) {
  const raw = typeof value === 'number' && Number.isFinite(value) ? String(value) : text(value);
  if (!/^\d+(?:\.\d{1,2})?$/.test(raw)) return null;
  const [whole, fraction = ''] = raw.split('.');
  const result = Number(whole) * 100 + Number(fraction.padEnd(2, '0'));
  return Number.isSafeInteger(result) ? result : null;
}
function day(value) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(text(value))) return null;
  const parsed = Date.parse(value + 'T00:00:00.000Z');
  return Number.isFinite(parsed) && new Date(parsed).toISOString().slice(0, 10) === value ? parsed / 86400000 : null;
}
function active(record) {
  return !!record && record.active !== false && !record.accountingExcluded && !record.deletedAt && !record.voidedAt && !record.reversedAt && !['void', 'voided', 'deleted', 'reversed', 'cancelled', 'canceled'].includes(record.status);
}
function cardName(card) { return card ? text(card.name) + ' ' + text(card.last4) : ''; }
function stable(value) {
  if (Array.isArray(value)) return value.map(stable);
  if (value && typeof value === 'object') return Object.fromEntries(Object.keys(value).sort().map(key => [key, stable(value[key])]));
  return value;
}
function fingerprint(value) { return crypto.createHash('sha256').update(JSON.stringify(stable(value))).digest('hex'); }
function permitted(check, nature, account) {
  if (!NATURES.includes(nature) || !text(account) || typeof check !== 'function') return false;
  try { return check(nature, account) === true; } catch { return false; }
}
function sourcePermitted(check, source) {
  return permitted(check, source.nature, source.account) && (source.associatedAccounts || []).every(account => permitted(check, account.nature, account.account));
}
function isSettlementNarration(value) {
  // Avoid treating a merchant's generic "payment refund" as card repayment.
  return /\b(?:autopay|auto\s*pay|payment\s+(?:received|thank\s*you)|(?:card|credit\s*card)\s+(?:bill\s+)?payment|bill\s+payment\s+(?:received|credit)|infinity\s+payment|payment\s*[-:]?\s*thank\s*you)\b/i.test(text(value));
}
function paymentReference(payment, transfer) {
  return text(payment && payment.reference || transfer && (transfer.reference || transfer.bankReference || transfer.utr));
}
function evidence(record) {
  return { amount: record.amount, date: record.date, reference: text(record.reference || record.bankReference || record.utr) };
}
function hasMatchingEvidence(source, predicate) {
  // A damaged CCP must not hide a real transfer behind its canonical identity.
  // These records are only inspected for collision detection, never exposed as
  // extra candidate options or accepted as a valid source while inconsistent.
  return [source, ...(source.evidence || [])].some(predicate);
}
function relatedTransfers(exp, payment) {
  const explicit = text(payment.transferId || payment.creditCardTransferId);
  return values(exp.transfers).filter(t => t.creditCardPaymentId === payment.id || explicit && t.id === explicit);
}
function catalog(store, exp, statement) {
  const card = (store.cards || {})[statement.cardId];
  if (!card) return [];
  const target = cardName(card), allPayments = values(store.payments), transfers = values(exp.transfers), out = [];
  for (const payment of allPayments.filter(p => p.cardId === card.id && active(p))) {
    const associated = relatedTransfers(exp, payment), transfer = associated[0];
    let blockedReason = '';
    if (!text(payment.id) || allPayments.filter(p => p.id === payment.id).length !== 1) blockedReason = 'The saved card-payment identity is ambiguous.';
    else if (associated.length > 1) blockedReason = 'The saved card payment has more than one associated transfer.';
    else if ((payment.transferId || payment.creditCardTransferId) && !transfer) blockedReason = 'The associated bank transfer is missing.';
    else if (transfer && (!active(transfer) || transfer.classification !== 'credit_card_payment' || transfer.creditCardPaymentId !== payment.id || transfer.creditCardId && transfer.creditCardId !== card.id || transfer.toAccount !== target || transfer.fromAccount !== payment.account || (transfer.fromNature || transfer.nature) !== payment.nature || cents(transfer.amount) !== cents(payment.amount) || transfer.date !== payment.date)) blockedReason = 'The saved card payment and its bank transfer no longer agree.';
    out.push({ id: 'payment:' + payment.id, source: payment, transfer, evidence: associated.map(evidence), associatedAccounts: associated.map(t => ({ nature: text(t.fromNature || t.nature), account: text(t.fromAccount) })), date: payment.date, amount: payment.amount, account: text(payment.account), nature: text(payment.nature), reference: paymentReference(payment, transfer), blockedReason, fingerprint: fingerprint({ card: { id: card.id, name: card.name, last4: card.last4 }, payment, transfers: associated }) });
  }
  for (const transfer of transfers.filter(t => active(t) && t.classification === 'credit_card_payment' && (t.toAccount === target || t.creditCardId === card.id))) {
    // A CCP and its transfer are one movement, not two alternative sources.
    if (transfer.creditCardPaymentId && allPayments.some(p => p.id === transfer.creditCardPaymentId && p.cardId === card.id && active(p))) continue;
    let blockedReason = '';
    if (!text(transfer.id) || transfers.filter(t => t.id === transfer.id).length !== 1) blockedReason = 'The saved transfer identity is ambiguous.';
    else if (transfer.toAccount !== target || transfer.creditCardId && transfer.creditCardId !== card.id) blockedReason = 'The saved transfer card identity and receiving account do not agree.';
    else if (transfer.creditCardPaymentId) blockedReason = 'The associated card-payment record is missing or inactive.';
    out.push({ id: 'transfer:' + transfer.id, source: transfer, transfer, date: transfer.date, amount: transfer.amount, account: text(transfer.fromAccount), nature: text(transfer.fromNature || transfer.nature), reference: paymentReference(null, transfer), blockedReason, fingerprint: fingerprint({ card: { id: card.id, name: card.name, last4: card.last4 }, transfer }) });
  }
  return out;
}
function sameSource(a, b, sources) {
  if (a === b) return true;
  const left = sources.find(x => x.id === a), right = sources.find(x => x.id === b);
  if (right && right.transfer && a === 'transfer:' + right.transfer.id) return true;
  if (left && left.transfer && b === 'transfer:' + left.transfer.id) return true;
  if (!left || !right) return false;
  return !!left.transfer && !!right.transfer && left.transfer.id === right.transfer.id;
}
function claimed(store, statement, row, source, sources) {
  return values(store.statements).some(other => {
    if (other.cardId !== statement.cardId || !active(other) || ['discarded', 'archived'].includes(other.status)) return false;
    return (other.rows || []).some(previous => {
      if (!activeRow(previous) || previous.classification !== 'card_payment' || previous.settlementMode !== 'link_existing' || !sameSource(previous.settlementSourceId, source.id, sources)) return false;
      if (other.id === statement.id && previous.id === row.id) return false;
      if (row.replaces && row.replaces.statementId === other.id && row.replaces.rowId === previous.id && rowMatches(row, previous)) return false;
      return true;
    });
  });
}
function sourceProblem(store, statement, row, source, sources) {
  if (source.blockedReason) return source.blockedReason;
  const amount = cents(row.amount), credit = cents(row.credit), debit = cents(row.debit || 0);
  if (amount === null || amount <= 0 || credit !== amount || debit !== 0) return 'A card-payment settlement must match a positive statement credit exactly.';
  if (cents(source.amount) === null || cents(source.amount) <= 0 || cents(source.amount) !== amount) return 'The payment amount must match the statement credit exactly, to the paise.';
  if (!NATURES.includes(source.nature) || !source.account) return 'The saved payment has no valid paying entity and account.';
  const sourceDay = day(source.date), rowDay = day(row.date);
  if (sourceDay === null || rowDay === null || Math.abs(sourceDay - rowDay) > 3) return 'The payment and statement dates must be valid and within three days.';
  if (claimed(store, statement, row, source, sources)) return 'This payment is already linked to another active statement transaction.';
  return '';
}
function candidates(store, exp, statement, row, canViewAccount) {
  const sources = catalog(store, exp, statement);
  return sources.filter(s => sourcePermitted(canViewAccount, s)).map(source => {
    const blockedReason = sourceProblem(store, statement, row, source, sources);
    return { id: source.id, label: source.id.slice(source.id.indexOf(':') + 1) + ' · ' + source.account, date: source.date, amount: source.amount, account: source.account, nature: source.nature, reference: source.reference, fingerprint: source.fingerprint, eligible: !blockedReason, blockedReason, dateReasonRequired: source.date !== row.date, bankMovementRecorded: !!source.transfer, status: source.transfer ? 'Existing recorded settlement; bank verification is separate' : 'Legacy card payment; bank movement not verified here' };
  });
}
function reviewSettlement(store, exp, statement, row, input = {}, canViewAccount) {
  const mode = text(input.settlementMode) || 'pending';
  const reason = text(input.settlementReason);
  const fields = { settlementMode: mode, settlementSourceId: '', settlementSourceFingerprint: '', settlementReason: reason, payingNature: '', payingAccount: '', settlementReference: '' };
  const error = message => ({ fields: null, error: message });
  const amount = cents(row.amount);
  if (amount === null || amount <= 0 || cents(row.credit) !== amount || cents(row.debit || 0) !== 0 || day(row.date) === null) return error('A card-payment settlement must be a valid dated, positive statement credit.');
  if (mode === 'pending') return { fields, error: '' };
  const sources = catalog(store, exp, statement);
  if (mode === 'link_existing') {
    const matching = sources.filter(s => s.id === text(input.settlementSourceId));
    if (matching.length !== 1 || !sourcePermitted(canViewAccount, matching[0])) return error('Choose one accessible existing payment and refresh the candidate list.');
    const source = matching[0], problem = sourceProblem(store, statement, row, source, sources);
    if (problem) return error(problem);
    if (!text(input.settlementSourceFingerprint) || input.settlementSourceFingerprint !== source.fingerprint) return error('The payment source changed after it was displayed. Refresh and review it again.');
    if (source.date !== row.date && reason.length < 10) return error('Explain the payment-versus-statement date difference (at least 10 characters).');
    Object.assign(fields, { settlementSourceId: source.id, settlementSourceFingerprint: source.fingerprint, payingNature: source.nature, payingAccount: source.account, settlementReference: source.reference });
    return { fields, error: '' };
  }
  if (mode === 'external') {
    const nature = text(input.payingNature), account = text(input.payingAccount);
    if (!NATURES.includes(nature) || account.length < 3 || reason.length < 10) return error('For an untracked/historical settlement, enter the paying entity, source and an explanation of at least 10 characters.');
    // An external description cannot be used to bypass a recorded movement,
    // including a claimed, inconsistent, or inaccessible matching source.
    if (sources.some(s => hasMatchingEvidence(s, e => cents(e.amount) === amount && day(e.date) !== null && Math.abs(day(e.date) - day(row.date)) <= 3 || ref(input.settlementReference) && ref(e.reference) === ref(input.settlementReference)))) return error('A matching recorded settlement already exists. Link and verify it instead of recording an external settlement.');
    Object.assign(fields, { payingNature: nature, payingAccount: account, settlementReference: text(input.settlementReference) });
    return { fields, error: '' };
  }
  return error('Choose pending, link an existing payment, or explicitly review an untracked/historical settlement.');
}
function validateStatement(store, exp, statement, canViewAccount) {
  for (const row of statement.rows || []) {
    if (!activeRow(row) || row.classification !== 'card_payment') continue;
    if (!row.settlementMode || row.settlementMode === 'pending') return 'Review the settlement source of every card-payment credit before finalizing.';
    const result = reviewSettlement(store, exp, statement, row, row, canViewAccount);
    if (result.error) return result.error;
    for (const key of ['settlementSourceId', 'settlementSourceFingerprint', 'payingNature', 'payingAccount', 'settlementReference']) if (text(row[key]) !== text(result.fields[key])) return 'The saved settlement fields no longer agree with the authoritative payment. Review the source again.';
  }
  return '';
}
function duplicatePayment(store, exp, body = {}) {
  const card = (store.cards || {})[body.cardId], amount = cents(body.amount), date = day(body.date), reference = ref(body.reference);
  if (!card || amount === null || amount <= 0 || date === null) return '';
  const conflict = source => {
    const existingRef = ref(source.reference);
    if (reference && existingRef && reference === existingRef) return true;
    return day(source.date) === date && cents(source.amount) === amount && !(reference && existingRef && reference !== existingRef);
  };
  if (catalog(store, exp, { cardId: card.id }).some(source => hasMatchingEvidence(source, conflict))) return 'A card payment with this reference or this date and amount is already recorded. Link the existing payment instead.';
  for (const statement of values(store.statements)) {
    if (statement.cardId !== card.id || statement.status !== 'finalized' || !active(statement)) continue;
    if ((statement.rows || []).some(row => activeRow(row) && row.classification === 'card_payment' && conflict({ date: row.date, amount: row.amount, reference: row.settlementReference || row.reference }))) return 'This card payment is already included in a finalized statement. Do not record a second withdrawal; review its existing settlement link.';
  }
  return '';
}
module.exports = { candidates, reviewSettlement, validateStatement, duplicatePayment, isSettlementNarration };

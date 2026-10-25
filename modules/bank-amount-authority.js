'use strict';

// A pure, fail-closed correction plan. Routes decide who may match a movement
// and obtain Owner approval; this module never grants access or saves a file.
const crypto = require('crypto');
const own = (object, key) => Object.prototype.hasOwnProperty.call(object || {}, key);
const list = value => Array.isArray(value) ? value : [];
const values = value => value && typeof value === 'object' ? Object.values(value) : [];
const text = value => String(value == null ? '' : value).trim();
const copy = value => JSON.parse(JSON.stringify(value));
function fail(message, status = 409) { const error = new Error(message); error.status = status; throw error; }
function canonical(value) {
  if (Array.isArray(value)) return '[' + value.map(canonical).join(',') + ']';
  if (value && typeof value === 'object') return '{' + Object.keys(value).sort().filter(key => value[key] !== undefined).map(key => JSON.stringify(key) + ':' + canonical(value[key])).join(',') + '}';
  return JSON.stringify(value);
}
function fingerprint(value) { return crypto.createHash('sha256').update(canonical(value)).digest('hex'); }
function cents(value) {
  if (value === '' || value == null || typeof value === 'boolean' || !Number.isFinite(Number(value))) fail('A finite amount with no more than two decimal places is required.', 400);
  const result = Math.round(Number(value) * 100);
  if (!Number.isSafeInteger(result) || Math.abs(Number(value) * 100 - result) > .00001) fail('A finite amount with no more than two decimal places is required.', 400);
  return result;
}
const money = value => value / 100;
function date(value) {
  const result = text(value);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(result) || !Number.isFinite(Date.parse(result + 'T00:00:00Z')) || new Date(result + 'T00:00:00Z').toISOString().slice(0, 10) !== result) fail('The verified bank transaction needs a valid date.', 400);
  return result;
}
function side(row) {
  const debit = cents(row && row.debit == null ? 0 : row.debit), credit = cents(row && row.credit == null ? 0 : row.credit);
  if (debit < 0 || credit < 0 || (debit > 0) === (credit > 0)) fail('Choose one positive money-in or money-out movement; mixed or empty directions are unsupported.', 400);
  return { direction: debit > 0 ? 'debit' : 'credit', amount: debit || credit };
}
function freeze(value) { if (value && typeof value === 'object') { Object.values(value).forEach(freeze); Object.freeze(value); } return value; }
function qualified(expense, payment) { const id = text(payment.id), prefix = text(expense.id) + '/'; return id.startsWith(prefix) ? id : prefix + id; }
function sources(store, movementId) {
  const result = [];
  for (const expense of values(store.expenses)) for (const payment of list(expense.payments)) if (qualified(expense, payment) === movementId) result.push({ type: 'expense_payment', record: expense, payment, id: expense.id });
  for (const [collection, type] of [['transfers', 'transfer'], ['receipts', 'receipt'], ['adjustments', 'adjustment']]) for (const record of list(store[collection])) if (text(record.id) === movementId) result.push({ type, collection, record, id: record.id });
  if (result.length !== 1) fail('This movement has no unique supported source. Review batches, reimbursements, salaries, loans, refunds and settlement reports separately.');
  return result[0];
}
const prohibitedKeys = ['accountingExcluded', 'personalFunds', 'historicalSettlement', 'creditOnly', 'refundCredit', 'provisional', 'batchPaymentId', 'batchId', 'settledByTransferId', 'creditCardId', 'creditCardStatementId', 'bankStatementEvidence', 'gokwikPostingId', 'gokwikSettlementId', 'paytmRoundingTransactionId', 'paytmSettlementId', 'paytmTransactionId', 'paytmPayoutId', 'settledThroughPaytm', 'originalToAccount', 'settlementId', 'refundRecoveryExpenseId', 'refundId', 'expenseRefundId', 'loanId', 'salaryAdvanceId', 'reimbursementAllocationId', 'automaticAxisTransferCharge', 'roundingDifference', 'reconciledClosingCorrection', 'bankTruthOnly'];
function hasProtectedMarker(record) {
  if(record.rentalFeeId)return true;
  return prohibitedKeys.some(key => !!record[key]) || list(record.batchPaymentIds).length || list(record.sourceIds).length || list(record.linkedEntries).length;
}
function callbackBlocked(value) { return value === true || typeof value === 'string' && !!value || value && typeof value === 'object' && !!(value.locked || value.protected || value.error); }
function linkedIds(record) {
  const ids = [];
  function ledger(value) { if (!value || typeof value !== 'object') return; if (value.id) ids.push(text(value.id)); ids.push(...list(value.sourceIds).map(text), ...list(value.linkedEntryIds).map(text)); list(value.linkedEntries).forEach(ledger); }
  ids.push(...list(record.linkedRecordIds).flatMap(id => text(id).split(/\s*,\s*/)));
  ledger(record.ledger); ledger(record);
  return ids;
}
function assertUnclaimed(store, source, movementId, draftId) {
  const ids = new Set([movementId]);
  if (source.type === 'expense_payment') {
    ids.add(text(source.record.id));
    list(source.record.payments).forEach(payment => ids.add(qualified(source.record, payment)));
    list(source.record.reimbursementPayments).forEach(payment => ids.add(qualified(source.record, payment)));
  }
  const contains = id => ids.has(text(id).replace(/\/RECEIVED$/, ''));
  for (const book of values(store.bankStatements)) for (const record of list(book.imports)) {
    if ([...list(record.reconciliationRows), ...list(record.carriedReconciliationRows), ...list(record.carryForwardMovements)].some(row => linkedIds(row).some(contains))) fail('A finalized reconciliation already protects this source. Its amount and dates cannot be changed.');
  }
  for (const draft of values(store.bankReconciliationDrafts)) if (text(draft.id) !== draftId && values(draft.resolutions).some(resolution => [resolution.appId, ...list(resolution.appIds)].some(contains))) fail('Another reconciliation already claims this source.');
  for (const collection of [store.bankDateOverrides, store.bankReconciliationLinks]) for (const [id, override] of Object.entries(collection || {})) if (contains(id) && override && !(override.provisional && text(override.reconciliationDraft) === draftId)) fail('Existing finalized or foreign bank evidence protects this source.');
  for (const record of [source.record, source.payment].filter(Boolean)) {
    const provenance = text(record.reconciliationDraft || record.bankReconciliationDraft);
    if (record.bankTransactionId || record.bankStatementEvidence || provenance && provenance !== draftId) fail('Statement-linked or finalized source amounts cannot be changed.');
  }
}
function assertEligible(store, draft, bank, movement, source, options) {
  if (bank.reversal || /(?:REVERSED\s*:|\bREVERSAL\b|^RVS\b)/i.test(text(bank.description))) fail('Failed payments and reversals require their exact reversal workflow.');
  if (list(movement.sourceIds).length || list(movement.linkedEntries).length || movement.pendingPosting || movement.gokwikSettlementId) fail('Composite or report-backed movements cannot use bank amount correction.');
  if (hasProtectedMarker(source.record) || source.payment && hasProtectedMarker(source.payment)) fail('Protected, batched, personal, refund, fee or settlement movements need their dedicated workflow.');
  const record = source.record, account = text(draft.account), nature = text(draft.nature || 'SANKI').toUpperCase();
  if (!account || !nature) fail('The reconciliation account and entity are required.');
  if (/^(?:bank|paytm|hidden)\s+charges?$/i.test(text(record.category || record.ledger)) || /refund|loan|settlement|salary|reimbursement/i.test(text(record.receiptType)) || list(store.reconciliationExpenses).some(item => text(item.adjustmentId) === movement.id || text(item.settlementId) === movement.id)) fail('Fees, report-backed expenses and protected receipt types require their dedicated workflow.');
  assertUnclaimed(store, source, movement.id, text(draft.id));
  if (typeof options.isProtected === 'function' && callbackBlocked(options.isProtected(movement.id, record, draft.id))) fail('This source is protected by a dedicated accounting workflow.');
  if (source.type === 'expense_payment') {
    if (typeof options.isLocked === 'function' && callbackBlocked(options.isLocked(record, movement.id, draft.id))) fail('This expense has protected payment evidence. Its amounts cannot be changed.');
    // Some approved bank books span business entities. The route authenticates
    // their tagged entity scope; the physical paying account must still match.
    if (text(source.payment.account || record.account) !== account) fail('The payment must belong to this reconciliation account.');
    if (!record.approvedAt || !['paid', 'partially_paid', 'approved'].includes(record.status) || record.paidAlready || record.fundedBy === 'claimant' || record.isInstallment || record.claimantFundingAccount || list(record.reimbursementPayments).length || list(record.vendorAdvanceApplications).length || list(record.refundHistory).length || record.refundedAmount || record.expenseCreditAmount || record.refundStatus) fail('Only approved, ordinary company-funded expenses without reimbursements, advances or refunds support this correction.');
    if (list(record.payments).some(payment => hasProtectedMarker(payment) || cents(payment.amount) <= 0)) fail('Every payment on this expense must be an ordinary positive company payment.');
    if (values(store.expenseRefunds).some(refund => refund.status !== 'voided' && list(refund.sources).some(item => text(item.expenseId) === text(record.id)))) fail('This expense has an active refund or credit note.');
  } else if (source.type === 'transfer') {
    if (record.classification && !['internal_transfer', 'owner_contribution', 'owner_withdrawal'].includes(record.classification) || record.receivedAmount != null || record.confirmedBankSplit || record.fundingTransferId || record.salaryAdvanceFunding || record.advanceId || record.reimbursementBatchId) fail('Exchange, allocated or split transfers require their dedicated workflow.');
    const from = text(record.fromAccount) === account;
    const to = text(record.toAccount) === account;
    if (from === to || side(movement).direction !== (from ? 'debit' : 'credit')) fail('The transfer must have one matching account side and money direction.');
  } else if (text(record.account) !== account) fail('The source must belong to this reconciliation account.');
}
function paymentTotal(expense) {
  const result = list(expense.payments).reduce((sum, payment) => sum + cents(payment.amount), 0);
  if (!Number.isSafeInteger(result)) fail('The payment total is outside the supported precise monetary range.');
  return result;
}
function expenseProjection(expense, delta, payments) {
  const oldAmount = cents(expense.amount), oldPaid = paymentTotal(expense), newAmount = oldAmount + delta, newPaid = payments ? paymentTotal({ payments }) : oldPaid + delta;
  if (!Number.isSafeInteger(newAmount) || !Number.isSafeInteger(newPaid) || oldAmount <= 0 || oldPaid > oldAmount || newAmount <= 0 || newPaid < 0 || newPaid > newAmount || expense.paidAmount != null && cents(expense.paidAmount) !== oldPaid) fail('The expense totals are inconsistent or the correction would create an invalid balance. Review it separately.');
  return { amount: money(newAmount), paidAmount: money(newPaid), status: newPaid >= newAmount ? 'paid' : newPaid > 0 ? 'partially_paid' : 'approved', ...(expense.requestedAmount != null && cents(expense.requestedAmount) === oldAmount ? { requestedAmount: money(newAmount) } : {}) };
}
function sourceBefore(source) {
  const record = source.record;
  if (source.type !== 'expense_payment') return { amount: record.amount, date: record.date };
  return { amount: source.payment.amount, date: source.payment.date, expense: { amount: record.amount, paidAmount: record.paidAmount, status: record.status, requestedAmount: record.requestedAmount, date: record.date } };
}
function bodyId(body) { return 'BAA-' + fingerprint(body).slice(0, 32); }
function ordinaryPlan(store, draft, bank, movement, options = {}) {
  if (!draft || !text(draft.id) || !bank || !movement || !text(movement.id)) fail('A verified bank row, draft and source movement are required.', 400);
  const mode = options.mode || 'auto'; if (!['auto', 'manual'].includes(mode)) fail('Choose automatic or manually verified amount authority.', 400);
  const bankRowId = text(options.bankRowId), match = bankRowId.match(/^bank-(\d+)$/);
  if (!match || !list(draft.transactions)[Number(match[1])] || fingerprint(draft.transactions[Number(match[1])]) !== fingerprint(bank)) fail('The selected bank evidence changed. Refresh the reconciliation.');
  const bankSide = side(bank), movementSide = side(movement);
  if (bankSide.direction !== movementSide.direction) fail('Bank and ledger movements must have the same money direction.', 400);
  const delta = bankSide.amount - movementSide.amount;
  if (mode === 'auto' && Math.abs(delta) > 100) fail('Automatic bank amount authority is limited to an inclusive ₹1.00 difference. Review this match manually.');
  const source = sources(store, text(movement.id));
  if (['__proto__', 'constructor', 'prototype'].includes(text(source.id))) fail('The source identifier is invalid.');
  assertEligible(store, draft, bank, movement, source, options);
  const rawAmount = cents(source.type === 'expense_payment' ? source.payment.amount : source.record.amount);
  if (Math.abs(rawAmount) !== movementSide.amount || source.type !== 'adjustment' && rawAmount <= 0 || source.type === 'receipt' && movementSide.direction !== 'credit' || source.type === 'expense_payment' && movementSide.direction !== 'debit' || source.type === 'adjustment' && (rawAmount < 0 ? 'debit' : 'credit') !== movementSide.direction) fail('The selected movement no longer matches its original source amount or direction.');
  const before = sourceBefore(source), after = { amount: money(source.type === 'adjustment' && bankSide.direction === 'debit' ? -bankSide.amount : bankSide.amount), date: date(bank.date) };
  if (source.type === 'expense_payment') after.expense = { ...expenseProjection(source.record, delta), date: source.record.date };
  const sourceNature = text(source.type === 'transfer' ? source.record[bankSide.direction === 'debit' ? 'fromNature' : 'toNature'] || source.record.nature || 'SANKI' : source.record.nature || 'SANKI').toUpperCase();
  const body = copy({ version: 1, mode, draftId: text(draft.id), bankRowId, nature: text(draft.nature || 'SANKI').toUpperCase(), sourceNature, account: text(draft.account), movementId: text(movement.id), sourceType: source.type, sourceId: text(source.id), paymentId: source.payment ? text(source.payment.id) : null, bankFingerprint: fingerprint(bank), bankSnapshot: bank, sourceFingerprint: fingerprint(source.record), originalAmount: money(movementSide.amount), bankAmount: money(bankSide.amount), amountDifference: money(delta), direction: bankSide.direction, before, after });
  return freeze({ ...body, id: bodyId(body) });
}
function ordinaryValidate(store, candidate, options = {}) {
  if (!candidate || candidate.version !== 1 || !candidate.id) fail('The bank correction plan is invalid.');
  const body = { ...candidate }; delete body.id;
  if (bodyId(body) !== candidate.id) fail('The bank correction plan changed. Review it again.');
  const applied = own(store.bankAmountAuthorityApplications, candidate.id) && store.bankAmountAuthorityApplications[candidate.id];
  if (applied) { if (applied.planFingerprint !== fingerprint(candidate)) fail('The existing correction audit does not match this plan.'); return { alreadyApplied: true, plan: candidate }; }
  const draft = own(store.bankReconciliationDrafts, candidate.draftId) && store.bankReconciliationDrafts[candidate.draftId], match = text(candidate.bankRowId).match(/^bank-(\d+)$/), bank = draft && match && list(draft.transactions)[Number(match[1])];
  if (!draft || !bank || draft.account !== candidate.account || text(draft.nature || 'SANKI').toUpperCase() !== candidate.nature || fingerprint(bank) !== candidate.bankFingerprint) fail('The bank evidence changed after review. Nothing was corrected.');
  const source = sources(store, candidate.movementId);
  if (source.type !== candidate.sourceType || text(source.id) !== candidate.sourceId || fingerprint(source.record) !== candidate.sourceFingerprint) fail('The original ledger source changed after review. Nothing was corrected.');
  const original = cents(candidate.originalAmount), movement = { id: candidate.movementId, debit: candidate.direction === 'debit' ? money(original) : 0, credit: candidate.direction === 'credit' ? money(original) : 0 };
  const rebuilt = ordinaryPlan(store, draft, bank, movement, { ...options, mode: candidate.mode, bankRowId: candidate.bankRowId });
  if (fingerprint(rebuilt) !== fingerprint(candidate)) fail('The correction no longer matches its verified bank and source evidence.');
  return { alreadyApplied: false, plan: candidate, source };
}
function ordinaryApplyAll(store, candidates, options = {}) {
  if (!Array.isArray(candidates)) fail('Supply the reviewed bank correction plans.', 400);
  const ids = new Set();
  for (const candidate of candidates) { if (!candidate || ids.has(candidate.movementId)) fail('A movement cannot be corrected twice in the same finalization.'); ids.add(candidate.movementId); }
  const checked = candidates.map(candidate => ordinaryValidate(store, candidate, options)), pending = checked.filter(item => !item.alreadyApplied), groups = new Map();
  // Validate and project EVERY source before committing any object replacement.
  for (const item of pending) {
    const { source, plan: candidate } = item, key = source.type + '/' + source.id;
    let group = groups.get(key);
    if (!group) { group = { source, before: copy(source.record), after: copy(source.record), plans: [] }; groups.set(key, group); }
    group.plans.push(candidate);
    if (source.type === 'expense_payment') {
      const payment = group.after.payments.find(value => text(value.id) === candidate.paymentId);
      Object.assign(payment, { amount: candidate.after.amount, date: candidate.after.date });
    } else Object.assign(group.after, candidate.after);
  }
  for (const group of groups.values()) if (group.source.type === 'expense_payment') {
    const delta = group.plans.reduce((sum, candidate) => sum + cents(candidate.amountDifference), 0);
    Object.assign(group.after, expenseProjection(group.before, delta, group.after.payments));
  }
  for (const group of groups.values()) {
    const latest = group.plans[group.plans.length - 1], metadata = candidate => ({ planId: candidate.id, draftId: candidate.draftId, bankRowId: candidate.bankRowId, sourceNature: candidate.sourceNature, originalAmount: candidate.originalAmount, amount: candidate.bankAmount, date: candidate.after.date, mode: candidate.mode, ...(options.at ? { at: text(options.at) } : {}) });
    group.after.bankAmountAuthority = { ...metadata(latest), originalAmount: group.before.amount, amount: group.after.amount, planIds: group.plans.map(candidate => candidate.id) };
    if (group.source.type === 'expense_payment') for (const candidate of group.plans) group.after.payments.find(payment => text(payment.id) === candidate.paymentId).bankAmountAuthority = metadata(candidate);
  }
  const audit = pending.map(({ plan: candidate, source }) => {
    const group = groups.get(source.type + '/' + source.id);
    return { planId: candidate.id, draftId: candidate.draftId, bankRowId: candidate.bankRowId, movementId: candidate.movementId, sourceType: candidate.sourceType, sourceId: candidate.sourceId, sourceNature: candidate.sourceNature, mode: candidate.mode, bankAmount: candidate.bankAmount, originalAmount: candidate.originalAmount, amountDifference: candidate.amountDifference, bankDate: candidate.after.date, bankReference: text(candidate.bankSnapshot.reference), before: copy(candidate.before), after: { ...copy(candidate.after), ...(source.type === 'expense_payment' ? { expense: { amount: group.after.amount, paidAmount: group.after.paidAmount, status: group.after.status, requestedAmount: group.after.requestedAmount, date: group.after.date } } : {}) } };
  });
  const journal = { ...(store.bankAmountAuthorityApplications || {}) };
  for (const item of pending) journal[item.plan.id] = { version: 1, planFingerprint: fingerprint(item.plan), draftId: item.plan.draftId, bankRowId: item.plan.bankRowId, movementId: item.plan.movementId, sourceNature: item.plan.sourceNature, before: copy(item.plan.before), after: copy(audit.find(event => event.planId === item.plan.id).after) };
  for (const group of groups.values()) {
    if (group.source.type === 'expense_payment') store.expenses[group.source.id] = group.after;
    else { const index = store[group.source.collection].indexOf(group.source.record); store[group.source.collection][index] = group.after; }
  }
  if (pending.length) store.bankAmountAuthorityApplications = journal;
  return { applied: pending.length, alreadyApplied: checked.length - pending.length, audits: audit, plans: candidates };
}
const universal = require('./bank-universal-authority')({ plan: ordinaryPlan, validate: ordinaryValidate, applyAll: ordinaryApplyAll, cents, fingerprint, date, side, assertUnclaimed });
function apply(store, candidate, options = {}) { return universal.applyAll(store, [candidate], options); }

module.exports = { ...universal, apply, cents, fingerprint };

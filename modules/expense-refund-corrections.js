'use strict';

// Corrections deliberately preserve the original event and owned movement IDs.
// They are not a void/repost operation and never write original bills/payments.
const crypto = require('node:crypto');
const refunds = require('./expense-refunds');
const guards = require('./source-correction-guards');
const clone = value => JSON.parse(JSON.stringify(value));
const fail = (message, status = 409) => { const error = new Error(message); error.status = status; throw error; };
const digest = value => crypto.createHash('sha256').update(JSON.stringify(value)).digest('hex');
const previewSecret = crypto.randomBytes(32);
const owns = (object, key) => Object.prototype.hasOwnProperty.call(object, key);
const fields = new Set(['amount', 'date', 'reference', 'description', 'reason', 'baseFingerprint', 'fingerprint']);

function text(value, label, max, required = false) {
  if (typeof value !== 'string') fail(label + ' must be text.', 400);
  const result = value.trim();
  if (result.length > max || /[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]/.test(result) || required && !result) fail('Enter a valid ' + label.toLowerCase() + '.', 400);
  return result;
}
function amount(value) {
  if (!['number', 'string'].includes(typeof value) || !/^\d+(?:\.\d{1,2})?$/.test(String(value))) fail('Enter a positive amount with at most two decimal places.', 400);
  if (!(Number(value) > 0) || !Number.isFinite(Number(value)) || !Number.isSafeInteger(refunds.cents(value))) fail('Enter a positive amount with at most two decimal places.', 400);
  return refunds.money(value);
}
function savedMoney(value, positive = false) {
  if (typeof value !== 'number' || !Number.isFinite(value) || value < 0 || positive && value <= 0 || !Number.isSafeInteger(refunds.cents(value)) || Math.abs(value * 100 - refunds.cents(value)) > 0.000001) fail('Saved refund/source amounts are invalid. Review the original history before correction.');
}
function fingerprint(snapshot, req, id, proposal) {
  return crypto.createHmac('sha256', previewSecret).update(JSON.stringify({ raw: snapshot.raw, externalRaw: snapshot.externalRaw, cardRaw: snapshot.cardRaw,
    actor: req.user || null, id, proposal: proposal || null })).digest('hex');
}
function authorize(req, policy, record, ctx) {
  if (!policy || !policy.canEdit(req)) fail('Your Manage permissions do not allow transaction corrections.', 403);
  ctx.checkRecord(record);
  if (!policy.natures(req).includes(record.nature)) fail('This refund entity is restricted.', 403);
}
function linked(snapshot, req, id, deps) {
  const store = snapshot.store, record = refunds.records(store).find(row => row.id === id);
  if (!record) fail('Refund/return not found.', 404);
  if (refunds.records(store).filter(row => row.id === id).length !== 1) fail('The refund identity is duplicated. Review its source history first.');
  const ctx = deps.context(req, store, record.date), policy = deps.policy;
  authorize(req, policy, record, ctx);
  guards.assertAudit(store);
  if (!['pending', 'received'].includes(record.status) || record.accountingExcluded) fail('Only an active refund or pending return can be corrected.');
  if (!Array.isArray(record.sources) || record.sources.length !== 1 || !Array.isArray(record.components) || (record.status === 'received' ? record.components.length !== 1 : record.components.length !== 0)) fail('Split or combined refunds require an allocation-specific correction; no records were changed.');
  if (record.parentId || refunds.records(store).some(row => refunds.active(row) && row.parentId === id)) fail('This return has linked pending/received history. Correct that dependency through its original workflow first.');
  const source = record.sources[0], component = record.components[0];
  savedMoney(record.amount, true); savedMoney(source.amount, true); savedMoney(source.billCreditAmount); savedMoney(source.personalReturned);
  if (component) savedMoney(component.amount, true);
  (source.paymentAllocations || []).forEach(row => savedMoney(row.amount, true));
  if (!['expense', 'payment'].includes(source.kind) || !source.expenseId || source.advanceId) fail('Vendor-advance conversions require their original advance correction workflow.');
  if ((source.paymentAllocations || []).length > 1) fail('This refund uses multiple original payments; an allocation-specific correction is required.');
  const expense = ctx.expenses.find(row => row.id === source.expenseId);
  if (!expense) fail('The original expense is missing or restricted.', 403);
  if (ctx.expenses.filter(row => row.id === source.expenseId).length !== 1) fail('The original expense identity is ambiguous. Review its source history first.');
  savedMoney(expense.amount, true); savedMoney(expense.paidAmount || 0);
  (expense.payments || []).filter(row => !row.accountingExcluded && !row.provisional).forEach(row => savedMoney(row.amount, true));
  const paymentIds = (expense.payments || []).map(row => row.id);
  if (new Set(paymentIds).size !== paymentIds.length) fail('The original payment identities are ambiguous. Review their source history first.');
  if (typeof policy.canAccessExpense === 'function' ? policy.canAccessExpense(req, expense) !== true : expense.ownerOnly && !policy.isOwner(req)) fail('Your accounting permissions do not allow corrections to the original expense.', 403);
  if ((record.nature === 'PERSONAL' || (source.paymentAllocations || []).some(row => row.personal)) && !policy.canPersonal(req)) fail('Personal-account access is required for this refund correction.', 403);
  if ((store.receipts || []).some(row => !row.accountingExcluded && row.refundRecoveryExpenseId === source.expenseId)) fail('An employee recovery depends on this refund. Correct the recovery through its dedicated workflow first.');
  if (component && component.externalMovementId) fail('This refund links an existing movement. Correct that movement through its dedicated source workflow first.');
  if (component && component.receiver === 'payer') fail('This refund affects a personal payer reimbursement. Use the reimbursement correction workflow to preserve its recovery history.');
  if (component && component.receiver !== 'company' || !!record.creditNote !== !!(component && component.mode === 'credit_note')) fail('The existing refund receiver or credit-note identity is inconsistent.');
  if (refunds.cents(source.billCreditAmount) !== refunds.cents(source.amount) || source.personalReturned !== 0) fail('The existing bill-credit allocation is inconsistent. Review its source history before correction.');
  const allocated = (source.paymentAllocations || []).reduce((sum, row) => sum + refunds.cents(row.amount), 0);
  if (record.status === 'received' && !record.creditNote ? allocated !== refunds.cents(source.amount) : (source.paymentAllocations || []).length !== 0) fail('The existing original-payment allocation is inconsistent. Review its source history before correction.');
  const receiptRows = (store.receipts || []).filter(row => row.expenseRefundId === id);
  const creditRows = (store.vendorAdvances || []).filter(row => row.expenseRefundId === id);
  const receipt = component && component.receiptId && receiptRows.find(row => row.id === component.receiptId);
  const credit = component && component.vendorAdvanceId && creditRows.find(row => row.id === component.vendorAdvanceId);
  if (receipt && (store.receipts || []).filter(row => row.id === receipt.id).length !== 1 || credit && (store.vendorAdvances || []).filter(row => row.id === credit.id).length !== 1) fail('The linked movement identity is duplicated. No records were changed.');
  if (receiptRows.length !== (receipt ? 1 : 0) || creditRows.length !== (credit ? 1 : 0)) fail('The linked refund history is inconsistent. Review its source records before correction.');
  if (component && refunds.MONEY_MODES.has(component.mode) && !receipt || component && refunds.CREDIT_MODES.has(component.mode) && !credit) fail('The original refund receipt or credit is missing; no replacement will be created.');
  if (component && !refunds.MODES.includes(component.mode)) fail('This refund mode needs a dedicated correction workflow.');
  if (receipt && (!component || !refunds.MONEY_MODES.has(component.mode)) || credit && (!component || !refunds.CREDIT_MODES.has(component.mode))) fail('The existing movement does not match its refund mode.');
  if (component && refunds.cents(component.amount) !== refunds.cents(record.amount) || refunds.cents(source.amount) !== refunds.cents(record.amount)) fail('The existing refund allocation does not balance. Review it before correcting.');
  for (const row of [receipt, credit].filter(Boolean)) {
    savedMoney(row.amount, true);
    if (row.accountingExcluded || row.nature !== record.nature || row.date !== record.date || refunds.cents(row.amount) !== refunds.cents(record.amount)) fail('The linked refund amount/date/entity no longer matches; refresh and review its history.');
  }
  if (credit) savedMoney(credit.remainingAmount);
  if (receipt && (receipt.account !== component.account || receipt.receiptType !== 'expense_refund' || !!receipt.creditCardRefund !== (component.mode === 'card') || receipt.personalRefund || receipt.note !== record.reason || (receipt.bankReference || '') !== (component.reference || '') || receipt.manualSaleId || receipt.creditCardStatementId || receipt.refundRecoveryExpenseId)) fail('The linked receipt belongs to another source workflow or its identity changed.');
  if (credit && (!credit.creditOnly || credit.account !== 'Refund credit' || credit.issuer !== component.issuer || credit.vendor !== component.issuer || (credit.expiryDate || '') !== (component.expiryDate || '') || credit.note !== 'Non-cash refund credit · ' + record.reason || (credit.paymentReference || '') !== (component.reference || '') || refunds.cents(credit.remainingAmount) !== refunds.cents(credit.amount) || (credit.applications || []).some(row => !row.accountingExcluded) || (credit.refundIds || []).length)) fail('This refund credit has been redeemed, converted or its source identity changed. Correct its dependent use before editing the original credit.');
  const ids = [record.id, receipt && receipt.id, credit && credit.id].filter(Boolean);
  if ((store.reconciliationExpenses || []).some(row => ids.includes(row.expenseRefundReceiptId))) fail('This refund is linked to a credit-card statement. Reopen its source review before correction.');
  const scopedAccounts = component && component.account ? [component.account] : Array.from(new Set((source.paymentAllocations || []).map(row => row.account).filter(Boolean)));
  const scopes = scopedAccounts.length ? scopedAccounts.map(account => ({ nature: record.nature, account })) : [{ nature: record.nature, account: '', nonMonetary: true }];
  // Original paying accounts are checked for visibility, but their reconciled
  // payments are not correction targets and must remain byte-for-byte intact.
  for (const account of [...(source.paymentAllocations || []).map(row => row.account), component && component.account].filter(Boolean)) {
    if (!policy.accountVisible(req, account)) fail('A linked account is restricted.', 403);
  }
  guards.assertUnlocked(store, { ids, scopes, dates: [record.date], records: [record, receipt, credit].filter(Boolean), external: snapshot.external, bookKey: policy.bookKey, canonicalAccount: policy.canonicalAccount });
  return { record, source, component, receipt, credit, ids, scopes, expense };
}
function projection(target) {
  return { refund: clone(target.record), receipts: target.receipt ? [clone(target.receipt)] : [], credits: target.credit ? [clone(target.credit)] : [] };
}
function inspect(snapshot, req, id, deps) {
  const record = refunds.records(snapshot.store).find(row => row.id === id);
  if (!record) fail('Refund/return not found.', 404);
  authorize(req, deps.policy, record, deps.context(req, snapshot.store, record.date));
  try {
    const target = linked(snapshot, req, id, deps);
    return { editable: true, baseFingerprint: fingerprint(snapshot, req, id), current: projection(target), fields: ['amount', 'date', 'reference', 'description'], today: deps.today() };
  } catch (error) {
    if (error.status !== 409) throw error;
    return { editable: false, blockedReason: error.message, today: deps.today() };
  }
}
function prepare(snapshot, req, id, input, deps, applying = false) {
  if (!input || typeof input !== 'object' || Array.isArray(input) || Object.keys(input).some(key => !fields.has(key))) fail('Only amount, date, transaction reference and description may be corrected.', 400);
  const target = linked(snapshot, req, id, deps), original = target.record;
  const proposal = {
    amount: owns(input, 'amount') ? amount(input.amount) : original.amount,
    date: owns(input, 'date') ? text(input.date, 'Date', 10, true) : original.date,
    reference: owns(input, 'reference') ? text(input.reference, 'Transaction reference', 200) : target.component && target.component.reference || '',
    description: owns(input, 'description') ? text(input.description, 'Description', 2000, true) : original.reason,
    reason: text(input.reason, 'Correction reason', 1000, true)
  };
  if (!refunds.validDate(proposal.date) || proposal.date > deps.today()) fail('Enter a valid date, not later than today.', 400);
  if (!target.component && proposal.reference) fail('A pending return has no receipt reference to correct.', 400);
  if (applying) {
    if (typeof input.fingerprint !== 'string' || input.fingerprint !== fingerprint(snapshot, req, id, proposal)) fail('The correction or accounting records changed after preview. Review the correction again.');
  } else if (input.baseFingerprint !== fingerprint(snapshot, req, id)) fail('The accounting records changed after this form opened. Close it and review the correction again.');
  guards.assertUnlocked(snapshot.store, { ids: target.ids, scopes: target.scopes, dates: [original.date, proposal.date], records: [original, target.receipt, target.credit].filter(Boolean), external: snapshot.external, bookKey: deps.policy.bookKey, canonicalAccount: deps.policy.canonicalAccount });
  if (proposal.amount === original.amount && proposal.date === original.date && proposal.reference === (target.component && target.component.reference || '') && proposal.description === original.reason) fail('No changes to apply.', 400);
  const valuation = clone(snapshot.store);
  valuation.expenseRefunds = refunds.records(valuation).filter(row => row.id !== id);
  valuation.receipts = (valuation.receipts || []).filter(row => row.expenseRefundId !== id);
  valuation.vendorAdvances = (valuation.vendorAdvances || []).filter(row => row.expenseRefundId !== id);
  const body = { requestId: 'correction-' + digest({ id, proposal }).slice(0, 32), date: proposal.date, reason: proposal.description,
    reasonType: original.reasonType, status: original.status, proofs: clone(original.proofs || []), sources: [{ key: target.source.key, amount: proposal.amount }],
    components: target.component ? [{ ...clone(target.component), amount: proposal.amount, reference: proposal.reference }] : [] };
  const recalculated = refunds.prepare(valuation, body, deps.context(req, valuation, proposal.date));
  if (recalculated.already || recalculated.nature !== original.nature || recalculated.vendor !== original.vendor || recalculated.creditNote !== original.creditNote) fail('The refund source identity changed. Review its original workflow.');
  const source = recalculated.sources[0];
  if (['key', 'kind', 'expenseId', 'paymentId'].some(key => (source[key] || '') !== (target.source[key] || ''))) fail('The original source identity changed.');
  if (target.component && ['mode', 'receiver', 'account', 'issuer', 'expiryDate', 'externalMovementId'].some(key => (recalculated.components[0][key] || '') !== (target.component[key] || ''))) fail('The refund destination or mode no longer matches its original source.');
  const identities = rows => (rows || []).map(row => ({ paymentId: row.paymentId, account: row.account, date: row.date, personal: !!row.personal }));
  if (JSON.stringify(identities(source.paymentAllocations)) !== JSON.stringify(identities(target.source.paymentAllocations))) fail('This amount would change the original payment allocation. Use an allocation-specific correction instead.');
  const store = clone(snapshot.store), afterRecord = refunds.records(store).find(row => row.id === id);
  afterRecord.amount = proposal.amount; afterRecord.date = proposal.date; afterRecord.reason = proposal.description;
  // Keep inputSignature/requestId for original request replay, while the live
  // canonical signature prevents recording the corrected event a second time.
  afterRecord.signature = recalculated.signature;
  ['amount', 'billCreditAmount', 'personalReturned', 'paymentAllocations'].forEach(key => { afterRecord.sources[0][key] = clone(source[key]); });
  if (target.component) { afterRecord.components[0].amount = proposal.amount; afterRecord.components[0].reference = proposal.reference; }
  const afterReceipt = target.receipt && store.receipts.find(row => row.id === target.receipt.id);
  const afterCredit = target.credit && store.vendorAdvances.find(row => row.id === target.credit.id);
  if (afterReceipt) Object.assign(afterReceipt, { amount: proposal.amount, date: proposal.date, bankReference: proposal.reference, note: proposal.description });
  if (afterCredit) Object.assign(afterCredit, { amount: proposal.amount, remainingAmount: proposal.amount, date: proposal.date, paymentReference: proposal.reference, note: 'Non-cash refund credit · ' + proposal.description });
  const before = projection(target), after = projection({ record: afterRecord, receipt: afterReceipt, credit: afterCredit });
  return { store, before, after, proposal, fingerprint: fingerprint(snapshot, req, id, proposal),
    impact: { originalExpensesAndPaymentsChanged: false, createsNewMovement: false, receiptIds: after.receipts.map(row => row.id), creditIds: after.credits.map(row => row.id),
      refundAmountBefore: original.amount, refundAmountAfter: proposal.amount, billCreditBefore: target.source.billCreditAmount, billCreditAfter: source.billCreditAmount } };
}
module.exports = { inspect, prepare };

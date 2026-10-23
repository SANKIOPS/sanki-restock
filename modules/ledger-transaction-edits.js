'use strict';

// Exact-ID edits only. Loading, eligibility checks and previews never migrate
// records or detach reconciliation evidence; every role uses the same locks.
const crypto = require('crypto');
const KINDS = new Set(['transfer', 'receipt', 'expense', 'personal_expense']);
const text = value => String(value == null ? '' : value).trim();
const clone = value => JSON.parse(JSON.stringify(value));
const list = value => Array.isArray(value) ? value : [];
const fail = (message, status = 409) => { const error = new Error(message); error.status = status; throw error; };
const has = (record, field) => Object.prototype.hasOwnProperty.call(record, field);
const dateValid = value => typeof value === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(value) && Number.isFinite(Date.parse(value + 'T00:00:00Z')) && new Date(value + 'T00:00:00Z').toISOString().slice(0, 10) === value;
const money = value => {
  if (!['number', 'string'].includes(typeof value) || typeof value === 'string' && !/^\d+(?:\.\d{1,2})?$/.test(value)) fail('Enter a positive amount with no more than two decimal places.', 400);
  const n = Number(value), cents = Math.round(n * 100);
  if (!Number.isFinite(n) || n <= 0 || !Number.isSafeInteger(cents) || Math.abs(n * 100 - cents) > 0.000001) fail('Enter a positive amount with no more than two decimal places.', 400);
  return cents / 100;
};
function nature(value) {
  const n = text(value || 'SANKI').toUpperCase();
  if (!['SANKI', 'SAMAST', 'PERSONAL'].includes(n)) fail('The saved transaction entity is invalid. Use its source workflow.');
  return n;
}
function collectReferences(value, references) {
  if(typeof value==='string')references.add(value);
  else if(Array.isArray(value))value.forEach(item=>collectReferences(item,references));
  else if(value&&typeof value==='object')for(const [key,item] of Object.entries(value)){references.add(key);collectReferences(item,references);}
}
function create(deps) {
  // The ledger may contain hundreds of visible rows. Index exact evidence once
  // for that immutable request snapshot, never cache it across requests.
  function evidence(s,external){const bank=new Set([...Object.keys(s.bankDateOverrides||{}),...Object.keys(s.bankReconciliationLinks||{})]),linked=new Set();for(const value of [s.bankStatements,s.bankReconciliationDrafts,s.bankMovementAuthority])collectReferences(value,bank);Object.values(external||{}).forEach(value=>collectReferences(value,linked));return{bank,external:linked};}
  function target(s, req, input) {
    if (!deps.canEdit(req)) fail('Manage transactions access is required to edit transfers, payments or receipts.', 403);
    const id = text(input.id), kind = text(input.kind);
    if (!KINDS.has(kind) || !id) fail('Choose a supported transaction.', 400);
    let candidates = [], expense = null, scopes = [];
    if (kind === 'transfer') candidates = list(s.transfers).filter(item => item.id === id);
    else if (kind === 'receipt') candidates = list(s.receipts).filter(item => item.id === id);
    else {
      const parts = id.split('/');
      if (parts.length !== 2 || !parts.every(Boolean)) fail('Choose an exact expense payment.', 400);
      expense = (s.expenses || {})[parts[0]];
      candidates = list(expense && expense.payments).filter(item => item.id === parts[1]);
    }
    if (candidates.length !== 1) fail('The transaction is missing or ambiguous. Refresh its source record.', 404);
    const record = candidates[0];
    if (kind === 'transfer') scopes = [{ nature: nature(record.fromNature || record.nature), account: text(record.fromAccount) }, { nature: nature(record.toNature || record.nature), account: text(record.toAccount) }];
    else scopes = [{ nature: nature(expense ? expense.nature : record.nature), account: text(record.account || expense && expense.account) }];
    if (expense && kind !== (record.personalFunds ? 'personal_expense' : 'expense')) fail('The selected payment kind changed. Refresh the ledger.');
    if (expense && expense.ownerOnly && !deps.isOwner(req)) fail('This expense is restricted to the Owner.', 403);
    for (const scope of scopes) {
      if (!scope.account || !deps.approvalNatures(req).includes(scope.nature) || scope.nature === 'PERSONAL' && !deps.canPersonal(req) || !deps.accountVisible(req, scope.account)) fail('You cannot edit this transaction in one of its saved account scopes.', 403);
      if (deps.personalAccount(scope.account,scope.nature) && !deps.canPersonal(req)) fail('This transaction touches a PERSONAL account. Personal Accounting permission is required.', 403);
    }
    if (record.reportingNature && (!deps.approvalNatures(req).includes(record.reportingNature) || record.reportingNature === 'PERSONAL' && !deps.canPersonal(req))) fail('You cannot edit the transaction’s saved reporting entity.', 403);
    const context = { nature: input.nature == null ? scopes[0].nature : text(input.nature).toUpperCase(), account: input.account == null ? scopes[0].account : text(input.account) };
    if (!deps.approvalNatures(req).includes(context.nature) || !deps.accountVisible(req, context.account)) fail('You cannot edit this ledger scope.', 403);
    // Payments can appear in the physical bank book and their Results entity
    // book. Neither view may alter the actual source entity in the audit.
    const sameScope = scopes.some(scope => scope.nature === context.nature && scope.account === context.account);
    const paymentScope = expense && context.account === scopes[0].account && deps.ledgerAccounts(s, context.nature).includes(context.account) && deps.accountBelongs(context.nature, context.account);
    if (!sameScope && !paymentScope) fail('The selected account or entity does not match the saved transaction.', 403);
    return { id, kind, record, expense, scopes, context, subjectType: expense ? 'expense_payment' : kind };
  }
  function protect(s, selected, external, proposedDate, preparedEvidence) {
    const { id, kind, record, expense, scopes } = selected, refs = new Set([id, ...(expense ? [expense.id] : [])]);
    const dates = Array.from(new Set([text(record.date), proposedDate].filter(Boolean)));
    if (!dates.length || dates.some(value => !dateValid(value))) fail('The transaction date cannot be verified. Use its source workflow.');
    if (record.accountingExcluded || record.historicalSettlement || record.provisional || expense && (expense.accountingExcluded || !deps.paymentPosted(expense))) fail('Excluded, historical or unapproved movements cannot be edited here.');
    if(record.paytmTransactionId||record.paytmRoundingTransactionId)fail('This entry is tied to a reviewed Paytm transaction. Correct it from Paytm reconciliation instead.');
    const sourceFields = ['bankReconciliationDraft', 'bankReconciliationEvidence', 'reconciliationDraft', 'creditCardStatementId', 'creditCardId', 'gokwikPostingId', 'paytmTransactionId', 'paytmRoundingTransactionId', 'paytmSettlementId', 'settledThroughPaytm', 'settledByTransferId', 'salaryAdvanceId', 'manualSaleId', 'orderId', 'shopifyOrderId', 'expenseRefundId', 'refundRecoveryExpenseId', 'tenancyId', 'rentalKind', 'batchPaymentId', 'batchId', 'vendorAdvanceId', 'refundCredit', 'creditOnly', 'convertedFromExpense', 'routedThroughIntermediary', 'intermediary', 'openingBalanceTreatment', 'openingBalanceTreatmentNote', 'confirmedBankSplit', 'bankMovementAuthority'];
    if (sourceFields.some(field => !!record[field]) || list(record.batchPaymentIds).length || list(record.linkedExpenseIds).length || expense && (expense.reconciliationSource || expense.creditCardStatementId || expense.gokwikPostingId)) fail('This movement belongs to a linked source workflow. Correct it there; its evidence cannot be edited independently.');
    if (kind === 'transfer' && ['money_exchange', 'credit_card_payment', 'salary_advance_funding'].includes(record.classification) || kind === 'receipt' && (record.creditCardRefund || ['product_sale', 'rental_income', 'rental_security', 'loan_received', 'expense_refund', 'refund_recovery'].includes(record.receiptType))) fail('This movement must be corrected in its dedicated source workflow.');
    const sourceError = deps.sourceProtection(s, selected); if (sourceError) fail(sourceError);
    if (expense) { const lock = deps.expenseLock(s, expense); if (lock.locked) { const error=new Error('This expense has a reconciled payment. Its payment amounts and dates are locked for every role, including the Owner. Correct descriptive details through the expense editor.');Object.assign(error,{status:409,financialFieldsLocked:true,financialLockReason:lock.reason});throw error; } }
    const linked=preparedEvidence||evidence(s,external);
    if ([...refs].some(id=>linked.bank.has(id))) fail('This transaction is reconciled or linked to an active bank reconciliation. Reopen or undo the source reconciliation before editing it.');
    if ([...refs].some(id=>linked.external.has(id))) fail('This movement is linked to a credit-card, salary, procurement or incentive record. Use its source workflow.');
    for (const scope of scopes) {
      const book = (s.bankStatements || {})[deps.bookKey(scope.nature, scope.account)] || {};
      if (book.reconciledThrough && dates.some(date => date <= book.reconciledThrough)) fail('This transaction touches a finalized bank period. Its dates and amounts are locked.');
      const opening = (scope.nature === 'SANKI' ? s.openingBalanceDates || {} : (s.openingBalanceDatesByNature || {})[scope.nature] || {})[scope.account];
      if (opening && dates.some(date => date < opening)) fail('The transaction would change a period before the account opening.');
      if (list(s.cashReconciliations).some(row => row && ['approved', 'pending'].includes(row.status) && text(row.account) === scope.account && dates.some(date => !row.verifiedThrough || date <= row.verifiedThrough))) fail('This transaction touches an approved or pending cash verification. Resolve that verification first.');
      if (Object.values(s.bankReconciliationApprovals || {}).some(approval => {
        if (!approval || approval.status !== 'pending') return false;
        const draft = (s.bankReconciliationDrafts || {})[approval.draftId], account = text(approval.account || draft && draft.account), from = text(approval.from || draft && draft.summary && draft.summary.from), to = text(approval.to || draft && draft.summary && draft.summary.to);
        return account === scope.account && dates.some(date => (!from || date >= from) && (!to || date <= to));
      })) fail('This transaction touches a statement awaiting Owner approval. Withdraw or resolve that approval before editing it.');
    }
  }
  function inspect(s, req, input, external, proposedDate, preparedEvidence) { const selected = target(s, req, input); protect(s, selected, external, proposedDate,preparedEvidence); return selected; }
  function eligibility(s, req, input, external,preparedEvidence) {
    if (!KINDS.has(input.kind)) return {};
    try { inspect(s, req, input, external,undefined,preparedEvidence); return { canEditLedgerTransaction: true }; }
    catch (error) { return { canEditLedgerTransaction: false, ledgerEditBlockedReason: error.message }; }
  }
  function fingerprint(snapshot, selected, req) {
    return crypto.createHash('sha256').update(JSON.stringify({ raw: snapshot.raw, externalRaw: snapshot.externalRaw, id: selected.id, kind: selected.kind, context: selected.context, user: req.user })).digest('hex');
  }
  const replyError = (res, error) => res.status(error.status || 409).json({ success: false, error: error.message || 'The transaction could not be verified. Nothing was changed.', ...(error.financialFieldsLocked?{financialFieldsLocked:true,financialLockReason:error.financialLockReason}: {}) });
  function register(router) {
    router.get('/api/expenses/ledger-entry/edit-preview', (req, res) => {
      try {
        if (!deps.canEdit(req)) fail('Manage transactions access is required.', 403);
        const snapshot = deps.read(), selected = inspect(snapshot.store, req, req.query || {}, snapshot.external), record = selected.record;
        res.json({ success: true, editable: true, fingerprint: fingerprint(snapshot, selected, req), scope: selected.context, entry: { id: selected.id, kind: selected.kind, amount: record.amount, date: record.date, source: record.source == null ? '' : String(record.source), note: record.note == null ? '' : String(record.note) } });
      } catch (error) { replyError(res, error); }
    });
  }
  function handlePatch(req, res, next) {
      const b = req.body || {}; if (!KINDS.has(b.kind)) return next();
      try {
        if (!deps.canEdit(req)) fail('Manage transactions access is required.', 403);
        const snapshot = deps.read(), s = snapshot.store, preparedEvidence=evidence(s,snapshot.external),selected = inspect(s, req, b, snapshot.external,undefined,preparedEvidence), record = selected.record;
        if (typeof b.fingerprint !== 'string' || b.fingerprint !== fingerprint(snapshot, selected, req)) fail('The transaction or its accounting evidence changed after review. Reload and review it again.');
        const reason = typeof b.reason === 'string' ? b.reason.trim() : '';
        if (!reason || reason.length > 2000) fail('An edit reason of no more than 2,000 characters is required.', 400);
        if (Object.keys(b).some(key => !['id', 'kind', 'nature', 'account', 'fingerprint', 'reason', 'amount', 'date', 'source', 'note'].includes(key))) fail('Only amount, date, receipt source and note may be edited here.', 400);
        const changes = {};
        if (has(b, 'amount')) changes.amount = money(b.amount);
        if (has(b, 'date')) { if (!dateValid(b.date)) fail('Enter a valid date as YYYY-MM-DD.', 400); changes.date = b.date; }
        for (const field of ['source', 'note']) if (has(b, field)) {
          if (typeof b[field] !== 'string' || b[field].length > 4000 || field === 'source' && selected.kind !== 'receipt') fail('The supplied description is not valid for this transaction.', 400);
          changes[field] = b[field];
          if (field === 'source' && !b[field].trim() && text(record.source)) fail('Receipt source cannot be cleared.', 400);
        }
        if (!Object.keys(changes).length) fail('Choose a transaction field to change.', 400);
        protect(s, selected, snapshot.external, changes.date || record.date,preparedEvidence);
        if (!Object.entries(changes).some(([field, value]) => value !== (record[field] == null && ['source', 'note'].includes(field) ? '' : record[field]))) return res.json({ success: true, unchanged: true, entry: clone(record) });
        const before = clone(record), beforeExpense = selected.expense ? clone(selected.expense) : null;
        Object.assign(record, changes, { editedAt: new Date().toISOString(), editedBy: req.user.username });
        if (selected.expense) deps.updateExpense(s, selected.expense);
        const priorAudit = list(s.auditLog).slice();
        deps.audit(s, req, 'LEDGER_ENTRY_EDITED', selected.subjectType, selected.id, { nature: selected.scopes[0].nature, account: selected.scopes[0].account, before, after: clone(record), note: reason });
        // This narrowly scoped correction must not trim older audit history.
        if (s.auditLog.length !== priorAudit.length + 1) s.auditLog = priorAudit.concat(s.auditLog.at(-1));
        if (beforeExpense) Object.assign(s.auditLog.at(-1), { expenseBefore: beforeExpense, expenseAfter: clone(selected.expense) });
        deps.commit(snapshot, s);
        res.json({ success: true, entry: clone(record) });
      } catch (error) { replyError(res, error); }
  }
  return { register, handlePatch, eligibility, evidence, kinds: KINDS };
}
module.exports = { create };

'use strict';

// Explicit reconciliation actions only: no startup migrations or inferred fees.
const { randomUUID, createHash } = require('node:crypto');
const ACCOUNT = 'IndusInd Bank 7883';
const SPECS = Object.freeze({
  dealer_commission: Object.freeze({ label: 'Dealer commission', category: 'Dealer Commission', receiptType: 'rental_dealer_commission' }),
  documentation_fee: Object.freeze({ label: 'Documentation / agreement fee', category: 'Documentation / Agreement Fee', receiptType: 'rental_documentation_fee' })
});
const list = value => Array.isArray(value) ? value : [];
const clone = value => JSON.parse(JSON.stringify(value));
const fail = (message, status = 409) => { const error = new Error(message); error.status = status; throw error; };
const isFeeKind = kind => Object.prototype.hasOwnProperty.call(SPECS, kind);
const active = item => item && !item.reversedAt && !item.voidedAt;
const canonical = value => Array.isArray(value) ? '[' + value.map(canonical).join(',') + ']' : value && typeof value === 'object' ? '{' + Object.keys(value).sort().filter(key => value[key] !== undefined).map(key => JSON.stringify(key) + ':' + canonical(value[key])).join(',') + '}' : JSON.stringify(value);
const sourceHash = source => createHash('sha256').update(canonical(source)).digest('hex');
function exactMoney(value) {
  const amount = Number(value), cents = Math.round(amount * 100);
  if (!['number', 'string'].includes(typeof value) || value === '' || !Number.isFinite(amount) || amount < 0 || !Number.isSafeInteger(cents) || Math.abs(amount * 100 - cents) > 0.00001) fail('The statement amount is invalid. Review the original bank evidence.');
  return cents / 100;
}
function bankIdentity(bank) {
  return sourceHash(['tenant-fee-bank-v1', ACCOUNT, String(bank.date || ''), String(bank.reference || ''), String(bank.description || ''), exactMoney(bank.debit || 0), exactMoney(bank.credit || 0), bank.balance == null ? null : Number(bank.balance)]);
}
function assertDraftWritable(store, draft, bank) {
  if (!draft || draft.account !== ACCOUNT) fail('Tenant commission and agreement fees are available only in IndusInd Bank 7883 reconciliation.', 400);
  if (draft.originalStatementPeriod || draft.reconstructedFromFinalized || draft.finalizedAt || draft.status === 'finalized') fail('This statement is locked or belongs to finalized history. Existing reconciliations cannot be changed.');
  const date = String(bank && bank.date || '');
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date) || !Number.isFinite(Date.parse(date + 'T00:00:00Z')) || new Date(date + 'T00:00:00Z').toISOString().slice(0, 10) !== date) fail('The statement transaction date is invalid.');
  if (Object.values(store.bankReconciliationApprovals || {}).some(approval => approval && approval.status === 'pending' && (approval.draftId === draft.id || approval.account === ACCOUNT && (!approval.from || date >= approval.from) && (!approval.to || date <= approval.to)))) fail('This reconciliation is awaiting Owner approval. Withdraw and review that request before changing a tenant fee.');
  for (const [key, book] of Object.entries(store.bankStatements || {})) {
    if (!book || !(key === ACCOUNT || key.endsWith('|' + ACCOUNT) || book.account === ACCOUNT)) continue;
    if (book.reconciledThrough && date <= book.reconciledThrough) fail('This transaction belongs to a finalized bank period. Existing reconciliations cannot be changed.');
  }
}
function containsReference(value, refs) {
  if (typeof value === 'string') return refs.has(value);
  if (Array.isArray(value)) return value.some(item => containsReference(item, refs));
  return !!value && typeof value === 'object' && Object.entries(value).some(([key, item]) => refs.has(key) || containsReference(item, refs));
}
function possibleSameBankRow(left, right) {
  if (!left || !right || String(left.date || '') !== String(right.date || '')) return false;
  // Reference formatting and optional narration/balance can differ between
  // uploads. A weaker or ambiguous identity must never authorize a new source.
  try { if (exactMoney(left.debit || 0) !== exactMoney(right.debit || 0) || exactMoney(left.credit || 0) !== exactMoney(right.credit || 0)) return false; }
  catch (_) { return true; }
  const reference = value => String(value || '').toLowerCase().replace(/[^a-z0-9]/g, ''), description = value => String(value || '').toLowerCase().replace(/\s+/g, ' ').trim();
  const leftRef = reference(left.reference), rightRef = reference(right.reference);
  if (leftRef && rightRef) return leftRef === rightRef;
  const leftDescription = description(left.description), rightDescription = description(right.description);
  if (!leftDescription || !rightDescription || leftDescription === rightDescription) return true;
  const balance = value => value != null && value !== '' && Number.isFinite(Number(value)) ? Number(value) : null;
  const leftBalance = balance(left.balance), rightBalance = balance(right.balance);
  return leftBalance == null || rightBalance == null || Math.abs(leftBalance - rightBalance) < 0.005;
}
function foreignBankClaimError(store, draft, bank) {
  for (const other of Object.values(store.bankReconciliationDrafts || {})) {
    if (!other || other === draft || other.id === draft.id || other.account !== ACCOUNT) continue;
    for (const [rowId, resolution] of Object.entries(other.resolutions || {})) {
      if (!resolution || resolution.action === 'unmatch_suggestion') continue;
      const ids = new Set([rowId, resolution.linkedRowId, resolution.primaryRowId, resolution.bankRowId].concat(list(resolution.bankRowIds), list(resolution.bankReferences).map(item => item && item.rowId)).filter(id => String(id || '').startsWith('bank-')));
      for (const id of ids) {
        const index = /^bank-(\d+)$/.exec(String(id)), original = index && list(other.transactions)[Number(index[1])];
        if (original && possibleSameBankRow(bank, original)) return 'This bank transaction already has a saved review in another reconciliation, or its identity is ambiguous. Review that saved decision instead of creating another tenant fee.';
        // A malformed grouped/counterpart reference cannot erase a physical
        // claim when the other statement still contains this bank evidence.
        if (!original && list(other.transactions).some(candidate => possibleSameBankRow(bank, candidate))) return 'Another saved reconciliation has an unverifiable bank-row claim for this transaction. Review that saved decision before recording a tenant fee.';
      }
    }
  }
  return '';
}
function assertNoDuplicate(store, draft, rowId, evidence) {
  const same = item => item && (item.draftId === draft.id && item.rowId === rowId || item.bankSignature && item.bankSignature === evidence.bankSignature);
  if (draft.resolutions && draft.resolutions[rowId] || list(store.rentals && store.rentals.fees).some(fee => active(fee) && same(fee.bankReconciliationEvidence)) || list(store.receipts).some(receipt => same(receipt.bankReconciliationEvidence)) || Object.values(store.expenses || {}).some(expense => same(expense.reconciliationSource) || same(expense.bankReconciliationEvidence))) fail('This bank transaction already has a recorded source. Link or review it instead of creating another tenant fee.');
  const claimError = foreignBankClaimError(store, draft, list(draft.transactions)[Number(String(rowId).slice(5))]);
  if (claimError) fail(claimError);
}
function record(store, context) {
  const { draft, row, body, register, nature, actor, now, canApproveExpense } = context;
  const kind = String(body.kind || ''), spec = isFeeKind(kind) ? SPECS[kind] : null;
  if (!spec) fail('Choose dealer commission or documentation / agreement fee.', 400);
  if (Object.keys(body).some(key => !['draftId', 'rowId', 'tenancyId', 'kind', 'note'].includes(key))) fail('Only the tenant, fee kind and reconciliation remark may be supplied; amount, date and account come from the bank statement.', 400);
  if (!row || !row.bank || row.status !== 'missing_in_app' || row.app || row.resolution) fail('Choose an unmatched bank transaction that has not been reviewed.', 400);
  const bank = row.bank, debit = exactMoney(bank.debit || 0), credit = exactMoney(bank.credit || 0);
  if ((debit > 0) === (credit > 0)) fail('Choose a statement row with exactly one payment or receipt amount.', 400);
  assertDraftWritable(store, draft, bank);
  if (row.reversalReviewRequired || row.reversalPairCandidate || row.bankReversalPair || bank.reversal) fail('Review the bank reversal and its original transaction instead of recording a tenant fee.');
  if (row.entityTag && (row.entityTag.sourceConflict || ['mixed_linked_entries', 'ambiguous_candidates'].includes(row.entityTag.source)) || list(row.reconciliationConflicts).length) fail('Resolve the conflicting source ownership before recording this tenant fee.');
  if (!['SAMAST', 'PERSONAL'].includes(nature)) fail('Tag this 7883 bank transaction as SAMAST or PERSONAL before recording a tenant fee.');
  if (debit && !canApproveExpense) fail('Expense approval and payment authority is required for the selected accounting entity.', 403);
  const tenancies = list(register && register.tenancies).filter(tenancy => tenancy.id === String(body.tenancyId || ''));
  if (tenancies.length !== 1) fail('Choose one existing rental tenant / floor.', 400);
  const tenancy = tenancies[0], note = String(body.note || '').trim();
  if (!note || note.length > 2000) fail('Enter a reconciliation remark of no more than 2,000 characters.', 400);
  const rowIndex = /^bank-(\d+)$/.exec(String(body.rowId || '')), original = rowIndex && list(draft.transactions)[Number(rowIndex[1])];
  if (!original || bankIdentity(original) !== bankIdentity(bank)) fail('The selected transaction no longer matches the retained bank evidence. Reload the statement.');
  const evidence = { draftId: draft.id, rowId: body.rowId, reference: String(bank.reference || ''), description: String(bank.description || ''), bankSignature: bankIdentity(bank) };
  assertNoDuplicate(store, draft, body.rowId, evidence);
  if (register.fees != null && !Array.isArray(register.fees)) fail('The tenant fee register cannot be verified. Nothing was recorded.');
  const direction = debit ? 'payment' : 'receipt', amount = debit || credit, id = 'TF-' + randomUUID(), source = [tenancy.tenant, tenancy.property, tenancy.floor].filter(Boolean).join(' · ');
  const fee = { id, tenancyId: tenancy.id, kind, direction, amount, date: bank.date, account: ACCOUNT, nature, expenseId: '', paymentId: '', receiptId: '', bankReconciliationEvidence: clone(evidence), note, createdBy: actor, createdAt: now };
  const linkage = { tenancyId: tenancy.id, rentalKind: kind, rentalFeeId: id, bankReconciliationEvidence: clone(evidence) };
  let expense, receipt;
  if (direction === 'payment') {
    let seq = Number.isSafeInteger(store.seq) ? store.seq : 0, expenseId;
    do { expenseId = 'EX-' + String(++seq).padStart(5, '0'); } while ((store.expenses || {})[expenseId]);
    const paymentId = 'PAY-001', payment = { id: paymentId, amount, date: bank.date, account: ACCOUNT, paymentType: 'UPI', proof: '', note: 'Verified against uploaded bank statement · ' + (bank.reference || draft.id), paidBy: actor, paidAt: now, personalFunds: false, bankReconciliationDraft: draft.id, bankReconciliationRow: body.rowId, ...clone(linkage) };
    expense = { id: expenseId, date: bank.date, particulars: spec.label + ' · ' + source, amount, requestedAmount: amount, isInstallment: false, nature, type: 'variable', ledger: spec.category, vendor: String(bank.description || spec.label).trim().slice(0, 120), claimant: actor, account: ACCOUNT, channel: 'Shared', bill: 'none', fundedBy: 'company', paymentType: 'UPI', qrPhoto: '', billPhoto: '', purchasePaymentProof: '', exceptionEvidence: 'Uploaded bank statement', exceptionReason: note, billNote: 'Tenant fee recorded from verified bank statement', paidAlready: false, personalPaidAmount: 0, reimbursementStatus: 'not_applicable', reimbursementAmount: 0, reimbursementPayments: [], paymentProof: '', status: 'paid', paidAmount: amount, payments: [payment], createdBy: actor, createdAt: now, approvedBy: actor, approvedAt: now, paidBy: actor, paidAt: now, reconciliationSource: { ...clone(evidence), bankReference: evidence.reference, bankDescription: evidence.description, remark: note }, ...clone(linkage) };
    // Match the normal store's empty proof/provisional fields up front so a
    // read-only reload cannot make unchanged source evidence look different.
    Object.assign(expense, { billPhotos: [], purchasePaymentProofs: [], paymentProofs: [], provisionalPaidAmount: 0 });
    payment.proofs = [];
    store.seq = seq; store.expenses = store.expenses || {}; store.expenses[expenseId] = expense;
    fee.expenseId = expenseId; fee.paymentId = paymentId; fee.sourceFingerprint = sourceHash(expense);
  } else {
    let seq = Number.isSafeInteger(store.receiptSeq) ? store.receiptSeq : 0, receiptId;
    do { receiptId = 'REC-' + String(++seq).padStart(5, '0'); } while (list(store.receipts).some(item => item.id === receiptId));
    receipt = { id: receiptId, nature, account: ACCOUNT, amount, receiptType: spec.receiptType, category: spec.category, source, date: bank.date, note, proof: '', proofs: [], proofException: 'Verified directly against uploaded bank statement', createdBy: actor, createdAt: now, ...clone(linkage) };
    store.receiptSeq = seq; store.receipts = list(store.receipts); store.receipts.push(receipt);
    fee.receiptId = receiptId; fee.sourceFingerprint = sourceHash(receipt);
  }
  register.fees = list(register.fees); register.fees.push(fee);
  register.activity = list(register.activity); register.activity.push({ at: now, by: actor, action: 'bank_tenant_fee_' + direction, tenancyId: tenancy.id, feeId: id, detail: spec.label + ' · ' + (fee.receiptId || fee.expenseId + '/' + fee.paymentId) + ' · ' + note });
  register.revision = (Number(register.revision) || 0) + 1; store.rentals = register;
  draft.resolutions = draft.resolutions || {};
  draft.resolutions[body.rowId] = { action: receipt ? 'create_receipt' : 'create_expense', reason: note, remark: note, category: spec.category, appId: receipt ? receipt.id : expense.id + '/' + fee.paymentId, receiptId: fee.receiptId, expenseId: fee.expenseId, rentalKind: kind, rentalFeeId: id, tenancyId: tenancy.id, rentalRecordIds: [id], postingNature: nature, by: actor, at: now };
  return { fee, ...(receipt ? { receipt } : { expense }), direction, rentalKind: kind, tenancy: { id: tenancy.id, tenant: tenancy.tenant, floor: tenancy.floor, property: tenancy.property } };
}
function undo(store, context) {
  const { draft, rowId, actor, now, reason } = context, resolution = draft && draft.resolutions && draft.resolutions[rowId];
  if (!resolution || !resolution.rentalFeeId) fail('Choose a saved tenant fee review.', 404);
  const index = /^bank-(\d+)$/.exec(String(rowId)), bank = index && list(draft.transactions)[Number(index[1])];
  assertDraftWritable(store, draft, bank);
  const fees = list(store.rentals && store.rentals.fees).filter(fee => fee.id === resolution.rentalFeeId && active(fee));
  if (fees.length !== 1) fail('The tenant fee source cannot be identified safely. Nothing was removed.');
  const fee = fees[0], evidence = fee.bankReconciliationEvidence;
  if (!bank || !evidence || evidence.draftId !== draft.id || evidence.rowId !== rowId || evidence.bankSignature !== bankIdentity(bank) || fee.tenancyId !== resolution.tenancyId || fee.kind !== resolution.rentalKind) fail('The saved tenant fee or its bank evidence changed. Review its source before undoing.');
  if (!String(reason || '').trim()) fail('Enter a reason for discarding this saved tenant fee review.', 400);
  const source = fee.direction === 'receipt' ? list(store.receipts).filter(receipt => receipt.id === fee.receiptId) : Object.values(store.expenses || {}).filter(expense => expense.id === fee.expenseId);
  if (source.length !== 1 || source[0].rentalFeeId !== fee.id || source[0].tenancyId !== fee.tenancyId || sourceHash(source[0]) !== fee.sourceFingerprint) fail('The linked transaction changed after this fee was recorded. Use its audited source correction workflow; nothing was removed.');
  const record = source[0], appId = fee.receiptId || fee.expenseId + '/' + fee.paymentId, refs = new Set([fee.id, appId, fee.expenseId || fee.receiptId]);
  const otherDrafts = Object.values(store.bankReconciliationDrafts || {}).map(item => item.id === draft.id ? { ...item, resolutions: Object.fromEntries(Object.entries(item.resolutions || {}).filter(([id]) => id !== rowId)), decisionAudit: [] } : item);
  const dependencies = [store.bankStatements, otherDrafts, store.bankDateOverrides, store.bankReconciliationLinks, store.bankMovementAuthority, list(store.expenseRefunds).filter(refund => refund.status !== 'voided'), store.vendorAdvances, store.receivables, store.transfers, store.adjustments, store.reconciliationExpenses, store.loanRepayments];
  if (dependencies.some(value => containsReference(value, refs))) fail('This tenant fee is linked to another review, finalized entry, refund or settlement. Resolve that dependency first; nothing was removed.');
  if (fee.direction === 'receipt' && ['allocations', 'credits', 'deposits'].some(key => containsReference(store.rentals[key], refs))) fail('This receipt has been used in the rental register. Reverse that dependent allocation first.');
  const before = { fee: clone(fee), source: clone(record), resolution: clone(resolution) };
  if (fee.direction === 'receipt') store.receipts = store.receipts.filter(receipt => receipt.id !== fee.receiptId);
  else delete store.expenses[fee.expenseId];
  Object.assign(fee, { reversedAt: now, reversedBy: actor, reversalReason: String(reason).trim() });
  store.rentals.activity = list(store.rentals.activity); store.rentals.activity.push({ at: now, by: actor, action: 'undo_bank_tenant_fee', tenancyId: fee.tenancyId, feeId: fee.id, detail: String(reason).trim() });
  store.rentals.revision = (Number(store.rentals.revision) || 0) + 1;
  delete draft.resolutions[rowId];
  draft.decisionAudit = list(draft.decisionAudit); draft.decisionAudit.push({ action: 'undo_tenant_fee', rowId, previous: clone(resolution), reason: String(reason).trim(), by: actor, at: now });
  return { before, fee: clone(fee), appId };
}
function evidenceError(store, draft) {
  const savedFees = list(store.rentals && store.rentals.fees).filter(fee => active(fee) && fee.bankReconciliationEvidence && fee.bankReconciliationEvidence.draftId === draft.id);
  const resolutions = Object.entries(draft.resolutions || {}).filter(([, resolution]) => resolution && resolution.rentalFeeId);
  if (savedFees.length !== resolutions.length) return 'The tenant fee source and saved reconciliation reviews disagree. Review that fee before finalizing.';
  for (const [rowId, resolution] of resolutions) {
    const matches = savedFees.filter(fee => fee.id === resolution.rentalFeeId), index = /^bank-(\d+)$/.exec(rowId), bank = index && list(draft.transactions)[Number(index[1])];
    if (matches.length !== 1 || !bank) return 'The tenant fee or original bank row cannot be identified uniquely.';
    const claimError = foreignBankClaimError(store, draft, bank);
    if (claimError) return claimError;
    const fee = matches[0], source = fee.direction === 'payment' ? (store.expenses || {})[fee.expenseId] : list(store.receipts).find(receipt => receipt.id === fee.receiptId), appId = fee.receiptId || fee.expenseId + '/' + fee.paymentId;
    if (!isFeeKind(fee.kind) || !['payment', 'receipt'].includes(fee.direction) || !source || sourceHash(source) !== fee.sourceFingerprint || fee.tenancyId !== source.tenancyId || fee.id !== source.rentalFeeId || fee.kind !== source.rentalKind || resolution.tenancyId !== fee.tenancyId || resolution.rentalKind !== fee.kind || resolution.appId !== appId || resolution.postingNature !== fee.nature || source.nature !== fee.nature || source.date !== bank.date || source.account !== ACCOUNT || Number(fee.amount) !== Number(source.amount) || fee.bankReconciliationEvidence.rowId !== rowId || fee.bankReconciliationEvidence.bankSignature !== bankIdentity(bank) || Number(source.amount) !== Number(fee.direction === 'payment' ? bank.debit : bank.credit)) return 'The tenant fee, linked source or bank evidence changed after review. Review the source before finalizing; nothing was posted.';
  }
  return '';
}
module.exports = { ACCOUNT, SPECS, isFeeKind, bankIdentity, assertDraftWritable, record, undo, evidenceError };

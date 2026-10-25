'use strict';

// Shared, read-only locks for source-owned corrections. A source workflow may
// update its own linked records, but may never detach independent bank evidence.
const text = value => String(value == null ? '' : value).trim();
const list = value => Array.isArray(value) ? value : [];
const values = value => value && typeof value === 'object' ? Object.values(value) : [];
function fail(message) { const error = new Error(message); error.status = 409; throw error; }
function validDate(value) {
  return typeof value === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(value) &&
    Number.isFinite(Date.parse(value + 'T00:00:00Z')) && new Date(value + 'T00:00:00Z').toISOString().slice(0, 10) === value;
}
function references(value, into = new Set()) {
  if (typeof value === 'string') {
    into.add(value);
    for (const token of value.split(/\s*,\s*/)) { into.add(token); if (token.endsWith('/RECEIVED')) into.add(token.replace(/\/RECEIVED$/, '')); }
  }
  else if (Array.isArray(value)) value.forEach(item => references(item, into));
  else if (value && typeof value === 'object') for (const [key, item] of Object.entries(value)) { references(key, into); references(item, into); }
  return into;
}
function assertUnlocked(store, options = {}) {
  if (!store || typeof store !== 'object' || Array.isArray(store)) fail('The accounting store cannot be verified.');
  for (const key of ['bankStatements', 'bankDateOverrides', 'bankReconciliationLinks', 'bankReconciliationDrafts', 'bankReconciliationApprovals', 'bankMovementAuthority', 'openingBalanceDates', 'openingBalanceDatesByNature']) {
    if (store[key] != null && (typeof store[key] !== 'object' || Array.isArray(store[key]))) fail('Saved accounting protection evidence is invalid. Nothing can be corrected until it is verified.');
  }
  if (store.cashReconciliations != null && !Array.isArray(store.cashReconciliations)) fail('Saved cash protection evidence is invalid.');
  const ids = new Set(list(options.ids).map(text).filter(Boolean));
  const scopes = list(options.scopes), dates = Array.from(new Set(list(options.dates)));
  if (!ids.size || !scopes.length || !dates.length || dates.some(date => !validDate(date))) fail('Exact movement identity, account scope and valid original/proposed dates are required.');
  const forbidden = ['bankReconciliationDraft', 'bankReconciliationEvidence', 'reconciliationDraft', 'reconciliation', 'reconciled', 'reconciledAt', 'reconciliationId', 'finalizedAt', 'locked', 'bankTransactionId', 'bankStatementEvidence', 'bankReconciliation', 'bankReconciliationLink', 'bankMovementAuthority', 'confirmedBankSplit', 'creditCardStatementId', 'statementId', 'salaryAdvanceId', 'gokwikPostingId', 'paytmTransactionId', 'paytmSettlementId', 'settledThroughPaytm', 'settledByTransferId', 'settlementId', 'loanId', 'manualSaleId', 'tenancyId', 'rentalKind', 'batchPaymentId', 'batchId', 'openingBalanceTreatment'];
  for (const record of list(options.records)) {
    if (!record || typeof record !== 'object' || Array.isArray(record)) fail('A linked source record is missing or invalid.');
    if (record.accountingExcluded || record.active === false || record.historicalSettlement || record.provisional || record.voidedAt || record.deletedAt || record.reversedAt || record.supersededBy || ['voided', 'cancelled', 'canceled', 'reversed', 'rejected', 'deleted'].includes(record.status)) fail('Excluded, historical, voided or provisional movements cannot be corrected here.');
    if (forbidden.some(field => !!record[field]) || list(record.batchPaymentIds).length || list(record.reimbursementAllocations).length) fail('This source movement has protected reconciliation or another workflow evidence. Correct that workflow first.');
  }
  const bank = references([store.bankDateOverrides, store.bankReconciliationLinks, store.bankStatements, store.bankReconciliationDrafts, store.bankMovementAuthority, store.bankAmountAuthorityApplications, store.bankAmountAuthorityProposals]);
  if ([...ids].some(id => bank.has(id))) fail('This movement is reconciled or linked to an active bank review. Its existing reconciliation must be handled through the normal workflow first.');
  // Only the exact owned graph is exempt. A separate receipt, reimbursement,
  // loan, settlement or refund that refers to it remains an independent lock.
  const foreign = new Set();
  for (const key of ['transfers','receipts','expenses','adjustments','reconciliationExpenses','expenseRefunds','vendorAdvances','vendorOpeningPayables','receivables','refunds','loanRepayments','creditCardSettlementAllocations','gokwikPostings','paytmSettlements','reimbursements','salaryAdvanceTransfers','rentals']) {
    for (const [recordKey, record] of Object.entries(store[key] || {})) {
      if (!record || typeof record !== 'object' || ids.has(text(record.id)) || ids.has(recordKey)) continue;
      references(record, foreign);
    }
  }
  if ([...ids].some(id => foreign.has(id))) fail('Another accounting record depends on this movement. Correct that dependent workflow first.');
  const external = references(options.external || {});
  if ([...ids].some(id => external.has(id))) fail('This movement is linked to another accounting source. Correct its dependent source first.');
  const bookKey = typeof options.bookKey === 'function' ? options.bookKey : (nature, account) => nature === 'PERSONAL' ? nature + '|' + account : account;
  const accountKey = value => text(typeof options.canonicalAccount === 'function' ? options.canonicalAccount(value) : value).toLowerCase();
  const bookAccountKey = value => accountKey(text(value).replace(/^(PERSONAL|SAMAST|SANKI)\|/i, ''));
  for (const scope of scopes) {
    const account = text(scope && scope.account), nature = text(scope && scope.nature), nonMonetary = !account && scope && scope.nonMonetary === true;
    if (!account && !nonMonetary || !['SANKI', 'SAMAST', 'PERSONAL'].includes(nature)) fail('The saved account/entity scope is invalid.');
    const books = store.bankStatements || {}, keys = new Set([bookAccountKey(bookKey(nature, account)), accountKey(account)]);
    // Physical account evidence is shared even when a row is shown in another
    // Results entity. Changing its display context cannot evade a closed book.
    for (const [key, book] of Object.entries(books)) {
      if (!book || typeof book !== 'object' || Array.isArray(book)) fail('Saved bank-period evidence is invalid.');
      if (book.imports != null && !Array.isArray(book.imports)) fail('Saved bank import evidence is invalid.');
      if (nonMonetary ? book.nature && book.nature !== nature || key.toUpperCase().startsWith('PERSONAL|') && nature !== 'PERSONAL' : !keys.has(bookAccountKey(key)) && accountKey(book.account) !== accountKey(account)) continue;
      if (book.reconciledThrough && (!validDate(book.reconciledThrough) || dates.some(date => date <= book.reconciledThrough))) fail('This movement touches a finalized bank period. Its amount and date are locked.');
    }
    const openings = [store.openingBalanceDates || {}, ...values(store.openingBalanceDatesByNature)];
    if (!nonMonetary && openings.some(map => Object.entries(map || {}).some(([key,date])=>accountKey(key)===accountKey(account)&&date&&(!validDate(date)||dates.some(value=>value<date))))) fail('This correction would affect a period before the account opening.');
    if (list(store.cashReconciliations).some(row => row && ['approved', 'pending'].includes(row.status) && (nonMonetary ? !row.nature || row.nature === nature : accountKey(row.account) === accountKey(account)) && (!validDate(row.verifiedThrough) || dates.some(date => date <= row.verifiedThrough)))) fail('This movement touches an approved or pending cash verification. Resolve that verification first.');
    for (const approval of values(store.bankReconciliationApprovals)) {
      if (!approval || typeof approval !== 'object' || Array.isArray(approval)) fail('Saved Owner approval evidence is invalid.');
      if (!approval || approval.status !== 'pending') continue;
      const draft = (store.bankReconciliationDrafts || {})[approval.draftId] || {};
      if (typeof draft !== 'object' || Array.isArray(draft) || draft.summary != null && (typeof draft.summary !== 'object' || Array.isArray(draft.summary))) fail('The pending approval statement evidence is invalid.');
      const approvalAccount = text(approval.account || draft.account);
      if (!approvalAccount) fail('A pending Owner approval has incomplete account evidence. Resolve it before correcting financial records.');
      if (nonMonetary ? (approval.nature || draft.nature) && (approval.nature || draft.nature) !== nature : accountKey(approvalAccount) !== accountKey(account) && accountKey(draft.account) !== accountKey(account)) continue;
      if (approval.account && draft.account && accountKey(approval.account) !== accountKey(draft.account)) fail('A pending Owner approval has inconsistent account evidence. Resolve it before correction.');
      if (['from','to'].some(key => approval[key] && draft.summary && draft.summary[key] && approval[key] !== draft.summary[key])) fail('A pending Owner approval has inconsistent statement boundaries. Resolve it before correction.');
      const from = text(approval.from || draft.summary && draft.summary.from), to = text(approval.to || draft.summary && draft.summary.to);
      if (from && !validDate(from) || to && !validDate(to) || from && to && from > to || dates.some(date => (!from || date >= from) && (!to || date <= to))) fail('This movement touches a statement awaiting Owner approval. Withdraw or resolve that approval through its normal workflow before correcting it.');
    }
  }
}
function assertPreserved(before, after, allowedKeys) {
  if (!before || !after || typeof before !== 'object' || typeof after !== 'object' || Array.isArray(before) || Array.isArray(after)) fail('The correction snapshot is invalid.');
  const allowed = new Set(allowedKeys);
  for (const key of new Set([...Object.keys(before), ...Object.keys(after)])) {
    if (!allowed.has(key) && JSON.stringify(before[key]) !== JSON.stringify(after[key])) fail('The correction attempted to change unrelated accounting records or reconciliation evidence. Nothing was saved.');
  }
}
function assertAudit(store, key = 'auditLog', prefix = 'AUD-') {
  const rows=store[key],seq=store.auditSeq;
  if(rows!=null&&!Array.isArray(rows)||seq!=null&&(!Number.isSafeInteger(seq)||seq<0)||seq==null&&list(rows).length)fail('The existing audit history or sequence cannot be verified. Nothing was changed.');
  const next=(seq||0)+1,seen=new Set();
  if(!Number.isSafeInteger(next))fail('The audit sequence is exhausted. Nothing was changed.');
  for(const row of list(rows)){
    if(!row||typeof row!=='object'||Array.isArray(row))fail('The saved audit entry is invalid. Nothing was changed.');
    const id=text(row.id);
    if(id&&seen.has(id))fail('The saved audit identities are duplicated. Nothing was changed.');
    if(id)seen.add(id);
    if(id.startsWith(prefix)&&/^\d+$/.test(id.slice(prefix.length))&&Number(id.slice(prefix.length))>=(next))fail('The audit sequence conflicts with retained history. Nothing was changed.');
  }
}
module.exports = { assertUnlocked, assertPreserved, assertAudit, references, validDate };

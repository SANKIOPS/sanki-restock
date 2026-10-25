'use strict';
// Bounded, read-only evidence projections. Never repair or save a reconciliation
// while explaining a claim or an existing payment's retained bank provenance.
const clone = value => value == null ? null : JSON.parse(JSON.stringify(value));
const pick = (value, fields) => Object.fromEntries(fields.filter(key => value && Object.prototype.hasOwnProperty.call(value, key)).map(key => [key, clone(value[key])]));
const list = value => Array.isArray(value) ? value : [];
const linkedIds = row => [...new Set([...list(row && row.linkedRecordIds), row && row.ledger && row.ledger.id].filter(Boolean))];
function snapshot(store, options) {
  const { account, nature, bookKey, movementId = '', draftId = '', rowId = '' } = options;
  if (!movementId && !(draftId && /^bank-\d+$/.test(rowId))) return { error: 'Select an exact recorded movement or saved bank row.', status: 400 };
  if (draftId && !/^bank-\d+$/.test(rowId)) return { error: 'Select an exact saved bank row.', status: 400 };
  const output = { success: true, account, nature, readOnly: true };
  if (movementId) {
    const book = (store.bankStatements || {})[bookKey] || {}, records = [];
    for (const record of list(book.imports)) {
      if (!record || typeof record !== 'object') continue;
      const rows = [...list(record.reconciliationRows), ...list(record.carriedReconciliationRows)].filter(row => linkedIds(row).includes(movementId));
      if (!rows.length) continue;
      records.push({ ...pick(record, ['id', 'draftId', 'from', 'to', 'uploadedAt', 'finalizedAt', 'finalizedBy', 'reconciliationBankCoverageComplete']),
        rows: rows.map(row => ({ ...pick(row, ['id', 'decision', 'corrected', 'resolutionAction', 'linkedRecordIds']), bank: pick(row.bank, ['transactionId', 'date', 'debit', 'credit', 'reference', 'description', 'balance']), ledger: pick(row.ledger, ['id', 'date', 'debit', 'credit']) })) });
    }
    if (!records.length) return { error: 'No finalized bank evidence for this movement exists in the selected account.', status: 404 };
    const bankRows = records.flatMap(record => record.rows.map(row => row.bank)), ids = new Set(bankRows.map(row => row.transactionId).filter(Boolean));
    const origins = new Set([...records.flatMap(record => [record.id, record.draftId]), (store.bankDateOverrides || {})[movementId] && store.bankDateOverrides[movementId].reconciliationDraft, (store.bankReconciliationLinks || {})[movementId] && store.bankReconciliationLinks[movementId].reconciliationDraft].filter(Boolean));
    output.movement = { id: movementId, records, bankDateOverride: clone((store.bankDateOverrides || {})[movementId]), bankReconciliationLink: clone((store.bankReconciliationLinks || {})[movementId]),
      storedTransactions: Object.values(book.transactions || {}).filter(bank => bank && (ids.has(bank.id) || bankRows.some(row => ['date', 'debit', 'credit', 'reference', 'description'].every(key => (row[key] || '') === (bank[key] || ''))))).map(bank => pick(bank, ['id', 'date', 'debit', 'credit', 'reference', 'description', 'firstSeenImport', 'lastSeenImport', 'reconciliationDraftId', 'reconciliationDraftRowIndex'])),
      finalizationAudit: list(store.auditLog).filter(entry => entry && entry.action === 'BANK_RECONCILIATION_FINALIZED' && entry.account === account && entry.nature === nature && entry.after && (origins.has(entry.after.draftId) || records.some(record => record.finalizedAt === entry.after.at))).map(entry => ({ ...pick(entry, ['id', 'at', 'user', 'action', 'subjectType', 'subjectId', 'account', 'nature']), after: pick(entry.after, ['at', 'by', 'through', 'draftId', 'transactionsReconciled', 'balanceReconciled']) })) };
  }
  if (draftId) {
    const draft = (store.bankReconciliationDrafts || {})[draftId], index = Number(rowId.replace('bank-', ''));
    if (!draft || draft.account !== account || String(draft.nature || 'SANKI').toUpperCase() !== nature || !Number.isSafeInteger(index) || !list(draft.transactions)[index]) return { error: 'The saved bank row does not belong to this account and reconciliation.', status: 404 };
    output.pending = { ...pick(draft, ['id', 'account', 'nature', 'createdAt', 'createdBy']), rowId, bank: pick(draft.transactions[index], ['date', 'debit', 'credit', 'reference', 'description', 'balance']), decision: clone((draft.resolutions || {})[rowId]),
      postedTransfers: list(store.transfers).filter(transfer => transfer && (transfer.reconciliationDraft || transfer.bankReconciliationEvidence && transfer.bankReconciliationEvidence.draftId) === draftId && (transfer.bankRowId || transfer.bankReconciliationEvidence && transfer.bankReconciliationEvidence.rowId) === rowId).map(transfer => pick(transfer, ['id', 'date', 'amount', 'receivedAmount', 'fromAccount', 'toAccount', 'fromNature', 'toNature', 'classification', 'accountingExcluded'])),
      approvals: Object.values(store.bankReconciliationApprovals || {}).filter(approval => approval && approval.draftId === draftId).map(approval => pick(approval, ['id', 'status', 'requestedAt', 'requestedBy', 'approvedAt', 'approvedBy'])) };
  }
  return output;
}
module.exports = { snapshot };

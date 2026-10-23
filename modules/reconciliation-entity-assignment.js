'use strict';

const crypto = require('crypto');
const NATURES = ['SANKI', 'SAMAST', 'PERSONAL'];
const normalized = value => typeof value === 'string' ? value.trim().toUpperCase() : '';
const bankSignature = bank => crypto.createHash('sha256').update(JSON.stringify(bank)).digest('hex');

// These are draft-only choices, not corrections to an already posted source.
// In particular, an expense's Results entity must not redefine its bank account.
function assignment(draft, rowId) {
  const value = draft && draft.entityAssignments && draft.entityAssignments[rowId];
  const index = String(rowId || '').match(/^bank-(\d+)$/), bank = index && (draft.transactions || [])[Number(index[1])];
  return value && bank && NATURES.includes(normalized(value.nature)) && value.bankSignature === bankSignature(bank) ? value : null;
}

function stale(draft, rowId) { return !!(draft && draft.entityAssignments && Object.prototype.hasOwnProperty.call(draft.entityAssignments, rowId) && !assignment(draft, rowId)); }

function evidenceError(draft) {
  return Object.keys(draft && draft.entityAssignments || {}).some(rowId => stale(draft, rowId))
    ? 'A transaction changed after its bulk entity assignment. Review the original bank evidence and entity choice again before finalizing.' : '';
}

function editable(draft, row) {
  const tag = row && row.entityTag;
  return !!(draft && !draft.originalStatementPeriod && !draft.reconstructedFromFinalized && row && row.bank &&
    /^bank-\d+$/.test(row.id) && !stale(draft, row.id) && row.status === 'missing_in_app' && !row.app && !row.resolution &&
    !(draft.resolutions || {})[row.id] && !row.matchingExclusion && !(draft.matchingExclusions || {})[row.id] &&
    !row.reversalReviewRequired && !row.ledgerClaimedElsewhere && !(row.reconciliationConflicts || []).length &&
    !(tag && (tag.sourceConflict || ['mixed_linked_entries', 'ambiguous_candidates', 'linked_ledger', 'finalized_bank_record'].includes(tag.source))) &&
    !(tag && tag.source === 'manual' && !assignment(draft, row.id)));
}

function register(router, deps) {
  router.post('/api/expenses/bank-statements/assign-entity-bulk', (req, res) => {
    const fail = (message, status = 409) => { const error = new Error(message); error.status = status; throw error; };
    try {
      const b = req.body || {}, s = deps.loadStore(), draft = (s.bankReconciliationDrafts || {})[b.draftId];
      if (!deps.isAdmin(req)) fail('Only the Owner or an Admin can assign entities to selected transactions.', 403);
      if (!draft) fail('This reconciliation draft has expired.', 404);
      if (!deps.canAccessBankDraft(req, s, draft)) fail('You cannot edit this reconciliation.', 403);
      if (b.account !== draft.account || normalized(b.nature) !== normalized(draft.nature)) fail('The selected reconciliation account changed. Refresh and select its rows again.');
      if (draft.originalStatementPeriod || draft.reconstructedFromFinalized) fail('A restored or finalized statement cannot use pending-row entity assignment.');
      if (Object.values(s.bankReconciliationApprovals || {}).some(value => value && value.draftId === draft.id && value.status === 'pending')) fail('This reconciliation is awaiting Owner approval. Withdraw that request before changing its entity choices.');
      const target = normalized(b.targetNature), shared = deps.isShared(draft.account), reason = typeof b.reason === 'string' ? b.reason.trim() : '';
      if (!NATURES.includes(target) || shared && !['SAMAST', 'PERSONAL'].includes(target)) fail('Choose a valid entity for this reconciliation.', 400);
      if (!deps.approvalNatures(req).includes(target) || target === 'PERSONAL' && !deps.canManagePersonalAccounting(req)) fail('You cannot assign the selected accounting entity.', 403);
      if (!reason || reason.length > 2000) fail('Enter a reason of no more than 2,000 characters for this assignment.', 400);
      if (!Array.isArray(b.rowIds) || !b.rowIds.length || b.rowIds.length > 500 || b.rowIds.some(id => typeof id !== 'string' || !/^bank-\d+$/.test(id)) || new Set(b.rowIds).size !== b.rowIds.length) fail('Select between 1 and 500 distinct bank transactions.', 400);
      const rowIds = b.rowIds.slice().sort(), snapshot = JSON.parse(JSON.stringify(s)), detached = snapshot.bankReconciliationDrafts[draft.id];
      const view = deps.draftReconciliation(snapshot, detached, { readOnly: true });
      const rows = rowIds.map(id => view.rows.find(row => row.id === id));
      for (const row of rows) {
        if (!editable(draft, row)) fail('A selected transaction is reviewed, linked, excluded, finalized or ambiguous. Nothing was assigned.');
        const old = assignment(draft, row.id), tag = row.entityTag;
        if (!deps.canManagePersonalAccounting(req) && (old && old.nature === 'PERSONAL' || tag && [tag.nature, tag.sourceNature].includes('PERSONAL'))) fail('The Owner must review this PERSONAL transaction.', 403);
        if (shared && tag && tag.source === 'manual' && tag.nature !== target) fail('A selected transaction already has a reviewed entity. Use its individual review workflow instead.');
      }
      const changes = rows.map(row => ({ rowId: row.id, date: row.bank.date, amount: Number(row.bank.debit || 0) + Number(row.bank.credit || 0), fromNature: (assignment(draft, row.id) || {}).nature || (row.entityTag || {}).nature || '', toNature: target, linkedRecordIds: [] }));
      const fingerprint = crypto.createHash('sha256').update(JSON.stringify({ draft, rowIds, target, reason, rows })).digest('hex');
      if (b.dryRun === true) return res.json({ success: true, fingerprint, updated: changes.filter(row => row.fromNature !== target).length, changes });
      if (b.dryRun !== false || typeof b.fingerprint !== 'string' || b.fingerprint !== fingerprint) fail('This selection was not reviewed or its evidence changed. Refresh the preview; nothing was saved.');
      const at = new Date().toISOString(), by = req.user && req.user.username || 'system';
      const changed = changes.filter(row => row.fromNature !== target);
      for (const change of changed) {
        const before = assignment(draft, change.rowId), row = rows.find(row => row.id === change.rowId), tag = { nature: target, reason, by, at, source: 'manual_bulk', bankSignature: bankSignature(row.bank) };
        draft.entityAssignments = draft.entityAssignments || {}; draft.entityAssignments[change.rowId] = tag;
        if (shared) { draft.transactionOwnership = draft.transactionOwnership || {}; draft.transactionOwnership[change.rowId] = { ...tag, source: 'manual' }; }
        draft.decisionAudit = draft.decisionAudit || []; draft.decisionAudit.push({ action: 'assign_transaction_entity_bulk', rowId: change.rowId, before, after: tag, reason, by, at });
      }
      if (changed.length) {
        deps.audit(s, req, 'BANK_RECONCILIATION_ENTITIES_ASSIGNED', 'bank_reconciliation', draft.id, { nature: target, account: draft.account, note: reason, bankRowIds: rowIds, changes: changed, draftOnly: true, bankMovementPreserved: true });
        deps.saveStore(s);
      }
      return res.json({ success: true, updated: changed.length, draft: deps.draftReconciliation(s, draft, { readOnly: true }) });
    } catch (error) { return res.status(error.status || 409).json({ success: false, error: error.message || 'Nothing was assigned.' }); }
  });
}

module.exports = { assignment, stale, evidenceError, editable, register };

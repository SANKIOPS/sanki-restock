'use strict';

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { planBankImportSourceRepair, applyBankImportSourceRepair } = require('./bank-import-source-repair');

// Reparse retained evidence, never accept a user-entered replacement amount.
// Preview and apply both authenticate, verify the PDF hash and re-read the book.
module.exports = function registerBankImportSourceRepair(router, deps) {
  router.post('/api/expenses/bank-statements/repair-import', async (req, res) => {
    try {
      const body = req.body || {}, initial = deps.loadStore();
      const draft = (initial.bankReconciliationDrafts || {})[body.draftId];
      if (!draft) return res.status(404).json({ success: false, error: 'This statement preview is no longer available.' });
      if (!deps.canAccessBankDraft(req, initial, draft)) return res.status(403).json({ success: false, error: 'You cannot repair this bank statement import.' });
      if (!['preview', 'apply'].includes(body.mode)) return res.status(400).json({ success: false, error: 'Preview the retained statement before applying an import repair.' });
      if ((draft.sourceStatements || []).length > 1) return res.status(409).json({ success: false, error: 'This preview combines multiple source files. Repair requires an individual original statement; no amounts were changed.' });
      const source = (draft.sourceStatements || [])[0] || draft;
      const file = deps.reconciliationFile(deps.statementDraftDir, source.temporaryFile);
      const expectedSourceHash = source.fileHash || draft.fileHash;
      if (!file || !expectedSourceHash || !fs.existsSync(file) || path.extname(source.originalName || file).toLowerCase() !== '.pdf') return res.status(409).json({ success: false, error: 'The retained original PDF is unavailable. No imported or accounting amounts were changed.' });
      const hashFile = () => crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex');
      const sourceHash = hashFile();
      if (sourceHash !== expectedSourceHash) return res.status(409).json({ success: false, error: 'The retained PDF no longer matches its saved evidence hash. No changes were made.' });
      const initialDraft = JSON.stringify(draft);
      const parsedRows = await deps.parseBankStatementUpload(file, source.originalName || path.basename(file), '');
      if (hashFile() !== sourceHash) return res.status(409).json({ success: false, error: 'The source PDF changed while being read. Preview it again.' });
      const store = deps.loadStore(), current = (store.bankReconciliationDrafts || {})[body.draftId];
      if (!current || JSON.stringify(current) !== initialDraft) return res.status(409).json({ success: false, error: 'The statement review changed while the PDF was read. Preview the repair again.' });
      if (!deps.canAccessBankDraft(req, store, current)) return res.status(403).json({ success: false, error: 'You cannot repair this bank statement import.' });
      const reviewStore = JSON.parse(JSON.stringify(store));
      const reviewRows = deps.draftReconciliation(reviewStore, reviewStore.bankReconciliationDrafts[current.id], { readOnly: true }).rows;
      const options = { store, draft: current, parsedRows, sourceHash, expectedSourceHash, reviewRows };
      const plan = planBankImportSourceRepair(options);
      const preview = { success: true, changed: plan.changed, fingerprint: plan.fingerprint, sourceHash, changes: plan.changes, beforeSummary: plan.beforeSummary, afterSummary: plan.afterSummary };
      if (body.mode === 'preview' || !plan.changed) return res.json(preview);
      const reason = String(body.reason || '').trim();
      if (!reason || reason.length > 2000) return res.status(400).json({ success: false, error: 'A repair reason is required (maximum 2,000 characters).' });
      const repaired = applyBankImportSourceRepair({ ...options, expectedFingerprint: body.fingerprint, actor: req.user.username, reason, at: new Date().toISOString() });
      store.bankReconciliationDrafts[current.id] = repaired.nextDraft;
      deps.audit(store, req, 'BANK_STATEMENT_IMPORT_REPAIRED', 'account', current.account, { nature: current.nature, account: current.account, draftId: current.id, sourceHash, before: { summary: plan.beforeSummary, rows: plan.changes.map(change => ({ rowId: change.rowId, ...change.before })) }, after: { summary: plan.afterSummary, rows: plan.changes.map(change => ({ rowId: change.rowId, ...change.after })) }, note: reason + ' Original PDF column evidence used; expenses, payments, adjustments and account openings unchanged.' });
      deps.saveStore(store);
      return res.json({ ...preview, applied: true });
    } catch (error) {
      return res.status(error.status || 409).json({ success: false, error: error.message || 'The retained statement could not be repaired safely. No changes were made.' });
    }
  });
};

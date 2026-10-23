'use strict';

const { buildRestorePlan, applyRestorePlan } = require('./excluded-transfer-restoration');
const clone = value => JSON.parse(JSON.stringify(value));
const fail = (status, message) => { const error = new Error(message); error.status = status; throw error; };

// This route deliberately has no creator, adjustment or finalization dependency.
// Its loader must read persisted state without migrations or repair-on-read writes.
module.exports = function registerExcludedTransferRestoration(router, deps) {
  const authorize = (req, state, intent) => {
    const draft = state.bankReconciliationDrafts && state.bankReconciliationDrafts[intent.draftId];
    if (!draft) fail(404, 'This reconciliation draft is unavailable. Refresh the statement.');
    if (!deps.canAccessBankDraft(req, state, draft)) fail(403, 'You cannot restore a transfer in this bank reconciliation.');
    const transfers = (Array.isArray(state.transfers) ? state.transfers : []).filter(item => item && item.id === intent.transferId);
    if (transfers.length !== 1) fail(409, 'The exact transfer ID is missing or duplicated. Nothing was changed.');
    const transfer = transfers[0];
    if (![transfer.fromAccount, transfer.toAccount].every(account => typeof account === 'string' && deps.canAccessAccount(req, state, account))) {
      fail(403, 'Both transfer accounts must be accessible to the Owner before restoration.');
    }
  };
  const handler = mode => (req, res) => {
    try {
      // A PERSONAL permission or Admin role does not authorize reversing an
      // Owner exclusion, even when that role can ordinarily log transfers.
      if (!deps.isOwner(req)) fail(403, 'Only the Owner can restore an excluded transfer.');
      const body = req.body || {}, intent = { draftId: body.draftId, rowId: body.rowId, transferId: body.transferId };
      if (Object.values(intent).some(value => typeof value !== 'string' || !value.trim() || value.length > 200)) fail(400, 'Choose the exact draft, bank row and excluded transfer ID.');
      Object.keys(intent).forEach(key => { intent[key] = intent[key].trim(); });
      const reason = typeof body.reason === 'string' ? body.reason.trim() : '';
      if (mode === 'apply' && (body.confirm !== true || reason.length < 10 || reason.length > 2000 || !/^[a-f0-9]{64}$/.test(body.fingerprint || ''))) {
        fail(400, 'Preview first, explicitly confirm, and enter a restoration reason of 10–2,000 characters.');
      }
      const state = deps.loadStore();
      authorize(req, state, intent);
      const before = JSON.stringify(state), work = clone(state);
      if (mode === 'preview') {
        const plan = buildRestorePlan(work, intent, deps.helpers);
        if (JSON.stringify(work) !== before) fail(409, 'The restoration preview attempted to change accounting state. Nothing was saved.');
        return res.json({ success: true, plan });
      }
      // Never trust a client-supplied plan, source account, bank evidence,
      // exclusion snapshot or array of records to clear.
      const result = applyRestorePlan(work, { version: 1, intent, fingerprint: body.fingerprint }, {
        user: req.user && req.user.username, reason, at: new Date().toISOString()
      }, deps.helpers);
      if (result.alreadyRestored) return res.json({ success: true, ...result });
      if (!result.restored) fail(409, 'The existing transfer was not restored. Nothing was saved.');
      // Synchronous recheck closes the interval between evidence review and
      // commit without running any repair or migration on unrelated records.
      const fresh = deps.loadStore();
      authorize(req, fresh, intent);
      if (JSON.stringify(fresh) !== before) fail(409, 'Accounting state changed during restoration. Preview again; nothing was saved.');
      deps.saveStore(work);
      return res.json({ success: true, ...result });
    } catch (error) {
      return res.status(error.status || 409).json({ success: false, error: error.message || 'The excluded transfer could not be restored safely. Nothing was changed.' });
    }
  };
  router.post('/api/expenses/bank-statements/restore-excluded-transfer/preview', handler('preview'));
  router.post('/api/expenses/bank-statements/restore-excluded-transfer/apply', handler('apply'));
};

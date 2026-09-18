const crypto = require('crypto');
const path = require('path');
const { parsePaytmReport, summarizePayouts } = require('./paytm-report');

function registerPaytmReports(router, deps) {
  const { loadStore, saveStore, audit, canAccess, upload, view, today } = deps;
  const deny = (req, res) => !canAccess(req) ? (res.status(403).json({ success: false, error: 'You cannot access Paytm reconciliation.' }), true) : false;
  router.get('/api/expenses/paytm-reports', (req, res) => {
    if (deny(req, res)) return;
    res.json({ success: true, ...view(loadStore()) });
  });
  router.post('/api/expenses/paytm-reports/preview', upload.array('reports', 20), (req, res) => {
    if (deny(req, res)) return;
    const files = req.files || [];
    if (!files.length) return res.status(400).json({ success: false, error: 'Choose one or more Paytm CSV reports.' });
    try {
      const store = loadStore(), all = new Map(), sources = [], warnings = [];
      for (const file of files) {
        if (path.extname(file.originalname || '').toLowerCase() !== '.csv') throw new Error('Only Paytm CSV files are supported.');
        const parsed = parsePaytmReport(file.buffer, file.originalname, today());
        sources.push({ name: file.originalname, hash: crypto.createHash('sha256').update(file.buffer).digest('hex'), rows: parsed.sourceRows, success: parsed.transactions.length, missingId: parsed.missingId, skipped: parsed.skipped });
        warnings.push(...parsed.warnings.map(warning => `${file.originalname}: ${warning}`));
        for (const tx of parsed.transactions) {
          const previous = all.get(tx.transactionId);
          if (previous && differs(previous, tx)) throw new Error(`Conflicting duplicate Paytm transaction ${tx.transactionId}.`);
          if (!previous) all.set(tx.transactionId, tx);
        }
      }
      const existing = store.paytmReportTransactions || {}, fresh = [];
      let duplicates = 0;
      for (const tx of all.values()) {
        if (existing[tx.transactionId]) {
          if (differs(existing[tx.transactionId], tx)) throw new Error(`Previously imported transaction ${tx.transactionId} has changed. Review the source report.`);
          duplicates++;
        } else fresh.push(tx);
      }
      if (fresh.length > 5000) throw new Error('Too many transactions in one preview. Upload a shorter period.');
      const draftId = `PTMR-${Date.now()}-${crypto.randomBytes(3).toString('hex')}`;
      store.paytmReportDrafts = store.paytmReportDrafts || {};
      store.paytmReportDrafts[draftId] = { id: draftId, at: new Date().toISOString(), by: req.user.username, sources, warnings, duplicates, transactions: fresh };
      saveStore(store);
      res.json({ success: true, draftId, sources, warnings, duplicates, newTransactions: fresh.length, payouts: summarizePayouts(fresh) });
    } catch (error) { res.status(400).json({ success: false, error: error.message || 'Could not read Paytm reports.' }); }
  });
  router.post('/api/expenses/paytm-reports/confirm', (req, res) => {
    if (deny(req, res)) return;
    const store = loadStore(), draftId = String(req.body && req.body.draftId || ''), draft = (store.paytmReportDrafts || {})[draftId];
    if (!draft) return res.status(404).json({ success: false, error: 'Preview not found. Upload the report again.' });
    store.paytmReportTransactions = store.paytmReportTransactions || {};
    let added = 0;
    for (const tx of draft.transactions) {
      const old = store.paytmReportTransactions[tx.transactionId];
      if (old && differs(old, tx)) return res.status(409).json({ success: false, error: `Transaction ${tx.transactionId} changed after preview. Upload again.` });
      if (!old) { store.paytmReportTransactions[tx.transactionId] = { ...tx, importedAt: new Date().toISOString(), importedBy: req.user.username }; added++; }
    }
    store.paytmReportImports = store.paytmReportImports || [];
    store.paytmReportImports.push({ id: draftId, at: new Date().toISOString(), by: req.user.username, sources: draft.sources, count: added, duplicates: draft.duplicates, warnings: draft.warnings });
    delete store.paytmReportDrafts[draftId];
    audit(store, req, 'PAYTM_REPORT_IMPORTED', 'paytm_report', draftId, { nature: 'SANKI', account: 'Paytm Settlement Clearing', after: { count: added, sources: draft.sources.map(source => source.name) }, note: 'Evidence imported only; no ledger or bank posting changed.' });
    saveStore(store);
    res.json({ success: true, added, duplicates: draft.duplicates, view: view(store) });
  });
}
function differs(a, b) {
  return ['date', 'amount', 'commission', 'gst', 'settledAmount', 'payoutId', 'utr'].some(key => String(a[key]) !== String(b[key]));
}
module.exports = { registerPaytmReports };

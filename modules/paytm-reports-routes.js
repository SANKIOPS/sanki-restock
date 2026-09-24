const crypto = require('crypto');
const path = require('path');
const { parsePaytmReport, summarizePayouts } = require('./paytm-report');
const { CLEARING, BANK, summarizeShopifyPayments, autoMatchShopifyNotes, validateOrderLink, validateSettlementReview, validatePayoutPosting } = require('./paytm-accounting');

function registerPaytmReports(router, deps) {
  const { loadStore, saveStore, audit, canAccess, canClassify, upload, view, today, orders, saleRows, shopifyClient, shopifyStore } = deps;
  const deny = (req, res) => !canAccess(req) ? (res.status(403).json({ success: false, error: 'You cannot access Paytm reconciliation.' }), true) : false;
  router.get('/api/expenses/paytm-reports', (req, res) => {
    if (deny(req, res)) return;
    res.json({ success: true, ...view(loadStore()) });
  });
  router.post('/api/expenses/paytm-reports/sync-shopify-payments', async (req, res) => {
    if (deny(req, res)) return;
    const orderId = String(req.body && req.body.orderId || '').trim();
    const order = (orders ? orders() : []).find(x => String(x.id) === orderId || String(x.orderNumber || x.number || '').replace(/\D/g, '') === orderId);
    if (!order || !/^\d+$/.test(String(order.id))) return res.status(404).json({ success: false, error: 'Choose a synced Shopify order with a numeric order ID.' });
    if (!shopifyClient || !shopifyStore) return res.status(503).json({ success: false, error: 'Shopify connection is not configured.' });
    const store = loadStore();
    if ((store.paytmPayoutPostings || []).some(x => (x.orderIds || []).includes(String(order.id)))) return res.status(409).json({ success: false, error: 'This order is in a posted payout. Correct the posting before changing its payment split.' });
    try {
      const response = await shopifyClient.request(`https://${shopifyStore}/admin/api/2024-01/orders/${order.id}/transactions.json`);
      if (!response.ok) throw new Error(`Shopify payment lookup failed (${response.status}).`);
      const body = await response.json(), summary = summarizeShopifyPayments(body.transactions);
      if (!summary.transactions.length) throw new Error('Shopify returned no successful sale/capture payment components. Keep this order under manual review.');
      store.paytmShopifyPayments = store.paytmShopifyPayments || {};
      const before = store.paytmShopifyPayments[order.id] || null;
      store.paytmShopifyPayments[order.id] = { ...summary, by: req.user.username, at: new Date().toISOString() };
      audit(store, req, 'PAYTM_SHOPIFY_PAYMENTS_SYNCED', 'shopify_order', String(order.id), { nature: 'SANKI', account: CLEARING, before, after: store.paytmShopifyPayments[order.id] });
      saveStore(store);
      res.json({ success: true, summary: store.paytmShopifyPayments[order.id], view: view(store) });
    } catch (error) { res.status(502).json({ success: false, error: error.message || 'Could not verify Shopify payment components.' }); }
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
          if (previous && differs(previous, tx)) { warnings.push(`Transaction ${tx.transactionId} differs between selected files; kept the first row for review.`); continue; }
          if (!previous) all.set(tx.transactionId, tx);
        }
      }
      const existing = store.paytmReportTransactions || {}, fresh = [], enrichments = [];
      let duplicates = 0;
      for (const tx of all.values()) {
        if (existing[tx.transactionId]) {
          if (differs(existing[tx.transactionId], tx)) { warnings.push(`Previously imported transaction ${tx.transactionId} has a genuine financial or identifier conflict. Saved evidence was not changed; review this ID separately.`); duplicates++; continue; }
          const enriched = mergeEvidence(existing[tx.transactionId], tx);
          if (JSON.stringify(enriched) !== JSON.stringify(existing[tx.transactionId])) enrichments.push(tx);
          duplicates++;
        } else fresh.push(tx);
      }
      if (fresh.length > 5000) throw new Error('Too many transactions in one preview. Upload a shorter period.');
      const draftId = `PTMR-${Date.now()}-${crypto.randomBytes(3).toString('hex')}`;
      store.paytmReportDrafts = store.paytmReportDrafts || {};
      store.paytmReportDrafts[draftId] = { id: draftId, at: new Date().toISOString(), by: req.user.username, sources, warnings, duplicates, transactions: fresh, enrichments };
      saveStore(store);
      res.json({ success: true, draftId, sources, warnings, duplicates, enrichedTransactions: enrichments.length, newTransactions: fresh.length, payouts: summarizePayouts([...all.values()]) });
    } catch (error) { res.status(400).json({ success: false, error: error.message || 'Could not read Paytm reports.' }); }
  });
  router.post('/api/expenses/paytm-reports/confirm', (req, res) => {
    if (deny(req, res)) return;
    const store = loadStore(), draftId = String(req.body && req.body.draftId || ''), draft = (store.paytmReportDrafts || {})[draftId];
    if (!draft) return res.status(404).json({ success: false, error: 'Preview not found. Upload the report again.' });
    store.paytmReportTransactions = store.paytmReportTransactions || {};
    let added = 0, enriched = 0;
    for (const tx of draft.transactions) {
      const old = store.paytmReportTransactions[tx.transactionId];
      if (old && differs(old, tx)) return res.status(409).json({ success: false, error: `Transaction ${tx.transactionId} changed after preview. Upload again.` });
      if (!old) { store.paytmReportTransactions[tx.transactionId] = { ...tx, importedAt: new Date().toISOString(), importedBy: req.user.username }; added++; }
    }
    for (const tx of draft.enrichments || []) {
      const old = store.paytmReportTransactions[tx.transactionId];
      if (!old || differs(old, tx)) return res.status(409).json({ success: false, error: `Transaction ${tx.transactionId} changed after preview. Upload again.` });
      const next = mergeEvidence(old, tx);
      if (JSON.stringify(next) !== JSON.stringify(old)) { store.paytmReportTransactions[tx.transactionId] = { ...next, enrichedAt: new Date().toISOString(), enrichedBy: req.user.username }; enriched++; }
    }
    store.paytmReportImports = store.paytmReportImports || [];
    store.paytmReportImports.push({ id: draftId, at: new Date().toISOString(), by: req.user.username, sources: draft.sources, count: added, enriched, duplicates: draft.duplicates, warnings: draft.warnings });
    const autoMatched = autoMatchShopifyNotes(store,store.paytmReportTransactions,orders ? orders() : [],saleRows ? saleRows(store) : []);
    autoMatched.forEach(link=>audit(store,req,'PAYTM_ORDER_AUTO_MATCHED','paytm_transaction',link.transactionId,{nature:'SANKI',account:CLEARING,after:link}));
    delete store.paytmReportDrafts[draftId];
    audit(store, req, 'PAYTM_REPORT_IMPORTED', 'paytm_report', draftId, { nature: 'SANKI', account: 'Paytm Settlement Clearing', after: { count: added, sources: draft.sources.map(source => source.name) }, note: 'Evidence imported only; no ledger or bank posting changed.' });
    saveStore(store);
    res.json({ success: true, added, enriched, duplicates: draft.duplicates, autoMatched: autoMatched.length, view: view(store) });
  });
  router.post('/api/expenses/paytm-reports/auto-match', (req, res) => {
    if (deny(req, res)) return;
    const store=loadStore(),matched=autoMatchShopifyNotes(store,store.paytmReportTransactions||{},orders ? orders() : [],saleRows ? saleRows(store) : []);
    matched.forEach(link=>audit(store,req,'PAYTM_ORDER_AUTO_MATCHED','paytm_transaction',link.transactionId,{nature:'SANKI',account:CLEARING,after:link}));
    if(matched.length)saveStore(store);
    res.json({success:true,autoMatched:matched.length,view:view(store)});
  });
  router.post('/api/expenses/paytm-reports/link-order', (req, res) => {
    if (deny(req, res)) return;
    const store = loadStore(), body = req.body || {}, transactionId = String(body.transactionId || ''), orderId = String(body.orderId || '');
    try {
      const tx = (store.paytmReportTransactions || {})[transactionId];
      if ((store.paytmPayoutPostings || []).some(x => x.transactionIds.includes(transactionId))) throw new Error('A posted payout link cannot be changed.');
      if ((store.paytmExcludedTransactions || {})[transactionId]) throw new Error('Restore this excluded transaction before linking it to a Shopify order.');
      if ((store.paytmManualResolutions || {})[transactionId]) throw new Error('Correct the manual sale/cash classification before linking this payment to Shopify.');
      const link = validateOrderLink(store, tx, orderId, orders(), saleRows(store), body.reason);
      store.paytmOrderLinks = store.paytmOrderLinks || {};
      const before = store.paytmOrderLinks[transactionId] || null;
      store.paytmOrderLinks[transactionId] = { ...link, by: req.user.username, at: new Date().toISOString() };
      audit(store, req, 'PAYTM_ORDER_LINKED', 'paytm_transaction', transactionId, { nature: 'SANKI', account: CLEARING, before, after: store.paytmOrderLinks[transactionId] });
      saveStore(store);
      res.json({ success: true, view: view(store) });
    } catch (error) { res.status(400).json({ success: false, error: error.message }); }
  });
  router.post('/api/expenses/paytm-reports/classify', (req, res) => {
    if (deny(req, res)) return;
    if (!canClassify || !canClassify(req)) return res.status(403).json({ success: false, error: 'Only the Owner may approve a manual Paytm sale or cash transfer.' });
    const store = loadStore(), body = req.body || {}, id = String(body.transactionId || '').trim();
    const tx = (store.paytmReportTransactions || {})[id], type = String(body.type || ''), party = String(body.party || '').trim(), details = String(body.details || '').trim(), reason = String(body.reason || '').trim(), proof = String(body.proof || '').trim();
    if (!tx) return res.status(404).json({ success: false, error: 'Paytm transaction not found.' });
    if (!['manual_sale', 'cash_transfer'].includes(type)) return res.status(400).json({ success: false, error: 'Choose manual sale or cash-to-bank transfer.' });
    if (!party || details.length < 5 || reason.length < 10 || !/^\/api\/expenses\/photo\/[a-zA-Z0-9._-]+$/.test(proof)) return res.status(400).json({ success: false, error: 'Enter the customer/third party, sale or transfer details, a reason of at least 10 characters, and upload proof.' });
    if ((store.paytmOrderLinks || {})[id] || (store.paytmManualResolutions || {})[id] || (store.paytmExcludedTransactions || {})[id] || (store.paytmPayoutPostings || []).some(x => (x.transactionIds || []).includes(id))) return res.status(409).json({ success: false, error: 'This Paytm payment is already linked, classified, excluded, or posted.' });
    const at = new Date().toISOString(), note = `Paytm ${id} · ${details} · ${reason}`;
    let record;
    if (type === 'manual_sale') {
      store.receiptSeq = Number(store.receiptSeq || 0) + 1;
      record = { id: 'REC-' + String(store.receiptSeq).padStart(5, '0'), nature: 'SANKI', account: CLEARING, amount: tx.amount, receiptType: 'product_sale', source: party, date: tx.date, note, proof, proofs: [proof], paytmTransactionId: id, createdBy: req.user.username, createdAt: at };
      store.receipts = store.receipts || []; store.receipts.push(record);
    } else {
      store.transferSeq = Number(store.transferSeq || 0) + 1;
      record = { id: 'TR-' + String(store.transferSeq).padStart(5, '0'), nature: 'SANKI', fromNature: 'SANKI', toNature: 'SANKI', classification: 'internal_transfer', fromAccount: 'Counter Cash', toAccount: CLEARING, amount: tx.amount, date: tx.date, proof, proofs: [proof], note, intermediary: party, routedThroughIntermediary: true, paytmTransactionId: id, createdBy: req.user.username, createdAt: at };
      store.transfers = store.transfers || []; store.transfers.push(record);
    }
    store.paytmManualResolutions = store.paytmManualResolutions || {};
    store.paytmManualResolutions[id] = { transactionId: id, type, amount: tx.amount, party, details, reason, proof, recordId: record.id, by: req.user.username, at };
    audit(store, req, 'PAYTM_TRANSACTION_CLASSIFIED', 'paytm_transaction', id, { nature: 'SANKI', account: CLEARING, after: store.paytmManualResolutions[id] });
    saveStore(store);
    res.json({ success: true, view: view(store) });
  });
  router.post('/api/expenses/paytm-reports/unclassify', (req, res) => {
    if (deny(req, res)) return;
    if (!canClassify || !canClassify(req)) return res.status(403).json({ success: false, error: 'Only the Owner may correct a manual Paytm classification.' });
    const store = loadStore(), id = String(req.body && req.body.transactionId || '').trim(), reason = String(req.body && req.body.reason || '').trim();
    const classification = (store.paytmManualResolutions || {})[id];
    if (!classification) return res.status(404).json({ success: false, error: 'Manual classification not found.' });
    if (reason.length < 10) return res.status(400).json({ success: false, error: 'Enter a correction reason of at least 10 characters.' });
    if ((store.paytmPayoutPostings || []).some(x => (x.transactionIds || []).includes(id))) return res.status(409).json({ success: false, error: 'This payment is in a posted payout. Reopen the payout before correcting it.' });
    const key = classification.type === 'manual_sale' ? 'receipts' : 'transfers';
    if (!(store[key] || []).some(x => x.id === classification.recordId && x.paytmTransactionId === id)) return res.status(409).json({ success: false, error: 'The linked accounting record changed. Review it before correcting this classification.' });
    store[key] = store[key].filter(x => x.id !== classification.recordId);
    delete store.paytmManualResolutions[id];
    audit(store, req, 'PAYTM_TRANSACTION_CLASSIFICATION_REVERSED', 'paytm_transaction', id, { nature: 'SANKI', account: CLEARING, before: classification, note: reason });
    saveStore(store);
    res.json({ success: true, view: view(store) });
  });
  router.post('/api/expenses/paytm-reports/exclude', (req, res) => {
    if (deny(req, res)) return;
    const store = loadStore(), body = req.body || {}, transactionId = String(body.transactionId || '').trim(), reason = String(body.reason || '').trim(), tx = (store.paytmReportTransactions || {})[transactionId];
    if (!tx) return res.status(404).json({ success: false, error: 'Paytm transaction not found.' });
    if ((store.paytmPayoutPostings || []).some(x => (x.transactionIds || []).includes(transactionId))) return res.status(400).json({ success: false, error: 'A posted payout transaction cannot be excluded.' });
    if ((store.paytmManualResolutions || {})[transactionId]) return res.status(409).json({ success: false, error: 'Correct the manual sale/cash classification before excluding this payment.' });
    if (reason.length < 10) return res.status(400).json({ success: false, error: 'Enter a reason of at least 10 characters for excluding this payment.' });
    store.paytmExcludedTransactions = store.paytmExcludedTransactions || {};
    store.paytmOrderLinks = store.paytmOrderLinks || {};
    const before = { exclusion: store.paytmExcludedTransactions[transactionId] || null, link: store.paytmOrderLinks[transactionId] || null };
    store.paytmExcludedTransactions[transactionId] = { reason, by: req.user.username, at: new Date().toISOString() };
    delete store.paytmOrderLinks[transactionId];
    audit(store, req, 'PAYTM_TRANSACTION_EXCLUDED', 'paytm_transaction', transactionId, { nature: 'SANKI', account: CLEARING, before, after: store.paytmExcludedTransactions[transactionId] });
    saveStore(store);
    res.json({ success: true, view: view(store) });
  });
  router.post('/api/expenses/paytm-reports/restore', (req, res) => {
    if (deny(req, res)) return;
    const store = loadStore(), body = req.body || {}, transactionId = String(body.transactionId || '').trim(), reason = String(body.reason || '').trim();
    const before = (store.paytmExcludedTransactions || {})[transactionId];
    if (!before) return res.status(404).json({ success: false, error: 'This transaction is not excluded.' });
    if (reason.length < 10) return res.status(400).json({ success: false, error: 'Enter a reason of at least 10 characters for restoring this payment.' });
    delete store.paytmExcludedTransactions[transactionId];
    audit(store, req, 'PAYTM_TRANSACTION_RESTORED', 'paytm_transaction', transactionId, { nature: 'SANKI', account: CLEARING, before, after: { restoredBy: req.user.username, reason } });
    saveStore(store);
    res.json({ success: true, view: view(store) });
  });
  router.post('/api/expenses/paytm-reports/finalize-settlement', (req, res) => {
    if (deny(req, res)) return;
    const store = loadStore(), payoutId = String(req.body && req.body.payoutId || '');
    try {
      const { payout, linked } = validateSettlementReview(store, payoutId, saleRows(store));
      store.paytmVerifiedSettlements = store.paytmVerifiedSettlements || [];
      if (store.paytmVerifiedSettlements.some(x => x.settlementId === payout.settlementId)) throw new Error('This Paytm settlement is already finalized.');
      const verified = { id: `PTMV-${Date.now()}`, settlementId: payout.settlementId, payoutId: payout.payoutId, utr: payout.utr, settledDate: payout.settledDate, transactionIds: payout.transactionIds, orderIds: linked.map(x => x.orderId).filter(Boolean), gross: payout.gross, customerGross: payout.customerGross, commission: payout.commission, platformFee: payout.platformFee, gst: payout.gst, net: payout.net, finalizedBy: req.user.username, finalizedAt: new Date().toISOString() };
      store.paytmVerifiedSettlements.push(verified);
      audit(store, req, 'PAYTM_SETTLEMENT_FINALIZED', 'paytm_settlement', payout.settlementId, { nature: 'SANKI', account: CLEARING, after: verified });
      saveStore(store);
      res.json({ success: true, verified, view: view(store) });
    } catch (error) { res.status(400).json({ success: false, error: error.message }); }
  });
  router.post('/api/expenses/paytm-reports/finalize-all', (req, res) => {
    if (deny(req, res)) return;
    const store = loadStore(), payouts = summarizePayouts(Object.values(store.paytmReportTransactions || {}));
    store.paytmVerifiedSettlements = store.paytmVerifiedSettlements || [];
    const finalized = [], skipped = [];
    for (const candidate of payouts) {
      if (store.paytmVerifiedSettlements.some(x => x.settlementId === candidate.settlementId) || (store.paytmPayoutPostings || []).some(x => x.settlementId === candidate.settlementId || x.payoutId === candidate.payoutId)) continue;
      try {
        const { payout, linked } = validateSettlementReview(store, candidate.payoutId, saleRows(store));
        const verified = { id: `PTMV-${Date.now()}-${finalized.length + 1}`, settlementId: payout.settlementId, payoutId: payout.payoutId, utr: payout.utr, settledDate: payout.settledDate, transactionIds: payout.transactionIds, orderIds: linked.map(x => x.orderId).filter(Boolean), gross: payout.gross, customerGross: payout.customerGross, commission: payout.commission, platformFee: payout.platformFee, gst: payout.gst, net: payout.net, finalizedBy: req.user.username, finalizedAt: new Date().toISOString() };
        store.paytmVerifiedSettlements.push(verified); finalized.push(verified);
        audit(store, req, 'PAYTM_SETTLEMENT_FINALIZED', 'paytm_settlement', payout.settlementId, { nature: 'SANKI', account: CLEARING, after: verified, note: 'Bulk finalization of all fully matched Paytm settlements' });
      } catch (error) { skipped.push({ settlementId: candidate.settlementId, reason: error.message }); }
    }
    if (finalized.length) saveStore(store);
    res.json({ success: true, finalized: finalized.length, skipped: skipped.length, skippedSettlements: skipped, view: view(store) });
  });
  router.post('/api/expenses/paytm-reports/post-payout', (req, res) => {
    if (deny(req, res)) return;
    const store = loadStore(), body = req.body || {}, payoutId = String(body.payoutId || ''), bankTransactionId = String(body.bankTransactionId || '');
    try {
      const { payout, bank, linked } = validatePayoutPosting(store, payoutId, bankTransactionId, saleRows(store));
      const postedAt = new Date().toISOString();
      const posting = { id: `PTMR-POST-${Date.now()}`, payoutId: payout.payoutId, settlementId: payout.settlementId, sourcePayoutIds: payout.sourcePayoutIds, transactionIds: payout.transactionIds, orderIds: linked.map(x => x.orderId).filter(Boolean), orderNumbers: linked.map(x => x.orderNumber).filter(Boolean), manualRecordIds: linked.map(x => x.recordId).filter(Boolean), bankAccount: BANK, bankTransactionId, utr: payout.utr, date: bank.date, payoutDate: payout.payoutDate, settledDate: payout.settledDate, gross: payout.gross, customerGross: payout.customerGross, nonCustomerAmount: payout.nonCustomerAmount, commission: payout.commission, platformFee: payout.platformFee, gst: payout.gst, net: payout.net, postedBy: req.user.username, postedAt };
      store.paytmPayoutPostings = store.paytmPayoutPostings || [];
      store.paytmPayoutPostings.push(posting);
      store.reconciliationExpenses = store.reconciliationExpenses || [];
      if (payout.commission + payout.platformFee + payout.gst > 0) store.reconciliationExpenses.push({ id: `BRE-${posting.id}`, nature: 'SANKI', date: bank.date, amount: Math.round((payout.commission + payout.platformFee + payout.gst) * 100) / 100, account: CLEARING, category: 'PAYTM CHARGES', type: 'running', vendor: 'Paytm', particulars: `Paytm charges for UTR ${payout.utr}; commission ${payout.commission}, platform fee ${payout.platformFee}, GST ${payout.gst}`, paytmPostingId: posting.id, bankTransactionId, createdBy: req.user.username, createdAt: postedAt });
      if (payout.nonCustomerAmount > 0) store.reconciliationExpenses.push({ id: `BRE-${posting.id}-VAS`, nature: 'SANKI', date: bank.date, amount: payout.nonCustomerAmount, account: CLEARING, category: 'PAYTM CHARGES', type: 'running', vendor: 'Paytm', particulars: `Paytm VAS deduction for UTR ${payout.utr}; deducted from settlement and not treated as Shopify revenue`, paytmPostingId: posting.id, bankTransactionId, createdBy: req.user.username, createdAt: postedAt });
      store.bankDateOverrides = store.bankDateOverrides || {};
      store.bankDateOverrides[posting.id] = { bankDate: bank.date, originalDate: payout.settledDate, bankTransactionId, bankReference: payout.utr, remark: 'Paytm settlement posted from verified detailed transaction report', by: req.user.username, at: postedAt, reconciliationDraft: posting.id };
      audit(store, req, 'PAYTM_PAYOUT_POSTED', 'paytm_payout', payoutId, { nature: 'SANKI', account: BANK, after: posting });
      saveStore(store);
      res.json({ success: true, posting, view: view(store) });
    } catch (error) { res.status(400).json({ success: false, error: error.message }); }
  });
}
function differs(a, b) {
  const financial = ['date', 'amount', 'commission', 'gst', 'settledAmount'];
  if (financial.some(key => String(a[key] ?? '') !== String(b[key] ?? ''))) return true;
  return ['platformFee', 'payoutId', 'utr', 'settledDate', 'transactionType'].some(key => valuePresent(a[key]) && valuePresent(b[key]) && String(a[key]) !== String(b[key]));
}
function valuePresent(value) { return value !== undefined && value !== null && String(value) !== ''; }
function mergeEvidence(old, current) {
  const next = { ...old };
  for (const key of ['platformFee', 'payoutId', 'payoutDate', 'utr', 'settledDate', 'transactionType', 'comments', 'paymentMode', 'posId', 'merchantOrderId', 'rrn', 'isCustomerPayment']) if (!valuePresent(next[key]) && valuePresent(current[key])) next[key] = current[key];
  return next;
}
module.exports = { registerPaytmReports };

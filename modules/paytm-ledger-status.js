'use strict';

// Read-only status projection. Never repairs records, approves a review, or posts
// money. A bank-only placeholder is evidence of the net credit, not the report.
const accounting = require('./paytm-accounting');
const { summarizePayouts, START_DATE } = require('./paytm-report');
const { approvedBaseAmount } = require('./paytm-rounding');
const cents = value => Math.round(Number(value || 0) * 100);
const day = value => String(value || '').slice(0, 10);
const unique = values => [...new Set(values.filter(Boolean))];
const latest = dates => dates.filter(Boolean).sort().at(-1) || '';
const labels = {
  posted: 'Reconciled — Paytm report verified and bank posted',
  finalized: 'Paytm report finalized — bank link pending',
  ready: 'Paytm report reviewed — finalization pending',
  incomplete: 'Paytm review required',
  report_pending: 'Pending — Paytm report verification required',
  bank_confirmed_report_pending: 'Bank settlement confirmed — Paytm report details pending',
  summary: 'Daily summary — see individual payment and settlement statuses'
};
function rowState(code, states = [], reasons = []) {
  return {
    code, label: labels[code], reasons: unique(reasons.concat(states.flatMap(s => s.reasons || []))),
    bankReasons: unique(states.flatMap(s => s.bankReasons || [])),
    transactionIds: unique(states.flatMap(s => s.transactionIds || [])),
    settlementIds: unique(states.map(s => s.settlementId)),
    settledDate: latest(states.map(s => s.settledDate)),
    finalizedAt: latest(states.map(s => s.finalizedAt)),
    finalizedBy: unique(states.map(s => s.finalizedBy)).join(', ')
  };
}
function savedSnapshot(record, payout) {
  if (!record || !record.transactionIds || record.transactionIds.length !== payout.transactionIds.length ||
      !payout.transactionIds.every(id => record.transactionIds.includes(id))) throw Error('Saved Paytm finalization no longer identifies these exact report transactions.');
  // Reviewed accidental returns retain the raw imported gross as evidence but
  // never turn the returned receipt into business sales or the refund into a
  // service fee. Match the same effective gross saved by the posting validator.
  const expected = { ...payout, gross: payout.effectiveCustomerGross ?? payout.gross };
  if (record.utr !== payout.utr || day(record.settledDate || record.date) !== payout.settledDate ||
      ['net', 'gross', 'commission', 'platformFee', 'gst', 'nonCustomerAmount'].some(key => cents(record[key]) !== cents(expected[key]))) {
    throw Error('Saved Paytm finalization amounts, deductions, UTR or settled date changed. Review the source evidence.');
  }
  if ((cents(payout.returnedReceiptAmount) || cents(payout.refundAmount)) &&
      (cents(record.rawCustomerGross) !== cents(payout.customerGross) ||
       cents(record.customerGross) !== cents(payout.effectiveCustomerGross) ||
       cents(record.returnedReceiptAmount) !== cents(payout.returnedReceiptAmount) ||
       cents(record.refundAmount) !== cents(payout.refundAmount))) {
    throw Error('Saved Paytm finalization no longer matches its exact accidental receipt/refund review. Review the source evidence.');
  }
}
function postedLegacyCompanion(store, posting, payout, bank) {
  const reference = value => String(value || '').trim().toUpperCase();
  const expectedReference = reference(payout.utr), expectedFees = cents(payout.commission) + cents(payout.platformFee) + cents(payout.gst) + cents(payout.nonCustomerAmount);
  const candidates = (store.paytmSettlements || []).filter(legacy => !legacy.accountingExcluded && !legacy.provisional &&
    legacy.id && legacy.bankAccount === accounting.BANK && day(legacy.date) === day(bank.date) &&
    reference(legacy.utr || legacy.bankReference || legacy.reference) === expectedReference &&
    cents(legacy.netAmount) === cents(payout.net) && (!legacy.bankTransactionId || legacy.bankTransactionId === bank.id));
  if (!expectedReference || candidates.length !== 1) return null;
  const legacy = candidates[0];
  const pendingDetail = legacy.bankTruthOnly === true &&
    ['axis_3448_paytm_bank_narration', 'owner_axis_3448_reconciliation_screenshot'].includes(legacy.source) &&
    cents(legacy.grossAmount) === cents(legacy.netAmount) && cents(legacy.chargeAmount) === 0 &&
    cents(legacy.paytmChargeAmount) === 0 && cents(legacy.hiddenChargeAmount) === 0 &&
    !(legacy.orderIds || []).length && !(legacy.transferIds || []).length && !(legacy.otherReceipts || []).length;
  const completeDetail = cents(legacy.grossAmount) === cents(payout.customerGross) && cents(legacy.chargeAmount) === expectedFees;
  if (!pendingDetail && !completeDetail) return null;
  const book = (store.bankStatements || {})[accounting.BANK] || {}, allowed = new Set([legacy.id, posting.id]);
  const matches = (book.imports || []).flatMap(record => [].concat(record.reconciliationRows || [], record.carriedReconciliationRows || [])
    .filter(row => row.bank && day(row.bank.date) === day(bank.date) && cents(row.bank.credit) === cents(payout.net) &&
      cents(row.bank.debit) === 0 && `${row.bank.reference || ''} ${row.bank.description || ''}`.toUpperCase().includes(expectedReference))
    .map(row => ({ record, row })));
  // Old and detailed IDs may name the same one bank credit. Recognize that
  // alias only from the original finalized row, never by amount/date alone.
  if (!matches.length || !matches.some(({ row }) => (row.linkedRecordIds || []).includes(legacy.id)) ||
      matches.some(({ record, row }) => !record.finalizedAt || !record.finalizedBy ||
        !(row.linkedRecordIds || []).length || (row.linkedRecordIds || []).some(id => !allowed.has(id)))) return null;
  return legacy;
}
function confirmedLegacyBank(store, settlement) {
  if (!settlement || settlement.provisional || !settlement.id || !settlement.bankReference) return null;
  const book = (store.bankStatements || {})[settlement.bankAccount || accounting.BANK] || {};
  const links = (book.imports || []).flatMap(record => !record.finalizedAt || !record.finalizedBy ? [] :
    [].concat(record.reconciliationRows || [], record.carriedReconciliationRows || [])
      .filter(row => (row.linkedRecordIds || []).length === 1 && row.linkedRecordIds[0] === settlement.id && row.bank &&
        day(row.bank.date) === day(settlement.date) && cents(row.bank.credit) === cents(settlement.netAmount) &&
        cents(row.bank.debit) === 0 && String(row.bank.reference || row.bank.description || '').includes(settlement.bankReference))
      .map(row => ({ bankTransactionId: row.bank.id, bankDate: day(row.bank.date), bankReference: settlement.bankReference,
        reconciliationId: record.id, finalizedAt: record.finalizedAt, finalizedBy: record.finalizedBy })));
  // Repeated carried copies of the same finalized evidence are not new claims.
  const identities = unique(links.map(x => [x.bankDate, x.bankReference, cents(settlement.netAmount)].join('|')));
  if (identities.length !== 1 || !links.length) return null;
  const bankIds = unique(links.map(x => x.bankTransactionId));
  const conflictingImport = (book.imports || []).some(record => [].concat(record.reconciliationRows || [], record.carriedReconciliationRows || [])
    .some(row => bankIds.includes(row.bank?.id) && (row.linkedRecordIds || []).some(id => id !== settlement.id)));
  const conflictingRecord = [...(store.adjustments || []), ...(store.receipts || []), ...(store.bankTruthMovements || []), ...(store.paytmPayoutPostings || [])]
    .some(record => !record.accountingExcluded && bankIds.includes(record.bankTransactionId));
  const conflictingOverride = Object.entries(store.bankDateOverrides || {}).some(([id, override]) => id !== settlement.id && bankIds.includes(override.bankTransactionId));
  const conflictingSettlement = (store.paytmSettlements || []).some(record => record.id !== settlement.id && !record.accountingExcluded &&
    (bankIds.includes(record.bankTransactionId) || record.bankReference === settlement.bankReference && day(record.date) === day(settlement.date)));
  return conflictingImport || conflictingRecord || conflictingOverride || conflictingSettlement ? null : links[0];
}
function buildProjection(store, saleRows, options = {}) {
  const asOf = day(options.to), inScope = date => !asOf || day(date) <= asOf;
  const original = accounting.reconciliationStatus(store, saleRows);
  const payouts = summarizePayouts(Object.values(store.paytmReportTransactions || {}));
  const payoutById = new Map(payouts.map(p => [p.settlementId, p])), legacyStateAliases = new Map();
  const states = original.settlements.map(saved => {
    const payout = payoutById.get(saved.settlementId), state = { ...saved,
      reasons: [...saved.reasons], bankReasons: [...saved.bankReasons], transactionIds: [...payout.transactionIds] };
    if (state.code === 'posted' || state.code === 'finalized') {
      try {
        const reviewedPayout = accounting.validateSettlementReview(store, payout.payoutId, saleRows, { allowExcluded: state.code === 'posted' }).payout;
        payout.transactionIds.forEach(id => {
          const tx = (store.paytmReportTransactions || {})[id], link = (store.paytmOrderLinks || {})[id] || (store.paytmManualResolutions || {})[id];
          if (tx.isCustomerPayment !== false && !(store.paytmExcludedTransactions || {})[id] && (!link || cents(link.amount) !== cents(tx.amount))) throw Error('A confirmed Paytm allocation no longer equals its original transaction amount.');
        });
        if (!state.historicalEvidenceLink) {
          const record = (state.code === 'posted' ? store.paytmPayoutPostings : store.paytmVerifiedSettlements || [])
            ?.find(x => x.settlementId === payout.settlementId || x.payoutId === payout.payoutId);
          savedSnapshot(record, reviewedPayout);
          if (!(record.postedAt || record.finalizedAt) || !(record.postedBy || record.finalizedBy)) throw Error('Paytm finalization audit evidence is missing.');
          if (state.code === 'posted') {
            const bank = accounting.matchPayoutBank(store, payout);
            if (!bank.bankMatched || bank.bankCandidates[0].id !== record.bankTransactionId ||
                day(record.date) !== bank.bankCandidates[0].date) throw Error('The posted payout no longer has its unique exact Axis bank credit.');
            const id = record.bankTransactionId, book = (store.bankStatements || {})[accounting.BANK] || {},
              companion = postedLegacyCompanion(store, record, payout, bank.bankCandidates[0]),
              allowedIds = new Set([record.id, ...(companion ? [companion.id] : [])]);
            if ((store.paytmPayoutPostings || []).some(x => x !== record && (x.bankTransactionId === id || (x.transactionIds || []).some(t => payout.transactionIds.includes(t)))) ||
                (store.paytmSettlements || []).some(x => x !== companion && !x.accountingExcluded && (x.bankTransactionId === id ||
                  String(x.utr || x.bankReference || x.reference || '').trim().toUpperCase() === String(payout.utr || '').trim().toUpperCase() && day(x.date) === day(record.date))) ||
                [...(store.adjustments || []), ...(store.receipts || []), ...(store.bankTruthMovements || [])].some(x => !x.accountingExcluded && x.bankTransactionId === id) ||
                Object.entries(store.bankDateOverrides || {}).some(([key, x]) => !allowedIds.has(key) && x.bankTransactionId === id) ||
                (book.imports || []).some(x => [].concat(x.reconciliationRows || [], x.carriedReconciliationRows || []).some(row => row.bank?.id === id && (row.linkedRecordIds || []).some(key => !allowedIds.has(key))))) {
              throw Error('Another ledger record claims this Paytm bank credit. Review its exclusive reconciliation ownership.');
            }
            if (companion) legacyStateAliases.set(companion.id, state);
            state.bankDate = day(record.date);
          }
        } else state.bankDate = day(payout.settledDate);
      } catch (error) { state.code = 'incomplete'; state.label = 'Incomplete — review required'; state.reasons.push(error.message); }
    }
    // Date filters cannot pull a future settlement back into an earlier ledger.
    if (state.code === 'posted' && (!inScope(state.settledDate) || !inScope(state.bankDate))) {
      state.code = 'finalized'; state.label = 'Paytm report finalized — bank link pending';
      state.bankReasons.push('The settlement or its Axis bank credit falls after the selected as-of date.');
    }
    return state;
  });
  const stateById = new Map(states.map(s => [s.settlementId, s]));
  const transactionStates = new Map();
  states.forEach(s => s.transactionIds.forEach(id => transactionStates.set(id, s)));
  const recordStates = new Map();
  legacyStateAliases.forEach((state, id) => recordStates.set(id, state));
  for (const key of ['paytmVerifiedSettlements', 'paytmPayoutPostings']) {
    (store[key] || []).forEach(record => {
      const state = stateById.get(record.settlementId) || states.find(s => s.payoutId === record.payoutId);
      if (state) recordStates.set(record.id, state);
    });
  }
  states.forEach(s => { if (s.historicalEvidenceLink) recordStates.set(s.historicalEvidenceLink.historicalSettlementId, s); });
  const manualStates = new Map(Object.entries(store.paytmManualResolutions || {}).filter(([, link]) => link.recordId)
    .map(([id, link]) => [link.recordId, transactionStates.get(id)]));
  const legacy = (store.paytmSettlements || []).filter(x => inScope(x.date)).map(record => {
    const state = recordStates.get(record.id), bank = confirmedLegacyBank(store, record);
    return { record, state, bank };
  });
  const receipts = saleRows.filter(row => row.account === accounting.CLEARING && row.date >= START_DATE &&
    inScope(row.date) && !row.accountingExcluded && cents(row.amount) > 0);
  const receiptStates = new Map();
  const byOrder = new Map();
  receipts.forEach(row => { const id = String(row.orderId || ''); if (!byOrder.has(id)) byOrder.set(id, []); byOrder.get(id).push(row); });
  byOrder.forEach((rows, orderId) => {
    const links = Object.entries(store.paytmOrderLinks || {}).filter(([, link]) => String(link.orderId) === orderId)
      .map(([id, link]) => ({ id, link, tx: (store.paytmReportTransactions || {})[id], state: transactionStates.get(id) }))
      .filter(x => x.tx && inScope(x.tx.date) && !(store.paytmExcludedTransactions || {})[x.id]);
    let overallocated = false, allocatedCents = 0;
    const sourceCents = rows.reduce((sum, r) => sum + cents(r.amount), 0);
    try {
      allocatedCents = links.reduce((sum, x) => sum + cents(approvedBaseAmount(store, x.link)), 0);
      overallocated = allocatedCents > sourceCents;
    }
    catch { /* Existing rounding validation supplies the exact blocker. */ }
    // A report batch and a Shopify receipt are separate evidence scopes. A
    // changed/partial receipt allocation cannot rewrite a fully validated bank
    // posting into an incomplete report. Keep its precise discrepancy on the
    // receipt; the existing posting validators remain authoritative for batches.
    rows.forEach(row => {
      let matched = [], sameDay = rows.filter(r => day(r.date) === day(row.date));
      const dayLinks = links.filter(x => day(x.tx.date) === day(row.date));
      try {
        if (dayLinks.length && dayLinks.reduce((sum, x) => sum + cents(approvedBaseAmount(store, x.link)), 0) === sameDay.reduce((sum, r) => sum + cents(r.amount), 0)) matched = dayLinks;
      } catch { /* Invalid rounding evidence cannot make a receipt complete. */ }
      if (!matched.length) {
        // Already-confirmed order identity is necessary. Amount alone is never
        // an order match; ambiguous same-order payment components stay pending.
        matched = links.filter(x => {
          try {
            const candidates = rows.filter(r => Math.abs(Date.parse(r.date) - Date.parse(x.tx.date)) <= 86400000 && cents(r.amount) === cents(approvedBaseAmount(store, x.link)));
            return candidates.length === 1 && candidates[0].id === row.id;
          } catch { return false; }
        });
      }
      const statuses = matched.map(x => x.state).filter(Boolean);
      let result;
      if (overallocated) result = rowState('incomplete', links.map(x => x.state).filter(Boolean), [
        'Confirmed Paytm report allocations total ₹' + (allocatedCents / 100).toFixed(2) +
        ', but the current Shopify payment components total ₹' + (sourceCents / 100).toFixed(2) +
        '. The ₹' + ((allocatedCents - sourceCents) / 100).toFixed(2) +
        ' excess requires receipt-allocation review; the existing bank postings are unchanged.'
      ]);
      else if (!statuses.length || statuses.length !== matched.length) {
        const candidates = sameDay.length === 1 ? dayLinks : [];
        const candidateStates = candidates.map(x => x.state).filter(Boolean);
        let confirmedCents = 0;
        try { confirmedCents = candidates.reduce((sum, x) => sum + cents(approvedBaseAmount(store, x.link)), 0); }
        catch { /* Invalid approved rounding cannot establish any coverage. */ }
        if (candidates.length && confirmedCents > 0 && confirmedCents < cents(row.amount)) {
          result = { ...rowState(candidateStates.some(s => s.code === 'incomplete') ? 'incomplete' : 'report_pending', candidateStates, [
            'Confirmed Paytm report amount ₹' + (confirmedCents / 100).toFixed(2) +
            ' covers part of the Shopify payment component ₹' + (cents(row.amount) / 100).toFixed(2) +
            '. The remaining ₹' + ((cents(row.amount) - confirmedCents) / 100).toFixed(2) +
            ' is not verified; no rounding adjustment or additional receipt has been assumed.'
          ]), receiptAmount: cents(row.amount) / 100, confirmedReportAmount: confirmedCents / 100,
            unverifiedAmount: (cents(row.amount) - confirmedCents) / 100 };
        } else result = rowState('report_pending', [], ['No verified detailed Paytm report allocation covers this receipt.']);
      }
      else if (statuses.some(s => s.code === 'incomplete')) result = rowState('incomplete', statuses);
      else if (statuses.every(s => s.code === 'posted')) result = rowState('posted', statuses);
      else if (statuses.some(s => s.code === 'ready')) result = rowState('ready', statuses);
      else result = rowState('finalized', statuses);
      receiptStates.set('PAYTM-SALE/' + row.id, result);
    });
  });
  function forRow(row) {
    if (manualStates.get(row.id)) { const state = manualStates.get(row.id); return rowState(state.code, [state]); }
    if (row.kind === 'paytm_customer_receipt') return receiptStates.get(row.id) || rowState('report_pending', [], ['No verified detailed Paytm report allocation covers this receipt.']);
    if (row.kind === 'paytm_pending_sale') return rowState('report_pending', [], ['The Paytm payment amount and allocation still require detailed report verification.']);
    if (row.kind === 'paytm_customer_receipts') return rowState('summary', [], ['This summary does not independently reconcile the payments listed for the day.']);
    if (!['paytm_settlement', 'paytm_charge', 'paytm_unknown_charge'].includes(row.kind)) return null;
    const baseId = String(row.id).replace(/\/(?:CHARGES|OTHER-DEDUCTIONS|VAS-DEDUCTION|UNKNOWN-CHARGES)$/, '');
    const state = recordStates.get(baseId) || stateById.get(row.settlement?.settlementId);
    if (state) return rowState(state.code, [state]);
    const legacyRow = legacy.find(x => x.record.id === baseId);
    if (legacyRow?.bank) return { ...rowState('bank_confirmed_report_pending', [], ['The exact bank net credit is finalized; the detailed Paytm gross, fees and payment allocations are not yet verified.']), ...legacyRow.bank };
    return rowState('report_pending', [], ['Detailed Paytm report verification and the exact finalized bank evidence are still required.']);
  }
  const pendingLegacy = legacy.filter(x => !x.state || x.state.code !== 'posted');
  const imported = Object.values(store.paytmReportTransactions || {}).filter(t => inScope(t.date));
  const scoped = states.filter(s => inScope(s.settledDate));
  const pending = scoped.filter(s => s.code !== 'posted');
  const receiptGaps = receipts.filter(r => receiptStates.get('PAYTM-SALE/' + r.id)?.code !== 'posted');
  const firstPendingReceiptDate = receiptGaps.map(r => day(r.date)).filter(Boolean).sort()[0] || '';
  const firstPendingBankOnlyDate = pendingLegacy.filter(x => x.bank).map(x => day(x.record.date)).filter(Boolean).sort()[0] || '';
  const reportSettlementGap = pending.map(s => s.settledDate).filter(Boolean).sort()[0] || '';
  const reportPaymentGap = imported.filter(t => transactionStates.get(t.transactionId)?.code !== 'posted').map(t => t.date).filter(Boolean).sort()[0] || '';
  const settlementGap = [reportSettlementGap, ...pendingLegacy.map(x => day(x.record.date)), firstPendingReceiptDate].filter(Boolean).sort()[0] || '';
  const completed = scoped.filter(s => s.code === 'posted');
  // These dates describe the imported Paytm REPORT, not blanket completion of
  // every Shopify receipt or historical bank-only record. Those independent
  // gaps remain explicit and conservatively constrain overall coverage below.
  const through = latest(completed.filter(s => !reportSettlementGap || s.settledDate < reportSettlementGap).map(s => s.settledDate));
  const receiptCoverageThrough = latest(receipts.filter(r => receiptStates.get('PAYTM-SALE/' + r.id)?.code === 'posted' &&
    (!firstPendingReceiptDate || day(r.date) < firstPendingReceiptDate)).map(r => day(r.date)));
  const overallReconciledThrough = latest(completed.filter(s => !settlementGap || s.settledDate < settlementGap).map(s => s.settledDate));
  const last = completed.filter(s => s.settledDate === through).sort((a, b) => String(a.finalizedAt).localeCompare(String(b.finalizedAt))).at(-1);
  const coverage = {
    asOf, importedPaymentsThrough: latest(imported.map(t => t.date)),
    importedSettlementsThrough: latest(scoped.map(s => s.settledDate)),
    reportPaymentsThrough: latest(imported.filter(t => transactionStates.get(t.transactionId)?.code === 'posted' && (!reportPaymentGap || t.date < reportPaymentGap)).map(t => t.date)),
    reportSettlementsThrough: through,
    latestVerifiedSettlementDate: latest(completed.map(s => s.settledDate)),
    receiptCoverageThrough, overallReconciledThrough, firstPendingReceiptDate, firstPendingBankOnlyDate,
    bankSettlementsThrough: latest([...completed.map(s => s.bankDate || s.settledDate), ...legacy.filter(x => x.bank).map(x => x.bank.bankDate)]),
    pendingBankOnlySettlements: pendingLegacy.filter(x => x.bank).length,
    pendingReceipts: receiptGaps.length, pendingSettlements: pending.length + pendingLegacy.filter(x => !x.state).length,
    firstPendingSettlementDate: settlementGap,
    lastVerified: last ? { at: last.finalizedAt, by: last.finalizedBy } : null
  };
  const counts = { total: states.length, posted: 0, finalized: 0, ready: 0, incomplete: 0, bankLinkPending: 0 };
  states.forEach(s => { counts[s.code]++; if (s.code !== 'posted') counts.bankLinkPending++; });
  return { coverage, forRow, sharedStatus: { ...original, counts, settlements: states,
    incompleteSettlements: states.filter(s => s.code === 'incomplete') } };
}
module.exports = { buildProjection };

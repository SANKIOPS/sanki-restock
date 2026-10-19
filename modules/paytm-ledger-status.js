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
  if (record.utr !== payout.utr || day(record.settledDate || record.date) !== payout.settledDate ||
      ['net', 'gross', 'commission', 'platformFee', 'gst', 'nonCustomerAmount'].some(key => cents(record[key]) !== cents(payout[key]))) {
    throw Error('Saved Paytm finalization amounts, deductions, UTR or settled date changed. Review the source evidence.');
  }
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
  const payoutById = new Map(payouts.map(p => [p.settlementId, p]));
  const states = original.settlements.map(saved => {
    const payout = payoutById.get(saved.settlementId), state = { ...saved,
      reasons: [...saved.reasons], bankReasons: [...saved.bankReasons], transactionIds: [...payout.transactionIds] };
    if (state.code === 'posted' || state.code === 'finalized') {
      try {
        accounting.validateSettlementReview(store, payout.payoutId, saleRows, { allowExcluded: state.code === 'posted' });
        payout.transactionIds.forEach(id => {
          const tx = (store.paytmReportTransactions || {})[id], link = (store.paytmOrderLinks || {})[id] || (store.paytmManualResolutions || {})[id];
          if (tx.isCustomerPayment !== false && !(store.paytmExcludedTransactions || {})[id] && (!link || cents(link.amount) !== cents(tx.amount))) throw Error('A confirmed Paytm allocation no longer equals its original transaction amount.');
        });
        if (!state.historicalEvidenceLink) {
          const record = (state.code === 'posted' ? store.paytmPayoutPostings : store.paytmVerifiedSettlements || [])
            ?.find(x => x.settlementId === payout.settlementId || x.payoutId === payout.payoutId);
          savedSnapshot(record, payout);
          if (!(record.postedAt || record.finalizedAt) || !(record.postedBy || record.finalizedBy)) throw Error('Paytm finalization audit evidence is missing.');
          if (state.code === 'posted') {
            const bank = accounting.matchPayoutBank(store, payout);
            if (!bank.bankMatched || bank.bankCandidates[0].id !== record.bankTransactionId ||
                day(record.date) !== bank.bankCandidates[0].date) throw Error('The posted payout no longer has its unique exact Axis bank credit.');
            const id = record.bankTransactionId, book = (store.bankStatements || {})[accounting.BANK] || {};
            if ((store.paytmPayoutPostings || []).some(x => x !== record && (x.bankTransactionId === id || (x.transactionIds || []).some(t => payout.transactionIds.includes(t)))) ||
                (store.paytmSettlements || []).some(x => !x.accountingExcluded && (x.bankTransactionId === id || x.bankReference === payout.utr && day(x.date) === day(record.date))) ||
                [...(store.adjustments || []), ...(store.receipts || []), ...(store.bankTruthMovements || [])].some(x => !x.accountingExcluded && x.bankTransactionId === id) ||
                Object.entries(store.bankDateOverrides || {}).some(([key, x]) => key !== record.id && x.bankTransactionId === id) ||
                (book.imports || []).some(x => [].concat(x.reconciliationRows || [], x.carriedReconciliationRows || []).some(row => row.bank?.id === id && (row.linkedRecordIds || []).some(key => key !== record.id)))) {
              throw Error('Another ledger record claims this Paytm bank credit. Review its exclusive reconciliation ownership.');
            }
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
    let overallocated = false;
    try { overallocated = links.reduce((sum, x) => sum + cents(approvedBaseAmount(store, x.link)), 0) > rows.reduce((sum, r) => sum + cents(r.amount), 0); }
    catch { /* Existing rounding validation supplies the exact blocker. */ }
    if (overallocated) links.forEach(x => {
      if (!x.state) return;
      x.state.code = 'incomplete'; x.state.label = 'Incomplete — review required';
      x.state.reasons = unique(x.state.reasons.concat('Confirmed Paytm allocations exceed the current Shopify payment components for this order. Review the original receipt allocation.'));
    });
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
      if (overallocated) result = rowState('incomplete', links.map(x => x.state).filter(Boolean));
      else if (!statuses.length || statuses.length !== matched.length) result = rowState('report_pending', [], ['No verified detailed Paytm report allocation covers this receipt.']);
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
  const settlementGap = [pending[0]?.settledDate, ...pending.map(s => s.settledDate), ...pendingLegacy.map(x => day(x.record.date)), ...receiptGaps.map(r => day(r.date))].filter(Boolean).sort()[0] || '';
  const paymentGap = [...imported.filter(t => transactionStates.get(t.transactionId)?.code !== 'posted').map(t => t.date), ...receiptGaps.map(r => r.date)].filter(Boolean).sort()[0] || '';
  const completed = scoped.filter(s => s.code === 'posted');
  const through = latest(completed.filter(s => !settlementGap || s.settledDate < settlementGap).map(s => s.settledDate));
  const last = completed.filter(s => s.settledDate === through).sort((a, b) => String(a.finalizedAt).localeCompare(String(b.finalizedAt))).at(-1);
  const coverage = {
    asOf, importedPaymentsThrough: latest(imported.map(t => t.date)),
    importedSettlementsThrough: latest(scoped.map(s => s.settledDate)),
    reportPaymentsThrough: latest(imported.filter(t => transactionStates.get(t.transactionId)?.code === 'posted' && (!paymentGap || t.date < paymentGap)).map(t => t.date)),
    reportSettlementsThrough: through,
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

'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

// Every source is synthetic and isolated before expenses initializes its store.
const root = fs.mkdtempSync(path.join(os.tmpdir(), 'sanki-paytm-return-'));
process.env.DATA_PATH = path.join(root, 'data.json');
process.env.SHOPIFY_STORE = 'synthetic-paytm-return.myshopify.com';
process.env.SHOPIFY_ACCESS_TOKEN = '';
for (const [env, file] of Object.entries({ ORDERS_PATH: 'orders.json', SALES_PATH: 'sales.json', PROCUREMENT_PATH: 'procurement.json', INCENTIVES_PATH: 'incentives.json', VELOCITY_REMITTANCE_PATH: 'velocity.json' })) process.env[env] = path.join(root, file);
for (const file of ['expenses.json', 'orders.json', 'sales.json', 'procurement.json', 'incentives.json', 'velocity.json', 'salary.json', 'credit-cards.json']) fs.writeFileSync(path.join(root, file), '{}');
const { router } = require('../modules/expenses');
const { buildReport } = require('../modules/pnl-report');
const { validatePayoutPosting } = require('../modules/paytm-accounting');
const { shopifyClient } = require('../modules/shopify-client');
let networkRequests = 0;
shopifyClient.request = async () => { networkRequests++; throw new Error('Network disabled in synthetic return-resolution tests'); };
const expenseFile = path.join(root, 'expenses.json');
const baseline = JSON.parse(fs.readFileSync(expenseFile, 'utf8'));
const copy = x => JSON.parse(JSON.stringify(x));
const current = () => JSON.parse(fs.readFileSync(expenseFile, 'utf8'));
const save = s => fs.writeFileSync(expenseFile, JSON.stringify(s));
const IDS = { receipt: 'SYNTH-ACCIDENTAL-200', refund: 'SYNTH-REFUND-200', sale: 'SYNTH-SALE-8116', payout: 'SYNTH-RETURN-UTR', legacy: 'SYNTH-FINALIZED-SETTLEMENT', bank: 'SYNTH-AXIS-CREDIT' };

function invoke(url, { method = 'GET', role = 'admin', body = {}, query = {}, files = [] } = {}) {
  const layer = router.stack.find(x => x.route && x.route.path === url && x.route.methods[method.toLowerCase()]);
  assert.ok(layer, 'Registered route ' + url);
  const out = { status: 200 };
  const pending = layer.route.stack.at(-1).handle({ method, body, query, files, headers: {}, user: { username: role === 'admin' ? 'prashant' : 'synthetic-' + role, role, roles: [role] } }, { status(n) { out.status = n; return this; }, json(value) { out.body = value; return this; } });
  assert.ok(!pending || typeof pending.then !== 'function', 'Synthetic routes must finish synchronously');
  return out;
}
const view = role => invoke('/api/expenses/paytm-reports', { role }).body;
const ledger = (account = 'Paytm Settlement Clearing', to = '2026-10-18') => invoke('/api/expenses/account-ledger', { query: { nature: 'SANKI', account, from: '2026-10-16', to } }).body;
const resolve = (extra = {}, role = 'admin') => invoke('/api/expenses/paytm-reports/resolve-return', { method: 'POST', role, body: { receiptTransactionId: IDS.receipt, refundTransactionId: IDS.refund, confirmed: true, reason: 'Verified accidental customer receipt and its exact same-settlement refund against the original Paytm report', ...extra } });
const attach = (role = 'admin') => invoke('/api/expenses/paytm-reports/attach-historical', { method: 'POST', role, body: { payoutId: IDS.payout, reason: 'Exact existing finalized Axis bank credit verified; attach report evidence without creating any second bank posting' } });
const reverse = (extra = {}, role = 'admin') => invoke('/api/expenses/paytm-reports/reverse-return', { method: 'POST', role, body: { receiptTransactionId: IDS.receipt, reason: 'Unfinalized return review was selected incorrectly; review the original source pair again', ...extra } });
const finalize = () => invoke('/api/expenses/paytm-reports/finalize-settlement', { method: 'POST', body: { payoutId: IDS.payout } });
const cents = x => Math.round(Number(x || 0) * 100);
const credits = entries => entries.reduce((sum, row) => sum + cents(row.credit), 0) / 100;
const debits = entries => entries.reduce((sum, row) => sum + cents(row.debit), 0) / 100;
const financialRows = entries => entries.map(({ id, date, kind, description, reference, credit, debit, balance, reconciliation, proof }) => ({ id, date, kind, description, reference, credit, debit, balance, reconciliation, proof }));

function setup({ historical = true, fees = 0 } = {}) {
  const s = copy(baseline), orderId = '9902878', date = '2026-10-16', settledDate = '2026-10-17';
  Object.assign(s, { expenses: {}, adjustments: [], receipts: [], transfers: [], vendorAdvances: [], vendorOpeningPayables: [], bankTruthMovements: [], reconciliationExpenses: [], receivables: {}, salesRefunds: [], paytmReportTransactions: {}, paytmOrderLinks: {}, paytmShopifyPayments: {}, paytmManualResolutions: {}, paytmRoundingReviews: {}, paytmReturnResolutions: {}, paytmHistoricalEvidenceLinks: {}, paytmVerifiedSettlements: [], paytmPayoutPostings: [], paytmSettlements: [], paytmExcludedTransactions: {}, bankStatements: {}, bankDateOverrides: {}, bankReconciliationDrafts: {}, bankReconciliationApprovals: {}, saleAllocationOverrides: {}, saleComponentAmountOverrides: {}, openingBalances: { 'Axis Bank 3448': 0, 'Paytm Settlement Clearing': 0 } });
  const tx = (transactionId, amount, more = {}) => ({ transactionId, date, time: date + ' 20:15:15', amount, commission: 0, gst: 0, platformFee: 0, settledAmount: amount, payoutId: 'SYNTH-SOURCE-PAYOUT', utr: IDS.payout, settledDate, transactionType: 'ACQUIRING', isCustomerPayment: true, source: 'synthetic-original-paytm.csv', ...more });
  s.paytmReportTransactions[IDS.receipt] = tx(IDS.receipt, 200);
  s.paytmReportTransactions[IDS.refund] = tx(IDS.refund, 200, { transactionType: 'REFUND', isCustomerPayment: false, time: date + ' 21:36:24' });
  s.paytmReportTransactions[IDS.sale] = tx(IDS.sale, 8116, { commission: fees, settledAmount: 8116 - fees });
  s.paytmExcludedTransactions[IDS.receipt] = { reason: 'Accidental transfer by a customer; returned in the same settlement', by: 'prashant', at: date + 'T15:00:00Z' };
  s.paytmOrderLinks[IDS.sale] = { transactionId: IDS.sale, orderId, orderNumber: '9902878', amount: 8116, reason: 'Original successful Shopify receipt independently verified' };
  const orders = { [orderId]: { id: orderId, number: 9902878, orderNumber: 9902878, name: '#9902878', channel: 'POS', financialStatus: 'paid', total: 8116, taxesIncluded: true, shipping: 0, discount: 0, paymentGateways: ['Paytm'], createdAt: date + 'T12:00:00Z', processedAt: date + 'T12:00:00Z', note: 'Synthetic original sale', paymentTransactions: [{ id: 'SYNTH-CAPTURE', kind: 'sale', status: 'success', gateway: 'paytm', amount: 8116 }], lineItems: [{ id: 'SYNTH-LINE', sku: 'SYNTH-PAID-SKU', title: 'Synthetic garment', qty: 1, price: 8116, discount: 0 }] } };
  const bank = { id: IDS.bank, date: settledDate, credit: 8116 - fees, debit: 0, balance: 8116 - fees, reference: IDS.payout, description: 'Synthetic exact Paytm payout ' + IDS.payout };
  s.bankStatements['Axis Bank 3448'] = { transactions: { [bank.id]: bank }, imports: [], reconciledThrough: settledDate, lastReconciliation: { closingBalance: bank.credit, ledgerClosingBalance: bank.credit, balanceDifference: 0, finalizedBy: 'synthetic-owner', finalizedAt: '2026-10-18T10:00:00Z' } };
  if (historical) {
    s.bankStatements['Axis Bank 3448'].imports.push({ id: 'SYNTH-FINALIZED-IMPORT', from: settledDate, to: settledDate, finalizedBy: 'synthetic-owner', finalizedAt: '2026-10-18T10:00:00Z', ownerApproval: { approvedBy: 'synthetic-owner', approvedAt: '2026-10-18T10:00:00Z', fingerprint: 'SYNTH-ORIGINAL-APPROVAL' }, statementSummary: { closingBalance: bank.credit, totalCredits: bank.credit }, reconciliationRows: [{ id: 'bank-0', bank: copy(bank), linkedRecordIds: [IDS.legacy], reason: 'Existing bank settlement verified before the detailed report was reviewed' }] });
    s.paytmSettlements.push({ id: IDS.legacy, bankAccount: 'Axis Bank 3448', bankTransactionId: bank.id, date: settledDate, bankReference: IDS.payout, grossAmount: bank.credit, netAmount: bank.credit, chargeAmount: 0, paytmChargeAmount: 0, hiddenChargeAmount: 0, bankTruthOnly: true, provisional: false, source: 'axis_3448_paytm_bank_narration', orderIds: [], transferIds: [], otherReceipts: [] });
  }
  save(s); fs.writeFileSync(process.env.ORDERS_PATH, JSON.stringify({ orders }));
  // Warm only the isolated store's existing migrations before immutable checks.
  view();
  return { store: current(), orders, bank };
}
function pnl(orders, expenses = current()) { return buildReport({ orders: { orders }, expenses, purchases: {}, salary: {}, manualSales: {}, feeds: [], opening: { lots: [{ id: 'SYNTH-LOT', sku: 'SYNTH-PAID-SKU', qty: 1, unitCost: 100, verified: true }] } }, { from: '2026-10-16', to: '2026-10-18' }); }
function rejectsWithoutWrite(extra, role = 'admin') { const before = fs.readFileSync(expenseFile, 'utf8'), out = resolve(extra, role); assert.ok(out.status >= 400, JSON.stringify(out)); assert.equal(out.body.success, false); assert.equal(fs.readFileSync(expenseFile, 'utf8'), before, 'Rejected review must not alter saved accounting'); return out; }
test.after(() => { assert.equal(path.dirname(path.resolve(root)), path.resolve(os.tmpdir())); assert.ok(path.basename(root).startsWith('sanki-paytm-return-')); fs.rmSync(root, { recursive: true, force: true }); });

test('candidate and saved-state reads never resolve accidental receipts or touch source accounting', () => {
  setup(); const before = fs.readFileSync(expenseFile, 'utf8'), state = view();
  assert.equal(state.success, true); assert.equal(state.returnReviews.candidates.length, 1);
  const candidate = state.returnReviews.candidates[0]; assert.equal(candidate.receiptTransactionId, IDS.receipt); assert.equal(candidate.refundTransactionId, IDS.refund); assert.equal(candidate.amount, 200);
  assert.match(state.payouts[0].reconciliationStatus.reasons.join(' '), /unresolved REFUND/i);
  assert.equal(state.payouts[0].bankMatched, true, 'Matching statement evidence alone does not resolve its source payment/refund');
  ledger(); view('owner'); assert.equal(fs.readFileSync(expenseFile, 'utf8'), before);
});

test('confirmed ₹200 accidental receipt/refund is net-zero and never becomes sales, expenses or cash transfer', () => {
  const { orders, store } = setup(), ordersBefore = fs.readFileSync(process.env.ORDERS_PATH, 'utf8'), reportBefore = pnl(orders), axisBefore = ledger('Axis Bank 3448');
  const result = resolve(); assert.equal(result.status, 200, JSON.stringify(result.body)); assert.equal(result.body.created, true);
  const after = current(); assert.equal(after.paytmReturnResolutions[IDS.receipt].amount, 200);
  for (const key of ['paytmReportTransactions', 'paytmOrderLinks', 'paytmExcludedTransactions', 'bankStatements', 'bankReconciliationApprovals', 'bankDateOverrides', 'receipts', 'transfers', 'expenses', 'adjustments', 'reconciliationExpenses', 'paytmSettlements', 'paytmPayoutPostings', 'paytmVerifiedSettlements']) assert.deepEqual(after[key], store[key], key);
  assert.equal(fs.readFileSync(process.env.ORDERS_PATH, 'utf8'), ordersBefore);
  assert.deepEqual(ledger('Axis Bank 3448').entries, axisBefore.entries);
  const clearing = ledger(); assert.equal(credits(clearing.entries.filter(row => row.kind === 'paytm_customer_receipt')), 8116); assert.equal(debits(clearing.entries.filter(row => /charge/.test(row.kind))), 0);
  const reportAfter = pnl(orders); assert.deepEqual(reportAfter.totals, reportBefore.totals); assert.equal(reportAfter.totals.grossSales, 8116); assert.equal(reportAfter.totals.otherIncome, 0); assert.equal(reportAfter.totals.expenses, 0);
  assert.equal(networkRequests, 0);
});

test('exact repeated resolution is idempotent and cannot add another audit, posting or expense', () => {
  setup(); assert.equal(resolve().status, 200); const before = fs.readFileSync(expenseFile, 'utf8');
  const repeated = resolve(); assert.equal(repeated.status, 200, JSON.stringify(repeated.body)); assert.equal(repeated.body.created, false); assert.equal(fs.readFileSync(expenseFile, 'utf8'), before);
});

test('resolved report attaches to its original finalized bank evidence once without a duplicate bank credit or fee', () => {
  const { store, orders } = setup(); assert.equal(resolve().status, 200);
  assert.ok(view().payouts[0].reconciliationStatus.historicalCandidate);
  const originalBank = JSON.stringify(current().bankStatements), originalApprovals = JSON.stringify(current().bankReconciliationApprovals), before = ledger('Axis Bank 3448'), reportBefore = pnl(orders);
  const result = attach(); assert.equal(result.status, 200, JSON.stringify(result.body)); assert.equal(result.body.link.historicalSettlementId, IDS.legacy);
  const after = current(); assert.equal(JSON.stringify(after.bankStatements), originalBank); assert.equal(JSON.stringify(after.bankReconciliationApprovals), originalApprovals);
  assert.deepEqual(after.paytmPayoutPostings, []); assert.deepEqual(after.reconciliationExpenses, []); assert.equal(after.paytmSettlements[0].grossAmount, 8116); assert.equal(after.paytmSettlements[0].chargeAmount, 0);
  assert.deepEqual(financialRows(ledger('Axis Bank 3448').entries), financialRows(before.entries)); assert.equal(credits(before.entries), 8116);
  assert.equal(view().payouts[0].reconciliationStatus.code, 'posted'); assert.deepEqual(pnl(orders).totals, reportBefore.totals);
  const once = fs.readFileSync(expenseFile, 'utf8'); assert.ok(attach().status >= 400); assert.equal(fs.readFileSync(expenseFile, 'utf8'), once);
  const repeatedResolution = resolve(); assert.equal(repeatedResolution.status, 200); assert.equal(repeatedResolution.body.created, false); assert.equal(fs.readFileSync(expenseFile, 'utf8'), once, 'A delayed retry after attachment returns the original review without a second write');
  assert.deepEqual(after.paytmReportTransactions, store.paytmReportTransactions); assert.deepEqual(after.paytmExcludedTransactions, store.paytmExcludedTransactions);
});

test('only genuine source fees are completed once; a returned receipt never inflates historical gross or fees', () => {
  const { orders } = setup({ fees: 16 }); assert.equal(resolve().status, 200); const beforeBank = JSON.stringify(current().bankStatements);
  assert.equal(attach().status, 200); const s = current(); assert.equal(s.paytmSettlements[0].grossAmount, 8116); assert.equal(s.paytmSettlements[0].netAmount, 8100); assert.equal(s.paytmSettlements[0].chargeAmount, 16);
  assert.equal(s.reconciliationExpenses.length, 1); assert.equal(s.reconciliationExpenses[0].amount, 16); assert.equal(JSON.stringify(s.bankStatements), beforeBank);
  assert.equal(debits(ledger().entries.filter(row => /charge/.test(row.kind))), 16); assert.equal(pnl(orders).totals.expenses, 16);
  assert.ok(attach().status >= 400); assert.equal(current().reconciliationExpenses.length, 1);
});

test('unclaimed-bank finalization and posting preserve effective business gross and do not post refunds as VAS costs', () => {
  setup({ historical: false }); assert.equal(resolve().status, 200); assert.equal(finalize().status, 200);
  const verified = current().paytmVerifiedSettlements[0]; assert.equal(verified.customerGross, 8116); assert.equal(verified.rawCustomerGross, 8316); assert.equal(verified.returnedReceiptAmount, 200); assert.equal(verified.refundAmount, 200); assert.equal(verified.nonCustomerAmount, 0);
  assert.equal(view().payouts[0].reconciliationStatus.code, 'finalized', 'Saved effective gross must not be compared with the raw returned receipt gross');
  const posted = invoke('/api/expenses/paytm-reports/post-payout', { method: 'POST', body: { payoutId: IDS.payout, bankTransactionId: IDS.bank } }); assert.equal(posted.status, 200, JSON.stringify(posted.body));
  const s = current(); assert.equal(s.paytmPayoutPostings.length, 1); assert.equal(s.paytmPayoutPostings[0].customerGross, 8116); assert.deepEqual(s.reconciliationExpenses, []); assert.equal(debits(ledger().entries.filter(row => /charge/.test(row.kind))), 0);
  assert.equal(view().payouts[0].reconciliationStatus.code, 'posted');
  const currentLedger = ledger(); assert.equal(currentLedger.paytmCoverage.reportSettlementsThrough, '2026-10-17'); assert.equal(currentLedger.entries.find(row => row.kind === 'paytm_customer_receipt').paytmReconciliation.code, 'posted');
  const earlierLedger = ledger('Paytm Settlement Clearing', '2026-10-16'); assert.equal(earlierLedger.paytmCoverage.reportSettlementsThrough, ''); assert.notEqual(earlierLedger.entries.find(row => row.kind === 'paytm_customer_receipt').paytmReconciliation.code, 'posted', 'A later bank settlement cannot be pulled into an earlier as-of ledger');
  const once = fs.readFileSync(expenseFile, 'utf8'); assert.ok(invoke('/api/expenses/paytm-reports/post-payout', { method: 'POST', body: { payoutId: IDS.payout, bankTransactionId: IDS.bank } }).status >= 400); assert.equal(fs.readFileSync(expenseFile, 'utf8'), once);
});

test('effective-gross projections still reject tampered return snapshots, bank ownership or original source', () => {
  const mutations = [s => s.paytmPayoutPostings[0].gross = 8316, s => s.paytmPayoutPostings[0].rawCustomerGross++, s => s.paytmPayoutPostings[0].customerGross++, s => s.paytmPayoutPostings[0].refundAmount++, s => s.paytmPayoutPostings[0].returnedReceiptAmount++, s => s.paytmPayoutPostings[0].nonCustomerAmount = 200, s => s.paytmReportTransactions[IDS.refund].transactionType = 'VAS Deductions', s => s.paytmReportTransactions[IDS.refund].time = '2026-10-16 21:36:25', s => s.bankDateOverrides.FOREIGN = { bankTransactionId: IDS.bank }];
  for (const mutate of mutations) {
    setup({ historical: false }); assert.equal(resolve().status, 200); assert.equal(finalize().status, 200); assert.equal(invoke('/api/expenses/paytm-reports/post-payout', { method: 'POST', body: { payoutId: IDS.payout, bankTransactionId: IDS.bank } }).status, 200);
    const s = current(); mutate(s); save(s); const before = fs.readFileSync(expenseFile, 'utf8');
    const state = view(); assert.equal(state.payouts[0].reconciliationStatus.code, 'incomplete'); const clearing = ledger(); assert.notEqual(clearing.entries.find(row => row.kind === 'paytm_customer_receipt').paytmReconciliation.code, 'posted'); assert.equal(clearing.paytmCoverage.reportSettlementsThrough, ''); assert.equal(fs.readFileSync(expenseFile, 'utf8'), before);
  }
});

test('claimants cannot approve a return resolution or mutate personal/account permissions', () => {
  setup(); const denied = rejectsWithoutWrite({}, 'claimant'); assert.equal(denied.status, 403);
  const deniedAccounting = rejectsWithoutWrite({}, 'accounting'); assert.equal(deniedAccounting.status, 403);
});

test('confirmation, nonempty review reason and two exact source transaction IDs are mandatory', () => {
  for (const extra of [{ confirmed: false }, { confirmed: undefined }, { confirmed: 'true' }, { reason: 'short' }, { receiptTransactionId: '' }, { refundTransactionId: '' }, { refundTransactionId: IDS.receipt }]) { setup(); rejectsWithoutWrite(extra); }
});

test('wrong settlement, non-refund type, unequal amount, wrong refund order and nonzero pair fees are not auto-approved', () => {
  const mutations = [s => s.paytmReportTransactions[IDS.refund].utr = 'SYNTH-OTHER-UTR', s => s.paytmReportTransactions[IDS.refund].transactionType = 'VAS Deductions', s => s.paytmReportTransactions[IDS.refund].amount = 199, s => s.paytmReportTransactions[IDS.refund].settledDate = '2026-10-18', s => s.paytmReportTransactions[IDS.receipt].commission = 1, s => s.paytmReportTransactions[IDS.refund].gst = 1];
  for (const mutate of mutations) { setup(); const s = current(); mutate(s); save(s); rejectsWithoutWrite({}); }
});

test('an unexcluded, Shopify-linked, manually classified or rounded customer receipt cannot be reused as an accidental return', () => {
  const mutations = [s => delete s.paytmExcludedTransactions[IDS.receipt], s => s.paytmOrderLinks[IDS.receipt] = { transactionId: IDS.receipt, orderId: 'SYNTH-FOREIGN', amount: 200 }, s => s.paytmManualResolutions[IDS.receipt] = { type: 'manual_sale', amount: 200 }, s => s.paytmRoundingReviews[IDS.receipt] = { transactionId: IDS.receipt, amount: 200 }];
  for (const mutate of mutations) { setup(); const s = current(); mutate(s); save(s); rejectsWithoutWrite({}); }
});

test('one refund cannot resolve two accidental receipts', () => {
  setup(); assert.equal(resolve().status, 200); const s = current(); s.paytmReportTransactions['SYNTH-SECOND-RECEIPT'] = { ...s.paytmReportTransactions[IDS.receipt], transactionId: 'SYNTH-SECOND-RECEIPT' }; s.paytmExcludedTransactions['SYNTH-SECOND-RECEIPT'] = copy(s.paytmExcludedTransactions[IDS.receipt]); save(s);
  rejectsWithoutWrite({ receiptTransactionId: 'SYNTH-SECOND-RECEIPT' });
});

test('changes to source, exclusion or paired refund invalidate saved evidence and block historical attachment', () => {
  const mutations = [s => s.paytmReportTransactions[IDS.receipt].amount++, s => s.paytmReportTransactions[IDS.refund].amount++, s => s.paytmReportTransactions[IDS.receipt].date = '2026-10-15', s => s.paytmReportTransactions[IDS.refund].time = '2026-10-16 21:36:25', s => s.paytmReportTransactions[IDS.refund].transactionType = 'VAS Deductions', s => s.paytmReportTransactions[IDS.refund].utr = 'SYNTH-OTHER-UTR', s => delete s.paytmReportTransactions[IDS.refund], s => s.paytmExcludedTransactions[IDS.receipt].reason = 'Changed exclusion after review'];
  for (const mutate of mutations) {
    setup(); assert.equal(resolve().status, 200); const s = current(); mutate(s); save(s); const before = fs.readFileSync(expenseFile, 'utf8');
    const reviewState = view(); assert.equal(reviewState.payouts.find(x => x.payoutId === IDS.payout).reconciliationStatus.code, 'incomplete'); assert.ok(reviewState.returnReviews.resolutions.some(x => !x.valid));
    assert.ok(attach().status >= 400); assert.equal(fs.readFileSync(expenseFile, 'utf8'), before);
  }
});

test('new resolution cannot silently reclassify an already verified, posted or historically attached payout', () => {
  const mutations = [s => s.paytmVerifiedSettlements.push({ settlementId: IDS.payout, transactionIds: [IDS.receipt, IDS.refund, IDS.sale] }), s => s.paytmPayoutPostings.push({ settlementId: IDS.payout, payoutId: IDS.payout, transactionIds: [IDS.receipt, IDS.refund, IDS.sale] }), s => s.paytmHistoricalEvidenceLinks[IDS.payout] = { settlementId: IDS.payout, bankTransactionId: IDS.bank, historicalSettlementId: IDS.legacy }];
  for (const mutate of mutations) { setup(); const s = current(); mutate(s); save(s); rejectsWithoutWrite({}); }
});

test('return resolution cannot bypass wrong finalized bank claims, changed amounts, missing Owner evidence or duplicate payout lock', () => {
  const mutations = [s => s.bankStatements['Axis Bank 3448'].imports[0].reconciliationRows[0].linkedRecordIds = ['SYNTH-FOREIGN-LEDGER'], s => s.bankStatements['Axis Bank 3448'].imports[0].reconciliationRows[0].linkedRecordIds.push('SYNTH-FOREIGN-LEDGER'), s => delete s.bankStatements['Axis Bank 3448'].imports[0].finalizedBy, s => s.paytmSettlements[0].netAmount++, s => s.paytmSettlements[0].date = '2026-10-18', s => s.bankDateOverrides.FOREIGN = { bankTransactionId: IDS.bank }];
  for (const mutate of mutations) { setup(); assert.equal(resolve().status, 200); const s = current(); mutate(s); save(s); const before = fs.readFileSync(expenseFile, 'utf8'); assert.ok(attach().status >= 400); assert.equal(fs.readFileSync(expenseFile, 'utf8'), before); }
  setup(); assert.equal(resolve().status, 200); assert.throws(() => validatePayoutPosting(current(), IDS.payout, IDS.bank, [{ id: 'SHOPIFY/9902878', orderId: '9902878', amount: 8116, account: 'Paytm Settlement Clearing' }]), /already uses this payout or bank transaction|finalized reconciliation/);
});

test('reimported bank-row IDs cannot bypass exact finalized UTR/date/net evidence or create another credit', () => {
  setup(); assert.equal(resolve().status, 200); const s = current();
  const history = s.bankStatements['Axis Bank 3448'].imports[0].reconciliationRows[0]; history.bank.id = 'SYNTH-OLD-IMPORT-BANK-ID'; delete s.paytmSettlements[0].bankTransactionId; save(s);
  const beforeBank = JSON.stringify(current().bankStatements), rows = [{ id: 'SHOPIFY/9902878', orderId: '9902878', amount: 8116, account: 'Paytm Settlement Clearing' }];
  assert.throws(() => validatePayoutPosting(current(), IDS.payout, IDS.bank, rows), /finalized reconciliation/);
  assert.equal(attach().status, 200); assert.equal(JSON.stringify(current().bankStatements), beforeBank); assert.deepEqual(current().paytmPayoutPostings, []); assert.equal(credits(ledger('Axis Bank 3448').entries), 8116);
});

test('source calendar validity and real chronological evidence are mandatory, not just equal amounts', () => {
  const mutations = [s => { s.paytmReportTransactions[IDS.receipt].date = '2026-02-30'; s.paytmReportTransactions[IDS.refund].date = '2026-03-03'; }, s => s.paytmReportTransactions[IDS.refund].date = '2026-10-15', s => s.paytmReportTransactions[IDS.refund].time = '2026-10-16 20:15:15', s => s.paytmReportTransactions[IDS.receipt].time = '', s => s.paytmReportTransactions[IDS.refund].time = '2026-10-16 25:00:00'];
  for (const mutate of mutations) { setup(); const s = current(); mutate(s); save(s); rejectsWithoutWrite({}); }
});

test('resolved excluded receipt cannot be restored, relinked or reclassified behind its audited pair', () => {
  setup(); assert.equal(resolve().status, 200);
  for (const [url, body] of [['restore', { transactionId: IDS.receipt, reason: 'Attempt to replace the excluded receipt behind its pair' }], ['link-order', { transactionId: IDS.receipt, orderId: '9902878', reason: 'Attempt to turn a non-sale refund pair into sales' }], ['classify', { transactionId: IDS.receipt, type: 'manual_sale', party: 'Synthetic customer', details: 'Attempted new sale', reason: 'Attempted replacement of audited refund pair', proof: '/api/expenses/photo/synthetic.jpg' }]]) {
    const before = fs.readFileSync(expenseFile, 'utf8'), out = invoke('/api/expenses/paytm-reports/' + url, { method: 'POST', role: 'owner', body }); assert.ok(out.status >= 400, url); assert.equal(fs.readFileSync(expenseFile, 'utf8'), before);
  }
});

test('unfinalized return can be reversed with its original evidence and exclusion preserved in audit history', () => {
  const { store } = setup(); assert.equal(resolve().status, 200); const reviewed = current(), originalReview = reviewed.paytmReturnResolutions[IDS.receipt];
  const result = reverse({}, 'owner'); assert.equal(result.status, 200, JSON.stringify(result.body)); assert.deepEqual(result.body.history.resolution, originalReview); assert.equal(result.body.history.reversedBy, 'synthetic-owner');
  const after = current(); assert.equal(after.paytmReturnResolutions[IDS.receipt], undefined);
  for (const key of ['paytmReportTransactions', 'paytmOrderLinks', 'paytmExcludedTransactions', 'bankStatements', 'bankReconciliationApprovals', 'bankDateOverrides', 'receipts', 'transfers', 'expenses', 'adjustments', 'reconciliationExpenses', 'paytmSettlements', 'paytmPayoutPostings', 'paytmVerifiedSettlements']) assert.deepEqual(after[key], store[key], key);
  assert.ok(JSON.stringify(after).includes('PAYTM_ACCIDENTAL_RECEIPT_RETURN_REVERSED')); assert.equal(view().payouts[0].reconciliationStatus.code, 'incomplete'); assert.equal(view().returnReviews.candidates.length, 1);
  const beforeRetry = fs.readFileSync(expenseFile, 'utf8'); assert.ok(reverse().status >= 400); assert.equal(fs.readFileSync(expenseFile, 'utf8'), beforeRetry);
});

test('correction of a stale unfinalized review is possible without weakening source validation', () => {
  setup(); assert.equal(resolve().status, 200); const s = current(); s.paytmExcludedTransactions[IDS.receipt].reason = 'Updated reason after an unfinalized mistaken review'; save(s);
  assert.equal(view().returnReviews.resolutions[0].valid, false); assert.equal(reverse().status, 200); assert.equal(current().paytmExcludedTransactions[IDS.receipt].reason, s.paytmExcludedTransactions[IDS.receipt].reason);
  assert.equal(resolve().status, 200); assert.equal(view().returnReviews.resolutions[0].valid, true);
});

test('return reversal requires an authorized actor and reason and is blocked after report finalization, posting or historical attachment', () => {
  for (const [extra, role] of [[{ reason: 'short' }, 'admin'], [{ receiptTransactionId: '' }, 'owner'], [{}, 'claimant'], [{}, 'accounting']]) {
    setup(); assert.equal(resolve().status, 200); const before = fs.readFileSync(expenseFile, 'utf8'); assert.ok(reverse(extra, role).status >= 400); assert.equal(fs.readFileSync(expenseFile, 'utf8'), before);
  }
  for (const action of ['verified', 'posted', 'attached']) {
    setup({ historical: action === 'attached' }); assert.equal(resolve().status, 200);
    if (action === 'attached') assert.equal(attach().status, 200);
    else { assert.equal(finalize().status, 200); if (action === 'posted') assert.equal(invoke('/api/expenses/paytm-reports/post-payout', { method: 'POST', body: { payoutId: IDS.payout, bankTransactionId: IDS.bank } }).status, 200); }
    const before = fs.readFileSync(expenseFile, 'utf8'), result = reverse({}, 'owner'); assert.ok(result.status >= 400); assert.equal(fs.readFileSync(expenseFile, 'utf8'), before); assert.equal(view().returnReviews.resolutions[0].canReverse, false);
  }
});

test('source enrichment of either paired transaction is refused before any preview is saved', () => {
  for (const id of [IDS.receipt, IDS.refund]) {
    setup(); assert.equal(resolve().status, 200); const tx = current().paytmReportTransactions[id];
    const csv = ['Transaction_ID,Transaction_Type,Transaction_Date,Status,Amount,Commission,GST,Platform_Fee,Settled_Amount,Payout_ID,UTR_No.,Settled_Date,Comments', [tx.transactionId, tx.transactionType, tx.time, 'SUCCESS', tx.amount, 0, 0, 0, tx.settledAmount, tx.payoutId, tx.utr, tx.settledDate, 'New source comment supplied after audited return'].join(',')].join('\n');
    const before = fs.readFileSync(expenseFile, 'utf8'), result = invoke('/api/expenses/paytm-reports/preview', { method: 'POST', files: [{ originalname: 'synthetic-enriched-report.csv', buffer: Buffer.from(csv) }] });
    assert.ok(result.status >= 400, JSON.stringify(result.body)); assert.match(result.body.error, /locked by an audited return/i); assert.equal(fs.readFileSync(expenseFile, 'utf8'), before);
  }
});

test('preexisting finalized refund-as-fee data is flagged for correction, not silently rewritten on reads', () => {
  setup(); const s = current(); s.paytmVerifiedSettlements.push({ id: 'SYNTH-OLD-VERIFIED', settlementId: IDS.payout, payoutId: IDS.payout, utr: IDS.payout, settledDate: '2026-10-17', transactionIds: [IDS.receipt, IDS.refund, IDS.sale], customerGross: 8316, gross: 8316, nonCustomerAmount: 200, commission: 0, platformFee: 0, gst: 0, net: 8116 });
  s.reconciliationExpenses.push({ id: 'SYNTH-OLD-REFUND-FEE', nature: 'SANKI', date: '2026-10-17', amount: 200, account: 'Paytm Settlement Clearing', category: 'PAYTM CHARGES', particulars: 'Legacy refund classification awaiting an explicit accounting correction' }); save(s);
  const before = fs.readFileSync(expenseFile, 'utf8'), reviewState = view(); assert.equal(reviewState.payouts[0].reconciliationStatus.code, 'incomplete'); assert.match(reviewState.payouts[0].reconciliationStatus.reasons.join(' '), /REFUND/i);
  ledger(); assert.equal(fs.readFileSync(expenseFile, 'utf8'), before); assert.equal(current().reconciliationExpenses[0].amount, 200); rejectsWithoutWrite({});
});

'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

// Importing expenses initializes and repairs its store. Isolate every input
// before import: these regressions can never load real money or Shopify data.
const root = fs.mkdtempSync(path.join(__dirname, '.tmp-paytm-ledger-status-'));
process.env.DATA_PATH = path.join(root, 'data.json');
process.env.SHOPIFY_STORE = 'synthetic-paytm-ledger-status.myshopify.com';
process.env.SHOPIFY_ACCESS_TOKEN = '';
for (const [env, file] of Object.entries({
  ORDERS_PATH: 'orders.json', SALES_PATH: 'sales.json', PROCUREMENT_PATH: 'procurement.json',
  INCENTIVES_PATH: 'incentives.json', VELOCITY_REMITTANCE_PATH: 'velocity.json'
})) process.env[env] = path.join(root, file);
for (const file of ['expenses.json', 'orders.json', 'sales.json', 'procurement.json',
  'incentives.json', 'velocity.json', 'salary.json', 'credit-cards.json']) {
  fs.writeFileSync(path.join(root, file), '{}');
}
const { router } = require('../modules/expenses');
const { shopifyClient } = require('../modules/shopify-client');
let networkRequests = 0;
shopifyClient.request = async () => {
  networkRequests++;
  throw new Error('Network is disabled in the synthetic Paytm status fixture');
};
const expenseFile = path.join(root, 'expenses.json');
const baseline = JSON.parse(fs.readFileSync(expenseFile, 'utf8'));
const copy = value => JSON.parse(JSON.stringify(value));
const current = () => JSON.parse(fs.readFileSync(expenseFile, 'utf8'));
const save = value => fs.writeFileSync(expenseFile, JSON.stringify(value));
const currentOrders = () => JSON.parse(fs.readFileSync(process.env.ORDERS_PATH, 'utf8')).orders;
const saveOrders = orders => fs.writeFileSync(process.env.ORDERS_PATH, JSON.stringify({ orders }));
const TRANSACTION = 'SYNTHETIC-PAYTM-728280';
const PAYOUT = 'SYNTHETIC-PB0327381793';

function invoke(url, { method = 'GET', role = 'admin', query = {}, body = {} } = {}) {
  const layer = router.stack.find(item => item.route && item.route.path === url && item.route.methods[method.toLowerCase()]);
  assert.ok(layer, 'Registered route: ' + url);
  const response = { status: 200 };
  const result = layer.route.stack.at(-1).handle({ method, path: url, query, body, headers: {},
    user: { username: role === 'admin' ? 'prashant' : 'synthetic-owner', role, roles: [role] }
  }, {
    status(code) { response.status = code; return this; },
    json(value) { response.body = value; return this; }
  });
  assert.ok(!result || typeof result.then !== 'function', 'Fixture uses synchronous read-only routes');
  return response;
}

function ledger(role = 'admin', to = '2026-10-19', account = 'Paytm Settlement Clearing') {
  const result = invoke('/api/expenses/account-ledger', { role,
    query: { nature: 'SANKI', account, from: '2026-10-01', to } });
  assert.equal(result.status, 200, JSON.stringify(result.body));
  assert.equal(result.body.success, true);
  return result.body;
}
function report(role = 'admin') {
  const result = invoke('/api/expenses/paytm-reports', { role });
  assert.equal(result.status, 200, JSON.stringify(result.body));
  return result.body;
}

function order(number, date, amount, suffix) {
  const id = String(9000000 + number);
  return { id, number, orderNumber: number, name: '#' + number, channel: 'POS',
    financialStatus: 'paid', total: amount, processedAt: date + 'T12:00:00Z', createdAt: date + 'T12:00:00Z',
    note: suffix + '\nSynthetic salesperson', paymentGateways: ['Paytm'],
    paymentTransactions: [{ id: 'SYNTHETIC-CAPTURE-' + number, kind: 'sale', status: 'success',
      gateway: 'Paytm', amount, processed_at: date + 'T12:00:00Z' }] };
}

function setup({ later = true, fullOrderAmount = 299 } = {}) {
  const store = copy(baseline);
  Object.assign(store, {
    expenses: {}, adjustments: [], receipts: [], transfers: [], vendorAdvances: [],
    reconciliationExpenses: [], bankTruthMovements: [], receivables: {}, salesRefunds: [],
    paytmReportTransactions: {}, paytmOrderLinks: {}, paytmRoundingReviews: {},
    paytmShopifyPayments: {}, paytmManualResolutions: {}, paytmExcludedTransactions: {},
    paytmSettlements: [], paytmPayoutPostings: [], paytmVerifiedSettlements: [],
    paytmHistoricalEvidenceLinks: {}, paytmReportDrafts: {}, paytmReportImports: [],
    bankDateOverrides: {}, bankReconciliationDrafts: {}, bankReconciliationApprovals: {},
    bankReconciliationLinks: {}, saleAllocationOverrides: {}, saleComponentAmountOverrides: {},
    openingBalances: { 'Axis Bank 3448': 100, 'Paytm Settlement Clearing': 0 },
    bankStatements: { 'Axis Bank 3448': { transactions: {}, imports: [], reconciledThrough: '2026-10-18' } }
  });
  const orders = {};
  const original = order(2871, '2026-10-13', fullOrderAmount, '728280');
  orders[original.id] = original;
  store.paytmReportTransactions[TRANSACTION] = {
    transactionId: TRANSACTION, date: '2026-10-13', amount: 299, commission: 8.94,
    platformFee: 0, gst: 0, settledAmount: 290.06, payoutId: PAYOUT, utr: PAYOUT,
    settledDate: '2026-10-14', isCustomerPayment: true
  };
  store.paytmOrderLinks[TRANSACTION] = {
    transactionId: TRANSACTION, orderId: original.id, orderNumber: '2871', amount: 299,
    matchBasis: 'transaction_id_suffix_in_shopify_note', transactionSuffix: '728280'
  };
  store.paytmReportImports.push({ id: 'SYNTHETIC-REPORT-13', at: '2026-10-19T08:00:00Z',
    by: 'prashant', count: 1, sources: [{ name: 'synthetic-report.csv' }] });
  const bank = { id: 'SYNTHETIC-BANK-14', date: '2026-10-14', credit: 290.06, debit: 0,
    reference: PAYOUT, description: 'Paytm ' + PAYOUT };
  const book = store.bankStatements['Axis Bank 3448'];
  book.transactions[bank.id] = bank;
  book.imports.push({ id: 'SYNTHETIC-BST-14', from: '2026-10-14', to: '2026-10-14',
    finalizedAt: '2026-10-15T08:00:00Z', finalizedBy: 'synthetic-owner',
    reconciliationRows: [{ bank, linkedRecordIds: ['SYNTHETIC-HIST-14'] }] });
  store.paytmSettlements.push({ id: 'SYNTHETIC-HIST-14', date: bank.date, bankAccount: 'Axis Bank 3448',
    bankReference: PAYOUT, netAmount: 290.06, grossAmount: 299, chargeAmount: 8.94,
    orderIds: [original.id], createdAt: '2026-10-15T08:00:00Z', createdBy: 'synthetic-owner' });
  if (later) {
    for (const [date, number, gross, net, suffix] of [
      ['2026-10-17', 2882, 6397, 8116, '946765'],
      ['2026-10-18', 2886, 5297, 13694, '889123']
    ]) {
      const laterOrder = order(number, date, gross, suffix);
      orders[laterOrder.id] = laterOrder;
      const id = 'SYNTHETIC-BANK-ONLY-' + date.slice(-2), reference = 'SYNTHETIC-UTR-' + date.slice(-2);
      const bankRow = { id: 'BANK-' + id, date, credit: net, debit: 0, reference };
      book.transactions[bankRow.id] = bankRow;
      book.imports.push({ id: 'BST-' + id, from: date, to: date,
        finalizedAt: date + 'T15:00:00Z', finalizedBy: 'prashant',
        reconciliationRows: [{ bank: bankRow, linkedRecordIds: [id] }] });
      store.paytmSettlements.push({ id, date, bankAccount: 'Axis Bank 3448',
        netAmount: net, grossAmount: net, chargeAmount: 0, paytmChargeAmount: 0,
        hiddenChargeAmount: 0, bankReference: reference, orderIds: [], transferIds: [], otherReceipts: [],
        bankTruthOnly: true, provisional: false, source: 'axis_3448_paytm_bank_narration',
        createdAt: date + 'T15:00:00Z', createdBy: 'prashant' });
    }
  }
  save(store);
  saveOrders(orders);
  // The existing audited workflow creates the attachment. It independently
  // verifies exact gross/fees/net, unique UTR and finalized bank ownership.
  const attached = invoke('/api/expenses/paytm-reports/attach-historical', { method: 'POST', body: {
    payoutId: PAYOUT, reason: 'Synthetic report agrees with original finalized bank credit, exact gross, fees, date and UTR'
  } });
  assert.equal(attached.status, 200, JSON.stringify(attached.body));
  return current();
}

function paytmRows(result) {
  return result.entries.filter(entry => entry.kind.startsWith('paytm_'));
}
function statusSnapshot(result) {
  return { coverage: result.paytmCoverage, through: result.reconciledThrough,
    last: result.lastReconciliation,
    entries: paytmRows(result).map(entry => ({ id: entry.id, date: entry.date,
      credit: entry.credit, debit: entry.debit, status: entry.paytmReconciliation })) };
}
function movementSnapshot(result) {
  return { balance: result.balance, entries: result.entries.map(entry =>
    [entry.id, entry.date, entry.credit, entry.debit, entry.balance]) };
}

test('Owner and Admin share exact posted receipt, fee and settlement status with distinct report and bank coverage', () => {
  setup();
  const owner = ledger('owner'), admin = ledger('admin');
  assert.deepEqual(statusSnapshot(owner), statusSnapshot(admin));
  assert.deepEqual(report('owner').reconciliationStatus, report('admin').reconciliationStatus);
  const completed = paytmRows(admin).filter(row => row.orderNumber === '2871' ||
    row.date === '2026-10-14' && row.reference === PAYOUT &&
      ['paytm_settlement', 'paytm_charge'].includes(row.kind));
  assert.equal(completed.length, 3, 'Receipt, settlement and fee are all shown');
  for (const row of completed) {
    assert.equal(row.paytmReconciliation.code, 'posted', row.id);
    assert.ok(row.paytmReconciliation.label, row.id + ' has a clear status label');
  }
  const coverage = admin.paytmCoverage;
  assert.equal(coverage.reportPaymentsThrough, '2026-10-13');
  assert.equal(coverage.reportSettlementsThrough, '2026-10-14');
  assert.equal(coverage.importedPaymentsThrough, '2026-10-13');
  assert.equal(coverage.importedSettlementsThrough, '2026-10-14');
  assert.equal(coverage.bankSettlementsThrough, '2026-10-18');
  assert.equal(admin.reconciledThrough, '2026-10-14', 'Bank-only settlement dates cannot advance report verification');
  assert.ok(!admin.lastReconciliation || admin.lastReconciliation.through === '2026-10-14');
});

test('later bank-confirmed placeholders and unreported Shopify receipts remain explicitly pending, not reconciled by date', () => {
  setup();
  const result = ledger();
  const bankOnly = result.entries.filter(row => row.id.startsWith('SYNTHETIC-BANK-ONLY-'));
  assert.equal(bankOnly.length, 2);
  for (const row of bankOnly) {
    assert.equal(row.paytmReconciliation.code, 'bank_confirmed_report_pending');
    assert.ok(row.paytmReconciliation.reasons.length > 0, 'Bank net alone cannot verify customer gross and fees');
  }
  const laterReceipts = result.entries.filter(row => row.kind === 'paytm_customer_receipt' && row.date > '2026-10-14');
  assert.equal(laterReceipts.length, 2);
  for (const row of laterReceipts) assert.equal(row.paytmReconciliation.code, 'report_pending');
  for (const row of result.entries.filter(row => row.kind === 'paytm_customer_receipts')) {
    assert.equal(row.paytmReconciliation.code, 'summary', 'A zero-money daily total is not independent finalized evidence');
  }
  assert.equal(result.paytmCoverage.pendingBankOnlySettlements, 2);
  assert.equal(result.paytmCoverage.pendingReceipts, 2);
  assert.equal(result.paytmCoverage.firstPendingSettlementDate, '2026-10-17');
});

test('read-only status and coverage refreshes do not write movements, change balances, or mutate report and bank evidence', () => {
  setup();
  const beforeLedger = ledger(), beforeBank = ledger('admin', '2026-10-19', 'Axis Bank 3448');
  report();
  const before = fs.readFileSync(expenseFile, 'utf8');
  const ordersBefore = fs.readFileSync(process.env.ORDERS_PATH, 'utf8');
  for (const role of ['owner', 'admin', 'owner']) {
    assert.deepEqual(movementSnapshot(ledger(role)), movementSnapshot(beforeLedger));
    assert.deepEqual(movementSnapshot(ledger(role, '2026-10-19', 'Axis Bank 3448')), movementSnapshot(beforeBank));
    report(role);
    assert.equal(fs.readFileSync(expenseFile, 'utf8'), before);
    assert.equal(fs.readFileSync(process.env.ORDERS_PATH, 'utf8'), ordersBefore);
  }
  assert.equal(networkRequests, 0);
});

test('as-of views cannot expose a future bank settlement as already completed or advance the cutoff past the selected date', () => {
  setup();
  const result = ledger('admin', '2026-10-13');
  assert.ok(!result.reconciledThrough || result.reconciledThrough <= '2026-10-13');
  for (const key of ['reportPaymentsThrough', 'reportSettlementsThrough', 'bankSettlementsThrough']) {
    assert.ok(!result.paytmCoverage[key] || result.paytmCoverage[key] <= '2026-10-13', key);
  }
  const receipt = result.entries.find(row => row.orderNumber === '2871');
  assert.ok(receipt);
  assert.notEqual(receipt.paytmReconciliation.code, 'posted', 'The actual bank settlement occurs tomorrow');
  assert.equal(result.entries.some(row => row.date > '2026-10-13'), false);
});

test('saved finalized or posted flags never conceal stale historical bank ownership and missing source evidence', () => {
  for (const mutate of [
    store => { store.bankStatements['Axis Bank 3448'].imports[0].reconciliationRows[0].linkedRecordIds.push('FOREIGN-RECORD'); },
    store => { delete store.bankStatements['Axis Bank 3448'].imports[0].finalizedAt; },
    store => { store.bankStatements['Axis Bank 3448'].transactions['SYNTHETIC-BANK-14'].credit = 290.07; },
    store => { store.paytmOrderLinks[TRANSACTION].amount = 298; },
    store => {
      delete store.paytmOrderLinks[TRANSACTION];
      store.paytmManualResolutions[TRANSACTION] = { transactionId: TRANSACTION, amount: 299,
        type: 'manual_sale', recordId: 'SYNTHETIC-MANUAL-SALE' };
      store.receipts.push({ id: 'SYNTHETIC-MANUAL-SALE', paytmTransactionId: TRANSACTION,
        nature: 'SANKI', date: '2026-10-13', account: 'Paytm Settlement Clearing',
        amount: 299, source: 'Synthetic manual customer', proof: '' });
    }
  ]) {
    const store = setup({ later: false });
    mutate(store);
    save(store);
    const result = ledger();
    const affected = paytmRows(result).filter(row => row.kind !== 'paytm_customer_receipts');
    assert.ok(affected.length);
    for (const row of affected) {
      assert.notEqual(row.paytmReconciliation.code, 'posted', row.id);
      assert.ok(row.paytmReconciliation.reasons.length + row.paytmReconciliation.bankReasons.length > 0, row.id);
    }
    assert.notEqual(result.reconciledThrough, '2026-10-14');
  }
});

test('a partially covered order is not blanket-marked posted merely because one payment transaction is reconciled', () => {
  setup({ later: false, fullOrderAmount: 500 });
  const result = ledger();
  const receipts = result.entries.filter(row => row.kind === 'paytm_customer_receipt' && row.orderNumber === '2871');
  assert.ok(receipts.length);
  assert.equal(receipts.reduce((sum, row) => sum + row.credit, 0), 500);
  for (const row of receipts) {
    assert.notEqual(row.paytmReconciliation.code, 'posted');
    assert.ok(row.paytmReconciliation.reasons.length, 'The uncovered remainder is explained');
  }
  assert.equal(result.entries.find(row => row.kind === 'paytm_settlement' && row.reference === PAYOUT).paytmReconciliation.code, 'posted',
    'The valid ₹299 settlement itself remains valid; only the larger receipt remains partially covered');
});

test('claimants cannot use the status projection to gain access to financial reconciliation routes', () => {
  setup();
  for (const url of ['/api/expenses/account-ledger', '/api/expenses/paytm-reports']) {
    const result = invoke(url, { role: 'claimant', query: { nature: 'SANKI', account: 'Paytm Settlement Clearing' } });
    assert.equal(result.status, 403, url);
  }
});

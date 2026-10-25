'use strict';

// Real Express routes, isolated fictional accounting records, no live services.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const directory = fs.mkdtempSync(path.join(__dirname, '.tmp-sanki-7883-payment-'));
process.env.DATA_PATH = path.join(directory, 'data.json');
process.env.PROCUREMENT_PATH = path.join(directory, 'procurement.json');
process.env.ORDERS_PATH = path.join(directory, 'orders.json');
process.env.SALES_PATH = path.join(directory, 'sales.json');
process.env.TEMP = directory;
process.env.TMP = directory;
process.env.TELEGRAM_BOT_TOKEN = '';
const file = path.join(directory, 'expenses.json');
const rentalSource = require.resolve('../modules/rental-source.json');
require.cache[rentalSource] = { id: rentalSource, filename: rentalSource, loaded: true, exports: { id: 'SYNTHETIC-7883-PAYMENT', asOf: '2098-01-01', rows: [] } };
fs.writeFileSync(file, '{}');
const { router, appBankMovements, summaryForPL } = require('../modules/expenses');
const account = 'IndusInd Bank 7883', date = '2098-04-02';
const expenseId = 'EX-SYNTHETIC-SANKI-7883', draftId = 'BRD-SYNTHETIC-SANKI-7883';
const read = () => JSON.parse(fs.readFileSync(file, 'utf8'));
const write = value => fs.writeFileSync(file, JSON.stringify(value));

function invoke(method, route, { body = {}, query = {}, role = 'admin', username = 'prashant', permissions = {} } = {}) {
  return new Promise((resolve, reject) => {
    let status = 200, complete = false;
    const timer = setTimeout(() => reject(new Error('Route did not complete: ' + route)), 10000);
    const finish = value => { if (complete) return; complete = true; clearTimeout(timer); resolve({ status, body: value }); };
    const req = { method, path: route, url: route, originalUrl: route, baseUrl: '', body, query,
      ip: '203.0.113.70', headers: {}, get: () => '', user: { role, roles: [role], username, permissions } };
    const res = { statusCode: 200, headersSent: false, status(value) { status = value; this.statusCode = value; return this; },
      setHeader() {}, getHeader() {}, removeHeader() {}, json(value) { this.headersSent = true; finish(value); return this; },
      send(value) { this.headersSent = true; finish(value); return this; }, end: finish };
    try { router.handle(req, res, error => { if (error) { clearTimeout(timer); reject(error); } else finish(undefined); }); }
    catch (error) { clearTimeout(timer); reject(error); }
  });
}
const owner = { role: 'owner', username: 'synthetic-owner' };
const bill = (id = expenseId, amount = 1650, nature = 'SANKI', vendor = 'Synthetic Equipment Vendor') => ({
  id, nature, vendor, date, amount, paidAmount: 0, status: 'approved', ledger: 'FLOWERS', type: 'variable', channel: 'Shared',
  particulars: 'Fictional business equipment bill', claimant: 'synthetic-claimant', createdBy: 'synthetic-claimant',
  approvedBy: 'synthetic-owner', approvedAt: date + 'T08:00:00.000Z', billPhoto: '/api/expenses/photo/synthetic-bill.jpg', payments: []
});
const payment = patch => ({ expenseIds: [expenseId], amount: 1650, account, date, paymentType: 'UPI',
  paymentProof: '/api/expenses/photo/synthetic-7883-proof.jpg', note: 'Synthetic approved business payment',
  requestId: 'synthetic-7883-payment-0001', ...patch });
const pay = (body = payment(), actor = {}) => invoke('POST', '/api/expenses/vendor-payments/batch', { ...actor, body });
let baseline;
test.before(async () => { assert.equal((await invoke('GET', '/api/expenses/config', owner)).status, 200); baseline = read(); });
test.after(() => fs.rmSync(directory, { recursive: true, force: true }));
function seed(extra = {}) {
  const store = structuredClone(baseline);
  Object.assign(store, { expenses: { [expenseId]: bill() }, transfers: [], receipts: [], adjustments: [],
    vendors: { 'synthetic equipment vendor': { name: 'Synthetic Equipment Vendor' } }, vendorsByNature: { SAMAST: {}, PERSONAL: {} },
    vendorDirectory: {}, vendorAdvances: [], vendorOpeningPayables: [], vendorPaymentRequests: {},
    bankTruthMovements: [], paytmSettlements: [], paytmPayoutPostings: [], reconciliationExpenses: [],
    expenseRefunds: [], salesRefunds: [], receivables: {}, loanRepayments: [], procurementAccounting: { paymentsByPo: {} },
    bankStatements: {}, bankReconciliationDrafts: {}, bankReconciliationApprovals: {}, bankReconciliationLinks: {}, bankDateOverrides: {},
    bankAmountAuthorityApplications: {}, bankAmountAuthorityProposals: {}, bankMovementAuthority: {},
    openingBalances: {}, openingBalancesByNature: { SAMAST: { [account]: 10000 }, PERSONAL: {} },
    physicalBankOpeningBalances: { [account]: 10000 }, openingBalanceDates: {}, cashReconciliations: [], auditLog: [], auditSeq: 0 }, extra);
  for (const [name, value] of Object.entries({ 'salary.json': { employees: {}, advances: {}, salaryPayments: [] },
    'salary-samast.json': { employees: {}, advances: {}, salaryPayments: [] }, 'incentives.json': {},
    'procurement.json': { pos: {} }, 'orders.json': { orders: {} }, 'sales.json': {},
    'credit-cards.json': { cards: {}, payments: [], statements: {} } })) fs.writeFileSync(path.join(directory, name), JSON.stringify(value));
  write(store); return store;
}
function assertNoInventedFunding(store) {
  assert.deepEqual(store.transfers, []);
  assert.deepEqual(store.adjustments, []);
  assert.deepEqual(store.bankTruthMovements, []);
  const e = store.expenses[expenseId];
  assert.equal(e.nature, 'SANKI');
  assert.equal(e.payments.length, 1);
  assert.equal(e.payments[0].account, account);
  assert.equal(e.payments[0].personalFunds, false);
  assert.ok(!e.personalPaidAmount);
  assert.ok(!e.reimbursementStatus);
  assert.ok(!(e.reimbursementPayments || []).length);
}
async function bankRows(nature = 'SAMAST') {
  const result = await invoke('GET', '/api/expenses/account-ledger', { ...owner, query: { nature, account, from: date, to: date } });
  assert.equal(result.status, 200, JSON.stringify(result.body));
  return result.body.entries.filter(row => row.id === expenseId + '/PAY-001');
}
function addDraft(amount) {
  const store = read();
  store.bankReconciliationDrafts[draftId] = { id: draftId, account, nature: 'SAMAST',
    transactions: [{ date, description: 'Synthetic Equipment Vendor actual payment', reference: 'SYNTHETIC-7883-BANK-REF', debit: amount, credit: 0, balance: 10000 - amount }],
    summary: { from: date, to: date, openingBalance: 10000, closingBalance: 10000 - amount, totalDebits: amount, totalCredits: 0, validated: true },
    resolutions: {}, transactionOwnership: {}, createdBy: owner.username, createdAt: date + 'T10:00:00.000Z',
    expiresAt: '2099-01-01T00:00:00.000Z', matchingPolicy: 'balanced_date_amount_v5' };
  write(store);
}

test('authorized Admin and Owner can choose shared 7883 for SANKI without granting all personal accounts', async () => {
  seed();
  for (const actor of [{}, owner]) {
    const result = await invoke('GET', '/api/expenses/config', actor);
    assert.equal(result.status, 200);
    assert.ok(!result.body.payingAccountsByNature.SANKI.includes(account), 'vendor payment access must not grant generic reimbursement or payroll payment access');
    for (const collection of ['vendorPaymentAccountsByNature']) {
      assert.ok(result.body[collection].SANKI.includes(account), collection + ' must offer the authorized shared funding source');
      for (const privateAccount of ['ICICI Bank 0992', 'ICICI Bank 0993', 'Gagan Personal Cash', 'Namita 5464', 'Namita Cash']) {
        assert.ok(!result.body[collection].SANKI.includes(privateAccount), privateAccount + ' is not granted by this change');
      }
    }
    assert.equal(result.body.reconciliationNatureByAccount[account], 'SAMAST', 'one physical reconciliation book remains canonical');
  }
});

test('an unassigned Admin or claimant cannot select or submit shared 7883 for a SANKI vendor payment', async () => {
  for (const actor of [{ username: 'synthetic-unassigned-admin' }, { username: 'synthetic-claimant', role: 'claimant' }]) {
    seed();
    const config = await invoke('GET', '/api/expenses/config', actor);
    assert.equal(config.status, 200);
    assert.ok(!config.body.vendorPaymentAccountsByNature.SANKI.includes(account));
    const before = fs.readFileSync(file, 'utf8');
    const result = await pay(payment(), actor);
    assert.ok([400, 403].includes(result.status), JSON.stringify(result.body));
    assert.equal(fs.readFileSync(file, 'utf8'), before, 'denied payment must not mutate accounting records');
  }
});

test('SANKI 7883 payment keeps expense classification, creates no reimbursement and debits the physical bank once', async () => {
  seed();
  const result = await pay();
  assert.equal(result.status, 200, JSON.stringify(result.body));
  assert.deepEqual(result.body.allocations.map(row => [row.expenseId, row.amount, row.balanceDue]), [[expenseId, 1650, 0]]);
  const store = read(); assertNoInventedFunding(store);
  assert.equal(store.expenses[expenseId].paidAmount, 1650);
  assert.equal(store.expenses[expenseId].status, 'paid');
  const movements = appBankMovements(store, account, 'SAMAST').filter(row => row.id === expenseId + '/PAY-001');
  assert.equal(movements.length, 1); assert.equal(movements[0].debit, 1650); assert.equal(movements[0].entity, 'SANKI');
  const ledger = await bankRows(); assert.equal(ledger.length, 1); assert.equal(ledger[0].debit, 1650); assert.equal(ledger[0].entity, 'SANKI');
  const vendors = await invoke('GET', '/api/expenses/vendors', { ...owner, query: { nature: 'SANKI', search: 'Synthetic Equipment Vendor', from: date, to: date } });
  assert.equal(vendors.body.vendors[0].ledgerClosingBalance, 0);
  assert.equal(summaryForPL(date, date).Shared.variable, 1650, 'paying bank cannot move the expense to SAMAST P&L');
  const samast = await invoke('GET', '/api/expenses/spending-dashboard', { ...owner, query: { nature: 'SAMAST', from: date, to: date } });
  assert.ok(!samast.body.payments.some(row => row.id === expenseId));
});

test('7883 overpayment emits one gross bank debit and a vendor credit, not a second outflow or extra P&L cost', async () => {
  seed();
  const result = await pay(payment({ amount: 2000 }));
  assert.equal(result.status, 200, JSON.stringify(result.body));
  assert.equal(result.body.vendorAdvanceCreated, 350);
  const store = read(); assertNoInventedFunding(store);
  assert.equal(store.vendorAdvances.length, 1);
  assert.deepEqual([store.vendorAdvances[0].nature, store.vendorAdvances[0].amount, store.vendorAdvances[0].remainingAmount, store.vendorAdvances[0].grossPaymentAmount], ['SANKI', 350, 350, 2000]);
  assert.equal(store.expenses[expenseId].payments[0].amount, 1650);
  const movements = appBankMovements(store, account, 'SAMAST').filter(row => row.debit > 0);
  assert.equal(movements.length, 1, JSON.stringify(movements)); assert.equal(movements[0].debit, 2000);
  const ledger = await bankRows(); assert.equal(ledger.length, 1); assert.equal(ledger[0].debit, 2000);
  const vendors = await invoke('GET', '/api/expenses/vendors', { ...owner, query: { nature: 'SANKI', search: 'Synthetic Equipment Vendor', from: date, to: date } });
  assert.equal(vendors.body.vendors[0].ledgerClosingBalance, -350);
  assert.equal(summaryForPL(date, date).Shared.variable, 1650);
});

test('repeat payment request is idempotent and altered replay cannot add another 7883 debit', async () => {
  seed(); const body = payment({ amount: 2000 });
  assert.equal((await pay(body)).status, 200);
  const snapshot = fs.readFileSync(file, 'utf8');
  const repeated = await pay(body); assert.equal(repeated.status, 200); assert.equal(repeated.body.already, true);
  assert.equal(fs.readFileSync(file, 'utf8'), snapshot);
  assert.equal((await pay({ ...body, amount: 2001 })).status, 409);
  assert.equal(fs.readFileSync(file, 'utf8'), snapshot);
  assert.equal(appBankMovements(read(), account, 'SAMAST').filter(row => row.debit > 0).length, 1);
});

test('7883 payment still enforces proof and same-vendor, same-entity approved bill guards', async () => {
  for (const kind of ['proof', 'mixed-vendor', 'mixed-entity', 'pending']) {
    const store = seed(), body = payment();
    if (kind === 'proof') body.paymentProof = '';
    if (kind === 'mixed-vendor' || kind === 'mixed-entity') {
      const second = bill('EX-SYNTHETIC-OTHER-7883', 100, kind === 'mixed-entity' ? 'SAMAST' : 'SANKI', kind === 'mixed-vendor' ? 'Different Synthetic Vendor' : 'Synthetic Equipment Vendor');
      store.expenses[second.id] = second; body.expenseIds.push(second.id);
    }
    if (kind === 'pending') { store.expenses[expenseId].status = 'pending'; delete store.expenses[expenseId].approvedAt; delete store.expenses[expenseId].approvedBy; }
    write(store); const before = fs.readFileSync(file, 'utf8');
    const result = await pay(body); assert.equal(result.status, 400, kind + ': ' + JSON.stringify(result.body));
    assert.equal(fs.readFileSync(file, 'utf8'), before, kind + ' guard cannot partially save a payment');
  }
});

test('shared 7883 reconciliation derives the SANKI source and finalizes without reclassification or duplicate posting', async () => {
  seed(); assert.equal((await pay()).status, 200); addDraft(1650);
  const before = read(), originalExpense = structuredClone(before.expenses[expenseId]);
  const preview = await invoke('POST', '/api/expenses/bank-statements/reconcile', { ...owner, body: { draftId, account, nature: 'SAMAST' } });
  assert.equal(preview.status, 200, JSON.stringify(preview.body));
  const row = preview.body.rows.find(item => item.id === 'bank-0');
  assert.equal(row.app.id, expenseId + '/PAY-001'); assert.equal(row.status, 'matched');
  assert.equal(row.entityTag.nature, '', 'date/amount matching alone must not authorize entity classification');
  const linked = await invoke('POST', '/api/expenses/bank-statements/resolve', { ...owner,
    body: { draftId, rowId: 'bank-0', action: 'link_existing', appId: expenseId + '/PAY-001', reason: 'Verify exact fictional bank payment against its original bill' } });
  assert.equal(linked.status, 200, JSON.stringify(linked.body));
  const linkedRow = linked.body.rows.find(item => item.id === 'bank-0');
  assert.equal(linkedRow.entityTag.nature, 'SANKI'); assert.equal(linkedRow.entityTag.source, 'linked_ledger');
  assert.equal(linkedRow.entityTag.sourceConflict, false); assert.equal(linked.body.balanceDifference, 0);
  for (const nature of ['SAMAST', 'PERSONAL']) {
    const snapshot = fs.readFileSync(file, 'utf8');
    const retag = await invoke('POST', '/api/expenses/bank-statements/retag', { ...owner,
      body: { draftId, rowId: 'bank-0', nature, reason: 'A bank funding source must not reclassify the original SANKI bill' } });
    assert.equal(retag.status, 409, JSON.stringify(retag.body));
    assert.equal(fs.readFileSync(file, 'utf8'), snapshot);
    const bulk = await invoke('POST', '/api/expenses/bank-statements/retag-bulk', { ...owner,
      body: { account, nature: 'SAMAST', targetNature: nature, reason: 'Do not move the source bill between entity reporting books', items: [{ draftId, rowId: 'bank-0' }] } });
    assert.equal(bulk.status, 409, JSON.stringify(bulk.body));
    assert.equal(fs.readFileSync(file, 'utf8'), snapshot);
  }
  const finalized = await invoke('POST', '/api/expenses/bank-statements/finalize', { ...owner, body: { draftId } });
  assert.equal(finalized.status, 200, JSON.stringify(finalized.body));
  const after = read(); assertNoInventedFunding(after);
  assert.deepEqual(after.expenses[expenseId], originalExpense, 'reconciliation cannot rewrite the expense');
  assert.deepEqual(Object.keys(after.bankStatements), [account], 'never create a second SANKI or PERSONAL physical bank book');
  const bank = Object.values(after.bankStatements[account].transactions); assert.equal(bank.length, 1);
  assert.equal(bank[0].debit, 1650); assert.equal(bank[0].entityTag.nature, 'SANKI');
  assert.equal(appBankMovements(after, account, 'SAMAST').filter(item => item.id === expenseId + '/PAY-001').length, 1);
  assert.equal(summaryForPL(date, date).Shared.variable, 1650);
  const book = structuredClone(after.bankStatements[account]);
  const reload = await invoke('GET', '/api/expenses/bank-statements', { ...owner, query: { nature: 'SAMAST', account } });
  assert.equal(reload.status, 200);
  assert.equal(reload.body.transactions.length, 1);
  assert.equal(reload.body.transactions[0].entityTag.nature, 'SANKI');
  assert.deepEqual(read().bankStatements[account], book, 'a normal reload must preserve finalized SANKI provenance');
  for (const targetNature of ['SAMAST', 'PERSONAL']) {
    const snapshot = fs.readFileSync(file, 'utf8');
    const bulk = await invoke('POST', '/api/expenses/bank-statements/retag-bulk', { ...owner,
      body: { account, nature: 'SAMAST', targetNature, reason: 'Finalized vendor evidence must retain its SANKI source entity', items: [{ recordId: book.imports[0].id, rowId: 'bank-0' }] } });
    assert.equal(bulk.status, 409, JSON.stringify(bulk.body));
    assert.equal(fs.readFileSync(file, 'utf8'), snapshot);
  }
});

test('one 7883 payment across two SANKI bills preserves both allocations and links the bank total once', async () => {
  const second = bill('EX-SYNTHETIC-SECOND-7883', 650), first = bill(expenseId, 1000);
  seed({ expenses: { [first.id]: first, [second.id]: second } });
  const result = await pay(payment({ expenseIds: [first.id, second.id] }));
  assert.equal(result.status, 200, JSON.stringify(result.body));
  assert.equal(result.body.allocations.length, 2);
  const store = read(), ids = [first.id + '/PAY-001', second.id + '/PAY-001'];
  const movements = appBankMovements(store, account, 'SAMAST').filter(row => ids.includes(row.id));
  assert.equal(movements.length, 2);
  assert.equal(movements.reduce((sum, row) => sum + row.debit, 0), 1650);
  assert.ok(movements.every(row => row.entity === 'SANKI'));
  assert.deepEqual(store.transfers, []); assert.deepEqual(store.adjustments, []); assert.deepEqual(store.vendorAdvances, []);
  addDraft(1650);
  const linked = await invoke('POST', '/api/expenses/bank-statements/resolve', { ...owner,
    body: { draftId, rowId: 'bank-0', action: 'link_multiple_existing', appIds: ids, reason: 'One verified bank payment settles both fictional vendor bills' } });
  assert.equal(linked.status, 200, JSON.stringify(linked.body));
  assert.equal(linked.body.rows.find(row => row.id === 'bank-0').entityTag.nature, 'SANKI');
  assert.equal(linked.body.balanceDifference, 0);
  const finalized = await invoke('POST', '/api/expenses/bank-statements/finalize', { ...owner, body: { draftId } });
  assert.equal(finalized.status, 200, JSON.stringify(finalized.body));
  const after = read(); assert.equal(Object.values(after.bankStatements[account].transactions).length, 1);
  assert.equal(after.expenses[first.id].payments.length, 1); assert.equal(after.expenses[second.id].payments.length, 1);
  assert.equal(summaryForPL(date, date).Shared.variable, 1650);
});

test('SANKI source recognition does not authorize manual SANKI tagging or an adjustment in shared 7883', async () => {
  seed(); assert.equal((await pay()).status, 200);
  const store = read(); store.expenses[expenseId].payments[0].bankReference = 'SYNTHETIC-7883-BANK-REF'; write(store); addDraft(1650);
  const preview = await invoke('POST', '/api/expenses/bank-statements/reconcile', { ...owner, body: { draftId, account, nature: 'SAMAST' } });
  assert.equal(preview.body.rows.find(row => row.id === 'bank-0').entityTag.nature, 'SANKI');
  const before = fs.readFileSync(file, 'utf8');
  const tagged = await invoke('POST', '/api/expenses/bank-statements/retag', { ...owner,
    body: { draftId, rowId: 'bank-0', nature: 'SANKI', reason: 'Do not grant an unrestricted manual third entity' } });
  assert.equal(tagged.status, 400, JSON.stringify(tagged.body));
  assert.equal(fs.readFileSync(file, 'utf8'), before);
  const adjusted = await invoke('POST', '/api/expenses/bank-statements/resolve', { ...owner,
    body: { draftId, rowId: 'bank-0', action: 'create_adjustment', reason: 'Cannot use vendor permission to invent a SANKI adjustment' } });
  assert.ok([400, 409].includes(adjusted.status), JSON.stringify(adjusted.body));
  assert.deepEqual(read().adjustments, []); assert.deepEqual(read().transfers, []);
  assert.equal(read().expenses[expenseId].payments.length, 1);
});

test('a verified 7883 bank fee stays a separate SANKI charge and is not reposted on finalization retry or reload', async () => {
  seed(); assert.equal((await pay()).status, 200);
  const saved = read();
  saved.expenses[expenseId].payments[0].bankReference = 'SYNTHETIC-7883-BANK-REF';
  write(saved); addDraft(1655.90);
  const source = structuredClone(read().expenses[expenseId]);
  const linked = await invoke('POST', '/api/expenses/bank-statements/resolve', { ...owner,
    body: { draftId, rowId: 'bank-0', action: 'split_allocation', appId: expenseId + '/PAY-001', principalAmount: 1650,
      chargeAmount: 5.90, chargeCategory: 'BANK CHARGES', reason: 'Bank evidence confirms the original vendor payment plus a separate transfer charge' } });
  assert.equal(linked.status, 200, JSON.stringify(linked.body));
  assert.equal(linked.body.rows.find(row => row.id === 'bank-0').entityTag.nature, 'SANKI');
  const finalized = await invoke('POST', '/api/expenses/bank-statements/finalize', { ...owner, body: { draftId } });
  assert.equal(finalized.status, 200, JSON.stringify(finalized.body));
  const after = read();
  assert.deepEqual(after.expenses[expenseId], source);
  assert.equal(after.adjustments.length, 1); assert.equal(after.adjustments[0].nature, 'SANKI'); assert.equal(after.adjustments[0].amount, -5.90);
  assert.equal(after.reconciliationExpenses.length, 1); assert.equal(after.reconciliationExpenses[0].amount, 5.90);
  assert.deepEqual(after.transfers, []); assert.deepEqual(after.bankTruthMovements, []);
  const movements = appBankMovements(after, account, 'SAMAST').filter(row => row.debit > 0);
  assert.equal(movements.length, 2); assert.equal(Math.round(movements.reduce((sum, row) => sum + row.debit, 0) * 100), 165590);
  const ledger = await invoke('GET', '/api/expenses/account-ledger', { ...owner, query: { nature: 'SAMAST', account, from: date, to: date } });
  assert.equal(ledger.status, 200); assert.equal(Math.round(ledger.body.entries.reduce((sum, row) => sum + row.debit, 0) * 100), 165590);
  const report = summaryForPL(date, date); assert.equal(report.Shared.variable, 1650); assert.equal(report.Shared.running, 5.90);
  const retry = await invoke('POST', '/api/expenses/bank-statements/finalize', { ...owner, body: { draftId } });
  assert.ok([200, 404, 409].includes(retry.status), JSON.stringify(retry.body));
  const reload = await invoke('GET', '/api/expenses/bank-statements', { ...owner, query: { nature: 'SAMAST', account } });
  assert.equal(reload.status, 200);
  assert.deepEqual(read().adjustments, after.adjustments); assert.deepEqual(read().reconciliationExpenses, after.reconciliationExpenses);
  assert.equal(Object.values(read().bankStatements[account].transactions).length, 1);
});

test('SANKI vendor payments respect existing SAMAST and PERSONAL warnings on the same physical bank', async () => {
  const transfers = [
    { id: 'TR-SYNTHETIC-SAMAST-WARNING', nature: 'SAMAST', fromNature: 'SAMAST', toNature: 'SAMAST', fromAccount: account, toAccount: 'Kirti Nagar Cash', date, amount: 120, classification: 'internal_transfer' },
    { id: 'TR-SYNTHETIC-PERSONAL-WARNING', nature: 'PERSONAL', fromNature: 'PERSONAL', toNature: 'PERSONAL', fromAccount: account, toAccount: 'ICICI Bank 0993', date, amount: 130, classification: 'internal_transfer' }
  ];
  seed({ transfers });
  const result = await pay();
  assert.equal(result.status, 409, JSON.stringify(result.body));
  assert.equal(result.body.requiresOverride, true);
  assert.ok(result.body.issues.some(issue => JSON.stringify(issue).includes(transfers[0].id)));
  assert.ok(result.body.issues.some(issue => JSON.stringify(issue).includes(transfers[1].id)));
  assert.equal(read().expenses[expenseId].payments.length, 0); assert.equal(read().expenses[expenseId].paidAmount, 0);
  const reason = 'Owner verified urgent vendor payment against the original bank proof despite existing transfer warnings';
  const paid = await pay(payment({ reconciliationOverrideReason: reason }), owner);
  assert.equal(paid.status, 200, JSON.stringify(paid.body));
  const store = read(), recorded = store.expenses[expenseId].payments[0];
  assert.equal(store.expenses[expenseId].nature, 'SANKI'); assert.equal(recorded.amount, 1650); assert.equal(recorded.personalFunds, false);
  assert.equal(recorded.reconciliationOverrideReason, reason); assert.ok(recorded.reconciliationIssuesAtPayment.length >= 2);
  assert.deepEqual(store.transfers, transfers); assert.deepEqual(store.adjustments, []);
  assert.ok(store.auditLog.some(event => event.action === 'PAYMENT_ALLOCATED' && event.subjectId === expenseId && event.nature === 'SANKI'));
});

test('shared charge recognition does not ingest unrelated or malformed SANKI adjustment records', async () => {
  const marker = { version: 1, nature: 'SANKI', draftId, rowId: 'bank-0', appIds: [expenseId + '/PAY-001'] };
  const adjustments = [
    { id: 'ADJ-SYNTHETIC-UNRELATED', nature: 'SANKI', account, date, amount: -7 },
    { id: 'ADJ-SYNTHETIC-WRONG-DRAFT', nature: 'SANKI', account, date, amount: -8, splitBankCharge: true, reconciliationDraft: draftId, bankRowId: 'bank-0', sharedVendorPaymentSource: { ...marker, draftId: 'BRD-SYNTHETIC-OTHER' } },
    { id: 'ADJ-SYNTHETIC-WRONG-ROW', nature: 'SANKI', account, date, amount: -9, roundingDifference: true, reconciliationDraft: draftId, bankRowId: 'bank-0', sharedVendorPaymentSource: { ...marker, rowId: 'bank-1' } }
  ];
  seed({ adjustments });
  assert.equal(appBankMovements(read(), account, 'SAMAST').some(row => adjustments.some(item => item.id === row.id)), false);
  const ledger = await invoke('GET', '/api/expenses/account-ledger', { ...owner, query: { nature: 'SAMAST', account, from: date, to: date } });
  assert.equal(ledger.status, 200);
  assert.equal(ledger.body.entries.some(row => adjustments.some(item => item.id === row.id)), false);
  assert.deepEqual(read().adjustments, adjustments, 'ignored records are not silently rewritten or removed');
});

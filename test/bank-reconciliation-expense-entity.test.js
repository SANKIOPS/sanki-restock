'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'sanki-bank-expense-entity-'));
process.env.DATA_PATH = path.join(tempDir, 'data.json');
const file = path.join(tempDir, 'expenses.json');
fs.writeFileSync(file, '{}');
const { router, appBankMovements } = require('../modules/expenses');
const draftId = 'BRD-EXPENSE-ENTITY', bankDate = '2098-06-02', amount = 995.27;
const read = () => JSON.parse(fs.readFileSync(file, 'utf8'));
const write = value => fs.writeFileSync(file, JSON.stringify(value));
const clone = value => JSON.parse(JSON.stringify(value));

function invoke(method, route, { body = {}, query = {}, role = 'owner', username = '', permissions = {} } = {}) {
  return new Promise((resolve, reject) => {
    let status = 200, complete = false;
    const timer = setTimeout(() => reject(Error('Route did not complete: ' + route)), 10000);
    const finish = value => { if (complete) return; complete = true; clearTimeout(timer); resolve({ status, body: value }); };
    const req = { method, path: route, url: route, originalUrl: route, baseUrl: '', body, query, ip: '203.0.113.20',
      headers: {}, get: () => '', user: { role, roles: [role], username: username || (role === 'admin' ? 'prashant' : role === 'owner' ? 'owner-user' : 'expense-bookkeeper'), permissions } };
    const res = { statusCode: 200, headersSent: false, status(value) { status = value; this.statusCode = value; return this; },
      setHeader() {}, getHeader() {}, removeHeader() {}, json(value) { this.headersSent = true; finish(value); return this; },
      send(value) { this.headersSent = true; finish(value); return this; }, end: finish };
    try { router.handle(req, res, error => { if (error) { clearTimeout(timer); reject(error); } else finish(undefined); }); }
    catch (error) { clearTimeout(timer); reject(error); }
  });
}
const create = (body = {}, actor = {}) => invoke('POST', '/api/expenses/bank-statements/create-expense', {
  ...actor, body: { draftId, rowId: 'bank-0', ledger: 'FOOD EXPENSE', vendor: 'Entity test vendor', particulars: 'Verified expense entity',
    billPhoto: '/api/expenses/photo/entity-proof.jpg', remark: 'Verified expense belongs to the chosen entity', ...body }
});

let baseline;
test.before(async () => { const result = await invoke('GET', '/api/expenses/config'); assert.equal(result.status, 200); baseline = read(); });
test.after(() => fs.rmSync(tempDir, { recursive: true, force: true }));
function seed({ account = 'Axis Bank 3448', nature = 'SANKI', tag } = {}) {
  const store = clone(baseline);
  Object.assign(store, { expenses: {}, transfers: [], receipts: [], adjustments: [], vendorAdvances: [], vendorOpeningPayables: [],
    bankTruthMovements: [], paytmSettlements: [], paytmPayoutPostings: [], reconciliationExpenses: [], salesRefunds: [], expenseRefunds: [],
    receivables: {}, loanRepayments: [], bankStatements: {}, bankDateOverrides: {}, bankReconciliationLinks: {}, bankReconciliationApprovals: {},
    bankAmountAuthorityApplications: {}, bankAmountAuthorityProposals: {}, bankMovementAuthority: {},
    procurementAccounting: { paymentsByPo: {} }, openingBalances: { [account]: 10000 }, openingBalancesByNature: {}, physicalBankOpeningBalances: {},
    openingBalanceDates: { [account]: '2098-01-01' }, auditLog: [], auditSeq: 0 });
  store.bankReconciliationDrafts = { [draftId]: { id: draftId, account, nature,
    transactions: [{ date: bankDate, description: 'UPI new vendor verified expense', reference: 'ENTITY-EXPENSE-TEST', debit: amount, credit: 0, balance: 9004.73 }],
    summary: { from: bankDate, to: bankDate, openingBalance: 10000, closingBalance: 9004.73, totalDebits: amount, totalCredits: 0, validated: true },
    resolutions: {}, transactionOwnership: tag ? { 'bank-0': { nature: tag, reason: 'Previously reviewed bank ownership', source: 'manual', by: 'owner-user', at: bankDate } } : {},
    createdBy: 'owner-user', createdAt: bankDate + 'T12:00:00Z', expiresAt: '2099-01-01T00:00:00Z', matchingPolicy: 'balanced_date_amount_v5' } };
  for (const [name, contents] of Object.entries({ 'salary.json': { salaryPayments: [], advances: {}, employees: {} },
    'salary-samast.json': {}, 'incentives.json': {}, 'procurement.json': { pos: {} }, 'credit-cards.json': { cards: {}, payments: [], statements: {} } })) fs.writeFileSync(path.join(tempDir, name), JSON.stringify(contents));
  write(store); return store;
}
async function denied(body, expected, actor = {}) {
  const before = fs.readFileSync(file, 'utf8'), result = await create(body, actor);
  assert.equal(result.status, expected, JSON.stringify(result.body));
  assert.equal(fs.readFileSync(file, 'utf8'), before, 'denial changes no expense, sequence, ownership tag, audit, payment or bank evidence');
  return result;
}

test('explicit expense entity changes only expense classification; original bank account, date and exact paise remain authoritative', async () => {
  const before = seed();
  const made = await create({ nature: ' samast ', account: 'Counter Cash', date: '2000-01-01', amount: 1, paidAmount: 1 }, { role: 'admin' });
  assert.equal(made.status, 200, JSON.stringify(made.body));
  const e = made.body.expense; assert.equal(e.nature, 'SAMAST'); assert.equal(e.amount, amount); assert.equal(e.paidAmount, amount); assert.equal(e.date, bankDate);
  assert.equal(e.payments.length, 1); assert.equal(e.payments[0].amount, amount); assert.equal(e.payments[0].date, bankDate); assert.equal(e.payments[0].account, 'Axis Bank 3448');
  const after = read(); assert.deepEqual(after.bankReconciliationDrafts[draftId].transactions, before.bankReconciliationDrafts[draftId].transactions);
  assert.equal(after.bankReconciliationDrafts[draftId].resolutions['bank-0'].postingNature, 'SAMAST');
  assert.deepEqual(after.bankReconciliationDrafts[draftId].transactionOwnership, {}, 'nonshared bank ownership is not retagged to match an expense entity');
  assert.equal(appBankMovements(after, 'Axis Bank 3448', 'SANKI').filter(row => row.id === e.id + '/PAY-001').length, 1);
  assert.equal(Object.keys(after.bankStatements).length, 0, 'creating an expense never finalizes the bank statement');
  const submitted = await invoke('POST', '/api/expenses/bank-statements/finalize', { role: 'admin', body: { draftId } });
  assert.equal(submitted.status, 202, JSON.stringify(submitted.body)); assert.equal(submitted.body.approval.status, 'pending');
  assert.equal(Object.keys(read().bankStatements).length, 0, 'Owner finalization remains required');
});

test('unrecognized, empty and forged explicit entities never fall back to SANKI', async () => {
  for (const nature of ['', null, 'UNKNOWN', 'HOTEL', 'A3', {}, ['SAMAST'], ['SAMAST', 'PERSONAL'], true, 1]) { seed(); await denied({ nature }, 400); }
});

test('PERSONAL expense creation requires existing private-accounting permission, including normalized explicit values', async () => {
  for (const nature of ['PERSONAL', ' personal ', 'PeRsOnAl']) { seed(); await denied({ nature }, 403, { role: 'admin' }); }
  seed(); const made = await create({ nature: 'PERSONAL' });
  assert.equal(made.status, 200, JSON.stringify(made.body)); assert.equal(made.body.expense.nature, 'PERSONAL');
});

test('changing the requested entity cannot grant access to a private bank draft or claimant mutation', async () => {
  seed({ account: 'ICICI Bank 0992', nature: 'PERSONAL' }); await denied({ nature: 'SANKI' }, 403, { role: 'admin' });
  seed(); await denied({ nature: 'SANKI' }, 403, { role: 'claimant' });
});

test('legacy omitted entity retains the derived expense entity without changing ownership metadata', async () => {
  seed(); const made = await create(); assert.equal(made.status, 200, JSON.stringify(made.body)); assert.equal(made.body.expense.nature, 'SANKI');
  seed({ account: 'IndusInd Bank 7883', nature: 'SAMAST', tag: 'PERSONAL' });
  const oldTag = clone(read().bankReconciliationDrafts[draftId].transactionOwnership);
  const personal = await create(); assert.equal(personal.status, 200, JSON.stringify(personal.body)); assert.equal(personal.body.expense.nature, 'PERSONAL');
  assert.deepEqual(read().bankReconciliationDrafts[draftId].transactionOwnership, oldTag);
  seed({ account: 'IndusInd Bank 7883', nature: 'SAMAST' }); await denied({}, 409);
});

test('shared 7883 entity choice and new expense save atomically after every category/proof/remark validation', async () => {
  for (const invalid of [{ ledger: 'NOT AN APPROVED CATEGORY' }, { billPhoto: '' }, { remark: '' }, { vendor: '' }]) {
    seed({ account: 'IndusInd Bank 7883', nature: 'SAMAST', tag: 'SAMAST' }); await denied({ nature: 'PERSONAL', ...invalid }, 400);
  }
  const before = seed({ account: 'IndusInd Bank 7883', nature: 'SAMAST', tag: 'SAMAST' });
  const made = await create({ nature: 'PERSONAL' }); assert.equal(made.status, 200, JSON.stringify(made.body));
  const after = read(), draft = after.bankReconciliationDrafts[draftId], row = made.body.rows.find(row => row.id === 'bank-0');
  assert.equal(made.body.expense.nature, 'PERSONAL'); assert.equal(draft.transactionOwnership['bank-0'].nature, 'PERSONAL');
  assert.equal(draft.transactionOwnership['bank-0'].reason, 'Verified expense belongs to the chosen entity');
  assert.equal(draft.resolutions['bank-0'].postingNature, 'PERSONAL'); assert.equal(row.entityTag.nature, 'PERSONAL'); assert.equal(row.entityTag.sourceConflict, false);
  assert.deepEqual(draft.transactions, before.bankReconciliationDrafts[draftId].transactions);
  assert.ok(draft.decisionAudit.some(event => event.action === 'retag_transaction_ownership' && event.before.nature === 'SAMAST' && event.after.nature === 'PERSONAL'));
  assert.ok(after.auditLog.some(event => event.action === 'BANK_RECONCILIATION_TRANSACTION_RETAGGED'));
  assert.equal(Object.keys(after.expenses).length, 1); assert.equal(Object.keys(after.bankStatements).length, 0);
  const snapshot = fs.readFileSync(file, 'utf8'); const repeat = await create({ nature: 'PERSONAL' }); assert.notEqual(repeat.status, 200); assert.equal(fs.readFileSync(file, 'utf8'), snapshot);
});

test('shared 7883 cannot accept SANKI or let an unauthorized Admin override an existing PERSONAL transaction tag', async () => {
  seed({ account: 'IndusInd Bank 7883', nature: 'SAMAST' }); await denied({ nature: 'SANKI' }, 400);
  seed({ account: 'IndusInd Bank 7883', nature: 'SAMAST', tag: 'PERSONAL' }); await denied({ nature: 'SAMAST' }, 403, { role: 'admin' });
  seed({ account: 'IndusInd Bank 7883', nature: 'SAMAST', tag: 'PERSONAL' }); await denied({}, 403, { role: 'admin' });
  seed({ account: 'IndusInd Bank 7883', nature: 'SAMAST' }); await denied({ nature: 'PERSONAL' }, 403, { role: 'admin' });
  seed({ account: 'IndusInd Bank 7883', nature: 'SAMAST' }); await denied({ nature: 'PERSONAL' }, 403, { role: 'accounting', permissions: { personalAccounting: true } });
  seed({ account: 'IndusInd Bank 7883', nature: 'SAMAST', tag: 'SAMAST' }); await denied({ nature: 'PERSONAL' }, 403, { role: 'accounting', permissions: { personalAccounting: true } });
  seed({ account: 'IndusInd Bank 7883', nature: 'SAMAST' });
  const authorized = await create({ nature: 'PERSONAL' }, { role: 'admin', permissions: { personalAccounting: true } });
  assert.equal(authorized.status, 200, JSON.stringify(authorized.body)); assert.equal(authorized.body.expense.nature, 'PERSONAL');
});

test('authorized private bookkeeper retains an existing matching shared entity tag without a retag audit', async () => {
  seed({ account: 'IndusInd Bank 7883', nature: 'SAMAST', tag: 'PERSONAL' });
  const before = clone(read().bankReconciliationDrafts[draftId].transactionOwnership);
  const made = await create({ nature: 'PERSONAL' }, { role: 'accounting', permissions: { personalAccounting: true } });
  assert.equal(made.status, 200, JSON.stringify(made.body)); assert.equal(made.body.expense.nature, 'PERSONAL');
  const after = read(); assert.deepEqual(after.bankReconciliationDrafts[draftId].transactionOwnership, before);
  assert.ok(!after.auditLog.some(row => row.action === 'BANK_RECONCILIATION_TRANSACTION_RETAGGED'));
  assert.ok(!(after.bankReconciliationDrafts[draftId].decisionAudit || []).some(row => row.action === 'retag_transaction_ownership'));
});

test('category validation uses the chosen entity context without accepting arbitrary category names', async () => {
  seed(); const result = await denied({ nature: 'SAMAST', ledger: 'FORGED-CATEGORY' }, 400); assert.match(result.body.error, /SAMAST/);
  seed(); const valid = await create({ nature: 'SAMAST', ledger: 'FOOD EXPENSE' }); assert.equal(valid.status, 200, JSON.stringify(valid.body)); assert.equal(valid.body.expense.ledger, 'FOOD EXPENSE');
});

test('existing reversal, resolved-row and damaged-approval locks cannot be bypassed by a new entity choice', async () => {
  for (const protect of [
    store => { store.bankReconciliationDrafts[draftId].resolutions['bank-0'] = { action: 'exclude', reason: 'Previously resolved' }; },
    store => { store.bankReconciliationDrafts[draftId].originalStatementPeriod = { from: bankDate, to: bankDate }; },
    store => { store.bankStatements['Axis Bank 3448'] = { transactions: { final: { ...store.bankReconciliationDrafts[draftId].transactions[0], id: 'BTX-ALREADY-FINALIZED' } }, imports: [] }; },
    store => { store.bankReconciliationDrafts[draftId].transactions[0].description = 'UPI REVERSAL failed payment'; }
  ]) {
    const store = seed(); protect(store); write(store);
    const before = fs.readFileSync(file, 'utf8'), result = await create({ nature: 'SAMAST' });
    assert.ok([400, 409].includes(result.status), JSON.stringify(result.body)); assert.equal(fs.readFileSync(file, 'utf8'), before);
  }
});

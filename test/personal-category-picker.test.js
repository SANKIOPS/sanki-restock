'use strict';

const test = require('node:test'), assert = require('node:assert/strict');
const fs = require('node:fs'), os = require('node:os'), path = require('node:path'), vm = require('node:vm');
const { personalCategoryChoices } = require('../modules/personal-category-choices');
const types = ['fixed', 'running', 'variable', 'marketing'];
const base = [{ name: 'Electricity Expense', nature: 'SANKI', group: 'Utilities', type: 'fixed' }];
const clone = value => JSON.parse(JSON.stringify(value));
const historical = (extra = {}) => ({ id: 'EX-SYNTHETIC-CAR', nature: 'PERSONAL', ledger: 'Car EMI', type: 'variable', categoryGroup: 'Synthetic finance', status: 'paid', approvedBy: 'synthetic-owner', approvedAt: '2098-09-15T08:00:00Z', ...extra });
const choices = (store, options = {}) => personalCategoryChoices(store, { base, retiredNames: ['Food & Dining', 'Legacy category'], types, ...options });
const hasCar = list => list.some(category => category.name.toLowerCase() === 'car emi');

test('approved PERSONAL custom classifications remain reusable without mutating any record', () => {
  const store = { expenses: { one: historical({ payments: [{ id: 'PAY-SYNTHETIC', amount: 123, bankReconciliationDraft: 'BRD-SYNTHETIC-OLD' }] }) }, bankReconciliationDrafts: { old: { resolutions: { row: { action: 'exclude', reason: 'Keep prior review' } } } }, auditLog: [] };
  const before = JSON.stringify(store), beforeBase = JSON.stringify(base), result = choices(store);
  assert.equal(hasCar(result), true);
  assert.deepEqual(result.find(category => category.name === 'Car EMI'), { name: 'Car EMI', nature: 'PERSONAL', group: 'Synthetic finance', type: 'variable' });
  assert.equal(JSON.stringify(store), before);
  assert.equal(JSON.stringify(base), beforeBase);
  result.find(category => category.name === 'Electricity Expense').type = 'marketing';
  assert.equal(JSON.stringify(base), beforeBase, 'returned metadata must not alias the supplied master catalogue');
});

test('pending, rejected, unapproved and other-entity records cannot introduce a PERSONAL option', () => {
  for (const patch of [{ status: 'pending' }, { status: 'rejected' }, { status: 'deleted' }, { approvedBy: '' }, { approvedAt: '' }, { nature: 'SANKI' }, { nature: 'SAMAST' }, { nature: '' }]) {
    assert.equal(hasCar(choices({ expenses: { one: historical(patch) } })), false, JSON.stringify(patch));
  }
  for (const status of ['approved', 'partially_paid', 'paid']) assert.equal(hasCar(choices({ expenses: { one: historical({ status }) } })), true, status);
});

test('inactive historical defaults stay retired, while an explicit active catalogue entry wins', () => {
  const store = { expenses: { food: historical({ ledger: 'Food & Dining' }), legacy: historical({ ledger: ' legacy CATEGORY ' }), active: historical({ ledger: 'Electricity Expense' }) } };
  assert.deepEqual(choices(store).map(category => category.name), ['Electricity Expense']);
  const activeBase = [...base, { name: 'Food & Dining', nature: 'SANKI', group: 'Reviewed', type: 'variable' }];
  assert.equal(choices(store, { base: activeBase }).filter(category => category.name === 'Food & Dining').length, 1);
});

test('category removal is respected until a later explicit creation, ignoring spelling case', () => {
  const store = { expenses: { one: historical() }, auditLog: [{ action: 'CATEGORY_CREATED', subjectType: 'category', subjectId: 'Car EMI' }, { action: 'CATEGORY_REMOVED', subjectType: 'category', subjectId: ' car emi ', before: { name: 'Car EMI' } }] };
  assert.equal(hasCar(choices(store)), false);
  store.auditLog.push({ action: 'EDITED', subjectType: 'expense', subjectId: 'EX-SYNTHETIC-CAR', after: { ledger: 'Car EMI' } });
  assert.equal(hasCar(choices(store)), false, 'editing an old expense never silently recreates its retired category');
  store.auditLog.push({ action: 'CATEGORY_CREATED', subjectType: 'category', subjectId: 'CAR EMI', after: { name: 'CAR EMI' } });
  assert.equal(hasCar(choices(store)), true);
  store.auditLog.push({ action: 'CATEGORY_REMOVED', before: { name: 'Car EMI' } });
  assert.equal(hasCar(choices(store)), false);
});

test('metadata follows case-insensitive master overrides, then latest approved history, without duplicates', () => {
  const store = { expenses: { old: historical({ id: 'EX-OLD', approvedAt: '2098-01-01', categoryGroup: 'Old group' }), recent: historical({ id: 'EX-NEW', ledger: 'CAR EMI', approvedAt: '2098-02-01', categoryGroup: 'Latest group', type: 'fixed' }), pending: historical({ id: 'EX-PENDING', ledger: 'car emi', status: 'pending', approvedAt: '2098-03-01', type: 'marketing' }) } };
  let result = choices(store).filter(category => category.name.toLowerCase() === 'car emi');
  assert.equal(result.length, 1); assert.equal(result[0].group, 'Latest group'); assert.equal(result[0].type, 'fixed');
  store.customLedgers = { 'car emi': { name: 'Car EMI', group: 'Approved master group', type: 'running' } };
  store.ledgerOverrides = { 'CaR EmI': { type: 'variable' } };
  result = choices(store).filter(category => category.name.toLowerCase() === 'car emi');
  assert.equal(result.length, 1); assert.equal(result[0].group, 'Approved master group'); assert.equal(result[0].type, 'variable');
  const withBase = [...base, { name: 'Car EMI', nature: 'SANKI', group: 'Active catalogue', type: 'marketing' }];
  result = choices(store, { base: withBase }).filter(category => category.name.toLowerCase() === 'car emi');
  assert.equal(result.length, 1); assert.equal(result[0].group, 'Active catalogue'); assert.equal(result[0].type, 'marketing');
});

test('disabled custom definitions and invalid names cannot be restored through historical use', () => {
  for (const patch of [{ active: false }, { deleted: true }, { disabled: true }]) assert.equal(hasCar(choices({ expenses: { one: historical() }, customLedgers: { 'Car EMI': patch } })), false);
  for (const ledger of ['', '   ', 'x'.repeat(121)]) assert.equal(choices({ expenses: { one: historical({ ledger }) } }).length, base.length);
  const result = choices({ expenses: { one: historical({ type: 'invalid-type' }) } });
  assert.equal(result.find(category => category.name === 'Car EMI').type, 'variable');
});

test('bank expense entity switching uses the scoped category choices and clears stale selections', () => {
  const html = fs.readFileSync(path.join(__dirname, '../public/expenses.html'), 'utf8');
  const choiceSource = html.slice(html.indexOf('    function categoryChoices('), html.indexOf('    function findLedger('));
  const syncSource = html.slice(html.indexOf('    function syncBankExpenseNature('), html.indexOf('    window.openBankExpense='));
  const nodes = { beNature: { value: 'PERSONAL' }, beLedger: { value: 'Old selection', disabled: false }, beLedgerList: { innerHTML: '' }, beMsg: { textContent: '' } };
  const personal = choices({ expenses: { one: historical() } });
  const context = { cfg: { ledgers: base, ledgersByNature: { SANKI: base, SAMAST: base, PERSONAL: personal } }, el: id => nodes[id], bankExpenseNatures: () => ['PERSONAL', 'SAMAST'], bankExpenseNature: '', esc: value => String(value || '').replaceAll('&', '&amp;').replaceAll('"', '&quot;').replaceAll('<', '&lt;') };
  vm.createContext(context); vm.runInContext(choiceSource + syncSource, context);
  context.syncBankExpenseNature(); assert.match(nodes.beLedgerList.innerHTML, /value="Car EMI"/); assert.equal(nodes.beLedger.value, ''); assert.equal(nodes.beLedger.disabled, false);
  nodes.beNature.value = 'SAMAST'; nodes.beLedger.value = 'Car EMI'; context.syncBankExpenseNature(); assert.doesNotMatch(nodes.beLedgerList.innerHTML, /Car EMI/); assert.equal(nodes.beLedger.value, '');
  nodes.beNature.value = ''; context.syncBankExpenseNature(); assert.equal(nodes.beLedgerList.innerHTML, ''); assert.equal(nodes.beLedger.disabled, true);
});

const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'sanki-personal-category-picker-'));
process.env.DATA_PATH = path.join(temp, 'data.json');
for (const [key, filename] of Object.entries({ ORDERS_PATH: 'orders.json', SALES_PATH: 'sales.json', PROCUREMENT_PATH: 'procurement.json', INCENTIVES_PATH: 'incentives.json', VELOCITY_REMITTANCE_PATH: 'velocity.json' })) process.env[key] = path.join(temp, filename);
const file = path.join(temp, 'expenses.json'); fs.writeFileSync(file, '{}');
const rentalSource = require.resolve('../modules/rental-source.json'); require.cache[rentalSource] = { id: rentalSource, filename: rentalSource, loaded: true, exports: { id: 'SYNTHETIC-CATEGORY-PICKER', rows: [] } };
const { router } = require('../modules/expenses');
const plan = require('../modules/sanki-category-plan.json'), finalization = require('../modules/sanki-category-finalization');
const read = () => JSON.parse(fs.readFileSync(file, 'utf8')), write = store => fs.writeFileSync(file, JSON.stringify(store));
const account = 'IndusInd Bank 7883', draftId = 'BRD-SYNTHETIC-CATEGORY', date = '2098-10-12'; let baseline;
function invoke(route, { method = 'GET', body = {}, role = 'owner', permissions = {} } = {}) {
  return new Promise((resolve, reject) => {
    let status = 200, done = false; const timer = setTimeout(() => reject(Error('Synthetic category route timed out')), 10000);
    const finish = body => { if (!done) { done = true; clearTimeout(timer); resolve({ status, body }); } };
    const req = { method, path: route, url: route, originalUrl: route, baseUrl: '', body, query: {}, headers: {}, get: () => '', user: { role, roles: [role], username: 'synthetic-' + role, permissions } };
    const res = { statusCode: 200, headersSent: false, status(value) { status = value; this.statusCode = value; return this; }, setHeader() {}, getHeader() {}, removeHeader() {}, json(value) { this.headersSent = true; finish(value); return this; }, send(value) { finish(value); return this; }, end: finish };
    try { router.handle(req, res, error => { if (error) { clearTimeout(timer); reject(error); } else finish(undefined); }); } catch (error) { clearTimeout(timer); reject(error); }
  });
}
test.before(async () => { assert.equal((await invoke('/api/expenses/config')).status, 200); baseline = read(); });
test.after(() => fs.rmSync(temp, { recursive: true, force: true }));
function seed() {
  const store = clone(baseline);
  Object.assign(store, { expenses: {}, transfers: [], receipts: [], adjustments: [], vendorAdvances: [], vendorOpeningPayables: [], expenseRefunds: [], salesRefunds: [], receivables: {}, loanRepayments: [], reconciliationExpenses: [], bankTruthMovements: [], paytmSettlements: [], paytmPayoutPostings: [], procurementAccounting: { paymentsByPo: {} }, creditCardSettlementAllocations: [], bankStatements: {}, bankReconciliationLinks: {}, bankDateOverrides: {}, bankReconciliationApprovals: {}, bankMovementAuthority: {}, bankAmountAuthorityApplications: {}, bankAmountAuthorityProposals: {}, openingBalances: {}, openingBalancesByNature: { SAMAST: { [account]: 10000 }, PERSONAL: { [account]: 0 } }, physicalBankOpeningBalances: { [account]: 10000 }, customLedgers: {}, ledgerOverrides: {}, auditLog: [], auditSeq: 0 });
  store.sankiCategoryCatalog = { version: plan.version, categories: [...plan.catalog, ...finalization.additions] };
  store.oneTimeMigrations[finalization.KEY] = { appliedAt: 'synthetic-fixture' };
  store.expenses['EX-SYNTHETIC-CAR'] = historical({ date: '2098-09-12', vendor: 'Synthetic lender', particulars: 'Synthetic historical classification only', amount: 123, requestedAmount: 123, paidAmount: 123, provisionalPaidAmount: 0, account, paymentType: 'UPI', bill: 'printed', billPhoto: '/synthetic-old-proof.jpg', billPhotos: ['/synthetic-old-proof.jpg'], paymentProof: '', paymentProofs: [], purchasePaymentProof: '', purchasePaymentProofs: [], claimant: 'synthetic-owner', createdBy: 'synthetic-owner', createdAt: '2098-09-12T08:00:00Z', channel: 'Shared', fundedBy: 'company', reimbursementPayments: [], payments: [{ id: 'PAY-SYNTHETIC', date: '2098-09-12', amount: 123, account, paymentType: 'UPI', proof: '/synthetic-old-proof.jpg', proofs: ['/synthetic-old-proof.jpg'], paidBy: 'synthetic-owner', paidAt: '2098-09-12T08:00:00Z' }] });
  const transactions = [{ date, description: 'Synthetic new debit for picker regression', reference: 'SYNTHETIC-CATEGORY-NEW', debit: 234, credit: 0, balance: 9766 }, { date, description: 'Synthetic already reviewed debit', reference: 'SYNTHETIC-CATEGORY-REVIEWED', debit: 5, credit: 0, balance: 9761 }];
  store.bankReconciliationDrafts = { [draftId]: { id: draftId, nature: 'SAMAST', account, transactions, summary: { from: date, to: date, openingBalance: 10000, closingBalance: 9761, totalDebits: 239, totalCredits: 0, validated: true }, resolutions: { 'bank-1': { action: 'exclude', reason: 'Previously reviewed synthetic decision', by: 'synthetic-owner', at: date } }, transactionOwnership: { 'bank-0': { nature: 'PERSONAL', reason: 'Previously verified synthetic entity', source: 'manual', by: 'synthetic-owner', at: date } }, createdBy: 'synthetic-owner', createdAt: date + 'T10:00:00Z', expiresAt: '2099-01-01T00:00:00Z', matchingPolicy: 'balanced_date_amount_v5' } };
  for (const [name, contents] of Object.entries({ 'salary.json': { salaryPayments: [], advances: {}, employees: {} }, 'salary-samast.json': {}, 'incentives.json': {}, 'procurement.json': { pos: {} }, 'credit-cards.json': { cards: {}, payments: [], statements: {} } })) fs.writeFileSync(path.join(temp, name), JSON.stringify(contents));
  write(store); return store;
}
const createExpense = (actor = {}, patch = {}) => invoke('/api/expenses/bank-statements/create-expense', { ...actor, method: 'POST', body: { draftId, rowId: 'bank-0', nature: 'PERSONAL', ledger: 'Car EMI', vendor: 'Synthetic lender', particulars: 'Synthetic new statement expense', bill: 'printed', billPhoto: '/synthetic-new-proof.jpg', paymentType: 'UPI', remark: 'Synthetic category test only', ...patch } });

test('configuration exposes recovered PERSONAL choices only to permitted personal accounting users', async () => {
  const before = seed();
  for (const actor of [{ role: 'owner' }, { role: 'admin', permissions: { personalAccounting: true } }, { role: 'accounting', permissions: { personalAccounting: true } }]) {
    const result = await invoke('/api/expenses/config', actor); assert.equal(result.status, 200); assert.equal(hasCar(result.body.ledgersByNature.PERSONAL), true);
    assert.equal(hasCar(result.body.ledgers), false); for (const nature of ['SANKI', 'SAMAST']) if (result.body.ledgersByNature[nature]) assert.equal(hasCar(result.body.ledgersByNature[nature]), false);
  }
  for (const role of ['claimant', 'personal_claimant', 'admin']) { const result = await invoke('/api/expenses/config', { role }); assert.equal(result.status, 200); assert.equal(hasCar(result.body.ledgers), false); assert.equal(hasCar(result.body.ledgersByNature.PERSONAL || []), false); }
  const after = read(); assert.deepEqual(after.expenses, before.expenses); assert.deepEqual(after.bankReconciliationDrafts, before.bankReconciliationDrafts);
});

test('permitted bank-expense creation reuses Car EMI while preserving the old expense and reviewed decisions', async () => {
  const before = seed(), result = await createExpense({ role: 'admin', permissions: { personalAccounting: true } });
  assert.equal(result.status, 200, JSON.stringify(result.body)); assert.equal(result.body.expense.ledger, 'Car EMI'); assert.equal(result.body.expense.nature, 'PERSONAL'); assert.equal(result.body.expense.amount, 234);
  const after = read(); assert.deepEqual(after.expenses['EX-SYNTHETIC-CAR'], before.expenses['EX-SYNTHETIC-CAR']);
  assert.deepEqual(after.bankReconciliationDrafts[draftId].resolutions['bank-1'], before.bankReconciliationDrafts[draftId].resolutions['bank-1']); assert.deepEqual(after.bankReconciliationDrafts[draftId].transactions, before.bankReconciliationDrafts[draftId].transactions);
  assert.deepEqual(after.bankReconciliationDrafts[draftId].transactionOwnership, before.bankReconciliationDrafts[draftId].transactionOwnership); assert.deepEqual(after.transfers, before.transfers); assert.deepEqual(after.receipts, before.receipts); assert.deepEqual(after.bankStatements, before.bankStatements);
  assert.ok(after.auditLog.some(entry => entry.action === 'CREATED_FROM_BANK_RECONCILIATION'));
  const raw = fs.readFileSync(file, 'utf8'), repeat = await createExpense({ role: 'admin', permissions: { personalAccounting: true } });
  assert.ok([400, 409].includes(repeat.status), JSON.stringify(repeat.body)); assert.equal(fs.readFileSync(file, 'utf8'), raw, 'a repeated submission must not duplicate the expense or reconciliation');
});

test('missing permission, unapproved categories and previously reviewed rows remain blocked without writes', async () => {
  for (const check of [{ actor: { role: 'admin' }, status: 403 }, { actor: { role: 'admin', permissions: { personalAccounting: true } }, patch: { ledger: 'Unapproved synthetic category' }, status: 400 }, { actor: { role: 'admin', permissions: { personalAccounting: true } }, patch: { nature: 'SAMAST' }, status: 400 }, { actor: { role: 'admin', permissions: { personalAccounting: true } }, patch: { rowId: 'bank-1' }, status: 400 }]) {
    seed(); const raw = fs.readFileSync(file, 'utf8'), result = await createExpense(check.actor, check.patch); assert.equal(result.status, check.status, JSON.stringify(result.body)); assert.equal(fs.readFileSync(file, 'utf8'), raw);
  }
  const store = seed(); store.auditLog.push({ action: 'CATEGORY_REMOVED', subjectType: 'category', subjectId: 'Car EMI' }); write(store);
  const raw = fs.readFileSync(file, 'utf8'), result = await createExpense({ role: 'admin', permissions: { personalAccounting: true } }); assert.equal(result.status, 400, JSON.stringify(result.body)); assert.equal(fs.readFileSync(file, 'utf8'), raw);
});

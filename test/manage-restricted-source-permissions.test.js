'use strict';
// Actual production routers and permissions; all financial data below is fictional.
const test = require('node:test'), assert = require('node:assert/strict'), fs = require('node:fs'), os = require('node:os'), path = require('node:path');
const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'restricted-source-permissions-'));
process.env.DATA_PATH = path.join(temp, 'data.json'); process.env.SESSION_SECRET = 'synthetic-restricted-source-secret';
for (const [key, name] of Object.entries({ PROCUREMENT_PATH: 'procurement.json', INCENTIVES_PATH: 'incentives.json', USERS_PATH: 'users.json' })) process.env[key] = path.join(temp, name);
const RealDate = Date;
global.Date = class extends RealDate { constructor(...args) { super(...(args.length ? args : ['2098-06-30T12:00:00Z'])); } static now() { return RealDate.parse('2098-06-30T12:00:00Z'); } };
const rental = require.resolve('../modules/rental-source.json'); require.cache[rental] = { id: rental, filename: rental, loaded: true, exports: { id: 'synthetic', rows: [] } };
const expenseFile = path.join(temp, 'expenses.json'), cardFile = path.join(temp, 'credit-cards.json');
fs.writeFileSync(expenseFile, JSON.stringify({ expenses: {}, receipts: [], receivables: {}, oneTimeMigrations: {} }));
const expenses = require('../modules/expenses'), cards = require('../modules/credit-cards'), refunds = require('../modules/expense-refunds');
const copy = value => JSON.parse(JSON.stringify(value)), write = (file, value) => fs.writeFileSync(file, JSON.stringify(value)), read = file => JSON.parse(fs.readFileSync(file, 'utf8'));
const baseline = read(expenseFile), day = '2098-06-10', bank = 'ICICI Bank 0993', cardId = 'CC-SYNTHETIC', cardAccount = 'Synthetic Card 9991';
const admin = { username: 'synthetic-admin', role: 'admin', roles: ['admin'], permissions: { personalAccounting: true } };
const noGrant = { ...admin, permissions: {} }, owner = { username: 'synthetic-owner', role: 'owner', roles: ['owner'], permissions: {} };
const reason = 'Correct fictional source after checking original evidence';
function seed() {
  const store = { ...copy(baseline), expenses: {}, receipts: [], transfers: [], vendorAdvances: [], expenseRefunds: [], reconciliationExpenses: [], creditCardSettlementAllocations: [], bankStatements: {}, bankReconciliationDrafts: {}, bankReconciliationApprovals: {}, bankDateOverrides: {}, bankReconciliationLinks: {}, bankMovementAuthority: {}, openingBalances: {}, openingBalanceDates: {}, openingBalanceDatesByNature: {}, cashReconciliations: [], auditLog: [{ id: 'SYNTHETIC-PRIOR', note: 'Preserve' }], auditSeq: 1, untouched: { note: 'Unrelated accounting data' } };
  const cardStore = { cards: { [cardId]: { id: cardId, name: 'Synthetic Card', last4: '9991', ownerOnly: true, active: true } }, payments: [], statements: {}, audit: [{ id: 'SYNTHETIC-CARD-PRIOR', note: 'Preserve' }], auditSeq: 1 };
  for (const name of ['salary.json', 'salary-samast.json', 'procurement.json', 'incentives.json']) write(path.join(temp, name), {});
  write(expenseFile, store); write(cardFile, cardStore); return { store, cardStore };
}
const bytes = () => [expenseFile, cardFile].map(file => fs.readFileSync(file, 'utf8'));
function api(method, route, { body = {}, query = {}, params = {}, user = admin } = {}) {
  const layer = expenses.router.stack.find(item => item.route?.path === route && item.route.methods[method.toLowerCase()]); assert.ok(layer, route);
  let status = 200, result;
  layer.route.stack[0].handle({ method, url: route, path: route, body, query, params, user: copy(user), headers: {}, get() { return ''; } }, { status(value) { status = value; return this; }, json(value) { result = value; return this; } });
  return { status, ...result };
}
function cardApi(action, body = {}, user = admin) {
  const route = '/api/expenses/credit-cards' + (action === 'cards' ? '' : '/' + cardId + '/payment-corrections' + (action === 'list' ? '' : '/transfer%3ATR-SYNTHETIC/' + action));
  return new Promise((resolve, reject) => {
    let status = 200; const headers = {}, req = { method: ['cards', 'list'].includes(action) ? 'GET' : 'POST', url: route, originalUrl: route, baseUrl: '', body, query: {}, user: copy(user), headers: {}, get() { return ''; } };
    const res = { headersSent: false, setHeader(k, v) { headers[k] = v; }, getHeader(k) { return headers[k]; }, removeHeader(k) { delete headers[k]; }, status(value) { status = value; return this; }, json(value) { this.headersSent = true; resolve({ status, ...value }); return this; }, end(value) { resolve({ status, value }); } };
    cards.router.handle(req, res, error => error ? reject(error) : resolve({ status: 404 }));
  });
}
function cardSeed() {
  const state = seed(); state.store.transfers.push({ id: 'TR-SYNTHETIC', date: day, amount: 100, nature: 'PERSONAL', fromNature: 'PERSONAL', toNature: 'PERSONAL', fromAccount: bank, toAccount: cardAccount, classification: 'credit_card_payment', note: 'Retain original note', proof: '/synthetic-proof.png' });
  write(expenseFile, state.store); return state;
}
const cardBody = { changes: { amount: '99.63' }, reason };
test.after(() => { global.Date = RealDate; fs.rmSync(temp, { recursive: true, force: true }); });

test('restricted card visibility and correction use the existing explicit PERSONAL management grant', async () => {
  const original = cardSeed(), before = bytes();
  for (const user of [admin, owner]) { const list = await cardApi('cards', {}, user); assert.equal(list.status, 200); assert.equal(list.cards.length, 1); assert.equal((await cardApi('preview', cardBody, user)).status, 200); }
  assert.equal((await cardApi('cards', {}, noGrant)).cards.length, 0);
  assert.equal((await cardApi('list', {}, noGrant)).status, 404);
  assert.deepEqual(bytes(), before, 'Listing and preview must be read-only');
  const p = await cardApi('preview', cardBody), applied = await cardApi('apply', { ...cardBody, fingerprint: p.fingerprint });
  assert.equal(applied.status, 200, applied.error); const after = read(expenseFile);
  assert.equal(after.transfers[0].amount, 99.63); assert.deepEqual(read(cardFile).cards, original.cardStore.cards); assert.deepEqual(read(cardFile).payments, []);
  for (const key of Object.keys(original.store).filter(key => !['transfers', 'auditLog', 'auditSeq'].includes(key))) assert.deepEqual(after[key], original.store[key], key);
  assert.equal(after.auditLog.at(-1).user, admin.username); assert.equal(after.auditLog.at(-1).before.transfer.amount, 100);
});

test('restricted card grant cannot be replaced by Admin role, username or page-only access', async () => {
  cardSeed(); const before = bytes();
  for (const user of [noGrant, { ...noGrant, username: 'prashant' }, { ...admin, role: 'accounting', roles: ['accounting'] }, { ...admin, role: 'claimant', roles: ['claimant'] }]) {
    const result = await cardApi('preview', cardBody, user); assert.ok([403, 404].includes(result.status), JSON.stringify(result));
  }
  assert.deepEqual(bytes(), before);
});

test('revoking PERSONAL access after restricted-card preview denies apply with bytes unchanged', async () => {
  cardSeed(); const p = await cardApi('preview', cardBody); assert.equal(p.status, 200, p.error); const before = bytes();
  assert.equal((await cardApi('apply', { ...cardBody, fingerprint: p.fingerprint }, noGrant)).status, 404); assert.deepEqual(bytes(), before);
});

for (const kind of ['approval', 'finalized', 'draft-link']) test('restricted-card ' + kind + ' lock still applies to granted Admin and Owner', async () => {
  const { store } = cardSeed();
  if (kind === 'approval') store.bankReconciliationApprovals.A = { status: 'pending', account: bank, from: '2098-06-01', to: '2098-06-30' };
  if (kind === 'finalized') store.bankStatements['PERSONAL|' + bank] = { reconciledThrough: day };
  if (kind === 'draft-link') store.bankReconciliationDrafts.D = { resolutions: { R: { appId: 'TR-SYNTHETIC' } } };
  write(expenseFile, store); const before = bytes();
  for (const user of [admin, owner]) { assert.equal((await cardApi('preview', cardBody, user)).status, 409); assert.equal((await cardApi('apply', { ...cardBody, fingerprint: 'forged' }, user)).status, 409); }
  assert.deepEqual(bytes(), before);
});

function expenseSeed(nature = 'PERSONAL', card = false) {
  const state = seed(), account = card ? cardAccount : bank;
  const expense = { id: 'EX-SYNTHETIC', nature, ownerOnly: true, date: '2098-06-01', vendor: 'Synthetic supplier', amount: 500, requestedAmount: 500, paidAmount: 250, status: 'partially_paid', approvedAt: day, account, payments: [{ id: 'PAY-SYNTHETIC', date: '2098-06-02', amount: 250, account, personalFunds: false, ...(card ? { creditCardId: cardId } : {}) }], reimbursementPayments: [], reimbursementAmount: 0, personalPaidAmount: 0, reimbursementStatus: 'not_applicable' };
  state.store.expenses[expense.id] = expense; write(expenseFile, state.store); return { ...state, expense, account };
}
test('owner-marked PERSONAL expense payment honors both grants but business private expense does not', () => {
  for (const nature of ['PERSONAL', 'SANKI']) {
    const { store } = expenseSeed(nature), query = { id: 'EX-SYNTHETIC/PAY-SYNTHETIC', kind: 'expense', nature, account: bank }, before = bytes();
    const p = api('GET', '/api/expenses/ledger-entry/edit-preview', { query });
    assert.equal(p.status, nature === 'PERSONAL' ? 200 : 403, p.error); assert.deepEqual(bytes(), before);
    if (nature === 'PERSONAL') {
      const body = { ...query, amount: '249.63', reason, fingerprint: p.fingerprint };
      assert.equal(api('PATCH', '/api/expenses/ledger-entry', { body, user: noGrant }).status, 403); assert.deepEqual(bytes(), before);
      const applied = api('PATCH', '/api/expenses/ledger-entry', { body }); assert.equal(applied.status, 200, applied.error);
      const after = read(expenseFile); assert.equal(after.expenses['EX-SYNTHETIC'].payments[0].amount, 249.63); assert.equal(after.expenses['EX-SYNTHETIC'].amount, 500); assert.deepEqual(after.bankStatements, store.bankStatements);
    }
  }
});

function refundSeed(nature = 'PERSONAL', card = false) {
  const state = expenseSeed(nature, card), context = { today: '2098-06-30', username: admin.username, expenses: [state.expense], checkRecord() {}, checkSource() {}, checkAccount: () => state.account };
  const record = refunds.post(state.store, refunds.prepare(state.store, { requestId: 'synthetic-restricted-refund', date: day, reason: 'Synthetic return', reasonType: 'return', status: 'received', sources: [{ key: 'expense:' + state.expense.id, amount: 50 }], components: [{ mode: card ? 'card' : 'bank', receiver: 'company', account: state.account, amount: 50, reference: 'SYNTHETIC-REF' }] }, context), context).refund;
  write(expenseFile, state.store); return { ...state, record };
}
function refundApi(method, suffix, id, body, user = admin) { return api(method, '/api/expenses/expense-refunds/:id/' + suffix, { params: { id }, body, user }); }
for (const card of [false, true]) test('PERSONAL refund ' + (card ? 'card' : 'bank') + ' correction and picker honor explicit grant, preserving original source', () => {
  refundSeed('PERSONAL', card);
  const picker = api('GET', '/api/expenses/expense-refunds', { query: { nature: 'PERSONAL' } }); assert.equal(picker.status, 200, picker.error); assert.ok(picker.sources.some(source => source.expenseId === 'EX-SYNTHETIC'));
  // The legacy list loader runs pre-existing migrations. Reseed to prove that
  // the dedicated correction snapshot itself remains read-only and unmigrated.
  const { store, record } = refundSeed('PERSONAL', card), before = bytes();
  const opening = refundApi('GET', 'correction-preview', record.id); assert.equal(opening.status, 200, opening.error); assert.equal(opening.editable, true, opening.blockedReason);
  const body = { amount: '49.63', reason, baseFingerprint: opening.baseFingerprint }, p = refundApi('POST', 'correction-preview', record.id, body); assert.equal(p.status, 200, p.error); assert.deepEqual(bytes(), before);
  const denied = refundApi('PATCH', 'correction', record.id, { ...body, fingerprint: p.fingerprint }, noGrant); assert.equal(denied.status, 403, denied.error); assert.deepEqual(bytes(), before);
  const applied = refundApi('PATCH', 'correction', record.id, { ...body, fingerprint: p.fingerprint }); assert.equal(applied.status, 200, applied.error);
  const after = read(expenseFile); assert.deepEqual(after.expenses, store.expenses); assert.equal(after.receipts[0].amount, 49.63); assert.equal(after.receipts[0].id, store.receipts[0].id); assert.equal(fs.readFileSync(cardFile, 'utf8'), before[1]);
});

test('PERSONAL grant does not expose business private refunds or defeat refund approval/finalized locks', () => {
  const hidden = refundSeed('SANKI'), beforeHidden = bytes();
  const denied = refundApi('GET', 'correction-preview', hidden.record.id); assert.equal(denied.status, 403, denied.error); assert.deepEqual(bytes(), beforeHidden);
  for (const kind of ['approval', 'finalized']) {
    const { store, record } = refundSeed();
    if (kind === 'approval') store.bankReconciliationApprovals.A = { status: 'pending', account: bank, from: '2098-06-01', to: '2098-06-30' };
    else store.bankStatements['PERSONAL|' + bank] = { reconciledThrough: day };
    write(expenseFile, store); const before = bytes();
    for (const user of [admin, owner]) { const locked = refundApi('GET', 'correction-preview', record.id, undefined, user); assert.equal(locked.editable, false, JSON.stringify(locked)); }
    assert.deepEqual(bytes(), before);
  }
});

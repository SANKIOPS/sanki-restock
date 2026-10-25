'use strict';
// Production policy + production router + real two-store transaction, on
// isolated fictional fixtures only. No live identifiers or snapshots.
const test = require('node:test'), assert = require('node:assert/strict'), fs = require('node:fs'), os = require('node:os'), path = require('node:path');
const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'card-correction-routes-'));
process.env.DATA_PATH = path.join(temp, 'data.json'); process.env.SESSION_SECRET = 'synthetic-card-correction-secret';
for (const [key, name] of Object.entries({ PROCUREMENT_PATH: 'procurement.json', INCENTIVES_PATH: 'incentives.json', USERS_PATH: 'users.json' })) process.env[key] = path.join(temp, name);
const rental = require.resolve('../modules/rental-source.json'); require.cache[rental] = { id: rental, filename: rental, loaded: true, exports: { id: 'synthetic', rows: [] } };
const expensesFile = path.join(temp, 'expenses.json'), cardsFile = path.join(temp, 'credit-cards.json');
fs.writeFileSync(expensesFile, JSON.stringify({ expenses: {}, receivables: {}, receipts: [], oneTimeMigrations: {} }));
const { router } = require('../modules/credit-cards');
const clone = value => JSON.parse(JSON.stringify(value)), read = file => JSON.parse(fs.readFileSync(file, 'utf8')), write = (file, value) => fs.writeFileSync(file, JSON.stringify(value));
const baseline = read(expensesFile), cardId = 'CC-SYNTHETIC', sourceId = 'payment:CCP-SYNTHETIC', day = '2098-04-17', bank = 'Axis Bank 3448', cardAccount = 'Synthetic Card 9991';
const admin = { username: 'synthetic-admin', role: 'admin', roles: ['admin'], permissions: {} }, owner = { username: 'synthetic-owner', role: 'owner', roles: ['owner'], permissions: {} };
const reason = 'Verified synthetic source: correct only the existing settlement amount';
function seed(mutate = () => {}) {
  const p = { id: 'CCP-SYNTHETIC', cardId, date: day, amount: 100, nature: 'SANKI', account: bank, active: true, reference: 'SYNTHETIC-REFERENCE', allocations: [{ nature: 'SANKI', amount: 100 }], proof: '/synthetic-proof.jpg' };
  const transfer = { id: 'TR-SYNTHETIC', date: day, amount: 100, nature: 'SANKI', fromNature: 'SANKI', toNature: 'SANKI', fromAccount: bank, toAccount: cardAccount, classification: 'credit_card_payment', creditCardId: cardId, creditCardPaymentId: p.id, reference: p.reference, proof: p.proof, note: 'Synthetic existing card payment' };
  const state = { expenses: { ...clone(baseline), expenses: {}, receipts: [], transfers: [transfer], creditCardSettlementAllocations: [{ id: 'CCA-SYNTHETIC', cardId, cardName: cardAccount, creditCardPaymentId: p.id, date: day, amount: 100, payerNature: 'SANKI', beneficiaryNature: 'SANKI', payingAccount: bank, status: 'self' }], bankStatements: {}, bankReconciliationDrafts: {}, bankReconciliationApprovals: {}, bankDateOverrides: {}, bankReconciliationLinks: {}, bankMovementAuthority: {}, openingBalanceDates: {}, openingBalanceDatesByNature: {}, cashReconciliations: [], auditLog: [{ id: 'AUD-OLD', note: 'Preserve this entire audit' }], auditSeq: 1, oneTimeMigrations: {}, untouched: { value: 'preserve' } }, cards: { cards: { [cardId]: { id: cardId, name: 'Synthetic Card', last4: '9991', active: true } }, payments: [p], statements: {}, auditSeq: 1, audit: [{ id: 'CCA-OLD', note: 'Preserve old audit' }] } };
  for (const name of ['salary.json', 'salary-samast.json', 'procurement.json', 'incentives.json']) write(path.join(temp, name), {});
  mutate(state); write(expensesFile, state.expenses); write(cardsFile, state.cards); return state;
}
function raw() { return [expensesFile, cardsFile].map(file => fs.readFileSync(file, 'utf8')); }
function dispatch(action, body = {}, user = admin, id = sourceId) {
  const route = '/api/expenses/credit-cards/' + encodeURIComponent(cardId) + '/payment-corrections' + (action === 'list' ? '' : '/' + encodeURIComponent(id) + '/' + action);
  return new Promise((resolve, reject) => {
    let status = 200; const headers = {}, req = { method: action === 'list' ? 'GET' : 'POST', url: route, originalUrl: route, baseUrl: '', query: {}, body, user: clone(user), headers: { 'user-agent': 'Synthetic correction regression', 'x-forwarded-for': '192.0.2.25' }, get(name) { return this.headers[name]; } };
    const res = { headersSent: false, setHeader(key, value) { headers[key] = value; }, getHeader(key) { return headers[key]; }, removeHeader(key) { delete headers[key]; }, status(value) { status = value; return this; }, json(value) { this.headersSent = true; resolve({ status, body: value }); return this; }, end(value) { resolve({ status, body: value }); } };
    router.handle(req, res, error => error ? reject(error) : resolve({ status: 404 }));
  });
}
const preview = (changes = { amount: '99.63' }, user = admin, id = sourceId) => dispatch('preview', { changes, reason }, user, id);
test.after(() => fs.rmSync(temp, { recursive: true, force: true }));
test('actual production router policy previews read-only then atomically corrects CCP/transfer/allocation', async () => {
  const before = seed(), original = raw(), list = await dispatch('list'); assert.equal(list.status, 200, JSON.stringify(list)); assert.equal(list.body.entries[0].editable, true, JSON.stringify(list));
  const p = await preview(); assert.equal(p.status, 200, JSON.stringify(p)); assert.deepEqual(raw(), original, 'Preview must not run old store migrations');
  const result = await dispatch('apply', { changes: { amount: '99.63' }, reason, fingerprint: p.body.fingerprint }); assert.equal(result.status, 200, JSON.stringify(result));
  const s = read(expensesFile), cards = read(cardsFile); assert.equal(s.transfers[0].amount, 99.63); assert.equal(cards.payments[0].amount, 99.63); assert.equal(cards.payments[0].allocations[0].amount, 99.63); assert.equal(s.creditCardSettlementAllocations[0].amount, 99.63);
  for (const key of Object.keys(before.expenses).filter(key => !['transfers', 'creditCardSettlementAllocations', 'auditLog', 'auditSeq'].includes(key))) assert.deepEqual(s[key], before.expenses[key], key);
  assert.deepEqual(cards.cards, before.cards.cards); assert.deepEqual(cards.statements, before.cards.statements); assert.deepEqual(cards.audit[0], before.cards.audit[0]); assert.deepEqual(s.auditLog[0], before.expenses.auditLog[0]);
  assert.equal(s.auditLog.at(-1).ip, '192.0.2.25'); assert.equal(s.auditLog.at(-1).userAgent, 'Synthetic correction regression'); assert.equal(s.auditLog.at(-1).note, reason); assert.equal(s.auditLog.at(-1).before.transfer.amount, 100); assert.equal(s.auditLog.at(-1).after.transfer.amount, 99.63); assert.equal(fs.existsSync(path.join(temp, '.bank-authority-transaction.json')), false);
});
test('actual legacy transfer-only preview remains blocked by pending Owner approval for Admin and Owner', async () => {
  seed(s => { s.cards.payments = []; s.expenses.creditCardSettlementAllocations = []; delete s.expenses.transfers[0].creditCardPaymentId; s.expenses.bankReconciliationApprovals['APP-SYNTHETIC'] = { status: 'pending', account: bank, from: '2098-04-01', to: '2098-04-30' }; });
  const original = raw();
  for (const user of [admin, owner]) { const p = await preview({ amount: '99.63' }, user, 'transfer:TR-SYNTHETIC'); assert.equal(p.status, 409, JSON.stringify(p)); assert.match(p.body.error, /Owner approval/); assert.deepEqual(raw(), original); }
  const list = await dispatch('list'); assert.equal(list.body.entries[0].editable, false); assert.match(list.body.entries[0].blockedReason, /Owner approval/);
});
test('actual legacy unlocked correction creates no synthetic card payment or other record', async () => {
  seed(s => { s.cards.payments = []; s.expenses.creditCardSettlementAllocations = []; delete s.expenses.transfers[0].creditCardPaymentId; });
  const p = await preview({ amount: '99.63' }, admin, 'transfer:TR-SYNTHETIC'); assert.equal(p.status, 200, JSON.stringify(p));
  const result = await dispatch('apply', { changes: { amount: '99.63' }, reason, fingerprint: p.body.fingerprint }, admin, 'transfer:TR-SYNTHETIC'); assert.equal(result.status, 200, JSON.stringify(result)); assert.equal(read(expensesFile).transfers[0].amount, 99.63); assert.deepEqual(read(cardsFile).payments, []); assert.deepEqual(read(expensesFile).creditCardSettlementAllocations, []);
});
test('actual approval and unrelated external file edits after preview invalidate the token', async () => {
  for (const mode of ['approval', 'external']) { seed(); const p = await preview(); if (mode === 'approval') { const s = read(expensesFile); s.bankReconciliationApprovals.P = { status: 'pending', account: bank }; write(expensesFile, s); } else write(path.join(temp, 'salary.json'), { syntheticRevision: 2 }); const original = raw(); const result = await dispatch('apply', { changes: { amount: '99.63' }, reason, fingerprint: p.body.fingerprint }); assert.equal(result.status, 409, JSON.stringify(result)); assert.deepEqual(raw(), original); }
});
test('actual access checks deny claimant/accounting and hide PERSONAL-only source even for special-visible Admin', async () => {
  seed(); for (const role of ['claimant', 'accounting']) assert.equal((await dispatch('list', {}, { username: 'synthetic-' + role, role, roles: [role], permissions: {} })).status, 403);
  seed(s => { s.expenses.transfers[0].fromAccount = 'ICICI Bank 0993'; s.cards.payments[0].account = 'ICICI Bank 0993'; s.expenses.creditCardSettlementAllocations[0].payingAccount = 'ICICI Bank 0993'; });
  const special = { ...admin, username: 'prashant' }, original = raw(); assert.deepEqual((await dispatch('list', {}, special)).body.entries, []); assert.equal((await preview(undefined, special)).status, 403); assert.deepEqual(raw(), original);
});
test('actual unreadable external evidence and pending atomic journal block preview without recovery or writes', async () => {
  for (const mode of ['external', 'journal']) { seed(); const original = raw(), extra = mode === 'external' ? path.join(temp, 'salary.json') : path.join(temp, '.bank-authority-transaction.json'); fs.writeFileSync(extra, 'not-json'); const result = await preview(); assert.equal(result.status, 409, JSON.stringify(result)); assert.deepEqual(raw(), original); assert.equal(fs.readFileSync(extra, 'utf8'), 'not-json'); fs.unlinkSync(extra); }
});

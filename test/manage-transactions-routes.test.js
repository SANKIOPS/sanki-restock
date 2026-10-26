'use strict';
const test = require('node:test'), assert = require('node:assert/strict'), fs = require('node:fs'), os = require('node:os'), path = require('node:path');
// All records, references, dates and amounts are fictional. These tests invoke
// the real router against isolated temporary files, never production data.
const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'manage-reference-route-'));
process.env.DATA_PATH = path.join(temp, 'data.json'); process.env.SESSION_SECRET = 'synthetic-reference-route-secret';
for (const [key, name] of Object.entries({ PROCUREMENT_PATH: 'procurement.json', INCENTIVES_PATH: 'incentives.json', USERS_PATH: 'users.json' })) process.env[key] = path.join(temp, name);
const rental = require.resolve('../modules/rental-source.json'); require.cache[rental] = { id: rental, filename: rental, loaded: true, exports: { id: 'synthetic', rows: [] } };
const file = path.join(temp, 'expenses.json'), cardFile = path.join(temp, 'credit-cards.json');
fs.writeFileSync(file, JSON.stringify({ expenses: {}, receivables: {}, receipts: [], oneTimeMigrations: {} }));
const { router } = require('../modules/expenses');
const clone = value => JSON.parse(JSON.stringify(value)), write = value => fs.writeFileSync(file, JSON.stringify(value));
const actor = { username: 'synthetic-manager', role: 'admin', roles: ['admin'], permissions: {} }, owner = { username: 'synthetic-owner', role: 'owner', roles: ['owner'], permissions: {} };
const day = '2098-07-17', bank = 'Axis Bank 3448', other = 'IndusInd Bank 8181', endpoint = '/api/expenses/transactions';
function dispatch(route = endpoint, query = {}, user = actor, method = 'GET') {
  return new Promise((resolve, reject) => {
    let status = 200;
    const req = { method, path: route, url: route, originalUrl: route, baseUrl: '', body: {}, query, params: {}, headers: {}, user: clone(user), get() { return ''; } }, headers = {};
    const res = { headersSent: false, statusCode: 200, status(n) { status = n; this.statusCode = n; return this; }, setHeader(k, v) { headers[k] = v; }, getHeader(k) { return headers[k]; }, removeHeader(k) { delete headers[k]; }, json(value) { this.headersSent = true; resolve({ status, body: value }); return this; }, end(value) { resolve({ status, body: value }); } };
    router.handle(req, res, error => error ? reject(error) : resolve({ status: 404 }));
  });
}
function bytes() {
  const output = {};
  function walk(directory, prefix = '') { for (const item of fs.readdirSync(directory, { withFileTypes: true })) { const key = prefix + item.name, absolute = path.join(directory, item.name); if (item.isDirectory()) walk(absolute, key + '/'); else output[key] = fs.readFileSync(absolute).toString('base64'); } }
  walk(temp); return output;
}
let baseline;
test.before(async () => { assert.equal((await dispatch('/api/expenses/config', {}, owner)).status, 200); baseline = JSON.parse(fs.readFileSync(file, 'utf8')); });
test.after(() => fs.rmSync(temp, { recursive: true, force: true }));
function seed(mutate = () => {}, cards = {}) {
  const expense = { id: 'EX-00121', nature: 'SANKI', amount: 345.67, requestedAmount: 345.67, date: day, vendor: 'Synthetic merchant', status: 'paid', paidAmount: 345.67, approvedBy: 'synthetic-owner', approvedAt: day, account: bank, payments: [{ id: 'PAY-001', amount: 345.67, date: day, account: bank, personalFunds: false, proof: '/synthetic-proof.jpg' }], reimbursementPayments: [], reimbursementAmount: 0, personalPaidAmount: 0, reimbursementStatus: 'not_applicable' };
  const s = { ...clone(baseline), expenses: { [expense.id]: expense }, transfers: [{ id: 'TR-00042', nature: 'SANKI', fromNature: 'SANKI', toNature: 'SANKI', fromAccount: bank, toAccount: other, classification: 'internal_transfer', amount: 222.63, date: day, note: 'Synthetic transfer' }], receipts: [{ id: 'REC-00042', nature: 'SANKI', account: bank, date: day, amount: 70.25, receiptType: 'other_income', category: 'Other Income', source: 'Synthetic payer' }], adjustments: [], reconciliationExpenses: [], loanRepayments: [], vendorAdvances: [], vendorOpeningPayables: [], expenseRefunds: [], refunds: [], creditCardSettlementAllocations: [], bankStatements: {}, bankDateOverrides: {}, bankReconciliationLinks: {}, bankReconciliationDrafts: {}, bankReconciliationApprovals: {}, bankMovementAuthority: {}, cashReconciliations: [], gokwikPostings: {}, openingBalances: { [bank]: 1000 }, openingBalanceDates: {}, openingBalanceDatesByNature: {}, auditSeq: 1, auditLog: [{ id: 'AUD-SYNTHETIC', action: 'PRIOR_SYNTHETIC', note: 'Retain exactly' }], oneTimeMigrations: {}, untouched: { nested: ['Original synthetic evidence'] } };
  for (const name of ['salary.json', 'salary-samast.json', 'credit-cards.json', 'procurement.json', 'incentives.json']) fs.writeFileSync(path.join(temp, name), '{}');
  mutate(s, cards); write(s); fs.writeFileSync(cardFile, JSON.stringify(cards)); return s;
}
async function lookup(reference, user = actor) { return dispatch(endpoint, { reference }, user); }

test('actual GET dispatch finds normalized refs and reaches existing exact-source previews without writes', async () => {
  seed(s => { s.categories = ['Legacy untouched category']; }); const before = bytes();
  for (const [reference, kind, id] of [['tr42', 'transfer', 'TR-00042'], ['REC42', 'receipt', 'REC-00042'], ['EX121/PAY1', 'expense', 'EX-00121/PAY-001']]) {
    const result = await lookup(reference); assert.equal(result.status, 200, JSON.stringify(result)); assert.equal(result.body.entries.length, 1);
    const row = result.body.entries[0]; assert.deepEqual(row.action, { type: 'ledger_edit', id, kind, nature: 'SANKI', account: bank });
    const { type, ...intent } = row.action, preview = await dispatch('/api/expenses/ledger-entry/edit-preview', intent); assert.equal(preview.status, 200, JSON.stringify(preview)); assert.match(preview.body.fingerprint, /^[a-f0-9]{64}$/);
  }
  const bill = await lookup('EX121'); assert.ok(bill.body.entries.some(row => row.action.type === 'expense' && row.action.id === 'EX-00121'));
  assert.deepEqual(bytes(), before, 'Search and previews must not migrate, audit, normalize, save or create files');
  const named = router.stack.findIndex(layer => layer.route?.path === endpoint && layer.route.methods.get), detail = router.stack.findIndex(layer => layer.route?.path === '/api/expenses/:id' && layer.route.methods.get); assert.ok(named >= 0 && named < detail);
  assert.deepEqual(Object.keys(router.stack[named].route.methods), ['get']);
});

test('real permissions deny non-managing roles before parsing stores and blank search never reads sources', async () => {
  seed(); fs.writeFileSync(file, 'synthetic invalid source'); const before = bytes();
  for (const role of ['accounting', 'samast_accounting', 'claimant', 'personal_claimant', 'inventory', 'stocksearch']) {
    const user = { username: 'synthetic-' + role, role, roles: [role], permissions: { personalAccounting: true } }, result = await lookup('TR42', user);
    assert.equal(result.status, 403, role); assert.match(result.body.error, /Manage transactions/); assert.doesNotMatch(JSON.stringify(result), /invalid source/);
  }
  assert.deepEqual((await lookup('')).body.entries, []); assert.equal((await lookup('TR42')).status, 409); assert.deepEqual(bytes(), before);
});

test('both transfer ends, physical PERSONAL accounts and restricted expense sources remain private', async () => {
  for (const mutate of [s => { s.transfers[0].toNature = 'PERSONAL'; s.transfers[0].toAccount = 'Namita 5464'; }, s => { s.transfers[0].toAccount = 'Namita 5464'; }, s => { s.transfers[0].toAccount = 'Services'; }]) {
    seed(mutate); const before = bytes(), result = await lookup('TR42'); assert.equal(result.status, 200); assert.deepEqual(result.body.entries, []); assert.deepEqual(bytes(), before);
    assert.equal((await lookup('TR42', owner)).body.entries.length, 1);
  }
  seed(s => { s.expenses['EX-00121'].nature = 'PERSONAL'; }); const before = bytes(); assert.deepEqual((await lookup('EX121')).body.entries, []);
  assert.ok((await lookup('EX121', { ...actor, permissions: { personalAccounting: true } })).body.entries.length > 0); assert.deepEqual(bytes(), before);
  for (const mutate of [e => { e.ownerOnly = true; }, e => { delete e.nature; }, e => { e.claimantFundingAccount = 'Namita 5464'; }]) { seed(s => mutate(s.expenses['EX-00121'])); assert.deepEqual((await lookup('EX121')).body.entries, []); }
});

test('actual pending approvals, finalized periods and review links show locks to Admin and Owner without changing evidence', async () => {
  for (const mutate of [s => { s.bankReconciliationApprovals.A = { status: 'pending', account: other, from: day, to: day }; }, s => { s.bankStatements[other] = { reconciledThrough: day }; }, s => { s.bankReconciliationDrafts.D = { resolutions: { 'bank-synthetic': { appIds: ['TR-00042'] } } }; }]) {
    seed(mutate); const before = bytes();
    for (const user of [actor, owner]) { const result = await lookup('TR42', user); assert.equal(result.status, 200); const row = result.body.entries[0]; assert.equal(row.editable, false); assert.equal(row.action.type, 'source_workflow'); assert.match(row.blockedReason, /approval|finalized|reconcil/i); }
    assert.deepEqual(bytes(), before);
  }
});

test('real refund authorization routes RF and owned REC aliases only to their authorized source graph', async () => {
  function refund(s) { s.expenseRefunds = [{ id: 'RF-00021', nature: 'SANKI', status: 'received', date: day, amount: 50.25, sources: [{ kind: 'expense', expenseId: 'EX-00121', reference: 'EX-00121', paymentAllocations: [{ paymentId: 'PAY-001', amount: 50.25, account: bank, personal: false }] }], components: [{ id: 'RFC-SYNTHETIC', mode: 'bank', receiver: 'company', account: bank, amount: 50.25, receiptId: 'REC-00021' }] }]; s.receipts.push({ id: 'REC-00021', nature: 'SANKI', date: day, account: bank, amount: 50.25, receiptType: 'expense_refund', expenseRefundId: 'RF-00021' }); }
  seed(refund); const before = bytes();
  for (const reference of ['RF21', 'REC21']) { const result = await lookup(reference); assert.equal(result.status, 200); assert.equal(result.body.entries.length, 1); assert.deepEqual(result.body.entries[0].action, { type: 'refund', id: 'RF-00021', nature: 'SANKI' }); }
  assert.deepEqual(bytes(), before);
  for (const change of [s => { delete s.expenses['EX-00121']; }, s => { s.expenses['EX-00121'].ownerOnly = true; }, s => { s.expenses['EX-00121'].nature = 'PERSONAL'; }, s => { s.expenseRefunds.push(clone(s.expenseRefunds[0])); }]) {
    seed(s => { refund(s); change(s); }); const unchanged = bytes(); assert.deepEqual((await lookup('REC21')).body.entries, []); assert.deepEqual((await lookup('RF21')).body.entries, []); assert.deepEqual(bytes(), unchanged);
  }
});

test('real card correction service exposes one complete graph, with its existing restriction and lock rules', async () => {
  function card(s, cards) {
    const id = 'CC-SYNTHETIC', name = 'Synthetic Card 9099'; Object.assign(cards, { cards: { [id]: { id, name: 'Synthetic Card', last4: '9099', active: true, ownerOnly: true } }, payments: [], statements: {}, audit: [], auditSeq: 0 });
    s.transfers.push({ id: 'TR-00041', nature: 'SANKI', fromNature: 'SANKI', toNature: 'SANKI', fromAccount: bank, toAccount: name, amount: 100.25, date: day, classification: 'credit_card_payment', creditCardId: id });
  }
  seed(card); const before = bytes(); assert.deepEqual((await lookup('TR41')).body.entries, []);
  const authorized = { ...actor, permissions: { personalAccounting: true } }, result = await lookup('TR41', authorized); assert.equal(result.status, 200); assert.equal(result.body.entries.length, 1); assert.deepEqual(result.body.entries[0].action, { type: 'credit_card', cardId: 'CC-SYNTHETIC', sourceId: 'transfer:TR-00041' }); assert.deepEqual(bytes(), before);
  seed((s, cards) => { card(s, cards); s.bankReconciliationLinks['TR-00041'] = { draftId: 'D-SYNTHETIC' }; }); const lockedBefore = bytes(), locked = (await lookup('TR41', authorized)).body.entries[0]; assert.equal(locked.editable, false); assert.equal(locked.action.type, 'source_workflow'); assert.equal(locked.amount, null); assert.deepEqual(bytes(), lockedBefore);
});

test('ordinary receipts and misclassified transfers enforce actual restricted-card access before disclosing fields', async () => {
  const cardName = 'Synthetic Card 9099', cardId = 'CC-SYNTHETIC', authorized = { ...actor, permissions: { personalAccounting: true } };
  for (const [reference, mutate] of [
    ['REC42', s => Object.assign(s.receipts[0], { account: cardName, creditCardRefund: true, receiptType: 'refund' })],
    ['REC42', s => { s.receipts[0].account = '  Synthetic   Card 9099  '; }],
    ['REC42', s => { s.receipts[0].creditCardId = cardId; }],
    ['TR42', s => { s.transfers[0].fromAccount = cardName; }],
    ['TR42', s => { s.transfers[0].toAccount = cardName; }]
  ]) {
    seed((s, cards) => { cards.cards = { [cardId]: { id: cardId, name: 'Synthetic Card', last4: '9099', ownerOnly: true, active: true } }; mutate(s); }); const before = bytes();
    const denied = await lookup(reference); assert.equal(denied.status, 200); assert.deepEqual(denied.body.entries, []); assert.doesNotMatch(JSON.stringify(denied.body), /Synthetic Card|70\.25|222\.63|Synthetic payer|Synthetic transfer/);
    const allowed = (await lookup(reference, authorized)).body.entries; assert.equal(allowed.length, 1); assert.equal(allowed[0].editable, false); assert.equal(allowed[0].action.type, 'source_workflow'); assert.match(allowed[0].blockedReason, /credit-card source/); assert.deepEqual(allowed[0].action.destination, { tab: 'creditcards', cardId }); assert.deepEqual(bytes(), before);
  }
  seed((s, cards) => { cards.cards = { [cardId]: { id: cardId, name: 'Synthetic Card', last4: '9099' }, other: { id: 'CC-OTHER', name: 'Synthetic Card', last4: '9099' } }; s.receipts[0].account = cardName; }); const before = bytes();
  assert.deepEqual((await lookup('REC42', owner)).body.entries, []); assert.deepEqual(bytes(), before);
});

test('real protected multi-card source has no guessed destination and inactive card sources never link to cash', async () => {
  function cardsFixture(s, cards) { cards.cards = { one: { id: 'CC-SYNTHETIC', name: 'Synthetic Card', last4: '9099' }, two: { id: 'CC-OTHER', name: 'Other synthetic card', last4: '8088' } }; }
  seed((s, cards) => { cardsFixture(s, cards); s.transfers[0].fromAccount = 'Synthetic Card 9099'; s.transfers[0].toAccount = 'Other synthetic card 8088'; });
  let before = bytes(), result = (await lookup('TR42')).body.entries[0]; assert.equal(result.editable, false); assert.equal(result.action.type, 'source_workflow'); assert.equal(result.action.destination, undefined); assert.deepEqual(bytes(), before);
  seed((s, cards) => { cardsFixture(s, cards); s.receipts[0].account = 'Synthetic Card 9099'; s.receipts[0].active = false; });
  before = bytes(); result = (await lookup('REC42')).body.entries[0]; assert.equal(result.editable, false); assert.deepEqual(result.action.destination, { tab: 'creditcards', cardId: 'CC-SYNTHETIC' }); assert.deepEqual(bytes(), before);
});

test('malformed inputs and incomplete atomic sources fail closed without recovery or writes', async () => {
  seed(); let before = bytes();
  for (const reference of ['x'.repeat(129), ['TR42'], 'TR42\n', '42']) assert.equal((await lookup(reference)).status, 400);
  assert.equal((await dispatch(endpoint, { reference: 'TR42', apply: 'yes' })).status, 400); assert.deepEqual(bytes(), before);
  fs.writeFileSync(cardFile, 'synthetic not json'); before = bytes(); const result = await lookup('TR42'); assert.equal(result.status, 409); assert.doesNotMatch(JSON.stringify(result), /synthetic not json|credit-cards\.json/); assert.deepEqual(bytes(), before);
  seed(); const journal = path.join(temp, '.bank-authority-transaction.json'); fs.writeFileSync(journal, '{"syntheticPending":true}'); before = bytes(); assert.equal((await lookup('TR42')).status, 409); assert.deepEqual(bytes(), before); fs.unlinkSync(journal);
});

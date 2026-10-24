'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'sanki-bank-authority-privacy-'));
process.env.DATA_PATH = path.join(tempDir, 'data.json');
const file = path.join(tempDir, 'expenses.json');
fs.writeFileSync(file, '{}');
const { router } = require('../modules/expenses');
const authority = require('../modules/bank-movement-authority');
const clone = value => JSON.parse(JSON.stringify(value));
const date = '2098-06-02', privateDescription = 'PRIVATE Vendor Secret-Claimant Confidential particulars';
const read = () => JSON.parse(fs.readFileSync(file, 'utf8'));
const write = value => fs.writeFileSync(file, JSON.stringify(value));
function invoke(route, query = {}, role = 'admin') {
  return new Promise((resolve, reject) => {
    let status = 200;
    const req = { method: 'GET', path: route, url: route, originalUrl: route, baseUrl: '', query, body: {}, headers: {}, get: () => '', user: { role, roles: [role], username: role === 'admin' ? 'prashant' : 'owner-user', permissions: {} } };
    const res = { headersSent: false, status(value) { status = value; return this; }, setHeader() {}, getHeader() {}, removeHeader() {}, json(body) { this.headersSent = true; resolve({ status, body }); return this; }, send(body) { resolve({ status, body }); return this; }, end(body) { resolve({ status, body }); return this; } };
    router.handle(req, res, error => error ? reject(error) : resolve({ status }));
  });
}
let baseline;
test.before(async () => { assert.equal((await invoke('/api/expenses/config', {}, 'owner')).status, 200); baseline = read(); });
test.after(() => fs.rmSync(tempDir, { recursive: true, force: true }));
function seed(account = 'IndusInd Bank 8181', nature = 'SANKI', sourceNature = 'PERSONAL', sourceIds = []) {
  const store = clone(baseline), id = sourceIds.length ? 'RB-PRIVATE-MIXED' : 'EX-PRIVATE/PAY-001';
  Object.assign(store, { expenses: {}, adjustments: [], receipts: [], transfers: [], vendorAdvances: [], vendorOpeningPayables: [], salesRefunds: [], expenseRefunds: [], receivables: {}, loanRepayments: [], bankTruthMovements: [], reconciliationExpenses: [], paytmSettlements: [], paytmPayoutPostings: [], bankReconciliationDrafts: {}, bankStatements: {}, bankDateOverrides: {}, bankReconciliationLinks: {}, auditLog: [], openingBalances: { [account]: 10000 }, openingBalancesByNature: { SAMAST: { [account]: 10000 } }, bankMovementAuthority: {} });
  store.bankMovementAuthority[authority.key(account, nature, id)] = { movementId: id, account, nature, sourceNature, original: { date: '2098-06-01', debit: 500, credit: 0 }, after: { date, debit: 499.63, credit: 0 }, sourceIds, description: privateDescription, reference: 'BANK-REF-12345678', entity: sourceNature, planId: 'PRIVACY-PLAN', effect: 'bank_cash_only' };
  for (const [name, contents] of Object.entries({ 'salary.json': { salaryPayments: [], advances: {}, employees: {} }, 'salary-samast.json': {}, 'incentives.json': {}, 'procurement.json': { pos: {} }, 'credit-cards.json': { cards: {}, statements: {}, payments: [] } })) fs.writeFileSync(path.join(tempDir, name), JSON.stringify(contents));
  write(store);
  return { store, id, account, nature };
}
async function view(f, role = 'admin') { const result = await invoke('/api/expenses/account-ledger', { account: f.account, nature: f.nature, to: date }, role); assert.equal(result.status, 200, JSON.stringify(result.body)); return result.body; }
function assertSafe(body, id) {
  assert.ok(!JSON.stringify(body).includes(privateDescription));
  assert.ok(!JSON.stringify(body).includes('Secret-Claimant'));
  const row = body.entries.find(entry => entry.id === id);
  assert.ok(row, JSON.stringify(body)); assert.equal(row.debit, 499.63); assert.equal(row.date, date);
  assert.equal(row.privacyRedacted, true); assert.equal(row.proof, ''); assert.deepEqual(row.proofs, []);
  assert.equal(row.editable, false); assert.equal(row.bankMovementAuthority.sourceChanged, true);
  assert.equal(body.balance, 9500.37);
}
test('a removed private expense retains bank cash without disclosing its saved vendor/claimant narration to Admin', async () => { const f = seed(); assertSafe(await view(f), f.id); });
test('Owner still sees retained source narration for the same orphaned bank fact', async () => { const f = seed(); const result = await view(f, 'owner'); assert.equal(result.entries.find(row => row.id === f.id).description, privateDescription); assert.equal(result.balance, 9500.37); });
test('a synthesized shared SAMAST/PERSONAL 7883 row does not disclose PERSONAL source details', async () => { const f = seed('IndusInd Bank 7883', 'SAMAST'); assertSafe(await view(f), f.id); });
test('mixed or missing batch provenance is conservatively redacted without losing the canonical cash movement', async () => { const f = seed('IndusInd Bank 8181', 'SANKI', 'SANKI', ['EX-DELETED/REIM-001', 'EX-OTHER/REIM-001']); assertSafe(await view(f), f.id); });
test('a still-present private source keeps existing redaction after its bank amount is projected', async () => {
  const f = seed();
  f.store.expenses['EX-PRIVATE'] = { id: 'EX-PRIVATE', nature: 'PERSONAL', vendor: privateDescription, particulars: privateDescription, claimant: 'Secret-Claimant', ledger: 'PERSONAL', approvedAt: '2098-06-01', date: '2098-06-01', amount: 500, requestedAmount: 500, paidAmount: 500, status: 'paid', payments: [{ id: 'PAY-001', amount: 500, account: f.account, date: '2098-06-01', proof: '/private-proof.jpg' }] };
  write(f.store);
  const body = await view(f); const row = body.entries.find(entry => entry.id === f.id);
  assert.ok(!JSON.stringify(body).includes(privateDescription)); assert.ok(!JSON.stringify(body).includes('/private-proof.jpg'));
  assert.equal(row.privacyRedacted, true); assert.equal(row.debit, 499.63); assert.equal(body.balance, 9500.37);
});

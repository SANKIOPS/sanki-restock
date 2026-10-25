'use strict';
// Synthetic records only: an open workspace is not an accounting claim.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const rentalSource = require.resolve('../modules/rental-source.json');
require.cache[rentalSource] = { id: rentalSource, filename: rentalSource, loaded: true, exports: { id: 'SYNTHETIC-OPEN-DRAFT-CLAIMS', asOf: '2098-01-01', rows: [] } };
const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'personal-transfer-open-claims-'));
process.env.DATA_PATH = path.join(directory, 'data.json');
const file = path.join(directory, 'expenses.json');
fs.writeFileSync(file, JSON.stringify({ expenses: {}, transfers: [], receipts: [], receivables: {}, oneTimeMigrations: {} }));
const { router } = require('../modules/expenses');
const clone = value => JSON.parse(JSON.stringify(value));
const read = () => JSON.parse(fs.readFileSync(file, 'utf8'));
const write = value => fs.writeFileSync(file, JSON.stringify(value));
const source = 'Namita 5464', destination = 'IndusInd Bank 7883', date = '2098-07-19', amount = 825, reference = '800000000101';
test.after(() => fs.rmSync(directory, { recursive: true, force: true }));

function invoke(route, body = {}, options = {}) {
  const method = options.method || 'POST';
  const layer = router.stack.find(item => item.route && item.route.path === route && item.route.methods[method.toLowerCase()]);
  assert.ok(layer, route); let status = 200, result;
  const req = { method, path: route, url: route, originalUrl: route, body, query: options.query || {}, params: {}, headers: {}, get() { return ''; }, user: { role: options.role || 'admin', roles: [options.role || 'admin'], username: 'prashant', permissions: options.permissions || { personalAccounting: true } } };
  layer.route.stack[0].handle(req, { status(code) { status = code; return this; }, json(value) { result = value; return this; } }, () => { throw Error('Unexpected middleware continuation'); });
  return { status, body: result };
}
assert.equal(invoke('/api/expenses/config', {}, { method: 'GET', role: 'owner' }).status, 200);
const baseline = read();
function makeDraft(id, incoming = false, bankReference = reference) {
  return { id, nature: incoming ? 'SAMAST' : 'PERSONAL', account: incoming ? destination : source, createdAt: '2098-07-19T08:00:00.000Z', createdBy: 'prashant', transactions: [{ date, reference: bankReference, description: 'UPI/' + bankReference + '/SYNTHETIC OWN TRANSFER', debit: incoming ? 0 : amount, credit: incoming ? amount : 0, balance: incoming ? 10825 : 9175 }], summary: { from: date, to: date, validated: true, openingBalance: 10000, closingBalance: incoming ? 10825 : 9175, totalDebits: incoming ? 0 : amount, totalCredits: incoming ? amount : 0 }, resolutions: {}, matchingPolicy: 'balanced_date_amount_v5', matchingExclusions: { 'bank-0': { reason: 'Synthetic explicit independent review', by: 'prashant' } }, ...(incoming ? { transactionOwnership: { 'bank-0': { nature: 'PERSONAL', source: 'manual', reason: 'Synthetic verified personal account ownership', by: 'prashant' } } } : {}) };
}
function decision(draft) { return { action: 'create_internal_transfer', otherNature: 'PERSONAL', otherAccount: draft.account === source ? destination : source, postingNature: 'PERSONAL', classification: 'internal_transfer', principalAmount: amount, chargeAmount: 0, reason: 'Synthetic explicit transfer decision with bank identity', by: 'prashant', at: '2098-07-19T08:01:00.000Z' }; }
function reset(other, current = makeDraft('BRD-SYNTHETIC-CURRENT')) {
  const store = clone(baseline);
  Object.assign(store, { expenses: {}, transfers: [], receipts: [], adjustments: [], reconciliationExpenses: [], bankReconciliationDrafts: { [current.id]: clone(current), [other.id]: clone(other) }, bankReconciliationApprovals: {}, bankStatements: {}, bankDateOverrides: {}, bankReconciliationLinks: {}, openingBalancesByNature: { PERSONAL: { [source]: 10000, [destination]: 10000 } }, physicalBankOpeningBalances: { [destination]: 10000 } });
  write(store); assert.equal(invoke('/api/expenses/config', {}, { method: 'GET', role: 'owner' }).status, 200);
  return { current: read().bankReconciliationDrafts[current.id], before: read() };
}
function stage(current) { return invoke('/api/expenses/bank-statements/resolve', { draftId: current.id, rowId: 'bank-0', ...decision(current) }); }

for (const incoming of [false, true]) test('an open ' + (incoming ? 'counterpart' : 'overlapping source') + ' reconciliation with the same bank evidence is not a pending transfer claim', () => {
  const other = makeDraft('BRD-SYNTHETIC-OPEN', incoming), fixture = reset(other), result = stage(fixture.current);
  assert.equal(result.status, 200, JSON.stringify(result.body));
  const after = read(); assert.equal(after.transfers.length, 0, 'review stages a decision only');
  assert.deepEqual(after.bankReconciliationDrafts[other.id], fixture.before.bankReconciliationDrafts[other.id], 'the open workspace is not changed or discarded');
  assert.equal(after.bankReconciliationDrafts[fixture.current.id].resolutions['bank-0'].action, 'create_internal_transfer');
});

for (const incoming of [false, true]) test('a saved same-identity ' + (incoming ? 'counterpart' : 'source') + ' transfer decision remains an active claim, even before approval', () => {
  const other = makeDraft('BRD-SYNTHETIC-CLAIMED', incoming); other.resolutions['bank-0'] = decision(other);
  const fixture = reset(other), result = stage(fixture.current);
  assert.equal(result.status, 409, JSON.stringify(result.body));
  assert.match(result.body.error, /already pending in reconciliation/);
  assert.match(result.body.error, /specific saved transfer decision, not merely an open account/);
  assert.match(result.body.error, new RegExp(other.id));
  assert.doesNotMatch(result.body.error, new RegExp(fixture.current.id), 'the message must not point back to the unstaged current candidate');
  assert.deepEqual(result.body.transferClaim, { type: 'pending_internal_transfer', draftId: other.id, rowId: 'bank-0', nature: other.nature, account: other.account, date, principalAmount: amount, bankReference: reference, bankReferenceVerified: true, reviewUrl: '/expenses.html?tab=cash&nature=' + encodeURIComponent(other.nature) + '&account=' + encodeURIComponent(other.account) + '&draft=' + encodeURIComponent(other.id) });
  assert.deepEqual(read(), fixture.before, 'claim rejection must not mutate either workspace or post a transfer');
});

test('an independently evidenced different same-source UTR is not blocked merely because its route, date and amount match', () => {
  const other = makeDraft('BRD-SYNTHETIC-DISTINCT', false, '800000000102'); other.resolutions['bank-0'] = decision(other);
  const fixture = reset(other), result = stage(fixture.current);
  assert.equal(result.status, 200, JSON.stringify(result.body));
  assert.deepEqual(read().bankReconciliationDrafts[other.id], fixture.before.bankReconciliationDrafts[other.id]);
  assert.equal(read().transfers.length, 0);
});

test('an unverifiable opposite-side pending decision is not silently ignored as an unclaimed workspace', () => {
  const other = makeDraft('BRD-SYNTHETIC-AMBIGUOUS', true, 'unverified'); other.transactions[0].description = 'Synthetic credit without reliable transaction identity'; other.resolutions['bank-0'] = decision(other);
  const fixture = reset(other), result = stage(fixture.current);
  assert.equal(result.status, 409, JSON.stringify(result.body)); assert.deepEqual(read(), fixture.before);
  assert.equal(result.body.transferClaim.bankReference, ''); assert.equal(result.body.transferClaim.bankReferenceVerified, false);
  assert.match(result.body.error, /bank reference not reliably verified/);
});

test('incoming creation exposes the same actionable saved-claim metadata without creating a second movement', () => {
  const other = makeDraft('BRD-SYNTHETIC-OUTGOING-CLAIM'); other.resolutions['bank-0'] = decision(other);
  const fixture = reset(other, makeDraft('BRD-SYNTHETIC-INCOMING', true));
  const result = invoke('/api/expenses/bank-statements/create-incoming', { draftId: fixture.current.id, rowId: 'bank-0', sourceKind: 'internal', fromNature: 'PERSONAL', fromAccount: source, classification: 'internal_transfer', note: 'Synthetic incoming transfer with independently reviewed bank row' });
  assert.equal(result.status, 409, JSON.stringify(result.body));
  assert.equal(result.body.transferClaim.draftId, other.id); assert.equal(result.body.transferClaim.account, source); assert.equal(result.body.transferClaim.rowId, 'bank-0');
  assert.deepEqual(read(), fixture.before, 'neither the source decision nor current incoming row can be rewritten');
});

test('saved claim metadata is not added to an unauthorized transfer request', () => {
  const other = makeDraft('BRD-SYNTHETIC-PRIVATE-CLAIM'); other.resolutions['bank-0'] = decision(other);
  const fixture = reset(other);
  const result = invoke('/api/expenses/bank-statements/resolve', { draftId: fixture.current.id, rowId: 'bank-0', ...decision(fixture.current) }, { role: 'accounting', permissions: {} });
  assert.equal(result.status, 403, JSON.stringify(result.body)); assert.equal(result.body.transferClaim, undefined);
  assert.deepEqual(read(), fixture.before);
});

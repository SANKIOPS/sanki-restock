'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

// All fixtures are fictional. Never let module initialization read rental data
// or a production store; DATA_PATH is an isolated file, not a shared directory.
const rentalSource = require.resolve('../modules/rental-source.json');
require.cache[rentalSource] = { id: rentalSource, filename: rentalSource, loaded: true, exports: { id: 'SYNTHETIC-RESTORE-ROUTE-TEST', asOf: '2098-01-01', file: 'Synthetic fixture', sheet: 'None', range: '', rows: [] } };
const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'excluded-transfer-restoration-routes-'));
process.env.DATA_PATH = path.join(temp, 'data.json');
process.env.INCENTIVES_PATH = path.join(temp, 'incentives.json');
const file = path.join(temp, 'expenses.json');
fs.writeFileSync(file, JSON.stringify({ expenses: {}, receipts: [], receivables: {}, oneTimeMigrations: {} }));
const { router } = require('../modules/expenses');
const clone = value => JSON.parse(JSON.stringify(value));
const read = () => JSON.parse(fs.readFileSync(file, 'utf8'));
const write = value => fs.writeFileSync(file, JSON.stringify(value));
const bytes = () => fs.readFileSync(file, 'utf8');
const source = 'IndusInd Bank 7883', receiving = 'ICICI Bank 0992';
const date = '2098-07-11', amount = 1825.75, reference = '900000006123';
const transferId = 'TR-SYNTHETIC-RESTORE-HTTP', draftId = 'BRD-SYNTHETIC-RESTORE-HTTP';
const previewRoute = '/api/expenses/bank-statements/restore-excluded-transfer/preview';
const applyRoute = '/api/expenses/bank-statements/restore-excluded-transfer/apply';
const intent = { draftId, rowId: 'bank-0', transferId };
const reason = 'Owner confirms this synthetic exclusion was accidental and requests restoration of the same transfer.';

function request(method, route, { role = 'owner', username = 'synthetic-owner', permissions = {}, body = {}, query = {} } = {}) {
  return { method, path: route, url: route, originalUrl: route, baseUrl: '', body, query, params: {}, headers: { 'user-agent': 'Synthetic excluded transfer restoration HTTP test' }, get(name) { return this.headers[String(name).toLowerCase()] || ''; }, user: { role, roles: [role], username, permissions } };
}
function invoke(route, options = {}) {
  const method = options.method || 'GET';
  const layer = router.stack.find(item => item.route?.path === route && item.route.methods[method.toLowerCase()]);
  assert.ok(layer, route); let status = 200, body;
  layer.route.stack[0].handle(request(method, route, options), { status(code) { status = code; return this; }, json(value) { body = value; return this; } }, () => { throw Error('Unexpected next: ' + route); });
  return { status, body };
}
function dispatch(route, options = {}) {
  const method = options.method || 'POST';
  return new Promise((resolve, reject) => {
    let status = 200, finished = false; const headers = {};
    const finish = body => { if (!finished) { finished = true; resolve({ status, body }); } };
    const res = { statusCode: 200, headersSent: false, status(code) { this.statusCode = status = code; return this; }, setHeader(name, value) { headers[String(name).toLowerCase()] = value; }, getHeader(name) { return headers[String(name).toLowerCase()]; }, removeHeader(name) { delete headers[String(name).toLowerCase()]; }, json(body) { this.headersSent = true; finish(body); return this; }, send(body) { this.headersSent = true; finish(body); return this; }, end(body) { this.headersSent = true; finish(body); return this; } };
    router.handle(request(method, route, options), res, error => error ? reject(error) : finish(undefined));
  });
}
assert.equal(invoke('/api/expenses/config').status, 200);
const baseline = read();
test.after(() => fs.rmSync(temp, { recursive: true, force: true }));

function reset(mutate = () => {}) {
  const tag = { nature: 'PERSONAL', status: 'manual', source: 'manual', linkedRecordIds: [transferId], sourceConflict: false, reason: 'Synthetic verified personal transaction' };
  const incoming = { id: 'BTX-SYNTHETIC-RESTORE-CREDIT', date, description: 'UPI/' + reference + '/Synthetic receiving personal transfer', reference, debit: 0, credit: amount, balance: 4825.75, transactionNature: 'PERSONAL', entityTag: clone(tag) };
  const transfer = { id: transferId, nature: 'PERSONAL', fromNature: 'PERSONAL', toNature: 'PERSONAL', classification: 'internal_transfer', fromAccount: source, toAccount: receiving, date, amount, bankStatementEvidence: true, bankReference: '9000000000', reconciliationDraft: 'BRD-SYNTHETIC-RESTORE-ORIGIN', bankRowId: 'bank-0', proof: '/synthetic-transfer-proof.jpg', createdBy: 'synthetic-owner', createdAt: date + 'T09:00:00.000Z', note: 'Original saved transfer metadata remains unchanged', accountingExcluded: true, accountingExclusion: { reason: 'Synthetic mistaken exclusion', by: 'synthetic-owner', at: date + 'T10:00:00.000Z' } };
  const state = Object.assign(clone(baseline), {
    transfers: [transfer], transferSeq: 103, adjustments: [], adjSeq: 11, reconciliationExpenses: [], bankTruthMovements: [], receipts: [], receivables: {}, loanRepayments: [], vendorAdvances: [], vendorOpeningPayables: [], salesRefunds: [],
    expenses: { 'EX-SYNTHETIC-UNRELATED': { id: 'EX-SYNTHETIC-UNRELATED', nature: 'SANKI', date: '2098-01-01', amount: 31, status: 'paid', paidAmount: 31, approvedAt: '2098-01-01', claimant: 'synthetic-user', vendor: 'Synthetic vendor', particulars: 'Preserved expense', ledger: 'Flowers', payments: [{ id: 'PAY-001', date: '2098-01-01', amount: 31, account: 'Prashant Cash', proof: '/synthetic-proof.jpg', proofs: ['/synthetic-proof.jpg'] }], reimbursementPayments: [] } },
    openingBalances: {}, openingBalancesByNature: { PERSONAL: { [source]: 10000, [receiving]: 3000 } }, physicalBankOpeningBalances: { [source]: 10000 },
    bankStatements: { ['PERSONAL|' + receiving]: { transactions: { [incoming.id]: incoming }, imports: [{ id: 'BST-SYNTHETIC-RESTORE-ORIGIN', draftId: transfer.reconciliationDraft, from: date, to: date, finalizedAt: date + 'T09:30:00.000Z', finalizedBy: 'synthetic-owner', reconciliationRows: [{ id: 'bank-0', bank: { ...clone(incoming), transactionId: incoming.id }, ledger: { id: transferId, date, reference, debit: 0, credit: amount }, linkedRecordIds: [transferId], entityTag: clone(tag), transactionNature: 'PERSONAL', decision: 'Bank-confirmed internal transfer', reason: 'Original finalized receiving link', correctionHistory: [{ by: 'synthetic-owner', reason: 'Preserve historical evidence' }] }] }] } },
    bankReconciliationDrafts: { [draftId]: { id: draftId, nature: 'SAMAST', account: source, transactions: [{ date, reference, description: 'UPI/' + reference + '/DR/Synthetic personal transfer debit', debit: amount, credit: 0, balance: 8174.25 }], summary: { from: date, to: date, openingBalance: 10000, closingBalance: 8174.25, totalDebits: amount, totalCredits: 0, validated: true }, transactionOwnership: { 'bank-0': { nature: 'PERSONAL', source: 'manual', reason: 'Verified personal ownership', by: 'synthetic-owner', at: date + 'T10:30:00.000Z' } }, resolutions: {}, matchingExclusions: { 'bank-0': { appId: transferId, reason: 'Review excluded original instead of creating a duplicate', by: 'synthetic-owner', at: date + 'T10:30:00.000Z' } }, createdBy: 'synthetic-owner', createdAt: date + 'T10:30:00.000Z', expiresAt: '2099-01-01T00:00:00.000Z', matchingPolicy: 'balanced_date_amount_v5' } },
    bankReconciliationApprovals: {}, bankReconciliationLinks: { 'EX-SYNTHETIC-UNRELATED/PAY-001': { reconciliationDraft: 'BST-SYNTHETIC-PRESERVED' } }, bankDateOverrides: { 'EX-SYNTHETIC-UNRELATED/PAY-001': { originalDate: '2098-01-01', bankDate: '2098-01-01', bankReference: 'SYNTHETIC-PRESERVED-REF' } },
    auditLog: [{ id: 'AUD-SYNTHETIC-PRIOR', seq: 1, action: 'EXISTING_AUDIT', subjectType: 'transfer', subjectId: transferId, note: 'Preserved audit entry' }], auditSeq: 1
  });
  mutate(state); write(state);
  // Finish pre-existing application normalization before testing no-write
  // restoration APIs. All normalization stays in this synthetic directory.
  assert.equal(invoke('/api/expenses/config').status, 200);
  return read();
}
const preview = (options = {}) => dispatch(previewRoute, { body: intent, ...options });
const apply = (fingerprint, options = {}) => dispatch(applyRoute, { body: { ...intent, fingerprint, reason, confirm: true }, ...options });
function unchangedFinancialRecords(before, after) {
  for (const key of ['expenses', 'receipts', 'adjustments', 'reconciliationExpenses', 'bankTruthMovements', 'loanRepayments', 'vendorAdvances', 'vendorOpeningPayables', 'salesRefunds', 'openingBalances', 'openingBalancesByNature', 'physicalBankOpeningBalances', 'bankStatements', 'bankDateOverrides', 'bankReconciliationLinks', 'bankReconciliationApprovals', 'transferSeq', 'adjSeq']) assert.deepEqual(after[key], before[key], key + ' must remain unchanged');
  assert.equal(after.transfers.length, before.transfers.length);
  for (const key of ['id', 'nature', 'fromNature', 'toNature', 'fromAccount', 'toAccount', 'date', 'amount', 'classification', 'bankReference', 'reconciliationDraft', 'bankRowId', 'proof', 'note', 'createdAt', 'createdBy']) assert.deepEqual(after.transfers[0][key], before.transfers[0][key], 'the original transfer ' + key + ' is immutable');
  assert.deepEqual(after.auditLog.slice(0, before.auditLog.length), before.auditLog);
}

test('real helpers preview a shared 7883 PERSONAL row without writing or restoring it', async () => {
  reset(); const before = bytes(), result = await preview();
  assert.equal(result.status, 200, JSON.stringify(result.body)); assert.equal(result.body.success, true);
  assert.equal(result.body.plan.transfer.id, transferId); assert.ok(result.body.plan.fingerprint);
  assert.equal(bytes(), before); assert.equal(read().transfers[0].accountingExcluded, true);
});

for (const role of ['admin', 'manager', 'claimant']) for (const route of [previewRoute, applyRoute]) test(role + ' cannot ' + route.split('/').at(-1) + ' an Owner-excluded transfer even with personal accounting permission', async () => {
  reset(); const before = bytes();
  const result = await dispatch(route, { role, username: role === 'admin' ? 'another-admin' : 'prashant', permissions: { personalAccounting: true }, body: { ...intent, confirm: true, fingerprint: 'a'.repeat(64), reason } });
  assert.equal(result.status, 403, JSON.stringify(result.body)); assert.equal(bytes(), before);
});

test('Prashant can preview and restore the exact excluded transfer without creating a duplicate', async () => {
  const before = reset(), actor = { role: 'admin', username: 'prashant', permissions: { personalAccounting: true } };
  const reviewed = await preview(actor); assert.equal(reviewed.status, 200, JSON.stringify(reviewed.body));
  const result = await apply(reviewed.body.plan.fingerprint, actor); assert.equal(result.status, 200, JSON.stringify(result.body));
  const after = read(); assert.equal(after.transfers.length, 1); assert.equal(after.transfers[0].accountingExcluded, false);
  assert.equal(after.bankReconciliationDrafts[draftId].resolutions['bank-0'].appId, transferId);
  assert.equal(after.transfers[0].restorationHistory.at(-1).by, 'prashant'); unchangedFinancialRecords(before, after);
});

test('Owner apply restores one existing transfer and stages its exact link without changing other financial data', async () => {
  const before = reset(), reviewed = await preview(); assert.equal(reviewed.status, 200, JSON.stringify(reviewed.body));
  const result = await apply(reviewed.body.plan.fingerprint); assert.equal(result.status, 200, JSON.stringify(result.body));
  const after = read(); assert.equal(after.transfers[0].accountingExcluded, false);
  assert.equal(after.bankReconciliationDrafts[draftId].resolutions['bank-0'].action, 'link_existing');
  assert.equal(after.bankReconciliationDrafts[draftId].resolutions['bank-0'].appId, transferId);
  assert.equal(after.transfers[0].restorationHistory.length, 1); unchangedFinancialRecords(before, after);
  const saved = bytes(), replay = await apply(reviewed.body.plan.fingerprint);
  assert.equal(replay.status, 200, JSON.stringify(replay.body)); assert.equal(replay.body.alreadyRestored, true); assert.equal(bytes(), saved, 'HTTP replay must not save another audit or money movement');
});

test('Owner restores both sides when the exact opposite finalized bank row was also excluded', async () => {
  const before = reset(state => {
    const row = state.bankStatements['PERSONAL|' + receiving].imports[0].reconciliationRows[0];
    row.linkedRecordIds = [];
    row.ledger = {};
    delete row.bank.transactionId;
    row.decision = 'Reviewed bank record without ledger link';
    row.reason = 'The linked transfer was later excluded';
    state.transfers[0].bankReconciliationEvidence = { recordId: 'BST-SYNTHETIC-RESTORE-ORIGIN', description: row.bank.description, reference };
  });
  const reviewed = await preview();
  assert.equal(reviewed.status, 200, JSON.stringify(reviewed.body));
  assert.equal(reviewed.body.plan.oppositeCorrection.recordId, 'BST-SYNTHETIC-RESTORE-ORIGIN');
  const result = await apply(reviewed.body.plan.fingerprint);
  assert.equal(result.status, 200, JSON.stringify(result.body));
  const after = read(), row = after.bankStatements['PERSONAL|' + receiving].imports[0].reconciliationRows[0];
  assert.equal(after.transfers.length, 1);
  assert.equal(after.transfers[0].accountingExcluded, false);
  assert.deepEqual(row.linkedRecordIds, [transferId]);
  assert.equal(row.ledger.id, transferId);
  assert.equal(row.corrected, true);
  assert.equal(row.correctionHistory.at(-1).type, 'restore_excluded_transfer');
  assert.deepEqual(after.expenses, before.expenses);
  assert.deepEqual(after.receipts, before.receipts);
  assert.deepEqual(after.bankTruthMovements, before.bankTruthMovements);
});

for (const changed of [
  { name: 'stale transfer amount', mutate: s => { s.transfers[0].amount += 1; } },
  { name: 'stale exclusion reason', mutate: s => { s.transfers[0].accountingExclusion.reason = 'Changed by another reviewer'; } },
  { name: 'changed actual bank row', mutate: s => { s.bankReconciliationDrafts[draftId].transactions[0].reference = '900000006124'; } },
  { name: 'new pending Owner request', mutate: s => { s.bankReconciliationApprovals['BRA-SYNTHETIC-PENDING'] = { id: 'BRA-SYNTHETIC-PENDING', draftId, status: 'pending' }; } }
]) test(changed.name + ' rejects Owner apply without saving anything', async () => {
  reset(); const reviewed = await preview(); assert.equal(reviewed.status, 200, JSON.stringify(reviewed.body));
  const state = read(); changed.mutate(state); write(state); const before = bytes();
  const result = await apply(reviewed.body.plan.fingerprint); assert.equal(result.status, 409, JSON.stringify(result.body)); assert.equal(bytes(), before);
});

for (const change of [{ confirm: false }, { confirm: undefined }, { reason: '' }, { reason: 'yes' }, { fingerprint: 'forged' }, { transferId: 'TR-SYNTHETIC-DOES-NOT-EXIST' }]) test('invalid explicit apply request is atomic: ' + JSON.stringify(change), async () => {
  reset(); const reviewed = await preview(); assert.equal(reviewed.status, 200, JSON.stringify(reviewed.body)); const before = bytes();
  const result = await apply(reviewed.body.plan.fingerprint, { body: { ...intent, fingerprint: reviewed.body.plan.fingerprint, reason, confirm: true, ...change } });
  assert.ok(result.status >= 400 && result.status < 500, JSON.stringify(result.body)); assert.equal(bytes(), before);
});

for (const [name, mutate] of [
  ['missing retained receiving transaction', s => { s.bankStatements['PERSONAL|' + receiving].transactions = {}; }],
  ['wrong selected ownership', s => { s.bankReconciliationDrafts[draftId].transactionOwnership['bank-0'].nature = 'SAMAST'; }],
  ['unfinalized opposite history', s => { delete s.bankStatements['PERSONAL|' + receiving].imports[0].finalizedAt; }],
  ['standalone bank truth', s => { s.bankTruthMovements.push({ id: 'BTR-SYNTHETIC-COMPETING', nature: 'PERSONAL', account: source, date, reference, debit: amount, credit: 0 }); }],
  ['another active transfer', s => { s.transfers.push({ ...clone(s.transfers[0]), id: 'TR-SYNTHETIC-COMPETING', accountingExcluded: false }); }]
]) test('real preview fails closed for ' + name + ' without a partial write', async () => {
  reset(mutate); const before = bytes(), result = await preview();
  assert.equal(result.status, 409, JSON.stringify(result.body)); assert.equal(bytes(), before);
});

test('ordinary config and ledger reads never restore exclusions automatically', () => {
  const before = reset();
  invoke('/api/expenses/config');
  invoke('/api/expenses/account-ledger', { query: { nature: 'SAMAST', account: source, to: date } });
  invoke('/api/expenses/account-ledger', { query: { nature: 'PERSONAL', account: receiving, to: date } });
  const after = read(); assert.equal(after.transfers[0].accountingExcluded, true); assert.equal(after.transfers[0].restorationHistory, undefined); unchangedFinancialRecords(before, after);
});

test('raw preview and successful apply do not run unrelated migrations or category normalization', async () => {
  reset(); const raw = read(); raw.oneTimeMigrations = {}; raw.syntheticUnnormalizedMarker = { spaces: '  retained exactly  ' };
  raw.expenses['EX-SYNTHETIC-UNRELATED'].ledger = '  Unnormalized synthetic category  '; write(raw);
  const before = bytes(), reviewed = await preview(); assert.equal(reviewed.status, 200, JSON.stringify(reviewed.body)); assert.equal(bytes(), before);
  const saved = await apply(reviewed.body.plan.fingerprint); assert.equal(saved.status, 200, JSON.stringify(saved.body));
  const after = read(); unchangedFinancialRecords(raw, after); assert.deepEqual(after.oneTimeMigrations, {}); assert.deepEqual(after.syntheticUnnormalizedMarker, raw.syntheticUnnormalizedMarker);
});

test('unauthorized denial happens before reading an invalid accounting file', async () => {
  reset(); fs.writeFileSync(file, '{ synthetic invalid json'); const before = bytes();
  for (const route of [previewRoute, applyRoute]) {
    const denied = await dispatch(route, { role: 'admin', username: 'another-admin', permissions: { personalAccounting: true }, body: { ...intent, fingerprint: 'a'.repeat(64), confirm: true, reason } });
    assert.equal(denied.status, 403); assert.equal(bytes(), before);
  }
});

test('an unsupported receiving account is rejected even when the selected 7883 account is accessible', async () => {
  reset(); const raw = read(); raw.transfers[0].toAccount = 'Unlisted synthetic personal account'; write(raw); const before = bytes();
  const result = await preview(); assert.equal(result.status, 403, JSON.stringify(result.body)); assert.equal(bytes(), before);
});

test('shared 7883 must use its authorized canonical reconciliation book', async () => {
  reset(); const raw = read(); raw.bankReconciliationDrafts[draftId].nature = 'PERSONAL'; write(raw); const before = bytes();
  const result = await preview(); assert.equal(result.status, 403, JSON.stringify(result.body)); assert.equal(bytes(), before);
});

test('unfinished bank-authority journal blocks restoration without recovering or altering either file', async () => {
  reset(); const reviewed = await preview(); assert.equal(reviewed.status, 200);
  const journal = path.join(temp, '.bank-authority-transaction.json'), journalBytes = JSON.stringify({ version: 1, syntheticPending: true }); fs.writeFileSync(journal, journalBytes);
  try {
    const before = bytes();
    for (const result of [await preview(), await apply(reviewed.body.plan.fingerprint)]) { assert.equal(result.status, 409, JSON.stringify(result.body)); assert.equal(bytes(), before); assert.equal(fs.readFileSync(journal, 'utf8'), journalBytes); }
  } finally { fs.unlinkSync(journal); }
});

for (const name of ['salary.json', 'salary-samast.json', 'credit-cards.json', 'incentives.json']) test('external ' + name + ' dependency blocks without changing either store', async () => {
  reset(); const dependency = path.join(temp, name), original = fs.existsSync(dependency) ? fs.readFileSync(dependency, 'utf8') : null;
  const content = JSON.stringify({ syntheticRecord: { id: 'SYNTHETIC-EXTERNAL-RECORD', fundingTransferId: transferId } }); fs.writeFileSync(dependency, content);
  try { const before = bytes(), result = await preview(); assert.equal(result.status, 409, JSON.stringify(result.body)); assert.equal(bytes(), before); assert.equal(fs.readFileSync(dependency, 'utf8'), content); }
  finally { if (original == null) fs.unlinkSync(dependency); else fs.writeFileSync(dependency, original); }
});

test('a malformed external dependency store fails closed without repair-on-read', async () => {
  reset(); const dependency = path.join(temp, 'salary.json'), original = fs.existsSync(dependency) ? fs.readFileSync(dependency, 'utf8') : null; fs.writeFileSync(dependency, '{ synthetic malformed salary');
  try { const before = bytes(), result = await preview(); assert.equal(result.status, 409, JSON.stringify(result.body)); assert.equal(bytes(), before); assert.equal(fs.readFileSync(dependency, 'utf8'), '{ synthetic malformed salary'); }
  finally { if (original == null) fs.unlinkSync(dependency); else fs.writeFileSync(dependency, original); }
});

test('an active receipt with the same receiving-bank UTR is a duplicate claim, not a restoration opportunity', async () => {
  reset(); const raw = read(); raw.receipts.push({ id: 'REC-SYNTHETIC-COMPETING', nature: 'PERSONAL', account: receiving, date, amount, bankReference: reference, source: 'Synthetic receipt' }); write(raw);
  const before = bytes(), result = await preview(); assert.equal(result.status, 409, JSON.stringify(result.body)); assert.equal(bytes(), before);
});

test('client-supplied financial fields, plan and exclusion arrays cannot alter the exact-ID restoration', async () => {
  const before = reset(), reviewed = await preview(); assert.equal(reviewed.status, 200);
  const result = await apply(reviewed.body.plan.fingerprint, { body: { ...intent, fingerprint: reviewed.body.plan.fingerprint, reason, confirm: true, amount: 999999, fromAccount: 'Counter Cash', toAccount: 'Unlisted account', date: '2098-12-31', plan: { transfer: { amount: 999999 }, exclusionsToClear: [{ draftId: 'BRD-UNRELATED', rowId: 'app-0' }] }, exclusionsToClear: [{ draftId, rowId: 'bank-0' }] } });
  assert.equal(result.status, 200, JSON.stringify(result.body)); unchangedFinancialRecords(before, read());
});

test('an atomic store-save failure does not persist a restored flag, link, audit or duplicate', async () => {
  reset(); const reviewed = await preview(); assert.equal(reviewed.status, 200); const before = bytes(), originalRename = fs.renameSync;
  fs.renameSync = function (from, to) { if (path.resolve(to) === path.resolve(file)) { const error = new Error('Synthetic disk failure before atomic replacement'); error.code = 'EIO'; throw error; } return originalRename.apply(this, arguments); };
  try { const result = await apply(reviewed.body.plan.fingerprint); assert.equal(result.status, 409, JSON.stringify(result.body)); assert.equal(bytes(), before); assert.equal(fs.readdirSync(temp).some(name => name.startsWith('expenses.json.tmp-')), false); }
  finally { fs.renameSync = originalRename; }
});

'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

// Every bank row and ledger record below is synthetic. Do not load the actual
// rental source or any live financial snapshot when the route module starts.
const rentalSource = require.resolve('../modules/rental-source.json');
require.cache[rentalSource] = { id: rentalSource, filename: rentalSource, loaded: true, exports: { id: 'SYNTHETIC-TRANSFER-TEST', asOf: '2098-01-01', file: 'Synthetic fixture', sheet: 'None', range: '', rows: [] } };
const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'personal-transfer-bank-identity-'));
process.env.DATA_PATH = path.join(temp, 'data.json');
const file = path.join(temp, 'expenses.json');
fs.writeFileSync(file, JSON.stringify({ expenses: {}, receipts: [], receivables: {}, oneTimeMigrations: {} }));
const { router, applyFinalizedInternalTransfers } = require('../modules/expenses');
const clone = value => JSON.parse(JSON.stringify(value));
const read = () => JSON.parse(fs.readFileSync(file, 'utf8'));
const write = value => fs.writeFileSync(file, JSON.stringify(value));
const sourceAccount = 'Namita 5464', destinationAccount = 'IndusInd Bank 7883';
const date = '2098-08-28', amount = 2000;
const currentRef = '313758628548', priorRef = '214681264504';
const bookKey = account => account === destinationAccount ? account : 'PERSONAL|' + account;

function request(method, route, { role = 'admin', username = 'prashant', permissions = { personalAccounting: true }, body = {}, query = {} } = {}) {
  return { method, path: route, url: route, originalUrl: route, baseUrl: '', body, query, params: {}, headers: { 'user-agent': 'Synthetic bank identity regression' }, get(name) { return this.headers[String(name).toLowerCase()] || ''; }, user: { role, roles: [role], username, permissions } };
}
function invoke(route, options = {}) {
  const method = options.method || 'POST';
  const layer = router.stack.find(item => item.route?.path === route && item.route.methods[method.toLowerCase()]);
  assert.ok(layer, route);
  let status = 200, body;
  layer.route.stack[0].handle(request(method, route, options), { status(code) { status = code; return this; }, json(value) { body = value; return this; } }, () => { throw Error('Unexpected next: ' + route); });
  return { status, body };
}
function dispatch(route, options = {}) {
  const method = options.method || 'POST';
  return new Promise((resolve, reject) => {
    let status = 200, finished = false;
    const headers = {};
    const finish = body => { if (!finished) { finished = true; resolve({ status, body }); } };
    const res = {
      statusCode: 200, headersSent: false,
      status(code) { this.statusCode = status = code; return this; },
      setHeader(name, value) { headers[String(name).toLowerCase()] = value; }, getHeader(name) { return headers[String(name).toLowerCase()]; }, removeHeader(name) { delete headers[String(name).toLowerCase()]; },
      json(body) { this.headersSent = true; finish(body); return this; }, send(body) { this.headersSent = true; finish(body); return this; }, end(body) { this.headersSent = true; finish(body); return this; }
    };
    router.handle(request(method, route, options), res, error => error ? reject(error) : finish(undefined));
  });
}
assert.equal(invoke('/api/expenses/config', { method: 'GET', role: 'owner' }).status, 200);
const baseline = read();
test.after(() => fs.rmSync(temp, { recursive: true, force: true }));

function draft({ id = 'BRD-CURRENT', incoming = false, reference = currentRef, createdAt = date + 'T10:00:00.000Z' } = {}) {
  const account = incoming ? destinationAccount : sourceAccount, debit = incoming ? 0 : amount, credit = incoming ? amount : 0;
  return {
    id, nature: incoming ? 'SAMAST' : 'PERSONAL', account,
    transactions: [{ date, description: incoming ? 'Synthetic transfer received from Namita' : 'Synthetic transfer to Gagan personal account', reference, debit, credit, balance: incoming ? 12000 : 8000 }],
    summary: { from: date, to: date, openingBalance: 10000, closingBalance: incoming ? 12000 : 8000, totalDebits: debit, totalCredits: credit, validated: true },
    resolutions: {}, matchingExclusions: { 'bank-0': { reason: 'Explicitly unmatched for independent bank identity review', by: 'prashant', at: createdAt } },
    ...(incoming ? { transactionOwnership: { 'bank-0': { nature: 'PERSONAL', source: 'manual', reason: 'Verified personal bank-side ownership', by: 'prashant', at: createdAt } } } : {}),
    matchingPolicy: 'balanced_date_amount_v5', createdBy: 'prashant', createdAt, expiresAt: '2099-01-01T00:00:00.000Z'
  };
}
function resolution(current, extra = {}) {
  return { action: 'create_internal_transfer', otherNature: 'PERSONAL', otherAccount: current.account === sourceAccount ? destinationAccount : sourceAccount, classification: 'internal_transfer', postingNature: 'PERSONAL', principalAmount: amount, chargeAmount: 0, reason: 'Verified separate bank UTR for the personal transfer', by: 'prashant', at: current.createdAt, ...extra };
}
function finalized({ id = 'TR-00046', reference = priorRef, incoming = false, excluded = false, evidence = true, origin = false, rawReference = '9810863742', description } = {}) {
  const transfer = { id, nature: 'PERSONAL', fromNature: 'PERSONAL', toNature: 'PERSONAL', classification: 'internal_transfer', fromAccount: sourceAccount, toAccount: destinationAccount, amount, date, proof: '/existing-transfer-proof.jpg', bankStatementEvidence: true, bankReference: rawReference, reconciliationDraft: 'BRD-ORIGIN-' + id, bankRowId: 'bank-0', note: 'Existing immutable bank-confirmed transfer', createdBy: 'owner-user', createdAt: date + 'T09:00:00.000Z', ...(excluded ? { accountingExcluded: true, accountingExclusion: { reason: 'Previously excluded after Owner review', by: 'owner-user', at: date + 'T09:10:00.000Z' } } : {}) };
  const account = incoming ? destinationAccount : sourceAccount;
  const bank = { date, reference, description: description ?? 'Verified earlier same-day bank movement ' + id, debit: incoming ? 0 : amount, credit: incoming ? amount : 0, balance: 10000 };
  const transactionId = 'BTX-ORIGINAL-' + id;
  const tag = { nature: 'PERSONAL', status: 'manual', source: 'manual', linkedRecordIds: [id], sourceConflict: false, reason: 'Synthetic original personal ownership' };
  const stored = { ...bank, id: transactionId, entityTag: clone(tag), transactionNature: 'PERSONAL' };
  const row = { id: 'bank-0', bank: { ...bank, transactionId }, ledger: { id, date, reference, debit: bank.debit, credit: bank.credit, description: 'Original verified personal transfer' }, linkedRecordIds: [id], entityTag: clone(tag), transactionNature: 'PERSONAL', decision: 'Bank-confirmed internal transfer', reason: 'Original finalized decision retained', correctionHistory: [{ reason: 'Immutable prior review', by: 'owner-user' }] };
  const record = { id: 'BST-ORIGINAL-' + id, draftId: transfer.reconciliationDraft, from: date, to: date, finalizedAt: date + 'T09:05:00.000Z', finalizedBy: 'owner-user', reconciliationRows: [row] };
  const originDraft = origin ? draft({ id: transfer.reconciliationDraft, incoming, reference, createdAt: transfer.createdAt }) : null;
  if (originDraft) { originDraft.transactions = [clone(bank)]; originDraft.resolutions['bank-0'] = resolution(originDraft); }
  return { transfer, account, book: evidence ? { transactions: { [transactionId]: stored }, imports: [record] } : null, originDraft };
}
function exactCurrent(item) {
  const current = draft({ incoming: item.account === destinationAccount });
  current.transactions = [clone(item.book.imports[0].reconciliationRows[0].bank)];
  delete current.transactions[0].transactionId;
  current.summary.openingBalance = current.transactions[0].balance + current.transactions[0].debit - current.transactions[0].credit;
  current.summary.closingBalance = current.transactions[0].balance;
  return current;
}
function reset({ current = draft(), existing = [], otherDrafts = [], extra = {} } = {}) {
  const store = Object.assign(clone(baseline), {
    expenses: { 'EX-PRESERVE': { id: 'EX-PRESERVE', nature: 'SANKI', date: '2098-01-01', amount: 75, status: 'paid', paidAmount: 75, approvedAt: '2098-01-01', claimant: 'existing-user', vendor: 'Existing vendor', particulars: 'Existing paid expense', ledger: 'Flowers', payments: [{ id: 'PAY-001', date: '2098-01-01', amount: 75, account: 'Prashant Cash', proof: '/existing-payment-proof.jpg', proofs: ['/existing-payment-proof.jpg'] }], reimbursementPayments: [] } },
    transfers: existing.map(item => clone(item.transfer)), receipts: [], receivables: {}, adjustments: [], reconciliationExpenses: [], loanRepayments: [], vendorAdvances: [], vendorOpeningPayables: [], salesRefunds: [], bankTruthMovements: [],
    bankDateOverrides: { 'EX-PRESERVE/PAY-001': { bankDate: '2098-01-01', originalDate: '2098-01-01', bankReference: 'SYNTHETIC-OLD-BANK-REF', reconciliationDraft: 'BRD-PRESERVED', by: 'owner-user', at: '2098-01-01T10:00:00Z' } },
    bankReconciliationLinks: { 'EX-PRESERVE/PAY-001': { type: 'single_bank_entry', reconciliationDraft: 'BRD-PRESERVED', remark: 'Existing approved payment link is immutable' } },
    bankReconciliationApprovals: { 'BRD-PRESERVED': { status: 'approved', by: 'owner-user', at: '2098-01-01T10:00:00Z', fingerprint: 'SYNTHETIC-PRESERVED-APPROVAL' } },
    bankReconciliationDrafts: { [current.id]: clone(current) },
    bankStatements: { 'Tiana 0425': { transactions: {}, imports: [{ id: 'BST-UNRELATED', finalizedAt: '2098-01-01T10:00:00Z', reconciliationRows: [], originalNote: 'Unrelated finalized reconciliation must not change' }] } },
    openingBalances: {}, openingBalancesByNature: { PERSONAL: { [sourceAccount]: 12000, [destinationAccount]: 10000 } }, physicalBankOpeningBalances: { [destinationAccount]: 10000 },
    auditLog: [{ seq: 1, action: 'EXISTING_AUDIT', note: 'Must remain unchanged' }], auditSeq: 1, transferSeq: 49, adjSeq: 0
  }, extra);
  for (const item of existing) {
    if (item.book) {
      const key = bookKey(item.account), book = store.bankStatements[key] || (store.bankStatements[key] = { transactions: {}, imports: [] });
      Object.assign(book.transactions, clone(item.book.transactions)); book.imports.push(...clone(item.book.imports));
    }
    if (item.originDraft) store.bankReconciliationDrafts[item.originDraft.id] = clone(item.originDraft);
  }
  for (const other of otherDrafts) store.bankReconciliationDrafts[other.id] = clone(other);
  write(store);
  // Finish existing load-time fixture normalization before taking no-write
  // comparisons. No real financial files are read or touched.
  assert.equal(invoke('/api/expenses/config', { method: 'GET', role: 'owner' }).status, 200);
  return { current: read().bankReconciliationDrafts[current.id], before: read() };
}
function stage(current, extra = {}) { return invoke('/api/expenses/bank-statements/resolve', { body: { draftId: current.id, rowId: 'bank-0', ...resolution(current), ...extra } }); }
function incoming(current, extra = {}) { return invoke('/api/expenses/bank-statements/create-incoming', { body: { draftId: current.id, rowId: 'bank-0', sourceKind: 'internal', fromNature: 'PERSONAL', fromAccount: sourceAccount, classification: 'internal_transfer', note: 'Verified incoming transfer with actual bank UTR', ...extra } }); }
function assertExistingUnchanged(before, after) {
  for (const transfer of before.transfers) assert.deepEqual(after.transfers.find(item => item.id === transfer.id), transfer, 'existing transfer and exclusion state must not change');
  for (const [key, book] of Object.entries(before.bankStatements)) {
    for (const record of book.imports) assert.deepEqual(after.bankStatements[key].imports.find(item => item.id === record.id), record, 'existing finalized report rows/history must not change');
    for (const [id, transaction] of Object.entries(book.transactions)) assert.deepEqual(after.bankStatements[key].transactions[id], transaction, 'existing stored physical bank evidence must not change');
  }
  for (const key of ['expenses', 'receipts', 'adjustments', 'reconciliationExpenses', 'vendorAdvances', 'vendorOpeningPayables', 'salesRefunds', 'bankTruthMovements']) assert.deepEqual(after[key], before[key], key + ' must not change');
  for (const key of ['bankDateOverrides', 'bankReconciliationLinks', 'bankReconciliationApprovals']) for (const [id, original] of Object.entries(before[key])) assert.deepEqual(after[key][id], original, 'existing ' + key + ' must not change');
  assert.deepEqual(after.auditLog.slice(0, before.auditLog.length), before.auditLog, 'prior audit history remains intact');
}

for (const excluded of [false, true]) test('different verified source-bank UTR permits an additional same-day ₹2,000 transfer' + (excluded ? ' even when the separate prior transfer is excluded' : ''), () => {
  const h = reset({ existing: [finalized({ id: excluded ? 'TR-00049' : 'TR-00046', excluded })] });
  const result = stage(h.current); assert.equal(result.status, 200, JSON.stringify(result.body));
  const staged = read(); assert.equal(staged.transfers.length, 1, 'review must not post a second transfer');
  assert.equal(staged.bankReconciliationDrafts[h.current.id].resolutions['bank-0'].action, 'create_internal_transfer');
  assertExistingUnchanged(h.before, staged);
  const posted = applyFinalizedInternalTransfers(staged.bankReconciliationDrafts[h.current.id], 'prashant');
  assert.equal(posted.created.length, 1); assert.equal(posted.blocked.length, 0);
  const after = read(), made = after.transfers.find(item => item.id === posted.created[0]);
  assert.equal(made.amount, amount); assert.equal(made.date, date); assert.equal(made.bankReference, currentRef);
  assertExistingUnchanged(h.before, after);
});

for (const excluded of [false, true]) test('same verified source-bank UTR blocks duplicate creation' + (excluded ? ' and requires review of the excluded original' : ''), () => {
  const original = finalized({ id: excluded ? 'TR-00049' : 'TR-00046', reference: currentRef, excluded });
  const h = reset({ current: exactCurrent(original), existing: [original] });
  const result = stage(h.current); assert.equal(result.status, 409, JSON.stringify(result.body));
  assert.match(result.body.error, excluded ? /excluded|restore|review/i : /already exists|existing/i);
  assert.match(result.body.error, new RegExp(excluded ? 'TR-00049' : 'TR-00046'));
  if (excluded) assert.match(result.body.error, /Previously excluded after Owner review/);
  assert.deepEqual(read(), h.before, 'a blocked duplicate must not write any transfer, decision, flag or history');
});

test('the ₹2,000 case checks every prior transfer: a distinct TR-00046 does not hide same-identity excluded TR-00049', () => {
  const excluded = finalized({ id: 'TR-00049', reference: currentRef, excluded: true });
  const h = reset({ current: exactCurrent(excluded), existing: [finalized(), excluded] });
  const result = stage(h.current); assert.equal(result.status, 409, JSON.stringify(result.body));
  assert.match(result.body.error, /TR-00049/); assert.match(result.body.error, /excluded|restore|review/i);
  assert.deepEqual(read(), h.before);
});

test('the same source UTR with contradictory physical metadata remains blocked rather than being called a new transfer', () => {
  const h = reset({ existing: [finalized({ id: 'TR-00049', reference: currentRef, excluded: true })] });
  const result = stage(h.current); assert.equal(result.status, 409, JSON.stringify(result.body));
  assert.match(result.body.error, /conflicting|review|ambiguous/i); assert.deepEqual(read(), h.before);
});

for (const excluded of [false, true]) test('a raw transfer reference is not proof of a distinct source bank transaction' + (excluded ? ' for an excluded original' : ''), () => {
  const h = reset({ existing: [finalized({ id: excluded ? 'TR-00049' : 'TR-00046', evidence: false, excluded, rawReference: priorRef })] });
  const result = stage(h.current); assert.equal(result.status, 409, JSON.stringify(result.body));
  assert.deepEqual(read(), h.before);
});

test('excluded prior identity with missing reference fails closed without unexcluding it', () => {
  const h = reset({ existing: [finalized({ id: 'TR-00049', reference: '', excluded: true, rawReference: '' })] });
  const result = stage(h.current); assert.equal(result.status, 409, JSON.stringify(result.body));
  assert.equal(read().transfers[0].accountingExcluded, true); assert.deepEqual(read(), h.before);
});

for (const weakReference of ['', '9810863742', 'MANUALLY-TYPED-DISTINCT']) test('weak actual prior bank reference remains ambiguous: ' + (weakReference || '(blank)'), () => {
  const h = reset({ existing: [finalized({ reference: weakReference })] });
  const result = stage(h.current); assert.equal(result.status, 409, JSON.stringify(result.body)); assert.deepEqual(read(), h.before);
});

for (const weakReference of ['', '9810863742', 'MANUALLY-TYPED-DISTINCT']) test('weak actual current bank reference cannot prove a distinct transfer: ' + (weakReference || '(blank)'), () => {
  const h = reset({ current: draft({ reference: weakReference }), existing: [finalized()] });
  const result = stage(h.current); assert.equal(result.status, 409, JSON.stringify(result.body)); assert.deepEqual(read(), h.before);
});

test('repeated current same-bank UTR is ambiguous even when its reference differs from the prior transfer', () => {
  const current = draft();
  current.transactions.push({ ...current.transactions[0], balance: 6000, description: 'Another bank occurrence with the same UTR' });
  current.summary.closingBalance = 6000; current.summary.totalDebits = 4000;
  const h = reset({ current, existing: [finalized()] }), result = stage(h.current);
  assert.equal(result.status, 409, JSON.stringify(result.body)); assert.deepEqual(read(), h.before);
});

test('request-supplied references cannot manufacture distinct bank identity', () => {
  const h = reset({ current: draft({ reference: priorRef }), existing: [finalized()] });
  const result = stage(h.current, { reference: currentRef, bankReference: currentRef, bankAccount: sourceAccount, bankDirection: 'debit' });
  assert.equal(result.status, 409, JSON.stringify(result.body)); assert.deepEqual(read(), h.before);
});

for (const same of [false, true]) test('an actual stored UPI narration supplies the bank identity rather than its phone-valued reference: ' + (same ? 'same UTR blocks' : 'distinct UTR permits'), () => {
  const item = finalized({ reference: '9810863742', description: 'UPI/' + (same ? currentRef : priorRef) + '/VERIFIED RECIPIENT/PERSONAL TRANSFER' });
  const h = reset({ existing: [item] }), result = stage(h.current);
  assert.equal(result.status, same ? 409 : 200, JSON.stringify(result.body));
  if (same) assert.deepEqual(read(), h.before); else assertExistingUnchanged(h.before, read());
});

test('a current actual UPI narration may identify its bank row when the separate reference contains a phone number', () => {
  const current = draft({ reference: '9810863742' }); current.transactions[0].description = 'UPI/' + currentRef + '/GAGAN PERSONAL/Sent using UPI';
  const h = reset({ current, existing: [finalized()] }), result = stage(h.current);
  assert.equal(result.status, 200, JSON.stringify(result.body)); assertExistingUnchanged(h.before, read());
});

for (const [label, reference, description] of [
  ['two different 12-digit UPI identifiers', '9810863742', 'UPI/' + priorRef + '/' + currentRef + '/AMBIGUOUS BANK EVIDENCE'],
  ['explicit bank UTR conflicts with narration', priorRef, 'UPI/111111111111/CONFLICTING BANK EVIDENCE'],
  ['non-UPI narration with a numerical account token', '9810863742', 'ACCOUNT/' + priorRef + '/NOT VERIFIED UPI EVIDENCE'],
  ['unbounded 12-digit narration token', '9810863742', 'UPI payment ' + priorRef + ' without slash-delimited bank reference'],
  ['12-digit token embedded in a longer number', '9810863742', 'UPI/9' + priorRef + '9/NOT A UNIQUE 12-DIGIT UTR']
]) test('ambiguous prior-bank narration cannot authorize a transfer: ' + label, () => {
  const h = reset({ existing: [finalized({ reference, description })] }), result = stage(h.current);
  assert.equal(result.status, 409, JSON.stringify(result.body)); assert.deepEqual(read(), h.before);
});

for (const [label, reference, description] of [
  ['two different UPI identifiers', '9810863742', 'UPI/' + currentRef + '/111111111111/AMBIGUOUS'],
  ['explicit current UTR conflicts with narration', currentRef, 'UPI/111111111111/CONFLICTING']
]) test('ambiguous current bank narration cannot prove a distinct transfer: ' + label, () => {
  const current = draft({ reference }); current.transactions[0].description = description;
  const h = reset({ current, existing: [finalized()] }), result = stage(h.current);
  assert.equal(result.status, 409, JSON.stringify(result.body)); assert.deepEqual(read(), h.before);
});

for (const incomingSide of [false, true]) test('opposite-bank-only differing UTR is not proof of a separate transfer (' + (incomingSide ? 'incoming' : 'outgoing') + ')', () => {
  const current = draft({ incoming: incomingSide }), h = reset({ current, existing: [finalized({ incoming: !incomingSide })] });
  const result = incomingSide ? incoming(h.current) : stage(h.current);
  assert.equal(result.status, 409, JSON.stringify(result.body)); assert.deepEqual(read(), h.before);
});

test('verified receiving-bank UTR permits a genuinely separate incoming transfer and repeated create does not duplicate it', () => {
  const h = reset({ current: draft({ incoming: true }), existing: [finalized({ incoming: true })] });
  const made = incoming(h.current); assert.equal(made.status, 200, JSON.stringify(made.body));
  assert.equal(made.body.transfer.amount, amount); assert.equal(made.body.transfer.date, date);
  assert.equal(made.body.transfer.bankReconciliationEvidence.reference, currentRef);
  const beforeReplay = read(); assert.equal(beforeReplay.transfers.length, 2);
  const replay = incoming(h.current); assert.notEqual(replay.status, 200); assert.deepEqual(read(), beforeReplay);
  assertExistingUnchanged(h.before, beforeReplay);
});

for (const distinct of [false, true]) test('cross-draft pending ' + (distinct ? 'different same-bank UTR can stage independently' : 'same same-bank UTR blocks another staged transfer'), () => {
  const earlier = draft({ id: 'BRD-EARLIER', reference: distinct ? priorRef : currentRef, createdAt: date + 'T09:00:00.000Z' });
  earlier.resolutions['bank-0'] = resolution(earlier);
  const h = reset({ otherDrafts: [earlier] }), result = stage(h.current);
  assert.equal(result.status, distinct ? 200 : 409, JSON.stringify(result.body));
  assert.deepEqual(read().bankReconciliationDrafts[earlier.id], h.before.bankReconciliationDrafts[earlier.id]);
  assert.equal(read().transfers.length, 0);
  if (!distinct) assert.deepEqual(read(), h.before);
});

test('differing opposite-side pending bank references remain a single transfer until linked', () => {
  const earlier = draft({ id: 'BRD-EARLIER', incoming: true, reference: priorRef, createdAt: date + 'T09:00:00.000Z' });
  earlier.resolutions['bank-0'] = resolution(earlier);
  const h = reset({ otherDrafts: [earlier] }), result = stage(h.current);
  assert.equal(result.status, 409, JSON.stringify(result.body)); assert.deepEqual(read(), h.before);
});

test('overlapping drafts with the same actual bank row/UTR cannot each create a transfer', () => {
  const earlier = draft({ id: 'BRD-EARLIER', createdAt: date + 'T09:00:00.000Z' });
  earlier.resolutions['bank-0'] = resolution(earlier);
  const current = clone(earlier); current.id = 'BRD-OVERLAP'; current.createdAt = date + 'T10:00:00.000Z'; current.resolutions = {};
  const h = reset({ current, otherDrafts: [earlier] }), result = stage(h.current);
  assert.equal(result.status, 409, JSON.stringify(result.body)); assert.deepEqual(read(), h.before);
});

test('actual retained origin draft rows can establish distinct identity when finalized history is absent', () => {
  const prior = finalized({ evidence: false, origin: true });
  const h = reset({ existing: [prior] }), result = stage(h.current);
  assert.equal(result.status, 200, JSON.stringify(result.body)); assertExistingUnchanged(h.before, read());
  assert.deepEqual(read().bankReconciliationDrafts[prior.originDraft.id], h.before.bankReconciliationDrafts[prior.originDraft.id]);
});

function exactStoredOrigin() {
  const item = finalized();
  item.book.imports[0].reconciliationRows = [];
  const bank = Object.values(item.book.transactions)[0];
  bank.reconciliationDraftId = item.transfer.reconciliationDraft;
  bank.reconciliationDraftRowIndex = 0;
  return item;
}

test('exact integer stored origin metadata plus a finalized book binding can verify an older transfer without a history link', () => {
  const h = reset({ existing: [exactStoredOrigin()] }), result = stage(h.current);
  assert.equal(result.status, 200, JSON.stringify(result.body)); assertExistingUnchanged(h.before, read());
});

for (const invalidIndex of [null, '', false, true, -1, 0.5, undefined]) test('invalid stored origin row index cannot be coerced into bank-0: ' + String(invalidIndex), () => {
  const item = exactStoredOrigin(); Object.values(item.book.transactions)[0].reconciliationDraftRowIndex = invalidIndex;
  const h = reset({ existing: [item] }), result = stage(h.current);
  assert.equal(result.status, 409, JSON.stringify(result.body)); assert.deepEqual(read(), h.before);
});

test('audit account labels alone cannot bind an old snapshot to a physical source bank', () => {
  const item = exactStoredOrigin(), bank = Object.values(item.book.transactions)[0];
  item.transfer.bankReconciliationEvidence = { draftId: item.transfer.reconciliationDraft, rowId: 'bank-0', reference: bank.reference, description: bank.description };
  item.book.imports[0].draftId = 'BRD-UNRELATED-IMPORT';
  const h = reset({ existing: [item] }), modified = read();
  modified.auditLog.push({ action: 'CREATED_FROM_BANK_RECONCILIATION', subjectType: 'transfer', subjectId: item.transfer.id, account: sourceAccount, after: clone(item.transfer) });
  write(modified);
  const result = stage(h.current); assert.equal(result.status, 409, JSON.stringify(result.body)); assert.deepEqual(read(), modified);
});

test('an unfinalized import cannot certify a saved original transfer snapshot', () => {
  const item = exactStoredOrigin(); delete item.book.imports[0].finalizedAt;
  const h = reset({ existing: [item] }), result = stage(h.current);
  assert.equal(result.status, 409, JSON.stringify(result.body)); assert.deepEqual(read(), h.before);
});

function bridgeFixture(change) {
  const active = finalized(), received = finalized({ incoming: true, reference: '555555555555', description: 'UPI/555555555555/VERIFIED OLD RECEIVING EVENT' });
  const receivedBook = received.book, oldId = Object.keys(receivedBook.transactions)[0], stored = receivedBook.transactions[oldId];
  delete receivedBook.transactions[oldId]; stored.id = 'BTX-ACTIVE-RECEIVING'; receivedBook.transactions[stored.id] = stored;
  const record = receivedBook.imports[0], row = record.reconciliationRows[0];
  record.id = 'BST-ACTIVE-RECEIVING'; record.draftId = 'BRD-RECEIVING-SIDE-LINK'; row.bank.transactionId = stored.id;
  const excluded = finalized({ id: 'TR-00049', evidence: false, excluded: true });
  excluded.transfer.bankReconciliationEvidence = { recordId: record.id, rowId: 'bank-0', reference: row.bank.reference, description: row.bank.description };
  const h = reset({ existing: [active, excluded] }), store = read();
  store.bankStatements[bookKey(destinationAccount)] = clone(receivedBook);
  if (change) change(store, { active, excluded, receivedBook });
  write(store); assert.equal(invoke('/api/expenses/config', { method: 'GET', role: 'owner' }).status, 200);
  return { current: read().bankReconciliationDrafts[h.current.id], before: read() };
}

function receivedNarration(reference = '555555555555', prefix = 'SYNTHETIC NAME ') {
  return prefix + 'UPI/SYNTHETIC NAME/9810863742@pty/Sent using/INDUSIND B/' + reference + '/PTMBABBAE2048A147C0AA0D96D54F15FD97';
}

// Reproduce the older create-incoming shape: its original draft has expired,
// the saved reference is a phone number, and only the immutable creation audit
// binds the saved received narration to the destination bank. The active
// transfer's finalized link must identify the very same stored physical credit.
function auditedIncomingBridgeFixture(change) {
  return bridgeFixture(store => {
    const book = store.bankStatements[bookKey(destinationAccount)], row = book.imports[0].reconciliationRows[0];
    const stored = book.transactions[row.bank.transactionId], description = receivedNarration();
    row.bank.reference = stored.reference = '9810863742';
    row.bank.description = stored.description = description;
    const excluded = store.transfers.find(item => item.id === 'TR-00049');
    delete excluded.reconciliationDraft; delete excluded.bankRowId;
    excluded.bankReconciliationEvidence = { draftId: 'BRD-EXPIRED-INCOMING', rowId: 'bank-0', reference: '9810863742', description };
    const audit = { action: 'CREATED_FROM_BANK_RECONCILIATION', subjectType: 'transfer', subjectId: excluded.id, nature: 'PERSONAL', account: sourceAccount, draftId: 'BRD-EXPIRED-INCOMING', bankRowId: 'bank-0', after: clone(excluded) };
    store.auditLog.push(audit);
    if (change) change(store, { excluded, audit, row, stored, book });
  });
}

for (const prefix of ['', 'SYNTHETIC NAME ', 'SYNTHETIC NAME\n', 'SYNTHETIC NAME \r\n  ']) test('a received UPI narration has a bounded bank identity with a name prefix: ' + JSON.stringify(prefix), () => {
  const original = finalized({ incoming: true, reference: '9810863742', description: receivedNarration(priorRef, prefix) });
  const h = reset({ current: draft({ incoming: true }), existing: [original] }), result = stage(h.current);
  assert.equal(result.status, 200, JSON.stringify(result.body));
  assert.equal(read().transfers.length, 1, 'review only saves a decision, not another transfer');
  assertExistingUnchanged(h.before, read());
});

for (const [label, reference, description] of [
  ['extra slash-delimited 12-digit token', '9810863742', receivedNarration(priorRef) + '/111111111111'],
  ['two UPI markers', '9810863742', receivedNarration(priorRef) + ' UPI/111111111111/AMBIGUOUS'],
  ['conflicting explicit bank reference', currentRef, receivedNarration(priorRef)],
  ['phone number with no bank UTR', '9810863742', receivedNarration('9810863742')],
  ['arbitrary account number outside UPI format', '9810863742', 'SYNTHETIC NAME ACCOUNT/' + priorRef + '/PERSONAL BANK'],
  ['free-form UPI sentence with an account number', '9810863742', 'SYNTHETIC NAME UPI payment ' + priorRef + ' received'],
  ['a 12-digit reference embedded in a longer number', '9810863742', receivedNarration('9' + priorRef + '9')],
  ['unbounded suffix after received bank reference', '9810863742', receivedNarration(priorRef) + '/NOT-VERIFIED']
]) test('a name-prefixed received narration still fails closed: ' + label, () => {
  const h = reset({ current: draft({ incoming: true }), existing: [finalized({ incoming: true, reference, description })] });
  const result = stage(h.current); assert.equal(result.status, 409, JSON.stringify(result.body));
  assert.deepEqual(read(), h.before);
});

test('the expired incoming audit may bind an excluded duplicate to the active transfer’s exact physical receiving credit', async () => {
  const h = auditedIncomingBridgeFixture(), reviewed = stage(h.current);
  assert.equal(reviewed.status, 200, JSON.stringify(reviewed.body));
  assert.equal(read().transfers.length, 2, 'review must not create a duplicate entry');
  assertExistingUnchanged(h.before, read());
  const reviewedDraft = clone(read().bankReconciliationDrafts[h.current.id]);
  const finalizedResult = await dispatch('/api/expenses/bank-statements/finalize', { role: 'owner', username: 'synthetic-owner', body: { draftId: h.current.id, deferClosingBalance: true } });
  assert.equal(finalizedResult.status, 200, JSON.stringify(finalizedResult.body));
  const after = read(); assert.equal(after.transfers.length, 3);
  const made = after.transfers.find(item => item.reconciliationDraft === h.current.id);
  assert.ok(made); assert.equal(made.bankReference, currentRef); assert.equal(made.date, date); assert.equal(made.amount, amount);
  assert.equal(after.transfers.find(item => item.id === 'TR-00049').accountingExcluded, true, 'the Owner exclusion must not be undone');
  assertExistingUnchanged(h.before, after);
  const replay = applyFinalizedInternalTransfers(reviewedDraft, 'synthetic-owner'); assert.equal(replay.created.length, 0);
  const finalizedReplay = await dispatch('/api/expenses/bank-statements/finalize', { role: 'owner', body: { draftId: h.current.id, deferClosingBalance: true } });
  assert.equal(finalizedReplay.status, 404); assert.equal(read().transfers.length, 3);
  assertExistingUnchanged(h.before, read());
});

for (const [label, corrupt] of [
  ['missing immutable original creation audit', (store) => { store.auditLog = store.auditLog.filter(item => item.subjectId !== 'TR-00049'); }],
  ['audit has the wrong account route', (store, fixture) => { fixture.audit.after.fromAccount = 'ICICI Bank 0992'; }],
  ['audit has the wrong date', (store, fixture) => { fixture.audit.after.date = '2098-08-29'; }],
  ['audit has the wrong amount', (store, fixture) => { fixture.audit.after.amount = 1999; }],
  ['audit is not a recognized bank creation action', (store, fixture) => { fixture.audit.action = 'MANUAL_TRANSFER_EDITED'; }],
  ['audit original contradicts retained source narration', (store, fixture) => { fixture.audit.after.bankReconciliationEvidence.description = receivedNarration('666666666666'); }],
  ['receiving finalized link and stored bank narration disagree', (store, fixture) => { fixture.row.bank.description = receivedNarration('666666666666'); }],
  ['two physical receiving rows have the same received UTR', (store, fixture) => { const duplicate = clone(fixture.stored); duplicate.id = 'BTX-DUPLICATE-RECEIVED'; fixture.book.transactions[duplicate.id] = duplicate; }],
  ['active representative has the wrong source date', (store) => { store.transfers.find(item => item.id === 'TR-00046').date = '2098-08-29'; }],
  ['active representative has the wrong principal amount', (store) => { store.transfers.find(item => item.id === 'TR-00046').amount = 1999; }],
  ['active representative has no source-bank proof', (store) => { delete store.bankStatements[bookKey(sourceAccount)]; }]
]) test('an audit-bound excluded opposite-bank event cannot authorize a new transfer with ' + label, () => {
  const h = auditedIncomingBridgeFixture(corrupt), result = stage(h.current);
  assert.equal(result.status, 409, JSON.stringify(result.body)); assert.deepEqual(read(), h.before);
});

test('one verified active two-bank representative may bridge its excluded duplicate without restoring either record', () => {
  const h = bridgeFixture(), result = stage(h.current);
  assert.equal(result.status, 200, JSON.stringify(result.body));
  assertExistingUnchanged(h.before, read());
  assert.equal(read().transfers.find(item => item.id === 'TR-00049').accountingExcluded, true);
  const applied = applyFinalizedInternalTransfers(read().bankReconciliationDrafts[h.current.id], 'prashant');
  assert.equal(applied.created.length, 1); assert.equal(read().transfers.length, 3);
  assertExistingUnchanged(h.before, read());
});

test('an excluded opposite-bank event cannot be bridged through an active transfer on another account route', () => {
  const h = bridgeFixture(store => { store.transfers.find(item => item.id === 'TR-00046').fromAccount = 'ICICI Bank 0992'; });
  const result = stage(h.current); assert.equal(result.status, 409, JSON.stringify(result.body)); assert.deepEqual(read(), h.before);
});

test('a representative with the same current-bank UTR cannot clear an excluded duplicate blocker', () => {
  const h = bridgeFixture(store => {
    const book = store.bankStatements[bookKey(sourceAccount)], bank = Object.values(book.transactions)[0];
    bank.reference = currentRef; book.imports[0].reconciliationRows[0].bank.reference = currentRef;
  });
  const result = stage(h.current); assert.equal(result.status, 409, JSON.stringify(result.body)); assert.deepEqual(read(), h.before);
});

test('two independently verified active representatives leave an excluded duplicate ambiguous', () => {
  const h = bridgeFixture(store => {
    const sourceRecord = store.bankStatements[bookKey(sourceAccount)].imports[0];
    const receivingRecord = store.bankStatements[bookKey(destinationAccount)].imports[0], receivingBank = receivingRecord.reconciliationRows[0].bank;
    const active = clone(store.transfers.find(item => item.id === 'TR-00046'));
    active.id = 'TR-00045'; delete active.reconciliationDraft; delete active.bankRowId;
    const sourceCopy = clone(sourceRecord); sourceCopy.id = 'BST-SECOND-ACTIVE-SOURCE';
    sourceCopy.reconciliationRows[0].linkedRecordIds = [active.id]; sourceCopy.reconciliationRows[0].ledger.id = active.id;
    store.bankStatements[bookKey(sourceAccount)].imports.push(sourceCopy);
    active.bankReconciliationEvidence = { recordId: receivingRecord.id, rowId: 'bank-0', reference: receivingBank.reference, description: receivingBank.description };
    store.transfers.push(active);
    store.auditLog.push({ action: 'CREATED_FROM_BANK_RECONCILIATION', subjectType: 'transfer', subjectId: active.id, after: clone(active) });
  });
  const result = stage(h.current); assert.equal(result.status, 409, JSON.stringify(result.body)); assert.deepEqual(read(), h.before);
});

test('a different excluded receiving event cannot be treated as the active transfer’s duplicate', () => {
  const h = bridgeFixture(store => {
    const book = store.bankStatements[bookKey(destinationAccount)], old = book.imports[0], row = clone(old.reconciliationRows[0]);
    row.bank.reference = '666666666666'; row.bank.description = 'UPI/666666666666/SEPARATE RECEIVING EVENT'; row.bank.balance = 12000; row.bank.transactionId = 'BTX-EXCLUDED-DIFFERENT'; row.linkedRecordIds = ['TR-00049']; row.ledger.id = 'TR-00049';
    book.transactions[row.bank.transactionId] = { ...clone(row.bank), id: row.bank.transactionId };
    const record = { ...clone(old), id: 'BST-EXCLUDED-DIFFERENT', draftId: 'BRD-EXCLUDED-DIFFERENT', reconciliationRows: [row] }; book.imports.push(record);
    store.transfers.find(item => item.id === 'TR-00049').bankReconciliationEvidence = { recordId: record.id, rowId: 'bank-0', reference: row.bank.reference, description: row.bank.description };
  });
  const result = stage(h.current); assert.equal(result.status, 409, JSON.stringify(result.body)); assert.deepEqual(read(), h.before);
});

for (const [label, corrupt] of [
  ['stored transaction and linked history disagree', (item, store) => { Object.values(store.bankStatements[bookKey(item.account)].transactions)[0].reference = '111111111111'; }],
  ['duplicate stored physical bank rows', (item, store) => { const book = store.bankStatements[bookKey(item.account)], tx = clone(Object.values(book.transactions)[0]); tx.id = 'BTX-AMBIGUOUS'; book.transactions[tx.id] = tx; }],
  ['repeated stored UTR with a different bank balance', (item, store) => { const book = store.bankStatements[bookKey(item.account)], tx = clone(Object.values(book.transactions)[0]); tx.id = 'BTX-REPEATED-UTR'; tx.balance = 8000; book.transactions[tx.id] = tx; }],
  ['history amount no longer agrees', (item, store) => { const row = store.bankStatements[bookKey(item.account)].imports[0].reconciliationRows[0]; row.bank.debit = 1999; }],
  ['multiple contradictory linked UTRs', (item, store) => { const book = store.bankStatements[bookKey(item.account)], row = clone(book.imports[0].reconciliationRows[0]); row.bank.reference = '111111111111'; row.bank.transactionId = 'BTX-CONFLICT'; book.transactions['BTX-CONFLICT'] = { ...row.bank, id: 'BTX-CONFLICT' }; book.imports.push({ ...clone(book.imports[0]), id: 'BST-CONFLICT', reconciliationRows: [row] }); }]
]) test('contradictory or ambiguous finalized source-bank evidence cannot authorize a new transfer: ' + label, () => {
  const item = finalized(), h = reset({ existing: [item] });
  const modified = read(); corrupt(item, modified); write(modified);
  assert.equal(invoke('/api/expenses/config', { method: 'GET', role: 'owner' }).status, 200);
  const before = read(), result = stage(h.current);
  assert.equal(result.status, 409, JSON.stringify(result.body)); assert.deepEqual(read(), before);
});

test('an overlapping finalized copy of the same physical transaction ID is evidence, not another transfer', () => {
  const item = finalized();
  item.book.imports.push({ ...clone(item.book.imports[0]), id: 'BST-CARRIED-COPY' });
  const h = reset({ existing: [item] }), result = stage(h.current);
  assert.equal(result.status, 200, JSON.stringify(result.body)); assertExistingUnchanged(h.before, read());
  assert.equal(read().transfers.length, 1, 'only a decision was saved; the copied history did not create money');
});

for (const [label, corrupt] of [
  ['conflicting UTR', row => { row.bank.reference = '111111111111'; }],
  ['conflicting bank balance', row => { row.bank.balance = 8000; }],
  ['excluded decision', row => { row.decision = 'Excluded after Owner review'; }],
  ['reversal pair decision', row => { row.resolutionAction = 'bank_reversal_pair'; }],
  ['raw bank reversal marker without an action', row => { row.bank.reversal = true; }],
  ['another ledger claim', row => { row.linkedRecordIds.push('TR-OTHER-PHYSICAL-CLAIM'); }],
  ['legacy foreign ledger claim without linked IDs', row => { row.linkedRecordIds = []; row.ledger.id = 'TR-OTHER-PHYSICAL-CLAIM'; }],
  ['conflicting bank amount', row => { row.bank.debit = 1999; }],
  ['conflicting bank date', row => { row.bank.date = '2098-08-29'; }]
]) test('every finalized copy of a bank transaction ID must agree before it proves identity: ' + label, () => {
  const item = finalized(), copy = clone(item.book.imports[0]); copy.id = 'BST-CONTRADICTORY-CARRIED-COPY';
  corrupt(copy.reconciliationRows[0]); item.book.imports.push(copy);
  const h = reset({ existing: [item] }), result = stage(h.current);
  assert.equal(result.status, 409, JSON.stringify(result.body));
  assert.deepEqual(read(), h.before, 'a contradictory carried copy must not change any financial record or reconciliation decision');
});

test('full Owner finalization of a proven distinct transfer and every replay preserve the old payment and history', async () => {
  const h = reset({ existing: [finalized()] }), reviewed = stage(h.current);
  assert.equal(reviewed.status, 200, JSON.stringify(reviewed.body));
  const reviewedDraft = clone(read().bankReconciliationDrafts[h.current.id]);
  const finalizedResult = await dispatch('/api/expenses/bank-statements/finalize', { role: 'owner', username: 'synthetic-owner', body: { draftId: h.current.id, deferClosingBalance: true } });
  assert.equal(finalizedResult.status, 200, JSON.stringify(finalizedResult.body));
  const after = read(); assert.equal(after.transfers.length, 2);
  assertExistingUnchanged(h.before, after);
  const actual = after.transfers.find(item => item.reconciliationDraft === h.current.id); assert.ok(actual);
  assert.equal(actual.bankReference, currentRef); assert.equal(actual.amount, amount); assert.equal(actual.date, date);
  const replayed = applyFinalizedInternalTransfers(reviewedDraft, 'synthetic-owner'); assert.equal(replayed.created.length, 0);
  const replay = await dispatch('/api/expenses/bank-statements/finalize', { role: 'owner', body: { draftId: h.current.id, deferClosingBalance: true } });
  assert.equal(replay.status, 404);
  assert.equal(read().transfers.length, 2); assertExistingUnchanged(h.before, read());
});

test('a legacy later twin decision remains blocked deterministically while the earlier same-bank decision posts once', () => {
  const earlier = draft({ id: 'BRD-EARLIER', createdAt: date + 'T09:00:00.000Z' }); earlier.resolutions['bank-0'] = resolution(earlier);
  const current = draft(); current.resolutions['bank-0'] = resolution(current);
  const h = reset({ current, otherDrafts: [earlier] });
  const later = applyFinalizedInternalTransfers(h.current, 'prashant'); assert.equal(later.created.length, 0); assert.equal(later.blocked.length, 1);
  const first = applyFinalizedInternalTransfers(read().bankReconciliationDrafts[earlier.id], 'prashant'); assert.equal(first.created.length, 1);
  const second = applyFinalizedInternalTransfers(read().bankReconciliationDrafts[earlier.id], 'prashant'); assert.equal(second.created.length, 0);
  const last = applyFinalizedInternalTransfers(read().bankReconciliationDrafts[current.id], 'prashant'); assert.equal(last.created.length, 0);
  assert.equal(read().transfers.length, 1); assertExistingUnchanged(h.before, read());
});

test('distinct references do not widen PERSONAL accounting permissions', () => {
  const h = reset({ existing: [finalized()] });
  const result = invoke('/api/expenses/bank-statements/resolve', { permissions: {}, body: { draftId: h.current.id, rowId: 'bank-0', ...resolution(h.current) } });
  assert.equal(result.status, 403); assert.deepEqual(read(), h.before);
});

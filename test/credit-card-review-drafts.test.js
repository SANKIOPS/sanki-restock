'use strict';

// Synthetic, isolated accounting stores: never use production statements.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'sanki-card-review-drafts-'));
process.env.DATA_PATH = path.join(temp, 'data.json');
fs.writeFileSync(path.join(temp, 'expenses.json'), '{}');
const { router } = require('../modules/credit-cards');
const expenseModule = require('../modules/expenses');
const drafts = require('../modules/credit-card-review-drafts');
test.after(() => fs.rmSync(temp, { recursive: true, force: true }));
const cardsFile = path.join(temp, 'credit-cards.json'), expensesFile = path.join(temp, 'expenses.json');
const reviewPath = '/api/expenses/credit-cards/statements/:id/review';
const discardPath = '/api/expenses/credit-cards/statements/:id/discard-review';
const owner = { username: 'review-tester', role: 'owner', roles: ['owner'] };
const reason = 'Discard mistaken draft classifications and review the retained original statement again.';
function invoke(routePath, { body = {}, id = 'CCS-TEST', user = owner } = {}) {
  const layer = router.stack.find(item => item.route && item.route.path === routePath && item.route.methods.post);
  assert.ok(layer, 'route exists');
  let status = 200, result;
  layer.route.stack[0].handle({ body, params: { id }, user }, { status(code) { status = code; return this; }, json(value) { result = value; return this; } });
  return { status, body: result };
}
function read(file) { return JSON.parse(fs.readFileSync(file, 'utf8')); }
function write(file, value) { fs.writeFileSync(file, JSON.stringify(value)); }
function snapshot() { return [cardsFile, expensesFile].map(file => fs.readFileSync(file, 'utf8')); }
function fixture() {
  for (const file of ['salary.json', 'salary-samast.json', 'incentives.json', 'procurement.json', '.bank-authority-transaction.json']) if (fs.existsSync(path.join(temp, file))) fs.unlinkSync(path.join(temp, file));
  const card = { id: 'CC-TEST', name: 'HDFC', last4: '7376', issuingBank: 'HDFC', openingOutstanding: 1432.12, ownerOnly: false };
  const base = { id: 'CCR-A', date: '2026-10-20', postingDate: '2026-10-21', narration: 'FACEBOOK ADS REF SOURCE001', merchant: 'Edited merchant', reference: 'SOURCE001', amount: 150.25, debit: 150.25, credit: 0, balance: 8123.55, classification: 'fee', type: 'marketing', category: 'IG Meta', suggestedCategory: 'Legacy suggestion', suggestedRule: 'facebook', nature: 'SANKI', channel: 'Both', claimant: 'Tester', bill: 'review-only-proof', confirmed: true, duplicateResolution: '', linkedExpenseId: '', duplicateWarnings: [], allocations: [], merchantAutofill: { version: 1, touchedFields: drafts.FIELDS.slice() } };
  const statement = { id: 'CCS-TEST', cardId: card.id, status: 'review', reviewRevision: 3, statementSourceOfTruth: true, originalName: 'retained-original.pdf', storedName: 'retained-original.pdf', fileHash: 'a'.repeat(64), mimeType: 'application/pdf', uploadedBy: 'Uploader', uploadedAt: '2026-10-21T10:00:00Z', periodFrom: '2026-10-20', periodTo: '2026-10-20', summary: { opening: 8123.55, debit: 150.25 }, rows: [base, { ...base, id: 'CCR-B', narration: 'MERCHANT REFUND SOURCE002', reference: 'SOURCE002', amount: 25, debit: 0, credit: 25, classification: 'refund', balance: 8098.55 }] };
  const cards = { cards: { [card.id]: card, 'CC-OTHER': { id: 'CC-OTHER', name: 'Other', last4: '8888' } }, statements: { [statement.id]: statement, 'CCS-OTHER': { id: 'CCS-OTHER', cardId: 'CC-OTHER', status: 'review', rows: [] } }, merchantRules: { facebook: { category: 'DO NOT CHANGE RULES' } }, payments: [{ id: 'CCP-OTHER', cardId: 'CC-OTHER', amount: 42.22 }], audit: [{ id: 'CCA-000001', action: 'STATEMENT_REVIEWED', subjectId: statement.id }], auditSeq: 1, cardSeq: 9, statementSeq: 12, paymentSeq: 4 };
  const expenses = { expenses: {}, transfers: [], receipts: [], reconciliationExpenses: [], bankStatements: {}, bankReconciliationDrafts: {}, bankReconciliationApprovals: {}, requests: [], auditLog: [], auditSeq: 0, openingBalances: { preserved: 123 }, untouched: { label: 'Keep every original byte in expenses.json' } };
  write(cardsFile, cards); write(expensesFile, expenses);
  fs.writeFileSync(path.join(temp, 'credit-card-statements', statement.storedName), 'SYNTHETIC SOURCE PDF - NOT A REAL STATEMENT');
  return { cards, expenses, statement, card };
}
function discard(options = {}) { return invoke(discardPath, { body: { reviewRevision: 3, reason }, ...options }); }
function savedReview(fields = {}, options = {}) { return invoke(reviewPath, { body: { reviewRevision: 3, rows: [{ id: 'CCR-A', classification: 'expense', type: 'marketing', category: 'IG Meta', nature: 'SANKI', channel: 'Both', ...fields }] }, ...options }); }
function rejected(change, pattern, options = {}) {
  const current = fixture(); change(current); write(cardsFile, current.cards); write(expensesFile, current.expenses);
  const before = snapshot(), result = discard(options);
  assert.ok(result.status >= 400, result.body && result.body.error);
  if (pattern) assert.match(result.body.error, pattern);
  assert.deepEqual(snapshot(), before, 'denied discard never writes either accounting store or audit');
  return result;
}

test('discard resets only draft review choices, preserving original evidence, rules and every unrelated record', () => {
  const original = fixture(), oldExpenses = fs.readFileSync(expensesFile, 'utf8'), pdf = fs.readFileSync(path.join(temp, 'credit-card-statements', original.statement.storedName));
  const result = discard();
  assert.equal(result.status, 200, result.body && result.body.error);
  const next = read(cardsFile), statement = next.statements['CCS-TEST'];
  assert.equal(statement.status, 'review'); assert.equal(statement.reviewRevision, 4);
  for (const field of ['id', 'cardId', 'originalName', 'storedName', 'fileHash', 'mimeType', 'uploadedBy', 'uploadedAt', 'periodFrom', 'periodTo', 'summary', 'statementSourceOfTruth']) assert.deepEqual(statement[field], original.statement[field], field);
  for (let i = 0; i < statement.rows.length; i++) {
    const row = statement.rows[i], source = original.statement.rows[i];
    for (const field of ['id', 'date', 'postingDate', 'narration', 'reference', 'amount', 'debit', 'credit', 'balance']) assert.equal(row[field], source[field], field);
    for (const field of ['type', 'category', 'nature', 'channel', 'claimant', 'bill', 'suggestedCategory', 'suggestedRule', 'settlementSourceId', 'expenseRefundReceiptId']) assert.equal(row[field], '', field);
    assert.equal(row.confirmed, false); assert.deepEqual(row.merchantAutofill, { version: 1, touchedFields: [] });
  }
  assert.equal(statement.rows[0].classification, 'expense'); assert.equal(statement.rows[0].merchant, 'Facebook');
  assert.equal(statement.rows[1].classification, 'refund');
  for (const key of ['cards', 'merchantRules', 'payments', 'cardSeq', 'statementSeq', 'paymentSeq']) assert.deepEqual(next[key], original.cards[key], key);
  assert.deepEqual(next.statements['CCS-OTHER'], original.cards.statements['CCS-OTHER']);
  assert.deepEqual(next.audit.slice(0, -1), original.cards.audit);
  const audit = next.audit.at(-1); assert.equal(audit.action, 'STATEMENT_REVIEW_DISCARDED'); assert.equal(audit.by, owner.username); assert.equal(audit.subjectId, statement.id); assert.equal(audit.details.reason, reason);
  assert.deepEqual(audit.details.before, original.statement); assert.deepEqual(audit.details.after, statement);
  assert.equal(next.auditSeq, 2); assert.equal(audit.id, 'CCA-000002');
  assert.equal(fs.readFileSync(expensesFile, 'utf8'), oldExpenses);
  assert.deepEqual(fs.readFileSync(path.join(temp, 'credit-card-statements', statement.storedName)), pdf);
  assert.equal(result.body.statement.canDiscardReview, true);
});

for (const [name, body] of [['omitted revision', { reason }], ['stale revision', { reason, reviewRevision: 2 }], ['string revision', { reason, reviewRevision: '3' }], ['negative revision', { reason, reviewRevision: -1 }], ['missing reason', { reviewRevision: 3 }], ['short reason', { reviewRevision: 3, reason: 'x' }], ['oversized reason', { reviewRevision: 3, reason: 'x'.repeat(2001) }], ['forged reset fields', { reviewRevision: 3, reason, amount: 1 }]]) test('discard rejects ' + name, () => rejected(() => {}, /revision|reason|Supply|Explain/i, { body }));

for (const [name, mutate] of [
  ['finalized status', x => { x.statement.status = 'finalized'; }],
  ['unknown status', x => { x.statement.status = 'pending'; }],
  ['reopened marker', x => { x.statement.reopenedAt = '2026-10-22T10:00:00Z'; }],
  ['reopen reason', x => { x.statement.reopenReason = 'Prior finalize was reopened'; }],
  ['retained finalization audit', x => { x.cards.audit[0].action = 'STATEMENT_FINALIZED'; }],
  ['retained reopen audit', x => { x.cards.audit[0].action = 'STATEMENT_REOPENED'; }],
  ['posted expense', x => { x.expenses.reconciliationExpenses.push({ id: 'CCE-CCS-TEST-CCR-A', creditCardStatementId: 'CCS-TEST' }); }],
  ['dependent receipt', x => { x.expenses.receipts.push({ id: 'RC-TEST', creditCardRowId: 'CCR-A' }); }],
  ['dependent approval request', x => { x.expenses.requests.push({ id: 'RQ-TEST', status: 'pending', meta: { statementId: 'CCS-TEST' } }); }],
  ['bank date override', x => { x.expenses.bankDateOverrides = { 'CCE-CCS-TEST-CCR-A': '2026-10-20' }; }],
  ['bank review link', x => { x.expenses.bankReconciliationDrafts = { DRAFT: { appId: 'CCE-CCS-TEST-CCR-A' } }; }],
  ['finalized card book', x => { x.expenses.bankStatements.HDFC = { account: 'HDFC 7376', reconciledThrough: '2026-10-31' }; }],
  ['pending card-book approval', x => { x.expenses.bankReconciliationApprovals.PENDING = { status: 'pending', account: 'HDFC 7376', from: '2026-10-01', to: '2026-10-31' }; }],
  ['approval corrupt scope', x => { x.expenses.bankReconciliationApprovals.PENDING = { status: 'pending' }; }],
  ['reconciled row', x => { x.statement.rows[0].reconciled = true; }],
  ['locked row', x => { x.statement.rows[0].locked = true; }],
  ['replaced row', x => { x.statement.rows[0].replaces = { statementId: 'OLD', rowId: 'OLDROW' }; }],
  ['duplicate provenance', x => { x.statement.rows[0].duplicateOf = { statementId: 'OLD', rowId: 'OLDROW' }; }],
  ['other-card dependency', x => { x.cards.payments[0].statementId = 'CCS-TEST'; }],
  ['other-statement dependency', x => { x.cards.statements['CCS-OTHER'].rows.push({ id: 'OTHERROW', linkedStatementId: 'CCS-TEST' }); }],
  ['duplicate row identity', x => { x.statement.rows[1].id = x.statement.rows[0].id; }],
  ['inconsistent amount', x => { x.statement.rows[0].amount = 1; }],
  ['invalid source date', x => { x.statement.rows[0].date = '2026-02-30'; }],
  ['invalid ownership allocations', x => { x.statement.rows[0].allocations = {}; }],
  ['invalid audit sequence', x => { x.cards.auditSeq = 0; }],
  ['invalid protection evidence', x => { x.expenses.bankStatements = []; }]
]) test('discard fails closed for ' + name, () => rejected(mutate));

test('private row and restricted card permissions cannot be bypassed by discard', () => {
  const admin = { username: 'no-private-grant', role: 'admin', roles: ['admin'], permissions: {} };
  rejected(x => { x.card.ownerOnly = true; }, /inaccessible/i, { user: admin });
  rejected(x => { x.statement.rows[0].nature = 'PERSONAL'; }, /private|accessible/i, { user: admin });
  rejected(x => { x.statement.rows[0].payingNature = 'PERSONAL'; x.statement.rows[0].payingAccount = 'HDFC 7376'; }, /private|accessible/i, { user: admin });
  rejected(() => {}, /inaccessible/i, { user: { username: 'claimant', role: 'staff', roles: ['staff'] } });
});

test('permitted Admin can discard an unposted restricted PERSONAL card review', () => {
  const current = fixture(); current.card.ownerOnly = true; current.statement.rows.forEach(row => { row.nature = 'PERSONAL'; row.channel = ''; }); write(cardsFile, current.cards);
  const result = discard({ user: { username: 'authorized-admin', role: 'admin', roles: ['admin'], permissions: { personalAccounting: true } } });
  assert.equal(result.status, 200, result.body && result.body.error);
});

test('corrupt linked stores and unfinished recovery journal are never overwritten', () => {
  fixture(); fs.writeFileSync(path.join(temp, 'salary.json'), '{corrupt'); let before = snapshot();
  assert.equal(discard().status, 409); assert.deepEqual(snapshot(), before); assert.equal(fs.readFileSync(path.join(temp, 'salary.json'), 'utf8'), '{corrupt');
  fixture(); fs.writeFileSync(path.join(temp, '.bank-authority-transaction.json'), '{pending'); before = snapshot();
  assert.equal(discard().status, 409); assert.deepEqual(snapshot(), before); assert.equal(fs.readFileSync(path.join(temp, '.bank-authority-transaction.json'), 'utf8'), '{pending');
});

test('linked external source evidence blocks discard without clearing that source', () => {
  fixture(); write(path.join(temp, 'salary.json'), { payments: [{ sourceId: 'CCE-CCS-TEST-CCR-A' }] }); const before = snapshot();
  const result = discard(); assert.equal(result.status, 409); assert.match(result.body.error, /linked|source/i); assert.deepEqual(snapshot(), before);
});

test('concurrent accounting evidence change aborts card-review commit', () => {
  fixture(); const originalRead = expenseModule.sourceCorrectionPolicy.read; let calls = 0; const beforeCards = fs.readFileSync(cardsFile, 'utf8');
  expenseModule.sourceCorrectionPolicy.read = () => { if (++calls === 2) { const exp = read(expensesFile); exp.concurrentUserRecord = { keep: true }; write(expensesFile, exp); } return originalRead(); };
  try { const result = discard(); assert.equal(result.status, 409); assert.match(result.body.error, /changed/i); assert.equal(fs.readFileSync(cardsFile, 'utf8'), beforeCards); assert.deepEqual(read(expensesFile).concurrentUserRecord, { keep: true }); }
  finally { expenseModule.sourceCorrectionPolicy.read = originalRead; }
});

test('merchant fill metadata is saved, canonicalized, monotonic and preserved for older clients', () => {
  const current = fixture(); current.statement.rows[0].classification = 'expense'; current.statement.rows[0].merchantAutofill = { version: 1, touchedFields: ['nature'] }; write(cardsFile, current.cards);
  let result = savedReview({ merchantAutofill: { version: 1, touchedFields: ['channel', 'category'] } }); assert.equal(result.status, 200, result.body && result.body.error);
  assert.deepEqual(result.body.statement.rows[0].merchantAutofill, { version: 1, touchedFields: ['category', 'nature', 'channel'] });
  result = savedReview({ merchantAutofill: { version: 1, touchedFields: [] } }, { body: { reviewRevision: 4, rows: [{ id: 'CCR-A', classification: 'expense', nature: 'SANKI', merchantAutofill: { version: 1, touchedFields: [] } }] } });
  assert.equal(result.status, 200); assert.deepEqual(result.body.statement.rows[0].merchantAutofill.touchedFields, ['category', 'nature', 'channel']);
  result = savedReview({}, { body: { reviewRevision: 5, rows: [{ id: 'CCR-A', classification: 'expense', nature: 'SANKI' }] } });
  assert.equal(result.status, 200); assert.deepEqual(result.body.statement.rows[0].merchantAutofill.touchedFields, ['category', 'nature', 'channel']);
});

test('older-client changes and deliberate clears consume only their changed fields once markers exist', () => {
  const current = fixture(); current.statement.rows[0].classification = 'expense'; current.statement.rows[0].merchantAutofill = { version: 1, touchedFields: [] }; write(cardsFile, current.cards);
  const result = savedReview({ category: 'Changed local category', nature: '', channel: 'Website' });
  assert.equal(result.status, 200, result.body && result.body.error);
  assert.deepEqual(result.body.statement.rows[0].merchantAutofill, { version: 1, touchedFields: ['category', 'nature', 'channel'] });
  assert.equal(result.body.statement.rows[0].nature, '');
});

for (const [name, value] of [['wrong version', { version: 2, touchedFields: [] }], ['non-array fields', { version: 1, touchedFields: {} }], ['unknown permission field', { version: 1, touchedFields: ['ownerOnly'] }], ['duplicate field', { version: 1, touchedFields: ['nature', 'nature'] }], ['oversized fields', { version: 1, touchedFields: Array(100).fill('nature') }], ['extra permission key', { version: 1, touchedFields: [], canEdit: true }], ['null metadata', null]]) test('review rejects ' + name + ' metadata atomically', () => {
  fixture(); const before = snapshot(), result = savedReview({ merchantAutofill: value }); assert.equal(result.status, 400); assert.deepEqual(snapshot(), before);
});

test('metadata never grants access to a private allocation or changes amount evidence', () => {
  fixture(); const before = snapshot(), result = savedReview({ nature: 'PERSONAL', amount: 1, merchantAutofill: { version: 1, touchedFields: drafts.FIELDS.slice() } }, { user: { username: 'no-private', role: 'admin', roles: ['admin'], permissions: {} } });
  assert.equal(result.status, 403); assert.deepEqual(snapshot(), before);
});

test('fresh rows from older clients stay compatible without adding cross-statement memory', () => {
  const current = fixture(); delete current.statement.rows[0].merchantAutofill; write(cardsFile, current.cards);
  const result = savedReview(); assert.equal(result.status, 200); assert.equal(Object.hasOwn(result.body.statement.rows[0], 'merchantAutofill'), false);
  assert.deepEqual(read(cardsFile).merchantRules, current.cards.merchantRules);
});

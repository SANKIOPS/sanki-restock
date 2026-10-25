'use strict';

// Synthetic isolated stores only. Duplicate imports intentionally share the
// canonical row IDs used by their already-finalized statement and postings.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'sanki-duplicate-card-discard-'));
process.env.DATA_PATH = path.join(temp, 'data.json');
fs.writeFileSync(path.join(temp, 'expenses.json'), '{}');
const { router } = require('../modules/credit-cards');
const expenseModule = require('../modules/expenses');
const accounting = require('../modules/credit-card-accounting');
// Restricted-card permission checks load auth lazily in production. Initialize
// the real dependency against this synthetic DATA_PATH before taking any byte
// baseline, including its normal one-time session-key initialization.
require('../auth');
const cardsFile = path.join(temp, 'credit-cards.json');
const expensesFile = path.join(temp, 'expenses.json');
const discardPath = '/api/expenses/credit-cards/statements/:id/discard-review';
const owner = { username: 'duplicate-review-owner', role: 'owner', roles: ['owner'] };
const admin = { username: 'duplicate-review-admin', role: 'admin', roles: ['admin'], permissions: {} };
const reason = 'Discard only the redundant statement review while preserving all original source postings.';
const clone = value => JSON.parse(JSON.stringify(value));
const read = file => JSON.parse(fs.readFileSync(file, 'utf8'));
const write = (file, value) => fs.writeFileSync(file, JSON.stringify(value));
test.after(() => fs.rmSync(temp, { recursive: true, force: true }));

function invoke({ body = { reviewRevision: 7, reason }, user = owner, id = 'CCS-REVIEW' } = {}) {
  const route = router.stack.find(layer => layer.route && layer.route.path === discardPath && layer.route.methods.post);
  assert.ok(route, 'the actual protected discard route is registered');
  let status = 200, result;
  route.route.stack[0].handle({ body, params: { id }, user }, {
    status(code) { status = code; return this; },
    json(value) { result = value; return this; }
  });
  return { status, body: result };
}

function bytes(directory = temp, prefix = '') {
  const result = {};
  for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
    const relative = prefix + entry.name;
    if (entry.isDirectory()) Object.assign(result, bytes(path.join(directory, entry.name), relative + '/'));
    else result[relative] = fs.readFileSync(path.join(directory, entry.name)).toString('base64');
  }
  return result;
}

function fixture({ posted = true } = {}) {
  for (const name of ['salary.json', 'salary-samast.json', 'incentives.json', 'procurement.json', '.bank-authority-transaction.json']) {
    const file = path.join(temp, name);
    if (fs.existsSync(file)) fs.unlinkSync(file);
  }
  const card = { id: 'CC-PRIMARY', name: 'Synthetic Card', issuingBank: 'Synthetic Bank', last4: '4321', openingOutstanding: 34.56, ownerOnly: false };
  const debit = { id: 'CCR-SHARED-A', date: '2026-06-12', postingDate: '2026-06-13', narration: 'SYNTHETIC MERCHANT PURCHASE REFERENCE A', reference: 'SYNTH-A', merchant: 'Synthetic merchant', amount: 123.45, debit: 123.45, credit: 0, balance: 158.01, classification: 'expense', type: 'running', category: 'Synthetic purchases', nature: 'SANKI', channel: 'Both', confirmed: true, duplicateResolution: '', allocations: [], duplicateWarnings: [] };
  const credit = { ...debit, id: 'CCR-SHARED-B', date: '2026-06-14', postingDate: '2026-06-15', narration: 'SYNTHETIC MERCHANT REFUND REFERENCE B', reference: 'SYNTH-B', amount: 4.56, debit: 0, credit: 4.56, balance: 153.45, classification: 'refund' };
  const source = { id: 'CCS-SOURCE', cardId: card.id, kind: 'billed', status: 'finalized', reviewRevision: 2, statementSourceOfTruth: true, originalName: 'synthetic-source.pdf', storedName: 'synthetic-source.pdf', fileHash: '1'.repeat(64), mimeType: 'application/pdf', uploadedAt: '2026-06-16T09:00:00.000Z', uploadedBy: 'synthetic-uploader', periodFrom: debit.date, periodTo: credit.date, finalizedAt: '2026-06-17T10:00:00.000Z', finalizedBy: owner.username, rows: [debit, credit] };
  const review = { id: 'CCS-REVIEW', cardId: card.id, kind: 'billed', status: 'review', reviewRevision: 7, statementSourceOfTruth: true, originalName: 'synthetic-duplicate.pdf', storedName: 'synthetic-duplicate.pdf', fileHash: '2'.repeat(64), mimeType: 'application/pdf', uploadedAt: '2026-06-18T09:00:00.000Z', uploadedBy: 'synthetic-uploader', periodFrom: debit.date, periodTo: credit.date, summary: { debit: debit.amount, credit: credit.amount }, rows: source.rows.map(row => ({ ...clone(row), merchant: 'Local merchant choice', type: 'marketing', category: 'Local draft classification', confirmed: false, duplicateResolution: '', duplicateOf: { statementId: source.id, rowId: row.id }, merchantAutofill: { version: 1, touchedFields: ['classification', 'category'] } })) };
  const cards = { cards: { [card.id]: card, 'CC-OTHER': { id: 'CC-OTHER', name: 'Unrelated Card', last4: '8765', ownerOnly: false } }, statements: { [source.id]: source, [review.id]: review }, payments: [{ id: 'CCP-UNRELATED', cardId: 'CC-OTHER', amount: 12.34, account: 'Unrelated account' }], merchantRules: { synthetic: { pattern: 'synthetic', category: 'Keep original rules' } }, audit: [{ id: 'CCA-000001', action: 'STATEMENT_FINALIZED', subjectId: source.id, by: owner.username, at: source.finalizedAt, details: { rows: source.rows.length } }, { id: 'CCA-000002', action: 'STATEMENT_UPLOADED', subjectId: review.id, by: 'synthetic-uploader', at: review.uploadedAt, details: { rows: review.rows.length } }], auditSeq: 2, cardSeq: 2, statementSeq: 2, paymentSeq: 1 };
  const expenses = { expenses: {}, transfers: [], receipts: [], reconciliationExpenses: [], bankStatements: {}, bankDateOverrides: {}, bankReconciliationLinks: {}, bankReconciliationDrafts: {}, bankReconciliationApprovals: {}, requests: [], auditLog: [{ id: 'AUD-000001', action: 'SYNTHETIC_PRESERVED', details: { unchanged: true } }], auditSeq: 1, openingBalances: { untouched: 31.21 }, marker: 'Preserve all expense-store bytes and records' };
  if (posted) accounting.syncPostings(cards, expenses);
  write(cardsFile, cards); write(expensesFile, expenses);
  fs.writeFileSync(path.join(temp, 'credit-card-statements', source.storedName), 'SYNTHETIC SOURCE EVIDENCE - NOT A REAL PDF');
  fs.writeFileSync(path.join(temp, 'credit-card-statements', review.storedName), 'SYNTHETIC DUPLICATE EVIDENCE - NOT A REAL PDF');
  return { card, cards, source, review, expenses };
}

function rejected(mutate, options = {}) {
  const state = fixture(); mutate(state);
  write(cardsFile, state.cards); write(expensesFile, state.expenses);
  const before = bytes(), result = invoke(options);
  assert.ok(result.status >= 400, 'unsafe discard must be rejected: ' + JSON.stringify(result.body));
  assert.equal(result.body.success, false);
  assert.deepEqual(bytes(), before, 'a rejected duplicate discard preserves every source file, accounting byte, audit and recovery marker');
  return result;
}

for (const posted of [false, true]) test('discard permits passive duplicate links with ' + (posted ? 'existing canonical-row source postings' : 'no own posted records') + ' and preserves the finalized source', () => {
  const state = fixture({ posted });
  const beforeFiles = bytes(), beforeCards = clone(state.cards), beforeExpenses = fs.readFileSync(expensesFile, 'utf8');
  const postedBefore = clone(accounting.cardExpenseRecords(state.expenses));
  assert.ok(state.review.rows.every(row => !accounting.activeRow(row)));
  if (posted) assert.ok(state.expenses.reconciliationExpenses.every(record => record.creditCardRowId === state.source.rows.find(row => row.id === record.creditCardRowId).id), 'fixture exercises the actual shared raw row ID collision');
  const result = invoke(); assert.equal(result.status, 200, result.body && result.body.error); assert.equal(result.body.success, true);
  const after = read(cardsFile), next = after.statements[state.review.id];
  assert.equal(next.status, 'review'); assert.equal(next.reviewRevision, 8);
  assert.deepEqual(after.statements[state.source.id], beforeCards.statements[state.source.id], 'the finalized statement and classifications are exactly unchanged');
  assert.equal(fs.readFileSync(expensesFile, 'utf8'), beforeExpenses, 'posted transactions and expense audit are byte-identical');
  assert.deepEqual(accounting.cardExpenseRecords(read(expensesFile)), postedBefore);
  for (const key of ['cards', 'payments', 'merchantRules', 'cardSeq', 'statementSeq', 'paymentSeq']) assert.deepEqual(after[key], beforeCards[key], key);
  assert.deepEqual(after.audit.slice(0, -1), beforeCards.audit); assert.equal(after.audit.length, beforeCards.audit.length + 1);
  const audit = after.audit.at(-1);
  assert.equal(audit.action, 'STATEMENT_REVIEW_DISCARDED'); assert.equal(audit.subjectId, state.review.id);
  assert.equal(audit.details.reason, reason); assert.deepEqual(audit.details.before, beforeCards.statements[state.review.id]); assert.deepEqual(audit.details.after, next); assert.equal(after.auditSeq, 3);
  for (const [index, row] of next.rows.entries()) {
    assert.deepEqual(row.duplicateOf, state.review.rows[index].duplicateOf, 'reset retains the verified canonical duplicate pointer');
    assert.equal(accounting.activeRow(row), false, 'reset must never turn an overlap into a second posting');
    for (const key of ['id', 'date', 'postingDate', 'narration', 'reference', 'amount', 'debit', 'credit', 'balance']) assert.equal(row[key], state.review.rows[index][key], key);
    assert.equal(row.confirmed, false); assert.equal(row.category, ''); assert.equal(row.nature, ''); assert.deepEqual(row.merchantAutofill, { version: 1, touchedFields: [] });
  }
  const afterFiles = bytes(); delete beforeFiles['credit-cards.json']; delete afterFiles['credit-cards.json'];
  assert.deepEqual(afterFiles, beforeFiles, 'both uploads and all other files remain byte-identical');
  assert.equal(result.body.statement.canDiscardReview, true, 'advisory state agrees with the protected route after reset');
});

test('authorized Admin may discard the redundant business review without changing source postings', () => {
  fixture(); const before = fs.readFileSync(expensesFile, 'utf8');
  const result = invoke({ user: admin }); assert.equal(result.status, 200, result.body && result.body.error); assert.equal(fs.readFileSync(expensesFile, 'utf8'), before);
});

test('qualified original-source bank links are preserved without being mistaken for a dependency on the duplicate review', () => {
  const state = fixture(), sourceId = state.expenses.reconciliationExpenses[0].id;
  state.expenses.bankDateOverrides[sourceId] = { bankDate: state.source.rows[0].date, originalDate: state.source.rows[0].date };
  state.expenses.bankReconciliationLinks[sourceId] = { appId: sourceId, draftId: 'SYNTHETIC-SOURCE-DRAFT' };
  write(expensesFile, state.expenses); const before = fs.readFileSync(expensesFile, 'utf8');
  const result = invoke(); assert.equal(result.status, 200, result.body && result.body.error);
  assert.equal(fs.readFileSync(expensesFile, 'utf8'), before, 'the original source-bank evidence is never detached or edited');
});

for (const [name, mutate] of [
  ['missing target statement', s => { delete s.cards.statements[s.source.id]; }],
  ['same-statement pointer', s => { s.review.rows[0].duplicateOf.statementId = s.review.id; }],
  ['wrong card source', s => { s.source.cardId = 'CC-OTHER'; }],
  ['source still in review', s => { s.source.status = 'review'; }],
  ['missing target row', s => { s.source.rows.shift(); }],
  ['ambiguous repeated target row identity', s => { s.source.rows.push(clone(s.source.rows[0])); }],
  ['malformed pointer string', s => { s.review.rows[0].duplicateOf = s.source.id + '/' + s.source.rows[0].id; }],
  ['pointer with extra untrusted fields', s => { s.review.rows[0].duplicateOf.ownerOnly = false; }],
  ['incomplete pointer', s => { delete s.review.rows[0].duplicateOf.rowId; }],
  ['reused target for two review rows', s => { const another = clone(s.review.rows[0]); another.id = 'CCR-DIFFERENT-REVIEW-ROW'; s.review.rows[1] = another; }],
  ['source row is itself a duplicate', s => { s.source.rows[0].duplicateOf = { statementId: 'CCS-OLDER', rowId: 'CCR-OLDER' }; }],
  ['source row is superseded', s => { s.source.rows[0].supersededBy = { statementId: 'CCS-NEWER', rowId: 'CCR-NEWER' }; }],
  ['source row is a replacement', s => { s.source.rows[0].replaces = { statementId: 'CCS-OLDER', rowId: 'CCR-OLDER' }; }],
  ['source row has inactive resolution', s => { s.source.rows[0].duplicateResolution = 'duplicate'; }],
  ['draft row has an explicit non-passive resolution', s => { s.review.rows[0].duplicateResolution = 'replace'; }],
  ['draft row has replacement provenance', s => { s.review.rows[0].replaces = { statementId: s.source.id, rowId: s.source.rows[0].id }; }],
  ['draft row has incoming supersession evidence', s => { s.review.rows[0].supersededBy = { statementId: 'CCS-DEPENDENT', rowId: 'CCR-DEPENDENT' }; }],
  ['draft row has statement ownership link', s => { s.review.rows[0].creditCardStatementId = s.source.id; }],
  ['mutated transaction date', s => { s.review.rows[0].date = '2026-06-11'; }],
  ['mutated posting date', s => { s.review.rows[0].postingDate = '2026-06-14'; }],
  ['mutated exact reference', s => { s.review.rows[0].reference = 'DIFFERENT-REFERENCE'; }],
  ['mutated narration', s => { s.review.rows[0].narration += ' CHANGED'; }],
  ['mutated source amount', s => { Object.assign(s.source.rows[0], { amount: 120.45, debit: 120.45 }); }],
  ['mutated review amount', s => { Object.assign(s.review.rows[0], { amount: 120.45, debit: 120.45 }); }],
  ['reversed review direction', s => { Object.assign(s.review.rows[0], { credit: s.review.rows[0].debit, debit: 0 }); }],
  ['review previously finalized', s => { s.review.finalizedAt = '2026-06-19T00:00:00.000Z'; }],
  ['review reopened from finalized state', s => { s.review.reopenedAt = '2026-06-19T00:00:00.000Z'; }],
  ['review retained finalization audit', s => { s.cards.audit[1].action = 'STATEMENT_FINALIZED'; }]
]) test('duplicate discard fails closed for ' + name, () => rejected(mutate));

for (const [name, mutate] of [
  ['own posted statement expense', s => { s.expenses.reconciliationExpenses.push({ ...clone(s.expenses.reconciliationExpenses[0]), id: 'CCE-CCS-REVIEW-CCR-SHARED-A', creditCardStatementId: s.review.id }); }],
  ['source posting has forged ownership', s => { s.expenses.reconciliationExpenses[0].creditCardStatementId = s.review.id; }],
  ['source posting card mismatch', s => { s.expenses.reconciliationExpenses[0].creditCardId = 'CC-OTHER'; }],
  ['source posting identity mismatch', s => { s.expenses.reconciliationExpenses[0].id = 'CCE-NOT-THE-EXACT-SOURCE'; }],
  ['ambiguous duplicate canonical source posting', s => { s.expenses.reconciliationExpenses.push(clone(s.expenses.reconciliationExpenses[0])); }],
  ['source posting amount mismatch', s => { s.expenses.reconciliationExpenses[0].amount += 1; }],
  ['source posting reference to review', s => { s.expenses.reconciliationExpenses[0].linkedStatementId = s.review.id; }],
  ['source posting nested qualified review reference', s => { s.expenses.reconciliationExpenses[0].independentEvidence = { sourceId: 'CCE-CCS-REVIEW-CCR-SHARED-A' }; }],
  ['source posting nested raw-row reference', s => { s.expenses.reconciliationExpenses[0].independentEvidence = { sourceRowId: s.review.rows[0].id }; }],
  ['source posting raw ID for another duplicate row', s => { s.expenses.reconciliationExpenses[0].otherRow = s.review.rows[1].id; }],
  ['receipt refers to review statement', s => { s.expenses.receipts.push({ id: 'RC-DEPENDENT', statementId: s.review.id }); }],
  ['receipt refers to qualified review row', s => { s.expenses.receipts.push({ id: 'RC-DEPENDENT', sourceId: s.review.id + '/' + s.review.rows[0].id }); }],
  ['receipt refers to ambiguous raw row', s => { s.expenses.receipts.push({ id: 'RC-DEPENDENT', creditCardRowId: s.review.rows[0].id }); }],
  ['card payment refers to review', s => { s.cards.payments.push({ id: 'CCP-DEPENDENT', cardId: s.card.id, statementId: s.review.id, amount: 123.45 }); }],
  ['other review has incoming review link', s => { s.cards.statements['CCS-DEPENDENT'] = { id: 'CCS-DEPENDENT', cardId: s.card.id, status: 'review', rows: [{ id: 'CCR-DEPENDENT', linkedStatementId: s.review.id }] }; }],
  ['another statement reuses ambiguous canonical ID', s => { s.cards.statements['CCS-AMBIGUOUS'] = { id: 'CCS-AMBIGUOUS', cardId: s.card.id, status: 'review', rows: [clone(s.source.rows[0])] }; }],
  ['bank date override refers to review posting', s => { s.expenses.bankDateOverrides['CCE-CCS-REVIEW-CCR-SHARED-A'] = { bankDate: '2026-06-12' }; }],
  ['active bank review refers to qualified review row', s => { s.expenses.bankReconciliationDrafts.DEPENDENT = { account: 'Synthetic Card 4321', rows: [{ appId: s.review.id + '/' + s.review.rows[0].id }] }; }],
  ['bank evidence refers to ambiguous raw row', s => { s.expenses.bankReconciliationLinks[s.review.rows[0].id] = { id: 'BANK-EVIDENCE' }; }],
  ['approval request refers to review', s => { s.expenses.requests.push({ id: 'RQ-DEPENDENT', status: 'pending', meta: { statementId: s.review.id } }); }],
  ['approval request refers to ambiguous raw row', s => { s.expenses.requests.push({ id: 'RQ-DEPENDENT', status: 'pending', meta: { sourceRow: s.review.rows[0].id } }); }],
  ['card account period already finalized', s => { s.expenses.bankStatements['Synthetic Card 4321'] = { account: 'Synthetic Card 4321', reconciledThrough: '2026-06-30', imports: [] }; }],
  ['Owner approval covers the card period', s => { s.expenses.bankReconciliationApprovals.PENDING = { status: 'pending', account: 'Synthetic Card 4321', from: '2026-06-01', to: '2026-06-30' }; }]
]) test('duplicate discard retains genuine dependency lock for ' + name, () => rejected(mutate));

test('external accounting source referring to ambiguous raw row blocks duplicate discard and stays byte-identical', () => {
  fixture(); write(path.join(temp, 'salary.json'), { payments: [{ id: 'EXTERNAL-SYNTHETIC', sourceId: 'CCR-SHARED-A' }] });
  const before = bytes(), result = invoke(); assert.equal(result.status, 409); assert.deepEqual(bytes(), before);
});

test('private/restricted source and review access stays protected for an ungranted Admin', () => {
  rejected(s => { s.card.ownerOnly = true; }, { user: admin });
  rejected(s => { s.review.rows[0].nature = 'PERSONAL'; }, { user: admin });
  rejected(s => { s.source.rows[0].nature = 'PERSONAL'; s.expenses.reconciliationExpenses[0].nature = 'PERSONAL'; }, { user: admin });
  rejected(s => { s.source.rows[0].allocations = [{ nature: 'PERSONAL', amount: 123.45 }]; }, { user: admin });
  rejected(() => {}, { user: { username: 'read-only-claimant', role: 'staff', roles: ['staff'] } });
});

for (const [name, body] of [
  ['missing revision', { reason }], ['stale revision', { reviewRevision: 6, reason }],
  ['string revision', { reviewRevision: '7', reason }], ['missing reason', { reviewRevision: 7 }],
  ['too-short reason', { reviewRevision: 7, reason: 'no' }],
  ['forged source field', { reviewRevision: 7, reason, sourceId: 'CCS-SOURCE' }]
]) test('duplicate discard requires ' + name + ' to be corrected without writes', () => rejected(() => {}, { body }));

test('duplicate discard never recovers or overwrites an unfinished accounting journal', () => {
  fixture(); fs.writeFileSync(path.join(temp, '.bank-authority-transaction.json'), '{synthetic unfinished transaction');
  const before = bytes(), result = invoke(); assert.equal(result.status, 409); assert.deepEqual(bytes(), before);
});

test('duplicate discard fails closed on corrupt external accounting evidence', () => {
  fixture(); fs.writeFileSync(path.join(temp, 'salary.json'), '{synthetic corrupt source');
  const before = bytes(), result = invoke(); assert.equal(result.status, 409); assert.deepEqual(bytes(), before);
});

for (const kind of ['expenses', 'cards', 'external']) test('concurrent ' + kind + ' evidence invalidates duplicate-review discard atomically', () => {
  fixture(); if (kind === 'external') write(path.join(temp, 'salary.json'), { preserved: true, payments: [] });
  const originalRead = expenseModule.sourceCorrectionPolicy.read; let reads = 0, concurrentBytes;
  expenseModule.sourceCorrectionPolicy.read = () => {
    if (++reads === 2) {
      const file = kind === 'expenses' ? expensesFile : kind === 'cards' ? cardsFile : path.join(temp, 'salary.json');
      const data = read(file); data.concurrentUserMarker = { keep: true }; write(file, data); concurrentBytes = bytes();
    }
    return originalRead();
  };
  try {
    const result = invoke(); assert.equal(result.status, 409, result.body && result.body.error); assert.match(result.body.error, /changed|conflict|safely/i);
    assert.ok(concurrentBytes, 'test injected a second-reader change');
    assert.deepEqual(bytes(), concurrentBytes, 'concurrent user data remains; discard creates no audit or partial write');
  } finally { expenseModule.sourceCorrectionPolicy.read = originalRead; }
});

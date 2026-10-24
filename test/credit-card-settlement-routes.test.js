'use strict';

// Isolated route regressions: these fixtures never touch production stores.
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'sanki-card-settlement-routes-'));
process.env.DATA_PATH = path.join(temp, 'data.json');
const { router, inferClassification } = require('../modules/credit-cards');
test.after(() => fs.rmSync(temp, { recursive: true, force: true }));
const cardsFile = path.join(temp, 'credit-cards.json');
const expensesFile = path.join(temp, 'expenses.json');
let sequence = 0;

function invoke(method, routePath, { body = {}, params = {}, query = {}, role = 'owner', username = 'settlement-tester' } = {}) {
  const layer = router.stack.find(x => x.route && x.route.path === routePath && x.route.methods[method.toLowerCase()]);
  assert.ok(layer, 'route exists: ' + method + ' ' + routePath);
  let status = 200, result, index = 0;
  const req = { body, params, query, user: { username, role, roles: [role] } };
  const res = { status(n) { status = n; return this; }, json(x) { result = x; return this; }, end() { return this; } };
  const next = () => { const handler = layer.route.stack[index++]; if (handler) handler.handle(req, res, next); };
  next();
  return { status, body: result };
}
function read(file) { return fs.existsSync(file) ? JSON.parse(fs.readFileSync(file, 'utf8')) : {}; }
function snapshot() { return [cardsFile, expensesFile].map(file => fs.existsSync(file) ? fs.readFileSync(file, 'utf8') : null); }
function newCard() {
  sequence++;
  const result = invoke('POST', '/api/expenses/credit-cards', { body: { name: 'Settlement test ' + sequence, last4: String(7000 + sequence), issuingBank: 'ICICI', openingOutstanding: 10000 } });
  assert.equal(result.status, 200);
  return result.body.card;
}
function manual(card, { amount = 100, date = '2026-10-20', classification = 'card_payment', narration = 'PAYMENT RECEIVED THANK YOU', reference = '' } = {}) {
  const result = invoke('POST', '/api/expenses/credit-cards/statements/manual', { body: { cardId: card.id, amount, date, classification, narration, reference } });
  assert.equal(result.status, 200, result.body && result.body.error);
  return result.body.statement;
}
function current(statement, options = {}) {
  const result = invoke('GET', '/api/expenses/credit-cards/statements', { query: { cardId: statement.cardId }, ...options });
  assert.equal(result.status, 200);
  return result.body.statements.find(x => x.id === statement.id);
}
function review(statement, fields, options = {}) {
  const latest = current(statement);
  return invoke('POST', '/api/expenses/credit-cards/statements/:id/review', {
    params: { id: statement.id },
    body: { reviewRevision: latest.reviewRevision || 0, rows: [{ id: statement.rows[0].id, classification: 'card_payment', ...fields }] },
    ...options
  });
}
function finalize(statement, options = {}) { return invoke('POST', '/api/expenses/credit-cards/statements/:id/finalize', { params: { id: statement.id }, body: { reviewRevision: current(statement).reviewRevision || 0 }, ...options }); }
function payment(card, fields = {}, options = {}) {
  return invoke('POST', '/api/expenses/credit-cards/payments', { body: { cardId: card.id, nature: 'SANKI', account: 'Axis Bank 3448', amount: 100, date: '2026-10-20', reference: 'SETTLE-' + card.id, ...fields }, ...options });
}
function candidateFor(statement, paymentId) {
  const candidate = current(statement).rows[0].settlementCandidates.find(x => (x.sourceId || x.id) === 'payment:' + paymentId);
  assert.ok(candidate, 'same-card exact-amount payment is offered');
  return candidate;
}
function linkFields(candidate, extra = {}) {
  return { settlementMode: 'link_existing', settlementSourceId: candidate.sourceId || candidate.id, settlementSourceFingerprint: candidate.fingerprint || candidate.sourceFingerprint, ...extra };
}
const external = { settlementMode: 'external', payingNature: 'SANKI', payingAccount: 'External payer account not maintained in this app', settlementReason: 'Paid from an external untracked source; statement verifies receipt.', settlementReference: 'EXTERNAL-SETTLEMENT' };

test('card-payment review stays pending until a verified settlement source is chosen', () => {
  const card = newCard(), statement = manual(card);
  const saved = review(statement, { settlementMode: 'pending' });
  assert.equal(saved.status, 200);
  const before = snapshot(), rejected = finalize(statement);
  assert.equal(rejected.status, 409);
  assert.match(rejected.body.error, /settlement|payment|source/i);
  assert.deepEqual(snapshot(), before, 'blocked finalization must not change either financial store');
  assert.equal(current(statement).status, 'review');
});

test('an external statement payment reduces only the card liability, never expense totals or a bank balance', () => {
  const card = newCard(), statement = manual(card, { amount: 298799.31 });
  const beforeExpenses = read(expensesFile), beforeTransfers = (beforeExpenses.transfers || []).length;
  const saved = review(statement, { ...external, type: 'marketing', category: 'MARKETING EXPENSE', nature: 'SANKI', channel: 'Both', expenseRefundReceiptId: 'malicious-refund-link' });
  assert.equal(saved.status, 200, saved.body && saved.body.error);
  const row = saved.body.statement.rows[0];
  assert.equal(row.category || '', '', 'expense fields are not retained on settlements');
  assert.equal(row.type || '', '', 'expense type is not retained on settlements');
  assert.equal(row.expenseRefundReceiptId || '', '', 'repayment cannot link to a merchant refund');
  const posted = finalize(statement);
  assert.equal(posted.status, 200, posted.body && posted.body.error);
  assert.equal(posted.body.outstanding, 10000 - 298799.31);
  const afterExpenses = read(expensesFile);
  assert.equal((afterExpenses.transfers || []).length, beforeTransfers, 'statement review/finalization must not fabricate a bank withdrawal');
  assert.equal((afterExpenses.reconciliationExpenses || []).filter(x => x.creditCardStatementId === statement.id).length, 0, 'card repayment is not a negative expense');
});

test('linking an existing card payment adds no second bank transfer and records its verified source', () => {
  const card = newCard(), paid = payment(card);
  assert.equal(paid.status, 200, paid.body && paid.body.error);
  const statement = manual(card), candidate = candidateFor(statement, paid.body.payment.id), before = read(expensesFile);
  assert.equal(review(statement, linkFields(candidate)).status, 200);
  assert.equal(finalize(statement).status, 200);
  const after = read(expensesFile), stored = current(statement).rows[0];
  assert.equal((after.transfers || []).length, (before.transfers || []).length);
  assert.equal(stored.settlementSourceId, 'payment:' + paid.body.payment.id);
  assert.equal(stored.settlementSourceFingerprint, candidate.fingerprint || candidate.sourceFingerprint);
  assert.equal((after.reconciliationExpenses || []).some(x => x.creditCardStatementId === statement.id), false);
});

test('a payment on a different card cannot be linked or leaked as a candidate', () => {
  const first = newCard(), second = newCard(), paid = payment(first);
  assert.equal(paid.status, 200);
  const statement = manual(second), row = current(statement).rows[0], before = snapshot();
  assert.equal(row.settlementCandidates.some(x => (x.sourceId || x.id) === 'payment:' + paid.body.payment.id), false);
  const rejected = review(statement, { settlementMode: 'link_existing', settlementSourceId: 'payment:' + paid.body.payment.id, settlementSourceFingerprint: '0'.repeat(64) });
  assert.ok(rejected.status >= 400);
  assert.deepEqual(snapshot(), before);
});

test('a transfer with a conflicting explicit card ID cannot be linked by matching its display name', () => {
  const card = newCard(), other = newCard(), statement = manual(card), exp = read(expensesFile);
  exp.transfers = exp.transfers || [];
  const transfer = { id: 'TR-WRONG-CARD-' + card.id, classification: 'credit_card_payment', creditCardId: other.id, fromNature: 'SANKI', nature: 'SANKI', fromAccount: 'Axis Bank 3448', toAccount: card.name + ' ' + card.last4, amount: 100, date: '2026-10-20', reference: 'CONFLICT-' + card.id };
  exp.transfers.push(transfer); fs.writeFileSync(expensesFile, JSON.stringify(exp));
  const candidate = current(statement).rows[0].settlementCandidates.find(x => x.id === 'transfer:' + transfer.id);
  assert.equal(!!candidate && candidate.eligible, false, 'explicit card identity takes priority over its display name');
  const before = snapshot(), rejected = review(statement, { settlementMode: 'link_existing', settlementSourceId: 'transfer:' + transfer.id, settlementSourceFingerprint: candidate && candidate.fingerprint || '0'.repeat(64) });
  assert.ok(rejected.status >= 400);
  assert.deepEqual(snapshot(), before);
  assert.ok(review(statement, external).status >= 400, 'an inconsistent recorded transfer cannot be bypassed by an external description');
  assert.deepEqual(snapshot(), before);
});

test('a CCP and its associated transfer must agree on the explicit card ID', () => {
  const card = newCard(), other = newCard(), paid = payment(card); assert.equal(paid.status, 200);
  const statement = manual(card), exp = read(expensesFile), transfer = exp.transfers.find(x => x.creditCardPaymentId === paid.body.payment.id);
  transfer.creditCardId = other.id; fs.writeFileSync(expensesFile, JSON.stringify(exp));
  const candidate = candidateFor(statement, paid.body.payment.id);
  assert.equal(candidate.eligible, false, 'an associated transfer cannot silently belong to a different card');
  const before = snapshot(), rejected = review(statement, linkFields(candidate));
  assert.ok(rejected.status >= 400);
  assert.deepEqual(snapshot(), before);
});

test('a genuine payment candidate prevents an external-source decision from bypassing the duplicate check', () => {
  const card = newCard(); assert.equal(payment(card).status, 200);
  const statement = manual(card), before = snapshot(), rejected = review(statement, external);
  assert.ok(rejected.status >= 400);
  assert.match(rejected.body.error, /existing|link|payment|source/i);
  assert.deepEqual(snapshot(), before);
});

test('same-card exact-amount matching allows at most three days and requires an explanation for a date offset', () => {
  const card = newCard(), paid = payment(card, { date: '2026-10-18' }); assert.equal(paid.status, 200);
  const statement = manual(card), candidate = candidateFor(statement, paid.body.payment.id), before = snapshot();
  const missingReason = review(statement, linkFields(candidate));
  assert.ok(missingReason.status >= 400);
  assert.deepEqual(snapshot(), before);
  assert.equal(review(statement, linkFields(candidate, { settlementReason: 'Payment posted two days after the bank debit.' })).status, 200);
  assert.equal(finalize(statement).status, 200);
  const farCard = newCard(), farPaid = payment(farCard, { date: '2026-10-16' }); assert.equal(farPaid.status, 200);
  const farStatement = manual(farCard);
  assert.equal(current(farStatement).rows[0].settlementCandidates.some(x => (x.sourceId || x.id) === 'payment:' + farPaid.body.payment.id && x.eligible), false);
});

test('one existing payment cannot be reserved by two independent statement reviews', () => {
  const card = newCard(), paid = payment(card); assert.equal(paid.status, 200);
  const first = manual(card), second = manual(card), firstCandidate = candidateFor(first, paid.body.payment.id), secondCandidate = candidateFor(second, paid.body.payment.id);
  assert.equal(review(first, linkFields(firstCandidate)).status, 200);
  const before = snapshot(), rejected = review(second, linkFields(secondCandidate));
  assert.ok(rejected.status >= 400);
  assert.match(rejected.body.error, /already|another|used|linked/i);
  assert.deepEqual(snapshot(), before);
});

test('changing a linked payment after review blocks finalization without partial posting', () => {
  const card = newCard(), paid = payment(card); assert.equal(paid.status, 200);
  const statement = manual(card), candidate = candidateFor(statement, paid.body.payment.id);
  assert.equal(review(statement, linkFields(candidate)).status, 200);
  const cc = read(cardsFile); cc.payments.find(x => x.id === paid.body.payment.id).reference += '-CHANGED';
  fs.writeFileSync(cardsFile, JSON.stringify(cc));
  const before = snapshot(), rejected = finalize(statement);
  assert.equal(rejected.status, 409);
  assert.match(rejected.body.error, /changed|stale|refresh|source/i);
  assert.deepEqual(snapshot(), before);
  assert.equal(current(statement).status, 'review');
});

test('deactivating a linked payment after review also blocks finalization', () => {
  const card = newCard(), paid = payment(card); assert.equal(paid.status, 200);
  const statement = manual(card), candidate = candidateFor(statement, paid.body.payment.id);
  assert.equal(review(statement, linkFields(candidate)).status, 200);
  const cc = read(cardsFile); cc.payments.find(x => x.id === paid.body.payment.id).active = false;
  fs.writeFileSync(cardsFile, JSON.stringify(cc));
  const before = snapshot(), rejected = finalize(statement);
  assert.equal(rejected.status, 409);
  assert.deepEqual(snapshot(), before);
});

test('a stale supplied review revision cannot overwrite a newer settlement decision', () => {
  const card = newCard(), statement = manual(card), initial = current(statement), staleRevision = initial.reviewRevision || 0;
  assert.equal(review(statement, { settlementMode: 'pending' }).status, 200);
  const before = snapshot();
  const rejected = invoke('POST', '/api/expenses/credit-cards/statements/:id/review', { params: { id: statement.id }, body: { reviewRevision: staleRevision, rows: [{ id: statement.rows[0].id, classification: 'card_payment', ...external }] } });
  assert.equal(rejected.status, 409);
  assert.match(rejected.body.error, /changed|refresh|revision|review/i);
  assert.deepEqual(snapshot(), before);
});

test('review and finalization reject omitted or stale review revisions before any mutation', () => {
  const card = newCard(), statement = manual(card), params = { id: statement.id }, before = snapshot();
  assert.equal(invoke('POST', '/api/expenses/credit-cards/statements/:id/review', { params, body: { rows: [{ id: statement.rows[0].id, classification: 'card_payment', ...external }] } }).status, 409);
  assert.deepEqual(snapshot(), before);
  assert.equal(invoke('POST', '/api/expenses/credit-cards/statements/:id/finalize', { params }).status, 409);
  assert.deepEqual(snapshot(), before);
  assert.equal(review(statement, external).status, 200);
  const afterReview = snapshot();
  assert.equal(invoke('POST', '/api/expenses/credit-cards/statements/:id/finalize', { params, body: { reviewRevision: 0 } }).status, 409);
  assert.deepEqual(snapshot(), afterReview);
});

test('review validation is atomic across multiple statement rows', () => {
  const card = newCard(), statement = manual(card);
  const cc = read(cardsFile), saved = cc.statements[statement.id];
  saved.rows.push({ ...saved.rows[0], id: saved.rows[0].id + '-SECOND', amount: 50, debit: 0, credit: 50 });
  fs.writeFileSync(cardsFile, JSON.stringify(cc));
  const before = snapshot();
  const rejected = invoke('POST', '/api/expenses/credit-cards/statements/:id/review', { params: { id: statement.id }, body: { reviewRevision: current(statement).reviewRevision || 0, rows: [{ id: saved.rows[0].id, classification: 'card_payment', ...external }, { id: saved.rows[1].id, classification: 'expense' }] } });
  assert.equal(rejected.status, 400);
  assert.deepEqual(snapshot(), before, 'an invalid later row must not save the earlier edit or audit event');
});

test('unclassified credits may be saved for review but cannot be finalized as an expense offset', () => {
  const card = newCard(), statement = manual(card, { classification: 'unclassified_credit', narration: 'UNEXPLAINED ACCOUNT CREDIT' });
  assert.equal(statement.rows[0].credit, 100);
  assert.equal(statement.rows[0].debit, 0);
  const saved = review(statement, { classification: 'unclassified_credit' });
  assert.equal(saved.status, 200);
  const before = snapshot(), rejected = finalize(statement);
  assert.equal(rejected.status, 409);
  assert.deepEqual(snapshot(), before);
});

test('explicit repayment narration cannot be relabelled as a merchant refund', () => {
  for (const narration of ['INFINITY PAYMENT RECEIVED, THANK YOU', 'AUTOPAY PAYMENT RECEIVED']) {
    const card = newCard(), statement = manual(card, { narration }), before = snapshot();
    const rejected = review(statement, { classification: 'refund', category: 'MARKETING EXPENSE', type: 'marketing', nature: 'SANKI', channel: 'Both' });
    assert.equal(rejected.status, 409);
    assert.match(rejected.body.error, /payment|repayment|settlement/i);
    assert.deepEqual(snapshot(), before);
  }
});

test('legacy misclassified settlement credits are blocked again at finalization', () => {
  const card = newCard(), statement = manual(card), cc = read(cardsFile), row = cc.statements[statement.id].rows[0];
  Object.assign(row, { classification: 'refund', category: 'MARKETING EXPENSE', type: 'marketing', nature: 'SANKI', channel: 'Both', confirmed: true });
  fs.writeFileSync(cardsFile, JSON.stringify(cc));
  const before = snapshot(), rejected = finalize(statement);
  assert.equal(rejected.status, 409);
  assert.match(rejected.body.error, /payment|repayment|settlement/i);
  assert.deepEqual(snapshot(), before, 'the legacy bad classification must not become a negative expense');
});

test('merchant refunds remain negative expenses while known settlements and unknown credits stay distinct', () => {
  assert.equal(inferClassification({ description: 'INFINITY PAYMENT RECEIVED, THANK YOU', credit: 100 }), 'card_payment');
  assert.equal(inferClassification({ description: 'LinkedIn refund', credit: 2 }), 'refund');
  assert.equal(inferClassification({ description: 'UNEXPLAINED CREDIT', credit: 100 }), 'unclassified_credit');
  const card = newCard(), statement = manual(card, { classification: 'refund', narration: 'LinkedIn merchant refund', amount: 2 });
  assert.equal(review(statement, { classification: 'refund', category: 'Subscriptions', type: 'running', nature: 'SANKI', channel: 'Both' }).status, 200);
  assert.equal(finalize(statement).status, 200);
  const postings = (read(expensesFile).reconciliationExpenses || []).filter(x => x.creditCardStatementId === statement.id);
  assert.equal(postings.length, 1);
  assert.equal(postings[0].amount, -2);
});

test('record-card-payment rejects an exact repeat and a payment already present in a finalized statement', () => {
  const card = newCard(), paid = payment(card); assert.equal(paid.status, 200);
  let before = snapshot(), repeat = payment(card);
  assert.equal(repeat.status, 409);
  assert.deepEqual(snapshot(), before);
  const other = newCard(), statement = manual(other, { reference: 'EXTERNAL-SETTLEMENT' });
  assert.equal(review(statement, external).status, 200);
  assert.equal(finalize(statement).status, 200);
  before = snapshot();
  repeat = payment(other, { reference: 'EXTERNAL-SETTLEMENT' });
  assert.equal(repeat.status, 409);
  assert.deepEqual(snapshot(), before, 'a rejected duplicate must not add a bank transfer, allocation, sequence or audit');
});

test('restricted bank sources are not offered to accounting users and cannot be linked by forged source ID', () => {
  const card = newCard(), paid = payment(card, { nature: 'PERSONAL', account: 'ICICI Bank 0992' });
  assert.equal(paid.status, 200, paid.body && paid.body.error);
  const statement = manual(card), candidate = candidateFor(statement, paid.body.payment.id), accounting = current(statement, { role: 'accounting', username: 'ordinary-accountant' });
  assert.equal(accounting.rows[0].settlementCandidates.some(x => (x.sourceId || x.id) === 'payment:' + paid.body.payment.id), false);
  const before = snapshot(), rejected = review(statement, linkFields(candidate), { role: 'accounting', username: 'ordinary-accountant' });
  assert.ok(rejected.status >= 400);
  assert.deepEqual(snapshot(), before);
});

test('initial review warnings do not leak private payment metadata, including from other cards', () => {
  const card = newCard(), paid = payment(card, { nature: 'PERSONAL', account: 'ICICI Bank 0992', reference: 'PRIVATE-WARNING-REFERENCE' });
  assert.equal(paid.status, 200);
  const accounting = { role: 'accounting', username: 'ordinary-accountant' };
  const sameCard = current(manual(card), accounting), otherCard = current(manual(newCard()), accounting);
  for (const statement of [sameCard, otherCard]) {
    assert.doesNotMatch(JSON.stringify(statement), /ICICI Bank 0992|PRIVATE-WARNING-REFERENCE/);
    assert.equal(statement.rows[0].duplicateWarnings.some(x => x.id === paid.body.payment.id), false);
  }
});

test('saved private settlement metadata is redacted and cannot be erased by an accounting review', () => {
  const card = newCard(), paid = payment(card, { nature: 'PERSONAL', account: 'ICICI Bank 0992', reference: 'PRIVATE-OWNER-REFERENCE' });
  assert.equal(paid.status, 200);
  const statement = manual(card), candidate = candidateFor(statement, paid.body.payment.id);
  assert.equal(review(statement, linkFields(candidate)).status, 200);
  const accountingOptions = { role: 'accounting', username: 'ordinary-accountant' }, hidden = current(statement, accountingOptions).rows[0];
  assert.equal(hidden.settlementProtected, true);
  assert.equal(hidden.payingAccount || '', '');
  assert.equal(hidden.payingNature || '', '');
  assert.equal(hidden.settlementSourceId || '', '');
  assert.equal(hidden.settlementSourceFingerprint || '', '');
  assert.equal(hidden.settlementReference || '', '');
  assert.doesNotMatch(JSON.stringify(hidden), /ICICI Bank 0992|PRIVATE-OWNER-REFERENCE/);
  const before = snapshot();
  assert.ok(review(statement, { settlementMode: 'pending' }, accountingOptions).status >= 400);
  assert.deepEqual(snapshot(), before);
  assert.ok(finalize(statement, accountingOptions).status >= 400);
  assert.deepEqual(snapshot(), before);
  const owner = current(statement).rows[0];
  assert.equal(owner.settlementSourceId, 'payment:' + paid.body.payment.id);
  assert.equal(owner.payingAccount, 'ICICI Bank 0992');
});

test('an external settlement requires an actual untracked source, valid payer and a meaningful explanation', () => {
  const card = newCard(), statement = manual(card);
  for (const fields of [
    { ...external, payingNature: '' },
    { ...external, payingAccount: '' },
    { ...external, settlementReason: 'short' },
    { ...external, payingAccount: 'Axis Bank 3448' }
  ]) {
    const before = snapshot(), rejected = review(statement, fields);
    assert.ok(rejected.status >= 400);
    assert.deepEqual(snapshot(), before);
  }
});

test('external mode cannot disguise a private tracked bank account from an accounting user', () => {
  const card = newCard(), statement = manual(card), before = snapshot();
  const rejected = review(statement, { ...external, payingAccount: 'ICICI Bank 0992' }, { role: 'accounting', username: 'ordinary-accountant' });
  assert.ok(rejected.status >= 400);
  assert.deepEqual(snapshot(), before);
});

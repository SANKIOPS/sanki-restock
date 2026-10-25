'use strict';
const test = require('node:test'), assert = require('node:assert/strict'), express = require('express');
const { create } = require('../modules/credit-card-payment-corrections');
const { assertUnlocked } = require('../modules/source-correction-guards');
const clone = value => JSON.parse(JSON.stringify(value));
const day = '2098-04-17', bank = 'Synthetic Bank 1001', cardId = 'CC-SYNTHETIC', cardAccount = 'Synthetic Card 9991', sourceId = 'payment:CCP-SYNTHETIC';
const admin = { username: 'synthetic-admin', role: 'admin', personal: true }, owner = { username: 'synthetic-owner', role: 'owner', personal: true };
const reason = 'Correct the synthetic payment after checking retained source evidence';
function fixture() {
  const payment = { id: 'CCP-SYNTHETIC', cardId, amount: 100, date: day, nature: 'SANKI', account: bank, allocations: [{ nature: 'SANKI', amount: 100 }], reference: 'SYNTHETIC-REF-ONE', note: 'Original note', proof: '/synthetic-proof.jpg', active: true, paymentKind: 'partial' };
  const transfer = { id: 'TR-SYNTHETIC', nature: 'SANKI', fromNature: 'SANKI', toNature: 'SANKI', fromAccount: bank, toAccount: cardAccount, amount: 100, date: day, reference: payment.reference, note: 'Original note', proof: payment.proof, classification: 'credit_card_payment', creditCardId: cardId, creditCardPaymentId: payment.id };
  const allocation = { id: 'CCA-SYNTHETIC', cardId, cardName: cardAccount, creditCardPaymentId: payment.id, date: day, amount: 100, payerNature: 'SANKI', beneficiaryNature: 'SANKI', payingAccount: bank, status: 'self', note: 'Original allocation note' };
  return { store: { transfers: [transfer], creditCardSettlementAllocations: [allocation], expenses: {}, receipts: [], bankStatements: {}, bankReconciliationDrafts: {}, bankReconciliationApprovals: {}, bankReconciliationLinks: {}, bankDateOverrides: {}, bankMovementAuthority: {}, openingBalances: { [bank]: 1234 }, auditSeq: 1, auditLog: [{ id: 'AUD-OLD', action: 'PRESERVE' }], unknownPreserved: { value: 'retain exactly' } }, cards: { cards: { [cardId]: { id: cardId, name: 'Synthetic Card', last4: '9991', active: true } }, payments: [payment], statements: {}, auditSeq: 1, audit: [{ id: 'CCA-OLD', action: 'PRESERVE' }], unknownPreserved: { future: 'preserve' } }, extra: { salary: {} } };
}
function harness(mutate = () => {}) {
  const state = fixture(); mutate(state); const router = express.Router(); let commits = 0;
  const policy = {
    canEdit: req => ['admin', 'owner'].includes(req.user.role) && !req.user.denied,
    isOwner: req => req.user.role === 'owner', natures: req => req.user.natures || ['SANKI', 'SAMAST', 'PERSONAL'], canPersonal: req => req.user.personal === true,
    accountVisible: (req, account) => !req.user.hiddenAccounts?.includes(account), bookKey: (nature, account) => nature === 'SANKI' ? account : nature + '|' + account,
    personalAccount: account => account === 'Synthetic private account',
    assertUnlocked,
    read: () => ({ raw: JSON.stringify(state.store), store: clone(state.store), cards: clone(state.cards), cardRaw: JSON.stringify(state.cards), external: { '/synthetic/credit-cards.json': clone(state.cards), '/synthetic/salary.json': clone(state.extra.salary) }, externalRaw: { '/synthetic/credit-cards.json': JSON.stringify(state.cards), '/synthetic/salary.json': JSON.stringify(state.extra.salary) } }),
    commit(snapshot, store, cards) {
      assert.equal(snapshot.raw, JSON.stringify(state.store), 'Expenses CAS must match');
      assert.equal(snapshot.cardRaw, JSON.stringify(state.cards), 'Cards CAS must match');
      if (state.commitError) throw Error('Synthetic atomic storage rejection');
      state.store = clone(store); state.cards = clone(cards); commits++;
    }
  };
  const service = create({ policy: () => policy }); service.register(router);
  function invoke(action, body = {}, user = admin, id = sourceId) {
    const route = '/api/expenses/credit-cards/:cardId/payment-corrections' + (action === 'list' ? '' : '/:sourceId/' + action), method = action === 'list' ? 'get' : 'post';
    const layer = router.stack.find(item => item.route?.path === route && item.route.methods[method]); assert.ok(layer);
    let status = 200, result;
    layer.route.stack[0].handle({ user: clone(user), params: { cardId, sourceId: id }, body }, { status(value) { status = value; return this; }, json(value) { result = value; return this; } });
    return { status, body: result };
  }
  function preview(changes = { amount: '99.63' }, user = admin, id = sourceId) { return invoke('preview', { changes, reason }, user, id); }
  function apply(prepared, changes = { amount: '99.63' }, user = admin, id = sourceId) { return invoke('apply', { changes, reason, fingerprint: prepared.body.fingerprint }, user, id); }
  return { state, policy, service, invoke, preview, apply, commits: () => commits, raw: () => JSON.stringify(state) };
}
test('listing and preview are read-only, correctly strip own cards external file and preserve unmigrated data', () => {
  const h = harness(s => { s.store.oneTimeMigrations = {}; }), raw = h.raw();
  const list = h.invoke('list'); assert.equal(list.status, 200); assert.equal(list.body.entries[0].editable, true, JSON.stringify(list));
  const preview = h.preview(); assert.equal(preview.status, 200, JSON.stringify(preview)); assert.equal(preview.body.before.payment.amount, 100); assert.equal(preview.body.after.payment.amount, 99.63); assert.match(preview.body.fingerprint, /^[a-f0-9]{64}$/); assert.equal(h.raw(), raw); assert.equal(h.commits(), 0);
});
for (const user of [admin, owner]) test(user.role + ' updates exactly one CCP, transfer and allocation together with complete audits', () => {
  const h = harness(), before = clone(h.state), p = h.preview(undefined, user), result = h.apply(p, undefined, user); assert.equal(result.status, 200, JSON.stringify(result)); assert.equal(h.commits(), 1);
  for (const row of [h.state.cards.payments[0], h.state.store.transfers[0], h.state.store.creditCardSettlementAllocations[0]]) assert.equal(row.amount, 99.63);
  assert.equal(h.state.cards.payments[0].allocations[0].amount, 99.63);
  for (const key of ['date', 'account', 'nature', 'reference', 'note', 'proof', 'paymentKind', 'id', 'cardId']) assert.deepEqual(h.state.cards.payments[0][key], before.cards.payments[0][key], key);
  for (const key of ['expenses', 'receipts', 'bankStatements', 'bankReconciliationDrafts', 'bankReconciliationApprovals', 'bankReconciliationLinks', 'bankDateOverrides', 'openingBalances', 'unknownPreserved']) assert.deepEqual(h.state.store[key], before.store[key], key);
  assert.deepEqual(h.state.cards.statements, before.cards.statements); assert.deepEqual(h.state.cards.cards, before.cards.cards); assert.deepEqual(h.state.cards.audit[0], before.cards.audit[0]); assert.deepEqual(h.state.store.auditLog[0], before.store.auditLog[0]);
  const audit = h.state.store.auditLog.at(-1); assert.equal(audit.user, user.username); assert.equal(audit.note, reason); assert.equal(audit.before.payment.amount, 100); assert.equal(audit.after.payment.amount, 99.63); assert.equal(audit.before.transfer.amount, 100); assert.equal(audit.after.allocations[0].amount, 99.63);
});
function legacy(state) { delete state.store.transfers[0].creditCardPaymentId; delete state.store.transfers[0].creditCardId; state.cards.payments = []; state.store.creditCardSettlementAllocations = []; }
test('legacy transfer-only settlement changes the exact record, never invents a CCP or allocation', () => {
  const h = harness(legacy), id = 'transfer:TR-SYNTHETIC', p = h.preview({ amount: 99.63 }, admin, id); assert.equal(p.status, 200, JSON.stringify(p)); assert.equal(p.body.before.payment, null);
  assert.equal(h.apply(p, { amount: 99.63 }, admin, id).status, 200); assert.deepEqual(h.state.cards.payments, []); assert.deepEqual(h.state.store.creditCardSettlementAllocations, []); assert.equal(h.state.store.transfers[0].amount, 99.63);
});
test('date/reference/note edits preserve graph identities and update only linked dates', () => {
  const h = harness(), changes = { date: '2098-04-18', reference: 'SYNTHETIC-REF-TWO', note: '<img src=x onerror=unsafe>' }, p = h.preview(changes); assert.equal(p.status, 200, JSON.stringify(p)); assert.equal(h.apply(p, changes).status, 200);
  assert.equal(h.state.store.transfers[0].date, changes.date); assert.equal(h.state.cards.payments[0].date, changes.date); assert.equal(h.state.store.creditCardSettlementAllocations[0].date, changes.date); assert.equal(h.state.store.creditCardSettlementAllocations[0].note, 'Original allocation note'); assert.equal(h.state.cards.payments[0].amount, 100);
});
test('same-value preview/apply does not save or append audit', () => { const h = harness(), before = h.raw(), p = h.preview({ amount: 100 }); assert.equal(p.body.unchanged, true); assert.equal(h.apply(p, { amount: 100 }).body.unchanged, true); assert.equal(h.raw(), before); assert.equal(h.commits(), 0); });
for (const role of ['accounting', 'claimant', 'personal_claimant', 'inventory']) test(role + ' has no correction permission despite broader menu access', () => { const h = harness(), raw = h.raw(), user = { ...admin, role }; for (const action of ['list', 'preview', 'apply']) assert.equal(h.invoke(action, { changes: { amount: 10 }, reason }, user).status, 403); assert.equal(h.raw(), raw); });
test('private paying account, entity, owner-only card and allocations are not exposed', () => {
  for (const mode of ['account', 'entity', 'owner', 'allocation']) {
    const h = harness(s => { if (mode === 'owner') s.cards.cards[cardId].ownerOnly = true; if (mode === 'allocation') { s.cards.payments[0].allocations[0].nature = 'PERSONAL'; s.store.creditCardSettlementAllocations[0].beneficiaryNature = 'PERSONAL'; s.store.creditCardSettlementAllocations[0].status = 'due'; } });
    const user = { ...admin, personal: false, ...(mode === 'account' ? { hiddenAccounts: [bank] } : {}), ...(mode === 'entity' ? { natures: ['SAMAST'] } : {}) }, raw = h.raw(), list = h.invoke('list', {}, user);
    assert.ok(list.status === 404 || list.body.entries.length === 0, JSON.stringify(list)); assert.ok([403, 404].includes(h.preview(undefined, user).status)); assert.equal(h.raw(), raw);
  }
});
test('duplicate private payment identities are suppressed rather than leaked in lists', () => { const h = harness(s => { s.cards.payments.push({ ...s.cards.payments[0], account: 'Hidden private bank', nature: 'PERSONAL' }); }); assert.deepEqual(h.invoke('list', {}, { ...admin, personal: false }).body.entries, []); });
test('physical private accounts stay private even if a stored payment is business-tagged', () => { const h = harness(s => { s.cards.payments[0].account = 'Synthetic private account'; s.store.transfers[0].fromAccount = 'Synthetic private account'; s.store.creditCardSettlementAllocations[0].payingAccount = 'Synthetic private account'; }); assert.deepEqual(h.invoke('list', {}, { ...admin, personal: false }).body.entries, []); assert.equal(h.preview(undefined, { ...admin, personal: false }).status, 403); });
test('legacy transfer cannot be independently edited past an indistinguishable unlinked CCP', () => { const h = harness(s => { delete s.store.transfers[0].creditCardPaymentId; s.store.creditCardSettlementAllocations = []; }); assert.equal(h.preview({ amount: 50 }, admin, 'transfer:TR-SYNTHETIC').status, 409); assert.equal(h.commits(), 0); });
const invalid = [{ amount: 0 }, { amount: -1 }, { amount: 1.001 }, { amount: '1e3' }, { amount: '20junk' }, { amount: Infinity }, { amount: {} }, { date: '2098-02-30' }, { account: 'Other' }, { nature: 'PERSONAL' }, { proof: 'new' }, { allocations: [] }, { note: {} }, { reference: 'x'.repeat(301) }];
for (const changes of invalid) test('rejects invalid or identity-changing proposal ' + JSON.stringify(changes), () => { const h = harness(), before = h.raw(); assert.equal(h.preview(changes).status, 400); assert.equal(h.raw(), before); });
test('missing/short reason, extra fields and missing preview cannot write', () => { const h = harness(), raw = h.raw(); for (const body of [{ changes: { amount: 99 } }, { changes: { amount: 99 }, reason: 'short' }, { changes: { amount: 99 }, reason, cardId: 'forged' }]) assert.equal(h.invoke('preview', body).status, 400); assert.equal(h.invoke('apply', { changes: { amount: 99 }, reason }).status, 409); assert.equal(h.raw(), raw); });
test('changed proposal, reason, actor, store, card evidence and external evidence invalidate preview', () => {
  for (const mode of ['proposal', 'reason', 'actor', 'store', 'cards', 'external']) {
    const h = harness(), p = h.preview(), body = { changes: { amount: '99.63' }, reason, fingerprint: p.body.fingerprint }; let user = admin;
    if (mode === 'proposal') body.changes.amount = 99.62; if (mode === 'reason') body.reason += ' changed'; if (mode === 'actor') user = owner;
    if (mode === 'store') h.state.store.unknownPreserved.version = 2; if (mode === 'cards') h.state.cards.unknownPreserved.version = 2; if (mode === 'external') h.state.extra.salary.version = 2;
    const raw = h.raw(); assert.equal(h.invoke('apply', body, user).status, 409, mode); assert.equal(h.raw(), raw); assert.equal(h.commits(), 0);
  }
});
test('revoked permissions and new approvals after preview fail without writes', () => { for (const revoked of [false, true]) { const h = harness(), p = h.preview(); if (!revoked) h.state.store.bankReconciliationApprovals.A = { status: 'pending', account: bank, from: '2098-04-01', to: '2098-04-30' }; const raw = h.raw(); assert.equal(h.apply(p, undefined, revoked ? { ...admin, denied: true } : admin).status, revoked ? 403 : 409); assert.equal(h.raw(), raw); } });
const locks = [
  ['bank draft resolution', s => { s.store.bankReconciliationDrafts.D = { resolutions: { R: { appId: 'TR-SYNTHETIC' } } }; }],
  ['bank exact alias', s => { s.store.bankReconciliationLinks['TR-SYNTHETIC/RECEIVED'] = {}; }],
  ['Owner approval', s => { s.store.bankReconciliationApprovals.A = { status: 'pending', account: bank, from: '2098-04-01', to: '2098-04-30' }; }],
  ['physical other-entity finalized period', s => { s.store.bankStatements['PERSONAL|' + bank] = { reconciledThrough: day }; }],
  ['opening boundary', s => { s.store.openingBalanceDates = { [bank]: '2098-04-18' }; }],
  ['cash approval', s => { s.store.cashReconciliations = [{ status: 'approved', account: bank, verifiedThrough: day }]; }],
  ['card review link', s => { s.cards.statements.S = { status: 'review', rows: [{ settlementSourceId: sourceId }] }; }],
  ['card transfer alias link', s => { s.cards.statements.S = { status: 'review', rows: [{ settlementSourceId: 'transfer:TR-SYNTHETIC' }] }; }],
  ['finalized card amount/date', s => { s.cards.statements.S = { cardId, status: 'finalized', rows: [{ classification: 'card_payment', amount: 100, date: '2098-04-19' }] }; }],
  ['finalized card normalized reference', s => { s.cards.statements.S = { cardId, status: 'finalized', rows: [{ classification: 'card_payment', reference: 'syntheticrefone', amount: 999, date: '2098-08-01' }] }; }],
  ['external salary link', s => { s.extra.salary.attachment = { linkedTransferId: 'TR-SYNTHETIC' }; }],
  ['Gokwik forward link', s => { s.store.gokwikPostings = { G: { transferId: 'TR-SYNTHETIC' } }; }],
  ['receipt forward link', s => { s.store.receipts.push({ id: 'R-OTHER', settledByTransferId: 'TR-SYNTHETIC' }); }],
  ['other payment forward link', s => { s.cards.payments.push({ id: 'CCP-OTHER', cardId: 'CC-OTHER', associatedPaymentId: 'CCP-SYNTHETIC' }); }]
];
for (const [label, mutate] of locks) test(label + ' stays locked for every role, including note-only proposals', () => { const h = harness(mutate), raw = h.raw(); for (const user of [admin, owner]) { assert.equal(h.preview({ note: 'New descriptive note' }, user).status, 409, label); assert.equal(h.invoke('apply', { changes: { note: 'New descriptive note' }, reason, fingerprint: 'forged' }, user).status, 409); } assert.equal(h.raw(), raw); });
for (const field of ['reconciled', 'reconciledAt', 'reconciliationId', 'finalizedAt', 'locked', 'bankTransactionId', 'bankStatementEvidence', 'accountingExcluded', 'historicalSettlement', 'bankReconciliationDraft', 'loanId', 'salaryAdvanceId', 'settledByTransferId', 'batchId']) test('source marker ' + field + ' is protected', () => { const h = harness(s => { s.store.transfers[0][field] = 'SYNTHETIC'; }), raw = h.raw(); assert.equal(h.preview().status, 409); assert.equal(h.raw(), raw); });
for (const field of ['settledAmount', 'reimbursedAmount', 'paidAmount', 'settlementId', 'settledBy', 'settledAt', 'settlements', 'linkedEntries']) test('allocation ' + field + ' cannot be changed independently', () => { const h = harness(s => { s.store.creditCardSettlementAllocations[0][field] = ['settlements', 'linkedEntries'].includes(field) ? [{ id: 'OTHER' }] : 1; }); assert.equal(h.preview().status, 409); assert.equal(h.commits(), 0); });
test('proposed dates cannot enter a finalized or pending period on either side', () => { for (const pending of [true, false]) { const h = harness(s => { if (pending) s.store.bankReconciliationApprovals.A = { status: 'pending', account: cardAccount, from: '2098-03-01', to: '2098-03-30' }; else s.store.bankStatements[cardAccount] = { reconciledThrough: '2098-03-30' }; }); assert.equal(h.preview().status, 200); assert.equal(h.preview({ date: '2098-03-15' }).status, 409); } });
test('proposed amount/date matching finalized card evidence is blocked', () => { const h = harness(s => { s.cards.statements.S = { cardId, status: 'finalized', rows: [{ amount: 98.75, date: day, classification: 'card_payment' }] }; }); assert.equal(h.preview().status, 200); assert.equal(h.preview({ amount: 98.75 }).status, 409); });
test('multi-allocation amount edits fail; permitted metadata edits preserve all ownership amounts', () => {
  const h = harness(s => { s.cards.payments[0].allocations = [{ nature: 'SANKI', amount: 60 }, { nature: 'SAMAST', amount: 40 }]; s.store.creditCardSettlementAllocations[0].amount = 60; s.store.creditCardSettlementAllocations.push({ ...s.store.creditCardSettlementAllocations[0], id: 'CCA-SECOND', beneficiaryNature: 'SAMAST', amount: 40, status: 'due' }); });
  assert.equal(h.preview().status, 409); const p = h.preview({ note: 'Metadata correction only' }); assert.equal(p.status, 200, JSON.stringify(p)); assert.equal(h.apply(p, { note: 'Metadata correction only' }).status, 200); assert.deepEqual(h.state.cards.payments[0].allocations.map(row => row.amount), [60, 40]);
});
test('bankReference/utr aliases cannot silently disagree after reference correction', () => { const h = harness(s => { s.store.transfers[0].bankReference = s.store.transfers[0].reference; }); assert.equal(h.preview().status, 200); assert.equal(h.preview({ reference: 'NEW-REFERENCE' }).status, 409); });
for (const [label, mutate] of [
  ['missing transfer', s => { s.store.transfers = []; }], ['duplicate transfer', s => { s.store.transfers.push(clone(s.store.transfers[0])); }],
  ['missing allocation', s => { s.store.creditCardSettlementAllocations = []; }], ['allocation card mismatch', s => { s.store.creditCardSettlementAllocations[0].cardName = 'Wrong'; }],
  ['allocation sum mismatch', s => { s.cards.payments[0].allocations[0].amount = 99; }], ['payment amount mismatch', s => { s.cards.payments[0].amount = 101; }],
  ['duplicate card name', s => { s.cards.cards.OTHER = { ...s.cards.cards[cardId], id: 'OTHER' }; }], ['receiving entity mismatch', s => { s.store.transfers[0].toNature = 'SAMAST'; }],
  ['reference mismatch', s => { s.store.transfers[0].reference = 'FORGED'; }], ['inactive record', s => { s.cards.payments[0].active = false; }]
]) test(label + ' rejects the full graph without partial save', () => { const h = harness(mutate), raw = h.raw(); assert.equal(h.preview().status, 409); assert.equal(h.raw(), raw); });
test('a new duplicate collision after proposed correction blocks saving', () => { const h = harness(s => { s.store.transfers.push({ ...s.store.transfers[0], id: 'TR-OTHER', creditCardPaymentId: '', amount: 99.63, reference: 'OTHER-REF' }); }); assert.equal(h.preview().status, 409); assert.equal(h.commits(), 0); });
test('atomic commit failure leaves both stores and audits untouched', () => { const h = harness(), p = h.preview(); h.state.commitError = true; const raw = h.raw(), result = h.apply(p); assert.equal(result.status, 409); assert.equal(h.raw(), raw); assert.equal(h.commits(), 0); });
test('full old audit history is retained beyond ten thousand records', () => { const h = harness(s => { s.store.auditLog = Array.from({ length: 10001 }, (_, i) => ({ id: 'AUD-OLD-' + i })); s.cards.audit = Array.from({ length: 10001 }, (_, i) => ({ id: 'CCA-OLD-' + i })); }); const before = clone(h.state), p = h.preview(); assert.equal(h.apply(p).status, 200); assert.deepEqual(h.state.store.auditLog.slice(0, 10001), before.store.auditLog); assert.deepEqual(h.state.cards.audit.slice(0, 10001), before.cards.audit); assert.equal(h.state.cards.audit.length, 10002); });

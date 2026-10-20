'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { build } = require('../modules/bank-card-authority');
const clone = value => JSON.parse(JSON.stringify(value));
function fixture({ amount = 26701, bankAmount = 26700.63, allocations = [{ nature: 'PERSONAL', amount }] } = {}) {
  const transfer = { id: 'TR-00136', nature: 'PERSONAL', fromNature: 'PERSONAL', toNature: 'PERSONAL', classification: 'credit_card_payment', creditCardPaymentId: 'CCP-00011', fromAccount: 'IndusInd Bank 7883', toAccount: 'HDFC 7376', amount, date: '2026-09-29', proof: '/transfer-proof.jpg', note: 'Credit card payment CCP-00011' };
  const payment = { id: 'CCP-00011', cardId: 'CC-0002', nature: 'PERSONAL', account: transfer.fromAccount, amount, date: transfer.date, active: true, allocations: clone(allocations), proof: '/card-proof.jpg', reference: 'OLD-REFERENCE', paymentKind: 'full' };
  const cards = { cards: { 'CC-0002': { id: 'CC-0002', name: 'HDFC', last4: '7376', active: true, openingOutstanding: 100000 } }, payments: [payment], statements: {}, audit: [] };
  const store = { transfers: [transfer], creditCardSettlementAllocations: allocations.map(item => ({ id: 'CCA-CCP-00011-' + item.nature, creditCardPaymentId: payment.id, cardId: payment.cardId, cardName: transfer.toAccount, date: transfer.date, amount: item.amount, payerNature: 'PERSONAL', beneficiaryNature: item.nature, payingAccount: transfer.fromAccount, status: item.nature === 'PERSONAL' ? 'self' : 'due' })), bankStatements: {}, bankDateOverrides: {}, bankReconciliationDrafts: {}, expenses: {}, reconciliationExpenses: [] };
  const context = { source: { type: 'transfer', record: transfer, id: transfer.id }, movement: { id: transfer.id, debit: amount, credit: 0 }, draft: { id: 'BRD-CARD', nature: 'SAMAST', account: transfer.fromAccount }, bank: { date: '2026-09-29', debit: bankAmount, credit: 0, reference: 'ACTUAL-BANK-REF', description: 'HDFC CARD PAYMENT' }, bankDate: '2026-09-29', bankAmount, delta: Math.round((bankAmount - amount) * 100) / 100, externalStores: { creditCards: cards } };
  return { store, context, cards, transfer, payment, build() { return build(store, context); } };
}
function pureReject(f, pattern) {
  const before = JSON.stringify({ store: f.store, cards: f.cards }); assert.throws(() => f.build(), pattern);
  assert.equal(JSON.stringify({ store: f.store, cards: f.cards }), before);
}
function applyLocally(f, plan) {
  const stores = { expenses: clone(f.store), creditCards: clone(f.cards) };
  for (const patch of plan.patches) { let parent = stores[patch.store]; for (const key of patch.path.slice(0, -1)) parent = parent[key]; assert.deepEqual(parent[patch.path.at(-1)], patch.before); parent[patch.path.at(-1)] = clone(patch.after); }
  return stores;
}
test('the TR-00136 ₹26701 to ₹26700.63 case plans one linked payment correction, not a purchase expense', () => {
  const f = fixture(), before = JSON.stringify({ store: f.store, cards: f.cards }), plan = f.build();
  assert.equal(JSON.stringify({ store: f.store, cards: f.cards }), before);
  assert.equal(plan.sourceType, 'credit_card_payment'); assert.equal(plan.sourceNature, 'PERSONAL'); assert.deepEqual(plan.linkedIds, ['TR-00136', 'CCP-00011', 'CCA-CCP-00011-PERSONAL']);
  assert.equal(plan.patches.length, 3); const after = applyLocally(f, plan);
  assert.equal(after.expenses.transfers.length, 1); assert.equal(after.expenses.transfers[0].amount, 26700.63); assert.equal(after.creditCards.payments[0].amount, 26700.63);
  assert.equal(after.creditCards.payments[0].allocations[0].amount, 26700.63); assert.equal(after.expenses.creditCardSettlementAllocations[0].amount, 26700.63);
  assert.deepEqual(after.expenses.expenses, {}); assert.deepEqual(after.expenses.reconciliationExpenses, []); assert.deepEqual(after.creditCards.statements, {});
  assert.equal(after.expenses.transfers[0].proof, '/transfer-proof.jpg'); assert.equal(after.creditCards.payments[0].reference, 'ACTUAL-BANK-REF');
  assert.deepEqual(plan.beforeSummary, { amount: 26701, date: '2026-09-29', creditCardPaymentId: 'CCP-00011', cardId: 'CC-0002' }); assert.equal(plan.afterSummary.amount, 26700.63);
  assert.equal(plan.guards.length, 2); assert.equal(JSON.parse(JSON.stringify(plan)).patches.length, 3);
});
test('large bank differences and actual date update all linked payment records without altering card purchases', () => {
  const f = fixture({ bankAmount: 30001.99 }); f.context.bank.date = '2026-10-02'; f.context.bankDate = '2026-10-02';
  const result = applyLocally(f, f.build());
  assert.equal(result.expenses.transfers[0].date, '2026-10-02'); assert.equal(result.creditCards.payments[0].date, '2026-10-02'); assert.equal(result.expenses.creditCardSettlementAllocations[0].date, '2026-10-02');
  assert.equal(result.creditCards.payments[0].amount, 30001.99); assert.equal(result.expenses.creditCardSettlementAllocations[0].amount, 30001.99);
});
test('non-card sources return null so the normal authority engine retains responsibility', () => {
  assert.equal(build({}, { source: { record: { id: 'OTHER', classification: 'internal_transfer' } } }), null);
  assert.equal(build({}, {}), null);
});
for (const [name, change] of Object.entries({
  'missing card store': f => { f.context.externalStores = {}; },
  'missing linked payment': f => { f.cards.payments = []; },
  'duplicate linked payment': f => { f.cards.payments.push(clone(f.payment)); },
  'duplicate transfer': f => { f.store.transfers.push({ ...f.transfer, id: 'TR-DUPLICATE' }); },
  'stale supplied transfer': f => { f.context.source.record = { ...f.transfer, amount: 999 }; },
  'unknown card': f => { f.payment.cardId = 'MISSING'; },
  'inactive card': f => { f.cards.cards['CC-0002'].active = false; },
  'inactive payment': f => { f.payment.active = false; },
  'different payer': f => { f.payment.account = 'DIFFERENT'; },
  'different date': f => { f.payment.date = '2026-09-28'; },
  'different old amount': f => { f.payment.amount = 1; },
  'wrong movement': f => { f.context.movement.id = 'OTHER'; },
  'opposite bank direction': f => { f.context.bank.credit = f.context.bank.debit; f.context.bank.debit = 0; },
  'bank amount changed': f => { f.context.bank.debit = 26700.62; },
  'reviewed delta changed': f => { f.context.delta = -.38; },
  'bank date changed': f => { f.context.bank.date = '2026-09-30'; },
  'bank reversal': f => { f.context.bank.reversal = true; },
  'finalized payment': f => { f.payment.finalizedAt = '2026-09-30'; },
  'reconciled payment': f => { f.payment.reconciledAt = '2026-09-30'; },
  'card identity mismatch': f => { f.cards.cards['CC-0002'].id = 'OTHER'; },
  'finalized bank': f => { f.store.bankStatements.BANK = { imports: [{ reconciliationRows: [{ linkedRecordIds: ['TR-00136'] }] }] }; },
  'finalized card payment alias': f => { f.store.bankStatements.BANK = { imports: [{ reconciliationRows: [{ ledger: { id: 'CCP-00011' } }] }] }; },
  'foreign draft': f => { f.store.bankReconciliationDrafts.OTHER = { id: 'OTHER', resolutions: { 'bank-0': { appId: 'TR-00136' } } }; },
  'finalized override': f => { f.store.bankDateOverrides['TR-00136'] = { reconciliationDraft: 'FINALIZED' }; },
  'missing ownership row': f => { f.store.creditCardSettlementAllocations = []; },
  'duplicate ownership row': f => { f.store.creditCardSettlementAllocations.push(clone(f.store.creditCardSettlementAllocations[0])); },
  'changed ownership amount': f => { f.store.creditCardSettlementAllocations[0].amount = 10; },
  'settled ownership': f => { f.store.creditCardSettlementAllocations[0].status = 'settled'; },
  'linked reimbursement': f => { f.store.creditCardSettlementAllocations[0].settledByTransferId = 'TR-OTHER'; }
})) test('card correction fails closed for ' + name, () => { const f = fixture(); change(f); pureReject(f); });
test('current unfinished draft provisional bank evidence is not mistaken for finalized history', () => {
  const f = fixture(); f.store.bankDateOverrides['TR-00136'] = { provisional: true, reconciliationDraft: 'BRD-CARD' }; assert.doesNotThrow(() => f.build());
});
function finalStatement(f, row = {}) {
  f.cards.statements.FINAL = { id: 'FINAL', cardId: 'CC-0002', status: 'finalized', rows: [{ id: 'CARD-ROW', classification: 'card_payment', amount: 26701, credit: 26701, date: '2026-09-29', narration: 'PAYMENT THANK YOU', ...row }] };
}
for (const [name, change] of Object.entries({
  'exact old amount/date': f => finalStatement(f),
  'exact bank amount/date': f => finalStatement(f, { amount: 26700.63, credit: 26700.63 }),
  'card posting delay': f => finalStatement(f, { date: '2026-10-01' }),
  'explicit payment ID': f => finalStatement(f, { date: '2020-01-01', amount: 3, credit: 3, creditCardPaymentId: 'CCP-00011' }),
  'exact bank reference': f => finalStatement(f, { date: '2020-01-01', amount: 3, credit: 3, reference: 'ACTUAL-BANK-REF' }),
  'duplicate-warning source': f => finalStatement(f, { date: '2020-01-01', amount: 3, credit: 3, duplicateWarnings: [{ kind: 'card_payment', id: 'CCP-00011' }] })
})) test('finalized card evidence is protected by ' + name, () => { const f = fixture(); change(f); pureReject(f, /finalized card statement/i); });
test('unrelated finalized purchase or clearly separate old payment is not overwritten or treated as this payment', () => {
  const f = fixture(); finalStatement(f, { classification: 'expense', credit: 0, debit: 26701, narration: 'MERCHANT PURCHASE' });
  f.cards.statements.FINAL.rows.push({ id: 'OLD-PAYMENT', classification: 'card_payment', amount: 26701, credit: 26701, date: '2025-01-01' });
  assert.doesNotThrow(() => f.build()); assert.equal(f.build().patches.some(patch => patch.path[0] === 'statements'), false);
});
test('review-only card statement remains untouched; guards change if it is finalized before application', () => {
  const f = fixture(); finalStatement(f); f.cards.statements.FINAL.status = 'review'; const first = f.build();
  f.cards.statements.FINAL.status = 'finalized'; pureReject(f, /finalized card statement/);
  assert.ok(first.guards.find(guard => guard.path[0] === 'statements').fingerprint);
});
test('mixed-entity payment requires explicit allocation even for a one-paisa difference', () => {
  const f = fixture({ amount: 100, bankAmount: 99.99, allocations: [{ nature: 'PERSONAL', amount: 60 }, { nature: 'SANKI', amount: 40 }] });
  pureReject(f, /multiple entities/);
  f.context.allocations = [{ nature: 'PERSONAL', amount: 59.99 }, { nature: 'SANKI', amount: 40 }];
  const plan = f.build(), after = applyLocally(f, plan);
  assert.equal(after.creditCards.payments[0].amount, 99.99); assert.equal(after.creditCards.payments[0].allocations[0].amount, 59.99);
  assert.equal(after.expenses.creditCardSettlementAllocations[0].amount, 59.99); assert.equal(after.expenses.creditCardSettlementAllocations[1].amount, 40);
  assert.doesNotMatch(JSON.stringify(plan.beforeSummary) + JSON.stringify(plan.afterSummary), /allocations|59\.99|60|40/);
});
test('mixed ownership permits date-only corrections without reallocating the old balances', () => {
  const f = fixture({ amount: 100, bankAmount: 100, allocations: [{ nature: 'PERSONAL', amount: 60 }, { nature: 'SAMAST', amount: 40 }] }); f.context.bank.date = '2026-10-01'; f.context.bankDate = '2026-10-01';
  const after = applyLocally(f, f.build()); assert.deepEqual(after.creditCards.payments[0].allocations, f.payment.allocations); assert.equal(after.expenses.creditCardSettlementAllocations[1].date, '2026-10-01');
});
for (const allocations of [
  [{ nature: 'PERSONAL', amount: 50 }, { nature: 'SANKI', amount: 50 }],
  [{ nature: 'PERSONAL', amount: 59 }, { nature: 'SAMAST', amount: 40.99 }],
  [{ nature: 'PERSONAL', amount: 58.99 }, { nature: 'SANKI', amount: 41 }],
  [{ nature: 'PERSONAL', amount: 99.99 }]
]) test('invalid or redistributive explicit ownership correction is rejected ' + JSON.stringify(allocations), () => {
  const f = fixture({ amount: 100, bankAmount: 99.99, allocations: [{ nature: 'PERSONAL', amount: 60 }, { nature: 'SANKI', amount: 40 }] }); f.context.allocations = allocations; pureReject(f);
});

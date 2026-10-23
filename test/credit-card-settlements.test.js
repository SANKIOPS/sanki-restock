'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const settlements = require('../modules/credit-card-settlements');
const clone = value => JSON.parse(JSON.stringify(value));
const access = (nature, account) => nature === 'SANKI' && account === 'Axis Bank 3448';
function fixture() {
  const payment = { id: 'CCP-00001', cardId: 'CC-0001', amount: 298799.31, date: '2026-08-27', nature: 'SANKI', account: 'Axis Bank 3448', reference: 'UTR-100', active: true };
  const transfer = { id: 'TR-00001', classification: 'credit_card_payment', fromNature: 'SANKI', toNature: 'SANKI', fromAccount: payment.account, toAccount: 'ICICI Sapphiro 8000', amount: payment.amount, date: payment.date, creditCardPaymentId: payment.id, reference: payment.reference };
  const row = { id: 'CCR-1', date: payment.date, amount: payment.amount, debit: 0, credit: payment.amount, narration: 'INFINITY PAYMENT RECEIVED, THANK YOU', classification: 'card_payment' };
  const statement = { id: 'CCS-00003', cardId: payment.cardId, status: 'review', rows: [row] };
  const store = { cards: { 'CC-0001': { id: payment.cardId, name: 'ICICI Sapphiro', last4: '8000' }, 'CC-0002': { id: 'CC-0002', name: 'Other Card', last4: '1234' } }, payments: [payment], statements: { [statement.id]: statement } };
  return { store, exp: { transfers: [transfer] }, statement, row, payment, transfer };
}
function input(f, overrides = {}) {
  const choice = settlements.candidates(f.store, f.exp, f.statement, f.row, access)[0];
  return { settlementMode: 'link_existing', settlementSourceId: choice && choice.id, settlementSourceFingerprint: choice && choice.fingerprint, ...overrides };
}
function review(f, overrides = {}, checker = access) { return settlements.reviewSettlement(f.store, f.exp, f.statement, f.row, input(f, overrides), checker); }
test('one canonical CCP-plus-transfer candidate binds authoritative settlement without mutating either store', () => {
  const f = fixture(), before = clone(f), candidates = settlements.candidates(f.store, f.exp, f.statement, f.row, access);
  assert.equal(candidates.length, 1);
  assert.equal(candidates[0].id, 'payment:CCP-00001');
  assert.equal(candidates[0].eligible, true);
  assert.equal(candidates[0].bankMovementRecorded, true);
  const result = review(f, { payingAccount: 'Malicious bank', payingNature: 'PERSONAL', settlementReference: 'fake', category: 'Sales', type: 'variable', expenseRefundReceiptId: 'refund' });
  assert.equal(result.error, '');
  assert.equal(result.fields.payingAccount, f.payment.account);
  assert.equal(result.fields.payingNature, f.payment.nature);
  assert.equal(result.fields.settlementReference, f.payment.reference);
  assert.equal(result.fields.category, undefined);
  assert.equal(result.fields.expenseRefundReceiptId, undefined);
  assert.deepEqual(f, before);
});
test('generic active exact-card settlement transfer is a link-only candidate', () => {
  const f = fixture(); f.store.payments = []; delete f.transfer.creditCardPaymentId;
  const choices = settlements.candidates(f.store, f.exp, f.statement, f.row, access);
  assert.equal(choices.length, 1); assert.equal(choices[0].id, 'transfer:TR-00001');
  assert.equal(review(f).error, '');
});
test('legacy payment without transfer is not presented as bank-verified', () => {
  const f = fixture(); f.exp.transfers = [];
  const choice = settlements.candidates(f.store, f.exp, f.statement, f.row, access)[0];
  assert.equal(choice.eligible, true); assert.equal(choice.bankMovementRecorded, false);
  assert.match(choice.status, /not verified/); assert.equal(review(f).error, '');
});
test('source amount requires exact integer paise; a one-paise mismatch cannot be linked', () => {
  const f = fixture(); f.row.amount += 0.01; f.row.credit = f.row.amount;
  assert.match(review(f).error, /exactly/);
  f.row.amount = f.row.credit = '298799.311'; assert.match(review(f).error, /positive statement credit/);
});
test('invalid and non-credit statement amounts fail closed', () => {
  for (const patch of [{ credit: 0 }, { debit: 1 }, { amount: -10 }, { amount: Infinity }, { amount: '1e4' }, { amount: '100.001' }, { amount: '' }]) {
    const f = fixture(); Object.assign(f.row, patch); assert.ok(review(f).error, JSON.stringify(patch));
  }
});
test('same-day links need no fabricated explanation, date differences require one within three days', () => {
  const f = fixture(); assert.equal(review(f).error, '');
  f.row.date = '2026-08-30'; assert.match(review(f).error, /date difference/);
  assert.equal(review(f, { settlementReason: 'Card issuer posted three days later' }).error, '');
  f.row.date = '2026-08-31'; assert.match(review(f, { settlementReason: 'Long enough reason for date' }).error, /within three days/);
});
test('invalid dates cannot be accepted by settlement review', () => {
  for (const date of ['2026-02-30', '2026-13-01', '27/08/2026', '', '2026-08-27T00:00:00Z']) {
    const f = fixture(); f.row.date = date; assert.ok(review(f).error);
  }
});
test('wrong-card, inactive and unrelated internal-transfer sources are not choices', () => {
  const f = fixture(); f.payment.cardId = 'CC-0002'; f.transfer.toAccount = 'Other Card 1234';
  assert.deepEqual(settlements.candidates(f.store, f.exp, f.statement, f.row, access), []);
  assert.ok(review(f, { settlementSourceId: 'payment:CCP-00001', settlementSourceFingerprint: 'fake' }).error);
  const g = fixture(); g.payment.active = false; g.transfer.accountingExcluded = true;
  assert.deepEqual(settlements.candidates(g.store, g.exp, g.statement, g.row, access), []);
  const h = fixture(); h.store.payments = []; h.transfer.classification = 'internal_transfer'; delete h.transfer.creditCardPaymentId;
  assert.deepEqual(settlements.candidates(h.store, h.exp, h.statement, h.row, access), []);
});
test('associated CCP transfer with conflicting explicit card identity cannot link or evade collision checks', () => {
  const f = fixture(); f.transfer.creditCardId = 'CC-0002';
  const choice = settlements.candidates(f.store, f.exp, f.statement, f.row, access)[0];
  assert.equal(choice.eligible, false); assert.match(choice.blockedReason, /no longer agree/);
  assert.match(review(f).error, /no longer agree/);
  assert.match(review(f, { settlementMode: 'external', payingNature: 'PERSONAL', payingAccount: 'Historical bank', settlementReason: 'This payment was received before this system existed' }).error, /already exists/);
  assert.ok(settlements.duplicatePayment(f.store, f.exp, { cardId: f.statement.cardId, amount: f.row.amount, date: f.row.date, reference: '' }));
});
test('standalone settlement requires both card identity and exact display account, without hiding suspicious evidence', () => {
  for (const patch of [f => { f.transfer.creditCardId = 'CC-0002'; }, f => { f.transfer.creditCardId = 'CC-0001'; f.transfer.toAccount = 'Wrong Card 1234'; }]) {
    const f = fixture(); f.store.payments = []; delete f.transfer.creditCardPaymentId; patch(f);
    const choice = settlements.candidates(f.store, f.exp, f.statement, f.row, access)[0];
    assert.equal(choice.eligible, false); assert.match(choice.blockedReason, /card identity/);
    assert.match(review(f).error, /card identity/);
    assert.match(review(f, { settlementMode: 'external', payingNature: 'PERSONAL', payingAccount: 'Historical bank', settlementReason: 'This payment was received before this system existed' }).error, /already exists/);
    assert.ok(settlements.duplicatePayment(f.store, f.exp, { cardId: f.statement.cardId, amount: f.row.amount, date: f.row.date, reference: '' }));
  }
});
test('account access fails closed without revealing candidate labels or fingerprint', () => {
  const f = fixture();
  for (const check of [undefined, () => false, () => 'true', () => { throw new Error('denied'); }]) {
    assert.deepEqual(settlements.candidates(f.store, f.exp, f.statement, f.row, check), []);
    assert.match(settlements.reviewSettlement(f.store, f.exp, f.statement, f.row, input(f), check).error, /accessible/);
  }
});
test('required expected fingerprint rejects stale source edits and forged snapshots', () => {
  const f = fixture(), savedInput = input(f);
  for (const fingerprint of ['', 'fake']) assert.match(review(f, { settlementSourceFingerprint: fingerprint }).error, /changed/);
  f.payment.proof = 'new proof';
  assert.match(settlements.reviewSettlement(f.store, f.exp, f.statement, f.row, savedInput, access).error, /changed/);
  const g = fixture(), transferInput = input(g); g.transfer.note = 'new source audit note';
  assert.match(settlements.reviewSettlement(g.store, g.exp, g.statement, g.row, transferInput, access).error, /changed/);
});
test('missing explicit, duplicated or mismatched associated transfers reject canonical CCP link', () => {
  const patches = [f => { f.payment.transferId = 'TR-missing'; f.exp.transfers = []; }, f => { f.exp.transfers.push({ ...f.transfer, id: 'TR-00002' }); }, f => { f.transfer.amount -= 1; }, f => { f.transfer.toAccount = 'Other Card 1234'; }, f => { f.transfer.fromAccount = 'Other Bank'; }, f => { f.transfer.classification = 'internal_transfer'; }, f => { f.transfer.active = false; }, f => { f.transfer.accountingExcluded = true; }, f => { f.transfer.date = '2026-08-28'; }, f => { f.transfer.fromNature = 'PERSONAL'; }];
  for (const patch of patches) { const f = fixture(); patch(f); assert.ok(review(f).error); }
});
test('duplicate source identifiers and orphan transfer relationships cannot be linked', () => {
  const f = fixture(); f.store.payments.push({ ...f.payment }); assert.match(review(f).error, /ambiguous|accessible/);
  const g = fixture(); g.store.payments = []; assert.match(review(g).error, /missing or inactive/);
  const h = fixture(); h.store.payments = []; delete h.transfer.creditCardPaymentId; h.exp.transfers.push({ ...h.transfer }); assert.match(review(h).error, /ambiguous|accessible/);
});
function reserve(f, changes = {}) {
  const linked = review(f); assert.equal(linked.error, '');
  f.store.statements['CCS-other'] = { id: 'CCS-other', cardId: f.statement.cardId, status: 'review', rows: [{ ...f.row, ...linked.fields, id: 'CCR-other', ...changes }] };
}
test('another saved or finalized active row reserves a source against repeat linking', () => {
  for (const status of ['review', 'finalized']) { const f = fixture(); reserve(f); f.store.statements['CCS-other'].status = status; assert.match(review(f).error, /already linked/); }
  const f = fixture(); Object.assign(f.row, review(f).fields); assert.equal(review(f).error, '', 'same row may be saved again');
});
test('canonical CCP cannot evade an older reservation under its transfer alias', () => {
  const f = fixture(); reserve(f, { settlementSourceId: 'transfer:TR-00001' }); assert.match(review(f).error, /already linked/);
});
test('true duplicates and superseded rows do not reserve another settlement', () => {
  for (const changes of [{ duplicateOf: { statementId: 'a', rowId: 'b' } }, { supersededBy: { statementId: 'a', rowId: 'b' } }, { duplicateResolution: 'duplicate' }, { duplicateResolution: 'link' }, { duplicateResolution: 'merge' }, { duplicateResolution: 'replace' }]) { const f = fixture(); reserve(f, changes); assert.equal(review(f).error, ''); }
});
test('genuine billed replacement may retain a settlement, but a different credit cannot', () => {
  const f = fixture(); reserve(f); f.row.replaces = { statementId: 'CCS-other', rowId: 'CCR-other' }; assert.equal(review(f).error, '');
  f.row.reference = 'different'; f.store.statements['CCS-other'].rows[0].reference = 'original'; assert.match(review(f).error, /already linked/);
});
test('pending review stores no payer or source and cannot be finalized', () => {
  const f = fixture(), result = review(f, { settlementMode: 'pending', payingNature: 'SANKI', payingAccount: 'fake' });
  assert.equal(result.error, ''); assert.equal(result.fields.payingAccount, ''); assert.equal(result.fields.settlementSourceId, '');
  Object.assign(f.row, result.fields); assert.match(settlements.validateStatement(f.store, f.exp, f.statement, access), /before finalizing/);
});
test('external settlement is descriptive, reasoned, untracked and creates no movement', () => {
  const f = fixture(); f.store.payments = []; f.exp.transfers = []; const before = clone(f);
  const external = { settlementMode: 'external', payingNature: 'PERSONAL', payingAccount: 'Historical external account', settlementReference: 'LEGACY-REF', settlementReason: 'Paid before this accounting system started' };
  const result = review(f, external); assert.equal(result.error, ''); assert.equal(result.fields.settlementSourceId, '');
  assert.deepEqual(f, before); Object.assign(f.row, result.fields); assert.equal(settlements.validateStatement(f.store, f.exp, f.statement, access), '');
  for (const patch of [{ payingNature: 'invalid' }, { payingAccount: '' }, { settlementReason: 'short' }]) assert.ok(review(f, { ...external, ...patch }).error);
});
test('external selection cannot bypass recorded, used, inaccessible or inconsistent matching settlement', () => {
  const external = { settlementMode: 'external', payingNature: 'PERSONAL', payingAccount: 'Untracked old bank', settlementReason: 'This is described as a historical payment' };
  for (const patch of [() => {}, f => reserve(f), f => { f.transfer.active = false; }]) { const f = fixture(); patch(f); assert.match(review(f, external).error, /already exists/); }
  const f = fixture(); assert.match(review(f, external, () => false).error, /already exists/);
});
test('damaged canonical payment cannot hide its associated transfer from external or duplicate guards', () => {
  const external = { settlementMode: 'external', payingNature: 'PERSONAL', payingAccount: 'Untracked old bank', settlementReason: 'This is described as a historical payment' };
  for (const patch of [f => { f.payment.amount = 99; }, f => { delete f.payment.amount; }, f => { f.payment.date = '2020-01-01'; }, f => { f.payment.date = 'invalid'; }, f => { f.payment.reference = 'WRONG-REFERENCE'; }]) {
    const f = fixture(); patch(f);
    assert.match(review(f, external).error, /already exists/);
    assert.ok(settlements.duplicatePayment(f.store, f.exp, { cardId: f.statement.cardId, amount: f.row.amount, date: f.row.date, reference: '' }));
    assert.ok(settlements.duplicatePayment(f.store, f.exp, { cardId: f.statement.cardId, amount: 10, date: '2026-09-01', reference: f.transfer.reference }));
  }
});
test('all associated transfer evidence remains collision-protected even with duplicate source relationships', () => {
  const f = fixture(); f.payment.amount = 99; f.transfer.amount = 99;
  f.exp.transfers.push({ ...f.transfer, id: 'TR-second', amount: f.row.amount, reference: 'SECOND-REF' });
  const external = { settlementMode: 'external', payingNature: 'PERSONAL', payingAccount: 'Historical untracked bank', settlementReason: 'This old payment needs a descriptive historical review' };
  assert.match(review(f, external).error, /already exists/);
  assert.ok(settlements.duplicatePayment(f.store, f.exp, { cardId: f.statement.cardId, amount: f.row.amount, date: f.row.date, reference: '' }));
  assert.ok(settlements.duplicatePayment(f.store, f.exp, { cardId: f.statement.cardId, amount: 10, date: '2026-09-01', reference: 'SECOND-REF' }));
});
test('inactive CCP with active orphan transfer still blocks external bypass and repeated payment', () => {
  const f = fixture(); f.payment.active = false;
  const external = { settlementMode: 'external', payingNature: 'PERSONAL', payingAccount: 'Historical untracked bank', settlementReason: 'This is described as a historical payment' };
  assert.match(review(f, external).error, /already exists/);
  assert.ok(settlements.duplicatePayment(f.store, f.exp, { cardId: f.statement.cardId, amount: f.row.amount, date: f.row.date, reference: '' }));
  assert.match(review(f).error, /missing or inactive/);
  f.transfer.active = false;
  assert.equal(review(f, external).error, '');
  assert.equal(settlements.duplicatePayment(f.store, f.exp, { cardId: f.statement.cardId, amount: f.row.amount, date: f.row.date, reference: '' }), '');
});
test('hidden associated transfer metadata is never returned under a visible canonical payment', () => {
  const f = fixture(); f.payment.reference = ''; f.transfer.fromNature = 'PERSONAL'; f.transfer.fromAccount = 'Private Account'; f.transfer.reference = 'PRIVATE-REFERENCE';
  const result = settlements.candidates(f.store, f.exp, f.statement, f.row, access);
  assert.deepEqual(result, []);
  assert.ok(!JSON.stringify(result).includes('PRIVATE-REFERENCE'));
  assert.match(review(f).error, /accessible/);
  const external = { settlementMode: 'external', payingNature: 'SANKI', payingAccount: 'Historical untracked bank', settlementReason: 'This is described as a historical payment' };
  assert.match(review(f, external).error, /already exists/);
});
test('unknown settlement modes and fabricated source IDs are rejected', () => {
  const f = fixture(); assert.ok(review(f, { settlementMode: 'create' }).error); assert.ok(review(f, { settlementSourceId: 'payment:missing' }).error);
});
test('finalization rechecks fingerprint, payer fields, reservations and excludes non-settlement rows', () => {
  const f = fixture(); Object.assign(f.row, review(f).fields); assert.equal(settlements.validateStatement(f.store, f.exp, f.statement, access), '');
  f.row.payingAccount = 'tampered'; assert.match(settlements.validateStatement(f.store, f.exp, f.statement, access), /no longer agree/);
  Object.assign(f.row, review(f).fields); f.transfer.proof = 'changed'; assert.match(settlements.validateStatement(f.store, f.exp, f.statement, access), /changed/);
  const g = fixture(); reserve(g); Object.assign(g.row, { settlementMode: 'link_existing', settlementSourceId: 'payment:CCP-00001' }); assert.match(settlements.validateStatement(g.store, g.exp, g.statement, access), /already linked/);
  const h = fixture(); h.row.classification = 'refund'; assert.equal(settlements.validateStatement(h.store, h.exp, h.statement, access), '');
  h.row.classification = 'card_payment'; h.row.duplicateOf = { statementId: 'previous', rowId: 'same' }; assert.equal(settlements.validateStatement(h.store, h.exp, h.statement, access), '');
});
test('two active rows in one statement cannot claim the same settlement', () => {
  const f = fixture(); Object.assign(f.row, review(f).fields); f.statement.rows.push({ ...f.row, id: 'CCR-two' });
  assert.match(settlements.validateStatement(f.store, f.exp, f.statement, access), /already linked/);
});
test('record-payment duplicate check blocks same reference, date/amount and finalized statement credit', () => {
  const f = fixture(); const body = { cardId: f.payment.cardId, amount: f.payment.amount, date: f.payment.date, reference: f.payment.reference };
  assert.ok(settlements.duplicatePayment(f.store, f.exp, body));
  assert.ok(settlements.duplicatePayment(f.store, f.exp, { ...body, reference: '' }));
  assert.ok(settlements.duplicatePayment(f.store, f.exp, { ...body, amount: 10, date: '2026-09-01' }));
  const g = fixture(); g.store.payments = []; g.exp.transfers = []; g.statement.status = 'finalized';
  assert.ok(settlements.duplicatePayment(g.store, g.exp, { ...body, reference: '' }));
  g.row.duplicateResolution = 'duplicate'; assert.equal(settlements.duplicatePayment(g.store, g.exp, body), '');
});
test('duplicate guard includes generic already-recorded card transfers', () => {
  const f = fixture(); f.store.payments = []; delete f.transfer.creditCardPaymentId;
  assert.ok(settlements.duplicatePayment(f.store, f.exp, { cardId: f.statement.cardId, amount: f.row.amount, date: f.row.date, reference: 'UTR-100' }));
});
test('distinct nonempty references, different cards and unrelated movements remain distinct', () => {
  const f = fixture(), body = { cardId: f.statement.cardId, amount: f.row.amount, date: f.row.date, reference: 'UTR-101' };
  assert.equal(settlements.duplicatePayment(f.store, f.exp, body), '');
  assert.equal(settlements.duplicatePayment(f.store, f.exp, { ...body, reference: 'UTR-100', cardId: 'CC-0002' }), '');
  assert.equal(settlements.duplicatePayment(f.store, f.exp, { ...body, reference: '', date: '2026-08-28' }), '');
  f.store.payments = []; f.transfer.classification = 'internal_transfer'; assert.equal(settlements.duplicatePayment(f.store, f.exp, { ...body, reference: '' }), '');
});
test('obvious card repayment narrations are distinct from ordinary merchant refund credits', () => {
  for (const narration of ['INFINITY PAYMENT RECEIVED, THANK YOU', 'AUTOPAY', 'Payment Received - Thank You', 'Credit card bill payment', 'CARD PAYMENT Axis Bank', 'Payment thank you']) assert.equal(settlements.isSettlementNarration(narration), true, narration);
  for (const narration of ['LINKEDIN refund', 'Merchant payment refund', 'cashback reward', 'IGST reversal', 'Refund for card purchase']) assert.equal(settlements.isSettlementNarration(narration), false, narration);
});

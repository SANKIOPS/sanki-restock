'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const engine = require('../modules/bank-amount-authority');
const cash = require('../modules/bank-movement-authority');
const account = 'IndusInd Bank 7883', oldDate = '2026-10-18', bankDate = '2026-10-19';
const clone = value => JSON.parse(JSON.stringify(value));
function fixture(id, { originalAmount = 500, bankAmount = 499.5, direction = 'debit', mode = 'auto', sourceIds = [] } = {}) {
  const movement = { id, date: oldDate, debit: direction === 'debit' ? originalAmount : 0,
    credit: direction === 'credit' ? originalAmount : 0, entity: 'PERSONAL',
    description: 'Source movement ' + id, reference: 'EXACT-BANK-REFERENCE', ...(sourceIds.length ? { sourceIds } : {}) };
  const bank = { date: bankDate, debit: direction === 'debit' ? bankAmount : 0,
    credit: direction === 'credit' ? bankAmount : 0, reference: 'EXACT-BANK-REFERENCE', description: 'Verified bank movement' };
  const draft = { id: 'BRD-UNIVERSAL', account, nature: 'SAMAST', transactions: [bank], resolutions: {} };
  const store = { expenses: {}, transfers: [], receipts: [], adjustments: [], vendorAdvances: [],
    reconciliationExpenses: [], bankStatements: {}, bankDateOverrides: {}, bankReconciliationLinks: {},
    bankReconciliationDrafts: { [draft.id]: draft }, importedEvidence: { immutable: true, providerAmount: originalAmount }, rawMovements: [movement] };
  const options = { universal: true, mode, bankRowId: 'bank-0', resolveMovement(state, selectedDraft, selectedId) {
    return state.rawMovements.filter(row => row.id === selectedId);
  } };
  return { store, draft, movement, bank, options, make() { return engine.plan(store, draft, bank, movement, options); } };
}
const families = [
  ['salary advance', 'ADV-00051'], ['salary payment', 'SALB-00007-001'], ['incentive', 'INCP-00009'],
  ['reimbursement', 'EX-00009/REIM-001'], ['claimant receiving leg', 'EX-00009/REIM-001/RECEIVED'],
  ['reimbursement batch', 'RB-20261019090000-TEST'], ['vendor advance', 'VADV-00002'],
  ['purchase allocation', 'PO-00004/PPAY-001'], ['purchase batch', 'PPB-123-TEST'],
  ['loan repayment', 'LOANPAY-00004'], ['receivable collection', 'RCV-00007/COL-001'],
  ['personal funding', 'PF-00005'], ['funding repayment', 'PF-00005/REPAY-001'],
  ['customer refund', 'SRF-00002'], ['expense refund', 'RF-00009/MODE-001'],
  ['Paytm payout', 'PTMR-POST-12345'], ['legacy Paytm settlement', 'PTM-AUTO-OLD'],
  ['GoKwik receipt', 'GKW-PAYMENT/12345'], ['Velocity payout', 'VELOCITY-abc123'],
  ['Velocity wallet leg', 'VELOCITY-abc123/WALLET'], ['Shopify receipt', 'SHOPIFY/123/NONCASH'],
  ['local sale', 'SALE/123'], ['future family', 'FUTURE/SOURCE']
];
for (const [family, id] of families) test(family + ' uses one physical bank cash correction without changing imported source evidence', () => {
  const f = fixture(id), evidence = clone(f.store.importedEvidence), rowBefore = clone(f.movement), original = clone(f.store);
  const plan = f.make();
  assert.equal(plan.version, 2);
  assert.equal(plan.sourceType, 'bank_movement');
  assert.equal(plan.effect, 'bank_cash_only');
  assert.equal(plan.originalAmount, 500);
  assert.equal(plan.bankAmount, 499.5);
  assert.equal(plan.sourceNature, 'PERSONAL');
  assert.deepEqual(f.store, original, 'planning must be read only');
  const result = engine.applyAll(f.store, [plan], f.options);
  assert.equal(result.applied, 1);
  assert.equal(result.audits[0].effect, 'bank_cash_only');
  assert.equal(result.audits[0].sourceNature, 'PERSONAL');
  assert.deepEqual(f.store.rawMovements, [rowBefore]);
  assert.deepEqual(f.store.importedEvidence, evidence);
  assert.deepEqual(f.store.adjustments, []);
  assert.deepEqual(f.store.reconciliationExpenses, []);
  assert.deepEqual(f.store.expenses, {});
  const projected = cash.project(f.store, account, 'SAMAST', f.store.rawMovements);
  assert.equal(projected.length, 1);
  assert.equal(projected[0].debit, 499.5);
  assert.equal(projected[0].date, bankDate);
  assert.equal(Object.keys(f.store.bankMovementAuthority).length, 1);
  const retried = engine.applyAll(f.store, [plan], f.options);
  assert.equal(retried.applied, 0);
  assert.equal(retried.alreadyApplied, 1);
  assert.equal(Object.keys(f.store.bankAmountAuthorityApplications).length, 1);
});

test('universal ordinary expenses retain the linked invoice-and-payment correction, not an unnecessary overlay', () => {
  const f = fixture('EX-ORDINARY/PAY-001');
  f.store.expenses['EX-ORDINARY'] = { id: 'EX-ORDINARY', nature: 'PERSONAL', date: oldDate, amount: 500,
    paidAmount: 500, requestedAmount: 500, status: 'paid', approvedAt: oldDate + 'T12:00:00Z',
    payments: [{ id: 'PAY-001', date: oldDate, amount: 500, account }] };
  const plan = f.make();
  assert.equal(plan.version, 1);
  engine.applyAll(f.store, [plan], f.options);
  assert.equal(f.store.expenses['EX-ORDINARY'].amount, 499.5);
  assert.equal(f.store.expenses['EX-ORDINARY'].payments[0].amount, 499.5);
  assert.equal(f.store.bankMovementAuthority, undefined);
});

test('provisional, installment and allocated ordinary payments can still preserve exact bank cash without guessing their document allocation', () => {
  for (const extra of [{ provisional: true }, { batchPaymentId: 'BATCH' }, { creditCardStatementId: 'STATEMENT' }]) {
    const f = fixture('EX-COMPLEX/PAY-001');
    f.store.expenses['EX-COMPLEX'] = { id: 'EX-COMPLEX', nature: 'PERSONAL', date: oldDate, amount: 1000,
      paidAmount: 500, status: 'partially_paid', isInstallment: true, approvedAt: oldDate + 'T12:00:00Z',
      payments: [{ id: 'PAY-001', date: oldDate, amount: 500, account, ...extra }] };
    const sourceBefore = clone(f.store.expenses['EX-COMPLEX']);
    const plan = f.make();
    assert.equal(plan.effect, 'bank_cash_only');
    engine.applyAll(f.store, [plan], f.options);
    assert.deepEqual(f.store.expenses['EX-COMPLEX'], sourceBefore);
    assert.equal(cash.project(f.store, account, 'SAMAST', f.store.rawMovements)[0].debit, 499.5);
    assert.deepEqual(f.store.reconciliationExpenses, []);
  }
});

test('manual matching has no amount cap while automatic matching keeps the inclusive ₹1 threshold', () => {
  for (const bankAmount of [499, 501]) assert.equal(fixture('ADV-ONE', { bankAmount }).make().bankAmount, bankAmount);
  for (const bankAmount of [498.99, 501.01]) assert.throws(() => fixture('ADV-ONE', { bankAmount }).make(), /inclusive ₹1/);
  const f = fixture('ADV-ONE', { bankAmount: 100000.37, mode: 'manual' });
  const result = engine.applyAll(f.store, [f.make()], f.options);
  assert.equal(result.applied, 1);
  assert.equal(cash.project(f.store, account, 'SAMAST', f.store.rawMovements)[0].debit, 100000.37);
});

test('incoming bank-authority cash is positive and cent-precise without changing sale principal', () => {
  const f = fixture('SHOPIFY/123', { bankAmount: 500.37, direction: 'credit' });
  engine.applyAll(f.store, [f.make()], f.options);
  const row = cash.project(f.store, account, 'SAMAST', f.store.rawMovements)[0];
  assert.equal(row.credit, 500.37);
  assert.equal(row.debit, 0);
  assert.equal(f.store.importedEvidence.providerAmount, 500);
});

test('bank, source, duplicate candidate and opposite direction changes fail before any object mutation', () => {
  for (const change of [
    f => { f.bank.debit = 499.49; },
    f => { f.store.rawMovements[0].debit = 501; },
    f => { f.store.rawMovements.push(clone(f.movement)); },
    f => { f.bank.credit = f.bank.debit; f.bank.debit = 0; },
    f => { f.bank.reversal = true; }
  ]) {
    const f = fixture('ADV-ONE'), plan = f.make(); change(f);
    const before = clone(f.store);
    assert.throws(() => engine.applyAll(f.store, [plan], f.options));
    assert.deepEqual(f.store, before);
  }
});

test('a stale second plan cannot partially apply the first source correction', () => {
  const f = fixture('ADV-FIRST'), first = f.make();
  const secondBank = { ...f.bank, reference: 'SECOND', debit: 750.25 }, secondMovement = { ...f.movement, id: 'ADV-SECOND', debit: 750 };
  f.store.rawMovements.push(secondMovement); f.draft.transactions.push(secondBank);
  const second = engine.plan(f.store, f.draft, secondBank, secondMovement, { ...f.options, bankRowId: 'bank-1' });
  secondMovement.debit = 751;
  const before = clone(f.store);
  assert.throws(() => engine.applyAll(f.store, [first, second], f.options));
  assert.deepEqual(f.store, before);
});

test('an aggregate and its child cannot both acquire cash corrections in one finalization', () => {
  const f = fixture('RB-AGGREGATE', { sourceIds: ['ADV-A', 'ADV-B'] }), first = f.make();
  const child = { ...f.movement, id: 'ADV-A', debit: 200, sourceIds: [] };
  const childBank = { ...f.bank, reference: 'CHILD-REFERENCE', debit: 199.5 };
  f.store.rawMovements.push(child); f.draft.transactions.push(childBank);
  const second = engine.plan(f.store, f.draft, childBank, child, { ...f.options, bankRowId: 'bank-1' });
  const before = clone(f.store);
  assert.throws(() => engine.applyAll(f.store, [first, second], f.options), /overlap|twice|same|already/i);
  assert.deepEqual(f.store, before);
});

test('same-bank finalized history, a foreign draft claim and an existing authority remain locked', () => {
  for (const protect of [
    f => { f.store.bankStatements[account] = { imports: [{ id: 'FINAL', finalizedAt: oldDate, reconciliationRows: [{ linkedRecordIds: [f.movement.id] }] }] }; },
    f => { f.store.bankReconciliationDrafts.OTHER = { id: 'OTHER', account, resolutions: { 'bank-0': { appId: f.movement.id } } }; },
    f => { f.store.bankDateOverrides[f.movement.id] = { reconciliationDraft: 'FINAL', bankTransactionId: 'BTX-FINAL' }; },
    f => { f.store.bankMovementAuthority = { [cash.key(account, 'SAMAST', f.movement.id)]: { movementId: f.movement.id } }; }
  ]) {
    const f = fixture('ADV-ONE'); protect(f);
    const before = clone(f.store);
    assert.throws(() => f.make());
    assert.deepEqual(f.store, before);
  }
});

test('a finalized opposite transfer leg stays unchanged while this physical bank receives its own exact cash fact', () => {
  const f = fixture('TR-TWO-BANKS');
  f.store.transfers.push({ id: f.movement.id, nature: 'SAMAST', fromNature: 'SAMAST', toNature: 'SAMAST',
    classification: 'internal_transfer', fromAccount: account, toAccount: 'Axis Bank 3448', date: oldDate, amount: 500 });
  f.store.bankStatements['Axis Bank 3448'] = { imports: [{ id: 'OTHER-FINAL', finalizedAt: oldDate,
    reconciliationRows: [{ id: 'bank-0', bank: { date: oldDate, debit: 0, credit: 500 }, linkedRecordIds: [f.movement.id] }] }] };
  const counterpart = clone(f.store.bankStatements['Axis Bank 3448']), source = clone(f.store.transfers);
  const plan = f.make();
  assert.equal(plan.effect, 'bank_cash_only');
  engine.applyAll(f.store, [plan], f.options);
  assert.deepEqual(f.store.transfers, source);
  assert.deepEqual(f.store.bankStatements['Axis Bank 3448'], counterpart);
  assert.equal(cash.project(f.store, account, 'SAMAST', f.store.rawMovements)[0].debit, 499.5);
  assert.equal(cash.delta(f.store, 'Axis Bank 3448', 'SAMAST'), 0);
});

test('a foreign-account bank-date link is ignored only when its opposite physical-bank provenance is proved', () => {
  const f = fixture('TR-PROVEN-OTHER');
  f.store.transfers.push({ id: f.movement.id, nature: 'SAMAST', classification: 'internal_transfer',
    fromAccount: account, toAccount: 'Axis Bank 3448', amount: 500, date: oldDate });
  f.store.bankStatements['Axis Bank 3448'] = { transactions: { prior: { id: 'BTX-OTHER', date: oldDate,
    debit: 0, credit: 500, reconciliationDraftId: 'BRD-OTHER' } }, imports: [{ id: 'BST-OTHER', draftId: 'BRD-OTHER',
    finalizedAt: oldDate, reconciliationRows: [{ id: 'bank-0', bank: { transactionId: 'BTX-OTHER', date: oldDate, credit: 500 }, linkedRecordIds: [f.movement.id] }] }] };
  f.store.bankDateOverrides[f.movement.id] = { bankDate: oldDate, bankTransactionId: 'BTX-OTHER', reconciliationDraft: 'BRD-OTHER' };
  const prior = clone(f.store.bankDateOverrides), transfer = clone(f.store.transfers);
  const plan = f.make();
  assert.equal(plan.effect, 'bank_cash_only');
  engine.applyAll(f.store, [plan], f.options);
  assert.deepEqual(f.store.bankDateOverrides, prior);
  assert.deepEqual(f.store.transfers, transfer);
  assert.equal(cash.project(f.store, account, 'SAMAST', f.store.rawMovements)[0].debit, 499.5);
});

test('another account may review the counterpart while this bank stages its own cash correction', () => {
  const f = fixture('TR-ACTIVE-COUNTERPART');
  f.store.transfers.push({ id: f.movement.id, nature: 'SAMAST', classification: 'internal_transfer',
    fromAccount: account, toAccount: 'Axis Bank 3448', amount: 500, date: oldDate });
  f.store.bankReconciliationDrafts.OTHER = { id: 'OTHER', account: 'Axis Bank 3448', nature: 'SAMAST',
    resolutions: { 'bank-0': { appId: f.movement.id, action: 'link_existing' } } };
  const other = clone(f.store.bankReconciliationDrafts.OTHER);
  const plan = f.make();
  assert.equal(plan.effect, 'bank_cash_only');
  engine.applyAll(f.store, [plan], f.options);
  assert.deepEqual(f.store.bankReconciliationDrafts.OTHER, other);
});

test('orphaned finalized source markers fail closed instead of being bypassed by universal fallback', () => {
  for (const marker of [{ bankTransactionId: 'BTX-UNKNOWN' }, { bankStatementEvidence: true },
    { reconciliationDraft: 'BRD-UNKNOWN' }, { bankReconciliationDraft: 'BRD-UNKNOWN' }]) {
    const f = fixture('TR-ORPHANED');
    f.store.transfers.push({ id: f.movement.id, nature: 'SAMAST', classification: 'internal_transfer',
      fromAccount: account, toAccount: 'Axis Bank 3448', amount: 500, date: oldDate, ...marker });
    const before = clone(f.store);
    assert.throws(() => f.make(), /protected|finalized|evidence|statement|bank/i);
    assert.deepEqual(f.store, before);
  }
});

test('an orphaned finalized payment marker remains locked even when its invoice is a supported family', () => {
  const f = fixture('EX-ORPHANED/PAY-001');
  f.store.expenses['EX-ORPHANED'] = { id: 'EX-ORPHANED', nature: 'PERSONAL', date: oldDate, amount: 500,
    paidAmount: 500, requestedAmount: 500, status: 'paid', approvedAt: oldDate + 'T12:00:00Z',
    payments: [{ id: 'PAY-001', date: oldDate, amount: 500, account, bankTransactionId: 'BTX-UNKNOWN' }] };
  const before = clone(f.store);
  assert.throws(() => f.make(), /protected|finalized|evidence|statement|bank/i);
  assert.deepEqual(f.store, before);
});

function cardFixture() {
  const f = fixture('TR-CARD', { originalAmount: 26701, bankAmount: 26700.63 });
  const transfer = { id: 'TR-CARD', nature: 'PERSONAL', fromNature: 'PERSONAL', toNature: 'PERSONAL',
    classification: 'credit_card_payment', creditCardPaymentId: 'CCP-ONE', fromAccount: account,
    toAccount: 'HDFC 7376', amount: 26701, date: oldDate, proof: '/existing-proof.jpg' };
  const payment = { id: 'CCP-ONE', cardId: 'CC-ONE', nature: 'PERSONAL', account, amount: 26701,
    date: oldDate, active: true, allocations: [{ nature: 'PERSONAL', amount: 26701 }], reference: 'OLD-REFERENCE' };
  const cards = { cards: { 'CC-ONE': { id: 'CC-ONE', name: 'HDFC', last4: '7376', active: true } },
    payments: [payment], statements: {}, audit: [] };
  f.store.transfers = [transfer];
  f.store.creditCardSettlementAllocations = [{ id: 'CCA-ONE', creditCardPaymentId: payment.id, cardId: payment.cardId,
    cardName: transfer.toAccount, date: oldDate, amount: 26701, payerNature: 'PERSONAL',
    beneficiaryNature: 'PERSONAL', payingAccount: account, status: 'self' }];
  f.options.externalStores = { creditCards: cards };
  f.options.externalStoreText = { creditCards: JSON.stringify(cards) };
  return { ...f, cards, transfer, payment };
}

test('native credit-card authority stages all linked corrections and returns one external-store commit without touching purchases', () => {
  const f = cardFixture(), externalBefore = clone(f.cards);
  const plan = f.make();
  assert.equal(plan.version, 2);
  assert.equal(plan.sourceType, 'credit_card_payment');
  assert.equal(plan.effect, 'linked_sources');
  assert.equal(plan.patches.length, 3);
  const result = engine.applyAll(f.store, [plan], f.options);
  assert.equal(f.store.transfers[0].amount, 26700.63);
  assert.equal(f.store.transfers[0].date, bankDate);
  assert.equal(f.store.creditCardSettlementAllocations[0].amount, 26700.63);
  assert.deepEqual(f.cards, externalBefore, 'the pure engine does not write the external store before atomic commit');
  assert.equal(result.externalStoreChanges.length, 1);
  const changedCards = JSON.parse(result.externalStoreChanges[0].after);
  assert.equal(changedCards.payments[0].amount, 26700.63);
  assert.equal(changedCards.payments[0].date, bankDate);
  assert.equal(changedCards.payments[0].allocations[0].amount, 26700.63);
  assert.deepEqual(changedCards.statements, {});
  assert.deepEqual(f.store.expenses, {});
  assert.deepEqual(f.store.adjustments, []);
  assert.equal(f.store.bankMovementAuthority, undefined);
});

test('finalized card-statement evidence remains untouched while actual source-bank cash stays authoritative', () => {
  const f = cardFixture();
  f.cards.statements.FINAL = { id: 'FINAL', cardId: 'CC-ONE', status: 'finalized', rows: [{ id: 'ROW',
    classification: 'card_payment', creditCardPaymentId: 'CCP-ONE', credit: 26701, amount: 26701, date: oldDate, narration: 'PAYMENT RECEIVED' }] };
  const externalBefore = clone(f.cards), transferBefore = clone(f.transfer);
  const plan = f.make();
  assert.equal(plan.effect, 'bank_cash_only');
  assert.equal(plan.retainedCardEvidence, true);
  const result = engine.applyAll(f.store, [plan], f.options);
  assert.equal(result.externalStoreChanges.length, 0);
  assert.deepEqual(f.cards, externalBefore);
  assert.deepEqual(f.store.transfers[0], transferBefore);
  assert.equal(cash.project(f.store, account, 'SAMAST', f.store.rawMovements)[0].debit, 26700.63);
  assert.deepEqual(f.store.reconciliationExpenses, []);
});

test('a card statement finalized after preview invalidates the native plan without a partial transfer correction', () => {
  const f = cardFixture(), plan = f.make();
  f.cards.statements.FINAL = { id: 'FINAL', cardId: 'CC-ONE', status: 'finalized', rows: [{ id: 'ROW',
    classification: 'card_payment', creditCardPaymentId: 'CCP-ONE', credit: 26701, amount: 26701, date: oldDate }] };
  const before = clone(f.store), cardsBefore = clone(f.cards);
  assert.throws(() => engine.applyAll(f.store, [plan], f.options), /changed|review/i);
  assert.deepEqual(f.store, before);
  assert.deepEqual(f.cards, cardsBefore);
});

test('a stale second source cannot partially apply a native card transfer or external card payment', () => {
  const f = cardFixture(), first = f.make();
  const secondBank = { ...f.bank, reference: 'SECOND', debit: 750.25 }, secondMovement = { ...f.movement, id: 'ADV-STALE', debit: 750 };
  f.store.rawMovements.push(secondMovement); f.draft.transactions.push(secondBank);
  const second = engine.plan(f.store, f.draft, secondBank, secondMovement, { ...f.options, bankRowId: 'bank-1' });
  secondMovement.debit = 751;
  const before = clone(f.store), cardsBefore = clone(f.cards);
  assert.throws(() => engine.applyAll(f.store, [first, second], f.options));
  assert.deepEqual(f.store, before);
  assert.deepEqual(f.cards, cardsBefore);
});

test('a tampered recomputed plan cannot introduce arbitrary patches or private snapshots', () => {
  const f = fixture('ADV-ONE'), plan = clone(f.make());
  plan.patches = [{ store: 'expenses', path: ['transfers'], before: [], after: [{ id: 'FAKE-PAYMENT' }] }];
  const body = { ...plan }; delete body.id;
  plan.id = 'BAA-' + engine.fingerprint(body).slice(0, 32);
  const before = clone(f.store);
  assert.throws(() => engine.applyAll(f.store, [plan], f.options), /changed|review/i);
  assert.deepEqual(f.store, before);
});

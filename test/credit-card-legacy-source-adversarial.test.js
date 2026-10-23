'use strict';
// Fictional records only. No files, production snapshots, routes or network access.
const test = require('node:test');
const assert = require('node:assert/strict');
const settlements = require('../modules/credit-card-settlements');
const clone = value => JSON.parse(JSON.stringify(value));
const account = 'ICICI Bank 0993', date = '2098-07-20', amount = 1234.56;
const expenseId = 'EX-SYNTHETIC-REPAYMENT', paymentId = 'PAY-SYNTHETIC', sourceId = 'legacy_expense_payment:' + expenseId + '/' + paymentId;
const access = (nature, name) => nature === 'PERSONAL' && name === account;
function fixture() {
  const card = { id: 'CC-SYNTHETIC-LEGACY', name: 'Synthetic Test Card', last4: '9001', active: true };
  const row = { id: 'CCR-SYNTHETIC', date, postingDate: date, narration: 'PAYMENT RECEIVED THANK YOU', reference: '', amount, credit: amount, debit: 0, classification: 'card_payment' };
  const statement = { id: 'CCS-SYNTHETIC-LEGACY', cardId: card.id, status: 'review', reviewRevision: 5, rows: [row] };
  const payment = { id: paymentId, date, amount, account, paymentType: 'UPI', personalFunds: true, proof: '/synthetic-proof.jpg', bankReference: 'SYNTHETIC-BANK-REF' };
  const expense = { id: expenseId, nature: 'PERSONAL', ledger: 'Credit Card', vendor: 'Credit card Payment 9001', particulars: 'Credit card Payment', telegramNarration: 'Credit card -9001 Payment',
    amount, paidAmount: amount, personalPaidAmount: amount, date, account, status: 'paid', approvedAt: '2098-07-20T12:00:00.000Z', paidAlready: true, fundedBy: 'claimant', paymentType: 'UPI',
    reimbursementStatus: 'not_applicable', reimbursementAmount: 0, reimbursementPayments: [], payments: [payment] };
  const bank = { transactionId: 'BTX-SYNTHETIC', date, debit: amount, credit: 0, reference: 'SYNTHETIC-BANK-REF', description: 'Synthetic repayment bank evidence' };
  const link = { id: 'bank-0', bank, ledger: { id: expenseId + '/' + paymentId, date, debit: amount, credit: 0 }, linkedRecordIds: [expenseId + '/' + paymentId], decision: 'Matched with ledger entry' };
  const record = { id: 'BST-SYNTHETIC', draftId: 'BRD-SYNTHETIC', finalizedAt: '2098-07-21T12:00:00.000Z', reconciliationRows: [link] };
  const store = { cards: { [card.id]: card }, payments: [], statements: { [statement.id]: statement } };
  const exp = { expenses: { [expense.id]: expense }, receipts: [], transfers: [], expenseRefunds: {}, bankReconciliationDrafts: {}, bankStatements: { ['PERSONAL|' + account]: { imports: [record] } },
    bankDateOverrides: { [expenseId + '/' + paymentId]: { originalDate: date, bankDate: date, reconciliationDraft: record.draftId } }, bankReconciliationLinks: {}, bankMovementAuthority: {} };
  return { store, exp, statement, card, row, expense, payment, bank, link, record };
}
function candidate(f, checker = access) { return settlements.candidates(f.store, f.exp, f.statement, f.row, checker).find(value => value.id === sourceId); }
function input(f, patch = {}) { const value = candidate(f); return { settlementMode: 'link_existing', settlementSourceId: sourceId, settlementSourceFingerprint: value && value.fingerprint || 'unavailable', settlementLegacyConfirmed: true, settlementReason: 'Synthetic verified historical repayment, not a merchant expense.', ...patch }; }
function review(f, patch = {}, checker = access) { return settlements.reviewSettlement(f.store, f.exp, f.statement, f.row, input(f, patch), checker); }
function unavailable(f, label = '') {
  assert.notEqual(candidate(f) && candidate(f).eligible, true, 'unsafe source must not be linkable: ' + label);
  assert.ok(review(f).error, 'forging the known legacy source ID must still fail server validation');
}

test('legacy repayment binds only evidence and leaves the expense, payment and finalized bank record unchanged', () => {
  const f = fixture(), before = clone(f), value = candidate(f);
  assert.ok(value, 'explicit finalized historical repayment is discoverable');
  assert.equal(value.eligible, true); assert.equal(value.sourceKind, 'legacy_expense_payment'); assert.equal(value.legacyConfirmationRequired, true);
  const result = review(f); assert.equal(result.error, '', result.error);
  assert.equal(result.fields.settlementSourceId, sourceId); assert.equal(result.fields.settlementLegacyConfirmed, true);
  assert.equal(result.fields.payingNature, 'PERSONAL'); assert.equal(result.fields.payingAccount, account);
  assert.deepEqual(f, before, 'source selection is statement metadata, not a rewrite or second cash movement');
});

test('legacy source requires a real affirmative acknowledgment and meaningful explanation', () => {
  for (const patch of [{ settlementLegacyConfirmed: false }, { settlementLegacyConfirmed: undefined }, { settlementLegacyConfirmed: 'true' }, { settlementLegacyConfirmed: 1 }, { settlementReason: '' }, { settlementReason: 'short' }]) {
    const f = fixture(), before = clone(f); assert.ok(review(f, patch).error, JSON.stringify(patch)); assert.deepEqual(f, before);
  }
});

test('ordinary expenses, ambiguous card identities and credit-funded purchases cannot become repayment aliases', () => {
  const patches = [
    f => { f.card.id = 'CC-SYNTHETIC-WRONG-IDENTITY'; },
    f => { f.expense.nature = 'SANKI'; },
    f => { f.expense.ledger = 'Subscriptions'; },
    f => { f.expense.vendor = 'Synthetic merchant 9001'; f.expense.particulars = 'Purchase'; f.expense.telegramNarration = 'Purchase'; },
    f => { f.expense.vendor = 'Credit card Payment 19001'; f.expense.telegramNarration = 'Credit card -19001 Payment'; },
    f => { f.store.cards.OTHER = { id: 'OTHER', name: 'Another hidden card', last4: '9001', active: false, ownerOnly: true }; },
    f => { f.payment.paymentType = 'Credit'; },
    f => { f.expense.creditCardId = f.card.id; f.payment.creditCardId = f.card.id; }
  ];
  for (const patch of patches) { const f = fixture(); patch(f); const before = clone(f); unavailable(f, String(patch)); assert.deepEqual(f, before); }
});

test('reimbursable, refunded, split, incomplete and excluded sources fail closed', () => {
  const patches = [
    f => { f.expense.reimbursementStatus = 'pending'; },
    f => { f.expense.reimbursementAmount = 1; },
    f => { f.expense.reimbursementPayments = [{ id: 'SYNTHETIC-REIM', amount: 1 }]; },
    f => { f.expense.paidAmount = amount - 0.01; },
    f => { f.expense.personalPaidAmount = amount - 0.01; },
    f => { f.expense.payments.push({ ...f.payment, id: 'PAY-SPLIT', amount: 1 }); },
    f => { f.payment.batchPaymentId = 'SYNTHETIC-BATCH'; },
    f => { f.expense.isInstallment = true; },
    f => { f.payment.personalFunds = false; },
    f => { f.expense.paidAlready = false; },
    f => { f.expense.fundedBy = 'company'; },
    f => { f.expense.active = false; },
    f => { f.payment.active = false; },
    f => { f.expense.deletedAt = '2098-07-22T12:00:00.000Z'; },
    f => { f.payment.reversedAt = '2098-07-22T12:00:00.000Z'; },
    f => { f.payment.creditOnly = true; },
    f => { f.expense.accountingExcluded = true; },
    f => { f.payment.accountingExcluded = true; },
    f => { f.expense.status = 'rejected'; },
    f => { f.payment.proof = ''; f.expense.paymentProof = ''; f.expense.purchasePaymentProof = ''; },
    f => { f.exp.expenseRefunds.REF = { id: 'REF', expenseId, paymentId, amount: 1, status: 'received' }; }
  ];
  for (const patch of patches) { const f = fixture(); patch(f); const before = clone(f); unavailable(f, String(patch)); assert.deepEqual(f, before); }
});

test('finalized bank evidence must name the exact qualified payment and retain its full amount and money direction', () => {
  const patches = [
    f => { delete f.record.finalizedAt; },
    f => { f.link.linkedRecordIds = ['EX-DIFFERENT/' + paymentId]; f.link.ledger.id = 'EX-DIFFERENT/' + paymentId; },
    f => { f.link.linkedRecordIds.push('EX-OTHER/PAY-OTHER'); },
    f => { f.bank.debit -= 0.01; },
    f => { f.bank.credit = 1; },
    f => { f.link.ledger.debit -= 0.01; },
    f => { f.bank.date = '2098-07-21'; },
    f => { const duplicate = { ...clone(f.record), id: 'BST-DUPLICATE', draftId: 'BRD-DUPLICATE' }; duplicate.reconciliationRows[0].bank.transactionId = 'BTX-DIFFERENT'; f.exp.bankStatements['PERSONAL|' + account].imports.push(duplicate); }
  ];
  for (const patch of patches) { const f = fixture(); patch(f); const before = clone(f); unavailable(f, String(patch)); assert.deepEqual(f, before); }
});

test('a finalized legacy source cannot be claimed by a second card or another active review', () => {
  const f = fixture(), linked = review(f); assert.equal(linked.error, '');
  f.store.cards.OTHER = { id: 'OTHER', name: 'Synthetic different card', last4: '9002', active: true };
  f.store.statements.OTHER = { id: 'OTHER', cardId: 'OTHER', status: 'review', rows: [{ ...clone(f.row), ...linked.fields, id: 'CCR-OTHER' }] };
  const before = clone(f); unavailable(f); assert.deepEqual(f, before);
});

test('conflicting effective-bank authority and new active-draft claims cannot be bypassed by the old finalized source', () => {
  const patches = [
    f => { f.exp.bankDateOverrides[expenseId + '/' + paymentId].bankDate = '2098-07-21'; },
    f => { f.exp.bankMovementAuthority[JSON.stringify([account.toLowerCase(), expenseId + '/' + paymentId])] = {
      account, nature: 'PERSONAL', movementId: expenseId + '/' + paymentId, sourceNature: 'PERSONAL', planId: 'SYNTHETIC-CONFLICT',
      original: { date, debit: amount, credit: 0 }, after: { date, debit: amount + 0.01, credit: 0 }
    }; },
    f => { f.exp.bankReconciliationDrafts.FOREIGN = { id: 'FOREIGN', account, nature: 'PERSONAL', transactions: [{ date, debit: amount, credit: 0 }], resolutions: { 'bank-0': { action: 'link_existing', appId: expenseId + '/' + paymentId } } }; }
  ];
  for (const patch of patches) { const f = fixture(); patch(f); const before = clone(f); unavailable(f); assert.deepEqual(f, before); }
});

test('nested aggregate aliases in finalized evidence cannot conceal a second source allocation', () => {
  const patches = [
    f => { f.link.ledger.sourceIds = ['EX-OTHER/PAY-OTHER']; },
    f => { f.link.ledger.linkedEntryIds = ['EX-OTHER/PAY-OTHER']; },
    f => { f.link.ledger.linkedEntries = [{ id: 'EX-OTHER/PAY-OTHER', debit: 1, credit: 0, date }]; },
    f => { f.link.linkedEntryIds = ['EX-OTHER/PAY-OTHER']; }
  ];
  for (const patch of patches) { const f = fixture(); patch(f); const before = clone(f); unavailable(f, String(patch)); assert.deepEqual(f, before); }
});

test('nested bank-authority and active-draft aliases must not escape protected-source checks', () => {
  const patches = [
    f => { f.exp.bankMovementAuthority['synthetic-aggregate-authority'] = {
      account, nature: 'PERSONAL', movementId: 'SYNTHETIC-AGGREGATE', linkedEntries: [{ id: expenseId + '/' + paymentId }],
      original: { date, debit: amount, credit: 0 }, after: { date, debit: amount + 0.01, credit: 0 }
    }; },
    f => { f.exp.bankReconciliationDrafts.FOREIGN = { id: 'FOREIGN', account, nature: 'PERSONAL', resolutions: { 'bank-0': { action: 'link_existing', linkedEntries: [{ id: expenseId + '/' + paymentId }] } } }; }
  ];
  for (const patch of patches) { const f = fixture(); patch(f); const before = clone(f); unavailable(f, String(patch)); assert.deepEqual(f, before); }
});

test('a separately captured CCP for the same physical cash debit cannot make an independent legacy source', () => {
  for (const cardId of ['CC-SYNTHETIC-LEGACY', 'CC-OTHER']) {
    const f = fixture(); f.store.cards['CC-OTHER'] = { id: 'CC-OTHER', name: 'Different card', last4: '9002', active: true };
    f.store.payments.push({ id: 'CCP-SYNTHETIC-DOUBLE', cardId, amount, date, nature: 'PERSONAL', account, reference: 'SYNTHETIC-BANK-REF', active: true });
    const before = clone(f); unavailable(f); assert.deepEqual(f, before);
  }
});

test('a damaged dedicated payment cannot hide an explicit qualified legacy-source identity', () => {
  for (const alias of [{ sourceExpenseId: expenseId, sourcePaymentId: paymentId }, { sourceId: expenseId + '/' + paymentId }, { expensePaymentId: expenseId + '/' + paymentId }]) {
    const f = fixture(); f.store.payments.push({ id: 'CCP-SYNTHETIC-DAMAGED', cardId: 'CC-OTHER', active: true, account: '', amount: null, date: '', ...alias });
    const before = clone(f); unavailable(f, JSON.stringify(alias)); assert.deepEqual(f, before);
  }
});

test('legacy source privacy uses current permissions and cannot be bypassed with a known source ID', () => {
  const f = fixture(), known = input(f);
  for (const checker of [undefined, () => false, () => 'true', () => { throw Error('restricted'); }]) {
    const before = clone(f); assert.deepEqual(settlements.candidates(f.store, f.exp, f.statement, f.row, checker), []);
    assert.ok(settlements.reviewSettlement(f.store, f.exp, f.statement, f.row, known, checker).error); assert.deepEqual(f, before);
  }
});

test('legacy fingerprints revalidate source descriptions, proof, payment and finalized bank evidence without rewriting any of them', () => {
  const patches = [
    f => { f.payment.proof = '/synthetic-replaced-proof.jpg'; },
    f => { f.expense.particulars += ' edited'; },
    f => { f.bank.reference += '-EDITED'; },
    f => { f.record.finalizedAt = '2098-07-22T12:00:00.000Z'; }
  ];
  for (const patch of patches) {
    const f = fixture(), saved = input(f); patch(f); const before = clone(f);
    assert.ok(settlements.reviewSettlement(f.store, f.exp, f.statement, f.row, saved, access).error); assert.deepEqual(f, before);
  }
});

test('legacy links never support expense offsets, forged payer fields or bypassing pending source review', () => {
  const f = fixture(), result = review(f, { payingNature: 'SANKI', payingAccount: 'Forged account', category: 'Sales offset', type: 'variable', expenseRefundReceiptId: 'REFUND-FORGED' });
  assert.equal(result.error, ''); assert.equal(result.fields.payingNature, 'PERSONAL'); assert.equal(result.fields.payingAccount, account);
  assert.equal(result.fields.category, undefined); assert.equal(result.fields.expenseRefundReceiptId, undefined);
  Object.assign(f.row, result.fields); assert.equal(settlements.validateStatement(f.store, f.exp, f.statement, access), '');
  f.row.settlementLegacyConfirmed = false; assert.ok(settlements.validateStatement(f.store, f.exp, f.statement, access));
});

test('legacy evidence still blocks external-source and duplicate-payment workarounds when linking is unavailable or private', () => {
  const external = { settlementMode: 'external', payingNature: 'PERSONAL', payingAccount: 'Historical synthetic account outside this app', settlementReason: 'Synthetic external description must not bypass this recorded source.' };
  for (const patch of [() => {}, f => { f.payment.proof = ''; }, f => { f.link.linkedRecordIds.push('EX-OTHER/PAY-OTHER'); }]) {
    const f = fixture(); patch(f); const before = clone(f);
    assert.ok(review(f, external).error);
    assert.ok(review(f, external, () => false).error, 'hidden sources still collision-protect the recorded money movement');
    assert.ok(settlements.duplicatePayment(f.store, f.exp, { cardId: f.card.id, amount, date, reference: 'SYNTHETIC-BANK-REF' }));
    assert.deepEqual(f, before);
  }
});

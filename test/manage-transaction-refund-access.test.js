'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const authorize = require('../modules/manage-transaction-refund-access');
const refunds = require('../modules/expense-refunds');
const { cardExpenseRecords } = require('../modules/credit-card-accounting');
const clone = value => JSON.parse(JSON.stringify(value));
const req = { user: { username: 'synthetic-manager' } };
const deps = {
  natures: request => request.personal ? ['SANKI', 'SAMAST', 'PERSONAL'] : ['SANKI', 'SAMAST'],
  canPersonal: request => !!request.personal,
  accountVisible: (request, account) => !String(account).includes('Restricted'),
  personalAccount: account => String(account).includes('Private'),
  canViewExpense: (request, expense) => !expense.hidden,
  canAccessExpense: (request, expense) => !expense.ownerOnly || !!request.personal,
  canAccessCard: (request, card) => !card.ownerOnly || !!request.personal
};
function fixture(options = {}) {
  const expense = { id: 'EX-00001', nature: options.nature || 'SANKI', date: '2098-06-01', vendor: 'Synthetic supplier', amount: 500, paidAmount: options.unpaid ? 0 : 500,
    status: options.unpaid ? 'approved' : 'paid', payments: options.unpaid ? [] : [{ id: 'PAY-001', amount: 500, date: '2098-06-02', account: 'Synthetic bank', personalFunds: false }] };
  const store = { expenses: { [expense.id]: expense }, reconciliationExpenses: [], receipts: [], expenseRefunds: [], vendorAdvances: [] };
  const cards = { cards: {}, statements: {} };
  const context = { today: '2098-06-30', username: req.user.username, expenses: [expense], checkSource() {}, checkRecord() {}, checkAccount: (sources, component) => component.account, checkExistingMovement() {} };
  const input = { requestId: 'synthetic-request', date: '2098-06-10', reason: 'Synthetic return', reasonType: 'return', status: options.pending ? 'pending' : 'received',
    sources: [{ key: 'expense:' + expense.id, amount: 100 }], components: [{ mode: options.mode || 'bank', receiver: 'company', amount: 100, account: 'Synthetic receiving bank', reference: 'SYNTHETIC-CREDIT' }] };
  const record = refunds.post(store, refunds.prepare(store, input, context), { username: req.user.username }).refund;
  return { snapshot: { store, cards }, record, expense };
}
function freeze(value) { if (value && typeof value === 'object') { Object.freeze(value); for (const child of Object.values(value)) freeze(child); } return value; }
function allowed(f, request = req, policy = deps) { return authorize(request, f.record, f.snapshot, policy); }

test('received company refund authorizes without modifying frozen source or request', () => {
  const f = fixture(), before = JSON.stringify(f); freeze(f); freeze(req);
  assert.equal(allowed(f), true); assert.equal(JSON.stringify(f), before);
});
test('pending return against an unpaid expense needs no receiving account or payments', () => {
  const f = fixture({ pending: true, unpaid: true }); assert.equal(f.record.components.length, 0); assert.equal(allowed(f), true);
});
test('credit notes and vouchers without money-receiving accounts remain visible', () => {
  for (const mode of ['credit_note', 'voucher', 'store_credit', 'vendor_credit']) assert.equal(allowed(fixture({ mode, unpaid: mode === 'credit_note' })), true, mode);
});
test('same source permission checks apply to both pending and received refunds', () => {
  for (const pending of [false, true]) { const f = fixture({ pending }); f.expense.ownerOnly = true; assert.equal(allowed(f), false); assert.equal(allowed(f, { personal: true }), true); }
});

const blocked = {
  'hidden expense': f => { f.expense.hidden = true; },
  'missing expense': f => { f.snapshot.store.expenses = {}; },
  'duplicate expense ID across source families': f => { f.snapshot.store.reconciliationExpenses.push(clone(f.expense)); },
  'normalized duplicate expense identity': f => { f.snapshot.store.expenses.alias = { ...f.expense, id: 'EX-1' }; },
  'missing saved payment': f => { f.expense.payments = []; f.expense.paidAmount = 0; },
  'duplicate original payment ID': f => { f.expense.payments.push(clone(f.expense.payments[0])); },
  'restricted current paying account': f => { f.expense.payments[0].account = 'Restricted bank'; },
  'restricted copied paying account': f => { f.record.sources[0].payments = [{ ...f.record.sources[0].paymentAllocations[0], account: 'Restricted bank' }]; },
  'restricted allocated paying account': f => { f.record.sources[0].paymentAllocations[0].account = 'Restricted bank'; },
  'receiving account restricted': f => { f.record.components[0].account = 'Restricted bank'; },
  'physically personal current account': f => { f.expense.payments[0].account = 'Private bank'; },
  'personal source flag': f => { f.record.sources[0].personal = true; },
  'personal original payment flag': f => { f.expense.payments[0].personalFunds = true; },
  'personal refund receiver': f => { f.record.components[0].receiver = 'payer'; },
  'personal receiving movement flag': f => { f.snapshot.store.receipts[0].personalRefund = true; },
  'source entity mismatch': f => { f.record.sources[0].nature = 'SAMAST'; },
  'invalid original entity': f => { f.expense.nature = 'UNKNOWN'; },
  'missing original entity is not defaulted': f => { delete f.expense.nature; },
  'unknown receiving mode': f => { f.record.components[0].mode = 'unknown'; },
  'missing receiving movement': f => { f.snapshot.store.receipts = []; },
  'duplicate receiving movement': f => { f.snapshot.store.receipts.push(clone(f.snapshot.store.receipts[0])); },
  'receiving movement scope differs': f => { f.snapshot.store.receipts[0].nature = 'PERSONAL'; },
  'duplicate refund identity': f => { f.snapshot.store.expenseRefunds.push(clone(f.record)); },
  'duplicate original source within refund': f => { f.record.sources.push(clone(f.record.sources[0])); },
  'private claimant funding source': f => { f.expense.claimantFundingAccount = 'Private bank'; },
  'missing parent identity': f => { f.record.parentId = 'RF-MISSING'; },
  'cyclic parent identity': f => { f.record.parentId = f.record.id; },
  'malformed source list': f => { f.record.sources = {}; },
  'malformed receiving list': f => { f.record.components = [null]; },
  'malformed original payments': f => { f.expense.payments = {}; },
  'unsupported original source': f => { f.record.sources[0].kind = 'other'; },
  'nonexistent explicit card ID': f => { f.expense.creditCardId = 'CC-MISSING'; }
};
for (const [name, change] of Object.entries(blocked)) test('fails closed: ' + name, () => { const f = fixture(); change(f); const before = JSON.stringify(f); assert.equal(allowed(f), false); assert.equal(JSON.stringify(f), before); });

test('PERSONAL entity requires explicit permission even when natures callback includes it', () => {
  const f = fixture({ nature: 'PERSONAL' }); assert.equal(allowed(f), false);
  assert.equal(allowed(f, req, { ...deps, natures: () => ['PERSONAL'] }), false);
  assert.equal(allowed(f, { personal: true }), true);
});
test('missing or throwing authorization callbacks fail closed', () => {
  const f = fixture();
  f.snapshot.cards.cards.CC1 = { id: 'CC1', name: 'Synthetic', last4: 'bank' };
  for (const name of Object.keys(deps)) { const missing = { ...deps }; delete missing[name]; assert.equal(allowed(f, req, missing), false, name); assert.equal(allowed(f, req, { ...deps, [name]: () => { throw new Error('private detail'); } }), false, name); }
});
test('legacy paid expense source is resolved without creating a saved payment', () => {
  const f = fixture(); f.expense.account = 'Synthetic bank'; f.expense.payments = [];
  for (const payment of f.record.sources[0].paymentAllocations) payment.paymentId = 'LEGACY';
  assert.equal(allowed(f), true); assert.equal(f.expense.payments.length, 0);
});
test('specific payment source must resolve to a unique payment', () => {
  const f = fixture(); Object.assign(f.record.sources[0], { kind: 'payment', paymentId: 'PAY-001', account: 'Synthetic bank' });
  assert.equal(allowed(f), true); f.record.sources[0].paymentId = 'PAY-MISSING'; assert.equal(allowed(f), false);
});
test('statement-backed card expense uses synthetic STATEMENT payment with card authorization', () => {
  const f = fixture(), card = { id: 'CC-1', name: 'Synthetic card', last4: '9999', ownerOnly: true };
  f.snapshot.cards = { cards: { [card.id]: card }, statements: { ST1: { id: 'ST1', cardId: card.id } } };
  f.snapshot.store.expenses = {};
  const entry = { id: f.expense.id, nature: 'SANKI', account: 'Synthetic card 9999', amount: 500, date: f.expense.date, creditCardId: card.id, creditCardStatementId: 'ST1', ownerOnly: true };
  f.snapshot.store.reconciliationExpenses.push(entry);
  const projected = cardExpenseRecords(f.snapshot.store)[0]; assert.equal(projected.payments[0].id, 'STATEMENT');
  for (const payment of f.record.sources[0].paymentAllocations) Object.assign(payment, { paymentId: 'STATEMENT', account: entry.account });
  assert.equal(allowed(f), false); assert.equal(allowed(f, { personal: true }), true);
  delete f.snapshot.cards.statements.ST1; assert.equal(allowed(f, { personal: true }), false);
});
test('card restriction is enforced by receiving-account name even without a card ID', () => {
  const f = fixture(); f.snapshot.cards.cards.CC1 = { id: 'CC1', name: 'Synthetic card', last4: '9999', ownerOnly: true };
  Object.assign(f.record.components[0], { mode: 'card', account: 'Synthetic card 9999' });
  Object.assign(f.snapshot.store.receipts[0], { account: 'Synthetic card 9999', creditCardRefund: true });
  assert.equal(allowed(f), false); assert.equal(allowed(f, { personal: true }), true);
});
test('unallocated advance source checks original nature/account and unique identity', () => {
  const f = fixture(), advance = { id: 'VADV-1', nature: 'SANKI', account: 'Synthetic bank', applications: [] };
  f.snapshot.store.vendorAdvances.push(advance);
  f.record.sources = [{ kind: 'advance', advanceId: advance.id, nature: 'SANKI', account: advance.account }];
  assert.equal(allowed(f), true);
  advance.account = 'Restricted bank'; assert.equal(allowed(f), false);
  advance.account = 'Synthetic bank'; f.snapshot.store.vendorAdvances.push(clone(advance)); assert.equal(allowed(f), false);
});
test('credit-only advance must retain an accessible original refund, not bypass source access', () => {
  const f = fixture(), advance = { id: 'CREDIT-1', nature: 'SANKI', creditOnly: true, applications: [], expenseRefundId: 'RF-MISSING' };
  f.snapshot.store.vendorAdvances.push(advance); f.record.sources = [{ kind: 'advance', advanceId: advance.id, nature: 'SANKI', creditOnly: true }];
  assert.equal(allowed(f), false); advance.expenseRefundId = f.record.id; assert.equal(allowed(f), false);
});
test('valid credit conversion authorizes its original refund graph, including hidden source rejection', () => {
  const f = fixture({ mode: 'voucher' }), store = f.snapshot.store, advance = store.vendorAdvances[0];
  const context = { today: '2098-06-30', username: 'synthetic-manager', expenses: [f.expense], checkSource() {}, checkRecord() {}, checkAccount: (sources, component) => component.account, checkExistingMovement() {} };
  const input = { requestId: 'synthetic-conversion', date: '2098-06-11', reason: 'Synthetic credit conversion', reasonType: 'overpayment', status: 'received', sources: [{ key: 'advance:' + advance.id, amount: 50 }], components: [{ mode: 'bank', receiver: 'company', account: 'Synthetic bank', amount: 50, reference: 'SECOND-REFERENCE' }] };
  f.record = refunds.post(store, refunds.prepare(store, input, context), { username: 'synthetic-manager' }).refund;
  assert.equal(allowed(f), true); f.expense.hidden = true; assert.equal(allowed(f), false);
});
test('valid received child can refer to its authorized pending return without a false cycle', () => {
  const f = fixture({ pending: true }), store = f.snapshot.store, parent = f.record;
  const context = { today: '2098-06-30', username: 'synthetic-manager', expenses: [f.expense], checkSource() {}, checkRecord() {}, checkAccount: (sources, component) => component.account, checkExistingMovement() {} };
  const input = { requestId: 'synthetic-child-refund', parentId: parent.id, date: '2098-06-11', reason: 'Synthetic receipt', reasonType: 'return', status: 'received', sources: [{ key: 'expense:' + f.expense.id, amount: 50 }], components: [{ mode: 'bank', receiver: 'company', account: 'Synthetic bank', amount: 50 }] };
  f.record = refunds.post(store, refunds.prepare(store, input, context), { username: 'synthetic-manager' }).refund;
  assert.equal(allowed(f), true); parent.sources[0].expenseId = 'EX-MISSING'; assert.equal(allowed(f), false);
});
test('applied advance payment requires accessible original advance and applied expense', () => {
  const f = fixture(), advance = { id: 'VADV-1', nature: 'SANKI', account: 'Synthetic bank', applications: [{ expenseId: f.expense.id }] };
  f.snapshot.store.vendorAdvances.push(advance); f.expense.payments = []; f.expense.vendorAdvanceApplications = [{ vendorAdvanceId: advance.id, amount: 500 }];
  for (const payment of f.record.sources[0].paymentAllocations) payment.paymentId = 'ADV-VADV-1-0';
  assert.equal(allowed(f), true); advance.applications[0].expenseId = 'EX-MISSING'; assert.equal(allowed(f), false);
});

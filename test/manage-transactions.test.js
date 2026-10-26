'use strict';
const test = require('node:test'), assert = require('node:assert/strict');
const { create, normalizeReference, referenceRank } = require('../modules/manage-transactions');
const { create: createLedgerEditor } = require('../modules/ledger-transaction-edits');
const { create: createCardCorrections } = require('../modules/credit-card-payment-corrections');
const { assertUnlocked } = require('../modules/source-correction-guards');
const clone = value => JSON.parse(JSON.stringify(value));
const day = '2098-07-17', bank = 'Synthetic Bank 1011', secondBank = 'Synthetic Bank 2022', cardName = 'Synthetic Card 9099', cardId = 'CC-SYNTHETIC';
const admin = { username: 'synthetic-manager', role: 'admin', personal: true };
function fixture() {
  const expense = { id: 'EX-00121', nature: 'SANKI', date: day, amount: 345.67, vendor: 'Synthetic merchant', status: 'paid', approvedAt: day + 'T00:00:00Z', payments: [{ id: 'PAY-001', date: day, amount: 345.67, account: bank, reference: 'SYNTHETIC-PAY-REF' }] };
  const cardPayment = { id: 'CCP-00041', cardId, amount: 100.25, date: day, nature: 'SANKI', account: bank, allocations: [{ nature: 'SANKI', amount: 100.25 }], reference: 'SYNTHETIC-CARD-REF', active: true };
  const cardTransfer = { id: 'TR-00041', nature: 'SANKI', fromNature: 'SANKI', toNature: 'SANKI', fromAccount: bank, toAccount: cardName, amount: 100.25, date: day, reference: cardPayment.reference, classification: 'credit_card_payment', creditCardId: cardId, creditCardPaymentId: cardPayment.id };
  return { store: { expenses: { [expense.id]: expense }, transfers: [cardTransfer, { id: 'TR-00042', nature: 'SANKI', fromNature: 'SANKI', toNature: 'SANKI', fromAccount: bank, toAccount: secondBank, amount: 222.63, date: day, note: 'Synthetic transfer' }],
    receipts: [{ id: 'REC-00042', nature: 'SANKI', account: bank, amount: 70.25, date: day, source: 'Synthetic payer', receiptType: 'other_income' }],
    creditCardSettlementAllocations: [{ id: 'CCA-SYNTHETIC', cardId, cardName, creditCardPaymentId: cardPayment.id, date: day, amount: 100.25, payerNature: 'SANKI', beneficiaryNature: 'SANKI', payingAccount: bank, status: 'self' }],
    reconciliationExpenses: [], vendorAdvances: [], expenseRefunds: [], bankStatements: {}, bankDateOverrides: {}, bankReconciliationDrafts: {}, bankReconciliationApprovals: {}, bankReconciliationLinks: {}, bankMovementAuthority: {}, openingBalanceDates: {}, openingBalanceDatesByNature: {}, cashReconciliations: [], auditLog: [], auditSeq: 0, oneTimeMigrations: {}, unknownFutureData: { retain: true } },
    cards: { cards: { [cardId]: { id: cardId, name: 'Synthetic Card', last4: '9099', active: true } }, payments: [cardPayment], statements: {}, audit: [], auditSeq: 0 } };
}
function addRefund(state) {
  const refund = { id: 'RF-00021', nature: 'SANKI', date: day, amount: 50.25, vendor: 'Synthetic merchant', status: 'received', sources: [{ kind: 'expense', expenseId: 'EX-00121', reference: 'EX-00121', nature: 'SANKI', paymentAllocations: [{ id: 'PAY-001', account: bank, amount: 50.25 }] }], components: [{ id: 'RFC-SYNTHETIC', mode: 'bank', receiver: 'company', account: bank, amount: 50.25, receiptId: 'REC-00021', reference: 'SYNTHETIC-REFUND-REF' }] };
  state.store.expenseRefunds.push(refund); state.store.receipts.push({ id: 'REC-00021', nature: 'SANKI', account: bank, amount: 50.25, date: day, source: 'Synthetic merchant', receiptType: 'expense_refund', expenseRefundId: refund.id });
  return refund;
}
function freeze(value) { if (value && typeof value === 'object') { Object.freeze(value); Object.values(value).forEach(freeze); } return value; }
function harness(mutate = () => {}, overrides = {}) {
  const state = fixture(); mutate(state); let reads = 0;
  const p = { canEdit: req => ['admin', 'owner'].includes(req.user.role) && !req.user.denied, isOwner: req => req.user.role === 'owner',
    natures: req => req.user.natures || ['SANKI', 'SAMAST', 'PERSONAL'], canPersonal: req => req.user.personal === true,
    accountVisible: (req, account) => !list(req.user.hiddenAccounts).includes(account), personalAccount: account => account === 'Synthetic private bank',
    canViewExpense: (req, expense) => !expense.hidden && (!expense.ownerOnly || req.user.role === 'owner' || req.user.personal === true),
    canAccessExpense: (req, expense) => !expense.hidden && (!expense.ownerOnly || req.user.role === 'owner' || req.user.personal === true),
    canAccessCard: (req, card) => !!card && (!card.ownerOnly || req.user.role === 'owner' || req.user.personal === true),
    bookKey: (nature, account) => nature === 'PERSONAL' ? nature + '|' + account : account, assertUnlocked,
    read() { reads++; return freeze({ raw: JSON.stringify(state.store), store: clone(state.store), cards: clone(state.cards), cardRaw: JSON.stringify(state.cards), external: { '/synthetic/credit-cards.json': clone(state.cards) }, externalRaw: { '/synthetic/credit-cards.json': JSON.stringify(state.cards) } }); },
    commit() { throw Error('Search must never commit'); }, audit() { throw Error('Search must never audit'); }
  };
  const ledgerEditor = createLedgerEditor({ ...p, approvalNatures: p.natures, ledgerAccounts: () => [bank, secondBank, cardName, 'Synthetic private bank'], accountBelongs: () => true, paymentPosted: e => ['approved', 'paid'].includes(e.status), expenseLock: () => ({ locked: false }), sourceProtection: () => '', updateExpense() { throw Error('Search must never update'); } });
  const cardService = createCardCorrections(p);
  const deps = { ...p, ledgerEditor, cardEntries: (snapshot, req, id) => cardService.listPayments(snapshot, req, id, p), authorizeRefund: (req, record) => !req.user.refundDenied && p.natures(req).includes(record.nature), ...overrides };
  const service = create(deps);
  const routes = [], router = { get(path, handler) { routes.push({ path, handler }); } }; service.register(router);
  function invoke(reference, user = admin, query) { let code = 200, body; routes[0].handler({ user, query: query || { reference } }, { status(value) { code = value; return this; }, json(value) { body = value; return this; } }); return { code, body }; }
  return { state, p, deps, service, invoke, routes, reads: () => reads };
}
function list(value) { return Array.isArray(value) ? value : []; }

test('canonical references normalize case, separators and zero padding without rewriting opaque references', () => {
  for (const value of ['TR42', 'tr-42', 'TR-00042', 'transfer:TR42', ' TR\u201300042 ']) assert.equal(normalizeReference(value), 'TR-00042');
  for (const value of ['EX121/PAY1', 'ex-00121 / pay-001']) assert.equal(normalizeReference(value), 'EX-00121/PAY-001');
  assert.equal(normalizeReference('CCP41'), 'CCP-00041'); assert.equal(normalizeReference('RF21'), 'RF-00021');
  assert.equal(normalizeReference('SYNTHETIC-CARD-REF'), 'SYNTHETIC-CARD-REF');
  assert.equal(referenceRank('TR-00042', ['TR-00420']), 99); assert.equal(referenceRank('121', ['EX-00121']), 1);
});
test('ordinary transfer and receipt expose exact safe actions; lookup is read-only including unmigrated data', () => {
  const h = harness(), before = clone(h.state);
  const transfer = h.invoke('tr42'); assert.equal(transfer.code, 200); assert.equal(transfer.body.entries.length, 1);
  assert.deepEqual(transfer.body.entries[0].action, { type: 'ledger_edit', id: 'TR-00042', kind: 'transfer', nature: 'SANKI', account: bank });
  assert.equal(transfer.body.entries[0].amount, 222.63); assert.equal(h.invoke('REC42').body.entries[0].action.type, 'ledger_edit');
  assert.deepEqual(h.state, before); assert.deepEqual(h.state.store.oneTimeMigrations, {});
});
test('bill reference finds bill and full payment identity; payment reference never opens the bill financial editor', () => {
  const h = harness(), rows = h.invoke('EX121').body.entries;
  assert.deepEqual(rows.map(r => r.action.type).sort(), ['expense', 'ledger_edit']);
  const paid = h.invoke('EX121/PAY1').body.entries; assert.equal(paid.length, 1); assert.equal(paid[0].action.id, 'EX-00121/PAY-001');
  assert.equal(paid[0].action.kind, 'expense'); assert.equal(h.invoke('SYNTHETIC-PAY-REF').body.entries[0].id, 'EX-00121/PAY-001');
});
test('card CCP and TR aliases open only the same dedicated complete payment graph', () => {
  const h = harness(), left = h.invoke('TR41').body.entries, right = h.invoke('CCP41').body.entries;
  assert.equal(left.length, 1); assert.equal(right.length, 1); assert.equal(left[0].key, right[0].key);
  assert.deepEqual(left[0].action, { type: 'credit_card', cardId, sourceId: 'payment:CCP-00041' });
  assert.equal(left[0].amount, 100.25);
});
test('legacy card transfer uses existing transfer identity without creating a payment', () => {
  const h = harness(s => { delete s.store.transfers[0].creditCardPaymentId; s.cards.payments = []; s.store.creditCardSettlementAllocations = []; }), before = clone(h.state);
  const row = h.invoke('TR41').body.entries[0]; assert.equal(row.action.type, 'credit_card'); assert.equal(row.action.sourceId, 'transfer:TR-00041'); assert.deepEqual(h.state, before);
});
test('refund RF, original expense and owned receipt references retain the dedicated refund workflow', () => {
  const h = harness(addRefund), before = clone(h.state);
  for (const reference of ['RF21', 'REC21', 'SYNTHETIC-REFUND-REF']) { const row = h.invoke(reference).body.entries[0]; assert.deepEqual(row.action, { type: 'refund', id: 'RF-00021', nature: 'SANKI' }); }
  assert.ok(h.invoke('EX121').body.entries.some(r => r.kind === 'refund')); assert.deepEqual(h.state, before);
});
test('refund-owned receipt never falls back to independent ledger editing when parent is inaccessible or malformed', () => {
  for (const mutate of [s => { addRefund(s); s.store.expenses['EX-00121'].hidden = true; }, s => { addRefund(s); delete s.store.expenses['EX-00121']; }, s => { addRefund(s); s.store.expenseRefunds.push(clone(s.store.expenseRefunds[0])); }]) {
    const h = harness(mutate); assert.deepEqual(h.invoke('REC21').body.entries, []); assert.deepEqual(h.invoke('RF21').body.entries, []);
  }
  assert.deepEqual(harness(addRefund).invoke('REC21', { ...admin, refundDenied: true }).body.entries, []);
});
test('both transfer ends and actual PERSONAL bank ownership are authorized before any fields or reasons are returned', () => {
  const h = harness(), hidden = h.invoke('TR42', { ...admin, hiddenAccounts: [secondBank] }); assert.deepEqual(hidden.body.entries, []); assert.doesNotMatch(JSON.stringify(hidden.body), /2022|222.63|Synthetic transfer/);
  const p = harness(s => { s.store.transfers[1].toNature = 'PERSONAL'; s.store.transfers[1].toAccount = 'Synthetic private bank'; });
  assert.deepEqual(p.invoke('TR42', { ...admin, personal: false }).body.entries, []);
  const mislabeled = harness(s => { s.store.transfers[1].toAccount = 'Synthetic private bank'; }); assert.deepEqual(mislabeled.invoke('TR42', { ...admin, personal: false }).body.entries, []);
});
test('private expenses, payments, restricted cards and hidden ownership allocations reveal no references', () => {
  const h = harness(s => { s.store.expenses['EX-00121'].nature = 'PERSONAL'; s.cards.cards[cardId].ownerOnly = true; });
  for (const ref of ['EX121', 'EX121/PAY1', 'CCP41', 'TR41']) assert.deepEqual(h.invoke(ref, { ...admin, personal: false }).body.entries, [], ref);
  const split = harness(s => { s.cards.payments[0].allocations[0].nature = 'PERSONAL'; s.store.creditCardSettlementAllocations[0].beneficiaryNature = 'PERSONAL'; s.store.creditCardSettlementAllocations[0].status = 'due'; }); assert.deepEqual(split.invoke('TR41', { ...admin, personal: false }).body.entries, []);
});

test('bill metadata requires an explicit entity and authorization of original accounts and card identities', () => {
  for (const mutate of [
    s => { delete s.store.expenses['EX-00121'].nature; },
    s => { s.store.expenses['EX-00121'].nature = 'UNKNOWN'; },
    s => { s.store.expenses['EX-00121'].account = 'Synthetic private bank'; },
    s => { s.store.expenses['EX-00121'].claimantFundingAccount = 'Synthetic private bank'; },
    s => { s.cards.cards[cardId].ownerOnly = true; s.store.expenses['EX-00121'].creditCardId = cardId; },
    s => { s.cards.cards[cardId].ownerOnly = true; s.store.expenses['EX-00121'].account = cardName; },
    s => { s.cards.cards[cardId].ownerOnly = true; s.store.expenses['EX-00121'].creditCardStatementId = 'ST-SYNTHETIC'; s.cards.statements['ST-SYNTHETIC'] = { id: 'ST-SYNTHETIC', cardId }; },
    s => { s.store.expenses['EX-00121'].creditCardId = 'CC-MISSING'; },
    s => { s.store.expenses['EX-00121'].creditCardStatementId = 'ST-MISSING'; }
  ]) {
    const h = harness(mutate); assert.deepEqual(h.invoke('EX121', { ...admin, personal: false }).body.entries, []);
  }
  const allowed = harness(s => { s.cards.cards[cardId].ownerOnly = true; s.store.expenses['EX-00121'].creditCardId = cardId; });
  assert.ok(allowed.invoke('EX121').body.entries.some(row => row.kind === 'expense_record'));
});

test('ordinary receipts and misclassified transfers cannot reveal restricted-card metadata', () => {
  for (const [reference, mutate] of [
    ['REC42', s => Object.assign(s.store.receipts[0], { account: cardName, creditCardRefund: true, receiptType: 'refund' })],
    ['REC42', s => { s.store.receipts[0].account = '  Synthetic   Card 9099  '; }],
    ['REC42', s => { s.store.receipts[0].creditCardId = cardId; }],
    ['TR42', s => { s.store.transfers[1].fromAccount = cardName; }],
    ['TR42', s => { s.store.transfers[1].toAccount = cardName; }]
  ]) {
    const h = harness(s => { s.cards.cards[cardId].ownerOnly = true; mutate(s); }), before = clone(h.state);
    const denied = h.invoke(reference, { ...admin, personal: false }); assert.equal(denied.code, 200); assert.deepEqual(denied.body.entries, []);
    assert.doesNotMatch(JSON.stringify(denied.body), /Synthetic Card|70\.25|222\.63|Synthetic payer|Synthetic transfer/);
    const allowed = h.invoke(reference).body.entries; assert.equal(allowed.length, 1); assert.equal(allowed[0].editable, false); assert.equal(allowed[0].action.type, 'source_workflow'); assert.match(allowed[0].blockedReason, /credit-card source/); assert.deepEqual(allowed[0].action.destination, { tab: 'creditcards', cardId }); assert.deepEqual(h.state, before);
  }
});

test('protected ordinary card sources never route back to a generic cash editor or guess a payment identity', () => {
  const inactive = harness(s => { s.store.receipts[0].account = cardName; s.store.receipts[0].active = false; });
  const row = inactive.invoke('REC42').body.entries[0]; assert.equal(row.editable, false); assert.deepEqual(row.action.destination, { tab: 'creditcards', cardId });
  const multiple = harness(s => { s.cards.cards.OTHER = { id: 'CC-OTHER', name: 'Other synthetic card', last4: '8088' }; s.store.transfers[1].fromAccount = cardName; s.store.transfers[1].toAccount = 'Other synthetic card 8088'; });
  const original = clone(multiple.state), result = multiple.invoke('TR42').body.entries[0]; assert.equal(result.editable, false); assert.equal(result.action.type, 'source_workflow'); assert.equal(result.action.destination, undefined); assert.deepEqual(multiple.state, original);
});

test('missing, inconsistent or ambiguous card identities fail closed even for authorized managers', () => {
  for (const mutate of [
    s => { s.store.receipts[0].creditCardId = 'CC-MISSING'; },
    s => { s.store.receipts[0].creditCardRefund = true; },
    s => { s.store.receipts[0].account = cardName; s.cards.cards.DUPLICATE = { ...s.cards.cards[cardId], id: 'CC-OTHER' }; },
    s => { s.store.receipts[0].creditCardId = cardId; s.cards.cards.DUPLICATE = { ...s.cards.cards[cardId], name: 'Synthetic duplicate' }; },
    s => { s.store.receipts[0].account = cardName; s.store.receipts[0].creditCardId = 'CC-OTHER'; s.cards.cards.OTHER = { id: 'CC-OTHER', name: 'Other synthetic card', last4: '8088' }; }
  ]) assert.deepEqual(harness(mutate).invoke('REC42').body.entries, []);
});
test('reconciliation, finalized periods and pending approvals explain locks and do not offer generic edits', () => {
  for (const mutate of [s => { s.store.bankReconciliationLinks['TR-00042'] = {}; }, s => { s.store.bankStatements[bank] = { reconciledThrough: day }; }, s => { s.store.bankReconciliationApprovals.A = { status: 'pending', account: bank, from: '2098-07-01', to: '2098-07-31' }; }]) {
    const h = harness(mutate), row = h.invoke('TR42').body.entries[0]; assert.equal(row.editable, false); assert.equal(row.action.type, 'source_workflow'); assert.match(row.blockedReason, /reconcil|finalized|approval/i);
  }
});
test('blocked card graphs are not enriched from raw payment amount or private note', () => {
  const h = harness(s => { s.store.transfers[0].amount = 199.99; s.store.transfers[0].note = 'UNVERIFIED PRIVATE NOTE'; });
  const row = h.invoke('TR41').body.entries[0]; assert.equal(row.editable, false); assert.equal(row.amount, null); assert.equal(row.date, ''); assert.equal(row.account, ''); assert.doesNotMatch(JSON.stringify(row), /199.99|UNVERIFIED PRIVATE NOTE/);
});
test('duplicate exact and canonical source identities are suppressed, including duplicate hidden records', () => {
  for (const mutate of [s => s.store.transfers.push({ ...s.store.transfers[1], nature: 'PERSONAL', fromNature: 'PERSONAL' }), s => s.store.transfers.push({ ...s.store.transfers[1], id: 'TR42' })]) assert.deepEqual(harness(mutate).invoke('TR42').body.entries, []);
  const h = harness(s => s.store.expenses['EX-00121'].payments.push(clone(s.store.expenses['EX-00121'].payments[0]))); assert.deepEqual(h.invoke('EX121/PAY1').body.entries, []);
  const c = harness(s => s.cards.payments.push({ ...s.cards.payments[0], nature: 'PERSONAL' })); assert.deepEqual(c.invoke('CCP41').body.entries, []);
});
test('reference search is bounded and returns choices rather than a silent numeric first match', () => {
  const h = harness(s => { for (let i = 0; i < 65; i++) s.store.receipts.push({ id: 'REC-' + String(12300 + i), nature: 'SANKI', account: bank, amount: 1, date: day, source: 'Synthetic match' }); });
  const result = h.invoke('123'); assert.equal(result.body.entries.length, 50); assert.equal(result.body.truncated, true); assert.match(result.body.message, /complete reference/);
  assert.equal(h.invoke('REC12305').body.entries.length, 1); assert.equal(h.invoke('21').code, 400);
});
test('authorization precedes all reads, empty search does not enumerate, and input is never executable', () => {
  const h = harness();
  for (const role of ['claimant', 'accounting', 'stocksearch']) assert.equal(h.invoke('TR42', { ...admin, role }).code, 403);
  assert.equal(h.reads(), 0); assert.deepEqual(h.invoke('').body.entries, []); assert.equal(h.reads(), 0);
  for (const bad of [[], {}, 'x'.repeat(129), 'TR42\n', '\u0000']) assert.equal(h.invoke(bad).code, 400);
  assert.equal(h.invoke('TR42', admin, { reference: 'TR42', action: 'apply' }).code, 400);
  assert.deepEqual(h.invoke('<script>synthetic()</script>').body.entries, []);
  assert.deepEqual(h.routes.map(r => r.path), ['/api/expenses/transactions']);
});
test('malformed/unreadable snapshots fail closed without echoing private details', () => {
  const h = harness(() => {}, { read() { throw Error('SECRET FILE PATH'); } }); const result = h.invoke('TR42'); assert.equal(result.code, 409); assert.doesNotMatch(JSON.stringify(result.body), /SECRET/);
  for (const key of ['transfers', 'receipts', 'expenseRefunds']) { const x = harness(s => { s.store[key] = {}; }); assert.equal(x.invoke('TR42').code, 409, key); }
});

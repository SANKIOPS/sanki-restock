'use strict';
const test = require('node:test'), assert = require('node:assert/strict'), express = require('express');
const { create } = require('../modules/credit-card-payment-corrections');
const { assertUnlocked } = require('../modules/source-correction-guards');
const clone = value => JSON.parse(JSON.stringify(value));
const cardId = 'CC-HELPER-SYNTHETIC', cardAccount = 'Synthetic Card 9997', sourceId = 'transfer:TR-HELPER-SYNTHETIC';
const businessBank = 'Synthetic business bank', personalBank = 'Synthetic PERSONAL bank';
const admin = { username: 'synthetic-admin', role: 'admin', personal: true }, owner = { ...admin, username: 'synthetic-owner', role: 'owner' };
const changes = { amount: '99.63' }, reason = 'Synthetic original evidence confirms this amount correction';

function harness(helper, bank = businessBank) {
  const state = {
    store: { expenses: {}, receipts: [], transfers: [{ id: 'TR-HELPER-SYNTHETIC', date: '2098-06-10', amount: 100,
      nature: 'SANKI', fromNature: 'SANKI', toNature: 'SANKI', fromAccount: bank, toAccount: cardAccount,
      classification: 'credit_card_payment', reference: 'PRIVATE-SYNTHETIC-REFERENCE', note: 'Private synthetic source' }],
    creditCardSettlementAllocations: [], bankStatements: {}, bankReconciliationDrafts: {}, bankReconciliationApprovals: {},
    bankReconciliationLinks: {}, bankDateOverrides: {}, bankMovementAuthority: {}, auditLog: [], auditSeq: 0 },
    cards: { cards: { [cardId]: { id: cardId, name: 'Synthetic Card', last4: '9997', ownerOnly: true, active: true } },
      payments: [], statements: {}, audit: [], auditSeq: 0 }
  };
  let commits = 0;
  const policy = {
    canEdit: req => ['owner', 'admin'].includes(req.user.role), isOwner: req => req.user.role === 'owner',
    natures: () => ['SANKI', 'SAMAST', 'PERSONAL'], canPersonal: req => req.user.personal === true,
    accountVisible: () => true, personalAccount: account => account === personalBank,
    bookKey: (nature, account) => nature + '|' + account, assertUnlocked,
    read: () => ({ store: clone(state.store), raw: JSON.stringify(state.store), cards: clone(state.cards),
      cardRaw: JSON.stringify(state.cards), external: {}, externalRaw: {} }),
    commit() { commits++; throw Error('This read-only test must never commit'); }
  };
  if (helper !== undefined) policy.canAccessCard = helper;
  const router = express.Router(); create({ policy: () => policy }).register(router);
  function invoke(action, user = admin, fingerprint = 'forged') {
    const route = '/api/expenses/credit-cards/:cardId/payment-corrections' + (action === 'list' ? '' : '/:sourceId/' + action);
    const layer = router.stack.find(item => item.route?.path === route && item.route.methods[action === 'list' ? 'get' : 'post']);
    assert.ok(layer); let status = 200, body;
    layer.route.stack[0].handle({ user: clone(user), params: { cardId, sourceId }, body: { changes, reason, fingerprint } },
      { status(value) { status = value; return this; }, json(value) { body = value; return this; } });
    return { status, body };
  }
  return { policy, state, invoke, raw: () => JSON.stringify(state), commits: () => commits };
}

test('missing or malformed card-access helper retains strict legacy Owner access, not Admin or PERSONAL grants', () => {
  for (const helper of [undefined, null, 'true', true, 1]) {
    const h = harness(helper), before = h.raw();
    for (const action of ['list', 'preview', 'apply']) assert.equal(h.invoke(action).status, 404);
    assert.equal(h.invoke('preview', owner).status, 200, 'Legacy Owner access is retained');
    assert.equal(h.raw(), before); assert.equal(h.commits(), 0);
  }
});

test('present card-access helper must return exact true and cannot fall back to an Owner role label', () => {
  for (const result of [undefined, null, false, 'true', 1, {}, []]) {
    const h = harness(() => result), before = h.raw();
    for (const user of [admin, owner]) for (const action of ['list', 'preview', 'apply']) {
      const reply = h.invoke(action, user); assert.equal(reply.status, 404);
      assert.doesNotMatch(JSON.stringify(reply), /PRIVATE-SYNTHETIC-REFERENCE|Private synthetic source/);
    }
    assert.equal(h.raw(), before); assert.equal(h.commits(), 0);
  }
  const granted = harness(() => true), before = granted.raw();
  assert.equal(granted.invoke('preview').status, 200); assert.equal(granted.raw(), before);
});

test('card-access helper revocation after preview rejects apply without exposing or modifying source records', () => {
  const h = harness(() => true), preview = h.invoke('preview'), before = h.raw();
  assert.equal(preview.status, 200);
  h.policy.canAccessCard = () => false;
  const result = h.invoke('apply', admin, preview.body.fingerprint);
  assert.equal(result.status, 404); assert.doesNotMatch(JSON.stringify(result), /PRIVATE-SYNTHETIC-REFERENCE/);
  assert.equal(h.raw(), before); assert.equal(h.commits(), 0);
});

test('card access alone cannot expose business-tagged payments from a physical PERSONAL account', () => {
  // Deliberately grant card access: the independent source-account privacy
  // check must still reject the mis-tagged bank source without PERSONAL access.
  const h = harness(() => true, personalBank), denied = { ...admin, personal: false }, before = h.raw();
  const listed = h.invoke('list', denied); assert.equal(listed.status, 200); assert.deepEqual(listed.body.entries, []);
  for (const action of ['preview', 'apply']) {
    const result = h.invoke(action, denied); assert.equal(result.status, 403);
    assert.doesNotMatch(JSON.stringify(result), /PRIVATE-SYNTHETIC-REFERENCE|Private synthetic source/);
  }
  assert.equal(h.invoke('preview', admin).status, 200, 'Explicit PERSONAL access permits the same physical account');
  assert.equal(h.raw(), before); assert.equal(h.commits(), 0);
});

test('exact card permission cannot bypass unavailable protection service or pending approval', () => {
  for (const mode of ['missing-service', 'pending-approval']) {
    const h = harness(() => true);
    if (mode === 'missing-service') delete h.policy.assertUnlocked;
    else h.state.store.bankReconciliationApprovals.A = { status: 'pending', account: businessBank, from: '2098-06-01', to: '2098-06-30' };
    const before = h.raw();
    for (const user of [admin, owner]) for (const action of ['preview', 'apply']) assert.equal(h.invoke(action, user).status, 409);
    assert.equal(h.raw(), before); assert.equal(h.commits(), 0);
  }
});

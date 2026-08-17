'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { apiRuleFor, apiAllowedForUser } = require('../auth');

function user(...roles) {
  return { username: 'test-user', role: roles[0], roles };
}

test('admin retains access to every API, including unknown future routes', () => {
  assert.equal(apiAllowedForUser(user('admin'), '/api/future-feature'), true);
});

test('ordinary roles fail closed for unknown APIs', () => {
  assert.equal(apiRuleFor('/api/future-feature'), null);
  assert.equal(apiAllowedForUser(user('sales'), '/api/future-feature'), false);
});

test('sales cannot mutate inventory, procurement, or accounting', () => {
  const sales = user('sales');
  assert.equal(apiAllowedForUser(sales, '/api/inventory/adjust'), false);
  assert.equal(apiAllowedForUser(sales, '/api/procurement/commit'), false);
  assert.equal(apiAllowedForUser(sales, '/api/expenses/123/pay'), false);
  assert.equal(apiAllowedForUser(sales, '/api/pl/summary'), false);
});

test('sales can use order, velocity, manual-sale, and messaging APIs', () => {
  const sales = user('sales');
  assert.equal(apiAllowedForUser(sales, '/api/orders-ledger'), true);
  assert.equal(apiAllowedForUser(sales, '/api/velocity/shipments'), true);
  assert.equal(apiAllowedForUser(sales, '/api/sales/record'), true);
  assert.equal(apiAllowedForUser(sales, '/api/whatsapp/send'), true);
});

test('procurement is confined to purchasing and size workflows', () => {
  const procurement = user('procurement');
  assert.equal(apiAllowedForUser(procurement, '/api/procurement/commit'), true);
  assert.equal(apiAllowedForUser(procurement, '/api/fresh/analyze'), true);
  assert.equal(apiAllowedForUser(procurement, '/api/casuals/plan'), true);
  assert.equal(apiAllowedForUser(procurement, '/api/sizetracker/compare'), true);
  assert.equal(apiAllowedForUser(procurement, '/api/orders-ledger'), false);
});

test('accounting can use P&L and expense APIs but cannot adjust stock', () => {
  const accounting = user('accounting');
  assert.equal(apiAllowedForUser(accounting, '/api/pl/summary'), true);
  assert.equal(apiAllowedForUser(accounting, '/api/expenses/list'), true);
  assert.equal(apiAllowedForUser(accounting, '/api/inventory/adjust'), false);
});

test('claimants can log expenses but cannot access accounting or unrelated APIs', () => {
  const claimant = user('claimant');
  assert.equal(apiAllowedForUser(claimant, '/api/expenses'), true);
  assert.equal(apiAllowedForUser(claimant, '/api/expenses/upload'), true);
  assert.equal(apiAllowedForUser(claimant, '/api/pl/summary'), false);
  assert.equal(apiAllowedForUser(claimant, '/api/orders-ledger'), false);
});

test('warehouse and stock-search roles receive only operational stock access', () => {
  assert.equal(apiAllowedForUser(user('warehouse'), '/api/showroom/queue/move'), true);
  assert.equal(apiAllowedForUser(user('warehouse'), '/api/orders-ledger'), true);
  assert.equal(apiAllowedForUser(user('stocksearch'), '/api/stock-search'), true);
  assert.equal(apiAllowedForUser(user('stocksearch'), '/api/racks/set'), true);
  assert.equal(apiAllowedForUser(user('stocksearch'), '/api/showroom/settings'), false);
});

test('shared authenticated endpoints remain available to all roles', () => {
  assert.equal(apiAllowedForUser(user('stocksearch'), '/api/auth/me'), true);
  assert.equal(apiAllowedForUser(user('accounting'), '/api/modules'), true);
});

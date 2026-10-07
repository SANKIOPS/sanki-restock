'use strict';
const test = require('node:test'), assert = require('node:assert/strict');
const view = require('../public/inventory-view-model');
const stocked = { sku: 'IN', availableQty: 2, displayQty: 1, warehouseQty: 1, totalQty: 2 };
const soldOut = { sku: 'OUT', availableQty: 0, displayQty: 0, warehouseQty: 0, totalQty: 0 };
const held = { sku: 'CLEAN', availableQty: 0, cleaningQty: 1, notForSaleQty: 0, displayQty: 0, warehouseQty: 0, totalQty: 1 };
const mixed = { handle: 'mixed', collection: 'SANKI Funky', availableQty: 2, totalQty: 3, variants: [stocked, soldOut, held] };
test('default search and expansion exclude unavailable variants of stocked products', () => {
  assert.deepEqual(view.visibleVariants(mixed, 'all').map(v => v.sku), ['IN']);
  assert.equal(view.includes({ variants: [soldOut] }, 'all'), false);
  assert.equal(view.includes(mixed, 'all'), true);
  assert.equal(view.listProduct(mixed, 'all').totalQty, 3, 'owned quantity stays accurate');
  assert.equal(view.listProduct(mixed, 'all').hiddenSkuCount, 2);
});
test('out-of-stock view includes sold-out sizes of mixed products and shows only their balances', () => {
  assert.equal(view.includes(mixed, 'outOfStock'), true);
  assert.deepEqual(view.visibleVariants(mixed, 'outOfStock').map(v => v.sku), ['OUT', 'CLEAN']);
  const p = view.listProduct(mixed, 'outOfStock');
  assert.equal(p.availableQty, 0); assert.equal(p.totalQty, 1); assert.equal(p.cleaningQty, 1);
  assert.equal(mixed.availableQty, 2); assert.equal(mixed.variants.length, 3, 'source catalogue is unchanged');
});
test('collection browsing hides zero availability and stockless empty products', () => {
  assert.equal(view.includes(mixed, 'casuals'), false);
  assert.equal(view.includes(mixed, 'funky'), true);
  assert.deepEqual(view.visibleVariants(mixed, 'funky'), [stocked]);
  assert.equal(view.includes({ variants: [] }, 'outOfStock'), false);
});
test('explicit care and location tiles retain meaningful pieces and their SKUs', () => {
  assert.deepEqual(view.visibleVariants(mixed, 'cleaning'), [held]);
  assert.deepEqual(view.visibleVariants(mixed, 'display'), [stocked]);
  assert.equal(view.includes({ variants: [{ availableQty: 0, notForSaleQty: 1 }] }, 'miscellaneous'), true);
  assert.equal(view.includes({ variants: [{ availableQty: 0, committedQty: 1 }] }, 'other'), true);
});

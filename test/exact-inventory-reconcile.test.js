'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { buildExactPlan } = require('../modules/exact-inventory-reconcile');

test('consolidates aliases onto the canonical Shopify variant and zeros unlisted stock', () => {
  const plan = buildExactPlan({
    variants: [
      { sku: 'OLD-1', variantId: 1, inventoryItemId: 101 },
      { sku: 'NEW-1', variantId: 2, inventoryItemId: 102 },
      { sku: 'EXTRA', variantId: 3, inventoryItemId: 103 },
    ],
    levels: {
      101: { display: 1, warehouse: 2 },
      102: { display: 0, warehouse: 3 },
      103: { display: 1, warehouse: 0 },
    },
    rows: [
      { sku: 'OLD-1', frontQty: 1, backQty: 1 },
      { sku: 'NEW-1', frontQty: 2, backQty: 4 },
    ],
    aliases: { 'OLD-1': 'NEW-1' },
    frontLocationId: 'display',
    backLocationId: 'warehouse',
    zeroUnlisted: true,
  });

  assert.deepEqual(plan.missing, []);
  assert.equal(plan.totals.desiredFront, 3);
  assert.equal(plan.totals.desiredBack, 5);
  assert.deepEqual(
    plan.changes.map(change => [change.sku, change.location, change.from, change.to]),
    [
      ['NEW-1', 'Display', 0, 3],
      ['NEW-1', 'Warehouse', 3, 5],
      ['OLD-1', 'Display', 1, 0],
      ['OLD-1', 'Warehouse', 2, 0],
      ['EXTRA', 'Display', 1, 0],
    ]
  );
});

test('reports a desired SKU that does not exist in Shopify', () => {
  const plan = buildExactPlan({
    variants: [],
    levels: {},
    rows: [{ sku: 'MISSING', frontQty: 1, backQty: 0 }],
    frontLocationId: 'display',
    backLocationId: 'warehouse',
  });
  assert.deepEqual(plan.missing, ['MISSING']);
  assert.deepEqual(plan.changes, []);
});

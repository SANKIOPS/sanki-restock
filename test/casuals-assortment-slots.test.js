const test = require('node:test');
const assert = require('node:assert/strict');

const { resolveAssortmentSlot } = require('../modules/casuals');

test('assortment recommendation resolves a short AI slot code to its full plan label', () => {
  const slots = [
    { name: 'R1 · Wide-leg · Clean high-waist, flat front', need: 1 },
    { name: 'R2 · Wide-leg · Single-pleat tailored', need: 1 }
  ];

  assert.equal(resolveAssortmentSlot('R2', slots), slots[1]);
  assert.equal(resolveAssortmentSlot('R2: Wide-leg single pleat tailored', slots), slots[1]);
  assert.equal(resolveAssortmentSlot(slots[1].name, slots), slots[1]);
  assert.equal(resolveAssortmentSlot('No match', slots), null);
});

test('assortment recommendation does not guess an ambiguous or unknown slot code', () => {
  const duplicateCodes = [
    { name: 'R2 · First option', need: 1 },
    { name: 'R2 · Second option', need: 1 }
  ];

  assert.equal(resolveAssortmentSlot('R2', duplicateCodes), null);
  assert.equal(resolveAssortmentSlot('R9', duplicateCodes), null);
});

'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const { createLookup } = require('../modules/inventory-barcode');
test('registered barcode route resolves at its full URL when mounted at the app root', async () => {
  const express = require('express'), app = express(), router = express.Router();
  require('../modules/inventory-barcode').register(router);
  app.use(router);
  const server = app.listen(0, '127.0.0.1');
  await require('node:events').once(server, 'listening');
  try {
    const response = await fetch(`http://127.0.0.1:${server.address().port}/api/inventory-categorization/barcode?barcode=`);
    assert.equal(response.status, 400);
    assert.equal(response.headers.get('cache-control'), 'no-store');
    assert.deepEqual(await response.json(), { success: false, error: 'Enter a valid barcode.' });
  } finally { await new Promise(resolve => server.close(resolve)); }
});
const variant = (barcode, sku = 'SKU1', id = 'v1') => ({ id, barcode, sku, product: { handle: 'tee', title: 'Tee' } });
const page = (nodes, hasNextPage = false, endCursor = null) => ({ productVariants: { nodes, pageInfo: { hasNextPage, endCursor } } });
test('barcode lookup preserves leading zeroes and verifies exact Shopify matches', async () => {
  const lookup = createLookup(async (query, variables) => {
    assert.match(query, /productVariants/);
    assert.equal(variables.query, 'barcode:"001234"');
    return page([variant('1234', 'WRONG'), variant('001234')]);
  });
  assert.deepEqual(await lookup(' 001234 '), { barcode: '001234', sku: 'SKU1', handle: 'tee', title: 'Tee' });
});
test('barcode filter safely quotes special characters', async () => {
  const code = 'abc" OR sku:*';
  const lookup = createLookup(async (_, variables) => { assert.equal(variables.query, 'barcode:' + JSON.stringify(code)); return page([variant(code)]); });
  assert.equal((await lookup(code)).sku, 'SKU1');
});
test('all result pages are checked before deciding that a barcode is unique', async () => {
  let calls = 0;
  const lookup = createLookup(async (_, variables) => {
    calls++;
    if (!variables.after) return page([variant('001')], true, 'next');
    assert.equal(variables.after, 'next');
    return page([variant('001', 'SKU2', 'v2')]);
  });
  await assert.rejects(lookup('001'), e => e.status === 409);
  assert.equal(calls, 2);
});
test('unknown barcode and missing SKU return actionable errors', async () => {
  await assert.rejects(createLookup(async () => page([]))('001'), e => e.status === 404);
  await assert.rejects(createLookup(async () => page([variant('001', '')]))('001'), e => e.status === 422);
});
test('invalid identifiers never send a Shopify query', async () => {
  const lookup = createLookup(() => { throw new Error('should not query'); });
  for (const code of ['', 'x'.repeat(201), 'a\u0000b', ['001']]) await assert.rejects(lookup(code), e => e.status === 400);
});
test('incomplete pagination fails instead of selecting an unconfirmed SKU', async () => {
  await assert.rejects(createLookup(async () => page([variant('001')], true))('001'), e => e.status === 502);
});
test('concurrent identical scans share the active Shopify read but a later scan checks fresh data', async () => {
  let finish, calls = 0;
  const lookup = createLookup(async () => {
    calls++;
    if (calls === 1) return new Promise(resolve => { finish = resolve; });
    return page([variant('001', 'UPDATED')]);
  });
  const first = lookup('001'), second = lookup(' 001 ');
  assert.equal(calls, 1);
  finish(page([variant('001')]));
  assert.equal((await first).sku, 'SKU1'); assert.equal((await second).sku, 'SKU1');
  assert.equal((await lookup('001')).sku, 'UPDATED'); assert.equal(calls, 2);
});
test('a failed shared barcode read can be retried without retaining a rejected promise', async () => {
  let calls = 0;
  const lookup = createLookup(async () => { calls++; if (calls === 1) throw new Error('offline'); return page([variant('001')]); });
  await assert.rejects(lookup('001'), /offline/);
  assert.equal((await lookup('001')).sku, 'SKU1'); assert.equal(calls, 2);
});
test('a multi-page cursor cycle fails promptly rather than hanging barcode lookup', async () => {
  let calls = 0;
  const lookup = createLookup(async (_, variables) => {
    calls++;
    return page([variant('001')], true, variables.after === 'A' ? 'B' : 'A');
  });
  await assert.rejects(lookup('001'), e => e.status === 502); assert.equal(calls, 3);
});
test('a duplicate found on the first page fails without unnecessary later Shopify calls', async () => {
  let calls = 0;
  const lookup = createLookup(async () => { calls++; return page([variant('001'), variant('001', 'SKU2', 'v2')], true, 'more'); });
  await assert.rejects(lookup('001'), e => e.status === 409); assert.equal(calls, 1);
});

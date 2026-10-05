'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const { ShopifyClient, parseRetryAfter } = require('../modules/shopify-client');

function response(status, headers = {}) {
  return {
    status,
    ok: status >= 200 && status < 300,
    headers: { get: name => headers[name.toLowerCase()] || headers[name] || null }
  };
}

test('uncertain recovery creates can disable retries without changing other request defaults', async () => {
  for (const failure of ['http', 'network']) {
    let calls = 0;
    const client = new ShopifyClient({store:'shop.example.com',token:'test-token',minIntervalMs:0,maxRetries:3,
      sleep:async()=>{throw new Error('A recovery create must not retry');},
      fetchImpl:async(url,options)=>{
        calls++;assert.equal(options.maxRetries,undefined);
        if(failure==='network')throw new Error('Connection lost after sending');
        return response(500);
      }});
    const request=client.request('https://shop.example.com/admin/api/products.json',{method:'POST',maxRetries:0});
    if(failure==='network')await assert.rejects(request,/Connection lost/);else assert.equal((await request).status,500);
    assert.equal(calls,1);assert.equal(client.maxRetries,3);
  }
});

test('Retry-After supports seconds', () => {
  assert.equal(parseRetryAfter('2'), 2000);
  assert.equal(parseRetryAfter(null), null);
});

test('429 is retried and Shopify credentials are attached', async () => {
  let clock = 1000;
  const sleeps = [];
  const calls = [];
  const replies = [response(429, { 'retry-after': '1' }), response(200)];
  const client = new ShopifyClient({
    store: 'shop.example.com', token: 'test-token', minIntervalMs: 0, maxRetries: 2,
    now: () => clock,
    sleep: async ms => { sleeps.push(ms); clock += ms; },
    fetchImpl: async (url, options) => { calls.push({ url, options }); return replies.shift(); }
  });

  const result = await client.request('https://shop.example.com/admin/api/test.json');
  assert.equal(result.status, 200);
  assert.equal(calls.length, 2);
  assert.equal(calls[0].options.headers['X-Shopify-Access-Token'], 'test-token');
  assert.ok(sleeps.some(ms => ms >= 1000));
});

test('concurrent calls are serialized and respect minimum spacing', async () => {
  let clock = 1000;
  const starts = [];
  const client = new ShopifyClient({
    store: 'shop.example.com', token: 'test-token', minIntervalMs: 550,
    now: () => clock,
    sleep: async ms => { clock += ms; },
    fetchImpl: async () => { starts.push(clock); return response(200); }
  });

  await Promise.all([
    client.request('https://shop.example.com/admin/api/one.json'),
    client.request('https://shop.example.com/admin/api/two.json'),
    client.request('https://shop.example.com/admin/api/three.json')
  ]);

  assert.deepEqual(starts, [1000, 1550, 2100]);
});

test('credentials cannot be sent to another host', async () => {
  const client = new ShopifyClient({ store: 'shop.example.com', token: 'test-token' });
  await assert.rejects(
    client.request('https://attacker.example/admin/api/products.json'),
    /unexpected host/
  );
});

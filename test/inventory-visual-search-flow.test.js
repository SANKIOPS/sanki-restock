const test = require('node:test');
const assert = require('node:assert/strict');
const Jimp = require('jimp');
const { PassThrough } = require('node:stream');
const { search, createService, register, vision, diagnostics, imageHeader, prepareInput, cleanCandidates, cleanMatches } = require('../modules/inventory-visual-search');
let font;
test.before(async () => { font = await Jimp.loadFont(Jimp.FONT_SANS_16_BLACK); });
function product(id, extras = {}) { return { handle: 'article-' + id, title: 'Actual garment ' + id, image: 'https://cdn.shopify.com/' + id + '.jpg', images: ['https://cdn.shopify.com/' + id + '.jpg', 'https://cdn.shopify.com/' + id + '-back.jpg'], variants: [{ sku: 'SKU-' + id, variant: 'Black / M', price: 1499, availableQty: 1 }], displayQty: 0, warehouseQty: 1, totalQty: 1, inventoryAt: '2026-10-25T08:00:00.000Z', ...extras }; }
function mockServices(overrides = {}) { return { config: {}, prepareInput: async () => 'query', loadFont: async () => font, sheet: async entries => ({ data: JSON.stringify(entries.map(p => p.id)), available: entries.map(p => p.id) }), photo: async () => new Jimp(2, 2, 0xffffffff), ...overrides }; }
function job(full = false) { return { full, status: 'running', controller: new AbortController() }; }
function matched(id) { return { id, confidence: 'high', reason: 'Same distinctive print and pocket placement' }; }
function response(status, body) { return { status, ok: status >= 200 && status < 300, json: async () => body }; }
async function eventually(check) { for (let i = 0; i < 200; i++) { if (check()) return; await new Promise(resolve => setTimeout(resolve, 10)); } assert.fail('Timed out waiting for isolated job'); }

test('focused search grounds shortlist and matches in real catalogue IDs, with fresh stock and honest partial coverage', async () => {
  const catalogue = Array.from({ length: 100 }, (_, id) => product(id)), calls = []; let reads = 0;
  const current = catalogue.map(p => ({ ...p, warehouseQty: 7, totalQty: 7 }));
  const work = job();
  await search(work, Buffer.from('not sent to external services'), async () => ++reads === 1 ? catalogue : current, mockServices({ ask: async (parts, prompt) => {
    calls.push({ parts, prompt });
    if (parts.length === 1) { assert.match(prompt, /Actual garment 99/); return { candidates: [99, 2, 99, 4444, '3', null] }; }
    if (prompt.includes('labelled catalogue contact sheet')) { assert.deepEqual(JSON.parse(parts[1]), [99, 2]); return { matches: [matched(99), matched(888)] }; }
    return { matches: [matched(99), matched(888)] };
  } }));
  assert.equal(work.status, 'complete'); assert.equal(calls.length, 3);
  assert.equal(work.matches.length, 1); assert.equal(work.matches[0].product.handle, 'article-99');
  assert.equal(work.matches[0].product.warehouseQty, 7); assert.equal(work.matches[0].product.variants[0].sku, 'SKU-99');
  assert.deepEqual(work.coverage, { inspected: 2, total: 100, selected: 2, missingImages: 0, unsearched: 98, strategy: 'focused' });
  assert.equal(work.canSearchFull, true); assert.match(work.message, /confirm|choose/i); assert.equal(reads, 2);
});

test('empty coarse shortlist does not invent a match or claim unsearched catalogue was inspected', async () => {
  const catalogue = Array.from({ length: 40 }, (_, id) => product(id)), work = job(); let asks = 0;
  await search(work, Buffer.alloc(0), async () => catalogue, mockServices({ ask: async () => { asks++; return { candidates: [999, '0'] }; }, photo: async () => assert.fail('No detailed photo should be loaded') }));
  assert.equal(work.status, 'complete'); assert.equal(asks, 1); assert.deepEqual(work.matches, []);
  assert.equal(work.coverage.inspected, 0); assert.equal(work.coverage.unsearched, 40); assert.equal(work.canSearchFull, true);
});

test('full catalogue search retains later batch winners, re-ranks repeatedly and bounds detailed image count', async () => {
  const catalogue = Array.from({ length: 400 }, (_, id) => product(id)), work = job(true); let detailCount = 0, calls = 0;
  await search(work, Buffer.alloc(0), async () => catalogue, mockServices({ ask: async (parts, prompt) => {
    calls++;
    if (prompt.includes('labelled catalogue contact sheet')) { const ids = JSON.parse(parts[1]); return { matches: [...new Set([ids[0], ids[Math.floor(ids.length / 2)], ids.at(-1)])].map(matched) }; }
    detailCount = parts.length; return { matches: [matched(399)] };
  } }));
  assert.equal(work.status, 'complete'); assert.equal(work.coverage.inspected, 400); assert.equal(work.coverage.unsearched, 0); assert.equal(work.canSearchFull, false);
  assert.equal(work.matches[0].product.handle, 'article-399'); assert.ok(detailCount <= 17); assert.ok(calls > 18, 'multiple winner re-ranking passes ran');
});

test('image coverage counts unreadable images and missing primary images separately from unsearched articles', async () => {
  const catalogue = [product(0), product(1), product(2, { image: null })], work = job();
  await search(work, Buffer.alloc(0), async () => catalogue, mockServices({ sheet: async () => ({ data: 'grid', available: [] }), ask: async () => assert.fail('Unavailable images must never be sent as seen') }));
  assert.equal(work.status, 'complete'); assert.equal(work.coverage.inspected, 0); assert.equal(work.coverage.missingImages, 3); assert.equal(work.coverage.unsearched, 0);
  assert.deepEqual(work.matches, []);
});

test('matches only retain articles that still exist when quantities are refreshed', async () => {
  let reads = 0; const work = job();
  await search(work, Buffer.alloc(0), async () => ++reads === 1 ? [product(0)] : [], mockServices({ ask: async () => ({ matches: [matched(0)] }) }));
  assert.equal(work.status, 'complete'); assert.deepEqual(work.matches, []);
});

test('cancellation aborts the active provider request and never falls back to another provider', async () => {
  const controller = new AbortController(); let calls = 0, signal;
  const pending = vision(['query'], 'compare', { GEMINI_API_KEY: 'mock', ANTHROPIC_API_KEY: 'mock-other' }, async (url, options) => {
    calls++; signal = options.signal;
    return new Promise((resolve, reject) => options.signal.addEventListener('abort', () => reject(Object.assign(new Error('Stopped'), { name: 'AbortError' })), { once: true }));
  }, { signal: controller.signal });
  controller.abort();
  await assert.rejects(pending, { name: 'AbortError' }); assert.equal(signal.aborted, true); assert.equal(calls, 1);
});

test('overall recognition deadline aborts the network request and marks the job failed, not complete', async () => {
  let aborted = false; const work = job(), catalogue = Array.from({ length: 40 }, (_, id) => product(id));
  await search(work, Buffer.alloc(0), async () => catalogue, mockServices({ config: { GEMINI_API_KEY: 'mock' }, ask: undefined, timeoutMs: 40, request: async (url, options) => new Promise((resolve, reject) => options.signal.addEventListener('abort', () => { aborted = true; reject(Object.assign(new Error('timeout'), { name: 'AbortError' })); }, { once: true })) }));
  assert.equal(aborted, true); assert.equal(work.status, 'failed'); assert.match(work.message, /timed out/);
});

test('jobs are isolated by user, bounded in a queue and real cancellation releases the next job', async () => {
  const gates = [], service = createService(async () => [], mockServices({ prepareInput: () => new Promise(resolve => gates.push(resolve)) }));
  const first = service.create('first', Buffer.from('private-photo'));
  const second = service.create('second', Buffer.from('second-photo'));
  assert.equal(service.get('second', first.id), null); assert.equal(service.cancel('second', first.id), null);
  const publicJob = service.get('first', first.id);
  for (const key of ['owner', 'buffer', 'controller']) assert.equal(key in publicJob, false);
  assert.equal(service.get('second', second.id).status, 'queued');
  assert.equal(service.create('first', Buffer.alloc(0)).status, 409);
  service.create('third', Buffer.alloc(0)); service.create('fourth', Buffer.alloc(0)); assert.equal(service.create('fifth', Buffer.alloc(0)).status, 429);
  assert.equal(service.cancel('second', second.id).status, 'cancelled');
  assert.equal(service.cancel('first', first.id).status, 'cancelled'); gates[0]('query');
  await eventually(() => gates.length === 2);
  assert.equal(service.get('second', second.id).status, 'cancelled');
  // Release queued isolated work so no background promise is left holding photos.
  gates[1]('query'); await eventually(() => gates.length === 3); gates[2]('query');
});
test('configuration diagnostics support Anthropic-only accounts without claiming vision or quota was tested', async () => {
  const urls = [];
  const result = await diagnostics({ ANTHROPIC_API_KEY: 'mock' }, async url => { urls.push(url); return response(200, { data: [{ id: 'claude-sonnet-4-6' }] }); });
  assert.equal(result.gemini.configured, false); assert.equal(result.anthropic.modelListed, true); assert.equal(result.recognitionTested, false);
  assert.equal(urls.length, 1); assert.match(urls[0], /\/v1\/models/); assert.doesNotMatch(urls[0], /messages/);
  const empty = await diagnostics({}, async () => assert.fail('No key means no provider request')); assert.equal(empty.gemini.configured, false); assert.equal(empty.anthropic.configured, false);
});

test('image signatures and dimensions are checked before allocating a bitmap or calling recognition', async () => {
  const image = new Jimp(32, 20, 0xffffffff), jpeg = await image.getBufferAsync(Jimp.MIME_JPEG), png = await image.getBufferAsync(Jimp.MIME_PNG);
  assert.deepEqual(imageHeader(jpeg), { type: 'jpeg', width: 32, height: 20 }); assert.deepEqual(imageHeader(png), { type: 'png', width: 32, height: 20 });
  const prepared = await prepareInput(png); assert.equal(imageHeader(Buffer.from(prepared, 'base64')).type, 'jpeg');
  assert.throws(() => imageHeader(Buffer.from('<svg>Not a JPEG photo</svg>')), /readable/);
  const oversized = Buffer.from(png); oversized.writeUInt32BE(100000, 16); oversized.writeUInt32BE(100000, 20); assert.throws(() => imageHeader(oversized), /resolution/);
});

test('candidate sanitization rejects malformed, repeated and invented IDs and downgrades unsupported confidence', () => {
  assert.deepEqual(cleanCandidates([null, {}, '1', 1, 1, 2, 777], new Set([1, 2])), [1, 2]);
  assert.deepEqual(cleanMatches([null, matched(777), { id: 1, confidence: 'high', reason: '' }], new Set([1])), [{ id: 1, confidence: 'low', reason: '' }]);
});

test('upload route validates real multipart bytes and owns read/cancel operations without extra permissions', async () => {
  const gates = [], routes = new Map(), router = { get: (path, handler) => routes.set('GET ' + path, handler), post: (path, handler) => routes.set('POST ' + path, handler) };
  register(router, async () => [], mockServices({ config: { GEMINI_API_KEY: 'mock' }, prepareInput: () => new Promise(resolve => gates.push(resolve)) }));
  const prefix = '/api/inventory-categorization/image-search';
  function route(method, suffix, who = 'one', body, file) {
    return new Promise((resolve, reject) => {
      const req = new PassThrough(); req.method = method; req.url = prefix + suffix; req.user = { username: who }; req.params = { id: suffix.slice(1) }; req.headers = {};
      let raw = Buffer.alloc(0);
      if (file) {
        const boundary = 'test-photo-boundary';
        raw = Buffer.concat([Buffer.from('--' + boundary + '\r\nContent-Disposition: form-data; name="image"; filename="article.jpg"\r\nContent-Type: image/jpeg\r\n\r\n'), file, Buffer.from('\r\n--' + boundary + '--\r\n')]);
        req.headers = { 'content-type': 'multipart/form-data; boundary=' + boundary, 'content-length': raw.length };
      } else req.body = body;
      const res = { statusCode: 200, status(code) { this.statusCode = code; return this; }, json(data) { resolve({ status: this.statusCode, body: data }); } };
      try { routes.get(method + ' ' + prefix + (method === 'GET' ? '/:id' : ''))(req, res); req.end(raw); } catch (error) { reject(error); }
    });
  }
  assert.equal((await route('POST', '', 'one', null, Buffer.from('pretend JPEG bytes, but not an image'))).status, 400);
  const jpeg = await new Jimp(20, 20, 0xffffffff).getBufferAsync(Jimp.MIME_JPEG);
  const posted = await route('POST', '', 'one', null, jpeg); assert.equal(posted.status, 202); const { id } = posted.body;
  assert.equal((await route('GET', '/' + id, 'two')).status, 404);
  const privateResult = (await route('GET', '/' + id)).body; assert.equal(privateResult.success, true); assert.equal(privateResult.owner, undefined); assert.equal(privateResult.buffer, undefined);
  assert.equal((await route('POST', '', 'two', { action: 'cancel', id })).status, 404);
  const cancelled = await route('POST', '', 'one', { action: 'cancel', id }); assert.equal(cancelled.body.status, 'cancelled'); gates[0]('query');
});
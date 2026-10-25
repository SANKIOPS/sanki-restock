const test = require('node:test'), assert = require('node:assert/strict'), Jimp = require('jimp');
const { vision, search, imageHeader } = require('../modules/inventory-visual-search');
function response(status, body) { return { status, ok: status >= 200 && status < 300, json: async () => body }; }
const compatibility = () => response(400, { error: { status: 'INVALID_ARGUMENT', message: 'This model only supports Interactions API.' } });
const legacy = value => response(200, { candidates: [{ content: { parts: [{ text: JSON.stringify(value) }] } }] });
const interaction = (value, extras = {}) => response(200, { status: 'completed', steps: [{ type: 'model_output', content: [{ type: 'text', text: JSON.stringify(value) }] }], ...extras });
let sequence = 0;
function config(extras = {}) { return { GEMINI_API_KEY: 'test-interactions-key-' + ++sequence, INVENTORY_GEMINI_MODEL: 'gemini-2.5-flash', ...extras }; }
function body(options) { return JSON.parse(options.body); }
function deferred() { let resolve; const promise = new Promise(done => { resolve = done; }); return { promise, resolve }; }
const isInteractions = url => url === 'https://generativelanguage.googleapis.com/v1/interactions';

test('working generateContent models remain on their valid endpoint without a compatibility retry', async () => {
  let calls = 0;
  const result = await vision(['jpeg-data'], 'compare', config(), async (url, options) => {
    calls++; assert.match(url, /\/v1beta\/models\/gemini-2\.5-flash:generateContent$/);
    assert.equal(body(options).contents[0].parts[1].inlineData.data, 'jpeg-data');
    return legacy({ matches: [] });
  });
  assert.deepEqual(result, { matches: [] }); assert.equal(calls, 1);
});

test('explicit compatibility rejection retries the same configured model through stateless Interactions with ordered JPEG images', async () => {
  const settings = config({ INVENTORY_GEMINI_MODEL: 'gemini-explicit-model' }), requests = [];
  const images = await Promise.all([0xff0000ff, 0x0000ffff].map(colour => new Jimp(3, 2, colour).getBufferAsync(Jimp.MIME_JPEG).then(buffer => buffer.toString('base64'))));
  const result = await vision(images, 'FIRST image is query; second is catalogue', settings, async (url, options) => {
    requests.push({ url, options });
    if (requests.length === 1) return compatibility();
    assert.equal(isInteractions(url), true);
    const payload = body(options);
    assert.equal(payload.model, 'gemini-explicit-model'); assert.equal(payload.store, false); assert.equal(payload.background, false); assert.equal(payload.stream, false);
    assert.deepEqual(payload.generation_config, { max_output_tokens: 4096 });
    assert.deepEqual(payload.response_format, { type: 'text', mime_type: 'application/json' });
    assert.deepEqual(payload.input, [{ type: 'text', text: 'FIRST image is query; second is catalogue' }, ...images.map(data => ({ type: 'image', mime_type: 'image/jpeg', data }))]);
    for (const input of payload.input.slice(1)) assert.equal(imageHeader(Buffer.from(input.data, 'base64')).type, 'jpeg');
    for (const key of ['tools', 'previous_interaction_id', 'agent', 'contents', 'generationConfig']) assert.equal(key in payload, false);
    assert.equal(options.headers['x-goog-api-key'], settings.GEMINI_API_KEY);
    assert.equal(options.signal, requests[0].options.signal, 'The compatibility retry shares the original deadline/cancellation signal');
    return interaction({ candidates: [7] });
  });
  assert.deepEqual(result, { candidates: [7] }); assert.equal(requests.length, 2);
});

test('API protocol cache is scoped to the exact API key and actual model, without caching photos or outputs', async () => {
  const first = config(), requests = [];
  async function request(url, options) {
    requests.push({ url, input: body(options) });
    return isInteractions(url) ? interaction({ matches: [] }) : compatibility();
  }
  await vision(['photo-a'], 'first', first, request);
  await vision(['photo-b'], 'second', first, request);
  assert.equal(requests.length, 3); assert.equal(isInteractions(requests[2].url), true);
  assert.equal(requests[2].input.input[0].text, 'second'); assert.equal(requests[2].input.input[1].data, 'photo-b');
  await vision(['photo-c'], 'other key', config(), request);
  await vision(['photo-d'], 'other model', { ...first, INVENTORY_GEMINI_MODEL: 'gemini-another-model' }, request);
  assert.equal(requests.length, 7); assert.match(requests[3].url, /generateContent$/); assert.match(requests[5].url, /generateContent$/);
});

test('unrelated HTTP 400s and authorization failures never trigger an Interactions compatibility retry', async () => {
  for (const [status, message] of [
    [400, 'Invalid API key.'], [400, 'Unknown name responseMimeType.'],
    [400, 'This model only supports Interactions API. Other unrelated request details.'],
    [403, 'This model only supports Interactions API.']
  ]) {
    let calls = 0;
    await assert.rejects(vision(['photo'], 'compare', config(), async url => {
      calls++; assert.equal(isInteractions(url), false); return response(status, { error: { message } });
    }));
    assert.equal(calls, 1, status + ': ' + message);
  }
});

test('compatibility retry is bounded even when Interactions also rejects the model and fallback credits are exhausted', async () => {
  const settings = config({ ANTHROPIC_API_KEY: 'test-anthropic-secret' }), urls = [];
  await assert.rejects(vision(['photo'], 'compare', settings, async url => {
    urls.push(url);
    if (url.includes('anthropic.com')) return response(400, { error: { message: 'Your credit balance is too low.' } });
    return compatibility();
  }), error => {
    assert.match(error.message, /Interactions API/); assert.match(error.message, /Anthropic: quota or billing/); return true;
  });
  assert.equal(urls.length, 3); assert.equal(isInteractions(urls[1]), true);
});

test('404 discovery can select an Interactions-only Flash model and caches both the model and its protocol', async () => {
  const settings = config(); delete settings.INVENTORY_GEMINI_MODEL;
  const urls = [];
  async function request(url) {
    urls.push(url);
    if (url.endsWith('/models')) return response(200, { models: [
      { name: 'models/gemini-99-flash-image', supportedGenerationMethods: ['generateContent'] },
      { name: 'models/gemini-4-flash', supportedGenerationMethods: ['interactions'] }
    ] });
    if (isInteractions(url)) return interaction({ matches: [] });
    return url.includes('gemini-2.5-flash') ? response(404, { error: { message: 'Not found' } }) : compatibility();
  }
  assert.deepEqual(await vision(['photo'], 'compare', settings, request), { matches: [] });
  assert.equal(urls.length, 4); assert.match(urls[2], /gemini-4-flash:generateContent/);
  assert.deepEqual(await vision(['new-photo'], 'compare again', settings, request), { matches: [] });
  assert.equal(urls.length, 5); assert.equal(isInteractions(urls[4]), true);
});

test('only final completed model_output text is parsed, ignoring thoughts, input, tools and SDK convenience fields', async () => {
  let calls = 0;
  const result = await vision(['photo'], 'compare', config(), async () => ++calls === 1 ? compatibility() : response(200, {
    status: 'completed', output_text: '{"matches":[{"id":900}]}', outputs: [{ type: 'text', text: '{"matches":[{"id":901}]}' }],
    steps: [
      { type: 'thought', summary: [{ type: 'text', text: '{"matches":[{"id":902}]}' }] },
      { type: 'user_input', content: [{ type: 'text', text: '{"matches":[{"id":903}]}' }] },
      { type: 'function_result', result: [{ type: 'text', text: '{"matches":[{"id":904}]}' }] },
      { type: 'model_output', content: [{ type: 'text', text: '{"matches":[{"id":905}]}' }] },
      { type: 'model_output', content: [{ type: 'image', data: 'not text' }, { type: 'text', text: '{"matches":[' }, { type: 'text', text: ']}' }] }
    ]
  }));
  assert.deepEqual(result, { matches: [] });
});

test('incomplete, failed, cancelled, in-progress and action-required interactions cannot masquerade as completed JSON matches', async () => {
  for (const status of ['incomplete', 'failed', 'cancelled', 'in_progress', 'requires_action', undefined]) {
    let calls = 0;
    await assert.rejects(vision(['photo'], 'compare', config(), async () => ++calls === 1 ? compatibility() : interaction({ matches: [{ id: 777 }] }, { status })), /recognition did not complete/);
    assert.equal(calls, 2);
  }
});

test('completed Interaction errors[] are failures and sanitize all provider error detail', async () => {
  const settings = config(), secret = settings.GEMINI_API_KEY; let calls = 0;
  await assert.rejects(vision(['photo'], 'compare', settings, async () => ++calls === 1 ? compatibility() : interaction({ matches: [] }, { errors: [
    { code: 'safety', message: 'Recognition blocked for ' + secret + ' at https://provider.example/request?key=' + secret + ' Bearer hidden-credential' }
  ] })), error => {
    assert.match(error.message, /interaction status: error/); assert.match(error.message, /Recognition blocked/);
    assert.doesNotMatch(error.message, new RegExp(secret + '|https?:|hidden-credential')); assert.match(error.message, /\[redacted\]/); return true;
  });
});

test('refusal prose, non-text-only output, missing output, invalid JSON and unexpected JSON shapes fail closed', async () => {
  const bodies = [
    { status: 'completed', steps: [{ type: 'model_output', content: [{ type: 'text', text: 'I cannot identify this image.' }] }] },
    { status: 'completed', steps: [{ type: 'model_output', content: [{ type: 'text', text: 'I refuse. {"matches":[]}' }] }] },
    { status: 'completed', steps: [{ type: 'thought', summary: [{ type: 'text', text: '{"matches":[]}' }] }] },
    { status: 'completed', steps: [{ type: 'model_output', content: [{ type: 'image', data: 'only-image' }] }] },
    { status: 'completed', steps: [{ type: 'model_output', content: [{ type: 'text', text: '{"matches":[' }] }] },
    { status: 'completed', steps: [{ type: 'model_output', content: [{ type: 'text', text: '{"refusal":"not identified"}' }] }] },
    { status: 'completed', steps: [] }
  ];
  for (const result of bodies) {
    let calls = 0;
    await assert.rejects(vision(['photo'], 'compare', config(), async () => ++calls === 1 ? compatibility() : response(200, result)));
    assert.equal(calls, 2);
  }
});

test('failed Interactions can still use a valid configured Anthropic fallback once', async () => {
  const urls = [];
  const result = await vision(['photo'], 'compare', config({ ANTHROPIC_API_KEY: 'mock-anthropic' }), async url => {
    urls.push(url);
    if (url.includes('anthropic.com')) return response(200, { content: [{ type: 'text', text: '{"matches":[]}' }] });
    return isInteractions(url) ? interaction({ matches: [{ id: 999 }] }, { status: 'incomplete' }) : compatibility();
  });
  assert.deepEqual(result, { matches: [] }); assert.equal(urls.length, 3);
});

test('cancellation during compatibility response parsing stops before the Interactions retry or provider fallback', async () => {
  const controller = new AbortController(); let calls = 0;
  await assert.rejects(vision(['photo'], 'compare', config({ ANTHROPIC_API_KEY: 'mock-anthropic' }), async () => {
    calls++; return { status: 400, ok: false, json: async () => { controller.abort(); return { error: { message: 'This model only supports Interactions API.' } }; } };
  }, { signal: controller.signal }), { name: 'AbortError' });
  assert.equal(calls, 1);
});

test('cancellation aborts an in-flight Interactions request and never starts Anthropic', async () => {
  const controller = new AbortController(), ready = deferred(); let calls = 0, interactionSignal;
  const pending = vision(['photo'], 'compare', config({ ANTHROPIC_API_KEY: 'mock-anthropic' }), async (url, options) => {
    calls++;
    if (!isInteractions(url)) return compatibility();
    interactionSignal = options.signal; ready.resolve();
    return new Promise((resolve, reject) => options.signal.addEventListener('abort', () => reject(Object.assign(new Error('cancelled'), { name: 'AbortError' })), { once: true }));
  }, { signal: controller.signal });
  await ready.promise; controller.abort();
  await assert.rejects(pending, { name: 'AbortError' }); assert.equal(interactionSignal.aborted, true); assert.equal(calls, 2);
});

test('the shared provider deadline aborts Interactions rather than leaving a background recognition running', async () => {
  let calls = 0, aborted = false;
  await assert.rejects(vision(['photo'], 'compare', config(), async (url, options) => {
    calls++; if (!isInteractions(url)) return compatibility();
    return new Promise((resolve, reject) => options.signal.addEventListener('abort', () => { aborted = true; reject(Object.assign(new Error('deadline'), { name: 'AbortError' })); }, { once: true }));
  }, { timeoutMs: 30 }), /recognition timed out/);
  assert.equal(calls, 2); assert.equal(aborted, true);
});

test('inventory search uses Interactions across all stages while SKU IDs and refreshed stock remain catalogue-owned', async () => {
  const font = await Jimp.loadFont(Jimp.FONT_SANS_16_BLACK);
  const settings = config(), catalogue = Array.from({ length: 40 }, (_, id) => ({
    handle: 'article-' + id, title: 'Actual article ' + id, image: 'https://cdn.shopify.com/' + id + '.jpg',
    variants: [{ sku: 'SKU-' + id, variant: 'Black / M', price: 1499 }], displayQty: 0, warehouseQty: 1, totalQty: 1
  }));
  let reads = 0, calls = 0;
  const job = { status: 'running', controller: new AbortController(), full: false };
  await search(job, Buffer.alloc(0), async () => ++reads === 1 ? catalogue : catalogue.map(p => ({ ...p, warehouseQty: 7, totalQty: 7 })), {
    config: settings, prepareInput: async () => 'query-jpeg-data', loadFont: async () => font,
    sheet: async entries => ({ data: 'catalogue-grid', available: entries.map(p => p.id) }), photo: async () => new Jimp(2, 2, 0xffffffff),
    request: async (url, options) => {
      calls++; if (!isInteractions(url)) return compatibility();
      const prompt = body(options).input[0].text;
      return interaction(prompt.includes('coarse shortlist') ? { candidates: [39, 2, 999] } : { matches: [{ id: 39, confidence: 'high', reason: 'Same distinctive seams and print' }, { id: 999, confidence: 'high', reason: 'Invented' }] });
    }
  });
  assert.equal(job.status, 'complete'); assert.equal(calls, 4); assert.equal(reads, 2);
  assert.equal(job.matches.length, 1); assert.equal(job.matches[0].product.handle, 'article-39'); assert.equal(job.matches[0].product.variants[0].sku, 'SKU-39');
  assert.equal(job.matches[0].product.warehouseQty, 7); assert.equal(job.matches[0].product.totalQty, 7); assert.equal(job.coverage.unsearched, 38);
});

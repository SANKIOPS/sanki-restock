const test = require('node:test'), assert = require('node:assert/strict'), fs = require('node:fs'), path = require('node:path'), vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname, '../public/inventory-visual-search.js'), 'utf8');
function deferred() { let resolve, reject; const promise = new Promise((r, j) => { resolve = r; reject = j; }); return { promise, resolve, reject }; }
async function flush() { await new Promise(resolve => setImmediate(resolve)); }
function harness(fetcher) {
  const ids = {}, images = [], timers = new Map(), revoked = [], requests = [], events = [], listeners = {}, canvases = [];
  let sequence = 0, timerSequence = 0;
  class Element {
    constructor(tag) { this.tagName = tag; this.children = []; this.hidden = false; this.disabled = false; this.attributes = {}; this._html = ''; this.textContent = ''; }
    set innerHTML(value) {
      this._html = value; this.children = [];
      if (this.className === 'visual-finder') {
        const actions = new Element('div'); actions.className = 'visual-actions'; this.actions = actions;
        for (const match of value.matchAll(/<(button|input|div)[^>]*id="([^"]+)"[^>]*>/g)) { const element = new Element(match[1]); element.id = match[2]; element.hidden = /\bhidden\b/.test(match[0]); ids[element.id] = element; }
      }
    }
    get innerHTML() { return this._html; }
    querySelector(selector) { return selector === '.visual-actions' ? this.actions : ids[selector.slice(1)]; }
    appendChild(element) { this.children.push(element); return element; }
    setAttribute(name, value) { this.attributes[name] = value; }
    click() { if (this.onclick) this.onclick(); }
    focus() { this.focused = true; }
    scrollIntoView(options) { this.scrollOptions = options; }
  }
  const finder = new Element('div'), panel = new Element('div');
  const document = { querySelector: selector => selector === '.finder' ? finder : selector === '.panel' ? panel : null, createElement(tag) {
    if (tag !== 'canvas') return new Element(tag);
    const canvas = { width: 0, height: 0, getContext: () => ({ fillRect() {}, drawImage(image, x, y, width, height) { canvas.drawn = { width, height }; } }), toBlob(callback, type, quality) { canvas.encoding = { type, quality }; callback(new Blob(['jpeg'], { type })); } };
    canvases.push(canvas); return canvas;
  } };
  class Image { set src(value) { this.url = value; images.push(this); } }
  class Form { constructor() { this.entries = []; } append(...entry) { this.entries.push(entry); } }
  const window = { dispatchEvent: event => events.push(event), addEventListener: (name, listener) => listeners[name] = listener, matchMedia: () => ({ matches: false }) };
  const context = { document, window, Image, Blob, FormData: Form, AbortController, CustomEvent: class { constructor(type, data) { this.type = type; this.detail = data.detail; } }, URL: { createObjectURL: () => 'blob:photo-' + ++sequence, revokeObjectURL: url => revoked.push(url) }, setTimeout: (callback, delay) => { const id = ++timerSequence; timers.set(id, { callback, delay }); return id; }, clearTimeout: id => timers.delete(id), console,
    fetch: (url, options = {}) => { const call = { url, options }; requests.push(call); return fetcher(call, requests); } };
  vm.runInNewContext(source, context, { filename: 'inventory-visual-search.js' });
  const host = finder.children[0];
  return { ids, host, images, requests, revoked, events, canvases, panel, listeners,
    choose(file = { size: 200, type: 'image/jpeg', name: 'phone.jpg' }, capture = false) { const input = ids[capture ? 'visualCapture' : 'visualFile']; input.files = [file]; input.onchange(); },
    load(width = 4000, height = 3000) { const image = images.at(-1); image.naturalWidth = width; image.naturalHeight = height; image.onload(); },
    runTimer(delay) { const found = [...timers].find(([, timer]) => timer.delay === delay); assert.ok(found, 'Timer ' + delay + ' is scheduled'); timers.delete(found[0]); found[1].callback(); },
    diagnostics: host.actions.children[0]
  };
}
function ok(data) { return Promise.resolve({ ok: true, status: 200, json: async () => ({ success: true, ...data }) }); }
function candidate() { return { id: 0, confidence: 'medium', reason: 'Similar pocket shape; confirm design', product: { handle: 'black-trousers', title: 'Black trousers', image: 'https://cdn.shopify.com/p.jpg', variants: [{ sku: 'BLACK-M', price: 1499 }, { sku: 'BLACK-L', price: 1599 }], displayQty: 1, warehouseQty: 2, totalQty: 3, inventoryAt: '2026-10-25T08:00:00.000Z' } }; }

test('phone capture prepares portrait photo proportionally as JPEG and displays honest candidate SKU/price/quantity details', async () => {
  const h = harness(call => call.options.method === 'POST' ? ok({ id: 'job-1' }) : ok({ status: 'complete', message: 'Confirm article', matches: [candidate()], coverage: { inspected: 2, total: 100, unsearched: 98, missingImages: 0 }, canSearchFull: true }));
  h.choose(undefined, true); h.load(3000, 4000); await flush();
  assert.equal(h.canvases[0].width, 1200); assert.equal(h.canvases[0].height, 1600); assert.equal(h.canvases[0].encoding.type, 'image/jpeg');
  assert.equal(h.requests[0].options.body.entries[0][0], 'image'); assert.equal(h.requests[0].options.body.entries[0][1].type, 'image/jpeg');
  assert.equal(h.ids.visualBroader.hidden, false); assert.match(h.ids.visualProgress.textContent, /Other articles have not been ruled out/);
  const card = h.ids.visualResults.children[1]; assert.match(card.innerHTML, /BLACK-M/); assert.match(card.innerHTML, /₹1,499–₹1,599/); assert.match(card.innerHTML, /Total owned 3/); assert.match(card.innerHTML, /confirm article/);
  card.click(); assert.equal(h.events[0].type, 'inventory:open-product'); assert.equal(h.events[0].detail.handle, 'black-trousers'); assert.equal(Object.keys(h.events[0].detail).length, 1, 'Photo does not infer an exact size SKU');
  assert.equal(h.panel.scrollOptions.block, 'start'); assert.equal(h.ids.visualUpload.disabled, false);
});

test('Clear during image preparation prevents upload and releases both object URLs', async () => {
  const h = harness(() => assert.fail('Cleared photo must not be uploaded'));
  h.choose(); h.ids.visualClear.click(); h.load(); await flush();
  assert.equal(h.requests.length, 0); assert.equal(h.revoked.length, 2); assert.equal(h.ids.visualResults.children.length, 0); assert.equal(h.ids.visualClear.hidden, true); assert.equal(h.ids.visualUpload.focused, true);
});

test('Clear during accepted upload cancels its late job instead of leaving recognition running', async () => {
  const upload = deferred();
  const h = harness(call => call.options.headers ? ok({ status: 'cancelled' }) : upload.promise);
  h.choose(); h.load(); await flush(); assert.equal(h.requests.length, 1);
  h.ids.visualClear.click(); upload.resolve({ ok: true, json: async () => ({ success: true, id: 'late-job' }) }); await flush();
  assert.equal(h.requests.length, 2); assert.deepEqual(JSON.parse(h.requests[1].options.body), { action: 'cancel', id: 'late-job' });
  assert.equal(h.ids.visualResults.children.length, 0); assert.equal(h.ids.visualProgress.textContent, '');
});

test('Clear cancels a running server job and stale polling cannot repopulate the cleared results', async () => {
  const polling = deferred();
  const h = harness(call => call.options.headers ? ok({ status: 'cancelled' }) : call.options.method === 'POST' ? ok({ id: 'active-job' }) : polling.promise);
  h.choose(); h.load(); await flush(); assert.equal(h.requests.length, 2);
  const signal = h.requests[1].options.signal; h.ids.visualClear.click(); assert.equal(signal.aborted, true);
  polling.resolve({ ok: true, json: async () => ({ success: true, status: 'complete', matches: [candidate()] }) }); await flush();
  assert.equal(h.ids.visualResults.children.length, 0); assert.equal(h.ids.visualProgress.textContent, '');
  assert.deepEqual(JSON.parse(h.requests[2].options.body), { action: 'cancel', id: 'active-job' });
});

test('full catalogue action reuses the prepared photo and explicitly sends full-search mode', async () => {
  let jobs = 0;
  const h = harness(call => call.options.method === 'POST' ? ok({ id: 'job-' + ++jobs }) : ok({ status: 'complete', matches: [], coverage: { inspected: 0, total: 100, unsearched: 100 }, canSearchFull: jobs === 1 }));
  h.choose(); h.load(); await flush(); assert.equal(h.ids.visualBroader.hidden, false);
  h.ids.visualBroader.click(); await flush();
  const posts = h.requests.filter(call => call.options.method === 'POST');
  assert.equal(posts.length, 2); assert.equal(posts[1].options.body.entries[1][1], '1'); assert.equal(h.canvases.length, 1, 'No second camera/file decode needed');
});

test('unsupported and oversize uploads fail before network or browser image decoding', () => {
  const h = harness(() => assert.fail('Invalid file should not reach service'));
  h.choose({ size: 26 * 1024 * 1024, type: 'image/jpeg' }); assert.match(h.ids.visualProgress.textContent, /25 MB/);
  h.choose({ size: 100, type: 'image/svg+xml', name: 'x.svg' }); assert.match(h.ids.visualProgress.textContent, /Choose a JPEG/);
  assert.equal(h.images.length, 0); assert.equal(h.requests.length, 0);
});

test('HEIC decode failure gives actionable phone instructions and restores controls', async () => {
  const h = harness(() => assert.fail('Unreadable file should not reach service'));
  h.choose({ size: 100, type: 'image/heic', name: 'iphone.heic' }); h.images[0].onerror(); await flush();
  assert.match(h.ids.visualProgress.textContent, /export HEIC as JPEG/); assert.equal(h.ids.visualUpload.disabled, false); assert.equal(h.revoked.length, 1);
});

test('Anthropic-only diagnostics are reported accurately without claiming an image recognition test', async () => {
  const h = harness(() => ok({ gemini: { configured: false }, anthropic: { configured: true, modelListed: true, model: 'claude-example' }, recognitionTested: false }));
  h.diagnostics.click(); await flush();
  assert.match(h.ids.visualProgress.textContent, /Anthropic/); assert.match(h.ids.visualProgress.textContent, /photo search is needed to verify recognition and quota/); assert.doesNotMatch(h.ids.visualProgress.textContent, /No Gemini key/);
});

test('temporary polling connection failure retries existing job instead of creating a second paid search', async () => {
  let polls = 0;
  const h = harness(call => call.options.method === 'POST' ? ok({ id: 'job' }) : ++polls === 1 ? Promise.reject(new Error('offline')) : ok({ status: 'complete', matches: [] }));
  h.choose(); h.load(); await flush(); assert.match(h.ids.visualProgress.textContent, /Reconnecting/);
  h.runTimer(2000); await flush(); assert.equal(h.requests.filter(call => call.options.method === 'POST').length, 1); assert.equal(polls, 2); assert.equal(h.ids.visualUpload.disabled, false);
});

test('leaving the page sends a lightweight cancellation and discards local photo references', async () => {
  const h = harness(call => call.options.headers ? ok({ status: 'cancelled' }) : call.options.method === 'POST' ? ok({ id: 'job' }) : ok({ status: 'running', message: 'Comparing' }));
  h.choose(); h.load(); await flush(); h.listeners.pagehide(); await flush();
  const cancel = h.requests.find(call => call.options.headers); assert.deepEqual(JSON.parse(cancel.options.body), { action: 'cancel', id: 'job' }); assert.equal(cancel.options.keepalive, true); assert.equal(h.revoked.length, 2);
});

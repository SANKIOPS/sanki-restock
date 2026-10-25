'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
class Element extends EventTarget {
  constructor() { super(); this.value = ''; this.hidden = false; this.disabled = false; this.open = false; this.children = []; this.readyState = 4; this.videoWidth = 1920; this.videoHeight = 1080; }
  setAttribute(key, value) { this[key] = value; }
  appendChild(el) { this.children.push(el); }
  insertAdjacentElement() {}
  focus() {}
  showModal() { this.open = true; }
  close() { this.open = false; this.dispatchEvent(new Event('close')); }
  play() { return Promise.resolve(); }
  pause() {}
  cloneNode() { const clone = new Element(); this.clone = clone; return clone; }
  replaceWith() {}
  remove() { this.removed = true; }
}
const tick = () => new Promise(resolve => setImmediate(resolve));
function deferred() { let resolve, reject; const promise = new Promise((r, j) => { resolve = r; reject = j; }); return { promise, resolve, reject }; }
function setup({ media, fetcher, native, decode, decoder = true, hasMedia = true } = {}) {
  const search = new Element(), reset = new Element(), parent = new Element(); search.parentNode = parent;
  const elements = [], document = new Element(), window = new Element(), parts = {}, timers = new Map(), drawings = [];
  let timerId = 0, decodeCalls = 0, cameraCalls = 0, requests = 0;
  for (const key of ['video', '.barcode-status', 'input', '[type="submit"]', '.barcode-retry', '.barcode-close', '.barcode-cancel', 'form']) parts[key] = new Element();
  document.createElement = tag => {
    const el = new Element(); el.tag = tag; el.querySelector = key => parts[key];
    if (tag === 'canvas') el.getContext = () => ({ save() {}, restore() {}, translate() {}, rotate() {}, drawImage(...args) { drawings.push({ canvas: el, width: el.width, height: el.height, args }); } });
    elements.push(el); return el;
  };
  document.getElementById = id => id === 'search' ? search : reset;
  document.body = new Element(); document.head = new Element();
  const fallback = { BarcodeFormat: { CODE_128: 4, EAN_13: 7, UPC_A: 14 }, BrowserMultiFormatReader: class {
    decodeFromCanvas(canvas) { decodeCalls++; if (decode) return decode(canvas, decodeCalls); throw new Error('NotFound'); }
  } };
  if (decoder) window.ZXingBrowser = fallback;
  if (native) window.BarcodeDetector = native;
  vm.runInNewContext(fs.readFileSync(require.resolve('../public/inventory-barcode.js'), 'utf8'), {
    document, window, navigator: hasMedia ? { mediaDevices: { getUserMedia: options => { cameraCalls++; return media ? media(options) : Promise.reject(Object.assign(new Error(), { name: 'NotFoundError' })); } } } : {},
    Event, CustomEvent, AbortController,
    setTimeout(callback, delay) { const id = ++timerId; timers.set(id, { callback, delay }); return id; }, clearTimeout(id) { timers.delete(id); },
    fetch: async (url, options) => { requests++; return fetcher ? fetcher(url, options) : { ok: true, json: async () => ({ success: true, match: { sku: 'SKU1', handle: 'tee' } }) }; }
  });
  return {
    search, reset, window, document, parts, elements, timers, drawings, clear: parent.children[0],
    scan: elements.find(e => e['aria-label'] === 'Scan barcode'), dialog: elements.find(e => e.tag === 'dialog'),
    calls: () => ({ decode: decodeCalls, camera: cameraCalls, requests }),
    loadScript() { window.ZXingBrowser = fallback; elements.filter(e => e.tag === 'script' && !e.removed).at(-1).onload(); },
    async timer(delay) { const entry = [...timers].find(([, item]) => item.delay === delay); assert.ok(entry, 'expected timer ' + delay); timers.delete(entry[0]); await entry[1].callback(); await tick(); }
  };
}
function camera() { let stops = 0; const track = { stop() { stops++; } }; return { stream: { getTracks: () => [track], getVideoTracks: () => [track] }, track, stops: () => stops }; }
function nativeDetector(detect, formats = ['code_128', 'ean_13']) {
  return class { static async getSupportedFormats() { return formats; } constructor(options) { this.formats = options.formats; } detect(canvas) { return detect(canvas); } };
}
function code128Pixels({ moduleWidth = 3, height = 90, ink = 0, paper = 255 } = {}) {
  // Code 128 C: start, 00, 12, 34, checksum 25, stop, with quiet zones.
  const patterns = ['211232', '212222', '112232', '131123', '321122', '2331112'];
  const row = Array(moduleWidth * 10).fill(paper);
  for (const pattern of patterns) Array.from(pattern).forEach((n, i) => { row.push(...Array(Number(n) * moduleWidth).fill(i % 2 ? paper : ink)); });
  row.push(...Array(moduleWidth * 10).fill(paper));
  const width = row.length, pixels = new Uint8ClampedArray(width * height * 4);
  for (let y = 0; y < height; y++) for (let x = 0; x < width; x++) { const i = (y * width + x) * 4; pixels[i] = pixels[i + 1] = pixels[i + 2] = row[x]; pixels[i + 3] = 255; }
  return { width, height, getContext: () => ({ getImageData: () => ({ data: pixels }) }) };
}
test('bundled fallback reads Code 128 pixels at different label sizes and contrast, preserving leading zeroes', () => {
  const context = { window: {} };
  vm.runInNewContext(fs.readFileSync(require.resolve('../public/inventory-barcode-decoder.js'), 'utf8'), context);
  for (const options of [{ moduleWidth: 1 }, { moduleWidth: 2, ink: 45, paper: 205 }, { moduleWidth: 4, height: 160 }]) {
    const result = new context.ZXingBrowser.BrowserMultiFormatReader(new Map()).decodeFromCanvas(code128Pixels(options));
    assert.equal(result.getText(), '001234');
  }
});
test('clear button empties the query and emits input so the existing list updates', () => {
  const ui = setup(); let inputs = 0;
  ui.search.addEventListener('input', () => inputs++);
  ui.search.value = 'SKU1'; ui.search.dispatchEvent(new Event('input'));
  assert.equal(ui.clear.hidden, false);
  ui.clear.onclick();
  assert.equal(ui.search.value, ''); assert.equal(ui.clear.hidden, true); assert.equal(inputs, 2);
});
test('fallback camera scan fills SKU once, bounds pixel work and releases all camera/timer resources', async () => {
  const cam = camera(); let matches = 0;
  const ui = setup({ media: async () => cam.stream, decode: () => ({ getText: () => '001234' }), fetcher: async url => {
    assert.match(url, /barcode=001234/); return { ok: true, json: async () => ({ success: true, match: { sku: 'SKU1', handle: 'tee' } }) };
  } });
  ui.window.addEventListener('inventory:barcode-match', () => matches++);
  ui.scan.onclick(); await tick();
  assert.equal(ui.calls().requests, 1); assert.equal(matches, 1); assert.equal(ui.search.value, 'SKU1');
  assert.equal(ui.dialog.open, false); assert.equal(cam.stops(), 1); assert.equal(ui.timers.size, 0);
  assert.equal(ui.drawings[0].width, 1280); assert.ok(ui.drawings[0].height < 720);
});
test('native detector avoids decoder download and never overlaps frame detection', async () => {
  const pending = deferred(), cam = camera(); let detections = 0;
  const ui = setup({ media: async () => cam.stream, decoder: false, native: nativeDetector(() => { detections++; return pending.promise; }) });
  ui.scan.onclick(); await tick();
  assert.equal(detections, 1); assert.equal(ui.elements.filter(e => e.tag === 'script').length, 0);
  assert.equal([...ui.timers.values()].filter(t => t.delay === 120).length, 0);
  pending.resolve([{ rawValue: '001234' }]); await tick();
  assert.equal(ui.calls().requests, 1); assert.equal(ui.search.value, 'SKU1'); assert.equal(cam.stops(), 1); assert.equal(ui.timers.size, 0);
});
test('camera permission and preview start before the fallback decoder has downloaded', async () => {
  const cam = camera(), ui = setup({ media: async () => cam.stream, decoder: false });
  ui.scan.onclick(); await tick();
  assert.equal(ui.calls().camera, 1); assert.equal(ui.parts.video.clone.srcObject, cam.stream); assert.equal(ui.calls().decode, 0);
  ui.loadScript(); await tick();
  assert.equal(ui.calls().decode, 1); ui.parts['.barcode-cancel'].onclick(); assert.equal(cam.stops(), 1); assert.equal(ui.timers.size, 0);
});
test('cancel during camera permission startup stops a late stream without decoding', async () => {
  const pending = deferred(), cam = camera();
  const ui = setup({ media: () => pending.promise });
  ui.scan.onclick(); await tick(); ui.parts['.barcode-cancel'].onclick(); pending.resolve(cam.stream); await tick();
  assert.equal(cam.stops(), 1); assert.equal(ui.calls().decode, 0); assert.equal(ui.dialog.open, false); assert.equal(ui.timers.size, 0);
});
test('cancel during native decoding ignores its late match and stops scheduling frames', async () => {
  const pending = deferred(), cam = camera();
  const ui = setup({ media: async () => cam.stream, native: nativeDetector(() => pending.promise) });
  ui.scan.onclick(); await tick(); ui.parts['.barcode-cancel'].onclick(); pending.resolve([{ rawValue: '001234' }]); await tick();
  assert.equal(ui.calls().requests, 0); assert.equal(ui.search.value, ''); assert.equal(cam.stops(), 1); assert.equal(ui.timers.size, 0);
});
test('cancelled barcode lookup cannot repopulate a cleared search', async () => {
  const pending = deferred(); let signal;
  const ui = setup({ fetcher: (_, options) => { signal = options.signal; return pending.promise; } });
  ui.scan.onclick(); await tick(); ui.parts.input.value = '001234';
  ui.parts.form.onsubmit({ preventDefault() {} }); ui.parts['.barcode-cancel'].onclick(); ui.clear.onclick();
  pending.resolve({ ok: true, json: async () => ({ success: true, match: { sku: 'LATE', handle: 'tee' } }) }); await tick();
  assert.equal(signal.aborted, true); assert.equal(ui.search.value, ''); assert.equal(ui.timers.size, 0);
});
test('repeated handheld scanner submissions share one lookup until it finishes', async () => {
  const pending = deferred(), ui = setup({ fetcher: () => pending.promise });
  ui.scan.onclick(); await tick(); ui.parts.input.value = '001234';
  ui.parts.form.onsubmit({ preventDefault() {} }); ui.parts.form.onsubmit({ preventDefault() {} });
  assert.equal(ui.calls().requests, 1);
  pending.resolve({ ok: false, json: async () => ({ error: 'No matching SKU' }) }); await tick();
  assert.equal(ui.parts['[type="submit"]'].disabled, false); ui.parts['.barcode-cancel'].onclick();
});
test('native failures switch to bundled fallback without requesting the camera again', async () => {
  const cam = camera(), ui = setup({ media: async () => cam.stream, native: nativeDetector(async () => { throw new Error('Native unavailable'); }), decode: () => ({ getText: () => '001234' }) });
  ui.scan.onclick(); await tick(); await ui.timer(120); await ui.timer(120);
  assert.equal(ui.calls().camera, 1); assert.equal(ui.calls().decode, 1); assert.equal(ui.search.value, 'SKU1'); assert.equal(ui.timers.size, 0);
});
test('several visible native barcodes require one intended label, with no arbitrary Shopify lookup', async () => {
  const cam = camera(), ui = setup({ media: async () => cam.stream, native: nativeDetector(async () => [{ rawValue: '001' }, { rawValue: '002' }]) });
  ui.scan.onclick(); await tick(); assert.match(ui.parts['.barcode-status'].textContent, /More than one/); assert.equal(ui.calls().requests, 0);
  ui.parts['.barcode-cancel'].onclick(); assert.equal(ui.timers.size, 0);
});
test('fallback periodically rotates and checks the full frame while maintaining one scheduled attempt', async () => {
  const cam = camera(), ui = setup({ media: async () => cam.stream }); ui.scan.onclick(); await tick();
  for (let i = 0; i < 4; i++) { assert.equal([...ui.timers.values()].filter(t => t.delay === 120).length, 1); await ui.timer(120); }
  assert.equal(ui.drawings[3].height, 1280); assert.ok(ui.drawings[3].width < ui.drawings[3].height);
  assert.equal(ui.drawings[4].height, 720); assert.equal(ui.drawings[4].width, 1280);
  ui.parts['.barcode-cancel'].onclick(); assert.equal(ui.timers.size, 0);
});
test('environment camera, bounded resolution and supported continuous focus are requested', async () => {
  const cam = camera(); let applied;
  cam.track.getCapabilities = () => ({ focusMode: ['continuous', 'manual'] }); cam.track.applyConstraints = async value => { applied = value; };
  const ui = setup({ media: async options => {
    assert.equal(options.video.facingMode.ideal, 'environment'); assert.equal(options.video.width.ideal, 1280); assert.equal(options.audio, false); return cam.stream;
  } });
  ui.scan.onclick(); await tick(); assert.equal(applied.advanced[0].focusMode, 'continuous'); ui.parts['.barcode-cancel'].onclick();
});
test('older tablet constraint rejection retries simple rear-camera constraints', async () => {
  const cam = camera(); let attempts = 0;
  const ui = setup({ media: async options => {
    attempts++; if (attempts === 1) throw Object.assign(new Error(), { name: 'OverconstrainedError' });
    assert.equal(options.video.width, undefined); return cam.stream;
  } });
  ui.scan.onclick(); await tick(); assert.equal(attempts, 2); assert.equal(ui.calls().decode, 1); ui.parts['.barcode-cancel'].onclick();
});
test('denied, busy, missing and unsupported cameras keep manual entry and retry usable', async () => {
  for (const [name, expected] of [['NotAllowedError', /denied/], ['NotReadableError', /busy/], ['NotFoundError', /No camera/], ['unsupported', /could not start/]]) {
    const ui = setup({ hasMedia: name !== 'unsupported', media: async () => { throw Object.assign(new Error(), { name }); }, fetcher: async () => ({ ok: false, json: async () => ({ error: 'No matching SKU' }) }) });
    ui.scan.onclick(); await tick(); assert.match(ui.parts['.barcode-status'].textContent, expected); assert.equal(ui.parts['.barcode-retry'].disabled, false);
    ui.parts.input.value = '000'; ui.parts.form.onsubmit({ preventDefault() {} }); await tick();
    assert.equal(ui.parts['.barcode-status'].textContent, 'No matching SKU'); assert.equal(ui.parts['[type="submit"]'].disabled, false); assert.equal(ui.dialog.open, true); ui.parts['.barcode-cancel'].onclick();
  }
});
test('a timed-out barcode lookup aborts, explains retry and does not keep the submit button locked', async () => {
  const ui = setup({ fetcher: (_, options) => new Promise((resolve, reject) => options.signal.addEventListener('abort', () => reject(Object.assign(new Error(), { name: 'AbortError' })))) });
  ui.scan.onclick(); await tick(); ui.parts.input.value = '001234'; ui.parts.form.onsubmit({ preventDefault() {} });
  await ui.timer(20000); assert.match(ui.parts['.barcode-status'].textContent, /taking too long/); assert.equal(ui.parts['[type="submit"]'].disabled, false); assert.equal(ui.timers.size, 0); ui.parts['.barcode-cancel'].onclick();
});
test('hidden tab closes scanner and releases its camera', async () => {
  const cam = camera(), ui = setup({ media: async () => cam.stream }); ui.scan.onclick(); await tick();
  ui.document.hidden = true; ui.document.dispatchEvent(new Event('visibilitychange'));
  assert.equal(ui.dialog.open, false); assert.equal(cam.stops(), 1); assert.equal(ui.timers.size, 0);
});
test('a failed decoder download releases the camera and can be retried', async () => {
  const cam = camera(), ui = setup({ media: async () => cam.stream, decoder: false });
  ui.scan.onclick(); await tick(); ui.elements.find(e => e.tag === 'script').onerror(); await tick();
  assert.match(ui.parts['.barcode-status'].textContent, /scanner could not load/); assert.equal(cam.stops(), 1); assert.equal(ui.timers.size, 0);
  ui.parts['.barcode-retry'].onclick(); await tick(); assert.equal(ui.elements.filter(e => e.tag === 'script').length, 2);
  ui.loadScript(); await tick(); assert.equal(ui.calls().decode, 1); ui.parts['.barcode-cancel'].onclick(); assert.equal(ui.timers.size, 0);
});
test('restarting a camera while old native detection is pending keeps the new preview and stream intact', async () => {
  const old = deferred(), first = camera(), second = camera(); let mediaCalls = 0, detections = 0;
  const ui = setup({ media: async () => (++mediaCalls === 1 ? first.stream : second.stream), native: nativeDetector(() => (++detections === 1 ? old.promise : Promise.resolve([]))) });
  ui.scan.onclick(); await tick(); ui.parts['.barcode-retry'].onclick(); await tick();
  const currentPreview = ui.parts.video.clone.clone;
  assert.equal(first.stops(), 1); assert.equal(currentPreview.srcObject, second.stream);
  old.resolve([{ rawValue: 'OLD' }]); await tick();
  assert.equal(ui.calls().requests, 0); assert.equal(currentPreview.srcObject, second.stream); assert.equal(second.stops(), 0);
  ui.parts['.barcode-cancel'].onclick(); assert.equal(second.stops(), 1); assert.equal(ui.timers.size, 0);
});
test('a disconnected camera stops scanning and offers restart instead of processing stale frames', async () => {
  const cam = camera(); let ended;
  cam.track.addEventListener = (type, callback) => { if (type === 'ended') ended = callback; };
  const ui = setup({ media: async () => cam.stream }); ui.scan.onclick(); await tick(); ended();
  assert.match(ui.parts['.barcode-status'].textContent, /camera stopped/); assert.equal(ui.parts['.barcode-retry'].disabled, false);
  assert.equal(cam.stops(), 1); assert.equal(ui.timers.size, 0); assert.equal(ui.calls().requests, 0); ui.parts['.barcode-cancel'].onclick();
});
test('a hung native detection retires that detector, switches to fallback and ignores its late result', async () => {
  const pending = deferred(), cam = camera(); let detections = 0;
  const ui = setup({ media: async () => cam.stream, native: nativeDetector(() => { detections++; return pending.promise; }) });
  ui.scan.onclick(); await tick(); assert.equal(detections, 1);
  const nativeCanvas = ui.drawings[0].canvas;
  await ui.timer(1800); await ui.timer(120); await ui.timer(120);
  assert.equal(detections, 1); assert.equal(ui.calls().decode, 2); assert.notEqual(ui.drawings.at(-1).canvas, nativeCanvas);
  pending.resolve([{ rawValue: 'LATE' }]); await tick();
  assert.equal(ui.calls().requests, 0); assert.equal(ui.search.value, ''); assert.equal(detections, 1);
  ui.parts['.barcode-cancel'].onclick(); assert.equal(ui.timers.size, 0); assert.equal(cam.stops(), 1);
});
test('cancelling a hung native detection clears its deadline and never starts a fallback or lookup', async () => {
  const pending = deferred(), cam = camera(); let detections = 0;
  const ui = setup({ media: async () => cam.stream, native: nativeDetector(() => { detections++; return pending.promise; }) });
  ui.scan.onclick(); await tick(); assert.equal([...ui.timers.values()].filter(t => t.delay === 1800).length, 1);
  ui.parts['.barcode-cancel'].onclick(); await tick();
  assert.equal(ui.timers.size, 0); assert.equal(ui.calls().decode, 0); assert.equal(cam.stops(), 1);
  pending.reject(new Error('Late native failure')); await tick();
  assert.equal(ui.calls().requests, 0); assert.equal(detections, 1); assert.equal(ui.timers.size, 0);
});

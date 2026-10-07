'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
class Element extends EventTarget {
  constructor() { super(); this.value = ''; this.hidden = false; this.disabled = false; this.open = false; this.children = []; }
  setAttribute(key, value) { this[key] = value; }
  appendChild(el) { this.children.push(el); }
  insertAdjacentElement() {}
  focus() {}
  showModal() { this.open = true; }
  close() { this.open = false; this.dispatchEvent(new Event('close')); }
  cloneNode() { return new Element(); }
  replaceWith() {}
}
const tick = () => new Promise(resolve => setImmediate(resolve));
test('bundled decoder reads a Code 128 label with leading zeroes from pixels', () => {
  const context = { window: {} };
  vm.runInNewContext(fs.readFileSync(require.resolve('../public/inventory-barcode-decoder.js'), 'utf8'), context);
  // Code 128 C: start, 00, 12, 34, checksum 25, stop, with quiet zones.
  const patterns = ['211232', '212222', '112232', '131123', '321122', '2331112'];
  const row = Array(30).fill(255);
  for (const pattern of patterns) Array.from(pattern).forEach((n, i) => { row.push(...Array(Number(n) * 3).fill(i % 2 ? 255 : 0)); });
  row.push(...Array(30).fill(255));
  const width = row.length, height = 90, pixels = new Uint8ClampedArray(width * height * 4);
  for (let y = 0; y < height; y++) for (let x = 0; x < width; x++) { const i = (y * width + x) * 4; pixels[i] = pixels[i + 1] = pixels[i + 2] = row[x]; pixels[i + 3] = 255; }
  const canvas = { width, height, getContext: () => ({ getImageData: () => ({ data: pixels }) }) };
  const result = new context.ZXingBrowser.BrowserMultiFormatReader().decodeFromCanvas(canvas);
  assert.equal(result.getText(), '001234');
});
function deferred() { let resolve; const promise = new Promise(r => { resolve = r; }); return { promise, resolve }; }
function setup({ media, fetcher } = {}) {
  const search = new Element(), reset = new Element(), parent = new Element(); search.parentNode = parent;
  const elements = [], document = new Element(), window = new Element(), parts = {};
  for (const key of ['video', '.barcode-status', 'input', '[type="submit"]', '.barcode-retry', '.barcode-close', '.barcode-cancel', 'form']) parts[key] = new Element();
  document.createElement = tag => { const el = new Element(); el.tag = tag; el.querySelector = key => parts[key]; elements.push(el); return el; };
  document.getElementById = id => id === 'search' ? search : reset;
  document.body = new Element(); document.head = new Element();
  let callback, stops = 0;
  window.ZXingBrowser = { BrowserMultiFormatReader: class { async decodeFromStream(stream, video, cb) { callback = cb; return { stop() { stops++; } }; } } };
  vm.runInNewContext(fs.readFileSync(require.resolve('../public/inventory-barcode.js'), 'utf8'), { document, window, navigator: { mediaDevices: { getUserMedia: media || (() => Promise.reject(Object.assign(new Error(), { name: 'NotFoundError' }))) } }, Event, CustomEvent, AbortController, fetch: fetcher || (async () => ({ ok: true, json: async () => ({ success: true, match: { sku: 'SKU1', handle: 'tee' } }) })) });
  return { search, reset, window, parts, clear: parent.children[0], scan: elements.find(e => e['aria-label'] === 'Scan barcode'), dialog: elements.find(e => e.tag === 'dialog'), callback: () => callback, stops: () => stops };
}
test('clear button empties the query and emits input so the existing list updates', () => {
  const ui = setup(); let inputs = 0;
  ui.search.addEventListener('input', () => inputs++);
  ui.search.value = 'SKU1'; ui.search.dispatchEvent(new Event('input'));
  assert.equal(ui.clear.hidden, false);
  ui.clear.onclick();
  assert.equal(ui.search.value, ''); assert.equal(ui.clear.hidden, true); assert.equal(inputs, 2);
});
test('camera decoded barcode fills the SKU exactly once and releases camera tracks', async () => {
  let trackStops = 0, requests = 0, matches = 0;
  const stream = { getTracks: () => [{ stop() { trackStops++; } }] };
  const ui = setup({ media: async () => stream, fetcher: async url => { requests++; assert.match(url, /barcode=001234/); return { ok: true, json: async () => ({ success: true, match: { sku: 'SKU1', handle: 'tee' } }) }; } });
  ui.window.addEventListener('inventory:barcode-match', () => matches++);
  ui.scan.onclick(); await tick();
  const cb = ui.callback(), result = { getText: () => '001234' }, controls = { stop() {} };
  cb(result, null, controls); cb(result, null, controls); await tick();
  assert.equal(requests, 1); assert.equal(matches, 1); assert.equal(ui.search.value, 'SKU1'); assert.equal(ui.dialog.open, false); assert.ok(trackStops > 0); assert.ok(ui.stops() > 0);
});
test('cancel during camera permission startup stops a late stream without starting decoding', async () => {
  const pending = deferred(); let stops = 0;
  const ui = setup({ media: () => pending.promise });
  ui.scan.onclick(); await tick(); ui.parts['.barcode-cancel'].onclick();
  pending.resolve({ getTracks: () => [{ stop() { stops++; } }] }); await tick();
  assert.equal(stops, 1); assert.equal(ui.callback(), undefined); assert.equal(ui.dialog.open, false);
});
test('cancelled barcode lookup cannot repopulate a cleared search', async () => {
  const pending = deferred(); let signal;
  const ui = setup({ fetcher: (_, options) => { signal = options.signal; return pending.promise; } });
  ui.scan.onclick(); await tick(); ui.parts.input.value = '001234';
  ui.parts.form.onsubmit({ preventDefault() {} }); ui.parts['.barcode-cancel'].onclick(); ui.clear.onclick();
  pending.resolve({ ok: true, json: async () => ({ success: true, match: { sku: 'LATE', handle: 'tee' } }) }); await tick();
  assert.equal(signal.aborted, true); assert.equal(ui.search.value, '');
});
test('camera and unknown barcode failures keep manual entry and retry usable', async () => {
  const ui = setup({ fetcher: async () => ({ ok: false, json: async () => ({ error: 'No matching SKU' }) }) });
  ui.scan.onclick(); await tick();
  assert.match(ui.parts['.barcode-status'].textContent, /No camera/);
  ui.parts.input.value = '000'; ui.parts.form.onsubmit({ preventDefault() {} }); await tick();
  assert.equal(ui.parts['.barcode-status'].textContent, 'No matching SKU'); assert.equal(ui.parts['[type="submit"]'].disabled, false); assert.equal(ui.dialog.open, true);
});

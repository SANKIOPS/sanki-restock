'use strict';
const test = require('node:test'), assert = require('node:assert/strict');
const fs = require('node:fs'), vm = require('node:vm');
const source = fs.readFileSync(require.resolve('../public/inventory-enhancements.js'), 'utf8');
const view = require('../public/inventory-view-model');
function section(from, until) { return source.slice(source.indexOf(from), source.indexOf(until, source.indexOf(from))); }
function deferred() { let resolve; return { promise: new Promise(r => { resolve = r; }), resolve: value => resolve(value) }; }
test('rapid repeated detail requests share one read, and a late response cannot replace the current selection', async () => {
  const pending = {}, reads = [], updated = [];
  const context = { details: {}, detailInflight: {}, expandedHandle: 'b', Promise, findProductRow: handle => handle, updateExpansion: handle => updated.push(handle), fetch: url => { const handle = url.split('/').pop(); reads.push(handle); pending[handle] = deferred(); return pending[handle].promise; } };
  vm.runInNewContext(section('function loadDetails(', 'var lightbox='), context);
  const a1 = context.loadDetails({ handle: 'a' }), a2 = context.loadDetails({ handle: 'a' }), b = context.loadDetails({ handle: 'b' });
  assert.deepEqual(reads, ['a', 'b']);
  pending.b.resolve({ ok: true, json: async () => ({ success: true, images: ['b.jpg'] }) }); await b;
  pending.a.resolve({ ok: true, json: async () => ({ success: true, images: ['a.jpg'] }) }); await Promise.all([a1, a2]);
  assert.deepEqual(updated, ['b']);
  await context.loadDetails({ handle: 'a' }); assert.deepEqual(reads, ['a', 'b']);
});
test('closing details before the read completes cannot reopen them, and failed galleries can retry', async () => {
  let calls = 0, updates = 0;
  const context = { details: {}, detailInflight: {}, expandedHandle: '', Promise, findProductRow: handle => handle, updateExpansion: () => updates++, fetch: async () => { calls++; return calls === 1 ? { ok: false } : { ok: true, json: async () => ({ success: true, images: ['a.jpg'] }) }; } };
  vm.runInNewContext(section('function loadDetails(', 'var lightbox='), context);
  await context.loadDetails({ handle: 'a', images: [] }); assert.equal(updates, 0);
  await context.loadDetails({ handle: 'a', images: [] }); assert.equal(calls, 2); assert.equal(context.details.a.error, undefined);
});
test('opening, switching and closing details retain the product rows and never replace the table body', () => {
  const products = ['a', 'b'].map(handle => ({ handle, variants: [{ sku: handle.toUpperCase(), availableQty: 1 }] }));
  const panels = [], rows = products.map(p => ({ dataset: { handle: p.handle }, label: {}, setAttribute(key, value) { this[key] = value; }, querySelector() { return this.label; }, insertAdjacentHTML(where, html) { panels.push({ html, remove() { panels.splice(panels.indexOf(this), 1); } }); } }));
  const host = { querySelectorAll: sel => sel === '.expanded-row' ? panels.slice() : rows, set innerHTML(value) { assert.fail('Product list was rebuilt while toggling details'); } };
  const context = { document: { getElementById: () => host }, products, mode: 'all', expandedHandle: 'a', view, preserveRow: (_, change) => change(), expansionRow: p => p.handle, bindExpansion() {} };
  vm.runInNewContext(section('function findProductRow(', 'function bindProductRows('), context);
  context.updateExpansion(rows[0]); assert.equal(panels[0].html, 'a');
  context.expandedHandle = 'b'; context.updateExpansion(rows[1]); assert.equal(panels.length, 1); assert.equal(panels[0].html, 'b');
  assert.equal(rows[0]['aria-expanded'], 'false'); assert.equal(rows[1]['aria-expanded'], 'true');
  context.expandedHandle = ''; context.updateExpansion(rows[1]); assert.equal(panels.length, 0); assert.equal(rows[1]['aria-expanded'], 'false');
});

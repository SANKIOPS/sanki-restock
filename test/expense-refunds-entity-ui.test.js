'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname, '../public/expense-refunds.js'), 'utf8');

function catalog(nature, extra = {}) {
  return { success: true, today: '2098-06-23', sources: [{ key: 'expense:' + nature, kind: 'expense', nature,
    reference: nature === 'SAMAST' ? 'EX-TEST-310' : 'EX-TEST-620', vendor: 'Synthetic stationery vendor', date: '2098-06-15',
    remaining: 731, unpaidAmount: 0, originalAmount: 731, paidAmount: 731, refundedAmount: 0, payments: [] }],
  existingMovements: ['SANKI', 'SAMAST'].map(entity => ({ id: 'RC-' + entity, nature: entity, mode: 'bank', remaining: 731, account: entity + ' Bank', date: '2098-06-23' })),
  refunds: [], recoverables: [], recoveries: [], modes: ['bank', 'cash', 'voucher'], reasons: ['return'], summary: {}, ...extra };
}

// Minimal DOM implementation drives the actual page handlers and form-building functions.
function harness({ allowed = ['SANKI', 'SAMAST'], response, postResponse, manage = true } = {}) {
  const nodes = {}, requests = [], uploads = [], messages = [];
  function parse(markup, owner) {
    for (const match of String(markup).matchAll(/<(input|select|textarea|button|div|p|h3)\b([^>]*)>/g)) {
      const attrs = match[2], id = (attrs.match(/\bid="([^"]+)"/) || [])[1];
      const classes = ((attrs.match(/\bclass="([^"]+)"/) || [])[1] || '').split(/\s+/).filter(Boolean);
      const child = makeNode(match[1]);
      child.id = id || ''; child.value = (attrs.match(/\bvalue="([^"]*)"/) || [])[1] || '';
      child.disabled = /\bdisabled\b/.test(attrs); child.dataset.index = (attrs.match(/data-index="([^"]*)"/) || [])[1];
      if (match[1] === 'select') {
        const rest = markup.slice(match.index + match[0].length), end = rest.indexOf('</select>');
        child.innerHTML = rest.slice(0, end);
      }
      if (id) nodes[id] = child;
      for (const name of classes) owner.fields['.' + name] = child;
      owner.controls.push(child);
      if (match[1] === 'h3' && !owner.fields.h3) owner.fields.h3 = child;
    }
  }
  function makeNode(tag = 'div') {
    let markup = '';
    const n = { tag, value: '', disabled: false, hidden: false, textContent: '', files: [], fields: {}, controls: [], children: [], insertions: [], dataset: {}, listeners: {}, open: false,
      addEventListener(event, fn) { this.listeners[event] = fn; },
      showModal() { this.open = true; }, close() { this.open = false; if (this.listeners.close) this.listeners.close(); },
      appendChild(child) { this.children.push(child); if (child.id) nodes[child.id] = child; },
      querySelector(selector) { return this.fields[selector] || null; },
      querySelectorAll(selector) { return selector === 'input,select,textarea,button' ? this.controls.concat(...this.children.map(child => child.controls)) : []; },
      insertAdjacentHTML(position, html) { this.insertions.push({ position, html }); parse(html, this); }, remove() {} };
    Object.defineProperty(n, 'innerHTML', { get: () => markup, set(value) {
      markup = String(value); n.fields = {}; n.controls = []; n.children = [];
      if (tag === 'select') {
        const options = [...markup.matchAll(/<option\b([^>]*)>([^<]*)/g)].map(m => ({ value: (m[1].match(/value="([^"]*)"/) || [])[1] || m[2], selected: /\bselected\b/.test(m[1]) }));
        n.value = (options.find(o => o.selected) || options[0] || {}).value || '';
      } else parse(markup, n);
    } });
    return n;
  }
  nodes.panel = makeNode();
  function all(selector) {
    const components = nodes.rf_components ? nodes.rf_components.children : [];
    if (selector === '#rf_components .rf-component') return components;
    if (selector === '#rf_components .rf_mode') return components.map(n => n.fields['.rf_mode']);
    if (selector === '#rf_components .rf_component_amount') return components.map(n => n.fields['.rf_component_amount']);
    if (selector === '.rf_allocation_amount') return nodes.rf_allocations ? nodes.rf_allocations.controls.filter(n => n.dataset.index !== undefined && n.tag === 'input') : [];
    return [];
  }
  const cfg = { canManageExpenseRefunds: manage, approvalNatures: allowed, accountsByNature: { SANKI: ['SANKI Bank'], SAMAST: ['SAMAST Bank'] } };
  let getResponse = response || (url => catalog(new URL(url, 'https://local.test').searchParams.get('nature')));
  const context = { window: { crypto: { randomUUID: () => 'request-id' } }, URLSearchParams, cfg, tab: 'receivables',
    document: { getElementById: id => nodes[id] || null, createElement: makeNode, head: makeNode(), body: makeNode(), querySelectorAll: all, querySelector: selector => all(selector)[0] || null },
    el: id => nodes[id], esc: value => String(value || '').replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;').replaceAll('"', '&quot;'),
    fmtBank: value => '₹' + Number(value || 0).toFixed(2), proofGallery: () => '', setMsg(text, ok) { messages.push({ text, ok }); },
    uploadFiles: async (files, nature) => { uploads.push({ files, nature }); return ['proof']; },
    api: async (url, options) => {
      requests.push({ url, options });
      if (options) return postResponse ? postResponse(url, options) : { success: true, preview: { amount: 731, components: [] }, impact: { billCredit: 731 }, refund: { id: 'RF-TEST' } };
      return getResponse(url);
    } };
  const instrumented = source.replace(/\}\(\)\);\s*$/, 'window.refundTest = { openForm, sourceOptions, doPreview, save, state: function () { return { data, formData, formNature, draft, filter }; }, setPageData: function (value) { data = value; } }; }());');
  vm.runInNewContext(instrumented, context);
  return { nodes, requests, uploads, messages, cfg, ...context.window.refundTest, render: context.window.renderRefunds, response(fn) { getResponse = fn; },
    async start() { context.window.renderRefunds(); await new Promise(resolve => setImmediate(resolve)); },
    components: () => nodes.rf_components ? nodes.rf_components.children : [] };
}

test('opening without Filter uses the visible entity and fresh authorized catalog for a reconciliation-created expense', async () => {
  const ui = harness(); await ui.start();
  assert.equal(ui.state().data.sources[0].reference, 'EX-TEST-620');
  ui.nodes.rf_entity.value = 'SAMAST';
  ui.nodes.rf_log.onclick(); await new Promise(resolve => setImmediate(resolve));
  ui.nodes.rf_source_search.value = '310'; ui.nodes.rf_source_search.oninput();
  assert.match(ui.nodes.rf_source.innerHTML, /EX-TEST-310.*Synthetic stationery vendor/);
  assert.equal(ui.state().formNature, 'SAMAST');
  assert.match(ui.nodes.rf_dialog.fields.h3.insertions[0].html, /Refund entity: <b>SAMAST<\/b>/);
  assert.ok(ui.nodes.rf_form_entity, 'Dialog exposes its pinned entity');
  assert.equal(ui.requests.length, 2);
  assert.equal(new URL(ui.requests[1].url, 'https://local.test').searchParams.get('nature'), 'SAMAST');
  assert.ok(ui.requests.every(r => !r.options), 'Opening and searching must be GET-only');
  assert.equal(ui.nodes.rf_save.disabled, true);
});

test('SAMAST-only Admin initializes the allowed entity instead of requesting unauthorized SANKI', async () => {
  const ui = harness({ allowed: ['SAMAST'] }); await ui.start();
  assert.equal(ui.nodes.rf_entity.value, 'SAMAST');
  await ui.openForm();
  assert.equal(ui.state().formNature, 'SAMAST');
  assert.ok(ui.requests.every(r => new URL(r.url, 'https://local.test').searchParams.get('nature') === 'SAMAST'));
  assert.match(ui.nodes.rf_source.innerHTML, /EX-TEST-310/);
});

test('accounts, existing receipts and proof remain pinned when the page entity/catalog changes', async () => {
  const ui = harness(); await ui.start(); ui.nodes.rf_entity.value = 'SAMAST'; await ui.openForm();
  ui.nodes.rf_entity.value = 'SANKI'; ui.setPageData(catalog('SANKI'));
  const component = ui.components()[0]; component.fields['.rf_mode'].onchange();
  assert.match(component.fields['.rf_account'].innerHTML, /SAMAST Bank/);
  assert.doesNotMatch(component.fields['.rf_account'].innerHTML, /SANKI Bank/);
  assert.match(component.fields['.rf_existing'].innerHTML, /RC-SAMAST/);
  assert.doesNotMatch(component.fields['.rf_existing'].innerHTML, /RC-SANKI/);
  ui.nodes.rf_add_source.onclick();
  ui.nodes.rf_reason.value = 'Returned stationery'; ui.nodes.rf_proofs.files = ['image'];
  await ui.doPreview();
  assert.equal(ui.uploads[0].nature, 'SAMAST');
  const payload = JSON.parse(ui.requests.at(-1).options.body);
  assert.equal(payload.sources[0].key, 'expense:SAMAST');
  assert.equal(payload.components[0].account, 'SAMAST Bank');
  assert.equal(ui.requests.filter(r => r.options).length, 1, 'Explicit preview does not save the refund');
});

test('receiving a pending return refreshes and pins its original entity regardless of page selector and stale parent amounts', async () => {
  const current = { id: 'RF-PENDING', nature: 'SAMAST', displayStatus: 'pending', pendingSources: [{ key: 'expense:SAMAST', amount: 100 }], reason: 'Partial return' };
  const ui = harness(); await ui.start();
  ui.response(() => catalog('SAMAST', { refunds: [current] }));
  await ui.openForm({ ...current, pendingSources: [{ key: 'expense:SAMAST', amount: 731 }] });
  assert.equal(ui.nodes.rf_entity.value, 'SANKI');
  assert.equal(ui.state().formNature, 'SAMAST');
  assert.equal(ui.state().draft.parentId, current.id);
  assert.equal(ui.state().draft.sources[0].amount, 100);
  assert.equal(ui.nodes.rf_form_status.disabled, true); assert.equal(ui.nodes.rf_add_source.disabled, true);
  assert.match(ui.components()[0].fields['.rf_account'].innerHTML, /SAMAST Bank/);
});

test('completed or inaccessible pending return cannot open with stale source allocations', async () => {
  for (const refunds of [[], [{ id: 'RF-P', nature: 'SAMAST', displayStatus: 'completed' }], [{ id: 'RF-P', nature: 'SAMAST', displayStatus: 'pending', pendingSources: [{ key: 'expense:HIDDEN', amount: 5 }] }]]) {
    const ui = harness({ response: () => catalog('SAMAST', { refunds }) }); await ui.start();
    await ui.openForm({ id: 'RF-P', nature: 'SAMAST' });
    assert.equal(ui.state().draft, null); assert.equal(ui.state().formData, null);
    assert.match(ui.nodes.rf_message.textContent, /no longer available/);
    assert.ok(ui.requests.every(r => !r.options));
  }
});

test('no matching source explains entity/search scope without suggesting duplicate expense creation', async () => {
  const ui = harness(); await ui.start(); ui.nodes.rf_entity.value = 'SAMAST'; await ui.openForm();
  ui.nodes.rf_source_search.value = 'not-present'; ui.nodes.rf_source_search.oninput();
  assert.equal(ui.nodes.rf_source.innerHTML, ''); assert.equal(ui.nodes.rf_add_source.disabled, true);
  assert.match(ui.nodes.rf_source_hint.textContent, /No matching authorized expense sources for SAMAST/);
  assert.match(ui.nodes.rf_source_hint.textContent, /Do not recreate an existing expense/);
  ui.nodes.rf_source_search.value = '310'; ui.nodes.rf_source_search.oninput();
  assert.equal(ui.nodes.rf_add_source.disabled, false); assert.equal(ui.nodes.rf_source_hint.textContent, '');
});

test('unauthorized entity or manager access is rejected without a catalog request', async () => {
  const ui = harness({ allowed: ['SAMAST'] }); await ui.start(); const before = ui.requests.length;
  ui.nodes.rf_entity.value = 'SANKI'; await ui.openForm();
  assert.equal(ui.requests.length, before); assert.equal(ui.state().formData, null);
  assert.match(ui.nodes.rf_message.textContent, /authorized/);
  ui.nodes.rf_entity.value = 'SAMAST'; ui.cfg.canManageExpenseRefunds = false; await ui.openForm();
  assert.equal(ui.requests.length, before);
});

test('failed fresh catalog does not fall back to an old authorized catalog', async () => {
  const ui = harness(); await ui.start(); ui.nodes.rf_entity.value = 'SAMAST';
  ui.response(() => ({ success: false, error: 'Access changed' })); await ui.openForm();
  assert.equal(ui.state().formData, null); assert.equal(ui.state().draft, null);
  assert.match(ui.nodes.rf_message.textContent, /Unable to load refund sources.*Access changed/);
  assert.equal(ui.nodes.rf_dialog.open, true); assert.ok(ui.requests.every(r => !r.options));
});

test('late response cannot overwrite a newer form or resurrect a closed form', async () => {
  const ui = harness(); await ui.start(); const waiting = [];
  ui.response(() => new Promise(resolve => waiting.push(resolve)));
  ui.nodes.rf_entity.value = 'SAMAST'; const first = ui.openForm();
  ui.nodes.rf_entity.value = 'SANKI'; const second = ui.openForm();
  waiting[1](catalog('SANKI')); await second;
  waiting[0](catalog('SAMAST')); await first;
  assert.equal(ui.state().formNature, 'SANKI'); assert.match(ui.nodes.rf_source.innerHTML, /EX-TEST-620/);
  const third = ui.openForm(); ui.nodes.rf_close.onclick(); waiting[2](catalog('SAMAST')); await third;
  assert.equal(ui.nodes.rf_dialog.open, false); assert.equal(ui.state().formData, null);
  assert.ok(ui.requests.every(r => !r.options));
});

test('permissions removed while a catalog is loading cannot expose its sources', async () => {
  const ui = harness(); await ui.start(); let finish;
  ui.response(() => new Promise(resolve => { finish = resolve; }));
  ui.nodes.rf_entity.value = 'SAMAST'; const opening = ui.openForm(); ui.cfg.approvalNatures = ['SANKI'];
  finish(catalog('SAMAST')); await opening;
  assert.equal(ui.state().formData, null); assert.equal(ui.state().draft, null);
  assert.match(ui.nodes.rf_message.textContent, /no longer have access/);
});

test('late failed request cannot replace a newer successfully loaded form', async () => {
  const ui = harness(); await ui.start(); let fail;
  ui.response(() => new Promise((resolve, reject) => { fail = reject; }));
  ui.nodes.rf_entity.value = 'SANKI'; const old = ui.openForm();
  ui.response(() => catalog('SAMAST')); ui.nodes.rf_entity.value = 'SAMAST'; await ui.openForm();
  fail(new Error('Old network failure')); await old;
  assert.equal(ui.state().formNature, 'SAMAST');
  assert.match(ui.nodes.rf_source.innerHTML, /EX-TEST-310/);
  assert.doesNotMatch(ui.nodes.rf_message.textContent, /Old network failure/);
});

test('opening and searching the refund form preserves the server records and requires an explicit preview/save', async () => {
  const existing = catalog('SAMAST'), before = JSON.stringify(existing);
  const ui = harness({ allowed: ['SAMAST'], response: () => existing }); await ui.start(); await ui.openForm();
  ui.nodes.rf_source_search.value = '310'; ui.nodes.rf_source_search.oninput(); ui.nodes.rf_add_source.onclick();
  ui.components()[0].fields['.rf_mode'].onchange();
  assert.equal(JSON.stringify(existing), before);
  assert.equal(ui.nodes.rf_save.disabled, true);
  assert.ok(ui.requests.every(r => !r.options)); assert.equal(ui.uploads.length, 0);
});

test('successful save reloads the pinned modal entity and shows the saved refund while preserving other page filters', async () => {
  const record = { id: 'RF-SAVED-SAMAST', nature: 'SAMAST', date: '2098-06-23', vendor: 'Synthetic stationery vendor',
    amount: 731, sources: [{ reference: 'EX-TEST-310', amount: 731 }], components: [], reasonType: 'return', reason: 'Returned stationery', status: 'received', displayStatus: 'received', proofs: [] };
  let saved = false;
  const ui = harness({
    response: url => { const nature = new URL(url, 'https://local.test').searchParams.get('nature'); return catalog(nature, { refunds: saved && nature === 'SAMAST' ? [record] : [] }); },
    postResponse: url => {
      if (url.endsWith('/preview')) return { success: true, preview: { amount: 731, components: [] }, impact: { billCredit: 731 } };
      saved = true; return { success: true, refund: record };
    }
  });
  await ui.start();
  ui.nodes.rf_from.value = '2098-06-01'; ui.nodes.rf_to.value = '2098-06-30'; ui.nodes.rf_status.value = 'received'; ui.nodes.rf_search.value = '310';
  ui.nodes.rf_filter.onclick(); await new Promise(resolve => setImmediate(resolve));
  ui.nodes.rf_entity.value = 'SAMAST'; await ui.openForm();
  ui.nodes.rf_add_source.onclick(); ui.nodes.rf_reason.value = record.reason; await ui.doPreview();
  // A later page selection must not move the already-reviewed refund's scope.
  ui.nodes.rf_entity.value = 'SANKI'; await ui.save();
  const loaded = new URL(ui.requests.at(-1).url, 'https://local.test').searchParams;
  assert.equal(loaded.get('nature'), 'SAMAST');
  assert.equal(loaded.get('from'), '2098-06-01'); assert.equal(loaded.get('to'), '2098-06-30');
  assert.equal(loaded.get('status'), 'received'); assert.equal(loaded.get('search'), '310');
  assert.equal(ui.state().filter.nature, 'SAMAST'); assert.equal(ui.nodes.rf_entity.value, 'SAMAST');
  assert.match(ui.nodes.panel.innerHTML, /RF-SAVED-SAMAST/);
  assert.equal(ui.nodes.rf_dialog.open, false);
  assert.match(ui.messages.at(-1).text, /Recorded RF-SAVED-SAMAST/);
  assert.equal(ui.requests.filter(request => request.options).length, 2, 'Only explicit preview and save POSTs are made');
});

test('failed save retains the original page entity and keeps the pinned dialog open', async () => {
  const ui = harness({ postResponse: url => url.endsWith('/preview') ? { success: true, preview: { amount: 731, components: [] }, impact: { billCredit: 731 } } : { success: false, error: 'Source changed; review again' } });
  await ui.start(); ui.nodes.rf_entity.value = 'SAMAST'; await ui.openForm();
  ui.nodes.rf_add_source.onclick(); ui.nodes.rf_reason.value = 'Return'; await ui.doPreview(); await ui.save();
  assert.equal(ui.state().filter.nature, 'SANKI'); assert.equal(ui.state().formNature, 'SAMAST');
  assert.equal(ui.nodes.rf_dialog.open, true); assert.match(ui.nodes.rf_message.textContent, /Source changed/);
  assert.equal(ui.requests.filter(request => !request.options).length, 2, 'No page reload after a rejected save');
  assert.equal(ui.messages.length, 0);
});

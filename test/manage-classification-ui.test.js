'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const classification = require('../public/manage-classification');

const html = fs.readFileSync(path.join(__dirname, '../public/manage.html'), 'utf8');
const script = fs.readFileSync(path.join(__dirname, '../public/manage-classification.js'), 'utf8');

test('Manage categories exposes separate setup and classification workspaces without replacing setup', () => {
  for (const id of ['categorySetupTab', 'classificationReviewTab', 'categorySetupPanel', 'classificationReviewPanel', 'newCategoryName', 'categoriesTable']) {
    assert.match(html, new RegExp(`id="${id}"`));
  }
  assert.match(html, /role="tablist" aria-label="Manage category workspace"/);
  assert.match(html, /id="categorySetupTab"[^>]+aria-selected="true"/);
  assert.match(html, /id="classificationReviewPanel"[^>]+hidden/);
});

test('classification workspace makes editable labels and protected financial evidence explicit', () => {
  for (const id of ['classificationEditEntity', 'classificationEditCategory', 'classificationEditVendor', 'classificationEditNarration']) assert.match(html, new RegExp(`id="${id}"`));
  assert.match(html, /Amount, date, paying account, payment method, proof and reconciliation evidence cannot be changed here/);
  assert.match(html, /Only entity and category can be changed in bulk/);
  assert.doesNotMatch(html, /id="classificationEditAmount"|id="classificationEditDate"|id="classificationEditPayment/);
});

test('classification adapters sort summaries by entity and category and encode optional filters safely', () => {
  const sorted = classification.sortCategories([
    { entity: 'SANKI', name: 'Travel' },
    { entity: 'PERSONAL', name: 'Food' },
    { entity: 'SANKI', name: 'Advertising' }
  ]);
  assert.deepEqual(sorted.map(row => `${row.entity}/${row.name}`), ['PERSONAL/Food', 'SANKI/Advertising', 'SANKI/Travel']);
  assert.equal(classification.queryString({ entities: ['SANKI', 'PERSONAL'], category: 'Food & Drinks', from: '2026-11-01', to: '2026-11-14', search: 'bed sheet', page: 2, limit: 50 }), '?entities=SANKI%2CPERSONAL&category=Food%20%26%20Drinks&from=2026-11-01&to=2026-11-14&search=bed%20sheet&page=2&limit=50');
  assert.equal(classification.rowKey({ sourceType: 'reconciliation_expense', recordId: 'EX-12' }), 'reconciliation_expense:EX-12');
});

test('classification field guards default safely and honor backend field restrictions', () => {
  assert.equal(classification.editable({ editableFields: ['category', 'vendor', 'narration'] }, 'entity'), false);
  assert.equal(classification.editable({ editableFields: ['category', 'vendor', 'narration'] }, 'category'), true);
  assert.equal(classification.editable({}, 'entity'), false);
  assert.equal(classification.editable({}, 'vendor'), true);
  assert.equal(classification.fieldLock({ fieldLocks: { entity: 'Linked source owns the entity.' } }, 'entity'), 'Linked source owns the entity.');
  assert.equal(classification.hasWriteToken({ revision: 0, fingerprint: 'current-row-hash' }), true);
  assert.equal(classification.hasWriteToken({ revision: 0, fingerprint: '' }), false);
});

test('writes use source-aware records and never send protected fields', () => {
  assert.match(script, /body\.sourceType = state\.editing\.sourceType/);
  assert.match(script, /records: Array\.from\(state\.selectedRows\.values\(\)\)/);
  assert.doesNotMatch(script, /expenseIds:/);
  assert.doesNotMatch(script, /changes:\s*\{[^}]*amount|changes:\s*\{[^}]*paymentMethod|changes:\s*\{[^}]*account/);
});

function controllerHarness() {
  const nodes = {};
  for (const match of html.matchAll(/<([a-z][a-z0-9]*)\b([^>]*\bid="([^"]+)"[^>]*)>/g)) {
    const [, tag, attrs, id] = match;
    const attributes = {};
    let markup = '';
    nodes[id] = {
      id, tagName: tag.toUpperCase(), hidden: /\bhidden\b/.test(attrs), value: '', textContent: '', className: '', disabled: false, checked: false, open: false, events: {},
      addEventListener(name, callback) { this.events[name] = callback; },
      querySelectorAll() { return []; },
      setAttribute(name, value) { attributes[name] = String(value); },
      getAttribute(name) { return attributes[name] == null ? null : attributes[name]; },
      focus() {}, showModal() { this.open = true; }, close() { this.open = false; }
    };
    Object.defineProperty(nodes[id], 'innerHTML', { get() { return markup; }, set(value) { markup = value; } });
  }
  const document = { getElementById(id) { assert.ok(nodes[id], `Element exists: ${id}`); return nodes[id]; } };
  const calls = [];
  const summary = {
    success: true,
    allowedEntities: ['SANKI', 'PERSONAL'],
    selectedEntities: ['SANKI', 'PERSONAL'],
    categoryChoices: { SANKI: [{ name: 'Props Expense', group: 'Marketing', type: 'variable' }], PERSONAL: [{ name: 'Food', group: 'Living', type: 'variable' }] },
    categories: [{ entity: 'SANKI', name: 'Props Expense', group: 'Marketing', type: 'variable', count: 1, total: 1200 }],
    expenses: [], pagination: { page: 1, pages: 1, total: 0 }
  };
  const row = { id: 'EX-9', recordId: 'EX-9', sourceType: 'expense', date: '2026-11-10', entity: 'SANKI', category: 'Props Expense', vendor: 'Studio Store', narration: 'Bed sheet', amount: 1200, account: 'Axis 3448', paymentMethod: 'UPI', reference: 'EX-00009', proofCount: 1, reconciled: true, editable: true, editableFields: ['entity', 'category', 'vendor', 'narration'], fieldLocks: {}, readOnlyReason: '', revision: 3, fingerprint: 'row-fingerprint' };
  async function request(url, options = {}) {
    const body = options.body ? JSON.parse(options.body) : null;
    calls.push({ url, method: options.method || 'GET', body });
    if (options.method === 'PATCH') return { success: true, expense: row, changes: ['vendor'] };
    if (url.endsWith('/bulk')) return { success: true, updated: [row], count: 1 };
    return summary;
  }
  const win = { location: { href: 'https://sanki.example/manage.html?section=categories&workspace=review' }, history: { replaceState(_a, _b, value) { this.value = value; } } };
  const app = classification.create(document, win, { approvalNatures: ['SANKI', 'PERSONAL'], canManagePersonalAccounting: true }, request, () => {});
  return { app, nodes, calls, row };
}

test('single correction sends only source-aware editable labels and reloads summaries', async () => {
  const h = controllerHarness();
  await h.app.init();
  assert.match(h.nodes.classificationTable.innerHTML, /Props Expense/);
  h.app.state.details = { expenses: [h.row], pagination: { page: 1, pages: 1, total: 1 } };
  h.app.openEditor('expense:EX-9');
  h.nodes.classificationEditVendor.value = 'Studio Props Store';
  h.nodes.classificationEditReason.value = 'Correct reporting classification';
  assert.equal(await h.app.saveEditor(), true);
  const write = h.calls.find(call => call.method === 'PATCH');
  assert.equal(write.url, '/api/expenses/classification-review/EX-9');
  assert.deepEqual(write.body, { vendor: 'Studio Props Store', reason: 'Correct reporting classification', sourceType: 'expense', expectedRevision: 3, fingerprint: 'row-fingerprint' });
  assert.equal(Object.hasOwn(write.body, 'amount'), false);
  assert.equal(h.calls.filter(call => call.method === 'GET').length, 2);
});

test('unclassified rows keep an explicit placeholder and vendor-only correction does not invent a category', async () => {
  const h = controllerHarness();
  await h.app.init();
  const unclassified = { ...h.row, category: '', vendor: 'Old vendor', narration: 'Existing narration' };
  h.app.state.details = { expenses: [unclassified], pagination: { page: 1, pages: 1, total: 1 } };
  h.app.openEditor('expense:EX-9');
  assert.equal(h.nodes.classificationEditCategory.value, '');
  assert.equal(h.nodes.classificationEditCategory.required, true);
  assert.match(h.nodes.classificationEditCategory.innerHTML, /<option value="" disabled selected>Select category<\/option>/);
  assert.doesNotMatch(h.nodes.classificationEditCategory.innerHTML, /value="Props Expense" selected/);

  h.nodes.classificationEditVendor.value = 'Corrected vendor';
  h.nodes.classificationEditReason.value = 'Correct vendor without guessing a category';
  assert.equal(await h.app.saveEditor(), true);
  const write = h.calls.find(call => call.method === 'PATCH');
  assert.equal(write.body.vendor, 'Corrected vendor');
  assert.equal(Object.hasOwn(write.body, 'category'), false);
});

test('changing entity requires an explicit destination category instead of selecting the first choice', async () => {
  const h = controllerHarness();
  await h.app.init();
  h.app.state.details = { expenses: [h.row], pagination: { page: 1, pages: 1, total: 1 } };
  h.app.openEditor('expense:EX-9');
  h.nodes.classificationEditEntity.value = 'PERSONAL';
  h.nodes.classificationEditEntity.onchange();
  assert.equal(h.nodes.classificationEditCategory.value, '');
  assert.match(h.nodes.classificationEditCategory.innerHTML, /Select category/);
  assert.doesNotMatch(h.nodes.classificationEditCategory.innerHTML, /value="Food" selected/);

  h.nodes.classificationEditReason.value = 'Move to correct entity';
  assert.equal(await h.app.saveEditor(), false);
  assert.match(h.nodes.classificationEditMessage.textContent, /Select a category for the destination entity/);
  assert.equal(h.calls.some(call => call.method === 'PATCH'), false);

  h.nodes.classificationEditCategory.value = 'Food';
  assert.equal(await h.app.saveEditor(), true);
  const write = h.calls.find(call => call.method === 'PATCH');
  assert.equal(write.body.entity, 'PERSONAL');
  assert.equal(write.body.category, 'Food');
});

test('opening Review after Category Setup always refreshes category choices', async () => {
  const h = controllerHarness();
  await h.app.init();
  assert.equal(h.calls.filter(call => call.method === 'GET').length, 1);
  await h.app.activate('setup');
  await h.app.activate('review');
  assert.equal(h.calls.filter(call => call.method === 'GET').length, 2);
});

test('bulk correction posts explicit record identities and entity/category only', async () => {
  const h = controllerHarness(); await h.app.init();
  h.app.state.selectedRows.set('expense:EX-9', h.row); h.app.openBulk();
  h.nodes.classificationBulkEntity.value = 'PERSONAL';
  h.nodes.classificationBulkCategory.value = 'Food';
  h.nodes.classificationBulkReason.value = 'Move private purchase to personal books';
  assert.equal(await h.app.saveBulk(), true);
  const write = h.calls.find(call => call.url.endsWith('/bulk'));
  assert.deepEqual(write.body, { records: [{ sourceType: 'expense', recordId: 'EX-9', expectedRevision: 3, fingerprint: 'row-fingerprint' }], changes: { entity: 'PERSONAL', category: 'Food' }, reason: 'Move private purchase to personal books' });
});

test('category details retain the last applied filters until filters are applied again', async () => {
  const h = controllerHarness(); await h.app.init();
  h.nodes.classificationSearch.value = 'not applied yet';
  await h.app.loadCategory(h.app.state.categories[0], 1);
  const detailCall = h.calls.at(-1);
  assert.match(detailCall.url, /category=Props%20Expense/);
  assert.doesNotMatch(detailCall.url, /not%20applied%20yet/);
});

test('protected entity is visibly disabled in row and bulk editors', async () => {
  const h = controllerHarness(); await h.app.init();
  const locked = { ...h.row, editableFields: ['category', 'vendor', 'narration'], fieldLocks: { entity: 'This reconciled source owns its entity.' }, readOnlyReason: 'Source-owned field.' };
  h.app.state.details = { expenses: [locked], pagination: { page: 1, pages: 1, total: 1 } };
  h.app.openEditor('expense:EX-9');
  assert.equal(h.nodes.classificationEditEntity.disabled, true);
  assert.match(h.nodes.classificationEntityHint.textContent, /This reconciled source owns its entity/);
  h.nodes.classificationCloseEdit.onclick?.();
  h.app.state.selectedRows.set('expense:EX-9', locked);
  h.app.openBulk();
  assert.equal(h.nodes.classificationBulkEntity.disabled, true);
  assert.match(h.nodes.classificationBulkScopeNote.textContent, /protected ownership/);
});


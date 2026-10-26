'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const ui = require('../public/manage');
const navigation = require('../public/manage-navigation');
const vm = require('node:vm');
const html = fs.readFileSync(path.join(__dirname, '../public/manage.html'), 'utf8');
const expenseHtml = fs.readFileSync(path.join(__dirname, '../public/expenses.html'), 'utf8');
const flush = () => new Promise(resolve => setImmediate(resolve));
function deferred() { let resolve, reject; const promise = new Promise((yes, no) => { resolve = yes; reject = no; }); return { promise, resolve, reject }; }
const row = (name, category = 'Flowers') => ({ name, categoryCount: 1, entryCount: 2, businesses: ['SANKI', 'SAMAST'], categories: [{ name: category, entryCount: 2 }] });
const initialGroups = () => ({ success: true, revision: 'revision-1', groups: [row('Travel'), row('Transport'), row('Final')] });
function harness({ cfg, modules, route, url = 'https://sanki.example/manage.html' } = {}) {
  const nodes = {}, calls = [], events = {};
  for (const match of html.matchAll(/<([a-z][a-z0-9]*)\b([^>]*\bid="([^"]+)"[^>]*)>/g)) {
    const [, tag, attrs, id] = match;
    const node = { id, tagName: tag.toUpperCase(), hidden: /\bhidden\b/.test(attrs), value: '', textContent: '', className: '', disabled: false, checked: false, indeterminate: false, open: false, events: {}, href: '',
      addEventListener(event, callback) { this.events[event] = callback; },
      querySelectorAll() { return Object.values(nodes).filter(n => ['BUTTON', 'INPUT', 'SELECT', 'TEXTAREA'].includes(n.tagName)); },
      focus() { this.focused = true; },
      showModal() { this.open = true; },
      close() { this.open = false; if (this.events.close) this.events.close(); }
    };
    let markup = '';
    Object.defineProperty(node, 'innerHTML', { get() { return markup; }, set(value) { markup = value; if (node.tagName === 'SELECT') { const first = markup.match(/<option value="([^"]*)"/); node.value = first ? first[1] : ''; } } });
    nodes[id] = node;
  }
  const config = Object.assign({ success: true, claimantOnly: false, canManageCategories: true, canManageVendors: true, canApprove: true, isAdmin: true, isOwner: false, categoryGroups: ['Travel', 'Transport', 'Final'], ledgers: [{ name: 'Existing', group: 'Travel', type: 'variable' }], types: ['fixed', 'variable', 'running', 'marketing'] }, cfg);
  const win = { location: { href: url, origin: new URL(url).origin, reload() { this.reloaded = true; } }, history: { replaceState(_state, _title, relative) { win.location.href = new URL(relative, win.location.origin).href; } }, crypto: { randomUUID: () => 'operation-' + (++win.id) }, id: 0, addEventListener(name, callback) { events[name] = callback; }, SankiManageNavigation: navigation };
  const doc = { getElementById(id) { assert.ok(nodes[id], 'Element exists: ' + id); return nodes[id]; } };
  const api = async (endpoint, options) => {
    const body = options.body && JSON.parse(options.body);
    calls.push({ endpoint, options, body });
    if (route) { const custom = route(endpoint, body, calls.length); if (custom !== undefined) return custom; }
    if (endpoint === '/api/auth/me') return { success: true, username: 'prashant', role: 'admin', roles: ['admin'] };
    if (endpoint === '/api/modules') return { success: true, modules: modules || [{ key: 'expenses', href: '/expenses.html', status: 'live' }] };
    if (endpoint === '/api/expenses/config') return config;
    if (endpoint === '/api/expenses/category-groups') return options.method ? { success: true, group: body.name } : initialGroups();
    if (endpoint.endsWith('/preview')) return Object.assign({ success: true, previewToken: 'signed-preview', categoryCount: 1, entryCount: 2, businesses: ['SANKI', 'SAMAST'], categories: [{ name: 'Flowers' }], warnings: [] }, body);
    if (endpoint.endsWith('/apply')) return { success: true, group: body.target, revision: 'revision-2', groups: [row(body.target)], affected: { categoryCount: 1, entryCount: 2 } };
    if (endpoint === '/api/expenses/custom-ledgers') return { success: true, ledger: body };
    throw Error('Unexpected endpoint: ' + endpoint);
  };
  const app = ui.createApp(doc, win, api);
  return { app, nodes, calls, config, win, events, async init(section) { await app.init(); if (section) await app.showTool(section); } };
}

test('transaction workspace opens directly only with exact Manage capability, without loading or changing transactions', async () => {
  const h=harness({cfg:{canEditLedgerTransactions:true},url:'https://sanki.example/manage.html?section=transactions'});let loaded=0;
  h.win.SankiManageTransactions={create(_doc,_win,config,_request,onBusy){assert.equal(config.canEditLedgerTransactions,true);return{load(){loaded++;return Promise.resolve(true);}};}};
  await h.init();assert.equal(loaded,1);assert.equal(h.nodes.transactionWorkspace.hidden,false);assert.equal(h.nodes.groupWorkspace.hidden,true);assert.equal(h.nodes.toolTitle.textContent,'Manage transactions');
  assert.equal(h.calls.some(call=>call.endpoint.includes('/transactions')||call.options.method),false);
  for(const cfg of [{canEditLedgerTransactions:false},{canEditLedgerTransactions:true,isAdmin:false},{canEditLedgerTransactions:true,claimantOnly:true}]){
    const denied=harness({cfg});await denied.init();assert.equal(await denied.app.showTool('transactions'),false);assert.equal(denied.nodes.transactionWorkspace.hidden,true);
  }
});

test('Manage is one shared hub; existing financial forms remain untouched and category shortcut uses hub', () => {
  assert.match(expenseHtml, /data-t="manage" id="manageHubTab"/);
  assert.match(expenseHtml, /if\(tab==='manage'\) return openManageHub\(\)/);
  assert.match(expenseHtml, /if\(tab==='categories'.*openManageHub\('categories'\)/);
  assert.match(expenseHtml, /onclick="openManageCategories\(\)"/);
  assert.match(expenseHtml, /window\.openManageCategories=function\(\)\{if\(cfg\.canManageCategories\)/);
  assert.match(html, /script src="\/sidebar\.js"/);
  assert.equal(new Set([...html.matchAll(/\bid="([^"]+)"/g)].map(m => m[1])).size, [...html.matchAll(/\bid="([^"]+)"/g)].length);
});

test('output escapes labels and return navigation cannot leave this app', () => {
  assert.equal(ui.esc('<img src=x onerror="x">&\''), '&lt;img src=x onerror=&quot;x&quot;&gt;&amp;&#39;');
  for (const value of ['https://evil.example', '//evil.example', '/\\evil.example', '/\nevil.example', 'javascript:alert(1)', '/manage.html?section=groups']) assert.equal(ui.safeReturn(value, 'https://sanki.example'), '');
  assert.equal(ui.safeReturn('/expenses.html?tab=spending#x', 'https://sanki.example'), '/expenses.html?tab=spending#x');
});

test('ledger Manage shortcuts retain the previous view and legacy Manage URLs cannot create a return loop', () => {
  const match = expenseHtml.match(/    function openManageHub\(section\)\{[^\n]+/);
  assert.ok(match);
  for (const tab of ['spending', 'cash', 'list', 'manage', 'categories']) {
    const original = new URL('https://sanki.example/expenses.html?tab=' + tab + '&account=Example#detail');
    const location = { href: original.href };
    const context = { URL, location, cfg: { claimantOnly: false } };
    vm.runInNewContext(match[0], context); context.openManageHub('groups');
    const destination = new URL(location.href, original.origin), back = new URL(destination.searchParams.get('returnTo'), original.origin);
    assert.equal(destination.pathname, '/manage.html'); assert.equal(destination.searchParams.get('section'), 'groups');
    assert.equal(back.searchParams.get('tab'), ['manage', 'categories'].includes(tab) ? 'list' : tab);
    assert.equal(back.searchParams.get('account'), 'Example'); assert.equal(back.hash, '#detail');
  }
  const location = { href: 'https://sanki.example/expenses.html?tab=list' };
  const claimant = { URL, location, cfg: { claimantOnly: true } }; vm.runInNewContext(match[0], claimant); claimant.openManageHub('groups');
  assert.equal(location.href, 'https://sanki.example/expenses.html?tab=list');
});

test('hub defaults every major heading collapsed and direct section opens only an authorized tool', async () => {
  const h = harness(); await h.init();
  assert.ok(h.nodes.manageHeads.innerHTML.includes('aria-expanded="false"'));
  assert.equal(h.nodes.manageHeads.innerHTML.includes('aria-expanded="true"'), false);
  assert.equal(h.nodes.manageHome.hidden, false);
  assert.equal(h.nodes.manageTool.hidden, true);
  assert.equal(await h.app.showTool('groups'), true);
  assert.equal(h.nodes.groupWorkspace.hidden, false);
  assert.equal(h.nodes.categoryWorkspace.hidden, true);
  assert.match(h.win.location.href, /section=groups/);
});

test('claimants, missing capabilities and absent module permissions cannot call management APIs', async () => {
  for (const options of [{ cfg: { claimantOnly: true } }, { cfg: { canManageCategories: false } }, { modules: [] }]) {
    const h = harness(options); await h.init();
    assert.equal(await h.app.showTool('groups'), false);
    h.nodes.newGroupName.value = 'Unauthorized';
    assert.equal(await h.app.addGroup(), false);
    assert.equal(await h.app.previewChange(), false);
    assert.equal(await h.app.applyChange(), false);
    assert.equal(h.calls.some(call => call.endpoint.includes('category-groups')), false);
  }
});

test('failed expense config hides maintenance instead of manufacturing admin capabilities', async () => {
  const h = harness({ route(endpoint) { if (endpoint === '/api/expenses/config') return Promise.reject(Error('Config unavailable')); } });
  await h.init();
  assert.equal(h.app.state.catalog.length, 0);
  assert.match(h.nodes.pageMessage.textContent, /temporarily unavailable/);
  assert.equal(await h.app.showTool('groups'), false);
});

test('group table shows usage once per group and escapes all data', async () => {
  const h = harness({ route(endpoint, body) { if (endpoint === '/api/expenses/category-groups' && !body) return { success: true, revision: 'r', groups: [row('<svg onload=x>', '<img src=x>')] }; } });
  await h.init('groups');
  assert.match(h.nodes.groupsTable.innerHTML, /&lt;svg onload=x&gt;/);
  assert.match(h.nodes.groupsTable.innerHTML, /&lt;img src=x&gt;/);
  assert.match(h.nodes.groupsTable.innerHTML, /SANKI, SAMAST/);
  assert.equal(h.nodes.groupsTable.innerHTML.includes('<svg'), false);
});

test('rename requires one valid source and a printable name and reason before sending a preview', async () => {
  const h = harness(); await h.init('groups');
  h.app.beginEdit('rename', ['Travel', 'Transport']); assert.equal(h.app.state.editAction, '');
  h.app.beginEdit('rename', ['Travel']);
  h.nodes.finalGroupName.value = 'Travel & Logistics';
  assert.equal(await h.app.previewChange(), false);
  h.nodes.groupChangeReason.value = 'Clean up names'; h.nodes.finalGroupName.value = 'Bad\nName';
  assert.equal(await h.app.previewChange(), false);
  h.nodes.finalGroupName.value = 'Travel & Logistics';
  assert.equal(await h.app.previewChange(), true);
  assert.equal(h.nodes.groupReviewDialog.open, true);
  const preview = h.calls.find(call => call.endpoint.endsWith('/preview'));
  assert.deepEqual(preview.body, { action: 'rename', sources: ['Travel'], target: 'Travel & Logistics', reason: 'Clean up names', revision: 'revision-1' });
  assert.match(h.nodes.groupPreviewSummary.innerHTML, /SAMAST/);
  assert.equal(h.calls.some(call => call.endpoint.endsWith('/apply')), false);
});

test('merge requires an existing destination outside selected sources and preserves source uniqueness', async () => {
  const h = harness(); await h.init('groups'); h.app.beginEdit('merge', ['Travel', 'Travel', 'Transport']);
  h.nodes.groupChangeReason.value = 'Consolidate duplicate heads';
  h.nodes.finalGroupName.value = 'New non-existent target'; assert.equal(await h.app.previewChange(), false);
  h.nodes.finalGroupName.value = 'Travel'; assert.equal(await h.app.previewChange(), false);
  h.nodes.finalGroupName.value = 'final'; assert.equal(await h.app.previewChange(), true);
  assert.deepEqual(h.app.state.preview.sources, ['Travel', 'Transport']); assert.equal(h.app.state.preview.target, 'Final');
});

test('busy preview blocks duplicate submissions and modal requires explicit confirmation', async () => {
  const pending = deferred(); const h = harness({ route(endpoint) { if (endpoint.endsWith('/preview')) return pending.promise; } }); await h.init('groups');
  h.app.beginEdit('rename', ['Travel']); h.nodes.finalGroupName.value = 'Trip'; h.nodes.groupChangeReason.value = 'Correct label';
  const first = h.app.previewChange(); assert.equal(h.app.state.busy, true); assert.equal(await h.app.previewChange(), false); assert.equal(await h.app.applyChange(), false);
  pending.resolve({ success: true, action: 'rename', sources: ['Travel'], target: 'Trip', revision: 'revision-1', previewToken: 'token', categories: [] });
  assert.equal(await first, true); assert.equal(h.app.state.busy, false);
  assert.equal(h.calls.filter(call => call.endpoint.endsWith('/preview')).length, 1);
  assert.equal(h.calls.some(call => call.endpoint.endsWith('/apply')), false);
});

test('loading groups blocks editing and financial mutations until the current catalog is ready', async () => {
  const pending = deferred(); const h = harness({ route(endpoint, body) { if (endpoint === '/api/expenses/category-groups' && !body) return pending.promise; } }); await h.init();
  const loading = h.app.showTool('groups'); assert.equal(h.app.state.busy, true);
  h.app.beginEdit('rename', ['Travel']); assert.equal(h.app.state.editAction, '');
  h.nodes.newGroupName.value = 'While loading'; assert.equal(await h.app.addGroup(), false);
  pending.resolve(initialGroups()); assert.equal(await loading, true); assert.equal(h.app.state.busy, false); assert.equal(h.app.state.revision, 'revision-1');
});

test('confirmation uses signed preview and stable idempotency key after connection failure', async () => {
  let attempt = 0;
  const h = harness({ route(endpoint, body) { if (endpoint.endsWith('/apply') && ++attempt === 1) return Promise.reject(Error('Connection lost')); } }); await h.init('groups');
  h.app.beginEdit('rename', ['Travel']); h.nodes.finalGroupName.value = 'Trip'; h.nodes.groupChangeReason.value = 'Correct label'; await h.app.previewChange();
  const id = h.app.state.operationId;
  assert.equal(await h.app.applyChange(), false); assert.equal(h.app.state.operationId, id); assert.equal(h.nodes.groupReviewDialog.open, true);
  assert.equal(await h.app.applyChange(), true);
  const writes = h.calls.filter(call => call.endpoint.endsWith('/apply'));
  assert.equal(writes.length, 2); assert.equal(writes[0].body.operationId, writes[1].body.operationId); assert.equal(writes[0].body.previewToken, 'signed-preview');
  assert.equal(h.nodes.groupReviewDialog.open, false); assert.equal(h.app.state.revision, 'revision-2'); assert.match(h.nodes.groupMessage.textContent, /No financial transactions/);
});

test('stale confirmation is invalidated and cannot be resubmitted until a fresh review', async () => {
  const h = harness({ route(endpoint) { if (endpoint.endsWith('/apply')) { const error = Error('Accounting records changed'); error.status = 409; error.code = 'STALE_PREVIEW'; return Promise.reject(error); } } }); await h.init('groups');
  h.app.beginEdit('rename', ['Travel']); h.nodes.finalGroupName.value = 'Trip'; h.nodes.groupChangeReason.value = 'Correct label'; await h.app.previewChange();
  assert.equal(await h.app.applyChange(), false); assert.equal(h.app.state.preview, null); assert.equal(h.nodes.confirmGroupChange.disabled, true); assert.equal(await h.app.applyChange(), false);
  assert.match(h.nodes.groupReviewMessage.textContent, /refresh groups/i); assert.equal(h.calls.filter(call => call.endpoint.endsWith('/apply')).length, 1);
});

test('pending apply blocks Escape, closing, changing tool, and second write', async () => {
  const pending = deferred(); const h = harness({ route(endpoint) { if (endpoint.endsWith('/apply')) return pending.promise; } }); await h.init('groups');
  h.app.beginEdit('rename', ['Travel']); h.nodes.finalGroupName.value = 'Trip'; h.nodes.groupChangeReason.value = 'Correct label'; await h.app.previewChange();
  const applying = h.app.applyChange(); let prevented = false; h.nodes.groupReviewDialog.events.cancel({ preventDefault() { prevented = true; } }); assert.equal(prevented, true);
  h.app.closeReview(); assert.equal(h.nodes.groupReviewDialog.open, true); assert.equal(await h.app.showTool('categories'), false); assert.equal(await h.app.applyChange(), false);
  pending.resolve({ success: true, revision: 'r2', group: 'Trip', groups: [row('Trip')] }); assert.equal(await applying, true);
});

test('adding a canonical duplicate reports reuse, and invalid inputs never post', async () => {
  const h = harness({ route(endpoint, body) { if (endpoint === '/api/expenses/category-groups' && body) return { success: true, already: true, group: 'Travel' }; } }); await h.init('groups');
  for (const value of ['', 'bad\nlabel', 'a'.repeat(121)]) { h.nodes.newGroupName.value = value; assert.equal(await h.app.addGroup(), false); }
  h.nodes.newGroupName.value = ' travel '; assert.equal(await h.app.addGroup(), true); assert.match(h.nodes.groupMessage.textContent, /rather than creating a duplicate/);
  assert.equal(h.calls.filter(call => call.endpoint === '/api/expenses/category-groups' && call.body).length, 1);
});

test('new category uses existing endpoint only; group changes never call payment or reconciliation APIs', async () => {
  const h = harness(); await h.init('categories'); h.nodes.newCategoryName.value = 'New category'; h.nodes.newCategoryGroup.value = 'Travel'; h.nodes.newCategoryType.value = 'variable';
  assert.equal(await h.app.addCategory(), true); assert.equal(h.config.ledgers.some(category => category.name === 'New category'), true);
  const write = h.calls.find(call => call.endpoint === '/api/expenses/custom-ledgers'); assert.deepEqual(write.body, { name: 'New category', group: 'Travel', type: 'variable' });
  assert.equal(h.calls.some(call => /payment|reconcil|batch-pay|transfer/.test(call.endpoint)), false);
  h.nodes.refreshCategories.onclick(); assert.equal(h.win.location.reloaded, true);
});

test('failed catalog load invalidates previous revision and prevents stale edits', async () => {
  let fail = false; const h = harness({ route(endpoint, body) { if (fail && endpoint === '/api/expenses/category-groups' && !body) return Promise.reject(Error('Catalog down')); } }); await h.init('groups'); fail = true;
  assert.equal(await h.app.loadGroups(), false); assert.equal(h.app.state.revision, ''); h.app.beginEdit('rename', ['Travel']); assert.equal(h.app.state.editAction, ''); assert.match(h.nodes.groupMessage.textContent, /Refresh/);
});

test('return context is retained and back to hub resets collapsed navigation without replaying any action', async () => {
  const h = harness({ url: 'https://sanki.example/manage.html?returnTo=%2Fexpenses.html%3Ftab%3Dspending&section=groups' }); await h.init();
  assert.equal(h.nodes.returnLink.href, '/expenses.html?tab=spending'); assert.equal(h.nodes.returnLink.hidden, false);
  h.app.showHome(); assert.equal(h.nodes.manageHome.hidden, false); assert.equal(h.nodes.manageTool.hidden, true); assert.equal(new URL(h.win.location.href).searchParams.has('section'), false); assert.equal(h.calls.some(call => call.options.method === 'POST'), false);
});

test('malformed server preview fails closed and does not open an actionable confirmation', async () => {
  const h = harness({ route(endpoint) { if (endpoint.endsWith('/preview')) return { success: true, target: 'Trip' }; } }); await h.init('groups'); h.app.beginEdit('rename', ['Travel']); h.nodes.finalGroupName.value = 'Trip'; h.nodes.groupChangeReason.value = 'Correct label';
  assert.equal(await h.app.previewChange(), false); assert.equal(h.nodes.groupReviewDialog.open, false); assert.equal(h.app.state.preview, null); assert.equal(h.calls.some(call => call.endpoint.endsWith('/apply')), false);
});

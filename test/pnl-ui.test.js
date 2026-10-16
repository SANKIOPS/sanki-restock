'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { buildReport } = require('../modules/pnl-report');
const source = fs.readFileSync(path.join(__dirname, '..', 'public', 'pnl.js'), 'utf8');
test('P&L collapse release versions script and styles to avoid stale cached assets', () => {
  const html=fs.readFileSync(path.join(__dirname,'..','public','pnl.html'),'utf8');
  assert.match(html, /href="\/pnl\.css\?v=20261008-collapsed-heads"/);
  const hash=require('node:crypto').createHash('sha256').update(source).digest('hex').slice(0,12);
  assert.ok(html.includes('/pnl.js?v='+hash),'current report code must be content-versioned');
});
function sample(missing = false) {
  return buildReport({ orders: { orders: { O: { id: 'O', name: '<unsafe invoice>', channel: 'POS', financialStatus: 'paid', taxesIncluded: true, total: 2999, processedAt: '2026-10-01', lineItems: [{ id: 'L', sku: 'SKU', qty: 1, price: 2999 }] } } }, opening: missing ? {} : { lots: [{ sku: 'SKU', qty: 10, unitCost: 999, verified: true }] } }, { from: '2026-10-01', to: '2026-10-08' });
}
function harness(fetcher, url='https://example.test/pnl.html') {
  const elements = new Map(), listeners = {}, groupRows = new Map(), tabs = ['statement', 'tax', 'collections', 'trends', 'policy'].map(id => ({ dataset: { tab: id }, listeners: {}, setAttribute() {}, addEventListener(name, fn) { this.listeners[name] = fn; } }));
  function element(id) {
    if (!elements.has(id)) elements.set(id, { id, value: '', innerHTML: '', textContent: '', hidden: false, disabled: false, open: false, listeners: {}, addEventListener(name, fn) { this.listeners[name] = fn; }, showModal() { this.open = true; }, close() { this.open = false; } });
    return elements.get(id);
  }
  element('channel').value = 'All';
  const document = { getElementById: element, body: { classList: { add() {}, remove() {} } }, querySelectorAll(selector) { const group=selector.match(/^\[data-expense-group="(\w+)"\]$/)?.[1]; return group ? groupRows.get(group)||[] : selector === '[data-tab]' ? tabs : selector === '.view' ? tabs.map(t => element(t.dataset.tab)) : []; }, addEventListener(name, fn) { listeners[name] = fn; } };
  const window={print(){},location:new URL(url),history:{state:null,replaceState(_state,_title,target){window.location=new URL(target,window.location);}}};
  const context=vm.createContext({document,fetch:fetcher,AbortController,URL,URLSearchParams,Date,location:window.location,window});
  vm.runInContext(fs.readFileSync(path.join(__dirname,'../public/navigation-state.js'),'utf8'),context);context.SankiNavigation=window.SankiNavigation;vm.runInContext(source,context);
  return { element, tabs, groupRows, url:()=>window.location.href, clickHead(head) { listeners.click({ target: { closest: selector => selector==='[data-group-toggle]' ? head : null } }); }, clickFilter(filter) { listeners.click({ target: { closest: selector => selector==='[data-filter]' ? { dataset: { filter } } : null } }); }, refresh() { element('filters').listeners.submit({ preventDefault() {} }); } };
}
const settle = () => new Promise(resolve => setImmediate(resolve));

test('P&L page refresh keeps selected tab, date range and channel without replaying mutations',async()=>{
  const calls=[],fetcher=async(url,options)=>{calls.push({url,options});return{ok:true,status:200,json:async()=>sample()};};
  const first=harness(fetcher,'https://example.test/pnl.html?from=2026-09-01&to=2026-09-15&channel=POS');await settle();first.tabs[2].listeners.click();
  const refreshed=harness(fetcher,first.url());await settle();
  assert.equal(refreshed.element('collections').hidden,false);assert.equal(refreshed.element('statement').hidden,true);assert.equal(refreshed.element('from').value,'2026-09-01');assert.equal(refreshed.element('to').value,'2026-09-15');assert.equal(refreshed.element('channel').value,'POS');
  assert.ok(calls.every(call=>!call.options?.method||call.options.method==='GET'));
});
test('P&L UI renders typed totals, escaped references, tabs and SKU drill-down', async () => {
  const ui = harness(async () => ({ ok: true, status: 200, json: async () => sample() })); await settle();
  assert.match(ui.element('cards').innerHTML, /1,542\.53/); assert.equal(ui.element('export').disabled, false);
  ui.tabs[1].listeners.click(); assert.equal(ui.element('statement').hidden, true); assert.equal(ui.element('tax').hidden, false);
  assert.match(ui.element('tax').innerHTML, /&lt;unsafe invoice&gt;/); assert.doesNotMatch(ui.element('tax').innerHTML, /<unsafe invoice>/);
  ui.clickFilter('sales'); assert.equal(ui.element('details').open, true); assert.match(ui.element('detailBody').innerHTML, /SKU \/ purchase allocations/);
});
test('incomplete results display unavailable instead of fabricated profit', async () => {
  const ui = harness(async () => ({ ok: true, status: 200, json: async () => sample(true) })); await settle();
  assert.match(ui.element('cards').innerHTML, /Unavailable/); assert.equal(ui.element('warnings').hidden, false);
  assert.match(ui.element('warnings').innerHTML, /Verified stock\/cost missing/);
});
test('a failed refresh clears stale results and disables exports', async () => {
  let fail = false;
  const ui = harness(async () => fail ? { ok: false, status: 500, json: async () => ({ error: 'Test failure' }) } : { ok: true, status: 200, json: async () => sample() }); await settle();
  fail = true; ui.refresh(); await settle();
  assert.equal(ui.element('cards').innerHTML, ''); assert.equal(ui.element('statement').innerHTML, ''); assert.equal(ui.element('export').disabled, true); assert.equal(ui.element('status').textContent, 'Test failure');
});
test('an older response cannot replace a newer selected report', async () => {
  const pending = [];
  const ui = harness(() => new Promise(resolve => pending.push(resolve)));
  ui.refresh();
  pending[1]({ ok: true, status: 200, json: async () => sample(true) }); await settle();
  pending[0]({ ok: true, status: 200, json: async () => sample() }); await settle();
  assert.match(ui.element('cards').innerHTML, /Unavailable/); assert.equal(ui.element('warnings').hidden, false);
});

test('shared Till date control uses accounting start rather than silently defaulting to this month', async () => {
  let requested;
  const ui = harness(async url => { requested = url; return { ok: true, status: 200, json: async () => sample() }; }); await settle();
  ui.element('from').disabled = true; ui.element('from').value = ''; ui.refresh(); await settle();
  assert.equal(new URL(requested, 'http://localhost').searchParams.get('from'), '2026-08-22');
});

test('major expense heads start collapsed and toggle only their own category rows', async () => {
  const result=sample();
  result.categories=[{id:'fixed/Rent',group:'fixed',category:'Rent',count:1,amount:100},{id:'running/Utilities',group:'running',category:'Utilities',count:1,amount:50}];
  const ui=harness(async()=>({ok:true,status:200,json:async()=>result})); await settle();
  const html=ui.element('statement').innerHTML;
  assert.equal((html.match(/data-group-toggle=/g)||[]).length,4);
  assert.equal((html.match(/aria-expanded="false"/g)||[]).length,4);
  assert.match(html, /aria-controls="expense-category-fixed-0"/);
  assert.match(html, /id="expense-category-fixed-0" data-expense-group="fixed" hidden/);
  assert.match(html, /id="expense-category-running-0" data-expense-group="running" hidden/);
  assert.match(html, /data-group-toggle="marketing" aria-expanded="false" disabled/);
  const fixed=[{hidden:true}],running=[{hidden:true}];
  ui.groupRows.set('fixed',fixed);ui.groupRows.set('running',running);
  const head={dataset:{groupToggle:'fixed'},expanded:'false',getAttribute(){return this.expanded;},setAttribute(name,value){assert.equal(name,'aria-expanded');this.expanded=value;}};
  ui.clickHead(head);assert.equal(head.expanded,'true');assert.equal(fixed[0].hidden,false);assert.equal(running[0].hidden,true);assert.equal(ui.element('details').open,false);
  ui.clickHead(head);assert.equal(head.expanded,'false');assert.equal(fixed[0].hidden,true);
  ui.clickFilter('category:fixed/Rent');assert.equal(ui.element('details').open,true);
  ui.refresh();await settle();assert.equal((ui.element('statement').innerHTML.match(/aria-expanded="false"/g)||[]).length,4);
});

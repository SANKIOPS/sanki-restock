'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { buildReport } = require('../modules/pnl-report');
const { loadSources } = require('../modules/pnl-routes');
const range = { from: '2026-10-01', to: '2026-10-08' };
function source() {
  return {
    orders: { orders: { sale: { id: 'sale', name: '#example', channel: 'POS', financialStatus: 'paid', processedAt: '2026-10-01', total: 2000, taxesIncluded: true, shipping: 0, lineItems: [{ id: 'line', sku: 'TEST-SKU', qty: 2, price: 1000 }] } } },
    shopifyCosts: { at: '2026-10-08T00:00:00Z', items: [{ id: 'inventory-item', sku: ' test-sku ', unitCost: 125.25, levels: [{ available: 0, on_hand: 0 }] }] },
    opening: { lots: [{ sku: 'TEST-SKU', qty: 1, unitCost: 900, verified: true }] }, purchases: {}, expenses: {}, salary: {}, feeds: []
  };
}
test('Shopify SKU cost overrides FIFO and does not depend on stock quantity or supplier payment', () => {
  const input=source(), before=JSON.stringify(input), report=buildReport(input,range);
  assert.equal(report.totals.cogs,250.5);
  assert.equal(report.totals.complete,true);
  assert.equal(report.transactions[0].details[0].allocations[0].purchaseId,'Shopify Cost per item');
  assert.equal(report.transactions[0].details[0].allocations[0].costAsOf,input.shopifyCosts.at);
  assert.equal(report.costSource.at,input.shopifyCosts.at);
  assert.match(report.basis,/synced Shopify/);
  assert.match(report.policy.cogs,/restate management P&L history/);
  assert.equal(JSON.stringify(input),before);
});
test('blank, zero, negative, nonnumeric and unmapped Shopify costs remain exceptions', () => {
  for(const cost of [null,'',0,-1,'unknown']) {
    const input=source();input.shopifyCosts.items[0].unitCost=cost;
    const report=buildReport(input,range);
    assert.equal(report.totals.cogs,null);
    assert.ok(report.warnings.some(w=>w.scope==='cogs'&&/Cost per item missing/.test(w.message)));
  }
  const input=source();input.shopifyCosts.items=[];
  assert.equal(buildReport(input,range).totals.cogs,null);
});
test('duplicate SKU inventory identities and non-INR costs cannot silently price a sale', () => {
  const input=source();input.shopifyCosts.items.push({...input.shopifyCosts.items[0],id:'different-inventory-item'});
  assert.equal(buildReport(input,range).totals.cogs,null);
  assert.match(buildReport(input,range).warnings[0].message,/Duplicate Shopify SKU/);
  input.shopifyCosts.items.pop();input.shopifyCosts.items[0].unitCostCurrency='USD';
  assert.equal(buildReport(input,range).totals.cogs,null);
});
test('repeated pages for the same inventory item are deduplicated without increasing cost', () => {
  const input=source();input.shopifyCosts.items.push({...input.shopifyCosts.items[0]});
  assert.equal(buildReport(input,range).totals.cogs,250.5);
});
test('returned SKU quantities reverse the sale allocation and cannot reverse it twice', () => {
  const input=source();
  input.orders.orders.sale.refunds=[{id:'credit-1',date:'2026-10-03',amount:1000,lineItems:[{lineItemId:'line',qty:1,subtotal:952.38,tax:47.62,restockType:'return'}]}];
  assert.equal(buildReport(input,range).totals.cogs,125.25);
  input.orders.orders.sale.refunds.push({id:'credit-2',date:'2026-10-04',amount:1000,lineItems:[{lineItemId:'line',qty:1,subtotal:952.38,tax:47.62,restockType:'return'}]});
  assert.equal(buildReport(input,range).totals.cogs,0);
  input.orders.orders.sale.refunds.push({id:'duplicate-credit',date:'2026-10-05',amount:1000,lineItems:[{lineItemId:'line',qty:1,subtotal:952.38,tax:47.62,restockType:'return'}]});
  const report=buildReport(input,range);
  assert.ok(report.warnings.some(w=>w.scope==='cogs'));
  assert.equal(report.totals.cogs,null);
});
test('P&L loads Shopify costs from the persisted snapshot without modifying the source', () => {
  const dir=fs.mkdtempSync(path.join(os.tmpdir(),'pnl-cost-read-'));
  try {
    const file=path.join(dir,'inventory-snapshot.json'),snapshot=source().shopifyCosts;
    fs.writeFileSync(file,JSON.stringify(snapshot));
    const before=fs.readFileSync(file,'utf8'),names=fs.readdirSync(dir);
    const sources=loadSources(dir,{});
    assert.deepEqual(sources.shopifyCosts,snapshot);
    assert.equal(sources.feeds.find(f=>f.name==='Shopify Cost per item').status,'ready');
    assert.equal(fs.readFileSync(file,'utf8'),before);
    assert.deepEqual(fs.readdirSync(dir),names);
  } finally {
    assert.equal(path.dirname(path.resolve(dir)),path.resolve(os.tmpdir()));
    assert.ok(path.basename(dir).startsWith('pnl-cost-read-'));
    fs.rmSync(dir,{recursive:true,force:true});
  }
});
test('cancelled and pre-accounting-start deliveries do not enter P&L', () => {
  const input=source(),sale=input.orders.orders.sale;
  sale.channel='Website';sale.financialStatus='paid';sale.deliveryComplete=true;
  input.orders.dispatch={sale:{packingStatus:'cancelled'}};
  assert.equal(buildReport(input,range).totals.salesCount,0);
  assert.equal(buildReport(input,range).warnings.filter(w=>w.scope==='sales').length,0);
  input.orders.dispatch.sale={packingStatus:'delivered',deliveredAt:'2026-04-21'};
  assert.equal(buildReport(input,range).totals.salesCount,0);
  assert.equal(buildReport(input,range).warnings.filter(w=>w.scope==='sales').length,0);
});

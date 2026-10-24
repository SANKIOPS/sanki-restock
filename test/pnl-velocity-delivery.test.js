'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { carrierDay, velocityCompletion } = require('../modules/pnl-delivery');
const { buildReport, completion } = require('../modules/pnl-report');
const { loadSources } = require('../modules/pnl-routes');
const { fetchTracking } = require('../modules/velocity-tracking');
const order = () => ({id:'order',name:'#example',createdAt:'2026-08-30T10:00:00Z',channel:'Website',financialStatus:'paid',deliveryComplete:true,taxesIncluded:true,total:1050,shipping:0,lineItems:[{id:'line',sku:'EXAMPLE',qty:1,price:1050}]});
const evidence = () => ({index:{lastSync:'2026-09-08T00:00:00Z',byOrder:{order:[{awb:'AWB-A',status:'delivered'}]},shipments:{'AWB-A':{status:'delivered',deliveredDate:'2026-09-05 14:35:00',lastUpdated:'2026-09-08T00:00:00Z'}}}});
test('Velocity joins an order with no Shopify AWB through exact external order ID',()=>{
 const o=order(),v=evidence(),before=JSON.stringify({o,v}),found=velocityCompletion(o,{},v,'2026-10-01');
 assert.equal(found.date,'2026-09-05');assert.deepEqual(found.awbs,['AWB-A']);assert.match(found.evidence,/Velocity/);assert.equal(JSON.stringify({o,v}),before);
});
test('P&L uses actual Velocity delivery date and records provenance without writing order fields',()=>{
 const o=order(),sources={orders:{orders:{order:o}},velocity:evidence(),shopifyCosts:{items:[{id:'item',sku:'EXAMPLE',unitCost:100}]}};
 const before=JSON.stringify(sources),report=buildReport(sources,{from:'2026-09-01',to:'2026-09-30'});
 assert.equal(report.totals.salesCount,1);assert.equal(report.transactions[0].date,'2026-09-05');assert.match(report.transactions[0].completionEvidence,/Velocity actual/);assert.deepEqual(report.transactions[0].deliveryAwbs,['AWB-A']);assert.equal(JSON.stringify(sources),before);
 assert.equal(buildReport(sources,{from:'2026-10-01',to:'2026-10-10'}).totals.salesCount,0);
});
test('split shipments wait for every AWB and then use latest actual delivery',()=>{
 const o=order(),v=evidence();o.shipments=[{awb:'AWB-B'}];v.index.shipments['AWB-B']={status:'in_transit',lastUpdated:'2026-09-08'};
 assert.equal(velocityCompletion(o,{},v,'2026-10-01').date,'');
 v.index.shipments['AWB-B']={status:'Delivered',deliveredDate:'2026-09-07'};
 assert.equal(velocityCompletion(o,{},v,'2026-10-01').date,'2026-09-07');
});
test('fulfilled/no AWB, edit timestamps, remittance and cancelled shipments cannot prove delivery',()=>{
 const o=order();assert.equal(velocityCompletion(o,{},{}),null);
 const v=evidence();v.index.shipments['AWB-A']={status:'delivered',lastUpdated:'2026-09-08'};
 assert.equal(velocityCompletion(o,{},v,'2026-10-01').date,'');
 v.index.byOrder.order=[{awb:'AWB-A',status:'cancelled'}];assert.equal(velocityCompletion(o,{},v),null);
});
test('cancelled/RTO orders remain excluded and manual dates/POS retain existing precedence',()=>{
 const o=order(),v=evidence();o.cancelledAt='2026-08-31';assert.equal(completion(o,{},v),null);delete o.cancelledAt;
 assert.equal(completion(o,{packingStatus:'rto'},v),null);
 assert.equal(completion(o,{deliveredAt:'2026-09-06'},v).date,'2026-09-06');
 o.channel='POS';o.processedAt='2026-08-30';assert.equal(completion(o,{},v).date,'2026-08-30');
});
test('invalid, pre-order and future carrier dates are never accepted',()=>{
 for(const date of ['2026-02-30','2026-04-21','2099-01-01','05/09/26','']){
  const v=evidence();v.index.shipments['AWB-A'].deliveredDate=date;
  assert.equal(velocityCompletion(order(),{},v,'2026-10-01').date,'');
 }
 assert.equal(carrierDay('2026-09-05 23:35:00'),'2026-09-05');assert.equal(carrierDay('2026-09-05T20:00:00Z'),'2026-09-06');
});
test('newer contradictory carrier status wins over stale delivery evidence',()=>{
 const v=evidence();v.tracking={shipments:{'AWB-A':{status:'rto_delivered',deliveredDate:'2026-09-05',lastUpdated:'2026-09-09T00:00:00Z'}}};
 assert.equal(velocityCompletion(order(),{},v,'2026-10-01').date,'');
});
test('dispatch AWB and legacy cached tracking are supported without index',()=>{
 const v={cache:{shipments:{'AWB-A':{status:'delivered',deliveredDate:'2026-09-05'}}}};
 assert.equal(velocityCompletion(order(),{velocityAwb:'AWB-A'},v,'2026-10-01').date,'2026-09-05');
});
test('delivery evidence is read from Orders storage override and remains unmodified',()=>{
 const dir=fs.mkdtempSync(path.join(os.tmpdir(),'pnl-delivery-read-'));
 try {
  const sub=path.join(dir,'orders');fs.mkdirSync(sub);const file=path.join(sub,'velocity_order_index.json');fs.writeFileSync(file,JSON.stringify(evidence().index));
  const before=fs.readFileSync(file,'utf8'),sources=loadSources(dir,{ORDERS_PATH:path.join(sub,'orders.json')});
  assert.deepEqual(sources.velocity.index,evidence().index);assert.equal(sources.feeds.find(f=>f.name==='Velocity automatic shipment discovery').updatedAt,evidence().index.lastSync);assert.equal(fs.readFileSync(file,'utf8'),before);assert.deepEqual(fs.readdirSync(sub),['velocity_order_index.json']);
 }finally{assert.equal(path.dirname(path.resolve(dir)),path.resolve(os.tmpdir()));assert.ok(path.basename(dir).startsWith('pnl-delivery-read-'));fs.rmSync(dir,{recursive:true,force:true});}
});
test('tracking API accepts flat Velocity and nested legacy shapes, rejects unsolicited AWBs',async()=>{
 const flat={current_status:'Delivered',delivery_date:'2026-09-05',courier_name:'Example Courier',weight:.5};
 const nested={tracking_data:{shipment_status:7,shipment_track:[{current_status:'Delivered',delivered_date:'2026-09-07',weight:.4}]}};
 const result=await fetchTracking({awbs:['A','B'],token:'synthetic',fetchImpl:async()=>({ok:true,json:async()=>({result:{A:flat,B:nested,UNREQUESTED:flat}})})});
 assert.equal(result.A.status,'Delivered');assert.equal(result.A.deliveredDate,'2026-09-05');assert.equal(result.B.status,'Delivered');assert.equal(result.B.deliveredDate,'2026-09-07');assert.equal(result.B.weight,.4);assert.equal(result.UNREQUESTED,undefined);
});

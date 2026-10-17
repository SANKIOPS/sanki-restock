'use strict';
const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs'),os=require('node:os'),path=require('node:path');
const racks=require('../modules/warehouse-racks');
const moves=require('../modules/stock-movements');
const care=require('../modules/inventory-care-store');
const inventory=require('../modules/inventory-state');
const data={source:'Fixture',effectiveFrom:'2026-10-14T00:00:00.000Z',skus:{SA1:{'13B':2,'8A':1}}};
const movement=(id,from,to,quantity=1,status='approved')=>({id,sku:'sa1',from,to,quantity,status,approvedAt:'2026-10-14T01:00:00.000Z'});
test('new sheet imports all rack headers, normalized SKUs and multiple rack references',()=>{
 assert.equal(racks.reference.racks.length,40);assert.equal(Object.keys(racks.reference.skus).length,511);
 const index=racks.createIndex();assert.equal(index('SA3115MF480').warehouseRack,'13B');
 assert.deepEqual(index('sa21k341fs').warehouseRacks,['8B','13C']);
 assert.equal(index('SA37100J 7293XL').warehouseRacks.includes('10B'),true);
 assert.equal(index('MISSING').warehouseRack,null);assert.equal(index('SA3115MF480').displayRack,null);
});
test('only confirmed post-sheet moves update references; retries and old history cannot double-apply',()=>{
 const from={location:'Warehouse',rack:'13B'},to={location:'Display',rack:''};
 const approved=movement('a',from,to,2),pending=movement('b',{location:'Warehouse',rack:'8A'},to,1,'pending');
 const cancelled=movement('c',{location:'Warehouse',rack:'8A'},to,1,'cancelled');
 const old={...movement('d',{location:'Warehouse',rack:'8A'},to),approvedAt:'2026-10-13T23:59:59.000Z'};
 const original=structuredClone(data);const index=racks.createIndex([approved,approved,pending,cancelled,old],data);
 assert.deepEqual(index('SA1').warehouseRacks,['8A']);assert.deepEqual(data,original);
 assert.deepEqual(racks.createIndex([approved],data)('SA1'),index('SA1'));
});
test('rack-only transfers change reference locations while preserving live quantity totals',async()=>{
 const state={version:1,baseline:null,positions:[],movements:[]};
 const live={mapping:{Display:'gid://shopify/Location/1',Warehouse:'gid://shopify/Location/2'},items:[{sku:'SA1',id:'gid://shopify/InventoryItem/1',tracked:true,product:{title:'Shirt'},levels:[{locationId:'gid://shopify/Location/1',available:1,on_hand:1},{locationId:'gid://shopify/Location/2',available:3,on_hand:3}]}]};
 const before=structuredClone(live);
 const m=moves.submitLive(state,{requestId:'rack-transfer-12345678',sku:'SA1',quantity:2,from:{location:'Warehouse',rack:'13B'},to:{location:'Warehouse',rack:'14A'},physicalConfirmed:true},{username:'staff'},live);
 await moves.review(state,m.id,{action:'approve'},{username:'tushar',roles:['inventory']},()=>{},()=>{throw Error('Rack-only moves must not write Shopify');});
 assert.deepEqual(live,before);assert.deepEqual(racks.createIndex([{...m,approvedAt:'2026-10-14T01:00:00.000Z'}],data)('SA1').warehouseRacks,['8A','14A']);
 assert.equal(moves.livePositions(state,live).reduce((n,p)=>n+p.quantity,0),4);
});
test('Display has one position, prevents rack-only moves, and requires an actual warehouse rack',()=>{
 const state={version:1,baseline:null,positions:[],movements:[]};
 const live={mapping:{Display:'gid://shopify/Location/1',Warehouse:'gid://shopify/Location/2'},items:[{sku:'SA1',id:'gid://shopify/InventoryItem/1',tracked:true,product:{title:'Shirt'},levels:[{locationId:'gid://shopify/Location/1',available:3},{locationId:'gid://shopify/Location/2',available:2}]}]};
 const body={requestId:'display-test-12345678',sku:'SA1',quantity:1,from:{location:'Display',rack:'T1'},to:{location:'Warehouse',rack:'13B'},physicalConfirmed:true};
 assert.throws(()=>moves.submitLive(state,{...body,to:{location:'Display',rack:'T2'}},{username:'staff'},live),/different destination/);
 assert.throws(()=>moves.submitLive(state,{...body,to:{location:'Warehouse',rack:''}},{username:'staff'},live),/warehouse rack/);
 const m=moves.submitLive(state,body,{username:'staff'},live);assert.equal(m.from.rack,'');assert.equal(moves.livePositions(state,live)[0].quantity,2);assert.equal(live.items[0].levels[0].available,3);
});
test('legacy Display consolidation preserves counted totals, history and care reservations across old racks',()=>{
 const dir=fs.mkdtempSync(path.join(os.tmpdir(),'rack-consolidation-'));try{
 const p=path.join(dir,'moves.json'),history=movement('old',{location:'Display',rack:'T1'},{location:'Warehouse',rack:'13B'});
 const state={version:1,baseline:null,positions:[{sku:'SA1',location:'Display',rack:'T1',quantity:2},{sku:'SA1',location:'Display',rack:'T2',quantity:3},{sku:'SA1',location:'Warehouse',rack:'13B',quantity:4}],movements:[history]};
 fs.writeFileSync(p,JSON.stringify(state));const loaded=moves.load(p);assert.equal(loaded.positions.length,2);assert.equal(loaded.positions[0].quantity,5);assert.equal(loaded.positions[0].rack,'');assert.deepEqual(loaded.movements,[history]);assert.deepEqual(JSON.parse(fs.readFileSync(p)),state);
 assert.deepEqual(moves.wholeDisplayPositions(loaded.positions),loaded.positions);
 const store={version:1,batches:[{id:'b1',kind:'cleaning',lines:[{id:'a',sku:'SA1',locationId:'d',rack:'T1',quantity:2}]},{id:'b2',kind:'miscellaneous',lines:[{id:'b',sku:'SA1',locationId:'d',rack:'T2',quantity:1}]}],operations:[{batchId:'b1',action:'open',status:'confirmed',lines:[{lineId:'a',quantity:2}]},{batchId:'b2',action:'open',status:'confirmed',lines:[{lineId:'b',quantity:1}]}]};
 assert.equal(care.reservedAt(store,'SA1','d','',true),3);assert.equal(care.reservedAt(store,'SA1','d','T1'),2);
 }finally{fs.rmSync(dir,{recursive:true});}
});
test('catalogue rack updates replace old rack labels without changing a single quantity',()=>{
 const live={at:new Date().toISOString(),mapping:{Display:'d',Warehouse:'w'},items:[{sku:'SA3115MF480',id:'i',product:{handle:'shirt',title:'Shirt'},levels:[{locationId:'d',available:2,on_hand:3,committed:1},{locationId:'w',available:5,on_hand:5}]}]};
 const before=structuredClone(live);const product=inventory.catalog(live,[{handle:'shirt',variants:[{sku:'SA3115MF480',displayRack:'T1',warehouseRack:'WRONG'}]}],new Map())[0];
 assert.equal(product.variants[0].warehouseRack,'13B');assert.equal(product.variants[0].displayRack,null);
 assert.equal(product.displayQty,2);assert.equal(product.warehouseQty,5);assert.equal(product.totalQty,8);assert.equal(product.availableQty,7);assert.deepEqual(live,before);
});

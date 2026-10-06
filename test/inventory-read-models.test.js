'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs'), os = require('os'), path = require('path');
const { createSnapshotCache } = require('../modules/inventory-snapshot-cache');
const { resolveMetadata, loadPurchases, createMetadataReader } = require('../modules/inventory-metadata');
const inventory = require('../modules/inventory-state');
function deferred() { let resolve, reject; const promise = new Promise((yes,no) => { resolve=yes;reject=no; }); return {promise,resolve,reject}; }
function live(at = '2026-10-06T00:00:00.000Z', quantity = 1) {
  return { at, mapping:{Display:'d'}, items:[{id:'i',sku:'SA1',product:{handle:'tee',title:'Tee',productType:'Crew Neck'},levels:[{locationId:'d',available:quantity,on_hand:quantity,quality_control:0,damaged:0,committed:0,reserved:0,safety_stock:0}]}] };
}
test('display cache survives restart, returns immediately during refresh, and is never a forced-write snapshot',async()=>{
  const dir=fs.mkdtempSync(path.join(os.tmpdir(),'sanki-snapshot-')),file=path.join(dir,'snapshot.json');
  try {
    const old=live();const first=createSnapshotCache({file,fetchSnapshot:async()=>old});await first.snapshot(true);
    const next=deferred(), fresh=live('2026-10-06T00:01:00.000Z',2);
    const restarted=createSnapshotCache({file,fetchSnapshot:()=>next.promise});
    const view=restarted.read();assert.equal(view.snapshot.items[0].levels[0].available,1);assert.equal(view.refreshing,true);assert.equal(view.stale,true);
    let finished=false;const forced=restarted.snapshot(true).then(s=>{finished=true;return s;});await new Promise(r=>setImmediate(r));assert.equal(finished,false);
    next.resolve(fresh);assert.deepEqual(await forced,fresh);assert.equal(restarted.read().snapshot.items[0].levels[0].available,2);
  }finally{fs.rmSync(dir,{recursive:true});}
});
test('simultaneous forced readers join one refresh rather than queueing full catalogue reads',async()=>{
  let calls=0;const pending=deferred(), cache=createSnapshotCache({fetchSnapshot:()=>{calls++;return pending.promise;},now:()=>Date.parse(live().at)});
  const a=cache.snapshot(true),b=cache.snapshot(true);await new Promise(r=>setImmediate(r));assert.equal(calls,1);
  pending.resolve(live());assert.deepEqual(await a,await b);assert.equal(calls,1);
});
test('refresh failure retains the last confirmed display and backs off without allowing stale write confirmation',async()=>{
  let clock=Date.parse(live().at),calls=0;
  const cache=createSnapshotCache({now:()=>clock,fetchSnapshot:async()=>{calls++;if(calls>1)throw Error('Shopify offline');return live();}});
  await cache.snapshot();clock+=61000;const view=cache.read();assert.equal(view.snapshot.items[0].levels[0].available,1);
  await new Promise(r=>setImmediate(r));assert.equal(cache.read().refreshError,'Shopify offline');assert.equal(cache.read().refreshing,false);assert.equal(calls,2);
  await assert.rejects(cache.snapshot(true),/offline/);assert.equal(calls,3);
});
test('a refresh started before a stock change cannot replace the new generation',async()=>{
  const before=deferred(),after=deferred();let calls=0;
  const cache=createSnapshotCache({now:()=>Date.parse(live().at),fetchSnapshot:()=>++calls===1?before.promise:after.promise});
  const reading=cache.snapshot(true);await new Promise(r=>setImmediate(r));cache.invalidate();before.resolve(live());await new Promise(r=>setImmediate(r));
  assert.equal(cache.read().snapshot,null);assert.equal(calls,2);after.resolve(live('2026-10-06T00:00:01.000Z',4));assert.equal((await reading).items[0].levels[0].available,4);
});
test('explicit Shopify labels classify a new product while preserving physical labels and flagging conflicts',()=>{
  const product={productType:'Crew Neck',tags:[' SANKI Category: T-SHIRT','SANKI Fit: oversized','SANKI Gender: unisex','SANKI Collection: funky']};
  const fresh=resolveMetadata(product);assert.equal(fresh.category,'T-Shirts');assert.equal(fresh.fit,'Oversized');assert.equal(fresh.gender,'Unisex');assert.equal(fresh.collection,'SANKI Funky');assert.deepEqual(fresh.classificationIssues,[]);
  const physical=resolveMetadata(product,{category:'T-Shirts',fit:'Relaxed',gender:'Unisex',collection:'SANKI Casuals'});assert.equal(physical.fit,'Relaxed');assert.equal(physical.collection,'SANKI Casuals');assert.ok(physical.classificationIssues.some(x=>x.startsWith('Conflicting fit')));
  const fields=resolveMetadata({...product,fitField:{value:'Baggy'}});assert.equal(fields.fit,'Baggy');assert.ok(fields.classificationIssues.some(x=>x.startsWith('Conflicting fit')));
});
test('posted purchasing records fill missing fields, unposted records do not, and conflicting purchases remain flagged',()=>{
  const dir=fs.mkdtempSync(path.join(os.tmpdir(),'sanki-metadata-')),file=path.join(dir,'purchases.json');
  try{
    fs.writeFileSync(file,JSON.stringify({pos:{one:{status:'posted',line:'casuals',lines:[{sku:'sa1',productType:'SHIRT',fit:'Regular',audience:'Women'}]},two:{status:'received',line:'funky',lines:[{sku:'sa1',fit:'Baggy'},{sku:'sa2',productType:'Jeans'}]}}}));
    const purchases=loadPurchases(file);assert.equal(purchases.has('SA2'),false);
    const result=resolveMetadata({productType:'Shirt'},{},purchases.get('SA1'));assert.equal(result.category,'Shirts');assert.equal(result.fit,'Regular');assert.equal(result.gender,'Women');assert.equal(result.collection,'SANKI Casuals');
    const conflicting=resolveMetadata({productType:'Shirt'},{},[{fit:'Baggy'},{fit:'Oversized'}]);assert.equal(conflicting.fit,'');assert.ok(conflicting.classificationIssues.some(x=>x.startsWith('Conflicting fit')));
  }finally{fs.rmSync(dir,{recursive:true});}
});
test('custom metadata is read once per new product, reused across variants and refreshes, and a failure preserves quantities',async()=>{
  const source=live();source.items[0].product.id='gid://shopify/Product/1';source.items.push({...source.items[0],id:'i2',sku:'SA2'});
  let calls=0;
  const enrich=createMetadataReader({physical:[],query:async(query,variables)=>{calls++;assert.deepEqual(variables.ids,['gid://shopify/Product/1']);return{nodes:[{id:variables.ids[0],categoryField:{value:'T-Shirts'},fitField:{value:'Oversized'}}]};}});
  const items=await enrich(source.items);assert.equal(items[0].product.categoryField.value,'T-Shirts');assert.equal(items[1].product.fitField.value,'Oversized');await enrich(source.items);assert.equal(calls,1);
  const failed=createMetadataReader({physical:[],query:async()=>{throw Error('Metadata offline');}});const retained=await failed(source.items);assert.equal(retained[0].levels[0].available,1);assert.match(retained[0].product.metadataError,/offline/);
});
test('a stale display snapshot keeps its historic cleaning balance when a newer action is confirmed',()=>{
  const snapshot=live(),register={batches:[{id:'b',kind:'cleaning',lines:[{id:'l',inventoryItemId:'i',locationId:'d',quantity:1}]}],operations:[{batchId:'b',action:'open',status:'confirmed',confirmedAt:'2026-10-06T00:00:01.000Z',lines:[{lineId:'l',quantity:1}]}]};
  assert.equal(inventory.withCare(snapshot,register).items[0].careCleaning.d,0);
  snapshot.at='2026-10-06T00:00:02.000Z';snapshot.items[0].levels[0].quality_control=1;assert.equal(inventory.withCare(snapshot,register).items[0].careCleaning.d,1);
});
test('care pending, cancellation and history work during a blocked full read, and a late response has the current status',()=>{
 const dir=fs.mkdtempSync(path.join(os.tmpdir(),'sanki-care-read-'));
 const source=`
 const assert=require('node:assert/strict'),express=require(${JSON.stringify(require.resolve('express'))});
 const inventory=require(${JSON.stringify(require.resolve('../modules/inventory-state'))});
 let begin,unblock;const started=new Promise(r=>begin=r),pending=new Promise(r=>unblock=r);inventory.snapshot=()=>{begin();return pending;};
 const care=require(${JSON.stringify(require.resolve('../modules/inventory-care'))}),store=require(${JSON.stringify(require.resolve('../modules/inventory-care-store'))});
 const live=${JSON.stringify(live())};live.items[0].tracked=true;live.items[0].inventoryPolicy='DENY';inventory.readSnapshot=()=>({snapshot:live,refreshing:true,stale:true,refreshError:null});
 const data=store.empty(),op=care.makeOpen(data,{requestId:'care-test-request-1234',kind:'cleaning',reason:'Dry cleaning',vendor:'Cleaner',expectedReturn:'2026-12-01',lines:[{sku:'SA1',quantity:1,locationId:'d',rack:''}]},{username:'staff',role:'warehouse'},live);store.save(data);
 const app=express();app.use(express.json());app.use((req,res,next)=>{req.user={username:'tushar',roles:['inventory','warehouse']};next();});app.use(care.router);
 const server=app.listen(0,'127.0.0.1',async()=>{try{
  const url='http://127.0.0.1:'+server.address().port+'/api/inventory-care',slow=fetch(url).then(r=>r.json());await started;
  const before=await fetch(url+'?registerOnly=1',{signal:AbortSignal.timeout(2000)}).then(r=>r.json());assert.equal(before.operations[0].status,'awaiting_approval');assert.equal(before.items,undefined);
  const quantities=await fetch(url+'?quantitiesOnly=1',{signal:AbortSignal.timeout(2000)}).then(r=>r.json());assert.equal(quantities.refreshing,true);assert.equal(quantities.totals.totalQty,1);assert.equal(quantities.operations,undefined);
  const unchanged=await fetch(url+'?quantitiesOnly=1&knownAt='+encodeURIComponent(live.at),{signal:AbortSignal.timeout(2000)}).then(r=>r.json());assert.equal(unchanged.at,live.at);assert.equal(unchanged.items,undefined);
  const result=await fetch(url+'/operations/'+op.id+'/review',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action:'cancel',note:'Replace test request'}),signal:AbortSignal.timeout(2000)}).then(r=>r.json());assert.equal(result.operation.status,'cancelled');
  const history=await fetch(url+'?registerOnly=1',{signal:AbortSignal.timeout(2000)}).then(r=>r.json());assert.equal(history.operations[0].status,'cancelled');
  unblock(live);assert.equal((await slow).operations[0].status,'cancelled');
 }catch(e){console.error(e);process.exitCode=1;}finally{server.closeAllConnections();server.close();}});
 `;
 try{const result=require('node:child_process').spawnSync(process.execPath,['-e',source],{encoding:'utf8',timeout:10000,env:{...process.env,STOCK_MOVEMENTS_PATH:path.join(dir,'moves.json'),INVENTORY_CARE_PATH:path.join(dir,'care.json'),INVENTORY_SNAPSHOT_PATH:path.join(dir,'snapshot.json')}});assert.equal(result.status,0,result.stderr||String(result.error||''));}
 finally{fs.rmSync(dir,{recursive:true});}
});
test('targeted SKU reads quote search terms, paginate, and exclude partial SKU matches; ID reads request only selected items',async()=>{
  const queries=[];
  const node=sku=>({id:'gid://shopify/InventoryItem/'+sku,sku,tracked:true,variant:{id:'v',title:'L',inventoryPolicy:'DENY',product:{handle:'tee',title:'Tee'}},inventoryLevels:{nodes:[{location:{id:'d',name:'Display'},quantities:Object.entries(live().items[0].levels[0]).filter(([k])=>k!=='locationId').map(([name,quantity])=>({name,quantity}))}],pageInfo:{hasNextPage:false,endCursor:null}}});
  const client={store:'test.myshopify.com',request:async(url,options)=>{const q=JSON.parse(options.body);queries.push(q);return{ok:true,status:200,json:async()=>({data:q.variables.ids?{nodes:[node('SA1')]}:{inventoryItems:{nodes:queries.length===1?[node('SA1-EXTRA')]:[node('SA1')],pageInfo:{hasNextPage:queries.length===1,endCursor:'next'}}}})};}};
  const stock=await inventory.snapshotSkus(['sa1'],client);assert.equal(stock.items.length,1);assert.equal(stock.items[0].sku,'SA1');assert.equal(queries[0].variables.query,'sku:"SA1"');assert.equal(queries[1].variables.after,'next');
  const selected=await inventory.snapshotItems(['gid://shopify/InventoryItem/SA1'],client);assert.equal(selected.items.length,1);assert.deepEqual(queries[2].variables.ids,['gid://shopify/InventoryItem/SA1']);assert.ok(!queries[2].query.includes('inventoryItems('));
});

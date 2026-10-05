const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const os = require('os');
const path = require('path');
const crypto = require('crypto');
const { makeOpen, makeAction, buildInput, confirm, registerData } = require('../modules/inventory-care');
const care = require('../modules/inventory-care-store');
const inventory = require('../modules/inventory-state');
const { apiAllowedForUser } = require('../auth');
const admin = { username: 'owner', roles: ['owner'] }, staff = { username: 'staff', roles: ['warehouse'] };
const location = 'gid://shopify/Location/1';
function live() { return { at: new Date().toISOString(), mapping: { Display: location, Warehouse: 'gid://shopify/Location/2' }, items: [{ id: 'gid://shopify/InventoryItem/10', sku: 'SA1', tracked: true, inventoryPolicy: 'DENY', variant: 'FS', variantId: 'gid://shopify/ProductVariant/100', product: { handle: 'tee', title: 'Tee', productType: 'T-Shirts' }, levels: [{ locationId: location, location: 'Display', available: 10000, on_hand: 10000, quality_control: 0, damaged: 0, committed: 0, reserved: 0, safety_stock: 0 }] }] }; }
function body(kind = 'cleaning', quantity = 50) { return { requestId: crypto.randomUUID(), kind, reason: kind === 'cleaning' ? 'Dry cleaning' : 'Altered customer return', vendor: 'Cleaner', expectedReturn: '2026-12-01', note: 'Counted pieces', lines: [{ sku: 'sa1', quantity, locationId: location, rack: 'T1' }] }; }
function confirmLocally(s, op, l) {
  const input = buildInput(s, op, l);
  for (const c of input.changes) {
    const level = l.items.find(i => i.id === c.inventoryItemId).levels.find(x => x.locationId === (c.locationId || c.from.locationId));
    if (op.action === 'dispose') { level.damaged += c.delta; level.on_hand += c.delta; }
    else { level[c.from.name] -= c.quantity; level[c.to.name] += c.quantity; }
  }
  op.status = 'confirmed'; return op;
}
test('10000 owned: 50 cleaning + 10 not for sale reduces available to 9940; partial return and damage preserve owned', () => {
  const s = care.empty(), l = live();
  const cleaning = makeOpen(s, body(), admin, l); confirmLocally(s, cleaning, l);
  const misc = makeOpen(s, body('miscellaneous', 10), admin, l); confirmLocally(s, misc, l);
  let d = registerData(s, l, admin); assert.equal(d.totals.totalQty, 10000); assert.equal(d.totals.availableQty, 9940); assert.equal(d.totals.cleaningQty, 50); assert.equal(d.totals.notForSaleQty, 10);
  const batch = s.batches[0], lineId = batch.lines[0].id;
  const release = makeAction(s, batch.id, { requestId: crypto.randomUUID(), action: 'release', inspected: true, note: '48 inspected and clean', lines: [{ lineId, from: 'cleaning', quantity: 48 }] }, admin);
  confirmLocally(s, release, l);
  const damage = makeAction(s, batch.id, { requestId: crypto.randomUUID(), action: 'classify', note: '2 damaged by cleaner', lines: [{ lineId, from: 'cleaning', quantity: 2 }] }, admin);
  confirmLocally(s, damage, l);
  d = registerData(s, l, admin); assert.equal(d.totals.availableQty, 9988); assert.equal(d.totals.cleaningQty, 0); assert.equal(d.totals.notForSaleQty, 12); assert.equal(d.totals.totalQty, 10000);
  const dispose = makeAction(s, batch.id, { requestId: crypto.randomUUID(), action: 'dispose', disposed: true, note: 'Permanently removed 2 damaged pieces', lines: [{ lineId, from: 'miscellaneous', quantity: 2 }] }, admin);
  assert.equal(buildInput(s, dispose, l).changes[0].changeFromQuantity, 12);
  confirmLocally(s, dispose, l); d = registerData(s, l, admin); assert.equal(d.totals.totalQty, 9998); assert.equal(d.totals.availableQty, 9988); assert.equal(d.totals.notForSaleQty, 10);
  assert.equal(care.reservedAt(s, 'SA1', location, 'T1'), 12);
});
test('duplicate requests cannot double log; changed payload rejected; invalid whole quantities rejected', () => {
  const s = care.empty(), l = live(), b = body(); makeOpen(s,b,staff,l); makeOpen(s,b,staff,l);
  assert.equal(s.batches.length,1); assert.equal(s.operations.length,1); assert.equal(care.balances(s,s.batches[0])[0].cleaning,0);
  assert.throws(()=>makeOpen(s,{...b,note:'different'},staff,l),/different action/);
  for (const q of [0,-1,0.5,'2',10001]) assert.throws(()=>makeOpen(care.empty(),body('cleaning',q),staff,l));
  assert.throws(()=>makeOpen(care.empty(),{...body(),vendor:''},staff,l),/cleaner/);
  assert.throws(()=>makeOpen(care.empty(),{...body(),expectedReturn:'2026-02-30'},staff,l),/date/);
  assert.throws(()=>makeOpen(care.empty(),{...body(),reason:'wrong'},staff,l),/reason/);
});
test('unknown, duplicate, untracked SKU, overselling and invalid locations cannot set stock aside',()=>{
  const l=live(); const b=body();
  assert.throws(()=>makeOpen(care.empty(),{...b,lines:[{...b.lines[0],sku:'UNKNOWN'}]},staff,l));
  assert.throws(()=>makeOpen(care.empty(),{...b,lines:[{...b.lines[0],locationId:'unknown'}]},staff,l));
  assert.throws(()=>makeOpen(care.empty(),{...b,lines:[b.lines[0],b.lines[0]]},staff,l),/duplicate/);
  l.items[0].tracked=false; assert.throws(()=>makeOpen(care.empty(),b,staff,l));
  l.items[0].tracked=true;l.items[0].inventoryPolicy='CONTINUE';assert.throws(()=>makeOpen(care.empty(),b,staff,l),/selling when out of stock/);
  l.items[0].inventoryPolicy='DENY';l.items.push({...l.items[0],id:'another'});assert.throws(()=>makeOpen(care.empty(),b,staff,l),/one tracked/);
});
test('staff requests require manager; return requires inspection, cannot exceed or repeat outstanding quantity',async()=>{
  const s=care.empty(),l=live(),op=makeOpen(s,body(),staff,l);
  await assert.rejects(confirm(s,op,staff),/Only the owner/);
  confirmLocally(s,op,l);const batch=s.batches[0],returnBody={requestId:crypto.randomUUID(),action:'release',inspected:true,note:'Checked',lines:[{lineId:batch.lines[0].id,from:'cleaning',quantity:50}]};
  assert.throws(()=>makeAction(s,batch.id,returnBody,staff),/Only the owner/);
  assert.throws(()=>makeAction(s,batch.id,{...returnBody,inspected:false},admin),/inspected/);
  assert.throws(()=>makeAction(s,batch.id,{...returnBody,lines:[{...returnBody.lines[0],quantity:51}]},admin),/exceeds/);
  const a=makeAction(s,batch.id,returnBody,admin);confirmLocally(s,a,l);assert.equal(makeAction(s,batch.id,returnBody,admin).id,a.id);
  assert.throws(()=>makeAction(s,batch.id,{...returnBody,requestId:crypto.randomUUID()},admin),/exceeds/);
});
test('unknown network result persists the exact request and idempotency key; retry confirms once',async()=>{
  const s=care.empty(),l=live(),op=makeOpen(s,body(),admin,l),dir=fs.mkdtempSync(path.join(os.tmpdir(),'sanki-care-')),file=path.join(dir,'care.json');
  const calls=[]; const deps={snapshot:async()=>l,save:x=>care.save(x,file),graphql:async(q,v)=>{calls.push(structuredClone(v));assert.equal(care.load(file).operations[0].status,'sync_pending');throw Error('network interrupted');}};
  await assert.rejects(confirm(s,op,admin,deps),/interrupted/); assert.equal(op.status,'sync_pending'); assert.equal(care.blockedSku(s,'SA1'),true);
  assert.throws(()=>makeOpen(s,body(),admin,l),/pending Shopify/);
  const reloaded=care.load(file),again=reloaded.operations[0];
  await confirm(reloaded,again,admin,{...deps,snapshot:async()=>{throw Error('Must reuse exact input');},graphql:async(q,v)=>{calls.push(structuredClone(v));return{inventoryMoveQuantities:{userErrors:[],inventoryAdjustmentGroup:{createdAt:'now'}}};}});
  assert.deepEqual(calls[0],calls[1]);assert.equal(care.load(file).operations[0].status,'confirmed');assert.equal(care.balances(reloaded,reloaded.batches[0])[0].cleaning,50);
  await confirm(reloaded,again,admin,{...deps,graphql:async()=>{throw Error('Must not send confirmed operation');}});
  fs.rmSync(dir,{recursive:true});
});
test('definitive Shopify rejection never changes local quantities; exhausted retry window stays blocked',async()=>{
  const s=care.empty(),l=live(),op=makeOpen(s,body(),admin,l);
  await assert.rejects(confirm(s,op,admin,{snapshot:async()=>l,save:()=>{},graphql:async()=>({inventoryMoveQuantities:{userErrors:[{code:'CHANGE_FROM_QUANTITY_STALE',message:'CHANGE_FROM_QUANTITY_STALE'}]}})}),/STALE/);
  assert.equal(op.status,'review_required');assert.equal(care.balances(s,s.batches[0])[0].cleaning,0);
  op.status='sync_pending';op.firstAttemptAt='2020-01-01T00:00:00Z';await assert.rejects(confirm(s,op,admin,{save:()=>{}}),/retry window/);
});
test('live catalogue totals include held, committed and all locations; outside quality control is not mislabeled cleaning',()=>{
  const l=live();l.items[0].levels[0]={...l.items[0].levels[0],available:1,on_hand:5,committed:1,quality_control:2,damaged:1};
  l.items[0].levels.push({...l.items[0].levels[0],locationId:'gid://shopify/Location/2',location:'Warehouse',available:1,on_hand:1,committed:0,quality_control:0,damaged:0});
  const d=registerData(care.empty(),l,admin);assert.equal(d.totals.displayQty,1);assert.equal(d.totals.warehouseQty,1);assert.equal(d.totals.availableQty,2);assert.equal(d.totals.totalQty,6);assert.equal(d.totals.cleaningQty,0);assert.equal(d.totals.otherUnavailableQty,2);
  const products=inventory.catalog(l,[{handle:'tee',category:'Tees',variants:[{sku:'SA1',displayQty:999,warehouseQty:999}]}]);assert.equal(products[0].totalQty,6);assert.equal(products[0].variants[0].totalQty,6);assert.equal(products[0].displayQty,1);
});
test('inventory care permissions match inventory roles; stylists, sales and accounting do not write',()=>{
  for(const role of ['inventory','warehouse','owner','admin'])assert.equal(apiAllowedForUser({role},'/api/inventory-care/batches','POST'),true);
  for(const role of ['sales','accounting','stocksearch','claimant'])assert.equal(apiAllowedForUser({role},'/api/inventory-care/batches','POST'),false);
});
test('snapshot paginates item and location lists and never hides an incomplete quantity',async()=>{
  const queries=[];const item={id:'gid://shopify/InventoryItem/10',sku:'SA1',tracked:true,variant:{id:'v',title:'FS',inventoryPolicy:'DENY',product:{handle:'tee',title:'Tee'}},inventoryLevels:{nodes:[],pageInfo:{hasNextPage:true,endCursor:'level-1'}}};
  const level={location:{id:location,name:'Display'},quantities:['available','on_hand','committed','quality_control','damaged','reserved','safety_stock'].map(name=>({name,quantity:name==='available'||name==='on_hand'?1:0}))};
  const pages=[{inventoryItems:{nodes:[item],pageInfo:{hasNextPage:true,endCursor:'item-1'}}},{inventoryItem:{inventoryLevels:{nodes:[level],pageInfo:{hasNextPage:false,endCursor:'level-2'}}}},{inventoryItems:{nodes:[],pageInfo:{hasNextPage:false,endCursor:'item-2'}}}];
  const client={store:'test.myshopify.com',request:async(url,options)=>{queries.push(JSON.parse(options.body).variables);return{ok:true,status:200,json:async()=>({data:pages.shift()})};}};
  const s=await inventory.fetchSnapshot(client);assert.equal(s.items[0].levels[0].available,1);assert.equal(queries[1].after,'level-1');assert.equal(queries[2].after,'item-1');
  assert.throws(()=>inventory.normalizeItem({...item,inventoryLevels:{nodes:[{...level,quantities:[]} ]}}),/incomplete/);
});
test('GraphQL throttling retries after the advertised budget refill without disabling CAS',async()=>{
  let calls=0;const delays=[];
  const client={store:'test.myshopify.com',sleep:async ms=>delays.push(ms),request:async()=>({ok:true,status:200,json:async()=>++calls===1?{errors:[{extensions:{code:'THROTTLED'}}],extensions:{cost:{requestedQueryCost:200,throttleStatus:{currentlyAvailable:0,restoreRate:100}}}}:{data:{confirmed:true}}})};
  assert.deepEqual(await inventory.graphql('query{shop{id}}',{},client),{confirmed:true});assert.equal(calls,2);assert.equal(delays[0],2250);
});

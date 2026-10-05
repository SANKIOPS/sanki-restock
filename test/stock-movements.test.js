const { test } = require('node:test');
const assert = require('node:assert/strict');
const { submit, canApprove, ready, rackChoices, load, save } = require('../modules/stock-movements');
const fs = require('fs');
const os = require('os');
const path = require('path');
function baseline() { return { version:1, baseline:{reconciled:true,reviewedBy:'owner',reconciledAt:'2026-09-15',racks:{Display:['1','Accesorries'],Warehouse:['5A']}}, positions:[{sku:'SA1',location:'Warehouse',rack:'5A',quantity:3}],movements:[] }; }
function body() { return {requestId:'12345678-1234-4321-8888-123456789012',sku:'sa1',quantity:2,from:{location:'Warehouse',rack:'5A'},to:{location:'Display',rack:'1'}}; }
test('moves preserve total pieces and immediately reflect physical position pending review',()=>{const s=baseline();const m=submit(s,body(),{username:'staff'});assert.equal(m.status,'pending');assert.equal(s.positions[0].quantity,1);assert.equal(s.positions[1].quantity,2);assert.equal(s.positions.reduce((n,p)=>n+p.quantity,0),3);});
test('duplicate submission is idempotent; changed duplicate is rejected',()=>{const s=baseline();submit(s,body(),{username:'staff'});submit(s,body(),{username:'staff'});assert.equal(s.movements.length,1);assert.throws(()=>submit(s,{...body(),quantity:1},{username:'staff'}),/another move/);});
test('unreconciled baseline, anonymous user, invalid quantities and nonexistent racks cannot change stock',()=>{const s=baseline();s.baseline.reconciled=false;assert.equal(ready(s),false);assert.throws(()=>submit(s,body(),{username:'staff'}),/baseline/);s.baseline.reconciled=true;assert.throws(()=>submit(s,body(),{}),/named staff/);for(const q of [0,-1,1.5,'2'])assert.throws(()=>submit(s,{...body(),quantity:q},{username:'staff'}));assert.throws(()=>submit(s,{...body(),to:{location:'Display',rack:'fake'}},{username:'staff'}));assert.equal(s.positions[0].quantity,3);});
test('insufficient source stock and identical positions are rejected',()=>{const s=baseline();assert.throws(()=>submit(s,{...body(),quantity:4},{username:'staff'}),/Not enough/);assert.throws(()=>submit(s,{...body(),to:body().from},{username:'staff'}));});
test('unassigned rack is permitted, correction-required SKU is blocked',()=>{const s=baseline();submit(s,{...body(),to:{location:'Display',rack:''}},{username:'staff'});assert.equal(s.positions[1].rack,'');s.movements[0].status='correction_required';assert.throws(()=>submit(s,{...body(),requestId:'other-unique-request-1234',quantity:1},{username:'staff'}),/correction/);});
test('Tushar manages inventory without granting approval to unrelated staff or read-only logins',()=>{
 assert.equal(canApprove({roles:['inventory','procurement','warehouse','stocksearch'],username:'tushar'}),true);
 assert.equal(canApprove({role:'warehouse',username:'tushar'}),true);
 for(const role of ['warehouse','inventory','stocksearch'])assert.equal(canApprove({role,username:'staff'}),false);
 assert.equal(canApprove({role:'stocksearch',username:'tushar'}),false);assert.equal(canApprove({role:'inventory',username:'tushar-other'}),false);
 assert.equal(canApprove({role:'admin'}),true);assert.equal(canApprove({roles:['owner']}),true);
});
test('atomic store survives reload; corrupt data fails closed',()=>{const dir=fs.mkdtempSync(path.join(os.tmpdir(),'sanki-move-test-'));const file=path.join(dir,'moves.json');assert.equal(ready(load(file)),false);save(baseline(),file);assert.equal(load(file).positions[0].quantity,3);fs.writeFileSync(file,'broken');assert.throws(()=>load(file));fs.rmSync(dir,{recursive:true});});
test('unreconciled form lists counted racks by location without enabling moves',()=>{const s={version:1,baseline:null,positions:[],movements:[]};const racks=rackChoices(s);assert.equal(ready(s),false);assert.ok(racks.Display.includes('Accesorries'));assert.ok(racks.Display.includes('T1'));assert.ok(racks.Warehouse.includes('5A'));assert.ok(racks.Warehouse.includes('14C'));assert.equal(racks.Warehouse.includes('T1'),false);assert.throws(()=>submit(s,body(),{username:'staff'}),/baseline/);assert.deepEqual(rackChoices(baseline()),baseline().baseline.racks);});

const {submitLive,livePositions,liveReady,review}=require('../modules/stock-movements');
function liveState(){return {mapping:{Display:'gid://shopify/Location/1',Warehouse:'gid://shopify/Location/2'},items:[{id:'gid://shopify/InventoryItem/10',sku:'SA3210J515L',tracked:true,product:{title:'Tee'},levels:[{locationId:'gid://shopify/Location/1',available:1,on_hand:1},{locationId:'gid://shopify/Location/2',available:0,on_hand:0}]}]};}
function liveStore(){return {version:1,baseline:null,positions:[],movements:[]};}
function liveBody(){return {requestId:'12345678-1234-4321-8888-123456789012',sku:'SA3210J515L',quantity:1,from:{location:'Display',rack:'T1'},to:{location:'Warehouse',rack:'5B'},physicalConfirmed:true};}
const mover={username:'staff',role:'warehouse'},manager={username:'reviewer',role:'admin'};
test('a new movement register can submit Display T1 to Warehouse 5B from live stock without inventing a reconciled baseline',()=>{
 const s=liveStore(),l=liveState();assert.equal(liveReady(l),true);const m=submitLive(s,liveBody(),mover,l);assert.equal(m.status,'pending');assert.equal(s.baseline,null);assert.equal(s.positions.length,0);assert.equal(l.items[0].levels[0].available,1);assert.equal(livePositions(s,l)[0].quantity,0);assert.equal(livePositions(s,l)[1].quantity,0);
 assert.equal(submitLive(s,liveBody(),mover,l).id,m.id);assert.equal(s.movements.length,1);assert.throws(()=>submitLive(s,{...liveBody(),requestId:'another-request-id-1234'},mover,l),/existing movement/);
});
test('live movement rejects missing confirmation, excess pieces, unknown racks, duplicate SKU and missing location mappings',()=>{
 for(const changes of [{physicalConfirmed:false},{quantity:2},{from:{location:'Display',rack:'unknown'}}])assert.throws(()=>submitLive(liveStore(),{...liveBody(),...changes},mover,liveState()));
 const l=liveState();l.items.push({...l.items[0]});assert.throws(()=>submitLive(liveStore(),liveBody(),mover,l),/one tracked/);l.items.pop();l.mapping.Warehouse=l.mapping.Display;assert.equal(liveReady(l),false);assert.throws(()=>submitLive(liveStore(),liveBody(),mover,l),/different Display/);
});
test('approval transfers 1 to 0 using exact persisted CAS, retains total and cannot double transfer',async()=>{
 const s=liveStore(),l=liveState(),m=submitLive(s,liveBody(),mover,l);let writes=0,persistedInput;
 const send=async(q,v)=>{if(q.startsWith('query'))return {inventoryItem:{source:{quantities:[{name:'available',quantity:1}]},destination:{quantities:[{name:'available',quantity:0}]}}};writes++;assert.deepEqual(persistedInput,v.input);assert.equal(v.key,m.id);const quantities=v.input.quantities;assert.deepEqual(quantities.map(x=>[x.changeFromQuantity,x.quantity]),[[1,0],[0,1]]);assert.equal(quantities.reduce((n,x)=>n+x.quantity,0),1);return {inventorySetQuantities:{inventoryAdjustmentGroup:{createdAt:'now'},userErrors:[]}};};
 const persist=()=>{persistedInput=structuredClone(m.syncInput);};
 await assert.rejects(review(s,m.id,{action:'approve'},mover,persist,send),/manager/);
 await review(s,m.id,{action:'approve'},{username:'staff',role:'admin'},persist,send);assert.equal(m.status,'approved');assert.equal(m.reviewedBy,m.submittedBy);assert.ok(m.reviewedAt);
 await review(s,m.id,{action:'approve'},manager,persist,send);assert.equal(writes,1);assert.equal(m.reviewedBy,m.submittedBy);
});
test('uncertain transfer retries the same key and quantities and cannot be physically cancelled',async()=>{
 const s=liveStore(),m=submitLive(s,liveBody(),mover,liveState());const calls=[];
 const send=async(q,v)=>{if(q.startsWith('query'))return {inventoryItem:{source:{quantities:[{name:'available',quantity:1}]},destination:{quantities:[{name:'available',quantity:0}]}}};calls.push(structuredClone(v));throw Error('network interrupted');};
 await assert.rejects(review(s,m.id,{action:'approve'},manager,()=>{},send),/interrupted/);assert.equal(m.status,'sync_pending');await assert.rejects(review(s,m.id,{action:'correction',reason:'return'},manager,()=>{},send),/already been attempted/);
 await review(s,m.id,{action:'approve'},manager,()=>{},async(q,v)=>{calls.push(structuredClone(v));return {inventorySetQuantities:{inventoryAdjustmentGroup:{createdAt:'now'},userErrors:[]}};});assert.deepEqual(calls[0],calls[1]);
});
test('failed CAS can be corrected and resolved without altering Shopify or leaving a stale reservation',async()=>{
 const s=liveStore(),l=liveState(),m=submitLive(s,liveBody(),mover,l);
 const send=async(q)=>q.startsWith('query')?{inventoryItem:{source:{quantities:[{name:'available',quantity:1}]},destination:{quantities:[{name:'available',quantity:0}]}}}:{inventorySetQuantities:{userErrors:[{code:'CHANGE_FROM_QUANTITY_STALE',message:'stale'}]}};
 await assert.rejects(review(s,m.id,{action:'approve'},manager,()=>{},send),/not applied/);assert.equal(m.syncRejected,true);
 await review(s,m.id,{action:'correction',reason:'Return physically'},manager,()=>{},send);
 await assert.rejects(review(s,m.id,{action:'resolve',reason:'Returned'},manager,()=>{},send),/Verify/);
 await review(s,m.id,{action:'resolve',reason:'Verified returned to T1',physicalCorrected:true},manager,()=>{},send);assert.equal(m.status,'cancelled');assert.equal(livePositions(s,l)[0].quantity,1);
});
test('same-location rack move records approval without altering Shopify quantity',async()=>{
 const s=liveStore(),m=submitLive(s,{...liveBody(),to:{location:'Display',rack:'1'}},mover,liveState());await review(s,m.id,{action:'approve'},manager,()=>{},async()=>{throw Error('Must not change Shopify for rack-only move');});assert.equal(m.status,'approved');
});

test('Tushar can approve his own or another employee movement and keeps both actors in history',async()=>{
 const tushar={username:'tushar',roles:['inventory','warehouse']};
 for(const submitter of [mover,tushar]){
  const s=liveStore(),m=submitLive(s,liveBody(),submitter,liveState());let writes=0;
  const send=async(q)=>q.startsWith('query')?{inventoryItem:{source:{quantities:[{name:'available',quantity:1}]},destination:{quantities:[{name:'available',quantity:0}]}}}:(writes++,{inventorySetQuantities:{inventoryAdjustmentGroup:{createdAt:'now'},userErrors:[]}});
  await review(s,m.id,{action:'approve'},tushar,()=>{},send);assert.equal(m.status,'approved');assert.equal(m.submittedBy,submitter.username);assert.equal(m.reviewedBy,'tushar');assert.ok(m.approvedAt);assert.equal(writes,1);
 }
});

test('cancel a pending request without moving Shopify stock, release its reservation and retain the audit',async()=>{
 const s=liveStore(),l=liveState(),m=submitLive(s,liveBody(),{username:'prashant',role:'admin'},l);let saves=0;
 const noWrite=async()=>{throw Error('Cancellation must not write Shopify');};
 await assert.rejects(review(s,m.id,{action:'cancel',reason:'Retest'},mover,()=>{},noWrite),/manager/);
 await assert.rejects(review(s,m.id,{action:'cancel',reason:' '},manager,()=>{},noWrite),/reason/);
 assert.equal(m.status,'pending');assert.equal(livePositions(s,l)[0].quantity,0);
 await review(s,m.id,{action:'cancel',reason:'Replace the test request'},{username:'prashant',role:'admin'},()=>saves++,noWrite);
 assert.equal(m.status,'cancelled');assert.equal(m.cancelledBy,'prashant');assert.ok(m.cancelledAt);assert.equal(m.cancellationReason,'Replace the test request');assert.equal(m.submittedBy,'prashant');assert.equal(s.movements.length,1);assert.equal(livePositions(s,l)[0].quantity,1);assert.equal(saves,1);
 await review(s,m.id,{action:'cancel',reason:'Repeated'},manager,()=>saves++,noWrite);assert.equal(saves,1);assert.equal(m.cancelledBy,'prashant');
 const replacement=submitLive(s,{...liveBody(),requestId:'replacement-request-id-1234'},mover,l);assert.equal(replacement.status,'pending');assert.equal(s.movements.length,2);
});

test('cancellation cannot release reservations for dispatched, uncertain, rejected or audited-position moves',async()=>{
 for(const changes of [{status:'sync_pending'},{syncInput:{}},{firstAttemptAt:new Date().toISOString()},{status:'correction_required',syncRejected:true},{mode:undefined}]){
  const s=liveStore(),m=submitLive(s,liveBody(),mover,liveState());Object.assign(m,changes);const before=structuredClone(m);
  await assert.rejects(review(s,m.id,{action:'cancel',reason:'Retest'},manager,()=>{throw Error('Must not save');},async()=>{throw Error('Must not send');}),/awaiting-approval/);assert.deepEqual(m,before);
 }
});

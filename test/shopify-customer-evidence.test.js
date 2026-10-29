'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const vm = require('node:vm');
const {refresh} = require('../modules/shopify-customer-evidence');
const copy = value => JSON.parse(JSON.stringify(value));
function setup() {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(),'shopify-customer-evidence-')), filePath = path.join(directory,'orders.json');
  const order = (id,number) => ({
    id,number,name:'#'+number,channel:'POS',currency:'INR',total:2499,createdAt:'2026-10-21T10:00:00Z',processedAt:'2026-10-21T10:00:00Z',
    financialStatus:'paid',customer:{name:'Fictional customer',email:'fictional@example.invalid'},note:'Keep note',refundAmount:0,moneyRefunded:0,storeCreditIssued:0,
    paymentGateways:['Cash'],paymentTransactions:[{id:'payment-'+id,kind:'sale',status:'success',gateway:'Cash',amount:2499}],refundTransactions:[],
    lineItems:[{id:'line-'+id,qty:1,price:2499}],customSourceField:{preserved:true}
  });
  const store = {orders:{'101':order('101',1001),'102':order('102',1002),'103':order('103',1003)},dispatch:{'101':{packingStatus:'packed',remarks:'Keep dispatch'}},sync:{lastSyncedAt:'2026-10-21T12:00:00Z'},otherMetadata:{retained:true}};
  const raw = order => ({id:order.id,order_number:order.number,name:order.name,source_name:'pos',currency:'INR',total_price:'2499.00',created_at:order.createdAt,processed_at:order.processedAt,customer:{id:'5001',name:'No raw customer data may be returned',email:'secret@example.invalid'}});
  const responses = {'101':raw(store.orders['101']),'102':raw(store.orders['102'])}, calls = [];
  fs.writeFileSync(filePath,JSON.stringify(store));
  const client = {request:async(url,options) => {calls.push({url,options});const selected=new URL(url).pathname.match(/orders\/(\d+)\.json$/)[1];return{ok:true,json:async()=>({order:copy(responses[selected])})};}};
  const options = {filePath,orderIds:['101','102'],actor:'accounting-admin',client,store:'fictional.myshopify.com',validate:()=>{},now:()=> '2026-10-22T09:00:00Z'};
  return {directory,filePath,store,responses,calls,options,read:()=>JSON.parse(fs.readFileSync(filePath,'utf8')),bytes:()=>fs.readFileSync(filePath,'utf8'),write:value=>fs.writeFileSync(filePath,JSON.stringify(value))};
}
function close(f) {fs.rmSync(f.directory,{recursive:true,force:true});}
async function failsWithoutChange(f,options=f.options,pattern) {
  const before=f.bytes();
  await assert.rejects(()=>refresh(options),error=>[400,409,502,503].includes(error.status)&&(!pattern||pattern.test(error.message)));
  assert.equal(f.bytes(),before);
  assert.deepEqual(fs.readdirSync(f.directory),['orders.json']);
}

test('hydrates both missing IDs and writes provenance while preserving all financial, customer and dispatch fields',async()=>{
  const f=setup();
  try {
    const before=copy(f.store),result=await refresh(f.options),after=f.read();
    assert.deepEqual(result,{success:true,updated:2,customerMatchVerified:true});
    assert.equal(JSON.stringify(result).includes('5001'),false);
    assert.equal(JSON.stringify(result).includes('secret'),false);
    for(const id of ['101','102']) {
      assert.equal(after.orders[id].customer.id,'5001');
      delete after.orders[id].customer.id;
    }
    const events=after.customerIdentityRefreshes;delete after.customerIdentityRefreshes;
    assert.deepEqual(after,before);
    assert.equal(events.length,1);
    assert.equal(events[0].actor,'accounting-admin');
    assert.equal(events[0].checkedAt,'2026-10-22T09:00:00Z');
    assert.deepEqual(events[0].orderIds,['101','102']);
    assert.deepEqual(events[0].changedOrderIds,['101','102']);
    assert.match(events[0].verifiedCustomerIdHash,/^[a-f0-9]{64}$/);
    assert.equal(JSON.stringify(events).includes('secret@example.invalid'),false);
    assert.equal(f.calls.length,2);
    for(const call of f.calls) {
      assert.equal(call.options.method,'GET');
      assert.equal(new URL(call.url).hostname,'fictional.myshopify.com');
      assert.equal(new URL(call.url).pathname.includes('/customers'),false);
    }
  } finally {close(f);}
});

test('one existing matching ID is retained and only the missing ID is filled',async()=>{
  const f=setup();
  try {
    f.store.orders['101'].customer.id='5001';f.write(f.store);
    assert.equal((await refresh(f.options)).updated,1);
    assert.deepEqual(f.read().customerIdentityRefreshes[0].changedOrderIds,['102']);
    assert.equal((await refresh(f.options)).updated,0);
  } finally {close(f);}
});

test('two distinct existing numeric POS orders and a synchronous accounting validator are mandatory',async()=>{
  for(const patch of [
    {orderIds:['101']},{orderIds:['101','101']},{orderIds:['101','missing']},{orderIds:['101','104']},{actor:''},{validate:null}
  ]) {
    const f=setup();try{await failsWithoutChange(f,{...f.options,...patch});assert.equal(f.calls.length,0);}finally{close(f);}
  }
  const f=setup();
  try {f.store.orders['102'].channel='Website';f.write(f.store);await failsWithoutChange(f);assert.equal(f.calls.length,0);}
  finally{close(f);}
});

test('missing or different authoritative customer IDs fail all-or-nothing',async()=>{
  for(const customer of [{},{id:''},{id:'5002'},null]) {
    const f=setup();
    try {f.responses['102'].customer=customer;await failsWithoutChange(f,f.options,/customer/);}
    finally{close(f);}
  }
});

test('an existing nonempty customer ID is never silently reassigned',async()=>{
  const f=setup();
  try {f.store.orders['101'].customer.id='5999';f.write(f.store);await failsWithoutChange(f,f.options,/conflicts with Shopify/);}
  finally{close(f);}
});

test('exact order IDs, numbers, channels, currency, paise totals and dates must match the saved source',async()=>{
  for(const patch of [
    {id:'999'},{order_number:2001},{name:'#changed'},{source_name:'web'},{currency:'USD'},
    {total_price:'2499.01'},{total_price:'2499.001'},{total_price:'invalid'},
    {created_at:'2026-10-20T10:00:00Z'},{processed_at:'2026-10-22T10:00:00Z'},{customer:{id:9007199254740992}}
  ]) {
    const f=setup();try{Object.assign(f.responses['102'],patch);await failsWithoutChange(f);}finally{close(f);}
  }
});

test('a null Shopify payment date uses the creation-date anchor but a genuinely different effective date is rejected',async()=>{
  const f=setup();
  try {
    f.responses['101'].processed_at=null;
    f.responses['102'].processed_at=null;
    f.store.orders['101'].processedAt=f.store.orders['101'].createdAt;
    delete f.store.orders['102'].processedAt;
    f.write(f.store);
    assert.equal((await refresh(f.options)).updated,2);
    const after=f.read();
    assert.equal(after.orders['101'].processedAt,f.store.orders['101'].createdAt);
    assert.equal(after.orders['102'].processedAt,undefined,'identity hydration does not add a payment date');
  }finally{close(f);}
  const changed=setup();
  try {
    changed.responses['102'].processed_at=null;
    changed.store.orders['102'].processedAt='2026-10-21T11:00:00Z';
    changed.write(changed.store);
    await failsWithoutChange(changed,changed.options,/payment date changed/);
  }finally{close(changed);}
});

test('provider errors and invalid JSON leave both cached identities unchanged',async()=>{
  const f=setup();
  try {
    f.options.client={request:async()=>({ok:false,status:503})};
    await failsWithoutChange(f,f.options,/503/);
    f.options.client={request:async()=>({ok:true,json:async()=>{throw Error('raw payload');}})};
    await failsWithoutChange(f,f.options,/unreadable/);
  }finally{close(f);}
});

test('selected financial drift during the network lookup aborts without overwriting the concurrent update',async()=>{
  const f=setup();
  try {
    const base=f.options.client.request;let altered;
    f.options.client={request:async(...args)=>{
      const result=await base(...args);
      if(f.calls.length===2){altered=f.read();altered.orders['101'].paymentTransactions[0].amount=2500;f.write(altered);}
      return result;
    }};
    await assert.rejects(()=>refresh(f.options),/Selected order evidence changed/);
    assert.deepEqual(f.read(),altered);
    assert.equal(f.read().orders['101'].customer.id,undefined);
  }finally{close(f);}
});

test('a concurrent unrelated order and dispatch edit survives the latest-cache merge',async()=>{
  const f=setup();
  try {
    const base=f.options.client.request;
    f.options.client={request:async(...args)=>{
      const result=await base(...args);
      if(f.calls.length===2){const current=f.read();current.orders['103'].total=5000;current.dispatch['101'].remarks='Concurrent dispatch';current.sync.extraCursor='keep latest';f.write(current);}
      return result;
    }};
    await refresh(f.options);
    const after=f.read();
    assert.equal(after.orders['103'].total,5000);
    assert.equal(after.dispatch['101'].remarks,'Concurrent dispatch');
    assert.equal(after.sync.extraCursor,'keep latest');
    assert.equal(after.orders['101'].customer.id,'5001');
  }finally{close(f);}
});

test('accounting callback receives before/after maps and can block every cache write',async()=>{
  const f=setup();
  try {
    let called=false;
    f.options.validate=(before,after)=>{
      called=true;assert.equal(before['101'].customer.id,undefined);assert.equal(after['101'].customer.id,'5001');
      assert.deepEqual(before['101'].paymentTransactions,after['101'].paymentTransactions);
      throw Object.assign(new Error('Protected accounting review required'),{status:409});
    };
    await failsWithoutChange(f,f.options,/Protected accounting review/);assert.equal(called,true);
  }finally{close(f);}
});

test('an asynchronous callback is rejected and callback arguments cannot mutate the saved financial fields',async()=>{
  const f=setup();
  try {
    f.options.validate=()=>Promise.resolve();
    await failsWithoutChange(f,f.options,/synchronously/);
    f.options.validate=(before,after)=>{before['101'].total=1;after['101'].paymentTransactions=[];};
    await refresh(f.options);
    assert.equal(f.read().orders['101'].total,2499);
    assert.equal(f.read().orders['101'].paymentTransactions.length,1);
  }finally{close(f);}
});

test('a final compare-and-swap detects cache changes made during validation',async()=>{
  const f=setup();
  try {
    let concurrent;
    f.options.validate=()=>{concurrent=f.read();concurrent.dispatch['101'].remarks='Changed during validation';f.write(concurrent);};
    await assert.rejects(()=>refresh(f.options),/cache changed/);
    assert.deepEqual(f.read(),concurrent);
    assert.deepEqual(fs.readdirSync(f.directory),['orders.json']);
  }finally{close(f);}
});

test('an atomic rename failure preserves the original cache and cleans its temporary file',async()=>{
  const f=setup();
  try {
    const before=f.bytes(),io=Object.create(fs);io.renameSync=()=>{throw Error('Simulated rename failure');};
    await assert.rejects(()=>refresh({...f.options,io}),/rename failure/);
    assert.equal(f.bytes(),before);
    assert.deepEqual(fs.readdirSync(f.directory),['orders.json']);
  }finally{close(f);}
});

test('invalid saved JSON is never replaced with an empty order store',async()=>{
  const f=setup();
  try {fs.writeFileSync(f.filePath,'{\"orders\":');await failsWithoutChange(f,f.options,/cannot be read safely/);assert.equal(f.calls.length,0);}
  finally{close(f);}
});

test('orders wrapper shares the importer mutex, releases it after success and failure, and does not expose an auth route',async()=>{
  const source=fs.readFileSync(path.join(__dirname,'../modules/orders.js'),'utf8');
  const start=source.indexOf('async function refreshAccountingCustomerEvidence('),end=source.indexOf('// ── Read + filter',start);
  assert.ok(start>=0&&end>start);
  let resolveRefresh,calls=0,failNext=false;
  const context={
    ORDERS_PATH:'cache-only.json',SHOPIFY_STORE:'fictional.myshopify.com',shopifyClient:{},
    require:name=>{assert.equal(name,'./shopify-customer-evidence');return{refresh:async options=>{calls++;assert.equal(options.filePath,'cache-only.json');if(failNext)throw Error('Provider unavailable');return new Promise(resolve=>{resolveRefresh=resolve;});}};}
  };
  vm.runInNewContext('let _syncing=false;\n'+source.slice(start,end)+'\nthis.call=refreshAccountingCustomerEvidence;this.running=()=>_syncing;',context);
  const first=context.call({orderIds:['101','102'],actor:'admin',validate:()=>{}});
  assert.equal(context.running(),true);
  await assert.rejects(()=>context.call({}),error=>error.status===409);
  assert.equal(calls,1);
  resolveRefresh({success:true});await first;assert.equal(context.running(),false);
  failNext=true;await assert.rejects(()=>context.call({}),/Provider unavailable/);assert.equal(context.running(),false);
  assert.equal(/router\.(post|get).*refreshAccountingCustomerEvidence/.test(source),false);
});

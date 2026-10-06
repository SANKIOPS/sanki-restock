const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs'), path = require('node:path'), os = require('node:os'), vm = require('node:vm');
const {buildRecoveryPlan, publicRecoveryPlan} = require('../modules/procurement-shopify-recovery');
const key = '6916|green';
const groupKey = line => [line.designCode, line.colour].join('|').toLowerCase();
const photo = {url:'/api/procurement/photo/approved.jpg',alt:'Green trousers'};
const makePo = () => ({id:'PO-0099',status:'posted',postedAt:'2026-09-09',warehouseLocationId:'55',
  lines:[
    {sku:'OLD28',classification:'EXISTING',qty:1,designCode:'6916',colour:'Green',sizeLabel:'28',productType:'Trouser',suggestedMrp:2299,photoUrl:'/raw.jpg'},
    {sku:'NEW30',classification:'NEW',qty:1,designCode:'6916',colour:'Green',sizeLabel:'30',productType:'Trouser',suggestedMrp:2299,photoUrl:'/raw.jpg'},
    {sku:'NEW32',classification:'NEW',qty:2,designCode:'6916',colour:'Green',sizeLabel:'32',productType:'Trouser',suggestedMrp:2299,photoUrl:'/raw.jpg'},
    {sku:'NOT34',classification:'NEW',qty:0,designCode:'6916',colour:'Green',sizeLabel:'34',photoUrl:'/raw.jpg'}],
  newProducts:[{key,seo:{title:'Green trousers',bodyHtml:'<p>Saved description</p>',handle:'green-trousers',tags:['Trouser']},images:[photo],
    variants:[{sku:'NEW30',sizeCode:'30',price:1999},{sku:'NEW32',sizeCode:'32',price:2099}]}],
  aiImages:{[key]:[{...photo,type:'model-front',approved:true,qa:{status:'pass'}}]},
  results:{created:[{productId:'deleted-original'}],adjusted:[{sku:'OLD28',added:1}]}});
const options = mode => ({groupKey,sizes:{},inventoryMode:mode,readPhoto:url => url===photo.url?{buf:Buffer.from('saved-photo')}:null});

test('posted recovery excludes original restocks and nonreceived lines and uses saved prices', () => {
  const po=makePo(),before=structuredClone(po),plan=buildRecoveryPlan(po,{},options('received'));
  assert.deepEqual(plan.products[0].skus,['NEW30','NEW32']);
  assert.deepEqual(plan.products[0].variants.map(v=>v.qty),[1,2]);
  assert.deepEqual(plan.products[0].variants.map(v=>v.price),[1999,2099]);
  assert.deepEqual(plan.excludedRestocks,[{sku:'OLD28',qty:1}]);
  assert.equal(plan.excludedNotReceived,1);
  assert.deepEqual(po,before);
});
test('zero inventory is explicit and does not change received quantities on the PO', () => {
  const plan=buildRecoveryPlan(makePo(),{},options('zero'));
  assert.equal(plan.products[0].receivedQty,3);
  assert.deepEqual(plan.products[0].variants.map(v=>v.qty),[0,0]);
});
test('Unisex recovery excludes old side photos from untyped posted snapshots while preserving saved files',()=>{
  const po=makePo();po.lines.forEach(line=>{line.audience='Unisex';});
  po.aiImages[key]=['front','female','male','model-side-female','model-side-male'].map(type=>({type,url:'/api/procurement/photo/'+type+'.jpg',approved:true,qa:{status:'pass'}}));
  po.newProducts[0].images=po.aiImages[key].map(({url})=>({url}));
  const plan=buildRecoveryPlan(po,{},{...options(),readPhoto:()=>({buf:Buffer.from('saved image')})});
  assert.deepEqual(plan.products[0].images.map(image=>image.url),po.aiImages[key].slice(0,3).map(image=>image.url));
  assert.equal(po.aiImages[key].length,5);
});

test('existing complete products are skipped, partial and duplicate matches are blocked', () => {
  const one={productId:'1'},two={productId:'2'};
  assert.equal(buildRecoveryPlan(makePo(),{NEW30:[one],NEW32:[one]},options()).products[0].status,'existing');
  for(const catalogue of [{NEW30:[one]},{NEW30:[one],NEW32:[two]},{NEW30:[one,one],NEW32:[one]}])
    assert.equal(buildRecoveryPlan(makePo(),catalogue,options()).products[0].status,'blocked');
});
test('unreadable, rejected and original reference photos cannot become recovery attachments', () => {
  for(const change of [
    po=>{po.newProducts[0].images=[{url:'/raw.jpg'}];},
    po=>{po.imageRejectionHistory=[{url:photo.url}];},
    po=>{po.aiImages[key][0].qa.status='needs-review';},
    po=>{po.newProducts[0].images=[{url:'/missing.jpg'}];}
  ]) {
    const po=makePo();change(po);
    const row=buildRecoveryPlan(po,{},options()).products[0];
    assert.equal(row.status,'blocked');assert.equal(row.images.length,0);
  }
});
test('an uncertain earlier create cannot be blindly repeated for absent SKUs', () => {
  const po=makePo();po.shopifyDraftRecoveryHistory=[{status:'needs-reconciliation',currentGroup:key}];
  assert.equal(buildRecoveryPlan(po,{},options()).products[0].status,'blocked');
});
test('bad source data blocks writes and preview changes when photos or stock mode changes', () => {
  assert.throws(()=>buildRecoveryPlan({...makePo(),status:'received'},{},options()));
  assert.throws(()=>buildRecoveryPlan(makePo(),{},options('invalid')));
  const duplicate=makePo();duplicate.lines[2].sku='NEW30';assert.throws(()=>buildRecoveryPlan(duplicate,{},options()));
  const missingSeo=makePo();delete missingSeo.newProducts[0].seo;assert.equal(buildRecoveryPlan(missingSeo,{},options()).products[0].status,'blocked');
  const plan=buildRecoveryPlan(makePo(),{},options('zero'));
  assert.notEqual(plan.fingerprint,buildRecoveryPlan(makePo(),{},options('received')).fingerprint);
  assert.notEqual(plan.fingerprint,buildRecoveryPlan(makePo(),{},{...options('zero'),readPhoto:()=>({buf:Buffer.from('changed')})}).fingerprint);
  const publicPlan=publicRecoveryPlan(plan);
  assert.equal(publicPlan.products[0].photoCount,1);assert.equal(publicPlan.products[0].seo,undefined);
});
test('purchase recovery UI parses, requires a preview, and shows the no-AI and restock exclusions', () => {
  const html=fs.readFileSync(path.join(__dirname,'../public/procurement.html'),'utf8');
  for(const script of html.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/g))new vm.Script(script[1]);
  assert.match(html,/Recover deleted Shopify drafts/);
  assert.match(html,/fingerprint:plan.fingerprint/);
  assert.match(html,/No AI requests are made/);
  assert.match(html,/Include deleted restock variants/);
});

const sandbox=fs.mkdtempSync(path.join(os.tmpdir(),'sanki-draft-recovery-'));
process.env.DATA_PATH=path.join(sandbox,'data.json');
process.env.PROCUREMENT_PATH=path.join(sandbox,'procurement.json');
process.env.SHOPIFY_STORE='test-recovery.myshopify.com';
process.env.SHOPIFY_ACCESS_TOKEN='fake-test-token';
const express=require('express');
const {router}=require('../modules/procurement');
const {shopifyClient}=require('../modules/shopify-client');
fs.writeFileSync(path.join(sandbox,'procurement-photos','approved.jpg'),'saved-photo');
const app=express();app.use(express.json());app.use((req,res,next)=>{req.user={role:req.headers['x-test-role']||'admin',username:'tester'};next();});app.use(router);
let server,base,products=[],writes=[],failCreate=false,failStock=false,holdCreate;
function seed() {
  fs.writeFileSync(process.env.PROCUREMENT_PATH,JSON.stringify({sizes:{},settings:{warehouseLocationId:'55'},pos:{'PO-0099':makePo()}}));
  products=[];writes=[];failCreate=false;failStock=false;holdCreate=null;
}
function response(body,status=200) {return {ok:status<400,status,headers:{get:()=>null},json:async()=>body,text:async()=>JSON.stringify(body)};}
shopifyClient.request=async(url,opts={})=>{
  if(!opts.method)return response({products});
  const payload=JSON.parse(opts.body);writes.push({url,payload,maxRetries:opts.maxRetries});
  if(url.endsWith('/products.json')) {
    if(holdCreate)await holdCreate;
    if(failCreate)throw new Error('Uncertain network failure');
    const product={...payload.product,id:100+products.length,
      variants:payload.product.variants.map((variant,index)=>({...variant,id:200+index,inventory_item_id:300+index}))};
    products.push(product);return response({product});
  }
  if(failStock)return response({errors:'stock failure'},500);
  return response({inventory_level:{available:payload.available}});
};
test.before(async()=>{server=app.listen(0,'127.0.0.1');await new Promise(resolve=>server.once('listening',resolve));base='http://127.0.0.1:'+server.address().port+'/api/procurement/pos/PO-0099/shopify-recovery';});
test.after(()=>{server.closeAllConnections();server.close();if(sandbox.startsWith(path.join(os.tmpdir(),'sanki-draft-recovery-')))fs.rmSync(sandbox,{recursive:true,force:true});});
const preview=async(mode='received')=>(await fetch(base+'?inventoryMode='+mode)).json();
const commit=async(plan,body={})=>fetch(base,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({approve:true,fingerprint:plan.fingerprint,inventoryMode:plan.inventoryMode,includeRestocks:plan.includeRestocks,priceMode:plan.priceMode,...body})});

test('recovery endpoint enforces authorization and rejects a missing or stale approval',async()=>{
  seed();assert.equal((await fetch(base,{headers:{'x-test-role':'viewer'}})).status,403);
  const {plan}=await preview();assert.equal((await commit(plan,{approve:false})).status,400);
  products.push({id:7,status:'draft',variants:[{sku:'NEW30'}]});assert.equal((await commit(plan)).status,409);assert.equal(writes.length,0);
});
test('endpoint restores only missing drafts, saved photos and quantities; re-running never adjusts stock twice',async()=>{
  seed();const before=makePo();const {plan}=await preview();assert.equal(writes.length,0);
  const result=await (await commit(plan)).json();assert.equal(result.success,true);assert.equal(result.created.length,1);
  const create=writes.find(write=>write.url.endsWith('/products.json'));
  assert.equal(create.maxRetries,0);assert.equal(create.payload.product.status,'draft');
  assert.deepEqual(create.payload.product.variants.map(v=>v.sku),['NEW30','NEW32']);
  assert.equal(create.payload.product.images[0].attachment,Buffer.from('saved-photo').toString('base64'));
  assert.deepEqual(writes.filter(write=>write.url.endsWith('/inventory_levels/set.json')).map(write=>write.payload.available),[1,2]);
  assert.ok(writes.every(write=>!write.url.endsWith('/inventory_levels/adjust.json')));
  const saved=JSON.parse(fs.readFileSync(process.env.PROCUREMENT_PATH)).pos['PO-0099'];
  before.lines.forEach((line,index)=>assert.deepEqual(Object.fromEntries(Object.keys(line).map(field=>[field,saved.lines[index][field]])),line));
  assert.deepEqual(saved.results,before.results);assert.equal(saved.status,'posted');assert.equal(saved.postedAt,before.postedAt);
  assert.equal(saved.shopifyRecoveryLinks[key].productId,'100');assert.equal(saved.shopifyDraftRecoveryHistory[0].status,'complete');
  const count=writes.length;const again=await preview();assert.equal(again.plan.products[0].status,'existing');
  assert.equal((await (await commit(again.plan)).json()).created.length,0);assert.equal(writes.length,count);
});
test('zero-stock mode sets zero only on the recreated variants',async()=>{
  seed();const {plan}=await preview('zero');assert.equal((await (await commit(plan)).json()).success,true);
  assert.deepEqual(writes.filter(write=>write.url.endsWith('/inventory_levels/set.json')).map(write=>write.payload.available),[0,0]);
});
test('uncertain create is recorded and later recovery cannot repeat it blindly',async()=>{
  seed();const {plan}=await preview();failCreate=true;assert.equal((await commit(plan)).status,409);
  const again=await preview();assert.equal(again.plan.products[0].status,'blocked');
  assert.equal(JSON.parse(fs.readFileSync(process.env.PROCUREMENT_PATH)).pos['PO-0099'].shopifyDraftRecoveryHistory[0].status,'needs-reconciliation');
});
test('stock setup failure retains the created product link and never retries an original stock adjustment',async()=>{
  seed();const {plan}=await preview();failStock=true;const result=await (await commit(plan)).json();
  assert.equal(result.success,false);assert.equal(result.created.length,1);
  const saved=JSON.parse(fs.readFileSync(process.env.PROCUREMENT_PATH)).pos['PO-0099'];
  assert.equal(saved.shopifyRecoveryLinks[key].productId,'100');assert.equal(saved.shopifyDraftRecoveryHistory[0].status,'needs-reconciliation');
  assert.equal((await preview()).plan.products[0].status,'existing');
});
test('concurrent recoveries for the same PO are rejected before a second create',async()=>{
  seed();const {plan}=await preview();let release;holdCreate=new Promise(resolve=>release=resolve);
  const first=commit(plan);
  while(!writes.length)await new Promise(resolve=>setTimeout(resolve,5));
  assert.equal((await commit(plan)).status,409);release();assert.equal((await first).status,200);
  assert.equal(writes.filter(write=>write.url.endsWith('/products.json')).length,1);
});


test('explicit recovery includes a deleted restock variant and preserves any surviving restock stock', () => {
  const po=makePo(), opts={...options('received'),includeRestocks:true};
  const missing=buildRecoveryPlan(po,{},opts);
  assert.equal(missing.products[0].status,'ready');
  assert.deepEqual(missing.products[0].variants.map(v=>[v.sku,v.qty]),[['OLD28',1],['NEW30',1],['NEW32',2]]);
  assert.deepEqual(missing.excludedRestocks,[]);
  const surviving=buildRecoveryPlan(po,{OLD28:[{productId:'old'}]},opts);
  assert.equal(surviving.products[0].status,'ready');
  assert.deepEqual(surviving.products[0].skus,['NEW30','NEW32']);
  assert.deepEqual(surviving.excludedRestocks,[{sku:'OLD28',qty:1}]);
  assert.equal(buildRecoveryPlan(po,{OLD28:[{productId:'1'},{productId:'2'}]},opts).products[0].status,'blocked');
  assert.notEqual(missing.fingerprint,buildRecoveryPlan(po,{},options('received')).fingerprint);
});

test('endpoint restores explicitly selected deleted restock stock without an adjustment',async()=>{
  seed();const {plan}=await (await fetch(base+'?inventoryMode=received&includeRestocks=true')).json();
  assert.equal(plan.includeRestocks,true);
  const result=await (await commit(plan)).json();assert.equal(result.success,true);
  assert.deepEqual(writes.find(w=>w.url.endsWith('/products.json')).payload.product.variants.map(v=>v.sku),['OLD28','NEW30','NEW32']);
  assert.deepEqual(writes.filter(w=>w.url.endsWith('/inventory_levels/set.json')).map(w=>w.payload.available),[1,1,2]);
  assert.ok(writes.every(w=>!w.url.endsWith('/inventory_levels/adjust.json')));
});


test('missing historical prices require an explicit choice and saved prices always win',()=>{
  const po=makePo();delete po.newProducts[0].variants[0].price;delete po.lines[1].suggestedMrp;
  assert.equal(buildRecoveryPlan(po,{},options('received')).products[0].status,'blocked');
  const calc=buildRecoveryPlan(po,{},{...options('received'),priceMode:'calculated',calculatePrice:()=>2499});
  assert.equal(calc.products[0].status,'ready');assert.deepEqual(calc.products[0].variants.map(v=>v.price),[2499,2099]);
  assert.deepEqual(calc.products[0].variants.map(v=>v.priceSource),['calculated','saved']);
  const zero=buildRecoveryPlan(po,{},{...options('received'),priceMode:'zero'});
  assert.equal(zero.products[0].status,'ready');assert.deepEqual(zero.products[0].variants.map(v=>v.price),[0,2099]);
  assert.notEqual(calc.fingerprint,zero.fingerprint);
  assert.equal(buildRecoveryPlan(po,{},{...options(),priceMode:'calculated',calculatePrice:()=>NaN}).products[0].status,'blocked');
});

test('explicit zero draft prices reach Shopify and remain preview-locked',async()=>{
  seed();const po=makePo();po.newProducts[0].variants.forEach(v=>delete v.price);po.lines.forEach(l=>delete l.suggestedMrp);
  fs.writeFileSync(process.env.PROCUREMENT_PATH,JSON.stringify({sizes:{},settings:{warehouseLocationId:'55'},pos:{'PO-0099':po}}));
  const {plan}=await(await fetch(base+'?inventoryMode=received&priceMode=zero')).json();
  assert.equal(plan.products[0].status,'ready');assert.equal((await commit(plan,{priceMode:'saved'})).status,409);assert.equal(writes.length,0);
  assert.equal((await(await commit(plan)).json()).success,true);
  assert.deepEqual(writes.find(w=>w.url.endsWith('/products.json')).payload.product.variants.map(v=>v.price),['0','0']);
});

test('background recovery responds before creation finishes and exposes durable progress without Shopify reads',async()=>{
  seed();const {plan}=await preview();let release;holdCreate=new Promise(resolve=>release=resolve);
  let readCount=0;const request=shopifyClient.request;
  shopifyClient.request=async(url,opts)=>{if(!opts||!opts.method)readCount++;return request(url,opts);};
  try{
    const accepted=await commit(plan,{background:true});assert.equal(accepted.status,202);
    const {runId}=await accepted.json();assert.ok(runId);
    while(!writes.length)await new Promise(resolve=>setTimeout(resolve,5));
    const before=readCount,progress=await(await fetch(base+'/status')).json();
    assert.equal(readCount,before);assert.equal(progress.active,true);
    assert.equal(progress.history[0].id,runId);assert.equal(progress.history[0].total,1);
    assert.equal(progress.history[0].currentGroup,key);assert.equal(progress.history[0].phase,'creating');
    assert.equal((await commit(plan,{background:true})).status,409);
    release();
    let finished;
    for(let tries=0;tries<100;tries++){
      finished=await(await fetch(base+'/status')).json();if(!finished.active)break;
      await new Promise(resolve=>setTimeout(resolve,5));
    }
    assert.equal(finished.active,false);assert.equal(finished.history[0].status,'complete');
    assert.deepEqual(finished.history[0].completed,[key]);assert.equal(finished.history[0].pieces,3);
    assert.equal(writes.filter(write=>write.url.endsWith('/products.json')).length,1);
  }finally{release();shopifyClient.request=request;}
});

test('restart marks a stranded run and blocks uncertain creation while leaving other missing products recoverable',async()=>{
  seed();const store=JSON.parse(fs.readFileSync(process.env.PROCUREMENT_PATH));
  store.pos['PO-0099'].shopifyDraftRecoveryHistory=[{id:'interrupted',status:'running',currentGroup:key,created:[],errors:[]}];
  fs.writeFileSync(process.env.PROCUREMENT_PATH,JSON.stringify(store));
  assert.equal((await fetch(base+'/status',{headers:{'x-test-role':'viewer'}})).status,403);
  const progress=await(await fetch(base+'/status')).json();assert.equal(progress.active,false);
  assert.equal(progress.history[0].status,'needs-reconciliation');assert.match(progress.history[0].errors[0],/service restart/);
  assert.equal((await preview()).plan.products[0].status,'blocked');assert.equal(writes.length,0);
  delete store.pos['PO-0099'].shopifyDraftRecoveryHistory[0].currentGroup;
  fs.writeFileSync(process.env.PROCUREMENT_PATH,JSON.stringify(store));
  assert.equal((await preview()).plan.products[0].status,'ready');
});

test('background creation failure remains available after the original request ends and is never repeated',async()=>{
  seed();const {plan}=await preview();failCreate=true;
  assert.equal((await commit(plan,{background:true})).status,202);
  let progress;
  for(let tries=0;tries<100;tries++){
    progress=await(await fetch(base+'/status')).json();if(!progress.active)break;
    await new Promise(resolve=>setTimeout(resolve,5));
  }
  assert.equal(progress.active,false);assert.equal(progress.history[0].status,'needs-reconciliation');
  assert.match(progress.history[0].errors[0],/Uncertain network failure/);
  assert.equal((await preview()).plan.products[0].status,'blocked');
  assert.equal(writes.filter(write=>write.url.endsWith('/products.json')).length,1);
});

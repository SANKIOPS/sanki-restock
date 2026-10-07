const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs'), path = require('node:path'), os = require('node:os');
const listingPhoto=require('./listing-photo-fixture');
const {fingerprint} = require('../modules/procurement-codex-batch');
const sandbox = fs.mkdtempSync(path.join(os.tmpdir(), 'sanki-resume-posting-'));
process.env.DATA_PATH = path.join(sandbox, 'data.json');
process.env.PROCUREMENT_PATH = path.join(sandbox, 'procurement.json');
process.env.SHOPIFY_STORE = 'test-resume.myshopify.com';
process.env.SHOPIFY_ACCESS_TOKEN = 'test-only-token';
const express = require('express');
const {router} = require('../modules/procurement');
const {shopifyClient} = require('../modules/shopify-client');
const app = express(); app.use(express.json());
app.use((req,res,next) => {req.user = {role:req.headers['x-test-role'] || 'admin',username:'tester'};next();});
app.use(router);
const key = 'trouser 1|grey';
const photoUrl = '/api/procurement/photo/reference.jpg';
const source = () => ({key,colour:'Grey',productType:'Trouser',designName:'Trouser 1',designCode:'',audience:'Women',line:'casuals',season:'',fit:'Baggy Fit',sizeLabels:['Waist 28','Waist 30'],photoUrl});
const makePo = () => ({id:'PO-0012',status:'posting_partial',vendor:'TEST 1',line:'casuals',warehouseLocationId:'55',
  lines:[{sku:'SA111XLZ9028',classification:'NEW',qty:3,designName:'Trouser 1',productType:'Trouser',colour:'Black',sizeLabel:'Waist 28',fit:'Baggy Fit',audience:'Women',perPcsYuan:51,weightGrams:300,photoUrl},
    ...['Waist 28','Waist 30'].map((sizeLabel,index)=>({sku:index?'SA116XLZ9230':'SA116XLZ9128',classification:'NEW',qty:index?2:1,designName:'Trouser 1',designCode:'',productType:'Trouser',colour:'Grey',sizeLabel,fit:'Baggy Fit',audience:'Women',perPcsYuan:51,weightGrams:300,photoUrl}))],
  aiImages:{[key]:['front','model-front','model-side'].map(type=>({type,url:'/api/procurement/photo/'+type+'.jpg',approved:true,qa:{status:'pass',productOnlyVerified:true},sourceFingerprint:fingerprint(source())}))},
  seoDraft:[{key,seoApproved:true,seo:{displayName:'Wide-Leg',title:'Grey Wide-Leg Trousers for Women',handle:'grey-wide-leg',metaTitle:'Grey Wide-Leg Trousers',metaDescription:'Grey trousers with a relaxed silhouette.',imageAlt:'Grey wide-leg trousers',bodyHtml:'<p>Grey trousers with a relaxed silhouette.</p>',tags:['Grey','Trousers','Women']}}],
  results:{created:[{productId:'7',title:'Already created',variants:[{sku:'SA111XLZ9028',qty:3}]}],adjusted:[],errors:[]}});
let products,writes,failCreate,failStock,holdCreate,server,base;
function seed(change) {
  const po=makePo();if(change)change(po);
  fs.writeFileSync(process.env.PROCUREMENT_PATH,JSON.stringify({settings:{warehouseLocationId:'55'},pos:{[po.id]:po}}));
  products=[{id:7,status:'draft',variants:[{id:70,sku:'SA111XLZ9028',inventory_item_id:700}]}];
  writes=[];failCreate=false;failStock=false;holdCreate=null;
  for(const type of ['front','model-front','model-side'])fs.writeFileSync(path.join(sandbox,'procurement-photos',type+'.jpg'),listingPhoto);
}
function response(body,status=200){return {ok:status<400,status,headers:{get:()=>null},json:async()=>body,text:async()=>JSON.stringify(body)};}
shopifyClient.request=async(url,options={})=>{
  if(!options.method)return response({products});
  const payload=JSON.parse(options.body);writes.push({url,payload,maxRetries:options.maxRetries});
  if(url.endsWith('/products.json')) {
    if(holdCreate)await holdCreate;
    if(failCreate)throw new Error('Uncertain creation response');
    const product={...payload.product,id:8,variants:payload.product.variants.map((v,index)=>({...v,id:80+index,inventory_item_id:800+index}))};
    products.push(product);return response({product});
  }
  if(failStock)return response({errors:'stock failed'},500);
  return response({inventory_level:{available:payload.available}});
};
test.before(async()=>{server=app.listen(0,'127.0.0.1');await new Promise(resolve=>server.once('listening',resolve));base='http://127.0.0.1:'+server.address().port;});
test.after(()=>{server.closeAllConnections();server.close();fs.rmSync(sandbox,{recursive:true,force:true});});
const resume = () => fetch(base+'/api/procurement/pos/PO-0012/resume-posting',{method:'POST',headers:{'Content-Type':'application/json'},body:'{}'});
const saved = () => JSON.parse(fs.readFileSync(process.env.PROCUREMENT_PATH)).pos['PO-0012'];

test('continuation accepts the original studio approval and creates only the missing colour',async()=>{
  seed();const before=makePo();
  const result=await(await resume()).json();assert.equal(result.success,true,JSON.stringify(result));
  assert.deepEqual(result.created,[key]);assert.equal(result.alreadyPresent.length,1);
  const create=writes.filter(w=>w.url.endsWith('/products.json'));assert.equal(create.length,1);assert.equal(create[0].maxRetries,0);
  assert.equal(create[0].payload.product.status,'draft');assert.equal(create[0].payload.product.images.length,3);
  assert.deepEqual(create[0].payload.product.variants.map(v=>v.sku),['SA116XLZ9128','SA116XLZ9230']);
  assert.deepEqual(writes.filter(w=>w.url.endsWith('/inventory_levels/set.json')).map(w=>[w.payload.inventory_item_id,w.payload.available]),[[800,1],[801,2]]);
  assert.ok(writes.every(w=>!w.url.endsWith('/inventory_levels/adjust.json')));
  assert.equal(saved().status,'posted');assert.equal(saved().results.created[0].productId,'7');
  assert.deepEqual(saved().aiImages,before.aiImages);assert.equal(saved().newProducts[0].images.length,3);
  assert.ok(saved().newProducts[0].variants.every(v=>v.price>0));
  const count=writes.length;assert.equal((await resume()).status,409);assert.equal(writes.length,count);
});

test('real reference, audience, line, season, fit and size changes still block approved images',async()=>{
  for(const change of [po=>po.lines[1].photoUrl='/changed.jpg',po=>po.lines[1].audience='Men',po=>po.line='funky',po=>po.season='Winter',po=>po.lines[1].fit='Fitted',po=>po.lines[2].sizeLabel='Waist 32']) {
    seed(change);const result=await(await resume()).json();assert.equal(result.success,false);assert.match(result.error,/no longer match/);assert.equal(writes.length,0);
  }
});

test('unapproved, rejected and unreadable images are not silently accepted',async()=>{
  for(const change of [po=>po.aiImages[key].forEach(im=>im.approved=false),po=>po.aiImages[key].forEach(im=>im.qa.status='needs-review'),po=>po.aiImages[key].forEach(im=>im.url='/missing.jpg')]) {
    seed(change);assert.equal((await resume()).status,409);assert.equal(writes.length,0);
  }
});

test('uncertain continuation creation is journaled and never blindly retried',async()=>{
  seed();failCreate=true;assert.equal((await resume()).status,409);assert.equal(saved().results.errors.length,1);assert.equal(saved().newProducts[0].images.length,3);
  assert.equal(writes.length,1);assert.equal(writes[0].maxRetries,0);failCreate=false;
  assert.equal((await resume()).status,409);assert.equal(writes.length,1);
});

test('a created draft ID survives stock failure and prevents duplicate creation',async()=>{
  seed();failStock=true;assert.equal((await resume()).status,409);
  assert.equal(saved().results.created.length,2);assert.equal(saved().results.created[1].productId,'8');
  assert.equal(saved().status,'posting_partial');const count=writes.length;
  assert.equal((await resume()).status,409);assert.equal(writes.length,count);
});

test('concurrent continuation is rejected while the first request completes',async()=>{
  seed();let release;holdCreate=new Promise(resolve=>{release=resolve;});
  const first=resume();for(let tries=0;tries<100&&!writes.length;tries++)await new Promise(resolve=>setTimeout(resolve,5));
  assert.equal(writes.length,1);assert.equal((await resume()).status,409);
  release();assert.equal((await first).status,200);assert.equal(writes.filter(w=>w.url.endsWith('/products.json')).length,1);
});

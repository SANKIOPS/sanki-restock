const test = require('node:test'), assert = require('node:assert/strict');
const fs = require('node:fs'), path = require('node:path'), os = require('node:os');
const sandbox = fs.mkdtempSync(path.join(os.tmpdir(), 'sanki-approval-safety-'));
process.env.DATA_PATH = path.join(sandbox, 'data.json');
process.env.PROCUREMENT_PATH = path.join(sandbox, 'procurement.json');
process.env.SHOPIFY_STORE = 'approval-safety.myshopify.com';
process.env.SHOPIFY_ACCESS_TOKEN = 'test-only'; process.env.OPENAI_API_KEY = 'test-only';
const express = require('express'), sharp = require('sharp');
const {router, genSeo} = require('../modules/procurement');
const pilot = require('../modules/procurement-openai-pilot');
const {fingerprint} = require('../modules/procurement-codex-batch');
const sourceNormalizer = require('../modules/procurement-image-source');
const {shopifyClient} = require('../modules/shopify-client');
const app = express(); app.use(express.json());
app.use((req,res,next)=>{req.user={role:'admin',username:'tester'};next();}); app.use(router);
let server, base, jpeg, serial=0, products=[], writes=[], catalogueGate, catalogueStarted;
const response = body => ({ok:true,status:200,headers:{get:()=>null},json:async()=>body,text:async()=>JSON.stringify(body)});
shopifyClient.request = async (url, options={}) => {
  if (!options.method || options.method==='GET') {
    if (catalogueStarted) {const started=catalogueStarted;catalogueStarted=null;started();}
    if (catalogueGate) await catalogueGate;
    return response({products});
  }
  const payload=JSON.parse(options.body); writes.push({url,payload});
  if (url.endsWith('/products.json')) {
    const product={...payload.product,id:100+products.length,
      variants:payload.product.variants.map((variant,index)=>({...variant,id:200+index,inventory_item_id:300+index}))};
    products.push(product); return response({product});
  }
  return response({inventory_level:{available:payload.available}});
};
test.before(async()=>{
  jpeg=await sharp({create:{width:8,height:12,channels:3,background:'#665544'}}).jpeg().toBuffer();
  server=app.listen(0,'127.0.0.1'); await new Promise(resolve=>server.once('listening',resolve));
  base='http://127.0.0.1:'+server.address().port;
});
test.after(()=>{server.closeAllConnections();server.close();fs.rmSync(sandbox,{recursive:true,force:true});});
const saved = () => JSON.parse(fs.readFileSync(process.env.PROCUREMENT_PATH)).pos['PO-SAFE'];
const save = po => fs.writeFileSync(process.env.PROCUREMENT_PATH,JSON.stringify({settings:{warehouseLocationId:'55'},pos:{[po.id]:po}}));
async function post(route, body={}) {
  const r=await fetch(base+route,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
  return {status:r.status,...await r.json()};
}
const approve = body => post('/api/procurement/pos/PO-SAFE/approve-po',body);
const commit = () => post('/api/procurement/commit',{poId:'PO-SAFE',approve:true});
const resume = () => post('/api/procurement/pos/PO-SAFE/resume-posting');
const recovery = plan => post('/api/procurement/pos/PO-SAFE/shopify-recovery',
  {approve:true,fingerprint:plan.fingerprint,inventoryMode:plan.inventoryMode,priceMode:plan.priceMode});
async function recoveryPreview() {return (await (await fetch(base+'/api/procurement/pos/PO-SAFE/shopify-recovery?inventoryMode=received')).json()).plan;}
function seed({approved=false,status='received'}={}) {
  serial++; products=[]; writes=[]; catalogueGate=null; catalogueStarted=null;
  const po={id:'PO-SAFE',status,vendor:'TEST',line:'casuals',warehouseLocationId:'55',lines:[],aiImages:{},imageStyling:{},seoDraft:[],newProducts:[],results:{created:[],adjusted:[],errors:[]}};
  for (const [i,colour] of ['White','Black'].entries()) {
    const key='d'+serial+'|'+colour.toLowerCase(), photoUrl='/api/procurement/photo/source-'+serial+'-'+i+'.jpg';
    const group={key,colour,productType:'Shirt',designName:'Cotton',designCode:'D'+serial,audience:'Women',line:'casuals',season:'',fit:'Regular Fit',sizeLabels:['FS'],photoUrl};
    const line={sku:'SA1'+(i+1)+'Z'+(serial*2+i)+'FS',classification:'NEW',qty:2,designName:'Cotton',designCode:group.designCode,productType:'Shirt',colour,sizeLabel:'FS',audience:'Women',fit:'Regular Fit',perPcsYuan:55,weightGrams:200,photoUrl};
    po.lines.push(line); po.imageStyling[key]=pilot.normalizeStyling({},group);
    po.aiImages[key]=pilot.pilotTypes(group).map(type=>({type,url:'/api/procurement/photo/'+serial+'-'+i+'-'+type+'.jpg',approved,source:'openai-pilot',sourceFingerprint:fingerprint(group),styling:po.imageStyling[key],qa:{status:'pass',productOnlyVerified:true}}));
    po.seoDraft.push({key,seo:genSeo(group),seoApproved:approved});
    po.newProducts.push({key,seo:genSeo(group),images:po.aiImages[key].map(({url})=>({url})),variants:[{sku:line.sku,sizeCode:'FS',price:1599}]});
    fs.writeFileSync(path.join(sandbox,'procurement-photos',path.basename(photoUrl)),jpeg);
    for (const image of po.aiImages[key]) fs.writeFileSync(path.join(sandbox,'procurement-photos',path.basename(image.url)),jpeg);
  }
  save(po); return po;
}
function corrupt(po, bytes, groupIndex=1, type='front') {
  const key=Object.keys(po.aiImages)[groupIndex], image=po.aiImages[key].find(candidate=>candidate.type===type);
  fs.writeFileSync(path.join(sandbox,'procurement-photos',path.basename(image.url)),bytes);
}
function running(po) {
  po.openaiPilot={attempts:[{id:'external-job',groupKey:Object.keys(po.aiImages)[0],status:'running',startedAt:new Date().toISOString()}]};save(po);
}
function unapproved(po) {assert.ok(Object.values(po.aiImages).flat().every(image=>!image.approved));assert.ok(po.seoDraft.every(draft=>!draft.seoApproved));}
function holdCatalogue() {
  let release, started;
  catalogueGate=new Promise(resolve=>release=resolve);const loading=new Promise(resolve=>started=resolve);catalogueStarted=started;
  return {loading,release:()=>{catalogueGate=null;release();}};
}
async function activeGeneration(po) {
  const groupKey=Object.keys(po.aiImages)[0]; po.openaiPilot={attempts:[{id:'prior',groupKey,status:'drafts-ready'}]};save(po);
  let release,started;const held=new Promise(resolve=>release=resolve),loading=new Promise(resolve=>started=resolve);
  const original=pilot.preflightFit;
  pilot.preflightFit=async()=>{started();await held;throw new Error('Mock cancellation: no provider call');};
  const accepted=await post('/api/procurement/pos/PO-SAFE/openai-pilot',{groupKey,retry:true,regenerateTypes:['front'],skipSeo:true,maxImageAttempts:2,generationVersion:3,generationEpoch:0,styling:po.imageStyling[groupKey]});
  assert.equal(accepted.status,202,JSON.stringify(accepted));await loading;
  return async()=>{release();for(let n=0;n<100&&saved().openaiPilot.attempts.some(attempt=>attempt.status==='running');n++)await new Promise(resolve=>setTimeout(resolve,5));pilot.preflightFit=original;assert.ok(saved().openaiPilot.attempts.every(attempt=>attempt.status!=='running'));};
}

test('bulk approval decodes every listing image and rejects empty, corrupt and truncated files atomically',async()=>{
  for (const bytes of [Buffer.alloc(0),Buffer.from('not-an-image'),jpeg.subarray(0,100)]) {
    const po=seed(); corrupt(po,bytes);const before=saved();
    const result=await approve({seoDrafts:[{groupKey:po.seoDraft[0].key,seo:{metaDescription:'Reviewed edit must remain unsaved on failure.'}}]});
    assert.equal(result.status,409,JSON.stringify(result));assert.match(result.error,/damaged|decoded/);
    assert.deepEqual(saved().seoDraft,before.seoDraft);unapproved(saved());assert.equal(writes.length,0);
  }
});
test('single-product approval also rejects damaged images',async()=>{
  const po=seed();corrupt(po,Buffer.alloc(0),0);
  const result=await post('/api/procurement/pos/PO-SAFE/approve-product',{groupKey:po.seoDraft[0].key});
  assert.equal(result.status,409);unapproved(saved());assert.equal(writes.length,0);
});
test('valid listing AVIF bytes are accepted and converted for Shopify without changing the saved file',async()=>{
  const po=seed(),image=po.aiImages[Object.keys(po.aiImages)[0]][0];
  const avif=await sharp(jpeg).avif().toBuffer(),name=path.join(sandbox,'procurement-photos',path.basename(image.url));
  fs.writeFileSync(name,avif); assert.equal((await approve()).status,200);
  assert.equal((await commit()).status,200);assert.deepEqual(fs.readFileSync(name),avif);
  assert.equal((await sharp(Buffer.from(writes[0].payload.product.images[0].attachment,'base64')).metadata()).format,'png');
});
test('commit checks all colours before creating anything when a previously approved file becomes corrupt',async()=>{
  const po=seed({approved:true});corrupt(po,Buffer.from('damaged-after-approval'));
  const result=await commit();assert.equal(result.status,409);assert.match(result.error,/damaged/);assert.equal(writes.length,0);assert.equal(saved().status,'received');
});
test('continuation decodes pending photos before any Shopify write',async()=>{
  const po=seed({approved:true,status:'posting_partial'});corrupt(po,Buffer.alloc(0));
  const result=await resume();assert.equal(result.status,409);assert.equal(writes.length,0);assert.equal(saved().status,'posting_partial');
});
test('recovery preflights every ready product before recreating the first draft',async()=>{
  const po=seed({approved:true,status:'posted'});corrupt(po,Buffer.from('corrupt-recovery-photo'));
  const plan=await recoveryPreview();const result=await recovery(plan);
  assert.equal(result.status,409);assert.match(result.error,/damaged/);assert.equal(writes.length,0);assert.equal(saved().shopifyDraftRecoveryHistory,undefined);
});
test('active regeneration blocks both approval and Shopify posting without calling a provider',async()=>{
  const po=seed({approved:true}),release=await activeGeneration(po);
  try {assert.equal((await approve()).status,409);assert.equal((await commit()).status,409);assert.equal(writes.length,0);assert.equal(saved().status,'received');}
  finally {await release();}
});
test('saved running generation blocks continuation and draft recovery',async()=>{
  for (const status of ['posting_partial','posted']) {
    const po=seed({approved:true,status});const plan=status==='posted'?await recoveryPreview():null;running(po);
    const result=status==='posted'?await recovery(plan):await resume();
    assert.equal(result.status,409);assert.match(result.error,/image generation/);assert.equal(writes.length,0);
  }
});
test('generation cannot start while posting is awaiting its catalogue check',async()=>{
  const po=seed({approved:true}),gate=holdCatalogue();const posting=commit();await gate.loading;
  const generation=await post('/api/procurement/pos/PO-SAFE/openai-pilot',{groupKey:po.seoDraft[0].key});
  assert.equal(generation.status,409);assert.match(generation.error,/Shopify posting/);gate.release();
  assert.equal((await posting).status,200);assert.equal(saved().openaiPilot,undefined);
});
test('posting rechecks job state after the awaited catalogue read before reserving or writing',async()=>{
  seed({approved:true});const gate=holdCatalogue(),posting=commit();await gate.loading;running(saved());gate.release();
  const result=await posting;assert.equal(result.status,409);assert.match(result.error,/image generation/);assert.equal(writes.length,0);assert.equal(saved().status,'received');
});
test('recovery rechecks a newly running image job after its catalogue read',async()=>{
  seed({approved:true,status:'posted'});const plan=await recoveryPreview(),gate=holdCatalogue();
  const recovering=recovery(plan);await gate.loading;running(saved());gate.release();
  const result=await recovering;assert.equal(result.status,409);assert.match(result.error,/image generation/);assert.equal(writes.length,0);assert.equal(saved().shopifyDraftRecoveryHistory,undefined);
});
test('approval rechecks the saved purchase after asynchronous image decoding',async()=>{
  seed();let release,started;const gate=new Promise(resolve=>release=resolve),loading=new Promise(resolve=>started=resolve);
  const original=sourceNormalizer.normalizeSource;let first=true;
  sourceNormalizer.normalizeSource=async source=>{if(first){first=false;started();await gate;}return original(source);};
  try {
    const approving=approve();await loading;const po=saved();po.lines[0].qty=3;save(po);release();
    const result=await approving;assert.equal(result.status,409);assert.match(result.error,/changed/);assert.equal(saved().lines[0].qty,3);unapproved(saved());
  } finally {sourceNormalizer.normalizeSource=original;}
});
test('recovery checks the latest listing after attachment decoding before recording an uncertain create',async()=>{
  seed({approved:true,status:'posted'});const plan=await recoveryPreview();let release,started;
  const gate=new Promise(resolve=>release=resolve),loading=new Promise(resolve=>started=resolve),original=sourceNormalizer.normalizeSource;
  let decodes=0;
  // Six whole-plan checks, three first-product checks, then attachment decoding.
  sourceNormalizer.normalizeSource=async source=>{if(++decodes===10){started();await gate;}return original(source);};
  try {
    const recovering=recovery(plan);await loading;const po=saved();po.newProducts[0].seo.title='Changed reviewed listing title';save(po);release();
    const result=await recovering;assert.equal(result.status,409);assert.match(result.error,/changed/);assert.equal(writes.length,0);
    const latest=saved();assert.equal(latest.newProducts[0].seo.title,'Changed reviewed listing title');assert.equal(latest.shopifyDraftRecoveryHistory[0].currentGroup,undefined);
  } finally {sourceNormalizer.normalizeSource=original;}
});
test('the historical missing flat-front fallback still posts only readable approved model views',async()=>{
  const po=seed({approved:true});
  for (const images of Object.values(po.aiImages)) fs.rmSync(path.join(sandbox,'procurement-photos',path.basename(images[0].url)));
  assert.equal((await commit()).status,200);
  assert.ok(writes.filter(write=>write.url.endsWith('/products.json')).every(write=>write.payload.product.images.length===2));
  assert.equal(saved().imageRecoveryHistory.length,2);
});

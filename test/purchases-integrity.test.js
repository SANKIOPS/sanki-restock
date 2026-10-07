const test=require('node:test'), assert=require('node:assert/strict');
const fs=require('node:fs'),path=require('node:path'),os=require('node:os'),vm=require('node:vm');
const sandbox=fs.mkdtempSync(path.join(os.tmpdir(),'sanki-purchases-integrity-'));
process.env.DATA_PATH=path.join(sandbox,'data.json');
process.env.PROCUREMENT_PATH=path.join(sandbox,'procurement.json');
process.env.SIZE_TRACKER_PATH=path.join(sandbox,'size-tracker.json');
process.env.CASUALS_PATH=path.join(sandbox,'casuals.json');
process.env.SHOPIFY_STORE='synthetic-only.myshopify.com';process.env.SHOPIFY_ACCESS_TOKEN='mock-only';
process.env.OPENAI_API_KEY='mock-only';process.env.ANTHROPIC_API_KEY='';
const express=require('express'), procurement=require('../modules/procurement'),sizes=require('../modules/size-tracker');
const casuals=require('../modules/casuals'),pilot=require('../modules/procurement-openai-pilot');
const profile=require('../public/purchase-product-profile'),costs=require('../public/purchase-costs');
const {shopifyClient}=require('../modules/shopify-client');
const app=express();app.use(express.json());app.use((req,res,next)=>{req.user={role:req.headers['x-role']||'admin',username:'test'};next();});
app.use(procurement.router);app.use(sizes.router);app.use(casuals.router);
let server,base,products=[],writes=[],readGate,readStarted,writeGate,writeStarted,failAdjust=false;
const response=data=>({ok:true,status:200,headers:{get:()=>null},json:async()=>data,text:async()=>JSON.stringify(data)});
shopifyClient.request=async(url,opt={})=>{
  if(!opt.method||opt.method==='GET'){
    if(readStarted){readStarted();readStarted=null;}if(readGate)await readGate;
    if(url.includes('/products.json'))return response({products});
    throw new Error('Unexpected external read blocked: '+url);
  }
  const payload=JSON.parse(opt.body);writes.push({url,payload,retries:opt.maxRetries});
  if(writeStarted){writeStarted();writeStarted=null;}if(writeGate)await writeGate;
  if(url.endsWith('/products.json')){
    const id=100+products.length,product={...payload.product,id,handle:'synthetic-'+id,variants:payload.product.variants.map((v,index)=>({...v,id:id*100+index,inventory_item_id:id*1000+index}))};
    products.push(product);return response({product});
  }
  if(url.endsWith('/inventory_levels/adjust.json')){
    if(failAdjust)throw new Error('Synthetic timeout after an uncertain adjustment');
    return response({inventory_level:{available:payload.available_adjustment}});
  }
  if(url.endsWith('/inventory_levels/set.json'))return response({inventory_level:{available:payload.available}});
  throw new Error('Unexpected external write blocked: '+url);
};
const realFetch=global.fetch;
global.fetch=(url,opt)=>{if(!String(url).startsWith('http://127.0.0.1:'))throw new Error('Real external API calls are forbidden in this test');return realFetch(url,opt);};
pilot.generateImage=async({type})=>({buffer:Buffer.from('synthetic-'+type),model:'mock'});
pilot.verifyImage=async()=>({status:'pass',productOnlyVerified:true,failed:[],uncertain:[],issues:[]});
pilot.preflightFit=async()=>({status:'not-required'});
pilot.generateSeo=async({group})=>({seo:procurement.genSeo(group),model:'mock'});
const load=()=>JSON.parse(fs.readFileSync(process.env.PROCUREMENT_PATH));
const save=s=>fs.writeFileSync(process.env.PROCUREMENT_PATH,JSON.stringify(s));
const seed=(pos={})=>{save({seq:0,settings:{warehouseLocationId:'55',exRate:15.3,freightPerGram:.35},pos});products=[];writes=[];readGate=null;writeGate=null;failAdjust=false;};
const send=async(route,body,method='POST',role='admin')=>{const r=await fetch(base+route,{method,headers:{'Content-Type':'application/json','x-role':role},...(method==='GET'?{}:{body:JSON.stringify(body||{})})});return {...await r.json(),status:r.status};};
const line=(name,type='Shirt')=>({designName:name,productType:type,colour:'Black',sizeLabel:'FS',fit:'Regular Fit',audience:'Men',qty:2,perPcsYuan:20.13,photoUrl:'/api/procurement/photo/source.jpg'});
const advance=(billNo,lines=[line(billNo)])=>send('/api/procurement/advance',{vendor:'SYNTHETIC',billNo,line:'casuals',lines});
const restockPo=()=>({id:'PO-RESTOCK',status:'received',vendor:'TEST',lines:[{...line('Existing','Trouser'),sku:'SA116XLZ9128',qty:3,weightGrams:200,classification:'EXISTING'}]});
function seedRestock(){const po=restockPo();seed({[po.id]:po});products=[{id:7,variants:[{id:70,sku:po.lines[0].sku,inventory_item_id:700}]}];return po;}
test.before(async()=>{fs.mkdirSync(path.join(sandbox,'procurement-photos'),{recursive:true});fs.writeFileSync(path.join(sandbox,'procurement-photos','source.jpg'),'synthetic-reference');server=app.listen(0,'127.0.0.1');await new Promise(r=>server.once('listening',r));base='http://127.0.0.1:'+server.address().port;});
test.after(()=>{global.fetch=realFetch;server.closeAllConnections();server.close();fs.rmSync(sandbox,{recursive:true,force:true});});

test('simultaneous advance saves retain both bills, unique PO IDs and reserved SKUs',async()=>{
  seed();let release,started;readGate=new Promise(r=>release=r);const waiting=new Promise(r=>started=r);readStarted=started;
  const first=advance('A');await waiting;const second=advance('B');release();readGate=null;
  const results=await Promise.all([first,second]);assert.deepEqual(results.map(r=>r.status),[200,200]);
  assert.equal(new Set(results.map(r=>r.poId)).size,2);assert.equal(new Set(results.flatMap(r=>r.lines.map(l=>l.sku))).size,2);
  assert.equal(Object.keys(load().pos).length,2);
  assert.equal((await advance('A')).status,409);assert.equal(Object.keys(load().pos).length,2);
});
test('simultaneous posting cannot double-adjust stock; another PO save is retained during the write',async()=>{
  const po=seedRestock();let release,started;writeGate=new Promise(r=>release=r);const waiting=new Promise(r=>started=r);writeStarted=started;
  const first=send('/api/procurement/commit',{poId:po.id,approve:true});await waiting;
  assert.equal((await send('/api/procurement/commit',{poId:po.id,approve:true})).status,409);
  const s=load();s.pos['PO-OTHER']={id:'PO-OTHER',status:'advance',billNo:'OTHER',lines:[]};s.settings.exRate=17;save(s);
  release();writeGate=null;assert.equal((await first).status,200);
  assert.equal(writes.filter(w=>w.url.endsWith('/adjust.json')).length,1);assert.equal(writes[0].payload.available_adjustment,3);assert.equal(writes[0].retries,0);
  assert.ok(load().pos['PO-OTHER']);assert.equal(load().settings.exRate,17);
  assert.equal((await send('/api/procurement/commit',{poId:po.id,approve:true})).status,409);
});
test('posting refuses changed quantities during preflight before any Shopify write',async()=>{
  const po=seedRestock();let release,started;readGate=new Promise(r=>release=r);const waiting=new Promise(r=>started=r);readStarted=started;
  const posting=send('/api/procurement/commit',{poId:po.id,approve:true});await waiting;
  const s=load();s.pos[po.id].lines[0].qty=8;save(s);release();readGate=null;
  const result=await posting;assert.equal(result.status,409);assert.match(result.error,/changed/);assert.equal(writes.length,0);assert.equal(load().pos[po.id].lines[0].qty,8);
});
test('uncertain restock results retain a durable journal and cannot be blindly resumed',async()=>{
  const po=seedRestock();failAdjust=true;const result=await send('/api/procurement/commit',{poId:po.id,approve:true});assert.equal(result.status,409);
  const saved=load().pos[po.id];assert.equal(saved.status,'posting_partial');assert.equal(saved.results.pendingOperation.sku,po.lines[0].sku);assert.ok(saved.postingAttemptId);
  assert.equal((await send('/api/procurement/pos/'+po.id+'/resume-posting')).status,409);assert.equal(writes.length,1);
});
test('restart with an unconfirmed write or restock cannot incorrectly mark the PO posted',async()=>{
  const po=seedRestock();const s=load();s.pos[po.id].status='posting_partial';s.pos[po.id].existingAdds=[{sku:po.lines[0].sku,qty:3}];s.pos[po.id].results={created:[],adjusted:[],errors:[],pendingOperation:{kind:'adjust',sku:po.lines[0].sku}};save(s);
  let result=await send('/api/procurement/pos/'+po.id+'/resume-posting');assert.equal(result.status,409);assert.match(result.error,/uncertain/);
  delete s.pos[po.id].results.pendingOperation;save(s);result=await send('/api/procurement/pos/'+po.id+'/resume-posting');assert.equal(result.status,409);assert.match(result.error,/restock/);assert.equal(writes.length,0);
});
test('India advance-to-post restocks actual received quantities and excludes missing zero-quantity lines',async()=>{
  const existing=seedRestock().lines[0];seed({});products=[{id:7,variants:[{id:70,sku:existing.sku,inventory_item_id:700}]}];
  const saved=await send('/api/procurement/advance',{vendor:'LOCAL TEST',billNo:'INDIA-TEST',origin:'india',transportTotal:100,lines:[existing,line('Missing new shirt')]});assert.equal(saved.status,200);
  const id=saved.poId;assert.equal(saved.lines[0].classification,'EXISTING');
  assert.equal((await send('/api/procurement/pos/'+id+'/receive',{qtys:{0:2,1:0},weights:{0:200}})).status,200);
  assert.equal((await send('/api/procurement/pos/'+id+'/mark-received')).status,200);
  assert.equal((await send('/api/procurement/commit',{poId:id,approve:true},'POST','cashier')).status,403);assert.equal(writes.length,0);
  const posted=await send('/api/procurement/commit',{poId:id,approve:true});assert.equal(posted.status,200,JSON.stringify(posted));
  assert.equal(posted.results.created.length,0);assert.equal(posted.results.adjusted[0].added,2);assert.equal(writes.length,1);assert.equal(products.length,1);assert.equal(load().pos[id].lines[1].qty,0);
});
test('saved legacy and custom measurement fits remain separate, editable and usable for size remapping',async()=>{
  const chart={M:{Waist:70,Hip:100,Inseam:75,Thigh:60,'Bottom hem':40}};
  fs.writeFileSync(process.env.SIZE_TRACKER_PATH,JSON.stringify({targets:{Trouser:{wideleg:chart,customfit:{L:{Waist:80,Hip:110,Inseam:85,Thigh:70,'Bottom hem':50}}}},tolerance:1.5}));
  const config=await send('/api/sizetracker/config',null,'GET');assert.ok(config.categories.find(c=>c.key==='Trouser').fits.some(f=>f.key==='customfit'));
  assert.equal((await send('/api/sizetracker/targets',{category:'Trouser',fit:'wideleg',chart})).status,200);
  const result=await send('/api/sizetracker/compare',{category:'Trouser',vendor:{XL:chart.M},wanted:['M'],tolerance:1.5});assert.equal(result.status,200);assert.equal(result.detectedFit,'wideleg');assert.equal(result.toSource[0].china,'XL');
  const targets=await send('/api/sizetracker/targets',null,'GET');assert.ok(targets.targets.Trouser.customfit.L);assert.ok(targets.targets.Trouser.wideleg.M);
});
test('each collapsed Fresh batch uses its own plan budget, including zero',async()=>{
  seed();const settings=casuals.settingsWithDefaults({});
  const batches=[200000,56000,0].map((budget,i)=>{const planSettings=structuredClone(settings);for(const c of Object.values(planSettings.categories))c.enabled=false;Object.assign(planSettings.categories.Trouser,{enabled:true,sizeMode:'cost',budget});return {id:'B'+i,num:i+1,name:'Batch '+i,categories:['Trouser'],planSettings};});
  fs.writeFileSync(process.env.CASUALS_PATH,JSON.stringify({settings,candidates:[],batches,activeBatch:'B0'}));
  const result=await send('/api/casuals/batches',null,'GET');assert.equal(result.status,200);assert.deepEqual(result.batches.map(b=>b.budget),[200000,56000,0]);
});
test('legacy numeric target sizes remain visible and selectable after the sourcing size run changes',async()=>{
  const chart={'28':{Waist:71,Hip:99,Inseam:76,Thigh:60,'Bottom hem':40}};
  fs.writeFileSync(process.env.SIZE_TRACKER_PATH,JSON.stringify({targets:{Trouser:{wideleg:chart}},tolerance:1.5}));
  const config=await send('/api/sizetracker/config',null,'GET');assert.ok(config.categories.find(c=>c.key==='Trouser').sizes.includes('28'));
  const result=await send('/api/sizetracker/compare',{category:'Trouser',vendor:{L:chart['28']},wanted:['28'],tolerance:1.5});
  assert.equal(result.status,200);assert.equal(result.toSource[0].desired,'28');assert.equal(result.toSource[0].china,'L');
});
test('costs allocate PO charges and rounding once; line/category/header totals reconcile',()=>{
  const po={exRate:15.3,freightPerGram:.35,localTransportYuan:2.2,otherCostsYuan:1.1,lines:[{qty:1,perPcsYuan:20.13,weightGrams:131},{qty:3,perPcsYuan:30.12,weightGrams:233},{qty:0,perPcsYuan:999,weightGrams:999}]};
  const result=costs(po);assert.equal(result.total,Math.round((20.13+3*30.12+3.3)*15.3+(131+3*233)*.35));assert.equal(result.lines.reduce((s,l)=>s+l.amount,0),result.total);assert.equal(result.lines[2].amount,0);
  const india=costs({origin:'india',transportTotal:10,lines:[{qty:1,perPcsYuan:100.4},{qty:2,perPcsYuan:200.4}]});assert.equal(india.total,511);assert.equal(india.lines.reduce((s,l)=>s+l.amount,0),511);
  assert.equal(costs({...po,exRate:0,freightPerGram:0},{exRate:99,freightPerGram:99}).total,0);
});
test('rendered purchase categories reconcile with the header and shared server summary',async()=>{
  const po=restockPo();po.id='PO-COST';po.exRate=15.3;po.freightPerGram=.35;po.localTransportYuan=2.2;po.otherCostsYuan=1.1;po.lines.push({...line('Second'),qty:1,weightGrams:131});seed({[po.id]:po});
  const html=fs.readFileSync(path.join(__dirname,'../public/procurement.html'),'utf8');
  const context={PurchaseCosts:costs,settings:{},historyMatchingLines:p=>p.lines,historyCategoryName:l=>l.productType,historyDesignKey:(p,l)=>l.designName};vm.createContext(context);
  for(const [start,end] of [['function historyTotals(po)','function historyStatusCode'],['function historyLineCost(po,line)','function purchaseCalculationPanel'],['function historyCategoryRows(pos)','function historyDesignGroups']])vm.runInContext(html.slice(html.indexOf(start),html.indexOf(end)),context);
  const rows=context.historyCategoryRows([po]);assert.equal(rows.reduce((s,r)=>s+r.value,0),context.historyTotals(po).total);
  const summary=await send('/api/procurement/summary',null,'GET');assert.equal(summary.totals.pending.cost,costs(po,{exRate:15.3,freightPerGram:.35}).total);
});
test('one automatic design MRP spans colours and sizes; manual overrides survive recalculation and reach Shopify',async()=>{
  seed();
  const rows=[{...line('Shared Shirt'),designCode:'SHARED',colour:'Black',sizeLabel:'M'},
    {...line('Shared Shirt'),designCode:'SHARED',colour:'Black',sizeLabel:'L'},
    {...line('Shared Shirt'),designCode:'SHARED',colour:'White',sizeLabel:'M'},
    {...line('Shared Shirt'),designCode:'SHARED',colour:'Pink',sizeLabel:'M',perPcsYuan:500}];
  const saved=await advance('UNIFORM-MRP',rows),id=saved.poId;
  let received=await send('/api/procurement/pos/'+id+'/receive',{weights:{0:100,1:100,2:350,3:100},qtys:{3:0}});
  assert.equal(received.status,200,JSON.stringify(received));
  assert.equal(new Set(received.lines.map(l=>l.calculatedMrp)).size,1);
  assert.equal(received.lines[0].calculatedMrp,received.lines[2].variantCalculatedMrp);
  assert.equal(received.lines.length,3,'missing colour is excluded and does not inflate shared price');
  const defaults=Object.fromEntries(received.lines.map(l=>[l.sku,l.suggestedMrp]));
  assert.equal((await send('/api/procurement/pos/'+id+'/selling-prices',{prices:defaults},'PATCH')).changed,0);
  assert.ok(load().pos[id].lines.every(l=>!l.manualMrp),'automatic save stays automatic');
  const overridden=received.lines[0].sku;
  assert.equal((await send('/api/procurement/pos/'+id+'/selling-prices',{prices:{[overridden]:1299}},'PATCH')).status,200);
  received=await send('/api/procurement/pos/'+id+'/receive',{weights:{2:500}});
  assert.equal(received.lines[0].suggestedMrp,1299);assert.equal(received.lines[0].mrpOverridden,true);
  assert.equal(received.lines[1].suggestedMrp,received.lines[2].suggestedMrp);
  assert.ok(received.lines[1].calculatedMrp>defaults[overridden],'unchanged automatic fields follow updated costs');
  const resetSku=received.lines[1].sku;
  await send('/api/procurement/pos/'+id+'/selling-prices',{prices:{[resetSku]:1499}},'PATCH');
  await send('/api/procurement/pos/'+id+'/selling-prices',{prices:{[resetSku]:null}},'PATCH');
  assert.equal(load().pos[id].lines[1].manualMrp,0);
  assert.equal((await send('/api/procurement/pos/'+id+'/mark-received')).status,200);
  const studio=await send('/api/procurement/pos/'+id+'/studio',null,'GET');
  for(const group of studio.newProducts){
    assert.equal((await send('/api/procurement/pos/'+id+'/openai-pilot',{groupKey:group.key,maxImageAttempts:2,generationVersion:3,generationEpoch:0})).status,202);
    for(let i=0;i<100;i++){if(load().pos[id].openaiPilot.attempts.slice(-1)[0].status!=='running')break;await new Promise(r=>setTimeout(r,10));}
  }
  assert.equal((await send('/api/procurement/pos/'+id+'/approve-po')).status,200);
  const posted=await send('/api/procurement/commit',{poId:id,approve:true});assert.equal(posted.status,200,JSON.stringify(posted));
  const prices=Object.fromEntries(products.flatMap(p=>p.variants.map(v=>[v.sku,Number(v.price)])));
  assert.equal(prices[overridden],1299);assert.equal(prices[resetSku],received.lines[1].calculatedMrp);
  assert.equal(prices[received.lines[2].sku],prices[resetSku]);assert.equal(prices[saved.lines[3].sku],undefined);
});

for(const type of ['Shirt','T-Shirt Hood','Perfumes','Belts','Bag','Unisex'])test('complete purchase flow from advance through generated review, approval and Shopify draft: '+type,async()=>{
  seed();const saved=await advance('FLOW-'+type,[{...line(type,type==='Unisex'?'Shirt':type),audience:type==='Unisex'?'Unisex':'Men'}]);assert.equal(saved.status,200,JSON.stringify(saved));const id=saved.poId;
  const receive=await send('/api/procurement/pos/'+id+'/receive',{weights:{0:200},qtys:{0:3}});assert.equal(receive.status,200);assert.equal(receive.po.status,'advance');assert.equal(receive.po.lines[0].ordered.qty,2);
  assert.equal((await send('/api/procurement/pos/'+id+'/mark-received')).status,200);
  assert.equal((await send('/api/procurement/commit',{poId:id,approve:true})).status,400);assert.equal(writes.length,0);
  const studio=await send('/api/procurement/pos/'+id+'/studio',null,'GET');const group=studio.newProducts[0];
  const generated=await send('/api/procurement/pos/'+id+'/openai-pilot',{groupKey:group.key,maxImageAttempts:2,generationVersion:3,generationEpoch:0});assert.equal(generated.status,202,JSON.stringify(generated));
  let po;for(let i=0;i<100;i++){po=load().pos[id];if(po.openaiPilot?.attempts[0]?.status!=='running')break;await new Promise(r=>setTimeout(r,10));}
  assert.equal(po.openaiPilot.attempts[0].status,'drafts-ready',JSON.stringify(po.openaiPilot));assert.deepEqual(po.aiImages[group.key].map(im=>im.type),profile(group).views);
  if(type==='Unisex'){
    assert.equal(po.aiImages[group.key].length,3,'product front and two model fronts only');
    const rejected=await send('/api/procurement/pos/'+id+'/openai-pilot',{groupKey:group.key,retry:true,generationVersion:3,generationEpoch:0,regenerateTypes:['model-side-female']});
    assert.equal(rejected.status,400);assert.match(rejected.error,/supported/);
    fs.writeFileSync(path.join(sandbox,'procurement-photos','legacy-side.jpg'),'legacy-side-image');
    const store=load();store.pos[id].aiImages[group.key].push(...['model-side-female','model-side-male'].map(t=>({...po.aiImages[group.key][1],type:t,approved:true,url:'/api/procurement/photo/legacy-side.jpg'})));save(store);
  }
  assert.equal((await send('/api/procurement/pos/'+id+'/approve-po')).status,200);
  const posted=await send('/api/procurement/commit',{poId:id,approve:true});assert.equal(posted.status,200,JSON.stringify(posted));assert.equal(load().pos[id].status,'posted');
  assert.equal(products.length,1);assert.equal(products[0].status,'draft');assert.equal(products[0].variants[0].sku,saved.lines[0].sku);assert.ok(products[0].images.every(im=>im.attachment));
  if(type==='Unisex'){
    assert.equal(products[0].images.length,3,'old side photos must not be included in new Shopify drafts');
    assert.ok(products[0].images.every(im=>Buffer.from(im.attachment,'base64').toString()!=='legacy-side-image'));
    assert.equal(load().pos[id].aiImages[group.key].length,5,'legacy files are retained');
  }
  assert.equal(writes.find(w=>w.url.endsWith('/set.json')).payload.available,3);assert.equal((await send('/api/procurement/commit',{poId:id,approve:true})).status,409);
});

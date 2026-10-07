const test=require('node:test'),assert=require('node:assert/strict');
const fs=require('node:fs'),path=require('node:path'),os=require('node:os'),vm=require('node:vm');
const listingPhoto=require('./listing-photo-fixture');
const sandbox=fs.mkdtempSync(path.join(os.tmpdir(),'sanki-approval-'));
process.env.DATA_PATH=path.join(sandbox,'data.json');process.env.PROCUREMENT_PATH=path.join(sandbox,'procurement.json');
process.env.SHOPIFY_STORE='approval-test.myshopify.com';process.env.SHOPIFY_ACCESS_TOKEN='test-only';
const express=require('express'),{router,genSeo}=require('../modules/procurement'),pilot=require('../modules/procurement-openai-pilot');
const {fingerprint}=require('../modules/procurement-codex-batch'),{shopifyClient}=require('../modules/shopify-client');
let catalogueGate,catalogueStarted,writes=0;
shopifyClient.request=async(url,options)=>{if(options?.method&&options.method!=='GET')writes++;if(catalogueStarted){catalogueStarted();catalogueStarted=null;}if(catalogueGate)await catalogueGate;return {ok:true,status:200,headers:{get:()=>null},json:async()=>({products:[]})};};
const app=express();app.use(express.json());app.use((req,res,next)=>{req.user={role:req.headers['x-test-role']||'admin',username:'tester'};next();});app.use(router);
let server,base;
const makePo=()=>{
 const po={id:'PO-TEST',status:'received',vendor:'TEST',line:'casuals',lines:[],aiImages:{},imageStyling:{},seoDraft:[]};
 for(const [i,colour] of ['White','Black'].entries()){
  const key='cotton|'+colour.toLowerCase(),photoUrl='/api/procurement/photo/source-'+i+'.jpg';
  po.lines.push({sku:'SA1'+(i+1)+'Z'+(i+1)+'FS',classification:'NEW',designName:'Cotton',designCode:'',productType:'Shirt',colour,audience:'Women',fit:'Regular Fit',sizeLabel:'FS',qty:2,perPcsYuan:55,weightGrams:200,photoUrl});
  const group={key,colour,productType:'Shirt',designName:'Cotton',designCode:'',audience:'Women',line:'casuals',season:'',fit:'Regular Fit',sizeLabels:['FS'],photoUrl};
  const styling=pilot.normalizeStyling({},group);po.imageStyling[key]=styling;
  po.aiImages[key]=pilot.pilotTypes(group).map(type=>({type,url:'/api/procurement/photo/'+i+'-'+type+'.jpg',approved:false,source:'openai-pilot',sourceFingerprint:fingerprint(group),styling:{...styling},qa:{status:'pass',productOnlyVerified:true,failed:[],uncertain:[],issues:[]}}));
  po.seoDraft.push({key,seo:genSeo(group),seoApproved:false});
 }
 return po;
};
function seed(change){const po=makePo();if(change)change(po);fs.writeFileSync(process.env.PROCUREMENT_PATH,JSON.stringify({pos:{[po.id]:po}}));fs.mkdirSync(path.join(sandbox,'procurement-photos'),{recursive:true});for(const images of Object.values(po.aiImages))for(const image of images)fs.writeFileSync(path.join(sandbox,'procurement-photos',path.basename(image.url)),listingPhoto);for(let i=0;i<2;i++)fs.writeFileSync(path.join(sandbox,'procurement-photos','source-'+i+'.jpg'),listingPhoto);return po;}
const saved=()=>JSON.parse(fs.readFileSync(process.env.PROCUREMENT_PATH)).pos['PO-TEST'];
async function send(route='approve-po',body={},role='admin'){const r=await fetch(base+'/api/procurement/pos/PO-TEST/'+route,{method:'POST',headers:{'Content-Type':'application/json','x-test-role':role},body:JSON.stringify(body)});return {status:r.status,...await r.json()};}
const noApproval=po=>{assert.ok(Object.values(po.aiImages).flat().every(x=>!x.approved));assert.ok(po.seoDraft.every(x=>!x.seoApproved));};
test.before(async()=>{server=app.listen(0,'127.0.0.1');await new Promise(r=>server.once('listening',r));base='http://127.0.0.1:'+server.address().port;});
test.after(()=>{server.closeAllConnections();server.close();fs.rmSync(sandbox,{recursive:true,force:true});assert.equal(writes,0,'Approval must not write to Shopify');});

test('approval rejects concurrent purchase changes instead of overwriting a styling save',async()=>{
 seed();let release,started;catalogueGate=new Promise(r=>release=r);const loading=new Promise(r=>started=r);catalogueStarted=started;
 const approval=send();await loading;const po=saved();po.imageStyling['cotton|white'].shoes='Ballet flats';fs.writeFileSync(process.env.PROCUREMENT_PATH,JSON.stringify({pos:{'PO-TEST':po}}));release();catalogueGate=null;
 const result=await approval;assert.equal(result.status,409);assert.match(result.error,/changed/i);assert.equal(saved().imageStyling['cotton|white'].shoes,'Ballet flats');noApproval(saved());
});
test('complete PO approval persists every checked image and SEO and safely repeats',async()=>{seed();for(let n=0;n<2;n++){const result=await send();assert.equal(result.status,200,JSON.stringify(result));assert.equal(result.approvedProducts,2);assert.ok(Object.values(saved().aiImages).flat().every(x=>x.approved));assert.ok(saved().seoDraft.every(x=>x.seoApproved));}});
test('one incomplete product leaves the entire PO unapproved',async()=>{seed(po=>po.aiImages['cotton|black'].pop());const result=await send();assert.equal(result.status,409);assert.match(result.error,/Cotton.*model-side/);noApproval(saved());});
test('missing files, held checks, raw references, changed sources and styling all block approval',async()=>{
 for(const change of [po=>po.aiImages['cotton|black'][0].url='/api/procurement/photo/missing.jpg',po=>po.aiImages['cotton|black'][0].qa.status='needs-review',po=>po.aiImages['cotton|black'][0].url=po.lines[1].photoUrl,po=>po.aiImages['cotton|black'][0].sourceFingerprint='old-reference',po=>po.imageStyling['cotton|black'].watch=true]){
  seed(change);fs.rmSync(path.join(sandbox,'procurement-photos','missing.jpg'),{force:true});const result=await send();assert.equal(result.status,409,JSON.stringify(result));noApproval(saved());
 }
});
test('duplicate size rows and running image generation block approval',async()=>{
 for(const change of [po=>po.lines.push({...po.lines[1],sku:'SA12Z3FS'}),po=>po.openaiPilot={attempts:[{status:'running',groupKey:'cotton|white',startedAt:new Date().toISOString()}]}]){seed(change);assert.equal((await send()).status,409);noApproval(saved());}
});
test('back references, audience, product-only verification and SEO remain required',async()=>{
 for(const change of [po=>po.backRefs={'cotton|black':'/api/procurement/photo/back.jpg'},po=>po.lines[1].audience='',po=>po.aiImages['cotton|black'][0].qa.productOnlyVerified=false,po=>po.seoDraft[1].seo.tags=[]]){seed(change);assert.equal((await send()).status,409);noApproval(saved());}
});
test('manual visual review and a recorded automatic fit fallback can be approved without regeneration',async()=>{
 seed(po=>{po.imageStyling['cotton|black'].fit='Slim fit';for(const image of po.aiImages['cotton|black']){image.qa.status='manual-reviewed';if(image.type.startsWith('model-'))image.requestedStyling={...image.styling,fit:'Slim fit'};}});
 assert.equal((await send()).status,200);assert.ok(Object.values(saved().aiImages).flat().every(x=>x.approved));
});
test('visible SEO edits are saved atomically with successful bulk approval',async()=>{seed();const seo={...saved().seoDraft[0].seo,metaDescription:'Edited description reviewed on screen.'};assert.equal((await send('approve-po',{seoDrafts:[{groupKey:'cotton|white',seo}]})).status,200);assert.equal(saved().seoDraft[0].seo.metaDescription,seo.metaDescription);});
test('invalid visible SEO does not save edits or partially approve other products',async()=>{seed();const before=saved().seoDraft;const result=await send('approve-po',{seoDrafts:[{groupKey:'cotton|black',seo:{metaDescription:''}}]});assert.equal(result.status,409);assert.deepEqual(saved().seoDraft,before);noApproval(saved());});
test('single-product approval shares source and styling guards',async()=>{seed(po=>po.aiImages['cotton|white'][0].sourceFingerprint='old');assert.equal((await send('approve-product',{groupKey:'cotton|white'})).status,409);noApproval(saved());});
test('permissions, locked purchases and unknown or duplicate edit groups cannot approve',async()=>{
 seed();assert.equal((await send('approve-po',{},'sales')).status,403);
 for(const status of ['posted','posting_partial']){seed(po=>po.status=status);assert.equal((await send()).status,409);noApproval(saved());}
 for(const seoDrafts of [[{groupKey:'not-a-product',seo:{}}],[{groupKey:'cotton|white',seo:{}},{groupKey:'cotton|white',seo:{}}]]){seed();assert.equal((await send('approve-po',{seoDrafts})).status,409);noApproval(saved());}
});
test('bulk UI captures visible SEO and waits for styling before submitting approval',async()=>{
 const html=fs.readFileSync(path.join(__dirname,'../public/procurement.html'),'utf8'),fn=html.slice(html.indexOf('    function rememberSeoEdits('),html.indexOf('    function refreshPostGate('));
 let release;const pending=new Promise(r=>release=r),requests=[],alerts=[],button={disabled:false,textContent:'Approve complete PO'};
 const context={productApprovalRun:null,el:()=>null,refreshPostGate:()=>{},lastReceive:{newProducts:[{key:'cotton|white'}]},receiveId:'PO-TEST',studio:{seo:{'cotton|white':{seo:{metaDescription:'Saved SEO'},approved:true}},styleSaves:{'cotton|white':pending}},confirm:()=>true,alert:m=>alerts.push(m),readSeoFields:()=>({seo:{metaDescription:'Visible edited SEO'}}),readJson:r=>r.json(),initStudioFor:()=>{},fetch:async(url,options)=>{requests.push(JSON.parse(options.body));return {json:async()=>({success:true})};}};
 vm.createContext(context);vm.runInContext(fn,context);const operation=context.approveWholePo(button);await new Promise(r=>setImmediate(r));assert.equal(requests.length,0);release();await operation;await new Promise(r=>setImmediate(r));assert.equal(requests[0].seoDrafts[0].seo.metaDescription,'Visible edited SEO');assert.equal(alerts.length,0);
});
test('failed styling saves stop bulk approval and re-enable its button',async()=>{
 const html=fs.readFileSync(path.join(__dirname,'../public/procurement.html'),'utf8'),fn=html.slice(html.indexOf('    function rememberSeoEdits('),html.indexOf('    function refreshPostGate('));
 const button={disabled:false,textContent:'Approve complete PO'},alerts=[];let requested=false;
 const context={productApprovalRun:null,el:()=>null,refreshPostGate:()=>{},lastReceive:{newProducts:[{key:'cotton|white'}]},receiveId:'PO-TEST',studio:{seo:{},styleSaves:{'cotton|white':Promise.reject(new Error('Styling save failed'))}},confirm:()=>true,alert:m=>alerts.push(m),readSeoFields:()=>null,readJson:r=>r.json(),initStudioFor:()=>{},fetch:async()=>{requested=true;}};
 vm.createContext(context);vm.runInContext(fn,context);await context.approveWholePo(button);assert.equal(requested,false);assert.equal(button.disabled,false);assert.equal(alerts[0],'Styling save failed');
});

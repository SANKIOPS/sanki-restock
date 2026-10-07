const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),os=require('node:os'),vm=require('node:vm');
const sandbox=fs.mkdtempSync(path.join(os.tmpdir(),'sanki-image-workflow-'));
process.env.DATA_PATH=path.join(sandbox,'data.json');process.env.PROCUREMENT_PATH=path.join(sandbox,'procurement.json');
process.env.SHOPIFY_STORE='workflow-test.myshopify.com';process.env.SHOPIFY_ACCESS_TOKEN='test-only';process.env.OPENAI_API_KEY='test-only';
const express=require('express'),{router,genSeo}=require('../modules/procurement'),pilot=require('../modules/procurement-openai-pilot');
const batch=require('../modules/procurement-codex-batch'),{shopifyClient}=require('../modules/shopify-client');
const group={key:'c-1|cream',colour:'Cream',productType:'Lower',designName:'Printed Lower',designCode:'C-1',audience:'Unisex',line:'SANKI Funky',season:'',fit:'Baggy Fit',sizeLabels:['FS'],photoUrl:'/api/procurement/photo/source.jpg'};
const poId='PO-TEST',key=group.key;
let server,base,calls,generate,verify,preflight;
const store=()=>JSON.parse(fs.readFileSync(process.env.PROCUREMENT_PATH));
const saved=()=>store().pos[poId];
function seed(change){
 calls={images:[],checks:0,preflight:0,seo:0};
 generate=async options=>({buffer:Buffer.from('returned-paid-image'),model:'test-model'});
 verify=async()=>({status:'pass',productOnlyVerified:true,failed:[],uncertain:[],issues:[]});preflight=async()=>({status:'not-required'});
 const po={id:poId,status:'received',vendor:'TEST',line:group.line,lines:[{...group,sku:'SA1XLZ1FS',qty:2,weightGrams:200,perPcsYuan:20,sizeLabel:'FS'}],imageStyling:{[key]:pilot.normalizeStyling({},group)},aiImages:{[key]:[]},seoDraft:[{key,seo:genSeo(group),seoApproved:false}],openaiPilot:{attempts:[]}};
 if(change)change(po);fs.writeFileSync(process.env.PROCUREMENT_PATH,JSON.stringify({settings:{},pos:{[poId]:po}}));return po;
}
async function request(route,body,role='admin'){
 const r=await fetch(base+route,{headers:{'Content-Type':'application/json','test-role':role},...(body!==undefined?{method:'POST',body:JSON.stringify(body)}:{})});return {status:r.status,...await r.json()};
}
const endpoint=route=>'/api/procurement/pos/'+poId+'/'+route;
const start=(extra={})=>request(endpoint('openai-pilot'),{groupKey:key,skipSeo:true,maxImageAttempts:2,generationVersion:3,generationEpoch:store().settings.imageGenerationEpoch||0,styling:pilot.normalizeStyling({},group),...extra});
async function finished(){for(let n=0;n<200;n++){const r=await request(endpoint('openai-pilot-status?groupKey='+encodeURIComponent(key)));if(r.pilot?.status!=='running')return r;await new Promise(r=>setTimeout(r,5));}throw new Error('Mock job did not finish');}
test.before(async()=>{
 fs.mkdirSync(path.join(sandbox,'procurement-photos'),{recursive:true});for(const name of ['source','saved','back'])fs.writeFileSync(path.join(sandbox,'procurement-photos',name+'.jpg'),'test-reference');
 pilot.preflightFit=async options=>{calls.preflight++;return preflight(options);};pilot.generateImage=async options=>{calls.images.push(options.type);return generate(options);};pilot.verifyImage=async options=>{calls.checks++;return verify(options);};pilot.generateSeo=async()=>{calls.seo++;throw new Error('Unexpected SEO call');};
 shopifyClient.request=async()=>({ok:true,status:200,headers:{get:()=>null},json:async()=>({products:[]})});
 const app=express();app.use(express.json());app.use((req,res,next)=>{req.user={role:req.headers['test-role']||'admin',username:'tester'};next();});app.use(router);
 server=app.listen(0,'127.0.0.1');await new Promise(r=>server.once('listening',r));base='http://127.0.0.1:'+server.address().port;
});
test.after(()=>{server.closeAllConnections();server.close();fs.rmSync(sandbox,{recursive:true,force:true});});

test('prompt preview is free, follows the saved product profile, and preserves printed lettering',async()=>{
 seed();const r=await request(endpoint('image-prompts?groupKey='+encodeURIComponent(key)));
 assert.equal(r.status,200);assert.deepEqual(r.views.map(v=>v.type),['front','female','male']);assert.equal(r.styling.femaleComplexion,'Fair');
 for(const view of r.views){assert.match(view.prompt,/existing printed artwork, logos and lettering/);assert.doesNotMatch(view.prompt,/no collage, text/);}
 assert.match(r.views[0].prompt,/LOWER garment/);assert.match(r.views[1].prompt,/adult .*woman/);assert.match(r.views[2].prompt,/adult .*man/);
 assert.deepEqual(calls,{images:[],checks:0,preflight:0,seo:0});
});
test('each provider failure stops the article immediately, without purchasing other views or SEO',async()=>{
 const examples=[['quota',429,'insufficient_quota'],['authentication',401,'invalid_api_key'],['access',403,'permission_denied'],['input',400,'invalid_image'],['rate-limit',429,'rate_limit_exceeded'],['provider',503,'server_error'],['safety',400,'moderation_blocked']];
 for(const [kind,status,code] of examples){
  const before=seed(po=>po.aiImages[key]=[{type:'front',url:'/api/procurement/photo/saved.jpg',approved:true,sourceFingerprint:batch.fingerprint(group),qa:{status:'pass',productOnlyVerified:true}}]);
  generate=async()=>{throw Object.assign(new Error('Provider failed'),{api:{status,code,requestId:'req_mock'}});};
  assert.equal((await start({regenerateTypes:['front','female','male'],skipSeo:false})).status,202);
  const result=await finished();assert.equal(result.pilot.failure.kind,kind);assert.equal(result.pilot.status,kind==='safety'?'blocked':'stopped');
  assert.deepEqual(calls.images,['front']);assert.equal(calls.checks,0);assert.equal(calls.seo,0);assert.deepEqual(saved().aiImages,before.aiImages);assert.equal(saved().lines[0].qty,2);
  await request(endpoint('openai-pilot-status?groupKey='+encodeURIComponent(key)));assert.deepEqual(calls.images,['front']);
 }
});
test('a visual-check quota error keeps the returned paid file held and stops remaining views',async()=>{
 seed();verify=async()=>{throw Object.assign(new Error('Quota exhausted'),{api:{status:429,code:'insufficient_quota'}});};
 assert.equal((await start()).status,202);const r=await finished();assert.equal(r.pilot.failure.kind,'quota');assert.deepEqual(calls.images,['front']);
 const held=saved().qaRejected[key];assert.equal(held.length,1);assert.equal(held[0].qa.status,'unavailable');assert.ok(fs.existsSync(path.join(sandbox,'procurement-photos',path.basename(held[0].url))));
 assert.equal(saved().aiImages[key].length,0);assert.equal((await request(endpoint('approve-product'),{groupKey:key})).status,409);
});
test('stopping during preflight prevents all image calls, freezes edits and rejects stale queued requests',async()=>{
 seed();let release;preflight=()=>new Promise(r=>release=r);assert.equal((await start()).status,202);
 assert.equal((await request(endpoint('image-styling'),{groupKey:key,styling:{tuck:'Tucked in'}})).status,409);
 assert.equal((await request(endpoint('line-photo'),{lineIndex:0,url:'/api/procurement/photo/back.jpg'})).status,409);
 const stop=await request('/api/procurement/stop-image-generation',{});assert.equal(stop.status,200);assert.equal(stop.stopped,1);
 release({status:'not-required'});assert.equal((await finished()).pilot.status,'cancelled');assert.deepEqual(calls.images,[]);
 const stale=await start({retry:true,generationEpoch:0});assert.equal(stale.status,409);assert.match(stale.error,/stopped or.*out of date/);
  preflight=async()=>({status:'not-required'});
 assert.equal((await start({retry:true})).status,202);assert.equal((await finished()).pilot.status,'drafts-ready');assert.deepEqual(calls.images,['front','female','male']);
});
test('an already submitted image returned after Stop stays held and never triggers verification or another view',async()=>{
 seed();let release,started;const arrived=new Promise(r=>started=r);generate=()=>{started();return new Promise(r=>release=r);};
 assert.equal((await start()).status,202);await arrived;await request('/api/procurement/stop-image-generation',{});
 release({buffer:Buffer.from('late-provider-image'),model:'mock'});assert.equal((await finished()).pilot.status,'cancelled');
 assert.deepEqual(calls.images,['front']);assert.equal(calls.checks,0);assert.equal(saved().qaRejected[key].length,1);assert.equal(saved().aiImages[key].length,0);
});
test('Stop requires generation access and old clients cannot start a billed job',async()=>{
 seed();assert.equal((await request('/api/procurement/stop-image-generation',{},'accounts')).status,403);
 assert.equal((await start({generationVersion:undefined})).status,409);assert.deepEqual(calls.images,[]);
});
test('a stale confirmation cannot authorize additional billed views',async()=>{
 seed();const result=await start({confirmedTypes:['front']});assert.equal(result.status,409);assert.match(result.error,/views changed/);assert.deepEqual(calls.images,[]);assert.equal(saved().openaiPilot.attempts.length,0);
});
test('adding or replacing a real back reference preserves verified front/model images and archives only old back drafts',async()=>{
 seed(po=>po.aiImages[key]=['front','female','male'].map(type=>({type,url:'/api/procurement/photo/saved.jpg',approved:true,sourceFingerprint:batch.fingerprint(group),qa:{status:'pass',productOnlyVerified:true}})));
 assert.equal((await request(endpoint('back-ref'),{groupKey:key,url:'/api/procurement/photo/back.jpg'})).status,200);
 const po=saved(),expected=batch.fingerprint(group,'/api/procurement/photo/back.jpg');assert.equal(po.aiImages[key].length,3);assert.ok(po.aiImages[key].every(image=>image.approved&&image.sourceFingerprint===expected));
 po.aiImages[key].push({type:'back',url:'/api/procurement/photo/saved.jpg',sourceFingerprint:expected});fs.writeFileSync(process.env.PROCUREMENT_PATH,JSON.stringify({settings:{},pos:{[poId]:po}}));
 assert.equal((await request(endpoint('back-ref'),{groupKey:key,url:''})).status,200);
 assert.equal(saved().aiImages[key].length,3);assert.equal(saved().referenceImageHistory[0].images[0].type,'back');assert.ok(saved().aiImages[key].every(image=>image.sourceFingerprint===batch.fingerprint(group)));
});
test('missing files and changed source fingerprints count as missing drafts, so they can be regenerated explicitly',async()=>{
 for(const image of [{type:'front',url:'/api/procurement/photo/missing.jpg',qa:{status:'pass',productOnlyVerified:true}},{type:'front',url:'/api/procurement/photo/saved.jpg',sourceFingerprint:'different-source',qa:{status:'pass',productOnlyVerified:true}}]){
  seed(po=>po.aiImages[key]=[image]);assert.equal((await start()).status,202);assert.equal((await finished()).pilot.status,'drafts-ready');assert.equal(calls.images[0],'front');assert.notEqual(saved().aiImages[key][0].url,image.url);
 }
});
test('free manifests share the paid image formula and never fabricate accessory models or unsupported back views',()=>{
 for(const product of [{productType:'Shirt',audience:'Women'},{productType:'T-Shirt Hood',audience:'Men'},{productType:'Lower',audience:'Unisex'},{productType:'Perfumes',audience:'Unisex'},{productType:'Belt',audience:'Unisex'}]){
  const g={...group,...product},po={id:'PO-MANIFEST',imageStyling:{[key]:pilot.normalizeStyling({},g)}},manifest=batch.prepare(po,g);
  assert.deepEqual(manifest.views.map(v=>v.type),pilot.pilotTypes(g));
  for(const view of manifest.views)assert.equal(view.prompt,pilot.imagePrompt(g,view.type,manifest.styling,view.type.startsWith('model-side')));
  if(/Perfume|Belt/i.test(product.productType))assert.ok(manifest.views.every(v=>['front','detail'].includes(v.type)));
 }
});

function browserContext(fetchImpl){
 const button={isConnected:true,textContent:'Generate'},status={textContent:''},message={textContent:''};
 const context={imageGenerationRun:null,lastReceive:{po:{status:'received'},newProducts:['first','second'].map(key=>({key,photoUrl:'/reference.jpg',designName:key,productType:'Lower'}))},receiveId:'PO-ORIGINAL',openaiPilotConfig:{configured:true,generationEpoch:0},studio:{styleSaves:{},images:{},rejected:{},selected:{}},
  el:id=>id==='generatePoImagesBtn'?button:id==='poGenerationStatus'?status:{querySelector:()=>message},missingPaidDrafts:()=>({images:['front']}),productNeedsSavedWeight:()=>false,paidTypesFor:()=>['front','female','male'],garmentCat:()=> 'lower',paidStylingOf:()=>({fit:'Auto'}),readJson:r=>r.json(),rerenderCard:()=>{},updateStudioSelection:()=>{},setTimeout:cb=>cb(),alert:()=>{},fetch:fetchImpl};
 vm.createContext(context);const html=fs.readFileSync(path.join(__dirname,'../public/procurement.html'),'utf8');
 vm.runInContext(html.slice(html.indexOf('    function generationCanContinue('),html.indexOf('    async function regeneratePaidImages(')),context);
 vm.runInContext(html.slice(html.indexOf('    function generationSafetyBlocked('),html.indexOf('    function studioCard(')),context);
 return {context,status,button};
}
test('browser stops a quota failure before submitting the next product',async()=>{
 const requests=[],{context,status}=browserContext(async(url,options)=>{requests.push({url,options});return {json:async()=>options?{success:true,pilot:{status:'running'}}:{success:true,pilot:{status:'stopped',failure:{kind:'quota',action:'Check billing.'}},images:[],rejectedImages:[]}};});
 await context.generatePaidGroups('all',null,true);assert.equal(requests.filter(r=>r.options).length,1);assert.match(status.textContent,/Check billing/);assert.equal(context.imageGenerationRun,null);
});
test('switching POs or stopping a local batch never submits a queued product to the new PO',async()=>{
 for(const stop of [false,true]){
  const requests=[];let context;
  ({context}=browserContext(async(url,options)=>{requests.push({url,options});if(options){if(stop)context.imageGenerationRun.stopped=true;else context.receiveId='PO-DIFFERENT';}return {json:async()=>({success:true,pilot:{status:'running'}})};}));
  await context.generatePaidGroups('all',null,true);assert.equal(requests.length,1);assert.match(requests[0].url,/PO-ORIGINAL/);assert.equal(context.imageGenerationRun,null);
 }
});

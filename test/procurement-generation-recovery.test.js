const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),os=require('node:os'),vm=require('node:vm');
const sandbox=fs.mkdtempSync(path.join(os.tmpdir(),'sanki-generation-'));
process.env.DATA_PATH=path.join(sandbox,'data.json');process.env.PROCUREMENT_PATH=path.join(sandbox,'procurement.json');
process.env.SHOPIFY_STORE='generation-test.myshopify.com';process.env.SHOPIFY_ACCESS_TOKEN='test-only';process.env.OPENAI_API_KEY='test-only';
const express=require('express'),{router,genSeo}=require('../modules/procurement'),pilot=require('../modules/procurement-openai-pilot');
const {fingerprint}=require('../modules/procurement-codex-batch'),{shopifyClient}=require('../modules/shopify-client');
let paidCalls=0,preflightGate,catalogueGate,catalogueStarted;
pilot.preflightFit=async()=>preflightGate?await preflightGate:{status:'not-required'};
pilot.generateImage=async()=>{paidCalls++;return {buffer:Buffer.from('test-only-image'),model:'test-model'};};
pilot.verifyImage=async()=>({status:'pass',productOnlyVerified:true,failed:[],uncertain:[],issues:[]});
shopifyClient.request=async()=>{if(catalogueStarted){catalogueStarted();catalogueStarted=null;}if(catalogueGate)await catalogueGate;return {ok:true,status:200,headers:{get:()=>null},json:async()=>({products:[]})};};
const app=express();app.use(express.json());app.use((req,res,next)=>{req.user={role:'admin'};next();});app.use(router);
const key='top|green',photoUrl='/api/procurement/photo/source.jpg';
const group={key,colour:'Green',productType:'Shirt',designName:'Top',designCode:'',audience:'Women',line:'casuals',season:'',fit:'Regular Fit',sizeLabels:['FS'],photoUrl};
let server,base;
function seed(attempt={}){const styling=pilot.normalizeStyling({},group);const po={id:'PO-0005',status:'received',vendor:'TEST',line:'casuals',lines:[{sku:'SA15Z1FS',designName:'Top',colour:'Green',productType:'Shirt',audience:'Women',fit:'Regular Fit',sizeLabel:'FS',qty:3,weightGrams:200,perPcsYuan:55,photoUrl}],aiImages:{[key]:[{type:'front',url:'/api/procurement/photo/saved.jpg',approved:true,sourceFingerprint:fingerprint(group),qa:{status:'pass',productOnlyVerified:true}}]},imageStyling:{[key]:styling},seoDraft:[{key,seo:genSeo(group),seoApproved:false}],openaiPilot:{attempts:[{groupKey:key,status:'running',startedAt:new Date(Date.now()-5*60*1000).toISOString(),views:[{type:'front'}],imageCalls:[{type:'front',attempt:1}],errors:[],...attempt}]}};fs.writeFileSync(process.env.PROCUREMENT_PATH,JSON.stringify({pos:{[po.id]:po}}));return po;}
const saved=()=>JSON.parse(fs.readFileSync(process.env.PROCUREMENT_PATH)).pos['PO-0005'];
async function get(route='openai-pilot-status?groupKey='+encodeURIComponent(key)){const r=await fetch(base+'/api/procurement/pos/PO-0005/'+route);return {status:r.status,...await r.json()};}
async function start(){const r=await fetch(base+'/api/procurement/pos/PO-0005/openai-pilot',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({groupKey:key,retry:true,regenerateTypes:['front'],skipSeo:true,maxImageAttempts:2,styling:pilot.normalizeStyling({},group)})});return {status:r.status,...await r.json()};}
test.before(async()=>{fs.mkdirSync(path.join(sandbox,'procurement-photos'),{recursive:true});for(const name of ['source','saved'])fs.writeFileSync(path.join(sandbox,'procurement-photos',name+'.jpg'),'test-only-reference');server=app.listen(0,'127.0.0.1');await new Promise(r=>server.once('listening',r));base='http://127.0.0.1:'+server.address().port;});
test.after(()=>{server.closeAllConnections();server.close();fs.rmSync(sandbox,{recursive:true,force:true});});

test('studio recovery cannot overwrite a job that completed while the catalogue loaded',async()=>{
 seed();let release,started;catalogueGate=new Promise(r=>release=r);const loading=new Promise(r=>started=r);catalogueStarted=started;
 const request=get('studio');await loading;const po=saved();po.openaiPilot.attempts[0].status='drafts-ready';po.openaiPilot.attempts[0].completedAt=new Date().toISOString();po.aiImages[key][0].url='/api/procurement/photo/new-result.jpg';fs.writeFileSync(process.env.PROCUREMENT_PATH,JSON.stringify({pos:{'PO-0005':po}}));release();catalogueGate=null;
 assert.equal((await request).status,200);assert.equal(saved().openaiPilot.attempts[0].status,'drafts-ready');assert.equal(saved().aiImages[key][0].url,po.aiImages[key][0].url);assert.equal(paidCalls,0);
});
test('reopening the PO recovers a recent pre-restart legacy job without generating or removing images',async()=>{const before=seed();const result=await get('studio');assert.equal(result.status,200);const after=saved();assert.equal(after.openaiPilot.attempts[0].status,'interrupted');assert.equal(after.openaiPilot.attempts[0].interruptionReason,'worker-restarted');assert.deepEqual(after.aiImages,before.aiImages);assert.deepEqual(after.openaiPilot.attempts[0].imageCalls,before.openaiPilot.attempts[0].imageCalls);assert.equal(after.lines[0].qty,3);assert.equal(paidCalls,0);});
test('foreign worker jobs and malformed old dates recover once with their audit intact',async()=>{
 for(const attempt of [{workerId:'previous-process',id:'abandoned',startedAt:new Date().toISOString()},{startedAt:'invalid'}]){seed(attempt);const result=await get();assert.equal(result.pilot.status,'interrupted');assert.match(result.pilot.errors[0].error,/no automatic paid retry/);await get();assert.equal(saved().openaiPilot.attempts[0].errors.length,1);assert.equal(paidCalls,0);}
});
test('an actual active job stays running even when old, rejects duplicate starts and allows explicit retry after interruption',async()=>{
 seed({workerId:'previous-process',id:'abandoned'});let release;preflightGate=new Promise(r=>release=r);
 const result=await start();assert.equal(result.status,202,JSON.stringify(result));assert.ok(result.pilot.id);assert.ok(result.pilot.workerId);assert.equal(saved().openaiPilot.attempts[0].status,'interrupted');
 try{const po=saved();po.openaiPilot.attempts[1].startedAt=new Date(Date.now()-100*60*1000).toISOString();fs.writeFileSync(process.env.PROCUREMENT_PATH,JSON.stringify({pos:{'PO-0005':po}}));assert.equal((await get()).pilot.status,'running');assert.equal((await start()).status,409);assert.equal(paidCalls,0);}finally{release({status:'not-required'});preflightGate=null;}
 let final;for(let n=0;n<100;n++){await new Promise(r=>setTimeout(r,5));final=await get();if(final.pilot.status!=='running')break;}
 assert.equal(final.pilot.status,'drafts-ready',JSON.stringify(final));assert.equal(paidCalls,1);assert.equal(saved().openaiPilot.attempts.length,2);
});
test('completed jobs remain completed and recovery never replays paid calls',async()=>{seed({status:'partial',completedAt:new Date().toISOString()});assert.equal((await get()).pilot.status,'partial');assert.equal(paidCalls,1);});
test('free progress UI uses only a GET, preserves visible SEO edits and refreshes the interrupted record',async()=>{
 const html=fs.readFileSync(path.join(__dirname,'../public/procurement.html'),'utf8'),fn=html.slice(html.indexOf('    function refreshGenerationProgress('),html.indexOf('    function studioCard('));
 const requests=[],button={textContent:'Check generation progress (free)',isConnected:true},record={groupKey:key,status:'running'};
 let readEdits=false,rendered=false;const context={receiveId:'PO-0005',lastReceive:{po:{openaiPilot:{attempts:[record]}}},studio:{styleSaves:{},images:{},rejected:{}},readSeoFields:()=>readEdits=true,readJson:r=>r.json(),rerenderCard:()=>rendered=true,alert:m=>{throw new Error(m);},fetch:async(url,options)=>{requests.push({url,options});return {json:async()=>({success:true,pilot:{status:'interrupted'},images:[{type:'front',url:'/saved.jpg'}],rejectedImages:[]})};}};
 vm.createContext(context);vm.runInContext(fn,context);await context.refreshGenerationProgress({key},0,button);assert.equal(readEdits,true);assert.equal(rendered,true);assert.equal(record.status,'interrupted');assert.equal(requests[0].options,undefined);assert.match(requests[0].url,/openai-pilot-status/);assert.equal(button.disabled,false);
});

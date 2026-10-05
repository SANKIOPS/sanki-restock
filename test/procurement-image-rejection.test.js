const test=require('node:test'),assert=require('node:assert/strict');
const fs=require('node:fs'),os=require('node:os'),path=require('node:path'),vm=require('node:vm');
const sandbox=fs.mkdtempSync(path.join(os.tmpdir(),'sanki-image-rejection-'));
process.env.DATA_PATH=path.join(sandbox,'data.json');
process.env.PROCUREMENT_PATH=path.join(sandbox,'procurement.json');
const {router,rejectGeneratedImage,imageCheckAccepted}=require('../modules/procurement');
const pilot=require('../modules/procurement-openai-pilot');
const key='6916|black';
const image=(type,url)=>({type,url,approved:true,qa:{status:'pass',productOnlyVerified:true}});
const makePo=()=>({id:'PO-0012',status:'received',lines:[{photoUrl:'/original-jogger.png',rawPhotoUrl:'/original-jogger.png',qty:5}],
  aiImages:{[key]:[image('front','/wrong-shirt.png'),image('model-front','/model-front.png'),image('model-side','/model-side.png')]},
  qaRejected:{[key]:[{type:'front',url:'/older-shirt.png',qa:{status:'needs-review'}}]},
  seoDraft:[{key,seoApproved:true}],openaiPilot:{attempts:[]}});
const rejection=(type='front',url='/wrong-shirt.png')=>({groupKey:key,type,url,reason:'Wrong garment',by:'tester'});

test('rejecting an active view removes it, preserves references and quantities, and archives older held drafts',()=>{
  const po=makePo(),original=structuredClone(po.lines);
  const result=rejectGeneratedImage(po,rejection(),'2026-10-05T10:00:00Z');
  assert.deepEqual(result.images.map(x=>x.type),['model-front','model-side']);
  assert.deepEqual(po.lines,original);
  assert.equal(result.rejectedImages.length,0);
  assert.deepEqual(po.imageRejectionHistory.map(x=>x.url),['/older-shirt.png','/wrong-shirt.png']);
  assert.ok(po.imageRejectionHistory.every(x=>!x.approved&&x.reason==='Wrong garment'&&x.by==='tester'));
  assert.equal(po.seoDraft[0].seoApproved,false);
});

test('rejecting a model front invalidates its dependent angle',()=>{
  const po=makePo();rejectGeneratedImage(po,rejection('model-front','/model-front.png'));
  const side=po.aiImages[key].find(x=>x.type==='model-side');
  assert.equal(side.approved,false);assert.equal(imageCheckAccepted(side),false);
});

test('rejecting a held candidate keeps a separately saved good image',()=>{
  const po=makePo();rejectGeneratedImage(po,rejection('front','/older-shirt.png'));
  assert.equal(po.aiImages[key][0].url,'/wrong-shirt.png');
  assert.equal(po.aiImages[key][0].approved,true);
  assert.deepEqual(po.qaRejected[key],[]);
});

test('stale rejection and original-reference rejection leave all data unchanged',()=>{
  for(const data of [rejection('front','/replaced.png'),rejection('original','/original-jogger.png'),rejection('front','/original-jogger.png')]){
    const po=makePo(),before=structuredClone(po);
    assert.throws(()=>rejectGeneratedImage(po,data));assert.deepEqual(po,before);
  }
});

test('unchecked images cannot be approved or posted',()=>{
  assert.equal(imageCheckAccepted({url:'/legacy.png',approved:true}),false);
  assert.equal(imageCheckAccepted({type:'front',qa:{status:'pass'}}),false);
  assert.equal(imageCheckAccepted({type:'front',qa:{status:'pass',productOnlyVerified:true}}),true);
  assert.equal(imageCheckAccepted({qa:{status:'unavailable'}}),false);
  assert.equal(imageCheckAccepted({qa:{status:'manual-reviewed'}}),true);
});

test('lower product-only prompt and corrective retry describe the featured trousers without shirt anatomy',()=>{
  for(const productType of ['Jogger','Trouser','Jeans','Shorts','Skirt']){
    const group={productType,colour:'Black',audience:'Women'};
    const prompt=pilot.imagePrompt(group,'front');
    assert.match(prompt,/Isolate ONLY the featured product/);
    assert.match(prompt,/waistband, rise, hip and leg silhouette/);
    assert.match(prompt,/Never substitute a shirt, top or jacket/);
    assert.doesNotMatch(prompt,/neckline|collar|sleeve length|shoulder seams/);
    assert.doesNotMatch(pilot.repairGuidance(['garmentMatch'],group,{},'front'),/neckline|sleeves/);
  }
});

test('product-only verification fails an added matching top even when the trousers match',()=>{
  const check={garmentMatch:{status:'pass',evidence:'Trousers match'},singleFrame:{status:'pass',evidence:'One photo'},
    productOnly:{status:'fail',evidence:'Invented matching long-sleeve top'}};
  const result=pilot.evaluateImageCheck(check,'front',{}, {productType:'Jogger'});
  assert.equal(result.status,'needs-review');assert.deepEqual(result.failed,['productOnly']);
  const incomplete=pilot.evaluateImageCheck({...check,productOnly:undefined},'front',{}, {productType:'Jogger'});
  assert.deepEqual(incomplete.uncertain,['productOnly']);
});

test('fresh generation sends the original reference, not the rejected shirt',async()=>{
  const original=Buffer.from('original-jogger');
  await pilot.generateImage({key:'test-only',group:{productType:'Jogger',audience:'Women'},source:{buf:original,mime:'image/jpeg'},type:'front',
    fetchImpl:async(url,options)=>{
      assert.equal(await options.body.get('image').text(),'original-jogger');
      assert.match(options.body.get('prompt'),/LOWER garment/);
      return {ok:true,json:async()=>({data:[{b64_json:Buffer.from('new-draft').toString('base64')}]})};
    }});
});

test('purchase page JavaScript parses and both held and active images offer rejection',()=>{
  const html=fs.readFileSync(path.join(__dirname,'../public/procurement.html'),'utf8');
  for(const script of html.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/g))new vm.Script(script[1]);
  assert.match(html,/data-reject-image data-image-type=.*esc\(t\[0\]\)/);
  assert.match(html,/data-reject-image data-image-type=.*esc\(image.type/);
  assert.match(html,/regenerateTypes:chosen,skipSeo:true/);
  assert.doesNotMatch(html,/b.title='Gemini generation/);
});

test('a fresh per-slot generation can be retried again in the same open page',async()=>{
  const html=fs.readFileSync(path.join(__dirname,'../public/procurement.html'),'utf8');
  const fn=html.slice(html.indexOf('    async function regeneratePaidImages('),html.indexOf('    function studioCard('));
  const payloads=[],po={status:'received',openaiPilot:{attempts:[]}},button={textContent:'Generate',isConnected:true};
  const context={lastReceive:{po},studio:{images:{},rejected:{},seo:{},styleSaves:{}},receiveId:'PO-0012',
    paidTypesFor:()=>['front'],productNeedsSavedWeight:()=>false,garmentCat:()=> 'lower',paidStylingOf:()=>({fit:'Auto'}),
    confirm:()=>true,alert:message=>{throw new Error(message);},el:()=>({querySelector:()=>({textContent:''})}),
    rerenderCard:()=>{},readJson:response=>response.json(),setTimeout:callback=>callback(),
    fetch:async(url,options)=>({json:async()=>{
      if(options){const body=JSON.parse(options.body);payloads.push(body);return {success:true,pilot:{groupKey:key,status:'running',startedAt:String(payloads.length)}};}
      return {success:true,pilot:{groupKey:key,status:'drafts-ready'},images:[image('front','/fresh.png')],rejectedImages:[]};
    }})};
  vm.createContext(context);vm.runInContext(fn,context);
  const np={key,photoUrl:'/original-jogger.png',colour:'Black'};
  await context.regeneratePaidImages(np,0,['front'],button);
  await context.regeneratePaidImages(np,0,['front'],button);
  assert.equal(payloads[0].retry,false);assert.equal(payloads[1].retry,true);
  assert.ok(payloads.every(body=>body.skipSeo&&body.maxImageAttempts===2));
});

test('rejection endpoint is free, persists its audit, and blocks unauthorized, stale, posted and in-flight actions',async t=>{
  const express=require('express'),app=express();app.use(express.json());
  app.use((req,res,next)=>{req.user={role:req.headers['x-test-role']||'admin',username:'tester'};next();});app.use(router);
  const server=app.listen(0,'127.0.0.1');await new Promise(resolve=>server.once('listening',resolve));
  t.after(()=>new Promise(resolve=>server.close(resolve)));
  const base='http://127.0.0.1:'+server.address().port;
  const save=po=>fs.writeFileSync(process.env.PROCUREMENT_PATH,JSON.stringify({pos:{'PO-0012':po}}));
  const send=async(body,role='admin',route='reject-image')=>{
    const response=await fetch(base+'/api/procurement/pos/PO-0012/'+route,{method:'POST',headers:{'Content-Type':'application/json','x-test-role':role},body:JSON.stringify(body)});
    return {status:response.status,body:await response.json()};
  };
  save(makePo());assert.equal((await send(rejection(),'sales')).status,403);
  assert.equal((await send({...rejection(),reason:''})).status,400);
  assert.equal((await send(rejection('front','/stale.png'))).status,409);
  const success=await send(rejection());assert.equal(success.status,200);
  const saved=JSON.parse(fs.readFileSync(process.env.PROCUREMENT_PATH));
  assert.equal(saved.pos['PO-0012'].imageRejectionHistory.length,2);
  assert.equal(saved.pos['PO-0012'].openaiPilot.attempts.length,0);
  assert.equal((await send(rejection())).status,409);
  save({...makePo(),status:'posted'});assert.equal((await send(rejection())).status,409);
  save({...makePo(),openaiPilot:{attempts:[{groupKey:key,status:'running',startedAt:new Date().toISOString()}]}});
  assert.equal((await send(rejection())).status,409);
  const retired=await send({},'admin','generate-images');assert.equal(retired.status,409);
  assert.match(retired.body.error,/No paid call was made/);
});

test.after(()=>fs.rmSync(sandbox,{recursive:true,force:true}));

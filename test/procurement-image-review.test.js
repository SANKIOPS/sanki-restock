const test=require('node:test'),assert=require('node:assert/strict');
const fs=require('node:fs'),path=require('node:path'),os=require('node:os'),vm=require('node:vm');
const sandbox=fs.mkdtempSync(path.join(os.tmpdir(),'sanki-image-review-'));
process.env.DATA_PATH=path.join(sandbox,'data.json');process.env.PROCUREMENT_PATH=path.join(sandbox,'procurement.json');
process.env.SHOPIFY_STORE='test-review.myshopify.com';process.env.SHOPIFY_ACCESS_TOKEN='test-only';
const express=require('express'),{router}=require('../modules/procurement'),pilot=require('../modules/procurement-openai-pilot');
const {fingerprint}=require('../modules/procurement-codex-batch'),{shopifyClient}=require('../modules/shopify-client');
const app=express();app.use(express.json());app.use((req,res,next)=>{req.user={role:'admin',username:'tester'};next();});app.use(router);
const key='top|blue',photoUrl='/api/procurement/photo/reference.jpg';
const group={key,colour:'Blue',productType:'T-Shirt',designName:'Top',designCode:'',audience:'Women',line:'casuals',season:'',fit:'Oversized',sizeLabels:['FS'],photoUrl};
const normal=()=>pilot.normalizeStyling({fit:'Slim fit',shoes:'Leather loafers'},group);
const makePo=()=>({id:'PO-0005',status:'received',vendor:'Test',line:'casuals',lines:[{sku:'SA22XLZ139FS',classification:'NEW',qty:3,designName:'Top',designCode:'',colour:'Blue',productType:'T-Shirt',audience:'Women',fit:'Oversized',sizeLabel:'FS',perPcsYuan:40,weightGrams:200,photoUrl}],
  imageStyling:{[key]:normal()},aiImages:{[key]:[]},qaRejected:{[key]:[{type:'front',url:'/api/procurement/photo/candidate.jpg',sourceFingerprint:fingerprint(group),styling:{...normal(),fit:'Auto'},at:'2026-10-05T10:00:00Z',qa:{status:'needs-review',failed:['productOnly'],uncertain:[],issues:['productOnly: A model is visible']}}]},openaiPilot:{attempts:[]}});
let server,base;
shopifyClient.request=async()=>({ok:true,status:200,headers:{get:()=>null},json:async()=>({products:[]})});
function seed(change){const po=makePo();if(change)change(po);fs.writeFileSync(process.env.PROCUREMENT_PATH,JSON.stringify({settings:{},pos:{[po.id]:po}}));for(const name of ['reference','candidate','model'])fs.writeFileSync(path.join(sandbox,'procurement-photos',name+'.jpg'),'test-only');}
const saved=()=>JSON.parse(fs.readFileSync(process.env.PROCUREMENT_PATH)).pos['PO-0005'];
const post=async(endpoint,body)=>{const response=await fetch(base+'/api/procurement/pos/PO-0005/'+endpoint,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});return {status:response.status,...await response.json()};};
const review=(type='front')=>post('qa-review',{groupKey:key,type,url:'/api/procurement/photo/candidate.jpg',reason:'The saved candidate shows only the correct garment.'});
test.before(async()=>{server=app.listen(0,'127.0.0.1');await new Promise(resolve=>server.once('listening',resolve));base='http://127.0.0.1:'+server.address().port;});
test.after(()=>{server.closeAllConnections();server.close();fs.rmSync(sandbox,{recursive:true,force:true});});

test('product-only held images can be manually reviewed regardless of model settings, including missing legacy styling',async()=>{
  for(const change of [po=>po.imageStyling[key].shoes='Ballet flats',po=>po.qaRejected[key][0].styling=null]){
    seed(change);const result=await review();assert.equal(result.success,true,JSON.stringify(result));
    assert.equal(saved().aiImages[key][0].qa.status,'manual-reviewed');assert.equal(saved().aiImages[key][0].approved,false);assert.equal(saved().aiImages[key][0].styling,null);
  }
});
test('actual source changes still reject a held product-only image before mutation',async()=>{
  seed(po=>po.lines[0].photoUrl='/changed.jpg');const result=await review();assert.equal(result.status,409);assert.match(result.error,/product reference changed/);assert.equal(saved().aiImages[key].length,0);
});
test('model review ignores the other gender and unused bag colour but reports real outfit changes',async()=>{
  seed(po=>{po.qaRejected[key][0].type='model-front';po.qaRejected[key][0].styling=normal();po.imageStyling[key].maleComplexion='Deep';po.imageStyling[key].bagColour='Black';});
  assert.equal((await review('model-front')).success,true);
  seed(po=>{po.qaRejected[key][0].type='model-front';po.qaRejected[key][0].styling=normal();po.imageStyling[key].shoes='Ballet flats';});
  const result=await review('model-front');assert.equal(result.status,409);assert.deepEqual(result.stylingChanges,[{field:'shoes',before:'Leather loafers',after:'Ballet flats'}]);assert.match(result.error,/Restore/);
});
test('requested fit and effective photo fit remain distinct and review does not reject a preflight fallback',async()=>{
  for(const modern of [false,true]){
    seed(po=>{const candidate=po.qaRejected[key][0];candidate.type='model-front';if(modern)candidate.requestedStyling=normal();else po.openaiPilot.attempts=[{groupKey:key,startedAt:'2026-10-05T09:00:00Z',styling:normal(),photoStyling:candidate.styling}];});
    assert.equal((await review('model-front')).success,true);assert.equal(saved().aiImages[key][0].styling.fit,'Auto');assert.equal(saved().aiImages[key][0].requestedStyling.fit,'Slim fit');
  }
});
test('restoring image styling restores its original check but never automatically approves it or hides a visual failure',async()=>{
  for(const qa of [{status:'pass',failed:[],uncertain:[],issues:[]},{status:'needs-review',failed:['garmentMatch'],uncertain:[],issues:['garmentMatch: Wrong garment']}]){
    seed(po=>{po.aiImages[key]=[{type:'model-front',url:'/api/procurement/photo/model.jpg',source:'openai-pilot',styling:normal(),approved:true,qa}];});
    const changed={...normal(),shoes:'Ballet flats'};
    assert.equal((await post('image-styling',{groupKey:key,styling:changed})).success,true);
    assert.equal(saved().aiImages[key][0].approved,false);assert.equal(saved().aiImages[key][0].qa.stylingChanged,true);
    await post('image-styling',{groupKey:key,styling:normal()});assert.deepEqual(saved().aiImages[key][0].qa,qa);assert.equal(saved().aiImages[key][0].approved,false);
  }
});
test('legacy styling invalidation can recover a known clean check but cannot clear real garment failures',async()=>{
  for(const failed of [[],['garmentMatch']]){
    seed(po=>{po.aiImages[key]=[{type:'model-front',url:'/api/procurement/photo/model.jpg',source:'openai-pilot',styling:normal(),approved:false,qa:{status:'needs-review',failed,uncertain:[],issues:['Model styling changed after this image was generated. Regenerate this view.']}}];});
    await post('image-styling',{groupKey:key,styling:normal()});assert.equal(saved().aiImages[key][0].qa.status,failed.length?'needs-review':'pass');assert.equal(saved().aiImages[key][0].approved,false);
  }
});
test('restoring styling cannot clear a changed-front continuity failure',async()=>{
  seed(po=>{po.aiImages[key]=[{type:'model-side',url:'/api/procurement/photo/model.jpg',source:'openai-pilot',styling:normal(),approved:false,qa:{status:'pass',failed:[],uncertain:[],issues:[]}}];po.qaRejected[key][0].type='model-front';po.qaRejected[key][0].styling=normal();});
  await post('image-styling',{groupKey:key,styling:{...normal(),shoes:'Ballet flats'}});
  await post('image-styling',{groupKey:key,styling:normal()});
  await review('model-front');await post('image-styling',{groupKey:key,styling:normal()});
  const side=saved().aiImages[key].find(image=>image.type==='model-side');assert.equal(side.qa.status,'needs-review');assert.match(side.qa.issues[0],/Matching front image changed/);
});
test('verification clearly identifies original, candidate and continuity images',async()=>{
  const photo=colour=>require('sharp')({create:{width:8,height:12,channels:3,background:colour}}).png().toBuffer();
  const [original,candidate,continuity]=await Promise.all(['#223344','#445566','#667788'].map(photo));
  const findings=Object.fromEntries(['garmentMatch','productOnly','singleFrame','angleMatch','fitMatch','pairMatch','shoeMatch','tuckMatch','bagMatch','shadesMatch','capMatch','chainMatch','watchMatch','modelMatch','outfitContinuity'].map(field=>[field,{status:'pass',evidence:'Matches'}]));
  await pilot.verifyImage({key:'test-only',group,type:'front',source:{mime:'image/jpeg',buf:original},generated:candidate,continuitySource:{mime:'image/jpeg',buf:continuity},fetchImpl:async(url,options)=>{
    const body=JSON.parse(options.body),content=body.input[0].content;
    assert.match(content[0].text,/judge productOnly solely from IMAGE 2/);assert.match(content[1].text,/ORIGINAL REFERENCE/);assert.match(content[3].text,/GENERATED CANDIDATE/);assert.match(content[5].text,/MATCHING MODEL FRONT/);assert.equal(body.max_output_tokens,2200);
    for(const [index,buf] of [[2,original],[4,candidate],[6,continuity]])assert.equal(content[index].image_url,'data:image/png;base64,'+buf.toString('base64'));
    return {ok:true,json:async()=>({output_text:JSON.stringify({detectedModelGender:'not-applicable',...findings})})};
  }});
});
test('review waits for pending styling saves and offers a free restoration action',()=>{
  const html=fs.readFileSync(path.join(__dirname,'../public/procurement.html'),'utf8');for(const script of html.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/g))new vm.Script(script[1]);
  assert.match(html,/Restore image styling \(free\)/);assert.match(html,/\(studio\.styleSaves\[np\.key\]\|\|Promise\.resolve\(\)\)\.then\(function\(\)\{return fetch\([^\n]+qa-review/);
});

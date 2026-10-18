const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {cleanMatches} = require('../modules/inventory-visual-search');
const {vision,providerError} = require('../modules/inventory-visual-search');
test('provider diagnostics redact credentials and URLs',()=>{
  const {safeProviderMessage}=require('../modules/inventory-visual-search');
  assert.equal(safeProviderMessage({error:{message:'key secret-value failed at https://example.com?key=123'}},['secret-value']),'key [redacted] failed at [redacted]');
});
function response(status,body){return {status,ok:status>=200&&status<300,json:async()=>body};}
test('invalid Gemini key falls back to configured Anthropic vision',async()=>{
  const calls=[];
  const result=await vision(['photo'],'compare',{GEMINI_API_KEY:'invalid',ANTHROPIC_API_KEY:'available',INVENTORY_VISION_MODEL:'gemini-example'},async(url,opts)=>{
    calls.push({url,body:JSON.parse(opts.body)});
    return calls.length===1?response(400,{error:{message:'API key not valid. Please pass a valid API key.'}}):response(200,{content:[{text:'{"matches":[]}'}]});
  });
  assert.deepEqual(result,{matches:[]});assert.equal(calls.length,2);assert.equal(calls[1].body.model,'claude-sonnet-4-6');
});
test('missing default Gemini model is replaced by an available vision model',async()=>{
  let count=0;
  const result=await vision(['photo'],'compare',{GEMINI_API_KEY:'available'},async(url)=>{
    count++;
    if(count===1)return response(404,{error:{message:'Not found'}});
    if(count===2)return response(200,{models:[{name:'models/gemini-3.6-flash',supportedGenerationMethods:['generateContent']}]});
    assert.match(url,/gemini-3\.6-flash:generateContent/);
    return response(200,{candidates:[{content:{parts:[{text:'{"matches":[]}'}]}}]});
  });
  assert.deepEqual(result,{matches:[]});assert.equal(count,3);
});
test('provider errors distinguish permission, model and quota without leaking raw error text',()=>{
  assert.match(providerError('google',403,{}),/permission/);
  assert.match(providerError('google',404,{}),/model/);
  assert.match(providerError('anthropic',400,{error:{message:'credit balance too low'}}),/billing/);
  assert.doesNotMatch(providerError('google',500,{error:{message:'secret token'}}),/secret token/);
});
test('visual search excludes invented IDs and duplicate matches',()=>{
  assert.deepEqual(cleanMatches([{id:2,confidence:'high',reason:'Same print'},{id:999,confidence:'high'},{id:2},{id:'3'},{id:3,confidence:'invented'}],new Set([2,3])),[{id:2,confidence:'high',reason:'Same print'},{id:3,confidence:'low',reason:''}]);
});
test('inventory photo search provides camera, upload and expandable SKU navigation',()=>{
  const ui=fs.readFileSync(path.join(__dirname,'../public/inventory-visual-search.js'),'utf8');
  assert.match(ui,/capture="environment"/);assert.match(ui,/Upload product photo/);assert.match(ui,/inventory:open-product/);
});

test('photo-search authorization permits search without granting Stylists stock-changing access',()=>{
  const {apiAllowedForUser}=require('../auth');
  const user=roles=>({username:'visual-search-test',role:roles[0],roles});
  const search='/api/inventory-categorization/image-search';
  for(const role of ['owner','admin','inventory','warehouse','sales','stocksearch']){
    assert.equal(apiAllowedForUser(user([role]),search,'POST'),true,role+' can submit a photo search');
    assert.equal(apiAllowedForUser(user([role]),search+'/diagnostics','GET'),true,role+' can check search availability');
    assert.equal(apiAllowedForUser(user([role]),search+'/test-job','GET'),true,role+' can retrieve search results');
  }
  for(const roles of [['stocksearch'],['stocksearch','claimant']]){
    const stylist=user(roles);
    assert.equal(apiAllowedForUser(stylist,'/api/inventory-categorization/catalog','GET'),true);
    for(const route of [search+'/diagnostics',search+'/test-job','/api/inventory-categorization/catalog']){
      assert.equal(apiAllowedForUser(stylist,route,'POST'),false,'search access does not permit writes to '+route);
    }
    for(const route of ['/api/inventory/adjust','/api/inventory-categorization/apply','/api/inventory-categorization/preview','/api/inventory-costs/set','/api/stock-movements']){
      assert.equal(apiAllowedForUser(stylist,route,'POST'),false,'search access cannot change stock through '+route);
    }
  }
  assert.equal(apiAllowedForUser(user(['claimant']),search,'POST'),false,'claimant-only access does not grant photo search');
});

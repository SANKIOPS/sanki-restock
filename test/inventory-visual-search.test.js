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

test('HTTP 400 distinguishes request/model configuration from actual image or token size limits',()=>{
  const invalid=providerError('google',400,{error:{status:'INVALID_ARGUMENT',message:'Unknown name "unsupportedField" at generationConfig'}});
  assert.match(invalid,/request or model configuration/);assert.doesNotMatch(invalid,/smaller.*photo/);
  const size=providerError('google',400,{error:{message:'Request payload size exceeds the limit.'}});
  assert.match(size,/payload or token limit/);assert.match(size,/smaller photo/);
  const tokens=providerError('google',400,{error:{message:'The input token count exceeds the maximum number of tokens allowed.'}});
  assert.match(tokens,/token limit/);
  const image=providerError('google',400,{error:{message:'Unable to decode the image data.'}});
  assert.match(image,/photo format or image data/);assert.match(image,/JPEG or PNG/);
});

test('HTTP 400 account prerequisites and structured invalid-key reasons receive account setup advice',()=>{
  const region=providerError('google',400,{error:{status:'FAILED_PRECONDITION',message:'User location is not supported for the API use.'}});
  assert.match(region,/region is not eligible/);assert.doesNotMatch(region,/smaller/);
  const account=providerError('google',400,{error:{status:'FAILED_PRECONDITION',message:'Project prerequisites are not met.'}});
  assert.match(account,/prerequisite is missing/);
  const invalid=providerError('google',400,{error:{message:'Bad Request',details:[{reason:'API_KEY_INVALID'}]}});
  assert.match(invalid,/API key is invalid/);assert.doesNotMatch(invalid,/smaller/);
});

test('vision includes actionable sanitized HTTP 400 detail and retains fallback billing failure',async()=>{
  const config={GEMINI_API_KEY:'private-google-secret',GOOGLE_API_KEY:'other-google-secret',ANTHROPIC_API_KEY:'private-anthropic-secret'};
  let count=0;
  await assert.rejects(vision(['photo'],'compare',config,async()=>++count===1?
    response(400,{error:{status:'INVALID_ARGUMENT',message:'Unsupported responseMimeType for private-google-secret. See https://provider.example/details?key=other-google-secret Bearer hidden-token'}}):
    response(400,{error:{message:'Your credit balance is too low'}})
  ),error=>{
    assert.match(error.message,/request or model configuration/);assert.match(error.message,/Provider detail: Unsupported responseMimeType/);
    assert.match(error.message,/Anthropic: quota or billing/);
    assert.doesNotMatch(error.message,/private-google-secret|other-google-secret|private-anthropic-secret|https?:\/\/|hidden-token/);
    assert.match(error.message,/\[redacted\]/);return true;
  });
  assert.equal(count,2,'The two configured providers were tried once each');
});

test('Gemini request uses documented JPEG inlineData and JSON response fields without unsupported sampling parameters',async()=>{
  const result=await vision(['jpeg-base64'],'compare',{GEMINI_API_KEY:'mock',INVENTORY_GEMINI_MODEL:'gemini-3.8-flash'},async(url,opts)=>{
    const body=JSON.parse(opts.body);
    assert.match(url,/\/models\/gemini-3\.8-flash:generateContent$/);
    assert.deepEqual(body.generationConfig,{responseMimeType:'application/json',maxOutputTokens:4096});
    assert.deepEqual(body.contents,[{role:'user',parts:[{text:'compare'},{inlineData:{mimeType:'image/jpeg',data:'jpeg-base64'}}]}]);
    assert.equal(opts.headers['x-goog-api-key'],'mock');
    return response(200,{candidates:[{content:{parts:[{text:'{"matches":[]}'}]}}]});
  });
  assert.deepEqual(result,{matches:[]});
});

test('discovered newer Gemini fallback also omits unsupported sampling parameters',async()=>{
  let count=0;
  const result=await vision(['photo'],'compare',{GEMINI_API_KEY:'fallback-sampling-test'},async(url,opts)=>{
    count++;
    if(count===1)return response(404,{error:{message:'Not found'}});
    if(count===2)return response(200,{models:[{name:'models/gemini-3.8-flash',supportedGenerationMethods:['generateContent']}]});
    assert.match(url,/gemini-3\.8-flash:generateContent/);
    assert.equal(JSON.parse(opts.body).generationConfig.temperature,undefined);
    return response(200,{candidates:[{content:{parts:[{text:'{"matches":[]}'}]}}]});
  });
  assert.deepEqual(result,{matches:[]});assert.equal(count,3);
});

test('a non-JSON HTTP 400 response does not invent an image size diagnosis',async()=>{
  await assert.rejects(vision(['photo'],'compare',{GEMINI_API_KEY:'mock'},async()=>({ok:false,status:400,json:async()=>{throw new Error('not JSON')}})),error=>{
    assert.match(error.message,/HTTP 400/);assert.match(error.message,/request or model configuration/);assert.doesNotMatch(error.message,/smaller.*photo/);return true;
  });
});

test('provider detail redaction removes control characters and caps long diagnostic text',()=>{
  const {safeProviderMessage}=require('../modules/inventory-visual-search');
  assert.equal(safeProviderMessage({error:{message:'line one\nline two\r\t\x00'}}),'line one line two   ');
  assert.equal(safeProviderMessage({error:{message:'x'.repeat(1000)}}).length,500);
});

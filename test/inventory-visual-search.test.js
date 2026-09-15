const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const {cleanMatches} = require('../modules/inventory-visual-search');
const {vision,providerError} = require('../modules/inventory-visual-search');
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
  const ui=fs.readFileSync('public/inventory-visual-search.js','utf8');
  assert.match(ui,/capture="environment"/);assert.match(ui,/Upload product photo/);assert.match(ui,/inventory:open-product/);
  const auth=fs.readFileSync('auth.js','utf8');assert.match(auth,/image-search', roles: \['admin', 'inventory', 'warehouse', 'sales', 'stocksearch'\]/);
});

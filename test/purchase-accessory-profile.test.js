const test=require('node:test'),assert=require('node:assert/strict');
const pilot=require('../modules/procurement-openai-pilot');
const {genSeo,canonicalSeoNaming}=require('../modules/procurement');
const passed=()=>Object.fromEntries(['garmentMatch','productOnly','singleFrame','angleMatch','fitMatch','pairMatch','shoeMatch','tuckMatch','bagMatch','shadesMatch','capMatch','chainMatch','watchMatch','modelMatch','outfitContinuity'].map(k=>[k,{status:'pass',evidence:'Visible match'}]));
for(const productType of ['Perfumes','Belts','Bag'])test(productType+' preserves product identity and uses matching generation, QA and approval view rules',async()=>{
  const group={productType,audience:'Women',fit:'Oversized',colour:'Black',designName:'Signature',sizeLabels:['FS']};
  assert.deepEqual(pilot.pilotTypes(group),['front','detail']);assert.deepEqual(pilot.pilotTypes(group,true),['front','back','detail']);
  assert.match(pilot.imagePrompt(group,'front'),/product-only/);assert.doesNotMatch(pilot.imagePrompt(group,'front'),/wearing this exact garment|Remove.*packaging/);
  assert.throws(()=>pilot.imagePrompt(group,'model-front'),/product-only/);assert.match(pilot.imagePrompt(group,'detail'),/close-up/);
  assert.doesNotMatch(genSeo(group).title,/Oversized|Fit/);assert.doesNotMatch(canonicalSeoNaming(genSeo(group),group).title,/Oversized|Fit/);
  let prompt;const finding={detectedModelGender:'not-applicable',...passed()};finding.fitMatch={status:'fail',evidence:'Irrelevant garment fit'};
  const verify=async(check)=>pilot.verifyImage({key:'mock-only',group,type:'detail',styling:{},source:{mime:'image/jpeg',buf:Buffer.from('reference')},generated:Buffer.from('candidate'),fetchImpl:async(url,opt)=>{assert.match(url,/responses$/);prompt=JSON.parse(opt.body).input[0].content[0].text;return {ok:true,json:async()=>({output_text:JSON.stringify(check)})};}});
  assert.equal((await verify(finding)).status,'pass');assert.match(prompt,/label|buckle|strap/);assert.match(prompt,/all model-only findings to pass/);
  finding.garmentMatch={status:'fail',evidence:'The bottle label or buckle differs from the original'};assert.equal((await verify(finding)).status,'needs-review');
  finding.garmentMatch={status:'uncertain',evidence:'Product label cannot be read safely'};assert.equal((await verify(finding)).status,'needs-review');
  await pilot.generateSeo({key:'mock-only',group,source:{mime:'image/jpeg',buf:Buffer.from('reference')},fetchImpl:async(url,opt)=>{
    prompt=JSON.parse(opt.body).input[0].content[0].text;
    return {ok:true,json:async()=>({output_text:JSON.stringify(genSeo(group))})};
  }});
  assert.doesNotMatch(prompt,/neckline|collar|V-Neck|Button-Detail/);assert.match(prompt,/Do not use clothing terms or garment fit/);
});
test('perfume generation preserves supplied packaging and printed labels rather than inventing scent claims',()=>{
  const prompt=pilot.imagePrompt({productType:'Perfumes'},'front');assert.match(prompt,/Keep its original packaging/);assert.match(prompt,/Never invent scent notes, volume, ingredients or label text/);
});

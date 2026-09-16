const test=require('node:test'),assert=require('node:assert/strict');
const pilot=require('../modules/procurement-openai-pilot');
const group={key:'971|black',colour:'Black',productType:'T-Shirt',audience:'Women',fit:'Muscle Fit',sizeLabels:['FS']};
const source={buf:Buffer.from('test-image'),mime:'image/jpeg'};

test('pilot limits views to supported front and audience model without a fabricated back',()=>{
  assert.deepEqual(pilot.pilotTypes(group),['front','female','model-front','model-side','detail']);
  assert.deepEqual(pilot.pilotTypes({...group,audience:'Unisex'}),['front','female','male','model-front','detail']);
  assert.match(pilot.imagePrompt(group,'front'),/Do not invent/);
});

test('image request sends one referenced edit, medium quality and no retry',async()=>{
  let calls=0;
  const out=await pilot.generateImage({key:'test-only',group,source,type:'front',fetchImpl:async(url,options)=>{
    calls++;assert.equal(url,'https://api.openai.com/v1/images/edits');
    assert.equal(options.body.get('quality'),'medium');
    assert.equal(options.body.get('size'),'1024x1536');
    assert.equal(options.body.get('model'),'gpt-image-1.5');
    assert.equal(options.body.get('image').size,source.buf.length);
    return {ok:true,json:async()=>({data:[{b64_json:Buffer.from('result').toString('base64')}],usage:{output_tokens:100}})};
  }});
  assert.equal(calls,1);assert.equal(out.buffer.toString(),'result');
  assert.deepEqual(out.usage,{output_tokens:100});
});

test('SEO request uses the original photo and returns complete structured draft',async()=>{
  const seo={displayName:'Diamond Stitch',title:'Black Diamond Stitch T-Shirt | SANKI',metaTitle:'Black Diamond Stitch T-Shirt | SANKI',metaDescription:'A black crew neck T-shirt with diamond stitching.',imageAlt:'Black diamond stitch T-shirt front view',tags:['Black','Crew Neck'],bodyHtml:'<p>Black crew neck T-shirt.</p>'};
  const out=await pilot.generateSeo({key:'test-only',group,source,fetchImpl:async(url,options)=>{
    assert.equal(url,'https://api.openai.com/v1/responses');
    const body=JSON.parse(options.body);
    assert.equal(body.store,false);
    assert.equal(body.text.format.type,'json_schema');
    assert.match(body.input[0].content[1].image_url,/^data:image\/jpeg;base64,/);
    return {ok:true,json:async()=>({output:[{content:[{type:'output_text',text:JSON.stringify(seo)}]}],usage:{input_tokens:200}})};
  }});
  assert.deepEqual(out.seo,seo);
  assert.deepEqual(out.usage,{input_tokens:200});
});

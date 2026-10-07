const test=require('node:test'),assert=require('node:assert/strict'),sharp=require('sharp');
const {normalizeSource}=require('../modules/procurement-image-source');
const pilot=require('../modules/procurement-openai-pilot');
const group={productType:'Trouser',colour:'Black',audience:'Men'};
const image=()=>sharp({create:{width:17,height:29,channels:3,background:'#123456'}});

test('AVIF bytes incorrectly labelled JPEG become a valid PNG without mutating the original',async()=>{
 const buf=await image().avif().toBuffer(),before=Buffer.from(buf);
 const result=await normalizeSource({buf,mime:'image/jpeg'});
 assert.equal(result.originalFormat,'avif');assert.equal(result.mime,'image/png');assert.equal(result.converted,true);
 const metadata=await sharp(result.buf).metadata();assert.equal(metadata.format,'png');assert.equal(metadata.width,17);assert.equal(metadata.height,29);
 assert.deepEqual(buf,before);assert.equal(await normalizeSource({buf,mime:'image/jpeg'}),result);
});
test('valid PNG JPEG and WebP bytes keep their format regardless of extension or declared MIME',async()=>{
 for(const format of ['png','jpeg','webp']){const buf=await image().toFormat(format).toBuffer();const r=await normalizeSource({buf,mime:'image/jpeg'});assert.equal(r.mime,'image/'+format);assert.equal(r.buf,buf);assert.equal(r.converted,false);}
});
test('TIFF and GIF references are converted into supported single-frame PNG files',async()=>{
 for(const format of ['tiff','gif']){const r=await normalizeSource({buf:await image().toFormat(format).toBuffer()});assert.equal(r.mime,'image/png');assert.equal((await sharp(r.buf).metadata()).pages||1,1);}
});
test('camera orientation is applied and oversized dimensions are reduced within 4096 pixels',async()=>{
 const buf=await image().jpeg().withMetadata({orientation:6}).toBuffer();const r=await normalizeSource({buf});const m=await sharp(r.buf).metadata();assert.equal(m.width,29);assert.equal(m.height,17);assert.equal(m.orientation,undefined);
 const large=await sharp({create:{width:5000,height:10,channels:3,background:'#123456'}}).png().toBuffer();const resized=await normalizeSource({buf:large});assert.equal((await sharp(resized.buf).metadata()).width,4096);
});
test('corrupt files fail locally before an image or vision provider is called',async()=>{
 let calls=0;const fetchImpl=async()=>{calls++;throw new Error('Must not call provider');},source={buf:Buffer.from('not an image'),mime:'image/jpeg'};
 for(const invoke of [()=>pilot.generateImage({group,source,type:'front',fetchImpl}),()=>pilot.preflightFit({group,source,styling:{fit:'Full length'},fetchImpl}),()=>pilot.generateSeo({group,source,fetchImpl}),()=>pilot.verifyImage({group,source,generated:source.buf,type:'front',fetchImpl})])await assert.rejects(invoke,error=>error.api.code==='invalid_source_image');
 assert.equal(calls,0);
});
test('the image edit uploads decoded PNG bytes with matching filename and MIME for AVIF sources',async()=>{
 const source={buf:await image().avif().toBuffer(),mime:'image/jpeg'};let calls=0;
 await pilot.generateImage({group,source,type:'front',fetchImpl:async(url,options)=>{
  calls++;const file=options.body.get('image');assert.equal(file.type,'image/png');assert.equal(file.name,'source.png');assert.equal((await sharp(Buffer.from(await file.arrayBuffer())).metadata()).format,'png');
  return {ok:true,json:async()=>({data:[{b64_json:'dGVzdA=='}]})};
 }});assert.equal(calls,1);
});

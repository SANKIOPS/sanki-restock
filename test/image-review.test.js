'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('fs'),os=require('os'),path=require('path');
const express=require('express'),sharp=require('sharp');
const {createImageReviewRouter,catalogueRows,statusFor}=require('../modules/image-review');
function product(){return {id:11,title:'Two-colour top',handle:'two-colour',status:'active',published_at:'2026-11-01T10:00:00Z',tags:'SANKI Collection: SANKI Casuals, SANKI Gender: Women, SANKI Category: Tops',options:[{name:'Color'},{name:'Size'}],variants:[{id:1,sku:'BLUE-S',title:'Blue / S',option1:'Blue',inventory_quantity:1},{id:2,sku:'BLUE-M',title:'Blue / M',option1:'Blue',inventory_quantity:0},{id:3,sku:'IVORY-S',title:'Ivory / S',option1:'Ivory',inventory_quantity:1}],images:[{id:9,src:'https://cdn.shopify.com/blue.png',position:1,variant_ids:[1,2]},{id:10,src:'https://cdn.shopify.com/ivory.png',position:2,variant_ids:[3]}]};}
test('catalogue groups sizes by colour, retains every gallery image, and fingerprints gallery changes',()=>{
  const p=product(),rows=catalogueRows([p]);assert.equal(rows.length,2);assert.equal(rows[0].variants.length,2);assert.equal(rows[0].stock,1);assert.equal(rows[0].images.length,2);assert.deepEqual(rows[0].collections,['Casuals']);assert.deepEqual(rows[0].genders,['Women']);
  const record={fingerprint:rows[0].fingerprint,decision:'correct'};assert.equal(statusFor(record,rows[0]),'correct');p.images[0].src='https://cdn.shopify.com/replaced.png';assert.equal(statusFor(record,catalogueRows([p])[0]),'website_changed');assert.equal(statusFor(null,rows[0]),'unreviewed');
});
test('owner workflow preserves originals, supports individual views, invalidates approvals, handles concurrent edits and never writes to Shopify',async t=>{
  const root=fs.mkdtempSync(path.join(os.tmpdir(),'image-review-')),p=product(),calls=[];
  const client={request:async(url,options)=>{calls.push({url,options});return{ok:true,headers:{get:()=>null},json:async()=>/products\/11\.json/.test(url)?{product:p}:{products:[p]}};}};
  const app=express();app.use(express.json());app.use((req,res,next)=>{req.user={username:'test-owner',roles:[req.headers['x-test-role']||'owner']};next();});app.use(createImageReviewRouter({root,store:'test.myshopify.com',client}));const server=app.listen(0,'127.0.0.1');await new Promise(r=>server.once('listening',r));t.after(()=>{server.close();fs.rmSync(root,{recursive:true,force:true});});const base='http://127.0.0.1:'+server.address().port+'/api/image-review';
  async function call(url,body,method='POST'){const r=await fetch(base+url,{method,headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});return{status:r.status,body:await r.json()};}
  const forbidden=await fetch(base+'/catalogue',{headers:{'x-test-role':'admin'}});assert.equal(forbidden.status,403);
  const listing=await(await fetch(base+'/catalogue?collection=Casuals&gender=Women')).json();assert.equal(listing.items.length,2);assert.equal(listing.summary.unreviewed,2);let item=listing.items[0];const url='/items/'+item.id,initial={revision:item.review.revision,fingerprint:item.fingerprint};
  let result=await call(url,{...initial,decision:'correct'},'PATCH');assert.equal(result.status,200);item=result.body.item;assert.equal(item.status,'correct');
  assert.equal((await call(url,{...initial,decision:'needs_correction'},'PATCH')).status,409);
  const png=await sharp({create:{width:40,height:60,channels:3,background:'#aabbcc'}}).png().toBuffer();
  async function photo(kind,view,bytes=png){const form=new FormData();form.append('revision',item.review.revision);form.append('fingerprint',item.fingerprint);form.append('kind',kind);form.append('view',view);form.append('image',new Blob([bytes],{type:'image/png'}),'photo.png');const r=await fetch(base+url+'/photos',{method:'POST',body:form});const d=await r.json();if(d.item)item=d.item;return{status:r.status,body:d};}
  assert.equal((await photo('proposal','front')).status,409);assert.equal((await photo('original','front',Buffer.from('not an image'))).status,400);
  assert.equal((await photo('original','front')).status,200);assert.equal(item.status,'ready_for_correction');const original=item.review.originals[0];assert.deepEqual(Buffer.from(await(await fetch(base.replace('/api/image-review','')+original.url)).arrayBuffer()),png);
  assert.equal((await photo('proposal','back')).status,409);
  await photo('proposal','front');let single=await call(url+'/approve',{revision:item.review.revision,fingerprint:item.fingerprint});assert.equal(single.status,200);item=single.body.item;assert.equal(item.status,'approved_for_upload');
  await photo('original','front');assert.equal(item.status,'ready_for_correction');assert.equal(item.review.approvedAt,null);assert.equal(item.review.proposals.length,0);
  assert.equal((await photo('original','back')).status,200);assert.equal(item.status,'ready_for_correction');assert.equal((await photo('original','detail')).status,200);assert.equal(item.status,'ready_for_correction');
  await photo('proposal','front');assert.equal(item.status,'awaiting_approval');let approval=await call(url+'/approve',{revision:item.review.revision,fingerprint:item.fingerprint});assert.equal(approval.status,409);
  await photo('proposal','back');await photo('proposal','detail');approval=await call(url+'/approve',{revision:item.review.revision,fingerprint:item.fingerprint});assert.equal(approval.status,200);item=approval.body.item;assert.equal(item.status,'approved_for_upload');
  await photo('original','front');assert.equal(item.review.originals.length,5);assert.equal(item.review.proposals.length,0);assert.equal(item.status,'ready_for_correction');assert.equal(item.review.approvedAt,null);
  await photo('proposal','front');await photo('proposal','back');await photo('proposal','detail');
  p.images[0].src='https://cdn.shopify.com/new-gallery.png';approval=await call(url+'/approve',{revision:item.review.revision,fingerprint:item.fingerprint});assert.equal(approval.status,409);
  const after=await(await fetch(base+'/catalogue?search=BLUE-S')).json();assert.equal(after.items[0].status,'website_changed');assert.equal(after.items.length,1);
  assert.ok(calls.every(c=>!c.options?.method||c.options.method==='GET'),'review must never mutate Shopify');
  const saved=JSON.parse(fs.readFileSync(path.join(root,'image-review.json')));assert.equal(saved.records[item.id].originals.length,5);assert.ok(saved.records[item.id].history.length>8);
  fs.writeFileSync(path.join(root,'image-review.json'),'{corrupt');const bad=await fetch(base+'/catalogue');assert.equal(bad.status,400);assert.equal(fs.readFileSync(path.join(root,'image-review.json'),'utf8'),'{corrupt');
});
test('Image Review is owner-only under Inventory, independent of Catalogue Images',()=>{
  const {visibleFor}=require('../modules/module-registry');const owner=visibleFor({username:'owner',roles:['owner']});assert.equal(owner.find(m=>m.key==='image-review').section,'Inventory');assert.ok(owner.some(m=>m.key==='catalogue-images'));assert.equal(visibleFor({username:'admin',roles:['admin']}).some(m=>m.key==='image-review'),false);
});

test('review screen exposes optional reference slots without separate proposed correction controls',()=>{
 const source=fs.readFileSync(path.join(__dirname,'../public/image-review.js'),'utf8');
 assert.ok(source.includes('one photo is enough to start'));assert.ok(source.includes("map(v=>slot(v))"));assert.ok(!source.includes('Upload proposed'));assert.ok(!source.includes('Approve correction set'));
});

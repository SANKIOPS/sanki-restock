'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const {applyPresentation,orderedIds}=require('../modules/image-review-presentation');
test('approval promotes five views, binds only this colour, merges mappings and requires live card verification',async()=>{
 const variants=[{id:'21',sku:'BLUE-S'},{id:'22',sku:'BLUE-M'}];
 const image=(id,variantIds=[])=>({id:String(id),src:'https://cdn.shopify.com/'+id+'.jpg',alt:'old',variantIds});
 const item={productId:'1',handle:'shirt',variants,images:[image(1,['11']),image(2,['21','22']),image(3)]};
 const job={id:'job',receipts:['front','detail','angle','back','product'].map((view,n)=>({view,newId:String(100+n),variantIds:[],stage:'replaced'}))};
 item.images.push(...job.receipts.map(r=>({...image(r.newId),alt:'SANKI review job '+r.view})));
 let fields=[{id:7,namespace:'sanki',key:'image_review_gallery',type:'json',value:JSON.stringify({version:1,variants:{11:{jobId:'other-colour'}}})}],ok=false,writes=[],events=[];
 const current=async()=>structuredClone(item);
 const shopify=async(url,options={})=>{
  if(!options.method)return{metafields:structuredClone(fields)};
  writes.push([url,options.method]);const b=JSON.parse(options.body);
  if(b.product)item.images=b.product.images.map(i=>item.images.find(x=>x.id===String(i.id)));
  if(b.image){for(const i of item.images)i.variantIds=i.id===String(b.image.id)?b.image.variant_ids:i.variantIds.filter(id=>!b.image.variant_ids.includes(id));}
  if(b.metafield)fields=[{...b.metafield,id:7}];return{};
 };
 const run=()=>applyPresentation({item,job,shopify,current,checkpoint:(_j,event)=>events.push(event),store:'test.myshopify.com',fetchPage:async url=>{assert.match(url,/view=sanki-review-verify&variant=21/);return{ok:true,text:async()=>ok?'SANKI_REVIEW_CARD:job:21':'old thumbnail'};}});
 assert.deepEqual(orderedIds(item,job),['1','100','101','102','103','104','2','3']);
 await assert.rejects(run,/live collection-card thumbnail is not confirmed/);
 assert.equal(job.presentation.verifiedAt,undefined);
 assert.deepEqual(item.images.find(i=>i.id==='100').variantIds,['21','22']);
 assert.deepEqual(item.images.find(i=>i.id==='1').variantIds,['11']);
 const map=JSON.parse(fields[0].value);assert.equal(map.variants['11'].jobId,'other-colour');assert.equal(map.variants['21'].front,'SANKI review job front');
 ok=true;await run();assert.ok(job.presentation.verifiedAt);
 assert.deepEqual(item.images.map(i=>i.id),['1','100','101','102','103','104','2','3']);
 assert.equal(writes.filter(([,m])=>m==='POST').length,0); // mapping update, no reuploads on retry
 assert.ok(events.includes('presentation-storefront-verified'));
 assert.deepEqual(item.variants,variants);
});
test('legacy single-view correction retains its publication behaviour',async()=>{
 const value={images:[]};assert.equal(await applyPresentation({job:{receipts:[{view:'front'}]},current:async()=>value}),value);
});

'use strict';
const ORDER=['front','detail','angle','back','product'];
const NAMESPACE='sanki',KEY='image_review_gallery';
const fail=message=>Object.assign(new Error(message),{status:409});
const marker=(job,view)=>'SANKI review '+job.id+' '+view;
function fullSet(job){return ORDER.every(view=>job.receipts.some(r=>r.view===view));}
function orderedIds(item,job){
 const ids=ORDER.map(view=>job.receipts.find(r=>r.view===view).newId),made=new Set(ids),own=new Set(item.variants.map(v=>String(v.id)));
 const rest=item.images.filter(i=>!made.has(i.id));
 // Insert at this colour's first former image, preserving every other image and its relative order.
 const anchor=rest.findIndex(i=>i.variantIds.some(id=>own.has(String(id))));
 const index=anchor<0?(new Set(item.images.flatMap(i=>i.variantIds)).size>own.size?rest.length:0):anchor;
 return [...rest.slice(0,index).map(i=>i.id),...ids,...rest.slice(index).map(i=>i.id)];
}
async function applyPresentation({item,job,shopify,current,checkpoint,fetchPage,store}){
 if(!fullSet(job))return current();
 const front=job.receipts.find(r=>r.view==='front'),variantIds=item.variants.map(v=>String(v.id));
 if(!job.presentation){job.presentation={version:1,variantIds,frontId:front.newId};checkpoint(job,'presentation-started');}
 let live=await current();
 if(!job.presentation.order){job.presentation.order=orderedIds(live,job);checkpoint(job,'presentation-order-planned');}
 const ids=job.presentation.order;
 if(ids.length!==live.images.length||ids.some(id=>!live.images.some(i=>i.id===id)))throw fail('The gallery changed during thumbnail publishing. Review before retrying.');
 await shopify('/products/'+item.productId+'.json',{method:'PUT',body:JSON.stringify({product:{id:Number(item.productId),images:ids.map((id,n)=>({id:Number(id),position:n+1}))}})});
 await shopify('/products/'+item.productId+'/images/'+front.newId+'.json',{method:'PUT',body:JSON.stringify({image:{id:Number(front.newId),variant_ids:variantIds}})});
 live=await current();
 if(JSON.stringify(live.images.map(i=>i.id))!==JSON.stringify(ids))throw fail('Website image order was not confirmed. Retry publishing.');
 const actual=live.images.find(i=>i.id===front.newId);
 if(JSON.stringify([...actual.variantIds].sort())!==JSON.stringify([...variantIds].sort())||live.images.some(i=>i.id!==front.newId&&i.variantIds.some(id=>variantIds.includes(id))))throw fail('The new front thumbnail is not assigned to every size of this colour. Retry publishing.');
 for(const receipt of job.receipts)receipt.variantIds=receipt===front?variantIds:receipt.variantIds.filter(id=>!variantIds.includes(id));
 const endpoint='/products/'+item.productId+'/metafields.json';
 const fields=(await shopify(endpoint+'?namespace='+NAMESPACE+'&key='+KEY)).metafields||[],field=fields.find(f=>f.namespace===NAMESPACE&&f.key===KEY);
 let value={version:1,variants:{}};
 if(field){try{value=JSON.parse(field.value);}catch{throw fail('The approved-gallery mapping needs repair. Existing data was kept.');}if(value.version!==1||!value.variants||typeof value.variants!=='object'||Array.isArray(value.variants))throw fail('Unsupported approved-gallery mapping. Existing data was kept.');}
 const entry={jobId:job.id,front:marker(job,'front'),detail:marker(job,'detail'),order:ORDER.map(v=>marker(job,v))};
 for(const id of variantIds)value.variants[id]=entry;
 const metafield={namespace:NAMESPACE,key:KEY,type:'json',value:JSON.stringify(value)};
 await shopify(field?'/metafields/'+field.id+'.json':endpoint,{method:field?'PUT':'POST',body:JSON.stringify({metafield:field?{...metafield,id:field.id}:metafield})});
 const saved=((await shopify(endpoint+'?namespace='+NAMESPACE+'&key='+KEY)).metafields||[]).find(f=>f.namespace===NAMESPACE&&f.key===KEY);
 if(!saved||saved.value!==metafield.value)throw fail('The approved thumbnail mapping was not confirmed. Retry publishing.');
 checkpoint(job,'presentation-assigned');
 // The theme emits this marker only after selecting both the exact approved front and detail.
 // Public requests carry no Admin credentials. A failed check retains uploaded media for retry.
 const url='https://'+store+'/products/'+encodeURIComponent(item.handle)+'?view=sanki-review-verify&variant='+encodeURIComponent(variantIds[0])+'&sanki_review='+encodeURIComponent(job.id);
 const response=await fetchPage(url,{timeout:30000,size:5*1024*1024});
 if(!response.ok)throw fail('The storefront could not be checked. Retry publishing; uploaded images are saved.');
 const html=await response.text();
 const confirmed=variantIds.some(id=>html.includes('SANKI_REVIEW_CARD:'+job.id+':'+id));
 if(!confirmed)throw fail('Images are uploaded, but the live collection-card thumbnail is not confirmed. Retry publishing after checking storefront visibility.');
 job.presentation.verifiedAt=new Date().toISOString();checkpoint(job,'presentation-storefront-verified');
 return current();
}
module.exports={applyPresentation,orderedIds,fullSet};

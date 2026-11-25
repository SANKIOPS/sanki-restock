'use strict';
const fs=require('fs'),path=require('path'),crypto=require('crypto');
const VIEWS=['front','back','detail'];
const {OUTPUT_VIEWS,outputViews}=require('./image-review-views');
const {applyPresentation}=require('./image-review-presentation');
const fail=(message,status=409)=>Object.assign(new Error(message),{status});
module.exports=function installQueue({router,wrap,context,read,save,persist,present,catalogue,client,store,directory,fetchImage=require('node-fetch'),updateProduct}){
 const publishing=new Set();
 const latestRefs=r=>VIEWS.filter(v=>!r.pendingViews||r.pendingViews.includes(v)).map(v=>[...(r.originals||[])].reverse().find(f=>f.view===v)).filter(Boolean);
 function validJob(record,id){const j=record.job;if(!j||j.id!==id)throw fail('This job was replaced. Reopen the product.');if(j.referenceSource!=='website_product_only'&&j.referenceKey!==JSON.stringify((j.recreateAll?VIEWS.map(v=>[...(record.originals||[])].reverse().find(f=>f.view===v)).filter(Boolean):latestRefs(record)).map(f=>f.id)))throw fail('Reference photos changed. Create images again.');return j;}
 router.get('/api/image-review/queue',wrap(async(req,res)=>{const items=await catalogue(),state=read();res.json({success:true,items:items.filter(i=>state.records[i.id]?.job&&['queued','processing','ready','publish_failed','publishing'].includes(state.records[i.id].job.status)).map(i=>present(i,state.records[i.id]))});}));
 router.get('/api/image-review/items/:id',wrap(async(req,res)=>{const item=(await catalogue()).find(i=>i.id===req.params.id);if(!item)throw fail('Product not found.',404);res.json({success:true,item:present(item,read().records[item.id])});}));
 router.post('/api/image-review/items/:id/queue',wrap(async(req,res)=>{
  const {item,state,record}=await context(req,true);if(publishing.has(item.productId)||record.job?.status==='publishing')throw fail('Publishing is in progress.');
  const recreate=req.body.recreateAll===true;
  if(recreate&&['queued','processing'].includes(record.job?.status))throw fail('This product already has an active image job.');
  let refs=recreate?VIEWS.map(v=>[...(record.originals||[])].reverse().find(f=>f.view===v)).filter(Boolean):latestRefs(record);
  let referenceSource='originals';
  if(recreate&&req.body.sourceImageId){
   const image=item.images.find(i=>i.id===String(req.body.sourceImageId));if(!image)throw fail('Choose a current product-only photo.',400);
   const own=new Set(item.variants.map(v=>v.id));
   if(image.variantIds.some(id=>!own.has(id))||(!image.variantIds.length&& (await catalogue()).some(i=>i.productId===item.productId&&i.id!==item.id)))throw fail('Choose a photo assigned to this colour.');
   const source=new URL(image.src);if(source.protocol!=='https:'||source.hostname!=='cdn.shopify.com')throw fail('Reference must be a Shopify CDN photo.');
   const response=await fetchImage(source.toString(),{redirect:'error',size:20*1024*1024,timeout:30000});if(!response.ok)throw fail('Could not save the reference photo. Try again.');
   const bytes=await response.buffer();if(bytes.length>20*1024*1024)throw fail('Reference photo is too large.');
   const meta=await require('sharp')(bytes,{limitInputPixels:80000000}).metadata();await require('sharp')(bytes,{limitInputPixels:80000000}).stats();if(!['jpeg','png','webp'].includes(meta.format)||!meta.width||!meta.height)throw fail('Reference photo format is unsupported.');
   fs.mkdirSync(directory,{recursive:true});const id=crypto.randomUUID(),file=id+'.'+(meta.format==='jpeg'?'jpg':meta.format);fs.writeFileSync(path.join(directory,file),bytes,{flag:'wx'});
   refs=[{id,file,view:'front',kind:'website_reference',sourceImageId:image.id,sourceUrl:image.src,confirmedBy:req.user.username,uploadedAt:new Date().toISOString(),sha256:crypto.createHash('sha256').update(bytes).digest('hex')}];referenceSource='website_product_only';
  }
  if(!refs.length)throw fail('Upload at least one photo for a view that needs fixing.');
  const targets={};const all=await catalogue();
  for(const ref of refs){const target=recreate?'add':String(req.body.targets?.[ref.view]||'');if(target!=='add'){
   const image=item.images.find(i=>i.id===target);if(!image)throw fail('Choose which website photo to replace for '+ref.view+'.');
   const own=new Set(item.variants.map(v=>v.id)),others=all.some(i=>i.productId===item.productId&&i.id!==item.id);
   if(image.variantIds.some(id=>!own.has(id))||(others&&!image.variantIds.length))throw fail('This website photo is shared with another colour. Choose Add new photo or resolve its colour assignment first.');
   if(Object.values(targets).includes(target))throw fail('Choose a different website photo for each corrected view.');
  }targets[ref.view]=target;}
  const plannedViews=!recreate&&req.body.fullSet===false?refs.map(f=>f.view):OUTPUT_VIEWS;
  for(const view of plannedViews)if(!targets[view])targets[view]='add';
  if(record.job)record.history.push({event:'job-replaced',job:record.job,at:new Date().toISOString()});
  if(record.proposals.length)record.history.push({event:'proposals-superseded',files:record.proposals,at:new Date().toISOString()});
  record.proposals=[];record.approvedAt=null;record.approvedProposalRevision=null;
  record.job={id:crypto.randomUUID(),status:'queued',createdAt:new Date().toISOString(),referenceSource,recreateAll:recreate,referenceKey:JSON.stringify(refs.map(f=>f.id)),references:refs.map(f=>({...f})),outputViews:plannedViews,generationViews:plannedViews,targets,fingerprint:item.fingerprint,notes:String(req.body.notes||''),gallery:item.images,receipts:[]};
  record.decision='needs_correction';res.json({success:true,item:persist(item,state,record,req,'job-queued')});
 }));
 router.post('/api/image-review/items/:id/claim',wrap(async(req,res)=>{
  const {item,state,record}=await context(req,true),job=validJob(record,req.body.jobId);
  if(job.fingerprint!==item.fingerprint)throw fail('The website gallery changed. Review and create images again.');
  if(job.status==='processing'&&Date.parse(job.leaseUntil)>Date.now())throw fail('This job is already being processed.');
  if(!['queued','processing'].includes(job.status))throw fail('This job is not waiting for generation.');
  job.status='processing';job.lease=crypto.randomUUID();job.leaseUntil=new Date(Date.now()+60*60*1000).toISOString();job.claimedBy=req.user.username;
  res.json({success:true,item:persist(item,state,record,req,'job-claimed')});
 }));
 router.get('/api/image-review/items/:id/packet',wrap(async(req,res)=>{
  const item=(await catalogue()).find(i=>i.id===req.params.id),record=read().records[req.params.id];if(!item||!record?.job)throw fail('Queue job not found.',404);
  const job=validJob(record,record.job.id);if(job.fingerprint!==item.fingerprint)throw fail('The website gallery changed. Requeue after reviewing it.');
  res.set('Cache-Control','private, no-store');res.attachment('image-job-'+job.id+'.json').json({version:1,item:present(item,record),job,originals:job.references.map(f=>({...f,base64:fs.readFileSync(path.join(directory,f.file)).toString('base64')})),instructions:(job.referenceSource==='website_product_only'?'The attached reference is an existing website product-only image explicitly selected as correct by the reviewer, not a raw garment photo. Preserve visible design exactly; if a requested view requires unseen construction, flag it for original evidence rather than inventing it. ':'')+'Use the attached references as design evidence. Generate every outputViews entry, using the attached references as shared design evidence; no separate raw angle/product photo is needed. On revisions regenerate only generationViews and retain other results. feedbackByView identifies the exact photo correction. Preserve exact colour, construction, pockets, trims, texture and sold components. Follow the current approved SANKI image/style SOP. Do not infer missing garment details from old generated website images. Return each generated view to this job with its lease, then complete only after visual fidelity checks. Never publish from the worker.'});
 }));
 router.post('/api/image-review/items/:id/complete',wrap(async(req,res)=>{
  const {item,state,record}=await context(req,true),job=validJob(record,req.body.jobId);
  if(job.fingerprint!==item.fingerprint)throw fail('The website gallery changed. Review and create images again.');
  if(job.status!=='processing'||job.lease!==req.body.lease||Date.parse(job.leaseUntil)<Date.now())throw fail('The generation claim expired. Claim the job again.');
  if(!outputViews(job).every(view=>record.proposals.some(f=>f.view===view&&f.jobId===job.id)))throw fail('Return an image for every requested view first.');
  if(!String(req.body.qa||'').trim())throw fail('Record the visual garment-fidelity checks first.');
  job.status='ready';job.generationViews=[];job.qa=String(req.body.qa).slice(0,4000);job.completedAt=new Date().toISOString();res.json({success:true,item:persist(item,state,record,req,'job-ready')});
 }));
 router.post('/api/image-review/items/:id/revise',wrap(async(req,res)=>{
  const {item,state,record}=await context(req,true),job=validJob(record,req.body.jobId);if(!['ready','queued','processing'].includes(job.status))throw fail('This job cannot be revised now.');
  const planned=req.body.fullSet===true?OUTPUT_VIEWS:outputViews(job);
  const feedbackByView={};
  for(const [view,text] of Object.entries(req.body.viewFeedback||{})){
   if(!planned.includes(view))throw fail('Unknown photo view.',400);
   if(typeof text!=='string')throw fail('Photo feedback must be text.',400);
   if(text.trim())feedbackByView[view]=text.trim().slice(0,2000);
  }
  const globalFeedback=String(req.body.feedback||'').trim().slice(0,4000);
  const missing=planned.filter(view=>!record.proposals.some(f=>f.view===view&&f.jobId===job.id));
  const regenerate=[...new Set([...Object.keys(feedbackByView),...missing,...(globalFeedback?planned:[])])];
  if(!regenerate.length)throw fail('Write a correction under the photo you want changed.');
  record.history.push({event:'revision-requested',files:record.proposals,job:JSON.parse(JSON.stringify(job)),at:new Date().toISOString()});
  record.proposals=record.proposals.filter(f=>!regenerate.includes(f.view));
  job.outputViews=[...planned];for(const view of planned)if(!job.targets[view])job.targets[view]='add';
  job.generationViews=regenerate;job.feedbackByView=feedbackByView;
  job.status='queued';job.feedback=globalFeedback||Object.entries(feedbackByView).map(([view,text])=>view+': '+text).join('\n');job.lease=null;job.qa=null;record.approvedAt=null;record.approvedProposalRevision=null;
  res.json({success:true,item:persist(item,state,record,req,'job-requeued')});
 }));
 async function shopify(url,options){const response=await client.request(`https://${store}/admin/api/2026-07${url}`,{...options,maxRetries:options?.method==='POST'?0:undefined});const body=response.status===204?{}:await response.json();if(!response.ok)throw fail('Shopify did not finish the image update ('+response.status+'). Retry publishing; completed uploads are retained.');return body;}
 router.post('/api/image-review/items/:id/publish',wrap(async(req,res)=>{
  // All writes require this explicit owner action and the exact current result revision.
  const initial=(await catalogue()).find(i=>i.id===req.params.id);if(!initial)throw fail('Product not found.',404);if(publishing.has(initial.productId))throw fail('This product is already publishing.');publishing.add(initial.productId);
  let started=false;
  const checkpoint=(job,event)=>{const state=read(),r=state.records[req.params.id];if(r.job.id!==job.id)throw fail('Job changed during publishing.');r.job=job;r.revision++;r.updatedAt=new Date().toISOString();r.history.push({event,at:r.updatedAt,by:req.user.username});save(state);};
  try{
   const {item,record}=await context(req,true),job=validJob(record,req.body.jobId);
   if(!['ready','publish_failed','publishing'].includes(job.status))throw fail('Wait for generated images before approving.');
   if(job.fingerprint!==item.fingerprint&&!job.receipts.length&&job.status==='ready')throw fail('Website images changed. Review and requeue before publishing.');
   const outputs=outputViews(job).map(view=>record.proposals.find(f=>f.view===view&&f.jobId===job.id));if(outputs.some(f=>!f))throw fail('Some generated views are missing.');
   // On retries, verify the gallery matches our last saved checkpoint; do not overwrite outside edits.
   if(job.receipts.length||job.status==='publish_failed'||job.status==='publishing'){
    const targets=new Set(Object.values(job.targets)),made=new Set(job.receipts.map(r=>r.newId));
    const assigned=new Set(job.presentation?.variantIds||[]);
    const comparable=images=>images.filter(i=>!targets.has(i.id)&&!made.has(i.id)&&!i.alt.startsWith('SANKI review '+job.id+' ')).map(i=>({id:i.id,src:i.src,alt:i.alt,variantIds:i.variantIds.filter(id=>!assigned.has(id))})).sort((a,b)=>a.id.localeCompare(b.id));
    if(JSON.stringify(comparable(job.gallery))!==JSON.stringify(comparable(item.images)))throw fail('Other website images changed. Review before retrying.');
    for(const old of job.gallery.filter(i=>targets.has(i.id))){const live=item.images.find(i=>i.id===old.id);if(live&&(live.src!==old.src||JSON.stringify(live.variantIds)!==JSON.stringify(old.variantIds)))throw fail('A selected replacement photo changed. Review before retrying.');}
   }
   job.status='publishing';job.approvedAt=job.approvedAt||new Date().toISOString();job.approvedBy=req.user.username;checkpoint(job,'publish-approved');started=true;
   const current=async()=>{const body=await shopify('/products/'+item.productId+'.json?fields=id,title,handle,status,published_at,product_type,tags,variants,images,options');return updateProduct(body.product).find(i=>i.id===item.id);};
   for(const output of outputs){
    let receipt=job.receipts.find(r=>r.view===output.view);const marker='SANKI review '+job.id+' '+output.view;
    if(!receipt){
     // Recover a successful POST whose response/checkpoint was interrupted, without creating a duplicate.
     const live=await current(),recovered=live.images.find(i=>i.alt===marker);
     const target=job.targets[output.view],old=target==='add'?null:live.images.find(i=>i.id===target);if(target!=='add'&&!old&&!recovered)throw fail('The selected replacement photo is missing.');
     const body=recovered?{image:recovered}:await shopify('/products/'+item.productId+'/images.json',{method:'POST',body:JSON.stringify({image:{attachment:fs.readFileSync(path.join(directory,output.file)).toString('base64'),alt:marker}})});
     if(!body.image?.id||!body.image.src)throw fail('Shopify has not confirmed the new photo.');
     receipt={view:output.view,newId:String(body.image.id),oldId:old?.id||null,oldImage:old||null,position:old?.position||live.images.length+1,variantIds:old?.variantIds||[],stage:'uploaded'};job.receipts.push(receipt);job.expectedFingerprint=(await current()).fingerprint;checkpoint(job,'publish-uploaded:'+output.view);
    }
    if(receipt.stage==='uploaded'){
     await shopify('/products/'+item.productId+'/images/'+receipt.newId+'.json',{method:'PUT',body:JSON.stringify({image:{id:Number(receipt.newId),position:receipt.position,variant_ids:receipt.variantIds}})});
     if(receipt.oldId&&!receipt.backup){
      const source=new URL(receipt.oldImage.src);if(source.protocol!=='https:'||source.hostname!=='cdn.shopify.com')throw fail('Original image backup requires a Shopify CDN source.');
      const response=await fetchImage(source.toString(),{redirect:'error',size:20*1024*1024,timeout:30000});if(!response.ok)throw fail('Could not back up the existing photo. Nothing was removed.');const bytes=await response.buffer();
      const format=(await require('sharp')(bytes).metadata()).format;if(!['jpeg','png','webp'].includes(format))throw fail('Cannot archive this source image format. Nothing was removed.');
      const filename=crypto.randomUUID()+'.'+(format==='jpeg'?'jpg':format);fs.writeFileSync(path.join(directory,filename),bytes,{flag:'wx'});receipt.backup={file:filename,sha256:crypto.createHash('sha256').update(bytes).digest('hex')};checkpoint(job,'publish-backup:'+output.view);
     }
     if(receipt.oldId&&(await current()).images.some(i=>i.id===receipt.oldId))await shopify('/products/'+item.productId+'/images/'+receipt.oldId+'.json',{method:'DELETE'});
     receipt.stage='replaced';job.expectedFingerprint=(await current()).fingerprint;checkpoint(job,'publish-replaced:'+output.view);
    }
   }
   const verified=await applyPresentation({item,job,shopify,current,checkpoint,fetchPage:fetchImage,store});for(const receipt of job.receipts){const image=verified.images.find(i=>i.id===receipt.newId);if(!image||JSON.stringify([...image.variantIds].sort())!==JSON.stringify([...receipt.variantIds].sort())||receipt.oldId&&verified.images.some(i=>i.id===receipt.oldId))throw fail('Shopify gallery verification is incomplete.');}
   job.status='published';delete job.error;job.publishedAt=new Date().toISOString();job.expectedFingerprint=verified.fingerprint;
   const state=read(),r=state.records[item.id];r.job=job;r.pendingViews=[];r.decision='unreviewed';r.approvedAt=job.approvedAt;r.approvedProposalRevision=r.proposalRevision;
   res.json({success:true,item:persist(verified,state,r,req,'job-published')});
  }catch(error){if(started){const state=read(),r=state.records[req.params.id];r.job.status='publish_failed';r.job.error=error.message;r.revision++;save(state);try{const fresh=(await catalogue(true)).find(i=>i.id===req.params.id);if(fresh)error.item=present(fresh,r);}catch{}}throw error;}finally{publishing.delete(initial.productId);}
 }));
};

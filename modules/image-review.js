'use strict';
const express = require('express');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const multer = require('multer');
const sharp = require('sharp');
const { shopifyClient } = require('./shopify-client');
const ORIGINAL_VIEWS = ['front', 'back', 'detail'];
const {OUTPUT_VIEWS,outputViews}=require('./image-review-views');
const hash = value => crypto.createHash('sha256').update(JSON.stringify(value)).digest('hex');
const tagsOf = p => Array.isArray(p.tags) ? p.tags : String(p.tags || '').split(',').map(t => t.trim());
function tagged(p, prefix) { return tagsOf(p).filter(t => t.toLowerCase().startsWith(prefix.toLowerCase())).map(t => t.slice(prefix.length).trim()).filter(Boolean); }
function categoryLabel(value) {
  const text=String(value||'Unclassified').trim(),key=text.toLowerCase().replace(/^women\s+/,'').replace(/[\s_-]+/g,'');
  const names={top:'Tops',tops:'Tops',shirt:'Shirts',shirts:'Shirts',tshirt:'T-Shirts',tshirts:'T-Shirts',tee:'T-Shirts',tees:'T-Shirts',trouser:'Trousers',trousers:'Trousers',jeans:'Jeans',blazer:'Blazers',blazers:'Blazers',hoodie:'Hoodies',hoodies:'Hoodies',jacket:'Jackets',jackets:'Jackets',sweater:'Sweaters',sweaters:'Sweaters',sweatshirt:'Sweatshirts',sweatshirts:'Sweatshirts',short:'Shorts',shorts:'Shorts',coordset:'Co-ord Sets',coordsets:'Co-ord Sets','2pcsset':'Co-ord Sets',lower:'Joggers & Lowers',lowers:'Joggers & Lowers',joggers:'Joggers & Lowers'};
  return names[key]||text;
}
function catalogueRows(products) {
  return [...products].sort((a,b) => String(b.published_at || '').localeCompare(String(a.published_at || '')) || String(a.id).localeCompare(String(b.id))).flatMap(p => {
    const option = (p.options || []).findIndex(o => /^colou?r$/i.test(o.name || ''));
    const groups = new Map();
    for (const v of p.variants || []) {
      const colour = String(option < 0 ? 'Unspecified' : v['option' + (option + 1)] || 'Unspecified').trim();
      const key = colour.toLowerCase();
      if (!groups.has(key)) groups.set(key, { colour, variants: [] });
      groups.get(key).variants.push({ id: String(v.id), sku: String(v.sku || ''), size: String(v.title || ''), stock: Number(v.inventory_quantity || 0) });
    }
    if (!groups.size) groups.set('unspecified', { colour:'Unspecified', variants:[] });
    const images = (p.images || []).filter(i => /^https:\/\//i.test(i.src || '')).map((i,index) => ({ id:String(i.id), src:i.src, alt:String(i.alt || ''), position:Number(i.position || index + 1), variantIds:(i.variant_ids || []).map(String), updatedAt:i.updated_at || '' })).sort((a,b) => a.position-b.position);
    return [...groups.entries()].map(([key,g]) => {
      const id = String(p.id) + '-' + hash(key).slice(0,12);
      const fingerprint = hash({ productId:String(p.id), colour:key, variants:g.variants.map(v => v.id).sort(), images });
      return { id, productId:String(p.id), title:p.title, handle:p.handle, colour:g.colour, variants:g.variants, images, fingerprint,
        collections:tagged(p,'SANKI Collection:').map(t => t.replace(/^SANKI\s+/i,'')), genders:tagged(p,'SANKI Gender:'), category:categoryLabel(tagged(p,'SANKI Category:')[0] || p.product_type),
        shopifyStatus:p.status, publishedAt:p.published_at || null, stock:g.variants.reduce((n,v) => n+Math.max(v.stock,0),0) };
    });
  });
}
function statusFor(record, item) {
  if (!record) return 'unreviewed';
  if(record.job&&['publishing','publish_failed'].includes(record.job.status))return record.job.status;
  if (record.fingerprint !== item.fingerprint) return 'website_changed';
  if(record.job&&['queued','processing','ready','publishing','publish_failed','published'].includes(record.job.status))return record.job.status;
  if (record.decision === 'correct') return 'correct';
  if (record.decision === 'unreviewed') return 'unreviewed';
  if (record.proposals?.length) {
    const latest = record.proposalRevision || 0;
    return record.approvedProposalRevision === latest ? 'approved_for_upload' : 'awaiting_approval';
  }
  if (record.decision === 'needs_correction') return record.originals?.length > 0 ? 'ready_for_correction' : 'awaiting_originals';
  return 'unreviewed';
}
function createImageReviewRouter(options = {}) {
  const router = express.Router();
  const root = options.root || (process.env.DATA_PATH ? path.dirname(process.env.DATA_PATH) : path.join(__dirname,'..'));
  const file = path.join(root,'image-review.json'), directory = path.join(root,'image-review-files');
  const store = options.store || process.env.SHOPIFY_STORE || '';
  const client = options.client || shopifyClient;
  let cache = null, syncedAt = null, inFlight = null;
  const read = () => {
    if (!fs.existsSync(file)) return { version:1, records:{} };
    const state = JSON.parse(fs.readFileSync(file,'utf8'));
    if (state.version !== 1 || !state.records || typeof state.records !== 'object') throw new Error('Image review store needs repair; no data was overwritten.');
    return state;
  };
  const save = state => {
    fs.mkdirSync(root,{recursive:true});
    const temp = file + '.tmp-' + crypto.randomUUID();
    fs.writeFileSync(temp,JSON.stringify(state,null,2));fs.renameSync(temp,file);
  };
  async function catalogue(refresh = false) {
    if (!refresh && cache && Date.now() - Date.parse(syncedAt) < 5*60*1000) return cache;
    if (inFlight) return inFlight;
    inFlight = (async () => {
      const products = [];let url = `https://${store}/admin/api/2026-07/products.json?limit=250&fields=id,title,handle,status,published_at,product_type,tags,variants,images,options`;
      while (url) {
        const response = await client.request(url);if (!response.ok) throw new Error('Could not read Shopify catalogue ('+response.status+').');
        const body = await response.json();if (!Array.isArray(body.products)) throw new Error('Invalid Shopify catalogue response.');products.push(...body.products);
        const match = (response.headers.get('link') || '').match(/<([^>]+)>;\s*rel="next"/);url = match ? match[1] : null;
        if (url && new URL(url).hostname !== store) throw new Error('Unexpected Shopify pagination host.');
      }
      cache = catalogueRows(products);syncedAt = new Date().toISOString();return cache;
    })();
    try { return await inFlight; } finally { inFlight=null; }
  }
  const publicFile = f => ({ ...f, url:'/api/image-review/files/'+f.file });
  function present(item, record) {
    return { ...item, status:statusFor(record,item), review:{ job:record?.job||null, pendingViews:record?.pendingViews||null, revision:record?.revision || 0, notes:record?.notes || '', decision:record?.decision || '', reviewedAt:record?.reviewedAt || null, originals:(record?.originals || []).map(publicFile), proposals:(record?.proposals || []).map(publicFile), approvedAt:record?.approvedAt || null } };
  }
  async function context(req, fresh = false) {
    let item = (await catalogue()).find(i => i.id === req.params.id);if (!item) { const error=new Error('Product colour not found. Refresh the catalogue.');error.status=404;throw error; }
    if(fresh){
      const response=await client.request(`https://${store}/admin/api/2026-07/products/${encodeURIComponent(item.productId)}.json?fields=id,title,handle,status,published_at,product_type,tags,variants,images,options`);
      if(!response.ok){const error=new Error('Could not verify the current Shopify gallery. No review was saved.');error.status=409;throw error;}
      const body=await response.json();if(!body.product)throw new Error('Invalid Shopify product response.');
      const current=catalogueRows([body.product]);cache=cache.filter(i=>i.productId!==item.productId).concat(current).sort((a,b)=>String(b.publishedAt||'').localeCompare(String(a.publishedAt||'')));
      item=current.find(i=>i.id===req.params.id);if(!item){const error=new Error('The product colour changed. Refresh the catalogue.');error.status=409;throw error;}
    }
    const state=read(), previous=state.records[item.id];
    if (!Number.isInteger(Number(req.body.revision)) || Number(req.body.revision) !== (previous?.revision || 0)) { const error=new Error('This product was updated in another window. Reload it before saving.');error.status=409;throw error; }
    if (req.body.fingerprint !== item.fingerprint) { const error=new Error('The website gallery changed. Reload and review the current images.');error.status=409;throw error; }
    if(previous?.job&&((previous.job.status==='publishing')||(previous.job.status==='publish_failed'))&&!req.path.endsWith('/publish')){const error=new Error('Finish or retry this product’s publishing before editing it.');error.status=409;throw error;}
    return { item, state, record:previous || { revision:0, originals:[], proposals:[], history:[] } };
  }
  function persist(item,state,record,req,event) {
    const latestState=read();if((latestState.records[item.id]?.revision||0)!==record.revision){const error=new Error('This product was updated in another window. Reload before saving.');error.status=409;throw error;}state=latestState;
    if(typeof req.body.notes==='string')record.notes=req.body.notes.slice(0,4000);
    record.gallerySnapshots=record.gallerySnapshots||[];
    if(!record.gallerySnapshots.some(s=>s.fingerprint===item.fingerprint))record.gallerySnapshots.push({fingerprint:item.fingerprint,images:item.images,at:new Date().toISOString()});
    record.revision++;record.fingerprint=item.fingerprint;record.updatedAt=new Date().toISOString();record.updatedBy=req.user.username;
    record.history.push({event,at:record.updatedAt,by:req.user.username});state.records[item.id]=record;save(state);return present(item,record);
  }
  const wrap = fn => async (req,res,next) => { try { await fn(req,res); } catch(error) { next(error); } };
  router.use('/api/image-review',(req,res,next) => require('./image-review-access').canAccess(req.user,req.originalUrl.split('?')[0],req.method) ? next() : res.status(403).json({success:false,error:'This action is restricted to the Owner.'}));
  router.get('/api/image-review/catalogue',wrap(async(req,res) => {
    const items=await catalogue(), state=read();
    const inCollection=items.filter(i=>!req.query.collection||i.collections.includes(req.query.collection));
    const inGender=inCollection.filter(i=>!req.query.gender||i.genders.includes(req.query.gender));
    const facets={collections:[...new Set(items.flatMap(i=>i.collections))].sort(),genders:[...new Set(inCollection.flatMap(i=>i.genders))].sort(),categories:[...new Set(inGender.map(i=>i.category))].sort()};
    let filtered=inGender.filter(i=>!req.query.category||i.category===req.query.category);
    const search=String(req.query.search || '').toLowerCase();if(search)filtered=filtered.filter(i => [i.title,i.colour,i.handle,...i.variants.map(v=>v.sku)].join(' ').toLowerCase().includes(search));
    const summary={};for(const item of filtered){const status=statusFor(state.records[item.id],item);summary[status]=(summary[status]||0)+1;}
    if(req.query.status)filtered=filtered.filter(i=>statusFor(state.records[i.id],i)===req.query.status);
    const offset=Math.max(0,Math.floor(Number(req.query.offset)||0)), limit=Math.min(48,Math.max(1,Math.floor(Number(req.query.limit)||24)));
    res.json({success:true,canPublish:(req.user.roles||[req.user.role]).includes('owner'),syncedAt,facets,summary,total:filtered.length,items:filtered.slice(offset,offset+limit).map(i=>present(i,state.records[i.id]))});
  }));
  router.post('/api/image-review/sync',wrap(async(req,res)=>{await catalogue(true);res.json({success:true,syncedAt});}));
  router.patch('/api/image-review/items/:id',wrap(async(req,res)=>{
    if(!['correct','needs_correction','unreviewed'].includes(req.body.decision))return res.status(400).json({success:false,error:'Choose a review decision.'});
    const {item,state,record}=await context(req,true);
    if(req.body.decision==='correct'&&!item.images.length)return res.status(400).json({success:false,error:'A product without website images cannot be marked correct.'});
    record.decision=req.body.decision;record.notes=String(req.body.notes || '').slice(0,4000);record.reviewedAt=new Date().toISOString();
    // Approval is tied to both website fingerprint and exact staged proposal revision.
    record.approvedProposalRevision=null;record.approvedAt=null;
    res.json({success:true,item:persist(item,state,record,req,'decision:'+record.decision)});
  }));
  const upload=multer({storage:multer.memoryStorage(),limits:{fileSize:20*1024*1024,files:1}});
  router.post('/api/image-review/items/:id/photos',upload.single('image'),wrap(async(req,res)=>{
    if(!(req.user.roles||[req.user.role]).includes('owner')&&req.body.kind!=='original')return res.status(403).json({success:false,error:'Image Reviewers can upload original references only.'});
    const kind=req.body.kind,view=String(req.body.view || '');
    if(!['original','proposal'].includes(kind)||!(kind==='original'?ORIGINAL_VIEWS:OUTPUT_VIEWS).includes(view))return res.status(400).json({success:false,error:'Choose original/proposal and front/back/detail.'});
    if(!req.file)return res.status(400).json({success:false,error:'Choose a front, back or detail image.'});
    // Decode bytes; do not trust file extensions or browser MIME types. Preserve original upload bytes.
    const metadata=await sharp(req.file.buffer,{limitInputPixels:80000000}).metadata();
    if(!['jpeg','png','webp'].includes(metadata.format)||metadata.pages>1)return res.status(400).json({success:false,error:'Use a single JPEG, PNG or WebP photo.'});
    await sharp(req.file.buffer,{limitInputPixels:80000000}).stats();
    const {item,state,record}=await context(req);
    if(record.job&&kind==='proposal'&&(record.job.id!==req.body.jobId||record.job.status!=='processing'||record.job.lease!==req.body.lease||Date.parse(record.job.leaseUntil)<Date.now()))return res.status(409).json({success:false,error:'Claim the current queue job before returning images.'});
    if(record.job&&kind==='proposal'&&!outputViews(record.job).includes(view))return res.status(409).json({success:false,error:'This view was not requested in the queue job.'});
    if(kind==='proposal'&&!record.job&&!record.originals.some(f=>f.view===view))return res.status(409).json({success:false,error:'Upload an original reference for this view before staging its correction.'});
    const name=crypto.randomUUID()+'.'+(metadata.format==='jpeg'?'jpg':metadata.format);
    const photo={jobId:kind==='proposal'?record.job?.id:undefined,id:crypto.randomUUID(),file:name,view,kind,bytes:req.file.size,width:metadata.width,height:metadata.height,sha256:crypto.createHash('sha256').update(req.file.buffer).digest('hex'),uploadedAt:new Date().toISOString(),uploadedBy:req.user.username};
    fs.mkdirSync(directory,{recursive:true});fs.writeFileSync(path.join(directory,name),req.file.buffer,{flag:'wx'});
    if(kind==='original'){
      record.originals.push(photo);record.decision='needs_correction';record.pendingViews=[...new Set([...(record.pendingViews||[]),view])];
      if(record.job){record.history.push({event:'job-invalidated',job:record.job,at:photo.uploadedAt});record.job=null;}
      // Earlier drafts stay in history; a new original requires a fresh correction set.
      if(record.proposals.length){record.history.push({event:'proposals-superseded',files:record.proposals,at:photo.uploadedAt});record.proposals=[];}
    }else{const previous=record.proposals.filter(f=>f.view===view);if(previous.length)record.history.push({event:'proposal-replaced',files:previous,at:photo.uploadedAt});record.proposals=record.proposals.filter(f=>f.view!==view).concat(photo);record.proposalRevision=(record.proposalRevision||0)+1;record.decision='needs_correction';}
    record.approvedProposalRevision=null;record.approvedAt=null;
    res.json({success:true,item:persist(item,state,record,req,'photo:'+kind+':'+view)});
  }));
  router.post('/api/image-review/items/:id/approve',wrap(async(req,res)=>{
    const {item,state,record}=await context(req,true);
    if(record.job)return res.status(409).json({success:false,error:'Use the queue review and publish controls.'});
    if(record.fingerprint!==item.fingerprint)return res.status(409).json({success:false,error:'Review the changed website gallery before approving.'});
    const requestedViews=[...new Set(record.originals.map(f=>f.view))];
    if(!requestedViews.length||!requestedViews.every(v=>record.proposals.some(f=>f.view===v)))return res.status(409).json({success:false,error:'Each uploaded reference view needs its matching correction before approval.'});
    record.approvedProposalRevision=record.proposalRevision;record.approvedAt=new Date().toISOString();
    res.json({success:true,item:persist(item,state,record,req,'proposal-approved')});
  }));
  require('./image-review-queue')({router,wrap,context,read,save,persist,present,catalogue,client,store,directory,fetchImage:options.fetchImage,updateProduct:product=>{const rows=catalogueRows([product]);cache=(cache||[]).filter(i=>i.productId!==String(product.id)).concat(rows).sort((a,b)=>String(b.publishedAt||'').localeCompare(String(a.publishedAt||'')));return rows;}});
  router.get('/api/image-review/files/:name',wrap(async(req,res)=>{
    const name=req.params.name;if(!/^[a-f0-9-]{36}\.(?:jpg|png|webp)$/.test(name))return res.sendStatus(404);
    const state=read();const linked=Object.values(state.records).some(r=>[...(r.originals||[]),...(r.proposals||[]),...(r.job?.receipts||[]).map(x=>x.backup).filter(Boolean),...(r.history||[]).flatMap(h=>[...(h.files||[]),...(h.job?.receipts||[]).map(x=>x.backup).filter(Boolean)])].some(f=>f.file===name));
    if(!linked)return res.sendStatus(404);res.set('Cache-Control','private, no-store');res.set('X-Content-Type-Options','nosniff');res.sendFile(path.join(directory,name));
  }));
  router.get('/api/image-review/export',wrap(async(req,res)=>{
    const items=await catalogue(),state=read();res.attachment('sanki-image-review.json').json({version:1,exportedAt:new Date().toISOString(),syncedAt,items:items.map(i=>({...present(i,state.records[i.id]),audit:state.records[i.id]?.history||[]})),note:'Review references, queue jobs, approved image publishing receipts and audit history.'});
  }));
  router.use('/api/image-review',(error,req,res,next)=>{if(res.headersSent)return next(error);res.status(error.status||(error.code==='LIMIT_FILE_SIZE'?413:400)).json({success:false,item:error.item,error:error.code==='LIMIT_FILE_SIZE'?'Photo exceeds 20 MB.':error.message || 'Could not save review.'});});
  return router;
}
module.exports={router:createImageReviewRouter(),createImageReviewRouter,catalogueRows,statusFor};

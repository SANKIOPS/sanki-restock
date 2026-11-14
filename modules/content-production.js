'use strict';
// Internal planning only: never publishes content or calls Meta.
const fs=require('fs'),path=require('path'),crypto=require('crypto'),express=require('express');
const {rolesOf,hasUserPermission,userSummary}=require('./auth-users');
const workflow=require('../public/content-workflow');
const BASE='/api/content-production',ACCOUNTS=['Femme','Homme','Fashion'];
const TYPES=workflow.TYPES;
const dir=()=>process.env.DATA_PATH?path.dirname(process.env.DATA_PATH):path.join(__dirname,'..');
const manager=u=>rolesOf(u).includes('owner');
const allowed=u=>!!u&&rolesOf(u).some(r=>['owner','admin','content_team'].includes(r));
const finances=u=>rolesOf(u).includes('owner')||hasUserPermission(u,'metaAds');
function fail(message,status=400){throw Object.assign(new Error(message),{status});}
const text=(v,n=500)=>String(v??'').trim().slice(0,n);
const day=(now=new Date())=>new Intl.DateTimeFormat('en-CA',{timeZone:'Asia/Kolkata',year:'numeric',month:'2-digit',day:'2-digit'}).format(now);
function date(v,optional=true){v=text(v,10);if(!v&&optional)return '';if(!/^\d{4}-\d{2}-\d{2}$/.test(v)||!Number.isFinite(Date.parse(v))||new Date(v).toISOString().slice(0,10)!==v)fail('Enter a valid date.');return v;}
const shift=(d,n)=>new Date(Date.parse(d+'T12:00:00Z')+n*86400000).toISOString().slice(0,10);
function url(v){v=text(v,2000);if(!v)return '';try{const u=new URL(v);if(!['https:','http:'].includes(u.protocol)||u.username||u.password)throw Error();return u.href;}catch{fail('Use a complete http or https link.');}}
function read(file,fallback){try{return JSON.parse(fs.readFileSync(file,'utf8'));}catch(e){if(e.code==='ENOENT')return fallback;fail('Saved records could not be read; no data was changed.',500);}}
function products(){
 const physical=require('../public/inventory-data.json'),snapshot=read(process.env.INVENTORY_SNAPSHOT_PATH||path.join(dir(),'inventory-snapshot.json'),null);
 const list=snapshot?require('./inventory-state').catalog(snapshot,physical):physical;
 return list.map(p=>({id:p.handle,title:p.title,image:p.image||p.images?.[0]||'',category:p.category||'',collection:p.collection||'',skus:(p.variants||[]).map(v=>v.sku).filter(Boolean),colours:[...new Set((p.variants||[]).map(v=>v.colour&& !/see product/i.test(v.colour)?v.colour:String(v.variant||'').split('/')[0].trim()).filter(Boolean))]}));
}
function meta(){return read(process.env.META_ADS_PATH||path.join(dir(),'meta-ads.json'),{ads:[],adSets:[],campaigns:[],results:[]});}
function shoots(){const s=read(path.join(dir(),'expenses.json'),{}),m=s.modelCalendar||{};return (m.shoots||[]).filter(x=>x.status!=='cancelled').map(x=>({id:x.id,date:x.date,status:x.status,model:(m.contracts||[]).find(c=>c.id===x.contractId)?.name||''}));}
function createStore(options={}){
 const file=options.file||process.env.CONTENT_PRODUCTION_PATH||path.join(dir(),'content-production.json');
 const catalog=options.products||products,adData=options.meta||meta,bookings=options.shoots||shoots,today=options.today||day;
 const users=options.userSummary||userSummary;
 function load(){const s=read(file,null);if(!s)return {version:1,revision:0,items:[],publications:[],settings:{start:today(),days:7,buffer:3,editLead:1,shootLead:2,accounts:ACCOUNTS.map(account=>({account,daily:0,shooter:'',editor:'',publisher:''}))},history:[]};if(s.version!==1||!Number.isInteger(s.revision)||!Array.isArray(s.items)||!Array.isArray(s.publications)||!Array.isArray(s.history)||!s.settings)fail('Unsupported content data. Existing data was preserved.',500);return s;}
 function change(revision,actor,fn){fs.mkdirSync(path.dirname(file),{recursive:true});let lock;try{lock=fs.openSync(file+'.lock','wx',0o600);}catch{fail('Another content save is in progress. Retry shortly.',409);}try{const s=load();if(revision!==s.revision)fail('Content changed. Refresh before saving.',409);const result=fn(s);s.revision++;s.history.unshift({at:new Date().toISOString(),actor,action:result.action,id:result.id||''});s.history=s.history.slice(0,2000);const temp=file+'.tmp-'+crypto.randomUUID();try{fs.writeFileSync(temp,JSON.stringify(s),{mode:0o600});fs.renameSync(temp,file);}finally{if(fs.existsSync(temp))fs.unlinkSync(temp);}return result;}finally{fs.closeSync(lock);fs.unlinkSync(file+'.lock');}}
 function person(v){v=text(v,100);if(v&&!allowed(users(v)))fail('Select a user with Content team access: '+v);return v;}
 function assigned(i,s,key){return i[key]?[i[key]]:s.settings.accounts.filter(a=>i.accounts.includes(a.account)).map(a=>a[key]).filter(Boolean);}
 function normalize(b,previous,s){
  const x={...previous,id:previous?.id||crypto.randomUUID(),title:text(b.title,200),accounts:b.accounts,type:workflow.videoType(b.type),productIds:b.productIds,model:text(b.model,120),shootDate:date(b.shootDate),editedDate:date(b.editedDate??previous?.editedDate??previous?.source?.editedDate),shotBy:text(b.shotBy??previous?.shotBy??previous?.source?.shooter,120),editedBy:text(b.editedBy??previous?.editedBy??previous?.source?.editor,120),dueDate:date(b.dueDate),shootStatus:b.shootStatus,editStatus:b.editStatus,shooter:person(b.shooter),editor:person(b.editor),publisher:person(b.publisher),rawUrl:url(b.rawUrl),finalUrl:url(b.finalUrl),notes:text(b.notes,3000),adIds:b.adIds||[],shootId:text(b.shootId,100),adCreative:!!b.adCreative};
  if(!x.title)fail('Give this content a name.');
  if(!Array.isArray(x.accounts)||x.accounts.some(a=>!ACCOUNTS.includes(a)))fail('Choose valid posting accounts.');x.accounts=[...new Set(x.accounts)];
  if((!TYPES.includes(x.type)&&!(previous&&!x.type))||!['planned','shot'].includes(x.shootStatus)||!['pending','editing','done'].includes(x.editStatus))fail('Choose a valid video type and status.');
  if(x.editStatus!=='pending'&&x.shootStatus!=='shot')fail('Mark the shoot done before editing.');
  if(x.editStatus==='done'&&!x.finalUrl&&!(previous?.source&&previous.editStatus==='done'&&!previous.finalUrl))fail('Add the final file link before marking editing done.');
  if(x.shootDate>today()&&x.shootStatus==='shot')fail('A future shoot cannot be marked done.');
  if(!Array.isArray(x.productIds)||x.productIds.length>30)fail('Choose up to 30 products.');
  const valid=new Set(catalog().map(p=>p.id));if(x.productIds.some(id=>!valid.has(id)&&!previous?.productIds.includes(id)))fail('Select products from the app catalogue.');x.productIds=[...new Set(x.productIds)];
  if(x.shootId&&!bookings().some(v=>v.id===x.shootId)&&x.shootId!==previous?.shootId)fail('Select an existing model-calendar shoot.');
  const ads=adData().ads||[];if(!Array.isArray(x.adIds)||x.adIds.some(id=>!ads.some(a=>a.id===id)))fail('Select existing ads.');
  for(const id of x.adIds)if(s.items.some(i=>i.id!==x.id&&!i.archived&&i.adIds?.includes(id)))fail('This ad is already linked to another content item.');
  return x;
 }
 function save(b,u,id){if(!manager(u))fail('Only the owner can edit content.',403);return change(b.revision,u.username,s=>{const old=id?s.items.find(i=>i.id===id):null;if(id&&!old)fail('Content not found.',404);
   const payload={...old,...b};if(!finances(u))payload.adIds=old?.adIds||[];
   const item=normalize(payload,old,s);if(Object.prototype.hasOwnProperty.call(b,'type'))item.typeOverride=true;if(Object.prototype.hasOwnProperty.call(b,'postingStatus')){if(!['posted','not_posted'].includes(b.postingStatus))fail('Choose Posted or Not posted.');item.postingOverride=b.postingStatus;}item.updatedAt=new Date().toISOString();item.updatedBy=u.username;item.createdAt=old?.createdAt||item.updatedAt;if(old)Object.assign(old,item);else s.items.push(item);return {action:old?'update':'create',id:item.id};});}
 function plan(s){
  const output=[],start=s.settings.start>today()?s.settings.start:today(),used=new Map(),latest={};
  for(const a of s.settings.accounts){const seen=new Set();used.set(a.account,seen);for(const p of s.publications.filter(p=>p.account===a.account)){seen.add(p.contentId);latest[p.contentId+'|'+a.account]=p;}}
  for(let n=0;n<s.settings.days+s.settings.buffer;n++)for(const a of s.settings.accounts)for(let k=0;k<a.daily;k++){
   const d=shift(start,n),slotId=a.account+'|'+d+'|'+k,posted=s.publications.find(p=>p.slotId===slotId);if(posted){output.push({...posted,date:d,account:a.account,stage:'posted',owner:posted.by});continue;}
   const seen=used.get(a.account),rank=i=>i.editStatus==='done'?0:i.shootStatus==='shot'?1:2;
   const candidates=s.items.filter(i=>!i.archived&&i.accounts.includes(a.account)&&!seen.has(i.id)&&!workflow.isPosted(i,s.publications,a.account)&&i.source?.tab!=='Delivery Videos'&&i.source?.tab!=='My Journey'&&!(i.shootDate&&i.shootStatus!=='shot'&&i.shootDate>d)).sort((a,b)=>rank(a)-rank(b)||(a.dueDate||'9999').localeCompare(b.dueDate||'9999')||a.createdAt.localeCompare(b.createdAt));
   const item=candidates[0];if(item)seen.add(item.id);
   const stage=!item?'brief':workflow.stage(item,s.publications);
   const due=stage==='shoot'&&item?.shootDate?item.shootDate:shift(d,stage==='shoot'?-s.settings.shootLead:stage==='edit'?-s.settings.editLead:0);
   output.push({slotId,date:d,account:a.account,contentId:item?.id||'',stage,due,owner:item?(stage==='shoot'?item.shooter||a.shooter:stage==='edit'?item.editor||a.editor:item.publisher||a.publisher):a.shooter,buffer:n>=s.settings.days});
  }return output;
 }
 function publish(b,u){if(!manager(u))fail('Only the owner can edit posting status.',403);return change(b.revision,u.username,s=>{const item=s.items.find(i=>i.id===b.contentId&&!i.archived);if(!item||!item.accounts.includes(b.account))fail('Select content for this account.');const a=s.settings.accounts.find(a=>a.account===b.account);if(!manager(u)&&u.username!==(item.publisher||a.publisher))fail('Only the posting owner can mark this posted.',403);if(item.editStatus!=='done'||!item.finalUrl)fail('Finish editing and attach the file first.');if(workflow.isPosted(item,s.publications,b.account))fail('Already posted on this account.');const postedDate=date(b.date,false);if(postedDate>today())fail('Do not mark a future post as published.');const link=url(b.url);if(!link)fail('Add the live post link.');const slotId=text(b.slotId,200);if(slotId){const slot=plan(s).find(p=>p.slotId===slotId);if(!slot||slot.contentId!==item.id||slot.account!==b.account)fail('The calendar changed. Refresh the slot.',409);if(s.publications.some(p=>p.slotId===slotId))fail('This slot is already posted.');}item.postingOverride='posted';s.publications.push({id:crypto.randomUUID(),contentId:item.id,account:b.account,date:postedDate,url:link,slotId,by:u.username});return {action:'posted',id:item.id};});}
 function configure(b,u){if(!manager(u))fail('Only the owner can change the posting plan.',403);return change(b.revision,u.username,s=>{const z=b.settings;if(!z||!Array.isArray(z.accounts)||z.accounts.length!==3||new Set(z.accounts.map(a=>a.account)).size!==3)fail('Configure all three accounts.');const integer=(v,min,max)=>{if(!Number.isInteger(v)||v<min||v>max)fail('Invalid planning number.');return v;};s.settings={start:date(z.start,false),days:integer(z.days,1,14),buffer:integer(z.buffer,0,7),editLead:integer(z.editLead,0,14),shootLead:integer(z.shootLead,0,14),accounts:z.accounts.map(a=>{if(!ACCOUNTS.includes(a.account))fail('Invalid account.');return {account:a.account,daily:integer(a.daily,0,5),shooter:person(a.shooter),editor:person(a.editor),publisher:person(a.publisher)};})};if(s.settings.shootLead<s.settings.editLead)fail('Allow at least as much lead time for shooting as editing.');return {action:'plan'};});}
 function importSheet(buffer,revision,u,preview){
  if(!manager(u))fail('Only the owner can import content.',403);
  let parsed;try{parsed=require('./content-production-import').parse(buffer);}catch(e){fail('Could not read this workbook: '+e.message);}
  const existing=load(),keys=new Map(existing.items.filter(i=>i.sourceKey).map(i=>[i.sourceKey,i])),urls=new Map();
  for(const i of existing.items)if(i.sourceKey&&i.finalUrl)urls.set(i.finalUrl,i);
  const matched=new Set(),entries=[];let unchanged=0;
  const owned=['title','accounts','type','contentFormat','model','shootDate','shootStatus','editStatus','rawUrl','finalUrl','adCreative','source','sourceKey'];
  for(const entry of parsed.records){
   const old=(entry.finalUrl&&urls.get(entry.finalUrl))||keys.get(entry.sourceKey);
   if(old&&matched.has(old.id))fail('Multiple source rows match the same content. Review the source before updating.');
   if(old)matched.add(old.id);
   const patch=Object.fromEntries(owned.filter(k=>k!=='type'||!old?.typeOverride).map(k=>[k,entry[k]]));
   if(!old||Object.keys(patch).some(k=>JSON.stringify(old[k])!==JSON.stringify(patch[k])))entries.push({entry,old,patch});else unchanged++;
  }
  const added=entries.filter(x=>!x.old).length,updated=entries.length-added;
  const report={count:added,updated,unchanged,skipped:parsed.skipped,warnings:parsed.warnings,ignoredTabs:parsed.ignoredTabs,records:entries.map(({entry,old})=>({title:entry.title,type:entry.type,accounts:entry.accounts,source:entry.source,editStatus:entry.editStatus,action:old?'Update':'Add'}))};
  if(preview)return report;
  return change(revision,u.username,s=>{
   const now=new Date().toISOString();
   for(const {entry,old,patch} of entries){
    if(old){const target=s.items.find(i=>i.id===old.id);Object.assign(target,patch,{updatedAt:now,updatedBy:u.username});target.notes=String(target.notes||'').replace(/^Source posting status:.*$/gm,'').trim();}
    else s.items.push({...entry,id:crypto.randomUUID(),createdAt:now,updatedAt:now,updatedBy:u.username});
   }
   return {action:'import',...report};
  });
 }
 function archive(b,u,id){if(!manager(u))fail('Only the owner can archive content.',403);return change(b.revision,u.username,s=>{const i=s.items.find(i=>i.id===id);if(!i)fail('Content not found.',404);i.archived=b.archived!==false;return {action:i.archived?'archive':'restore',id};});}
 function view(u){const s=load(),m=finances(u)?adData():null;const linkedIds=new Set(s.items.flatMap(i=>i.adIds||[]));const items=s.items.map(i=>{const display={...i,type:workflow.videoType(i.type),legacyType:TYPES.includes(i.type)?'':i.type,editedDate:i.editedDate??i.source?.editedDate??'',shotBy:i.shotBy??i.source?.shooter??'',editedBy:i.editedBy??i.source?.editor??''};return finances(u)?display:(({adIds,...rest})=>rest)(display);});return {...s,items,summary:workflow.summary(s.items,s.publications),history:manager(u)?s.history:[],plan:plan(s),today:today(),permissions:{manage:manager(u),results:finances(u)},ads:m?m.ads.filter(a=>!a.archived).map(a=>({id:a.id,name:a.name,status:a.status,updatedAt:a.updatedAt,adSetId:a.adSetId,mediaUrls:a.mediaUrls||[]})):[],results:m?(m.results||[]).filter(r=>!r.archived&&linkedIds.has(r.adId)):[]};}
 return {load,save,publish,configure,view,plan,catalog,bookings,importSheet,archive};
}
function createRouter(store=createStore()){
 const r=express.Router();r.use(BASE,(req,res,next)=>{if(!req.user)return res.status(401).json({success:false,error:'Sign in first.'});if(!allowed(req.user))return res.status(403).json({success:false,error:'Content team access required.'});if(!['GET','HEAD','OPTIONS'].includes(req.method)&&!manager(req.user))return res.status(403).json({success:false,error:'Only the owner can change content.'});next();});r.use(BASE,express.json({limit:'256kb'}));
 const run=fn=>(req,res)=>{try{res.json({success:true,...fn(req)});}catch(e){res.status(e.status||500).json({success:false,error:e.status?e.message:'Unable to save content. Existing records were preserved.'});}};
 const upload=require('multer')({storage:require('multer').memoryStorage(),limits:{fileSize:10*1024*1024,files:1}});
 r.post(BASE+'/import',(req,res,next)=>{if(!manager(req.user))return res.status(403).json({success:false,error:'Only the owner can import content.'});upload.single('file')(req,res,err=>{if(err)return res.status(400).json({success:false,error:'Choose one XLSX file below 10 MB.'});next();});},run(req=>{if(!req.file||!req.file.originalname.toLowerCase().endsWith('.xlsx'))fail('Choose an XLSX workbook.');if(req.body.preview!=='true'&&req.body.confirmed!=='true')fail('Preview and confirm the import first.');return store.importSheet(req.file.buffer,Number(req.body.revision),req.user,req.body.preview==='true');}));
 r.get(BASE,run(req=>store.view(req.user)));
 r.get(BASE+'/catalogue',run(()=>({products:store.catalog()})));
 r.get(BASE+'/shoots',run(()=>({shoots:store.bookings()})));
 r.get(BASE+'/people',run(req=>{if(!manager(req.user))return {people:[]};const au=require('./auth-users');return {people:au.listContentPeople()};}));
 r.post(BASE+'/items',run(req=>store.save(req.body,req.user)));
 r.patch(BASE+'/items/:id',run(req=>store.save(req.body,req.user,req.params.id)));
 r.post(BASE+'/items/:id/archive',run(req=>store.archive(req.body,req.user,req.params.id)));
 r.post(BASE+'/posted',run(req=>store.publish(req.body,req.user)));
 r.post(BASE+'/settings',run(req=>store.configure(req.body,req.user)));
 return r;
}
module.exports={createStore,createRouter,router:createRouter(),day,ACCOUNTS,TYPES,allowed};

'use strict';
const express=require('express'),crypto=require('crypto'),fs=require('fs'),path=require('path');
const inventoryState=require('./inventory-state');
const INVENTORY_DATA=require('../public/inventory-data.json');
const START='2026-09-11';
const roles=req=>(req.user&&(req.user.roles||[req.user.role]))||[];
const canManageExpenses=req=>roles(req).some(r=>['owner','admin','accounting'].includes(r));
const allowed=req=>canManageExpenses(req)||roles(req).includes('model_calendar');
function requireExpenseAccess(req){if(!canManageExpenses(req))fail('Your role can manage calendar details but cannot create, link or change expenses and payments.',403);}
const money=v=>Math.round(Number(v||0)*100)/100;
function fail(message,status=400){const e=new Error(message);e.status=status;throw e;}
function text(v,max=2000){return String(v||'').trim().slice(0,max);}
function validDate(v){return /^\d{4}-\d{2}-\d{2}$/.test(v)&&v>=START&&Number.isFinite(Date.parse(v))&&new Date(v).toISOString().slice(0,10)===v;}
function businessDate(now=new Date()){
 const parts=new Intl.DateTimeFormat('en-GB',{timeZone:'Asia/Kolkata',year:'numeric',month:'2-digit',day:'2-digit'}).formatToParts(now).reduce((out,part)=>{out[part.type]=part.value;return out;},{});
 return parts.year+'-'+parts.month+'-'+parts.day;
}
function calendar(s){
 const mc=s.modelCalendar||(s.modelCalendar={revision:0,contracts:[],shoots:[]});
 mc.contracts=Array.isArray(mc.contracts)?mc.contracts:[];mc.shoots=Array.isArray(mc.shoots)?mc.shoots:[];mc.outfits=Array.isArray(mc.outfits)?mc.outfits:[];
 return mc;
}
function expenseFor(s,c){return c.expenseId&&s.expenses&&s.expenses[c.expenseId];}
function contractShoots(mc,c){return mc.shoots.filter(x=>x.contractId===c.id);}
function contractIsCancelled(mc,c){return contractShoots(mc,c).every(x=>x.status==='cancelled');}
function requireActiveContract(mc,c){if(contractIsCancelled(mc,c))fail('This model package is cancelled. Restore or schedule a shoot before changing its package or expense.');}
function financial(s,c){
 const e=expenseFor(s,c),usable=e&&!e.accountingExcluded&&!['rejected','deleted'].includes(e.status)&&['SANKI','A3'].includes(e.nature||'SANKI');
 const posted=usable&&['approved','partially_paid','paid'].includes(e.status),paid=posted?money(e.paidAmount):0,fee=money(c.amount),bill=usable?money(e.amount):fee;
 return {expenseId:c.expenseId||'',expenseStatus:e?e.status:(c.expenseId?'missing':'not_created'),paid,due:Math.max(0,money(bill-paid)),agreedAmount:fee,billedAmount:usable?bill:null,amountMismatch:!!usable&&bill!==fee,covered:fee>0?Math.min(c.includedShoots,Math.floor(Math.round(paid*100)*c.includedShoots/Math.round(fee*100))):0,paymentStatus:paid>=bill&&bill>0?'paid':paid>0?'partially_paid':'unpaid',warning:c.expenseId&&!usable?'Linked expense is missing, rejected, excluded, or belongs to another entity. Review it in Expenses.':'',reimbursementStatus:usable?e.reimbursementStatus||'not_applicable':'not_applicable',payments:posted?(e.payments||[]).filter(p=>p.active!==false).map(p=>({id:p.id,amount:money(p.amount),date:p.date,account:p.account||'',note:p.note||'',paidBy:p.paidBy||''})):[]};
}
function view(s){const mc=calendar(s);return {revision:mc.revision,businessDate:businessDate(),contracts:mc.contracts.map(c=>({...c,finance:financial(s,c)})),shoots:mc.shoots,outfits:mc.outfits};}

const lowerCategory=value=>/trouser|jean|pant|lower|jogger|cargo/i.test(String(value||''));
const upperCategory=value=>/top|shirt|t-?shirt|tee|sweater|sweatshirt|hoodie|jacket|winter|kurta|sando/i.test(String(value||''));
function cleanColour(value,title){let colour=String(value||'').split('/')[0].trim();if(!colour||/^(?:xxxs|xxs|xs|s|m|l|xl|xxl|xxxl|\d+|universal size.*|free size.*)$/i.test(colour)){const match=String(title||'').match(/\b(black|white|blue|navy|grey|gray|brown|beige|cream|khaki|green|olive|pink|purple|red|maroon|orange|yellow)\b/i);colour=match?match[1]:'Standard';}return colour;}
const pairKey=(upper,lower)=>[upper.id,lower.id].sort().join('::');
function purchaseDesigns(){
 const out=new Map(),file=process.env.PROCUREMENT_PATH||path.join(process.env.DATA_PATH?path.dirname(process.env.DATA_PATH):path.join(__dirname,'..'),'procurement.json');
 try{const data=JSON.parse(fs.readFileSync(file,'utf8'));for(const po of Object.values(data.pos||{}))for(const line of po.lines||[]){const sku=String(line.sku||'').trim().toUpperCase();if(sku)out.set(sku,String(line.designId||line.designCode||line.designName||sku).trim().toUpperCase());}}catch{}
 return out;
}
function plannerProducts(live){
 const designs=purchaseDesigns(),products=inventoryState.catalog(live,INVENTORY_DATA),out=[];
 for(const product of products){
  const groups=new Map();
  for(const variant of product.variants||[]){
   if(Number(variant.totalQty)<=0)continue;
   const colour=cleanColour(variant.variant,product.title),key=colour.toLowerCase();
   if(!groups.has(key))groups.set(key,{colour,variants:[]});groups.get(key).variants.push(variant);
  }
  for(const group of groups.values()){
   const first=group.variants[0],fallbackDesign=String(product.handle||product.title).replace(/-(black|white|blue|navy|grey|gray|brown|beige|cream|khaki|green|olive|pink|purple|red|maroon)(-|$).*/i,'').toUpperCase();
   const savedCategory=String(product.category||'').trim(),classification=!savedCategory||/^uncategorized$/i.test(savedCategory)?product.productType||product.title:savedCategory,kind=lowerCategory(classification)?'lower':upperCategory(classification)?'upper':'';if(!kind)continue;
   out.push({id:product.handle+'::'+group.colour.toLowerCase(),handle:product.handle,title:product.title,colour:group.colour,category:product.category||product.productType||'',kind,image:product.image||'',designId:designs.get(String(first.sku||'').toUpperCase())||fallbackDesign,updatedAt:product.updatedAt||'',totalQty:group.variants.reduce((sum,v)=>sum+Number(v.totalQty||0),0),skus:group.variants.map(v=>v.sku).filter(Boolean)});
  }
 }
 return out;
}
const neutrals=new Set(['black','white','cream','beige','grey','gray','brown','khaki','navy','olive']);
function colourScore(a,b){a=String(a||'').toLowerCase();b=String(b||'').toLowerCase();return neutrals.has(a)||neutrals.has(b)?2:a===b?1:0;}
function buildCombinations(products,outfits,modelName,count){
 const active=(outfits||[]).filter(x=>x.status==='assigned'||x.status==='shot'),shotIds=new Set(active.filter(x=>x.status==='shot').flatMap(x=>[x.upper.id,x.lower.id])),reservedIds=new Set(active.filter(x=>x.status==='assigned').flatMap(x=>[x.upper.id,x.lower.id])),usedPairs=new Set(active.map(x=>pairKey(x.upper,x.lower))),modelDesigns=new Set(active.filter(x=>x.modelName===modelName).flatMap(x=>[x.upper.designId,x.lower.designId]));
 const order=(a,b)=>(shotIds.has(a.id)-shotIds.has(b.id))||String(b.updatedAt).localeCompare(String(a.updatedAt))||b.handle.localeCompare(a.handle),eligible=p=>p.image&&!reservedIds.has(p.id)&&!modelDesigns.has(p.designId);
 const uppers=products.filter(p=>p.kind==='upper'&&eligible(p)).sort(order),lowers=products.filter(p=>p.kind==='lower'&&eligible(p)).sort(order),used=new Set(),selectedDesigns=new Set(modelDesigns),result=[];
 for(const upper of uppers){
  if(result.length>=count)break;
  if(selectedDesigns.has(upper.designId))continue;
  const choices=lowers.filter(lower=>!used.has(lower.id)&&!selectedDesigns.has(lower.designId)&&lower.designId!==upper.designId&&!usedPairs.has(pairKey(upper,lower))).sort((a,b)=>((shotIds.has(upper.id)?1:0)+(shotIds.has(a.id)?1:0))-((shotIds.has(upper.id)?1:0)+(shotIds.has(b.id)?1:0))||colourScore(upper.colour,b.colour)-colourScore(upper.colour,a.colour)||order(a,b));
  const lower=choices[0];if(!lower)continue;used.add(upper.id);used.add(lower.id);selectedDesigns.add(upper.designId);selectedDesigns.add(lower.designId);result.push({upper,lower});
 }
 return result;
}
function checkRevision(s,b){if(b.revision!==calendar(s).revision)fail('The calendar changed in another window. Refresh before saving.',409);}
function validateContract(b){
 const name=text(b.name,100),instagram=text(b.instagram,300),kind=b.kind,month=text(b.month,7),amount=money(b.amount),includedShoots=kind==='monthly'?Number(b.includedShoots):1;
 if(!name)fail('Enter the model name.');if(instagram&&!/^https:\/\/(www\.)?instagram\.com\/[A-Za-z0-9_.]+\/?$/.test(instagram))fail('Enter an Instagram profile link such as https://www.instagram.com/modelname/.');
 if(!['shoot','day','monthly'].includes(kind))fail('Choose per shoot, per day, or monthly package.');if(!/^\d{4}-(0[1-9]|1[0-2])$/.test(month)||month<'2026-09')fail('Choose September 2026 or a later month.');
 if(!Number.isFinite(amount)||amount<=0||amount>100000000)fail('Enter a package price greater than zero.');if(!Number.isInteger(includedShoots)||includedShoots<1||includedShoots>1000)fail('Shoots included must be a whole number between 1 and 1000.');
 return {name,instagram,kind,month,amount,includedShoots,details:text(b.details)};
}
function checkShoot(mc,c,b,except,currentDate=businessDate()){if(!validDate(b.date))fail('Choose a date from 11 September 2026 onward.');if(!b.date.startsWith(c.month))fail('The shoot date must fall within the package month.');if(!['scheduled','completed','cancelled'].includes(b.status))fail('Choose a valid shoot status.');if(b.status==='completed'&&b.date>currentDate)fail('A future shoot can be scheduled, but cannot be marked completed.');if(b.status!=='cancelled'&&mc.shoots.filter(x=>x.contractId===c.id&&x.id!==except&&x.status!=='cancelled').length>=c.includedShoots)fail('All shoots in this package are booked.');}
function prepareExpenseLink(s,req,b){if(!b.modelCalendarContractId)return null;requireExpenseAccess(req);const mc=calendar(s),c=mc.contracts.find(x=>x.id===b.modelCalendarContractId);if(!c)fail('Model package not found.',404);requireActiveContract(mc,c);if(c.expenseId)fail('This package already has a linked expense. Open it from Model Calendar.',409);if(b.nature!=='SANKI'||money(b.amount)!==c.amount)fail('The expense must use SANKI and the agreed model package amount.');return c;}
function attachExpense(s,c,e){c.expenseId=e.id;e.modelCalendarContractId=c.id;calendar(s).revision++;}
function createRouter({loadStore,saveStore,audit}){
 const router=express.Router();router.use('/api/model-calendar',(req,res,next)=>{if(!allowed(req))return res.status(403).json({success:false,error:'You need the Model Calendar, SANKI Accounting, Admin or Owner role.'});next();});
 function route(method,path,handler){router[method](path,(req,res,next)=>{try{handler(req,res);}catch(e){if(e.status)return res.status(e.status).json({success:false,error:e.message});next(e);}});}
 function response(s,req){return {...view(s),permissions:{canManageExpenses:canManageExpenses(req)}};}
 function persist(s,req,action,id){calendar(s).revision++;audit(s,req,action,'model_calendar',id,{nature:'SANKI',after:calendar(s).shoots.find(x=>x.id===id)||calendar(s).contracts.find(x=>x.id===id)||{id}});saveStore(s);}
 route('get','/api/model-calendar',(req,res)=>res.json({success:true,...response(loadStore(),req)}));
 route('get','/api/model-calendar/contracts/:id',(req,res)=>{const s=loadStore(),c=calendar(s).contracts.find(x=>x.id===req.params.id);if(!c)fail('Package not found.',404);res.json({success:true,contract:{...c,finance:financial(s,c)}});});
 route('post','/api/model-calendar/bookings',(req,res)=>{const b=req.body||{},s=loadStore(),mc=calendar(s);checkRevision(s,b);let c=mc.contracts.find(x=>x.id===b.contractId);if(b.contractId&&!c)fail('Package not found.',404);if(!c)c={id:crypto.randomUUID(),...validateContract(b.contract||{}),expenseId:'',createdAt:new Date().toISOString(),createdBy:req.user.username};const shoot={id:crypto.randomUUID(),contractId:c.id,date:text(b.date,10),status:'scheduled',notes:text(b.notes)};checkShoot(mc,c,shoot);if(!mc.contracts.some(x=>x.id===c.id))mc.contracts.push(c);mc.shoots.push(shoot);persist(s,req,'MODEL_SHOOT_CREATED',shoot.id);res.json({success:true,...response(s,req)});});
 route('post','/api/model-calendar/shoots/:id',(req,res)=>{const b=req.body||{},s=loadStore(),mc=calendar(s);checkRevision(s,b);const shoot=mc.shoots.find(x=>x.id===req.params.id);if(!shoot)fail('Shoot not found.',404);const c=mc.contracts.find(x=>x.id===shoot.contractId),previousStatus=shoot.status,next={...shoot,date:text(b.date,10),status:b.status,notes:text(b.notes)};checkShoot(mc,c,next,shoot.id);const changedAt=new Date().toISOString(),changedBy=req.user.username;if(previousStatus!==next.status){next.statusHistory=[...(shoot.statusHistory||[]),{from:previousStatus,to:next.status,changedAt,changedBy}];if(next.status==='completed'){next.completedAt=changedAt;next.completedBy=changedBy;}else{delete next.completedAt;delete next.completedBy;}}Object.assign(shoot,next);persist(s,req,'MODEL_SHOOT_UPDATED',shoot.id);res.json({success:true,...response(s,req)});});
 route('delete','/api/model-calendar/shoots/:id',(req,res)=>{const b=req.body||{},s=loadStore(),mc=calendar(s);checkRevision(s,b);const index=mc.shoots.findIndex(x=>x.id===req.params.id);if(index<0)fail('Shoot not found.',404);const shoot=mc.shoots[index],c=mc.contracts.find(x=>x.id===shoot.contractId);if(shoot.status!=='cancelled')fail('Only a cancelled shoot can be removed.');if(c&&c.expenseId)fail('This shoot has a linked expense. Keep it for audit, or unlink/correct the expense before removing it.');const before={...shoot};mc.shoots.splice(index,1);if(c&&!contractShoots(mc,c).length)mc.contracts=mc.contracts.filter(x=>x.id!==c.id);mc.revision++;audit(s,req,'MODEL_SHOOT_REMOVED','model_calendar',shoot.id,{nature:'SANKI',before,after:null,note:'Cancelled shoot removed because it had no linked expense or payment record.'});saveStore(s);res.json({success:true,...response(s,req)});});
 route('post','/api/model-calendar/contracts/:id',(req,res)=>{const b=req.body||{},s=loadStore(),mc=calendar(s);checkRevision(s,b);const c=mc.contracts.find(x=>x.id===req.params.id);if(!c)fail('Package not found.',404);requireActiveContract(mc,c);const patch=validateContract(b);if(c.expenseId&&(patch.amount!==c.amount||patch.kind!==c.kind||patch.includedShoots!==c.includedShoots||patch.month!==c.month))fail('This package is linked to an expense. Financial terms are locked to preserve its payment allocation.');const active=mc.shoots.filter(x=>x.contractId===c.id&&x.status!=='cancelled');if(active.length>patch.includedShoots||active.some(x=>!x.date.startsWith(patch.month)))fail('The updated package must include its existing shoot dates and count.');Object.assign(c,patch);persist(s,req,'MODEL_PACKAGE_UPDATED',c.id);res.json({success:true,...response(s,req)});});
 route('post','/api/model-calendar/contracts/:id/link-expense',(req,res)=>{requireExpenseAccess(req);const b=req.body||{},s=loadStore(),mc=calendar(s);checkRevision(s,b);const c=mc.contracts.find(x=>x.id===req.params.id),e=s.expenses&&s.expenses[text(b.expenseId,30)];if(!c||!e)fail('Package or expense not found.',404);requireActiveContract(mc,c);if(c.expenseId)fail('Package already linked.',409);if(!['SANKI','A3'].includes(e.nature||'SANKI')||e.accountingExcluded||['rejected','deleted'].includes(e.status))fail('Choose an active SANKI expense.');if(money(e.amount)!==c.amount)fail('The expense amount must match the agreed package total.');if(e.modelCalendarContractId||mc.contracts.some(x=>x.expenseId===e.id))fail('This expense is already linked to another package.',409);attachExpense(s,c,e);persist(s,req,'MODEL_EXPENSE_LINKED',c.id);res.json({success:true,...response(s,req)});});
 route('post','/api/model-calendar/contracts/:id/unlink-expense',(req,res)=>{requireExpenseAccess(req);const b=req.body||{},s=loadStore(),mc=calendar(s);checkRevision(s,b);const c=mc.contracts.find(x=>x.id===req.params.id);if(!c)fail('Package not found.',404);const e=expenseFor(s,c);if(e&&(Number(e.paidAmount)>0||(e.payments||[]).some(p=>Number(p.amount)>0&&p.active!==false)))fail('Remove or correct the recorded payment in Expenses before unlinking.');if(e)delete e.modelCalendarContractId;c.expenseId='';persist(s,req,'MODEL_EXPENSE_UNLINKED',c.id);res.json({success:true,...response(s,req)});});
 async function catalog(){const read=inventoryState.readSnapshot(false);return plannerProducts(read.snapshot||await inventoryState.snapshot());}
 router.get('/api/model-calendar/planner',async(req,res,next)=>{try{const s=loadStore(),mc=calendar(s),products=await catalog(),shot=new Set(mc.outfits.filter(x=>x.status==='shot').flatMap(x=>[x.upper.id,x.lower.id])),assigned=new Set(mc.outfits.filter(x=>x.status==='assigned').flatMap(x=>[x.upper.id,x.lower.id]));res.json({success:true,revision:mc.revision,businessDate:businessDate(),models:[...new Set(mc.contracts.map(c=>c.name).filter(Boolean))].sort(),outfits:mc.outfits,stats:{notShot:products.filter(p=>!shot.has(p.id)).length,assigned:assigned.size,shot:shot.size}});}catch(e){next(e);}});
 router.post('/api/model-calendar/planner/suggest',async(req,res,next)=>{try{const b=req.body||{},s=loadStore(),mc=calendar(s),modelName=text(b.modelName,100),count=Math.max(1,Math.min(20,Math.round(Number(b.count)||0)));if(!mc.contracts.some(c=>c.name===modelName))fail('Choose a model from Model Calendar.');const combinations=buildCombinations(await catalog(),mc.outfits,modelName,count);if(!combinations.length)fail('No unused upper and trouser combinations are available for this model.');res.json({success:true,combinations,requested:count});}catch(e){if(e.status)return res.status(e.status).json({success:false,error:e.message});next(e);}});
 router.post('/api/model-calendar/planner/assign',async(req,res,next)=>{try{const b=req.body||{},s=loadStore(),mc=calendar(s);checkRevision(s,b);const modelName=text(b.modelName,100),date=text(b.date,10),requested=Array.isArray(b.combinations)?b.combinations:[];if(!mc.contracts.some(c=>c.name===modelName))fail('Choose a model from Model Calendar.');if(!validDate(date))fail('Choose a valid shoot date.');if(!requested.length||requested.length>20)fail('Choose between 1 and 20 combinations.');const products=await catalog(),byId=new Map(products.map(p=>[p.id,p])),active=mc.outfits.filter(x=>x.status==='assigned'||x.status==='shot'),reserved=new Set(active.filter(x=>x.status==='assigned').flatMap(x=>[x.upper.id,x.lower.id])),pairs=new Set(active.map(x=>pairKey(x.upper,x.lower))),modelDesigns=new Set(active.filter(x=>x.modelName===modelName).flatMap(x=>[x.upper.designId,x.lower.designId])),batchIds=new Set(),created=[];
   for(const row of requested){const upper=byId.get(String(row.upperId||'')),lower=byId.get(String(row.lowerId||''));if(!upper||!lower||upper.kind!=='upper'||lower.kind!=='lower')fail('One suggested product is no longer available. Generate the combinations again.',409);if(batchIds.has(upper.id)||batchIds.has(lower.id)||reserved.has(upper.id)||reserved.has(lower.id))fail('A product is already assigned to another outfit.',409);if(modelDesigns.has(upper.designId)||modelDesigns.has(lower.designId))fail('This model has already worn one of these designs. Generate the combinations again.',409);if(pairs.has(pairKey(upper,lower)))fail('This exact outfit combination has already been used.',409);batchIds.add(upper.id);batchIds.add(lower.id);modelDesigns.add(upper.designId);modelDesigns.add(lower.designId);pairs.add(pairKey(upper,lower));created.push({id:crypto.randomUUID(),modelName,date,status:'assigned',upper,lower,createdAt:new Date().toISOString(),createdBy:req.user.username});}
   mc.outfits.push(...created);persist(s,req,'MODEL_OUTFITS_ASSIGNED',created[0].id);res.json({success:true,...response(s,req),created});
  }catch(e){if(e.status)return res.status(e.status).json({success:false,error:e.message});next(e);}});
 route('post','/api/model-calendar/planner/outfits/:id/shot',(req,res)=>{const b=req.body||{},s=loadStore(),mc=calendar(s);checkRevision(s,b);const outfit=mc.outfits.find(x=>x.id===req.params.id);if(!outfit)fail('Outfit not found.',404);if(outfit.status!=='assigned')fail('Only an assigned outfit can be marked shot.');outfit.status='shot';outfit.shotAt=new Date().toISOString();outfit.shotBy=req.user.username;persist(s,req,'MODEL_OUTFIT_SHOT',outfit.id);res.json({success:true,...response(s,req)});});
 route('delete','/api/model-calendar/planner/outfits/:id',(req,res)=>{const b=req.body||{},s=loadStore(),mc=calendar(s);checkRevision(s,b);const index=mc.outfits.findIndex(x=>x.id===req.params.id);if(index<0)fail('Outfit not found.',404);if(mc.outfits[index].status!=='assigned')fail('Completed outfit history cannot be removed.');const removed=mc.outfits.splice(index,1)[0];persist(s,req,'MODEL_OUTFIT_RELEASED',removed.id);res.json({success:true,...response(s,req)});});
 return router;
}
module.exports={createRouter,prepareExpenseLink,attachExpense,financial,view,validDate,businessDate,contractIsCancelled,buildCombinations};

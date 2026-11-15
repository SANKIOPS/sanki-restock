'use strict';
const crypto=require('node:crypto');
const guards=require('./source-correction-guards');
const clone=v=>JSON.parse(JSON.stringify(v)),hash=v=>crypto.createHash('sha256').update(typeof v==='string'?v:JSON.stringify(v)).digest('hex');
const list=v=>Array.isArray(v)?v:[],money=v=>Math.round(Number(v||0)*100)/100;
function fail(message,status=409){throw Object.assign(new Error(message),{status});}
const allowedChanges=new Set(['status','rejectReason','rejectedAt','rejectedBy','reimbursementStatus','duplicateOf','duplicateResolution']);
function financialRecord(e){return Object.fromEntries(Object.entries(e).filter(([key])=>!allowedChanges.has(key)));}
function plan(snapshot,input,req,deps){
 if(!deps.isOwner(req))fail('Only the Owner can resolve duplicate expense submissions.',403);
 const pairs=input.pairs,reason=String(input.reason||'').trim(),s=snapshot.store;
 if(!Array.isArray(pairs)||!pairs.length||pairs.length>100||!reason||reason.length>2000)fail('Select 1–100 confirmed duplicate pairs and enter the evidence/reason.',400);
 if(pairs.some(p=>!p||typeof p!=='object'||Array.isArray(p)||Object.keys(p).some(k=>!['sourceId','targetId'].includes(k))))fail('Each pair needs only sourceId and targetId.',400);
 const sources=new Set(),targets=new Set(pairs.map(p=>p.targetId));
 for(const p of pairs){if(!p||!/^EX-[A-Za-z0-9_-]+$/.test(p.sourceId)||!/^EX-[A-Za-z0-9_-]+$/.test(p.targetId)||sources.has(p.sourceId)||targets.has(p.sourceId))fail('Duplicate sources must be distinct, with no self-links, chains or cycles.',400);sources.add(p.sourceId);}
 const references=guards.references({bankStatements:s.bankStatements,bankReconciliationDrafts:s.bankReconciliationDrafts,bankReconciliationLinks:s.bankReconciliationLinks,bankDateOverrides:s.bankDateOverrides,bankMovementAuthority:s.bankMovementAuthority,expenseRefunds:s.expenseRefunds,creditCardSettlementAllocations:s.creditCardSettlementAllocations});
 const accounts=new Map(),changes=[],alreadyLinked=[];
 for(const pair of pairs){
  const source=s.expenses?.[pair.sourceId],target=s.expenses?.[pair.targetId];
  if(!source||!target)fail('An original or paid expense no longer exists.');
  if(!deps.canAccess(req,source)||!deps.canAccess(req,target))fail('A selected expense is restricted.',403);
  if(source.duplicateOf===target.id&&source.status==='rejected'){alreadyLinked.push(pair);continue;}
  if(source.status!=='pending'||source.approvedAt||source.accountingExcluded||source.duplicateOf||source.ownerOnly)fail(source.id+': only an active, unapproved pending submission can be closed.');
  if(target.status!=='paid'||target.accountingExcluded||target.duplicateOf||!(money(target.amount)>0)||money(source.amount)!==money(target.amount))fail(source.id+': the kept copy must be an active fully paid expense for the same bill amount.');
  if(!/^\d{4}-\d{2}-\d{2}$/.test(source.date)||!/^\d{4}-\d{2}-\d{2}$/.test(target.date)||Math.abs(Date.parse(source.date)-Date.parse(target.date))>7*86400000)fail(source.id+': bill dates differ too much for this pending-submission workflow.');
  const paid=list(target.payments).filter(p=>!p.accountingExcluded&&!p.voided&&!p.provisional&&!s.bankReconciliationDrafts?.[p.bankReconciliationDraft]);
  if(money(paid.reduce((n,p)=>n+Number(p.amount||0),0))<money(target.amount)||money(target.paidAmount)<money(target.amount))fail(target.id+': paid status is not backed by sufficient existing payment records.');
  if(['reconciliationSource','bankReconciliationDraft','bankReconciliationEvidence','reconciliationDraft','creditCardId','creditCardStatementId','salaryAdvanceId','rentalFeeId','inventoryPurchase','procurementPoId','purchaseOrderId'].some(key=>source[key])||list(source.reimbursementPayments).length||Number(source.reimbursementAmount)>0||list(source.vendorAdvanceApplications).length)fail(source.id+': linked source workflow must be reviewed first.');
  if([...references].some(ref=>ref===source.id||ref.startsWith(source.id+'/')))fail(source.id+': the pending copy itself is referenced by protected accounting evidence.');
  const originalPayments=list(source.payments);
  if(originalPayments.length){
   if(!source.paidAlready||source.claimant!==target.claimant||originalPayments.length!==1||originalPayments[0].personalFunds!==true||originalPayments[0].provisional||originalPayments[0].accountingExcluded||originalPayments[0].voided||money(originalPayments[0].amount)!==money(source.amount)||!paid.some(p=>p.personalFunds===true&&p.account===originalPayments[0].account&&p.date===originalPayments[0].date&&money(p.amount)===money(originalPayments[0].amount)))fail(source.id+': an independent or posted payment must be corrected through its payment workflow.');
  }else if(Number(source.paidAmount)>0||Number(source.personalPaidAmount)>0||source.paidAlready)fail(source.id+': paid totals do not match the retained payment graph.');
  for(const e of [source,target])for(const p of [...list(e.payments),...list(e.reimbursementPayments)])if(p.account)accounts.set(p.account,e.nature||'SANKI');
  changes.push({sourceId:source.id,targetId:target.id,amount:source.amount,sourceEntity:source.nature,targetEntity:target.nature,proofsRetained:!!source.billPhoto||list(source.billPhotos).length>0,personalPaymentRetained:originalPayments.length>0});
 }
 const movements=Object.fromEntries([...accounts].sort().map(([account,nature])=>[account,deps.movements(s,account,nature)]));
 return {changes,alreadyLinked,reason,accounts:[...accounts].sort(),movementHash:hash(movements),protectedHash:hash(Object.fromEntries(Object.entries(s).filter(([key])=>!['expenses','auditLog','auditSeq'].includes(key)))),fingerprint:hash(snapshot.raw+'\n'+JSON.stringify({pairs,reason})),canonicalHash:hash(pairs.map(p=>s.expenses[p.targetId]))};
}
function apply(snapshot,input,req,deps){
 const prepared=plan(snapshot,input,req,deps);
 if(input.fingerprint!==prepared.fingerprint)fail('The records changed after preview. Reload and review; nothing was saved.');
 const after=clone(snapshot.store),now=new Date().toISOString();
 for(const c of prepared.changes){const e=after.expenses[c.sourceId],before=clone(e);e.status='rejected';e.rejectReason='Confirmed duplicate of '+c.targetId+' — '+prepared.reason;e.rejectedAt=now;e.rejectedBy=req.user.username;if(e.paidAlready)e.reimbursementStatus='rejected';e.duplicateOf=c.targetId;e.duplicateResolution={targetId:c.targetId,at:now,by:req.user.username,reason:prepared.reason,paymentsPreserved:true,reconciliationPreserved:true};deps.audit(after,req,'EXPENSE_DUPLICATE_LINKED','expense',e.id,{nature:e.nature,before,after:clone(e),note:e.rejectReason});}
 const protectedHash=hash(Object.fromEntries(Object.entries(after).filter(([key])=>!['expenses','auditLog','auditSeq'].includes(key))));
 if(protectedHash!==prepared.protectedHash||hash(input.pairs.map(p=>after.expenses[p.targetId]))!==prepared.canonicalHash)fail('Protected accounting records changed. Nothing was saved.');
 const changed=new Set(prepared.changes.map(c=>c.sourceId));
 for(const [id,e]of Object.entries(snapshot.store.expenses||{}))if(hash(changed.has(id)?financialRecord(e):e)!==hash(changed.has(id)?financialRecord(after.expenses[id]):after.expenses[id]))fail('An expense financial field changed. Nothing was saved.');
 if(Object.keys(after.expenses).length!==Object.keys(snapshot.store.expenses).length)fail('Expense count changed. Nothing was saved.');
 const movements=Object.fromEntries(prepared.accounts.map(([account,nature])=>[account,deps.movements(after,account,nature)]));
 if(hash(movements)!==prepared.movementHash)fail('This closure would change account movements. Use the source correction workflow; nothing was saved.');
 if(prepared.changes.length)deps.commit(snapshot,after);
 return {success:true,linked:prepared.changes,alreadyLinked:prepared.alreadyLinked,integrity:{existingPaidEntriesUnchanged:true,expenseCountUnchanged:true,paymentsUnchanged:true,reconciliationsUnchanged:true,accountMovementsUnchanged:true},protectedHash:prepared.protectedHash,movementHash:prepared.movementHash};
}
function decorate(s,req,e,canAccess){return {duplicateOf:e.duplicateOf||'',duplicateSubmissions:Object.values(s.expenses||{}).filter(x=>x.duplicateOf===e.id&&canAccess(req,x)).map(x=>({id:x.id,nature:x.nature,date:x.date,amount:x.amount,vendor:x.vendor,particulars:x.particulars,billPhoto:x.billPhoto,billPhotos:x.billPhotos||[],resolution:x.duplicateResolution}))};}
function register(router,deps){for(const mode of ['preview','apply'])router.post('/api/expenses/duplicate-links/'+mode,(req,res)=>{try{if(!deps.isOwner(req))fail('Only the Owner can resolve duplicate expense submissions.',403);const snapshot=deps.read(),input=req.body||{};const result=mode==='preview'?{success:true,...plan(snapshot,input,req,deps)}:apply(snapshot,input,req,deps);res.json(result);}catch(error){res.status(error.status||409).json({success:false,error:error.message||'Nothing was saved.'});}});}
module.exports={plan,apply,decorate,register};

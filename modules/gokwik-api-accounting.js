'use strict';
// Stage API evidence in the existing accounting workflow. Existing approvals,
// posted settlements and finalized bank routing keep their original protections.
const g=require('./gokwik-accounting');
const PENDING_AUDIT='_gokwikApiPendingAudit',IMPORT_AUDIT='automatic-import-v1',LINK_AUDIT='automatic-order-link-v1';
const AUTOMATIC_BY='GoKwik automatic API',AUTOMATIC_REASON='Exact Shopify merchant ID and order number from authenticated GoKwik API';
const importIdFor=utr=>'GKW-API-'+g.hash(utr).slice(0,20);
const auditFields=(readOnly,kind)=>readOnly?{[PENDING_AUDIT]:kind}:{at:new Date().toISOString()};
function stage(store,rows,orders,options={}){
 const readOnly=options.readOnly===true;
 const groups=new Map();for(const r of rows){const a=groups.get(r.utr)||[];a.push(r);groups.set(r.utr,a);}
 store.gokwikTransactions||={};store.gokwikImports||={};store.gokwikOrderLinks||={};const issues=[];let added=0,linked=0;
 for(const [utr,entries] of groups){
  if(!utr||entries.some(r=>r.date<g.START))continue;
  const fail=message=>issues.push({utr,message});
  if(entries.some(r=>r.type!=='Payment'||!r.paymentId||!r.orderId||!r.orderNumber||r.debit||!r.provider)){fail('Batch includes a refund, adjustment or missing identity; retained as order evidence, accounting review required');continue;}
  if(new Set(entries.map(r=>r.paymentId)).size!==entries.length){fail('Payment has multiple settlement entries; accounting allocation review required');continue;}
  if(entries.some(r=>Object.keys(r.deductions||{}).some(k=>k!=='gokwik'&&r.deductions[k]))){fail('Additional account split requires accounting classification');continue;}
  const transactions=entries.map(r=>({paymentId:r.paymentId,orderId:r.orderId,orderNumber:String(r.orderNumber).replace(/^#/,''),gokwikOrderId:r.providerOrderId||r.referenceId||'',date:r.transactionDate?require('./gokwik-api').day(r.transactionDate):'',settledDate:r.date,utr,provider:r.provider,currency:'INR',gross:r.gross,fee:r.fee??r.fees,tax:r.tax??0,additionalFee:r.additionalFee??0,additionalTax:r.additionalTax??0,deduction:r.reportedGokwikDeduction||0,credit:r.credit,net:r.net,rrn:r.rrn||'',shopifyTransactionId:r.shopifyTransactionId||'',method:r.method||'',paymentMode:r.method||''}));
  if(transactions.some(t=>['gross','fee','tax','additionalFee','additionalTax','deduction','net'].some(k=>Math.abs(t[k]*100-Math.round(t[k]*100))>.00001)||!t.date||t.date<g.START||t.date>t.settledDate||!t.gokwikOrderId||g.money(t.gross-t.fee-t.tax-t.additionalFee-t.additionalTax-t.deduction)!==g.money(t.net))){fail('Batch needs exact payment dates, identity or fee reconciliation');continue;}
  const conflict=transactions.some(t=>{const old=store.gokwikTransactions[t.paymentId];return old&&['orderId','orderNumber','utr','settledDate','provider','gross','net','deduction'].some(k=>String(old[k])!==String(t[k]));});
  if(conflict){fail('API conflicts with saved accounting evidence; saved records retained');continue;}
  const posted=Object.values(store.gokwikPostings||{}).some(p=>p.utr===utr);
  if(posted&&transactions.some(t=>!store.gokwikTransactions[t.paymentId])){fail('API adds entries to a posted settlement; existing posting retained');continue;}
  try{g.batches([...Object.values(store.gokwikTransactions),...transactions.filter(t=>!store.gokwikTransactions[t.paymentId])]);}catch(e){fail(e.message);continue;}
  const importId=importIdFor(utr);
  for(const t of transactions)if(!store.gokwikTransactions[t.paymentId]){store.gokwikTransactions[t.paymentId]={...t,importId};added++;}
  store.gokwikImports[importId]||={id:importId,name:'GoKwik automatic settlement API',sourceType:'gokwik_api_v2',hash:g.hash(entries),source:Buffer.from(JSON.stringify(entries)).toString('base64'),...auditFields(readOnly,IMPORT_AUDIT),count:transactions.length};
  for(const t of transactions){if(store.gokwikOrderLinks[t.paymentId]||posted)continue;try{const link=g.validateLink(store,store.gokwikTransactions[t.paymentId],orders);store.gokwikOrderLinks[t.paymentId]={...link,...auditFields(readOnly,LINK_AUDIT),by:AUTOMATIC_BY,reason:AUTOMATIC_REASON};linked++;}catch(e){fail(e.message);}}
 }
 store.gokwikApiAccounting={added,linked,issues};return store.gokwikApiAccounting;
}

// Read projections are deterministic, not accounting creation events. The save
// boundary supplies the real creation time only for newly staged API evidence;
// saved or manual records (including legacy records without a time) are untouched.
function finalizeStagedAudit(store){
 const pending=[];
 const automaticImport=(record,id)=>record&&record.id===id&&/^GKW-API-[a-f0-9]{20}$/.test(id)&&record.sourceType==='gokwik_api_v2'&&record.name==='GoKwik automatic settlement API'&&typeof record.hash==='string'&&typeof record.source==='string'&&Number.isInteger(record.count)&&record.count>0;
 for(const [id,record] of Object.entries(store.gokwikImports||{})){
  if(!record||!Object.hasOwn(record,PENDING_AUDIT))continue;
  if(record[PENDING_AUDIT]!==IMPORT_AUDIT||!automaticImport(record,id))throw Error('Invalid pending GoKwik API import audit metadata.');
  pending.push(record);
 }
 for(const [paymentId,record] of Object.entries(store.gokwikOrderLinks||{})){
  if(!record||!Object.hasOwn(record,PENDING_AUDIT))continue;
  const tx=(store.gokwikTransactions||{})[paymentId],importId=tx&&tx.utr?importIdFor(tx.utr):'';
  if(record[PENDING_AUDIT]!==LINK_AUDIT||record.by!==AUTOMATIC_BY||record.reason!==AUTOMATIC_REASON||!tx||tx.paymentId!==paymentId||String(record.orderId)!==String(tx.orderId)||String(record.orderNumber)!==String(tx.orderNumber)||!automaticImport((store.gokwikImports||{})[importId],importId))throw Error('Invalid pending GoKwik API order-link audit metadata.');
  pending.push(record);
 }
 // Validate every marker first so malformed metadata cannot partially timestamp
 // a store. Never replace an existing timestamp, even on a marked record.
 const unstamped=pending.filter(record=>!Object.hasOwn(record,'at'));
 if(unstamped.length){const at=new Date().toISOString();for(const record of unstamped)record.at=at;}
 for(const record of pending)delete record[PENDING_AUDIT];
 return {stamped:unstamped.length,cleared:pending.length};
}
module.exports={stage,finalizeStagedAudit};

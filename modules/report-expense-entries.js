'use strict';
// Add app identity to existing report rows; never replace amounts or bank dates.
function enrich(store,row,resolver){
 if(row.kind==='income'){
  const records=Array.isArray(store.receipts)?store.receipts:Object.values(store.receipts||{}),refs=[row.reference,row.id,String(row.id||'').replace(/^(?:RECEIPT|MANUAL-RECEIPT)\//,'')];
  const receipt=records.find(r=>refs.includes(r.id)&&!r.accountingExcluded&&!r.voided&&!r.voidedAt&&String(r.reportingNature||r.nature||'SANKI').toUpperCase()===row.entity);
  if(receipt)return {...row,appReceipt:{id:receipt.id,date:receipt.date,...require('../public/expense-report-presentation').presentReceipt(receipt),amount:receipt.amount}};
  return row;
 }

 if(!['expense','personal_use','unclassified'].includes(row.kind))return row;
 const refs=[row.reference,row.id].filter(Boolean);
 const reconciled=(Array.isArray(store.reconciliationExpenses)?store.reconciliationExpenses:Object.values(store.reconciliationExpenses||{})).find(e=>refs.some(ref=>String(ref)===e.id||String(ref).startsWith(e.id+'/'))&&!e.accountingExcluded&&String(e.nature||'SANKI').toUpperCase()===row.entity);
 if(reconciled)return {...row,appRecord:{id:reconciled.id,date:reconciled.date,...require('../public/expense-report-presentation').present({...reconciled,ledger:reconciled.category}),source:'Reconciliation expense'}};
 for(const ref of refs){const id=String(ref).split('/')[0],e=store.expenses?.[id];if(!e||e.accountingExcluded||String(e.reportingNature||e.nature||'SANKI').toUpperCase()!==row.entity)continue;
 return {...row,appExpense:{id:e.id,date:e.date,...(resolver||require('./report-description-recovery').createResolver(store))(e),status:e.status||'',amount:e.amount,searchNarration:[e.narration,e.note,e.reconciliationSource?.bankDescription].filter(Boolean).join(' ')}};
 }return row;
}
function evidence(store,expense,{canAccess,bookKey,natureForAccount=(nature)=>nature}){
 const snapshot=require('./bank-reconciliation-evidence').snapshot;
 return (expense.payments||[]).map(p=>{const account=p.account||expense.account||'',nature=natureForAccount(expense.nature||'SANKI',account),movementId=expense.id+'/'+p.id;
 if(!canAccess(nature,account))return {paymentId:p.id,account,available:false,message:'Bank evidence is restricted for this account.'};
 const result=snapshot(store,{nature,account,bookKey:bookKey(nature,account),movementId});
 return {paymentId:p.id,account,available:!!result.success,message:result.error||'',evidence:result.success?result.movement:null};
 });
}
function enrichMany(store,rows){const resolver=require('./report-description-recovery').createResolver(store);return rows.map(row=>enrich(store,row,resolver));}
module.exports={enrich,enrichMany,evidence};

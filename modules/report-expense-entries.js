'use strict';
// Add app identity to existing report rows; never replace amounts or bank dates.
function enrich(store,row){
 if(!['expense','personal_use','unclassified'].includes(row.kind))return row;
 const refs=[row.reference,row.id].filter(Boolean);
 for(const ref of refs){const id=String(ref).split('/')[0],e=store.expenses?.[id];if(!e||e.accountingExcluded||String(e.reportingNature||e.nature||'SANKI').toUpperCase()!==row.entity)continue;
 return {...row,appExpense:{id:e.id,date:e.date,...require('../public/expense-report-presentation').present(e),status:e.status||'',amount:e.amount,searchNarration:[e.narration,e.note,e.reconciliationSource?.bankDescription].filter(Boolean).join(' ')}};
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
module.exports={enrich,evidence};

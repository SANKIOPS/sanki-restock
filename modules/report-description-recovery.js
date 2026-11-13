'use strict';
const presentation=require('../public/expense-report-presentation');
const list=value=>Array.isArray(value)?value:Object.values(value||{}),nature=value=>String(value||'SANKI').toUpperCase();
function createResolver(store){
 let index;
 function candidates(){if(index)return index;index=new Map();const add=(id,candidate)=>{if(!id||!candidate.particulars)return;if(!index.has(id))index.set(id,[]);index.get(id).push(candidate);};
  for(const event of list(store.auditLog)){if(event.subjectType!=='expense')continue;for(const side of ['after','before']){const snapshot=event[side];if(!snapshot||typeof snapshot.particulars!=='string'||(snapshot.id&&snapshot.id!==event.subjectId))continue;if(side==='before'&&!presentation.isBankNarration(event.after?.particulars))continue;add(event.subjectId,{particulars:snapshot.particulars,bankDescription:snapshot.reconciliationSource?.bankDescription,nature:nature(snapshot.nature||event.nature),at:event.at||'',source:'Original app history '+event.id,kind:'audit'});}}
  for(const book of list(store.bankStatements))for(const record of list(book.imports)){if(!record.finalizedAt)continue;for(const row of [...list(record.reconciliationRows),...list(record.carriedReconciliationRows)]){const ledger=row.ledger;if(!ledger?.id||!ledger.entity||(Array.isArray(row.linkedRecordIds)&&row.linkedRecordIds.length&&!row.linkedRecordIds.includes(ledger.id)))continue;add(String(ledger.id).split('/')[0],{particulars:ledger.particulars,bankDescription:row.bank?.description,nature:nature(ledger.entity),at:record.finalizedAt,source:'Finalized reconciliation '+record.id+' · '+ledger.id,movementId:ledger.id,kind:'reconciliation'});}}
  return index;
 }
 return function resolve(expense){const direct=presentation.present(expense);if(!direct.particularsMissing)return direct;
  // An explicit latest clearing of particulars must not be undone by an older snapshot.
  const edits=list(expense.auditHistory).flatMap(e=>list(e.changes).filter(c=>c.field==='particulars').map(c=>({...c,at:e.editedAt||'',source:'Original app edit '+(e.id||'')}))).sort((a,b)=>b.at.localeCompare(a.at));
  if(!String(expense.particulars||'').trim()){
   if(edits[0]?.after==='')return direct;
   const latest=list(store.auditLog).filter(e=>e.subjectType==='expense'&&e.subjectId===expense.id&&typeof e.after?.particulars==='string'&&(!e.after.id||e.after.id===expense.id)).sort((a,b)=>String(b.at).localeCompare(String(a.at)))[0];if(latest?.after.particulars==='')return direct;
  }
  const movementIds=new Set(list(expense.payments).map(p=>expense.id+'/'+p.id));
  const options=(candidates().get(expense.id)||[]).filter(c=>c.nature===nature(expense.nature)&&(!c.movementId||movementIds.has(c.movementId)));
  for(const edit of edits){if(edit.after)options.push({particulars:edit.after,at:edit.at,source:edit.source,nature:nature(expense.nature)});if(presentation.isBankNarration(edit.after)&&edit.before)options.push({particulars:edit.before,at:edit.at,source:edit.source,nature:nature(expense.nature)});}
  const usable=options.map(c=>({...c,value:presentation.appParticulars(c.particulars,c.bankDescription||expense.reconciliationSource?.bankDescription||'')})).filter(c=>c.value).sort((a,b)=>String(b.at).localeCompare(String(a.at)));
  if(!usable.length)return direct;const latest=usable[0];if(usable.some(c=>c.at===latest.at&&c.value!==latest.value))return direct;
  return {...direct,particulars:latest.value,particularsMissing:false,descriptionSource:latest.source};
 };
}
module.exports={createResolver};

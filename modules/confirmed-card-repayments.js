'use strict';
const settlements=require('./credit-card-settlements');
const {activeRow}=require('./credit-card-accounting');
// Reuse the existing evidence/fingerprint validator. A category name alone
// cannot turn a purchase into a confirmed liability repayment.
function confirmed(cards,expenses){
 const result=new Map();
 for(const statement of Object.values(cards?.statements||{})){
  if(statement.status!=='finalized')continue;
  for(const row of statement.rows||[]){
   if(!activeRow(row)||row.classification!=='card_payment'||!row.confirmed||row.settlementMode!=='link_existing'||!row.settlementLegacyConfirmed||!String(row.settlementSourceId||'').startsWith('legacy_expense_payment:'))continue;
   const candidates=settlements.candidates(cards,expenses,statement,row,()=>true).filter(c=>c.id===row.settlementSourceId&&c.eligible&&c.bankMovementRecorded&&c.fingerprint===row.settlementSourceFingerprint);
   if(candidates.length===1)result.set(row.settlementSourceId.slice('legacy_expense_payment:'.length),{cardId:statement.cardId,statementId:statement.id,rowId:row.id,amount:row.amount,date:row.date});
  }
 }
 return result;
}
module.exports={confirmed};

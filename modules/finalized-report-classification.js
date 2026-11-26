'use strict';

const list=value=>Array.isArray(value)?value:Object.values(value||{});
const text=value=>String(value==null?'':value).trim();

function isSuspense(value){return /^(?:a3\s+)?suspense$/i.test(text(value));}
function savedCategory(expense){return text(expense&&expense.ledger||expense&&expense.category)||'Unclassified paid expense';}
function rowCategory(row){
  for(const value of [row&&row.category,row&&row.ledger&&row.ledger.category,row&&row.ledger&&row.ledger.ledger]){
    const category=text(value);if(category&&!isSuspense(category))return category;
  }
  return'';
}
function rowCategoryGroup(row){
  for(const value of [row&&row.categoryGroup,row&&row.group,row&&row.ledger&&row.ledger.categoryGroup,row&&row.ledger&&row.ledger.group]){
    const group=text(value);if(group)return group;
  }
  return'';
}
function activePayments(expense){return list(expense&&expense.payments).filter(payment=>payment&&!payment.accountingExcluded&&!payment.voided&&Number(payment.amount)>0);}

// Reporting-only precedence. A temporary Suspense label may be replaced only
// by a non-Suspense category saved on an exact, finalized bank row. Nothing in
// the source expense or reconciliation history is edited here.
function resolve(store,expense,paymentId=''){
  const original=savedCategory(expense),expenseId=text(expense&&expense.id),part=text(paymentId);
  if(!expenseId||!isSuspense(original))return{category:original,source:'saved_expense',resolved:false,conflict:false};
  const exact=part?expenseId+'/'+part:'',allowExpenseLink=!part||activePayments(expense).length<=1,matches=[];
  for(const [bookKey,book] of Object.entries(store&&store.bankStatements||{}))for(const record of list(book&&book.imports)){
    if(!record||!record.finalizedAt)continue;
    for(const row of [].concat(record.reconciliationRows||[],record.carriedReconciliationRows||[])){
      if(!row||row.reopenedAt)continue;
      const linked=(row.linkedRecordIds||[]).map(String),exactMatch=!!exact&&linked.includes(exact),expenseMatch=allowExpenseLink&&linked.includes(expenseId);
      if(!exactMatch&&!expenseMatch)continue;
      const category=rowCategory(row);if(!category)continue;
      matches.push({category,categoryGroup:rowCategoryGroup(row),bookKey,recordId:record.id||'',rowId:row.id||'',finalizedAt:record.finalizedAt,exact:exactMatch});
    }
  }
  const strongest=matches.some(match=>match.exact)?matches.filter(match=>match.exact):matches;
  const categories=[...new Set(strongest.map(match=>match.category))];
  if(categories.length!==1)return{category:original,source:'saved_expense',resolved:false,conflict:categories.length>1,matches:strongest};
  const match=strongest.slice().sort((a,b)=>String(b.finalizedAt).localeCompare(String(a.finalizedAt)))[0];
  return{category:categories[0],categoryGroup:match.categoryGroup||'',source:'finalized_bank_reconciliation',resolved:true,conflict:false,evidence:match,matches:strongest};
}

module.exports={isSuspense,resolve};

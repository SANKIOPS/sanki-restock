'use strict';
(function(root,factory){const api=factory();if(typeof module==='object'&&module.exports)module.exports=api;else root.ReportExpenseGroups=api;})(typeof window==='object'?window:this,function(){
 const labels={salary:'Salaries',rent:'Store Rent',interest:'Bank Interest Paid',operating:'Operating Expenses'};
 function bucket(row){const category=String(row.category||'').trim();if(row.sourceKind==='salary'||/\bsalar(?:y|ies)\b/i.test(category))return 'salary';if(/^(?:store|shop|showroom)\s+rent(?:\s+expense)?$/i.test(category))return 'rent';if(row.sourceKind==='loan_interest'||/^(?:bank |loan )?interest(?: expense| paid)?$/i.test(category))return 'interest';return 'operating';}
 function group(rows){const result=Object.entries(labels).map(([key,label])=>({key,label,amount:0,rows:[]}));for(const row of rows){if(row.kind!=='expense')continue;const item=result.find(g=>g.key===bucket(row));item.rows.push(row);item.amount+=Number(row.amount)||0;}for(const g of result)g.amount=Math.round((g.amount+Number.EPSILON)*100)/100;return result;}
 return {group,bucket};
});

'use strict';
(function(root,factory){const api=factory();if(typeof module==='object'&&module.exports)module.exports=api;else root.CompactReportSearch=api;})(typeof window==='object'?window:this,function(){
 const normalize=value=>String(value??'').normalize('NFKC').toLocaleLowerCase('en-IN').replace(/_/g,' ').replace(/\s+/g,' ').trim();
 const fields=['id','reference','date','expenseDate','kind','type','productType','category','description','particulars','vendor','source','sourceType','narration','searchNarration','name','account','from','to','fromEntity','toEntity','classification','treatment','status','amount','outstanding','balance','note','notes','bankReference','bankDescription'];
 function content(row){if(row==null)return '';if(typeof row!=='object')return String(row);return fields.map(k=>row[k]??'').concat(row.appExpense?content(row.appExpense):'').join(' ');}
 function matches(row,query){const needle=normalize(query);return !needle||normalize(content(row)).includes(needle);}
 return {normalize,content,matches};
});

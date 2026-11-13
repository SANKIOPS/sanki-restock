'use strict';
const XLSX=require('xlsx'),crypto=require('crypto');
const tabs=['Femme','Homme','Fashion','UGC','Funny Videos','Delivery Videos','My Journey'];
function parse(buffer){
 const book=XLSX.read(buffer,{type:'buffer',cellDates:true}),records=[],warnings=[],seen=new Map();let skipped=0;
 const value=c=>String(c?.v??'').trim(),link=c=>{const v=c?.l?.Target||value(c);return /^https?:\/\//i.test(v)?v:'';},at=(s,r,c)=>s[XLSX.utils.encode_cell({r,c})];
 const date=c=>c?.v instanceof Date?c.v.toISOString().slice(0,10):/^\d{4}-\d{2}-\d{2}/.test(value(c))?value(c).slice(0,10):'';
 for(const name of tabs){const s=book.Sheets[name];if(!s?.['!ref'])continue;const range=XLSX.utils.decode_range(s['!ref']);if(range.e.r>10000)throw Error('Sheet is too large. Export up to 10,000 rows per tab.');const delivery=name==='Delivery Videos',journey=name==='My Journey';
  for(let r=journey?5:1;r<=range.e.r;r++){
   const cells=Array.from({length:delivery?18:25},(_,c)=>at(s,r,c));
   // Ignore empty/formula-only scaffolding and embedded statistics.
   const meaningful=cells.slice(journey?1:0,journey?11:delivery?18:13).some(c=>c&&c.t!=='e'&&c.f==null&&value(c));if(!meaningful)continue;
   const final=link(cells[journey?6:delivery?13:11]),raw=delivery?link(cells[12]):'',model=value(cells[journey?2:delivery?5:2]);
   const type=journey?'Brand story':name==='UGC'?'UGC':name==='Funny Videos'?'Funny':delivery?'Product demonstration':'Model showcase';
   const account=['Femme','Homme','Fashion'].includes(name)?name:delivery&&['Femme','Homme','Fashion'].includes(value(cells[7]))?value(cells[7]):'';
   const title=value(cells[journey?3:delivery?17:11])||[model,type,date(cells[delivery?2:5]),value(cells[6])].filter(Boolean).join(' · ');
   if(!title&&!final)continue;
   const key=crypto.createHash('sha256').update(name+'|'+(r+1)).digest('hex').slice(0,24);
   if(final&&seen.has(final)){skipped++;warnings.push(name+' row '+(r+1)+': same final link as '+seen.get(final)+'. Review before importing this row separately.');continue;}if(final)seen.set(final,name+' row '+(r+1));
   const originalEdit=value(cells[7]),editStatus=journey?(final?'done':'pending'):delivery?final?'done':'pending':/^done$/i.test(originalEdit)&&final?'done':/^in progress$/i.test(originalEdit)?'editing':'pending';
   const post=value(cells[delivery?14:12]);
   const notes=['Imported from '+name+' row '+(r+1)+'.',post?'Source posting status: '+post+'; verify account, date and live URL.':'',!account?'Posting account needs selection.':'',delivery&&value(cells[6])?'Source product SKUs: '+value(cells[6]):'',!final&&value(cells[journey?6:delivery?13:11])?'Source file label without a usable URL: '+value(cells[journey?6:delivery?13:11]):'',!journey&&!delivery&&originalEdit==='Done'&&!final?'Source says editing done, but a final file link is missing.':''].filter(Boolean).join('\n');
   records.push({sourceKey:'sheet-'+key,title:title||type,accounts:account?[account]:[],type,productIds:[],model,shootDate:date(cells[delivery?2:5]),dueDate:'',shootStatus:journey||delivery||/^done$/i.test(value(cells[3]))?'shot':'planned',editStatus,shooter:'',editor:'',publisher:'',rawUrl:raw,finalUrl:final,notes,adIds:[],shootId:'',adCreative:delivery,source:{tab:name,row:r+1,editor:value(cells[delivery?3:8]),shooter:value(cells[delivery?1:4]),posted:post,productSkus:delivery?value(cells[6]):'',links:cells.flatMap((c,i)=>link(c)?[{column:i+1,label:value(c),url:link(c)}]:[])}});
  }
 }
 return {records,warnings,skipped,ignoredTabs:book.SheetNames.filter(n=>!tabs.includes(n))};
}
module.exports={parse};

'use strict';
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const KEY='owner-reviewed-12-categories-2026-09-26-v1';
const additions=['UJ','MD','Medical & Healthcare','Commission & Brokerage','Welfare'].map(name=>({name,group:name}));
const rows=[
 ['EX-00086',800,'Gagan Sir Personal Expenses',null,'PERSONAL'],
 ['EX-00090',50,'Medical & Healthcare','Medical & Healthcare'],
 ['EX-00112',20000,'UJ','UJ'],
 ['EX-00113',16000,'MD','MD'],
 ['EX-00128',1640,'Gagan Sir Personal Expenses',null,'PERSONAL'],
 ['EX-00254',2000,'HR Payments','Commission & Brokerage'],
 ['EX-00361',3725,'General Expense','Miscellaneous Office Expenses'],
 ['EX-00420',720,'Gagan Sir Personal Expenses',null,'PERSONAL'],
 ['EX-00460',158,'Medical & Healthcare','Medical & Healthcare'],
 ['EX-00489',60,'Maintenence','Fittings Repairs & Maintenance'],
 ['EX-00490',318,'Maintenence','Electrical Repairs & Maintenance'],
 ['EX-00507',2600,'Maintenence','Fittings Repairs & Maintenance']
];
function prepare(s){
 if(s.oneTimeMigrations?.[KEY])return null;
 if(!s.sankiCategoryCatalog?.categories)return null;
 const categories=s.sankiCategoryCatalog.categories.map(x=>({...x}));
 for(const a of additions){const old=categories.find(x=>x.name.toLowerCase()===a.name.toLowerCase());if(old&&old.group!==a.group)throw Error('Reviewed group conflicts for '+a.name);if(!old)categories.push({...a});}
 const changes=rows.map(([id,amount,old,target,nature='SANKI'])=>{
  const e=s.expenses?.[id];
  if(!e||e.amount!==amount||e.ledger!==old||e.nature!=='SANKI'||e.status!=='paid')throw Error('Reviewed expense changed; manual review required: '+id);
  const c=target&&categories.find(x=>x.name===target);
  if(target&&!c)throw Error('Missing reviewed subcategory '+target);
  const before={nature:e.nature,ledger:e.ledger,categoryGroup:e.categoryGroup??null};
  return{id,before,after:{nature,ledger:target||old,categoryGroup:c?.group??null}};
 });
 return{categories,changes};
}
function applyWithBackup(s,expensePath){
 const prepared=prepare(s);if(!prepared)return null;
 const raw=fs.readFileSync(expensePath);JSON.parse(raw);
 const dir=path.join(path.dirname(expensePath),'backups',KEY+'-'+Date.now());
 fs.mkdirSync(dir,{recursive:true,mode:0o700});
 const backupFile=path.join(dir,'expenses.json'),sha256=crypto.createHash('sha256').update(raw).digest('hex');
 fs.writeFileSync(backupFile,raw,{flag:'wx',mode:0o600});
 const now=new Date().toISOString();
 fs.writeFileSync(path.join(dir,'manifest.json'),JSON.stringify({createdAt:now,sha256,bytes:raw.length,commit:process.env.RAILWAY_GIT_COMMIT_SHA}),{flag:'wx',mode:0o600});
 s.sankiCategoryCatalog.categories=prepared.categories;
 for(const c of prepared.changes){
  const e=s.expenses[c.id];e.nature=c.after.nature;e.ledger=c.after.ledger;
  if(c.after.categoryGroup)e.categoryGroup=c.after.categoryGroup;else delete e.categoryGroup;
  s.auditSeq=(s.auditSeq||0)+1;s.auditLog=s.auditLog||[];
  s.auditLog.push({id:'AUD-'+String(s.auditSeq).padStart(6,'0'),at:now,user:'gaganlambasanki',action:'EDITED',subjectType:'expense',subjectId:c.id,nature:e.nature,device:'Owner-reviewed category finalization',before:c.before,after:c.after,note:'Owner-approved SANKI_12_Expenses_For_Review.xlsx; original payments, amounts, dates and proof references retained.'});
 }
 s.oneTimeMigrations=s.oneTimeMigrations||{};
 return s.oneTimeMigrations[KEY]={appliedAt:now,backupFile,sha256,changed:12,personalTotal:3160,reviewedTotal:48071,additions,changes:prepared.changes};
}
module.exports={KEY,rows,additions,prepare,applyWithBackup};

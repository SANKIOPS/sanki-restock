'use strict';
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const plan=require('./sanki-category-plan.json');
const norm=x=>String(x||'').trim().toLowerCase();
const byName=new Map(plan.catalog.map(x=>[norm(x.name),x]));
const mapping=new Map(Object.entries({...plan.mapping,...plan.aliases}).map(([a,b])=>[norm(a),b]));
const historical=new Set(plan.historicalOnly.map(norm));
function isSanki(e){return String(e.nature||'SANKI').toUpperCase()==='SANKI';}
function active(s){return s.sankiCategoryCatalog?.version===plan.version;}
function catalog(s){return active(s)?s.sankiCategoryCatalog.categories:[];}
function metadata(s,name){return catalog(s).find(c=>norm(c.name)===norm(name));}
function destination(e){
 const old=e.ledger||e.category||'';
 if(historical.has(norm(old)))return null;
 const override=plan.expenseOverrides[e.id];
 if(override&&norm(old)===norm(override.from))return byName.get(norm(override.to));
 return byName.get(norm(mapping.get(norm(old))||old))||null;
}
function invariants(s){
 const strip=e=>Object.fromEntries(Object.entries(e).filter(([k])=>!['ledger','categoryGroup','category'].includes(k)));
 return JSON.stringify({expenses:Object.fromEntries(Object.entries(s.expenses||{}).map(([id,e])=>[id,strip(e)])),reconciliationExpenses:(s.reconciliationExpenses||[]).map(strip)});
}
function migrate(s,{now=new Date().toISOString(),backupFile=null}={}){
 if(s.oneTimeMigrations?.[plan.version])return null;
 const before=invariants(s),changes=[],unresolved=[],preserved=[];
 const update=(e,field)=>{
  if(!isSanki(e))return;
  const old=e[field]||'',target=destination(e);
  if(!target){(historical.has(norm(old))?preserved:unresolved).push({id:e.id,category:old,status:e.status||''});return;}
  if(old!==target.name||e.categoryGroup!==target.group){
   const previous={ledger:old,categoryGroup:e.categoryGroup??null};
   e[field]=target.name;e.categoryGroup=target.group;
   changes.push({id:e.id,field,before:previous,after:{ledger:target.name,categoryGroup:target.group}});
  }
 };
 Object.values(s.expenses||{}).forEach(e=>update(e,'ledger'));
 (s.reconciliationExpenses||[]).forEach(e=>update(e,'category'));
 if(invariants(s)!==before)throw Error('Category migration changed non-category expense fields');
 s.sankiCategoryCatalog={version:plan.version,categories:plan.catalog.map(x=>({...x})),activatedAt:now};
 s.auditLog=s.auditLog||[];
 for(const c of changes){s.auditSeq=(s.auditSeq||0)+1;s.auditLog.push({id:'AUD-'+String(s.auditSeq).padStart(6,'0'),at:now,user:'Owner-directed migration',action:'EDITED',subjectType:'expense',subjectId:c.id,nature:'SANKI',device:'Category migration',before:c.before,after:c.after,note:'Historical categorization updated from the owner-reviewed workbook; all non-category fields preserved.'});}
 const result={appliedAt:now,backupFile,changed:changes.length,unresolved,preserved,changes};
 s.oneTimeMigrations=s.oneTimeMigrations||{};s.oneTimeMigrations[plan.version]=result;
 return result;
}
function applyWithBackup(s,expensePath){
 if(s.oneTimeMigrations?.[plan.version])return null;
 if(!fs.existsSync(path.join(path.dirname(expensePath),plan.version+'.enabled')))return null;
 const dir=path.join(path.dirname(expensePath),'backups',plan.version+'-'+Date.now());
 fs.mkdirSync(dir,{recursive:true,mode:0o700});
 const raw=fs.readFileSync(expensePath),backupFile=path.join(dir,'expenses.json');
 JSON.parse(raw);fs.writeFileSync(backupFile,raw,{flag:'wx',mode:0o600});
 fs.writeFileSync(path.join(dir,'manifest.json'),JSON.stringify({createdAt:new Date().toISOString(),commit:process.env.RAILWAY_GIT_COMMIT_SHA,sha256:crypto.createHash('sha256').update(raw).digest('hex'),bytes:raw.length}),{flag:'wx',mode:0o600});
 return migrate(s,{backupFile});
}
function attachGroups(s){
 if(!active(s))return;
 for(const e of [...Object.values(s.expenses||{}),...(s.reconciliationExpenses||[])]){
  if(!isSanki(e))continue;
  const field=Object.hasOwn(e,'ledger')?'ledger':'category';
  const target=destination(e);
  if(target&&e[field]!==target.name){
   const old=e[field];e[field]=target.name;
   s.auditSeq=(s.auditSeq||0)+1;s.auditLog=s.auditLog||[];
   s.auditLog.push({id:'AUD-'+String(s.auditSeq).padStart(6,'0'),at:new Date().toISOString(),user:'system',action:'EDITED',subjectType:'expense',subjectId:e.id,nature:'SANKI',device:'Category normalization',before:{ledger:old},after:{ledger:target.name},note:'Applied the owner-approved category alias.'});
  }
  const c=metadata(s,e.ledger||e.category);
  if(c)e.categoryGroup=c.group;
  else if(e.categoryGroup)delete e.categoryGroup;
 }
}
module.exports={plan,active,catalog,metadata,migrate,applyWithBackup,attachGroups,invariants};

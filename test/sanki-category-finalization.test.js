const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),os=require('node:os'),path=require('node:path');
const f=require('../modules/sanki-category-finalization'),c=require('../modules/sanki-categories');
function fixture(){return{expenses:Object.fromEntries(f.rows.map(([id,amount,ledger])=>[id,{id,amount,ledger,nature:'SANKI',status:'paid',date:'2026-09-01',payments:[{id:'PAY-001',account:'Prashant Axis 3645',amount,proof:'bill.jpg'}]}])),sankiCategoryCatalog:{version:c.plan.version,categories:structuredClone(c.plan.catalog)},oneTimeMigrations:{},bankStatements:{keep:{closing:123}},receipts:[{id:'keep'}]};}
test('review applies once, backs up exact source and preserves payment and unrelated data',()=>{
 const s=fixture(),before=structuredClone(s),dir=fs.mkdtempSync(path.join(os.tmpdir(),'category-review-')),file=path.join(dir,'expenses.json');fs.writeFileSync(file,JSON.stringify(s));
 try{
 const r=f.applyWithBackup(s,file);assert.deepEqual(JSON.parse(fs.readFileSync(r.backupFile)),before);assert.equal(r.changed,12);
 assert.equal(s.sankiCategoryCatalog.categories.length,73);assert.equal(new Set(s.sankiCategoryCatalog.categories.map(x=>x.group)).size,24);
 for(const [id,e] of Object.entries(s.expenses)){const strip=x=>Object.fromEntries(Object.entries(x).filter(([k])=>!['nature','ledger','categoryGroup'].includes(k)));assert.deepEqual(strip(e),strip(before.expenses[id]));}
 assert.deepEqual(s.bankStatements,before.bankStatements);assert.deepEqual(s.receipts,before.receipts);
 assert.equal(Object.values(s.expenses).filter(x=>x.nature==='PERSONAL').reduce((a,e)=>a+e.amount,0),3160);
 c.attachGroups(s);assert.equal(s.expenses['EX-00254'].ledger,'Commission & Brokerage');assert.equal(s.expenses['EX-00361'].categoryGroup,'Office & Administration');
 const once=JSON.stringify(s);assert.equal(f.applyWithBackup(s,file),null);assert.equal(JSON.stringify(s),once);
 }finally{fs.rmSync(dir,{recursive:true,force:true});}
});
test('changed financial records and conflicting category groups stop before mutation',()=>{
 for(const mutate of [s=>s.expenses['EX-00086'].amount=801,s=>s.expenses['EX-00112'].nature='PERSONAL',s=>s.sankiCategoryCatalog.categories.push({name:'Welfare',group:'Staff & Labour'})]){const s=fixture();mutate(s);const before=JSON.stringify(s);assert.throws(()=>f.prepare(s));assert.equal(JSON.stringify(s),before);}
});
test('backup failure leaves live in-memory records untouched',()=>{const s=fixture(),before=JSON.stringify(s);assert.throws(()=>f.applyWithBackup(s,'/no-such-review-backup/expenses.json'));assert.equal(JSON.stringify(s),before);});

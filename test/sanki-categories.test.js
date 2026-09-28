const test=require('node:test'),assert=require('node:assert/strict');
const c=require('../modules/sanki-categories');
test('historical mappings preserve every financial field and all other entities',()=>{
 const e={id:'x',nature:'SANKI',ledger:'BSES',amount:99,date:'2026-01-02',type:'fixed',status:'paid',payments:[{amount:99,account:'Cash',proof:'x.jpg'}]};
 const s={expenses:{x:e,y:{...e,id:'y',nature:'SAMAST'},z:{...e,id:'z',nature:'PERSONAL'}},auditLog:[]};
 const before=c.invariants(s),other=JSON.stringify([s.expenses.y,s.expenses.z]);
 const r=c.migrate(s);assert.equal(r.changed,1);assert.equal(e.ledger,'Electricity Expense');assert.equal(e.categoryGroup,'Rent, Utilities & Communication');assert.equal(c.invariants(s),before);assert.equal(JSON.stringify([s.expenses.y,s.expenses.z]),other);
 const once=JSON.stringify(s);assert.equal(c.migrate(s),null);assert.equal(JSON.stringify(s),once);
});
test('keeps agreed historical exceptions and ambiguous removed entries; maps proven needle expense',()=>{
 const s={expenses:Object.fromEntries([['a','Interior Expense-A3'],['b','Office Equipment Expense'],['c','Social Media Expenses'],['d','General Expense'],['EX-00336','General Expense']].map(([id,ledger])=>[id,{id,ledger,nature:'SANKI',amount:30}]))};
 const r=c.migrate(s);assert.equal(r.preserved.length,3);assert.equal(r.unresolved.length,1);assert.equal(s.expenses.d.ledger,'General Expense');assert.equal(s.expenses['EX-00336'].ledger,'Tailoring Expense');
});
test('catalog has 19 groups and 68 unique subcategories and resolves all 103 approved names',()=>{
 assert.equal(c.plan.catalog.length,68);assert.equal(new Set(c.plan.catalog.map(x=>x.group)).size,19);assert.equal(new Set(c.plan.catalog.map(x=>x.name.toLowerCase())).size,68);
 const s={expenses:Object.fromEntries(Object.entries(c.plan.mapping).map(([old,to],i)=>[i,{id:String(i),ledger:old,nature:'SANKI'}]))};
 const r=c.migrate(s);assert.equal(r.unresolved.length,0);assert.equal(Object.values(s.expenses).length,103);
 for(const e of Object.values(s.expenses))assert.ok(c.metadata(s,e.ledger));
});
test('later category edits refresh group metadata without changing amounts',()=>{
 const s={expenses:{x:{id:'x',ledger:'BSES',amount:42}}};c.migrate(s);s.expenses.x.ledger='Paintwork';c.attachGroups(s);assert.equal(s.expenses.x.categoryGroup,'Repairs & Maintenance');assert.equal(s.expenses.x.amount,42);
});

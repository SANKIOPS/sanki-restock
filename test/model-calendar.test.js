'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),os=require('node:os'),path=require('node:path');
const dir=fs.mkdtempSync(path.join(os.tmpdir(),'sanki-model-calendar-'));
process.env.DATA_PATH=path.join(dir,'data.json');
fs.writeFileSync(path.join(dir,'expenses.json'),JSON.stringify({expenses:{}}));
// Tests must not send notifications or read the production store.
require.cache[require.resolve('../modules/telegram')]={id:require.resolve('../modules/telegram'),filename:require.resolve('../modules/telegram'),loaded:true,exports:{}};
const express=require('express'),{router}=require('../modules/expenses'),{apiAllowedForUser}=require('../auth'),{financial}=require('../modules/model-calendar');
let server,base;
test.before(async()=>{const app=express();app.use(express.json());app.use((req,res,next)=>{const role=req.headers['x-test-role']||'owner';req.user={username:'model-calendar-test',role,roles:[role]};next()});app.use(router);server=await new Promise(resolve=>{const s=app.listen(0,'127.0.0.1',()=>resolve(s))});base='http://127.0.0.1:'+server.address().port;});
test.after(async()=>{if(server){server.closeAllConnections();await new Promise(r=>server.close(r))}fs.rmSync(dir,{recursive:true,force:true})});
async function call(method,url,body,role='owner'){const r=await fetch(base+url,{method,headers:{'Content-Type':'application/json','x-test-role':role},body:body?JSON.stringify(body):undefined});return {status:r.status,...await r.json()};}
async function get(){return call('GET','/api/model-calendar');}
async function book(overrides={}){const s=await get();return call('POST','/api/model-calendar/bookings',{revision:s.revision,date:'2026-09-11',contract:{name:'Test Model',instagram:'https://www.instagram.com/testmodel/',kind:'monthly',month:'2026-09',amount:20000,includedShoots:4},...overrides});}
function expenseBody(c){return {modelCalendarContractId:c.id,nature:'SANKI',date:'2026-09-11',ledger:'Models Payment',amount:c.amount,type:'marketing',vendor:c.name,particulars:'September model package',bill:'printed',billPhoto:'/test-bill.jpg',paymentType:'Cash',paidAlready:false};}
test('booking → linked expense → proof-gated partial and full payment → correction',async()=>{
 let s=await book();assert.equal(s.status,200);const c=s.contracts[0],shoot=s.shoots[0];assert.equal(c.finance.paid,0);assert.equal(c.finance.expenseStatus,'not_created');
 const noProof=expenseBody(c);delete noProof.billPhoto;assert.equal((await call('POST','/api/expenses',noProof)).status,400);
 const submitted=await call('POST','/api/expenses',expenseBody(c));assert.equal(submitted.status,200);const id=submitted.expense.id;
 assert.equal((await call('POST','/api/expenses',expenseBody(c))).status,409);
 const pay={amount:7500,account:'Counter Cash',paymentType:'Cash',paymentProof:'/test-pay.jpg',date:'2026-09-13'};
 assert.equal((await call('POST','/api/expenses/'+id+'/pay',pay)).status,400);
 assert.equal((await call('POST','/api/expenses/'+id+'/approve',{})).status,200);
 assert.equal((await call('POST','/api/expenses/'+id+'/pay',{amount:7500,account:'Counter Cash',paymentType:'Cash'})).status,400);
 assert.equal((await call('POST','/api/expenses/'+id+'/pay',pay)).status,200);
 s=await get();let f=s.contracts.find(x=>x.id===c.id).finance;assert.equal(f.paid,7500);assert.equal(f.due,12500);assert.equal(f.covered,1);assert.equal(f.paymentStatus,'partially_paid');assert.equal(s.shoots.find(x=>x.id===shoot.id).status,'scheduled');
 assert.equal((await call('POST','/api/model-calendar/contracts/'+c.id+'/unlink-expense',{revision:s.revision})).status,400);
 assert.equal((await call('POST','/api/expenses/'+id+'/pay',{...pay,amount:12500})).status,200);
 f=(await get()).contracts.find(x=>x.id===c.id).finance;assert.equal(f.covered,4);assert.equal(f.paymentStatus,'paid');
 assert.equal((await call('DELETE','/api/expenses/'+id+'/payments/PAY-002',{reason:'Test correction'})).status,200);
 f=(await get()).contracts.find(x=>x.id===c.id).finance;assert.equal(f.covered,1);assert.equal(f.due,12500);
 const disk=JSON.parse(fs.readFileSync(path.join(dir,'expenses.json'),'utf8'));assert.equal(disk.modelCalendar.contracts[0].expenseId,id);assert.equal(disk.expenses[id].modelCalendarContractId,c.id);assert.equal(Object.values(disk.expenses).filter(e=>e.modelCalendarContractId===c.id).length,1);
});
test('calendar rejects unauthorized access, invalid dates, overbooking and stale changes',async()=>{
 for(const role of ['claimant','sales','inventory','samast_accounting'])assert.equal((await call('GET','/api/model-calendar',undefined,role)).status,403);
 assert.equal((await book({date:'2026-09-10'})).status,400);assert.equal((await book({date:'2026-09-31'})).status,400);assert.equal((await book({date:'2026-10-01'})).status,400);
 let s=await book({contract:{name:'Single Model',kind:'day',month:'2026-09',amount:5000}});const c=s.contracts.at(-1),shoot=s.shoots.at(-1),old=s.revision;
 assert.equal((await call('POST','/api/model-calendar/bookings',{revision:old,date:'2026-09-14',contractId:c.id})).status,400);
 s=await call('POST','/api/model-calendar/shoots/'+shoot.id,{revision:old,date:shoot.date,status:'cancelled'});assert.equal(s.status,200);
 assert.equal((await call('POST','/api/model-calendar/bookings',{revision:old,date:'2026-09-14',contractId:c.id})).status,409);
 assert.equal((await call('POST','/api/model-calendar/bookings',{revision:s.revision,date:'2026-09-14',contractId:c.id})).status,200);
});
test('existing expense links preserve accounting records and reject duplicate allocation',async()=>{
 let s=await book({contract:{name:'Existing Model',kind:'monthly',month:'2026-09',amount:10000,includedShoots:2}}),c=s.contracts.at(-1);const b=expenseBody(c);delete b.modelCalendarContractId;const e=(await call('POST','/api/expenses',b)).expense;
 s=await call('POST','/api/model-calendar/contracts/'+c.id+'/link-expense',{revision:s.revision,expenseId:e.id});assert.equal(s.status,200);
 const c2=(await book({contract:{name:'Duplicate Model',kind:'monthly',month:'2026-09',amount:10000,includedShoots:2}})).contracts.at(-1);s=await get();assert.equal((await call('POST','/api/model-calendar/contracts/'+c2.id+'/link-expense',{revision:s.revision,expenseId:e.id})).status,409);
 assert.equal((await call('POST','/api/model-calendar/contracts/'+c.id,{revision:s.revision,...c,amount:12000})).status,400);
 const bad={...expenseBody(c2),nature:'PERSONAL'};assert.equal((await call('POST','/api/expenses',bad)).status,400);
 assert.equal((await call('POST','/api/expenses',expenseBody(c2),'claimant')).status,403);
});
test('excluded and rejected expenses cannot make models appear paid',()=>{
 const c={expenseId:'EX-1',amount:10000,includedShoots:4};for(const e of [{status:'rejected',nature:'SANKI'},{status:'paid',nature:'PERSONAL'},{status:'paid',nature:'SANKI',accountingExcluded:true}]){const f=financial({expenses:{'EX-1':{...e,amount:10000,paidAmount:10000}}},c);assert.equal(f.paid,0);assert.ok(f.warning)}
});

test('central auth gate protects the model API',()=>{for(const role of ['owner','admin','accounting'])assert.equal(apiAllowedForUser({roles:[role]},'/api/model-calendar'),true);for(const role of ['claimant','sales','samast_accounting'])assert.equal(apiAllowedForUser({roles:[role]},'/api/model-calendar/bookings'),false);});

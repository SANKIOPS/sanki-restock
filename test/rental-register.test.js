const test=require('node:test'),assert=require('node:assert/strict');
const {register}=require('../modules/rental-register');
function harness(){let store={receipts:[{id:'RC-1',nature:'PERSONAL',amount:10000,date:'2026-09-01',account:'IndusInd Bank 7883'}]},routes={};register({get:(p,f)=>routes.get=f,post:(p,f)=>routes.post=f},{loadStore:()=>structuredClone(store),saveStore:s=>store=s,isOwner:req=>req.user.role==='owner',audit:()=>{}});return {call(body,role='owner'){let result,status=200;routes.post({body,user:{username:'owner',role}},{status(n){status=n;return this},json(x){result=x}});return {status,result};},get(){let result;routes.get({user:{role:'owner'}},{json:x=>result=x});return result;},store:()=>store};}
test('dated security settles final rent without adding a bank receipt',()=>{
  const h=harness(),id='lujo-2';
  assert.equal(h.call({action:'deposit',tenancyId:id,amount:100000,date:'2026-04-01',reason:'Opening security held'}).status,200);
  assert.equal(h.call({action:'invoice',tenancyId:id,month:'2026-10',baseRent:89250,reason:'Agreed full October rent'}).status,200);
  const i=h.get().invoices[0];assert.equal(i.bankDue,96390);
  assert.equal(h.call({action:'security',tenancyId:id,invoiceId:i.id,amount:96390,date:'2026-10-12',reason:'Final rent adjusted from security'}).status,200);
  assert.equal(h.get().invoices[0].outstanding,0);assert.equal(h.get().tenancies.find(t=>t.id===id).securityBalance,3610);
  assert.equal(h.store().receipts.length,1);
  assert.equal(h.call({action:'invoice',tenancyId:id,month:'2026-11'}).status,400);
});
test('duplicate charges, excessive settlements, invalid dates and nonowners are rejected',()=>{
  const h=harness(),id='amty-4';
  assert.equal(h.call({action:'deposit',tenancyId:id,amount:100,date:'2026-02-30',reason:'Invalid'}).status,400);
  assert.equal(h.call({action:'invoice',tenancyId:id,month:'2026-09'},'admin').status,403);
  h.call({action:'invoice',tenancyId:id,month:'2026-09'});const i=h.get().invoices[0];assert.equal(i.status,'Due date not set');
  assert.equal(h.call({action:'invoice',tenancyId:id,month:'2026-09'}).status,400);
  assert.equal(h.call({action:'receipt',tenancyId:id,invoiceId:i.id,receiptId:'RC-1',amount:10001,date:'2026-09-01',reason:'Rent'}).status,400);
  assert.equal(h.call({action:'receipt',tenancyId:id,invoiceId:i.id,receiptId:'RC-1',amount:10000,date:'2026-09-01',reason:'Rent'}).status,200);
  assert.equal(h.call({action:'receipt',tenancyId:id,invoiceId:i.id,receiptId:'RC-1',amount:1,date:'2026-09-01',reason:'Duplicate'}).status,400);
  assert.equal(h.store().receipts.length,1);
});
test('rent changes preserve previously created charges and Fraganote stops after August',()=>{
  const h=harness(),id='amty-4';h.call({action:'invoice',tenancyId:id,month:'2026-09'});
  h.call({action:'terms',tenancyId:id,baseRent:90000,startMonth:'2026-10',dueDay:5,endDate:'',cgst:9,sgst:9,tds:10});
  h.call({action:'invoice',tenancyId:id,month:'2026-10'});
  assert.deepEqual(h.get().invoices.map(i=>i.baseRent),[83790,90000]);assert.equal(h.get().invoices[1].dueDate,'2026-10-05');
  assert.equal(h.call({action:'invoice',tenancyId:'fraganote-3',month:'2026-09'}).status,400);
});

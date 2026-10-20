const test=require('node:test'),assert=require('node:assert/strict');
const {register}=require('../modules/rental-register');
function harness(){let store={rentalSourceImport:require('../modules/rental-source.json').id,rentals:{tenancies:require('./fixtures/legacy-rentals')()},receipts:[{id:'RC-1',nature:'PERSONAL',amount:10000,date:'2026-09-01',account:'IndusInd Bank 7883'}]},routes={};register({get:(p,f)=>{if(p==='/api/expenses/rentals')routes.get=f},post:(p,f)=>routes.post=f},{loadStore:()=>structuredClone(store),saveStore:s=>store=s,isOwner:req=>req.user.role==='owner',audit:()=>{},clock:()=>new Date('2026-11-01T10:00:00Z')});return {call(body,role='owner'){let result,status=200;routes.post({body,user:{username:'owner',role}},{status(n){status=n;return this},json(x){result=x}});return {status,result};},get(){let result;routes.get({user:{role:'owner'}},{json:x=>result=x});return result;},store:()=>store};}
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
test('SAMAST tenant-linked rent is available without including unrelated business money or security as rent',()=>{
  const h=harness(),tenancyId='amty-4',otherTenancyId='lujo-2',receipt={id:'SAMAST-RENT',nature:'SAMAST',receiptType:'rental_income',tenancyId,amount:1000,date:'2026-09-01',account:'IndusInd Bank 7883'};
  h.store().receipts.push(receipt,
    {...receipt,id:'SAMAST-OTHER',receiptType:'other_income'},
    {...receipt,id:'SAMAST-UNASSIGNED',tenancyId:''},
    {...receipt,id:'SAMAST-UNKNOWN-TENANT',tenancyId:'missing'},
    {...receipt,id:'SAMAST-EXCLUDED',accountingExcluded:true},
    {...receipt,id:'SAMAST-SECURITY',receiptType:'rental_security'});
  const view=h.get();assert.ok(view.receipts.some(x=>x.id===receipt.id));
  for(const id of ['SAMAST-OTHER','SAMAST-UNASSIGNED','SAMAST-UNKNOWN-TENANT','SAMAST-EXCLUDED'])assert.equal(view.receipts.some(x=>x.id===id),false);
  const advance=id=>({action:'advance',tenancyId,receiptId:id,amount:1000,reason:'Tenant rent paid in advance'});
  assert.equal(h.call(advance('SAMAST-OTHER')).status,400);
  assert.equal(h.call(advance('SAMAST-SECURITY')).status,400,'held security cannot be allocated again as fresh rent');
  assert.equal(h.call({...advance(receipt.id),tenancyId:otherTenancyId}).status,400,'a linked rent receipt cannot be assigned to another tenant');
  assert.equal(h.call(advance(receipt.id)).status,200);assert.deepEqual(h.get().integrity,[]);
});
test('tenant-linked SAMAST rent cannot be split into another tenant invoice',()=>{
  const h=harness(),tenancyId='amty-4',otherTenancyId='lujo-2';
  h.store().receipts.push({id:'SAMAST-SPLIT',nature:'SAMAST',receiptType:'rental_income',tenancyId,amount:1000,date:'2026-09-01',account:'IndusInd Bank 7883'});
  h.call({action:'invoice',tenancyId,month:'2026-09'});h.call({action:'invoice',tenancyId:otherTenancyId,month:'2026-09'});
  const own=h.get().invoices.find(x=>x.tenancyId===tenancyId),other=h.get().invoices.find(x=>x.tenancyId===otherTenancyId);
  const body={action:'split_receipt',tenancyId,receiptId:'SAMAST-SPLIT',date:'2026-09-01',reason:'Tenant receipt allocation',splits:[{invoiceId:other.id,amount:1000}]};
  assert.equal(h.call(body).status,400);
  assert.equal(h.call({...body,splits:[{invoiceId:own.id,amount:1000}]}).status,200);
  assert.deepEqual(h.get().integrity,[]);
});
test('bank security receipt undo checks the dated security balance rather than a later replacement deposit',()=>{
  const {receiptRemovalError}=require('../modules/rental-register'),r={tenancies:[{id:'tenant',securityOpening:0,securityOpeningDate:'2026-09-01'}],credits:[],allocations:[],deposits:[
    {id:'SEC-ORIGINAL',tenancyId:'tenant',receiptId:'REC-ORIGINAL',date:'2026-09-01',amount:1000},
    {id:'REFUND',tenancyId:'tenant',date:'2026-09-02',amount:-1000},
    {id:'SEC-LATER',tenancyId:'tenant',date:'2026-09-03',amount:1000}
  ]};
  const before=JSON.stringify(r);assert.match(receiptRemovalError(r,'REC-ORIGINAL',['SEC-ORIGINAL']),/security settlements or refunds/);assert.equal(JSON.stringify(r),before);
  r.deposits[1].reversedAt='2026-09-04';assert.equal(receiptRemovalError(r,'REC-ORIGINAL',['SEC-ORIGINAL']),'');
});

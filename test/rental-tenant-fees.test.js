'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const fs=require('node:fs'),vm=require('node:vm');
const rental=require('../modules/rental-register');
const source=require('../modules/rental-source.json');
const clone=value=>structuredClone(value),today='2026-10-25';
function fixture(){
  const tenant=id=>({id,tenant:'Synthetic '+id,floor:id,property:'Synthetic building',baseRent:1000,netRent:1000,startMonth:'2026-10',trackingFrom:'2026-10',dueDay:5,cgst:0,sgst:0,tds:0,confirmedAt:'2026-10-01',openingConfirmed:true,phone:'123',history:[],autoEnabled:false,securityOpening:500,securityOpeningDate:'2026-10-01'});
  return {rentalSourceImport:source.id,rentals:{revision:4,tenancies:[tenant('TEN-A'),tenant('TEN-B')],invoices:[{id:'RENT-A',tenancyId:'TEN-A',month:'2026-10',bankDue:1000,baseRent:1000,cgst:0,sgst:0,tds:0,dueDate:'2026-10-05'}],deposits:[],allocations:[{id:'ALLOC-A',tenancyId:'TEN-A',invoiceId:'RENT-A',kind:'receipt',receiptId:'REC-RENT',amount:200,date:'2026-10-10'}],credits:[{id:'CREDIT-A',tenancyId:'TEN-A',receiptId:'REC-RENT',amount:100,date:'2026-10-10'}],reminders:[],activity:[],automation:{lastCheckedDate:today}},receipts:[{id:'REC-RENT',nature:'PERSONAL',receiptType:'rental_income',tenancyId:'TEN-A',amount:400,date:'2026-10-10',account:'IndusInd Bank 7883'}],expenses:{},bankStatements:{synthetic:{imports:[{id:'FINAL-UNCHANGED'}],drafts:{existing:{decisions:{reviewed:{action:'exclude',reason:'Owner reviewed'}}}}}},auditLog:[]};
}
function fees(){return ['dealer_commission','documentation_fee'].flatMap((kind,k)=>['payment','receipt'].map((direction,d)=>({id:'FEE-'+k+'-'+d,tenancyId:'TEN-A',kind,direction,amount:101+k*100+d,date:'2026-10-21',account:'IndusInd Bank 7883',nature:'PERSONAL',...(direction==='payment'?{expenseId:'EX-FEE-'+k,paymentId:'PAY-FEE-'+k}:{receiptId:'REC-FEE-'+k}),note:'Synthetic fee <reference>',bankReconciliationEvidence:{draftId:'SYNTHETIC',bankRowId:'bank-fee-'+k+'-'+d}})));}
function harness(store=fixture()){
  let saved=clone(store),writes=0;const routes={};
  const jobs=rental.register({get:(p,f)=>routes['GET '+p]=f,post:(p,f)=>routes['POST '+p]=f},{loadStore:()=>clone(saved),saveStore:s=>{writes++;saved=clone(s)},isOwner:req=>req.user.role==='owner',audit:()=>{},clock:()=>new Date(today+'T10:00:00Z')});
  return {jobs,store:()=>clone(saved),writes:()=>writes,call:body=>{let status=200,result;routes['POST /api/expenses/rentals']({body,user:{role:'owner',username:'synthetic-owner'}},{status:n=>{status=n;return {json:x=>result=x}},json:x=>result=x});return {status,result}}};
}
test('fee payments and receipts project against their tenant without changing rent, security, advances or existing records',()=>{
  const s=fixture(),baseline=rental.snapshot(clone(s),today);s.rentals.fees=fees();
  s.receipts.push(...s.rentals.fees.filter(f=>f.direction==='receipt').map(f=>({id:f.receiptId,nature:f.nature,receiptType:'rental_'+f.kind,rentalKind:f.kind,rentalFeeId:f.id,tenancyId:f.tenancyId,amount:f.amount,date:f.date,account:f.account})));
  const before=clone(s),view=rental.snapshot(s,today);
  assert.deepEqual(s,before);assert.deepEqual(view.fees,s.rentals.fees);assert.notEqual(view.fees,s.rentals.fees);assert.notEqual(view.fees[0],s.rentals.fees[0]);
  for(const key of ['summary','tenancies','invoices','deposits','allocations','credits','receipts','followups','alerts','integrity'])assert.deepEqual(view[key],baseline[key],key+' must ignore tenant fees');
  assert.equal(view.invoices[0].outstanding,800);assert.equal(view.tenancies[0].securityBalance,500);assert.equal(view.tenancies[0].creditBalance,100);
});
test('projection and same-source scheduler do not initialize fees or migrate old records',()=>{
  const s=fixture(),before=clone(s),view=rental.snapshot(s,today);assert.deepEqual(view.fees,[]);assert.deepEqual(s,before);assert.equal(Object.hasOwn(s.rentals,'fees'),false);
  const h=harness(s);h.jobs.tick();assert.equal(h.writes(),0);assert.deepEqual(h.store(),before);
});
test('reversed fee history stays visible separately and never makes its receipt available as rent',()=>{
  const s=fixture(),fee={...fees()[1],reversedAt:'2026-10-24T10:00:00Z',reversedBy:'synthetic-owner',reversalReason:'Synthetic reversal'};s.rentals.fees=[fee];
  s.receipts.push({id:fee.receiptId,nature:'PERSONAL',receiptType:'other_income',amount:fee.amount,date:fee.date});
  const view=rental.snapshot(s,today);assert.equal(view.fees[0].reversedAt,fee.reversedAt);assert.equal(view.receipts.some(r=>r.id===fee.receiptId),false);
});
test('every fee marker blocks receipt, split and advance allocation without writes, even for PERSONAL receipts',()=>{
  const markers=[{receiptType:'rental_dealer_commission'},{receiptType:'rental_documentation_fee'},{rentalKind:'dealer_commission'},{rentalKind:'documentation_fee'},{rentalFeeId:'FEE-ID'},{linkedOnly:true}];
  for(const marker of markers)for(const nature of ['PERSONAL','SAMAST'])for(const action of ['receipt','split_receipt','advance']){
    const s=fixture();s.receipts.push({id:'REC-FEE',nature,receiptType:'rental_income',tenancyId:'TEN-A',amount:1000,date:'2026-10-01',...marker});
    if(marker.linkedOnly)s.rentals.fees=[{id:'FEE-LINK',tenancyId:'TEN-A',kind:'dealer_commission',direction:'receipt',receiptId:'REC-FEE',amount:1000,date:'2026-10-01'}];
    const h=harness(s),before=h.store(),result=h.call({action,tenancyId:'TEN-A',invoiceId:'RENT-A',receiptId:'REC-FEE',amount:50,date:today,reason:'Synthetic rent allocation',splits:[{invoiceId:'RENT-A',amount:50}]});
    assert.equal(result.status,400,JSON.stringify({marker,nature,action}));assert.equal(h.writes(),0);assert.deepEqual(h.store(),before);
  }
});
test('rental correction endpoint cannot reverse a fee outside its original bank workflow',()=>{
  const s=fixture();s.rentals.fees=fees();const h=harness(s),before=h.store();
  assert.equal(h.call({action:'reverse',kind:'fee',id:s.rentals.fees[0].id,tenancyId:'TEN-A',reason:'Attempt outside reconciliation'}).status,400);assert.deepEqual(h.store(),before);assert.equal(h.writes(),0);
});
const escape=value=>String(value==null?'':value).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
async function render(tab){
  const s=fixture();s.rentals.fees=[...fees(),{...fees()[0],id:'OTHER-TENANT-FEE',tenancyId:'TEN-B',expenseId:'OTHER-TENANT-EXPENSE',paymentId:'OTHER-TENANT-PAYMENT'},{...fees()[1],id:'REVERSED-FEE',receiptId:'REVERSED-RECEIPT',reversedAt:'2026-10-24',reversalReason:'Wrong synthetic source <reviewed>'}];
  const calls=[],host={innerHTML:'',querySelector:()=>null,addEventListener(){},querySelectorAll:()=>[]},context={window:{},SankiNavigation:{choice:()=>tab,text:()=> 'TEN-A',month:()=> '2026-10',set(){}},console};
  vm.runInNewContext(fs.readFileSync(require.resolve('../public/rental-register.js'),'utf8'),context);
  context.window.mountRentalRegister(host,async (url,options)=>{calls.push({url,options});return rental.snapshot(clone(s),today)},escape,value=>'INR '+value);await new Promise(resolve=>setImmediate(resolve));return {html:host.innerHTML,calls};
}
test('selected-tenant fee UI separates payment/receipt amounts, escapes references and adds no correction controls',async()=>{
  const {html,calls}=await render('payments'),feeSection=html.match(/<section class="card"><h3>Tenant-linked dealer commission[\s\S]*?<\/section>/)[0];
  assert.match(feeSection,/Dealer commission/);assert.match(feeSection,/Documentation \/ agreement fee/);assert.match(feeSection,/<th>Paid<\/th><th>Received<\/th>/);assert.match(feeSection,/EX-FEE-0 \/ PAY-FEE-0/);assert.match(feeSection,/REC-FEE-1/);assert.match(feeSection,/Synthetic fee &lt;reference&gt;/);
  assert.match(feeSection,/Reversed · Wrong synthetic source &lt;reviewed&gt;/);assert.doesNotMatch(feeSection,/OTHER-TENANT/);assert.doesNotMatch(feeSection,/<form|<button|data-reverse/);assert.match(html,/data-action="settle"/);assert.match(html,/data-action="security_record"/);
  assert.equal(calls.length,1);assert.equal(calls[0].options,undefined);
});
test('fee history shows the tenant of each source without adding it to rent correction forms',async()=>{
  const {html,calls}=await render('history'),feeSection=html.match(/<section class="card"><h3>Dealer commission and documentation[\s\S]*?<\/section>/)[0];
  assert.match(feeSection,/Synthetic TEN-A · TEN-A/);assert.match(feeSection,/Synthetic TEN-B · TEN-B/);assert.match(feeSection,/OTHER-TENANT-EXPENSE/);assert.doesNotMatch(feeSection,/<form|<button|data-reverse/);assert.equal(calls.length,1);
});

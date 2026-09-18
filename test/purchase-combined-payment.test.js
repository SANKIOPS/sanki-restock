const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm'),path=require('node:path');
const source=fs.readFileSync(path.join(__dirname,'../modules/expenses.js'),'utf8');
function route(items){
  const start=source.indexOf("router.post('/api/expenses/procurement-payables/batch'"),end=source.indexOf('\n});',start)+4;
  let fn,saves=0;const store={procurementAccounting:{paymentsByPo:{}}},calls=[];
  vm.runInNewContext(source.slice(start,end),{router:{post:(_,f)=>fn=f},canApprove:()=>true,loadStore:()=>store,procurementPayables:()=>items,proofList:(a,b)=>b?[b]:[],allowedPayingAccount:(_,n,a)=>a==='SANKI Bank'?a:'',round0:Math.round,procurementAccounting:s=>s.procurementAccounting,PAYMENT_TYPES:['UPI'],crypto:require('node:crypto'),audit:(...a)=>calls.push(a),saveStore:()=>saves++});
  return {pay:body=>{let code=200,result;const res={status:n=>{code=n;return res;},json:x=>{result=x;}};fn({body,user:{username:'tester'}},res);return {code,result,saves};},store,calls};
}
const items=[{id:'PO-1',supplier:'Vendor A',billNo:'101',balanceDue:30000},{id:'PO-2',supplier:'Vendor A',billNo:'102',balanceDue:40000}];
test('combined purchase payment retains one reference and separate bill allocations',()=>{
 const ctx=route(items),r=ctx.pay({poIds:['PO-1','PO-2'],amount:70000,account:'SANKI Bank',date:'2026-09-18',paymentProof:'/proof.png',reference:'BANK-123',paymentType:'UPI'});
 assert.equal(r.code,200);assert.equal(r.result.totalAmount,70000);assert.equal(r.result.allocations.length,2);assert.equal(r.saves,1);
 const a=ctx.store.procurementAccounting.paymentsByPo['PO-1'].payments[0],b=ctx.store.procurementAccounting.paymentsByPo['PO-2'].payments[0];
 assert.equal(a.amount,30000);assert.equal(b.amount,40000);assert.equal(a.batchPaymentId,b.batchPaymentId);assert.equal(a.reference,'BANK-123');assert.deepEqual(Array.from(a.linkedPoIds),['PO-1','PO-2']);assert.equal(ctx.calls.length,2);
});
test('combined payment rejects cross-vendor and wrong amount atomically',()=>{
 let ctx=route([{...items[0]},{...items[1],supplier:'Vendor B'}]);assert.equal(ctx.pay({poIds:['PO-1','PO-2']}).code,400);assert.equal(ctx.store.procurementAccounting.paymentsByPo['PO-1'],undefined);
 ctx=route(items);assert.equal(ctx.pay({poIds:['PO-1','PO-2'],amount:60000,account:'SANKI Bank',date:'2026-09-18',paymentProof:'/proof.png'}).code,400);assert.equal(ctx.store.procurementAccounting.paymentsByPo['PO-1'],undefined);
});

test('bank ledger shows one debit for a combined purchase payment',()=>{
 const start=source.indexOf("  if (nature === 'SANKI') {\n    const combinedPurchases = new Map();"),end=source.indexOf("  salaryAdvanceEntries().filter(x=>x.payingNature===nature",start);
 assert.ok(start>0&&end>start);
 const block=source.slice(start,end),entries=[];
 const batchPaymentId='PPB-1',payments=[{id:'PO-1',vendor:'Mediator',payments:[{id:'PPAY-1',batchPaymentId,amount:30000,account:'SANKI Bank',date:'2026-09-18',reference:'BANK-123'}]},{id:'PO-2',vendor:'Mediator',payments:[{id:'PPAY-1',batchPaymentId,amount:40000,account:'SANKI Bank',date:'2026-09-18',reference:'BANK-123'}]}];
 vm.runInNewContext(block,{nature:'SANKI',entries,account:'SANKI Bank',s:{},procurementPayables:()=>payments,num:Number});
 assert.equal(entries.length,1);assert.equal(entries[0].debit,70000);assert.deepEqual(Array.from(entries[0].linkedPoIds),['PO-1','PO-2']);
});

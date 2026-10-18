const test=require('node:test'),assert=require('node:assert/strict'),fs=require('fs'),os=require('os'),path=require('path');
const api=require('../modules/gokwik-api'),auto=require('../modules/gokwik-auto-sync');
const sample={transaction_type:'Payment',payment_id:'PAY1',reference_id:'REF1',merchant_order_id:'7241015361788',amount:150,fee:3.75,tax:.68,credit:145.57,debit:0,deductions:{gokwik:75},settlement_utr:'UTR12345678',settlement_date:'2026-08-25T00:00:00Z',settled_by:'easebuzz'};
test('API split net subtracts all account deductions and real debit exactly once',()=>{
 const r=api.normalize({...sample,debit:5,deductions:{gokwik:75,other:2}},'2026-08-25');assert.equal(r.net,63.57);assert.equal(r.bankVerified,false);assert.equal(r.credit,145.57);assert.equal(r.gross,150);
 const refund=api.normalize({...sample,transaction_type:'Refund',credit:0,debit:150,deductions:{}},'2026-08-25');assert.equal(refund.net,-150);
 const precise=api.normalize({...sample,credit:'145.5712',deductions:{}},'2026-08-25');assert.equal(precise.net,145.5712);
 assert.throws(()=>api.normalize({...sample,merchant_order_id:'7.24E+12'},'2026-08-25'),/identifier/);
 assert.throws(()=>api.normalize({...sample,credit:null},'2026-08-25'),/missing/);
});
test('API pagination uses documented query and private headers, and preserves identical legitimate entries',async()=>{
 const calls=[];const rows=await api.fetchDay({date:'2026-08-25',appId:'private-id',appSecret:'private-secret',pageSize:2,pause:async()=>{},fetchImpl:async(url,options)=>{
  calls.push({url,options});return {ok:true,status:200,json:async()=>({page_number:calls.length,count:calls.length===1?2:0,items:calls.length===1?[sample,sample]:[]})};
 }});
 assert.equal(calls.length,2);assert.equal(new URL(calls[0].url).searchParams.get('from'),'2026-08-25');assert.equal(new URL(calls[0].url).searchParams.get('to'),'2026-08-25');assert.equal(calls[0].options.headers['gk-app-secret'],'private-secret');assert.ok(!calls[0].url.includes('private'));assert.equal(rows.length,2);assert.notEqual(rows[0].apiRowId,rows[1].apiRowId);
});
test('invalid or repeating pagination and access denial fail closed',async()=>{
 await assert.rejects(api.fetchDay({date:'2026-08-25',appId:'a',appSecret:'s',pause:async()=>{},pageSize:1,fetchImpl:async()=>({ok:true,json:async()=>({count:1,items:[sample]})})}),/repeated/);
 await assert.rejects(api.fetchDay({date:'2026-08-25',appId:'a',appSecret:'s',fetchImpl:async()=>({ok:false,status:403})}),/activation/);
});
test('API records supersede matching uploaded payments without doubling collection',()=>{
 const apiRow=api.normalize(sample,'2026-08-25');const merged=api.mergedEvidence([{paymentId:'PAY1',type:'Payment',utr:'UTR12345678',gross:150},{paymentId:'OTHER',type:'Payment',utr:'OLD',gross:20}],[apiRow]);assert.equal(merged.length,2);assert.equal(merged.reduce((n,r)=>n+r.gross,0),170);
});
test('API refund is included in batch bank net without being another customer payment',()=>{
 const {attachSettlementLinks}=require('../modules/order-settlement-links'),{integrateOrder}=require('../modules/sales-order-view');
 const payment=api.normalize({...sample,deductions:{}},'2026-08-25'),refund=api.normalize({...sample,transaction_type:'Refund',reference_id:'REFUND1',credit:0,debit:25,deductions:{}},'2026-08-25');
 const view=integrateOrder({id:payment.orderId,total:150,lineItems:[]},{},[],[],[],[payment,refund]);assert.equal(view.received,150);
 attachSettlementLinks([view],{bankStatements:{'Tiana Traders IndusInd 0437':{transactions:{credit:{credit:120.57,utr:'UTR12345678',date:'2026-08-25'}}}}},[payment,refund],[]);assert.equal(view.bankEvidence.length,1);
});
test('daily checkpoints resume and retain prior evidence on provider failure',async()=>{
 const dir=fs.mkdtempSync(path.join(os.tmpdir(),'gokwik-api-')),file=path.join(dir,'evidence.json');const oldId=process.env.GOKWIK_APP_ID,oldSecret=process.env.GOKWIK_APP_SECRET;
 process.env.GOKWIK_APP_ID='id';process.env.GOKWIK_APP_SECRET='secret';const write=(f,v)=>fs.writeFileSync(f,v);
 try{
  const orders={one:{channel:'Website',createdAt:'2026-08-25T12:00:00Z'}};
  await auto.sync({file,orders,write,now:new Date('2026-08-28T12:00:00Z'),force:true,pause:async()=>{},fetchDay:async({date})=>[api.normalize(sample,date)]});
  assert.equal(auto.read(file).days['2026-08-25'].length,1);
  await assert.rejects(auto.sync({file,orders,write,now:new Date('2026-08-28T12:00:00Z'),force:true,pause:async()=>{},fetchDay:async()=>{throw Error('provider offline');}}),/offline/);
  assert.equal(auto.read(file).days['2026-08-25'].length,1);assert.equal(auto.read(file).error,'provider offline');
  await assert.rejects(auto.sync({file,orders,write,now:new Date('2026-08-28T12:00:00Z'),force:true,pause:async()=>{},fetchDay:async()=>[]}),/previously populated/);
 }finally{if(oldId==null)delete process.env.GOKWIK_APP_ID;else process.env.GOKWIK_APP_ID=oldId;if(oldSecret==null)delete process.env.GOKWIK_APP_SECRET;else process.env.GOKWIK_APP_SECRET=oldSecret;fs.rmSync(dir,{recursive:true,force:true});}
});
test('repeated settlement entries for one payment never double the customer receipt',()=>{
 const {integrateOrder}=require('../modules/sales-order-view');const r=api.normalize(sample,'2026-08-25');
 const view=integrateOrder({id:r.orderId,total:150,lineItems:[]},{},[],[],[],[r,{...r,date:'2026-08-26',utr:'OTHERUTR'}]);
 assert.equal(view.received,150);assert.equal(view.balance,0);assert.equal(view.onlineEvidence.length,2);
});

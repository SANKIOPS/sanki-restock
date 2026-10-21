'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),path=require('node:path'),vm=require('node:vm'),crypto=require('node:crypto');
const source=fs.readFileSync(path.join(__dirname,'..','modules','telegram.js'),'utf8');

// Exercise the real callback/upload handlers with synthetic records and no
// Telegram, OCR, network, timers or production storage side effects.
function harness(status='approved',paidAmount=0,roles=['admin']){
  const expense={id:'EX-00999',nature:'SANKI',status,approvedAt:status==='pending'?null:'2026-10-17T10:00:00Z',amount:418,paidAmount,payments:[],vendor:'Synthetic vendor',ledger:'Fuel Expenses'},sent=[],recorded=[];
  const expenses={telegramExpense:()=>expense,telegramPayingAccounts:()=>['Prashant Axis 3645','Counter Cash'],telegramRecordPayment(id,actor,body,access){recorded.push({kind:'payment',id,actor,body:{...body},access});return{success:true,expense,payment:{amount:body.amount,account:body.account}};},telegramRecordProvisionalPayment(id,actor,body,access){recorded.push({kind:'provisional',id,actor,body:{...body},access});return{success:true,expense,payment:{amount:body.amount,account:body.account}};}};
  const context={module:{exports:{}},console,Buffer,process:{env:{DATA_PATH:path.join(__dirname,'synthetic','data.json')},pid:1},setTimeout:()=>({unref(){}}),setInterval:()=>({unref(){}}),__sent:sent,__recorded:recorded,__ocr:{account:'3645'},require(name){if(name==='./expenses')return expenses;if(name==='./auth-users')return{userSummary:()=>({username:'prashant',roles})};if(name==='express')return{Router:()=>({get(){},post(){}}),json:()=>()=>{}};if(name==='https')return{request(){throw new Error('Unexpected network request');}};if(name==='fs')return{readFileSync(){throw new Error('No runtime data');},existsSync:()=>false};if(name==='path')return path;if(name==='crypto')return crypto;if(name==='tesseract.js')return{createWorker(){throw new Error('Unexpected OCR worker');}};if(name==='@tesseract.js-data/eng')return{};throw new Error('Unexpected dependency: '+name);}};
  vm.runInNewContext(source+'\n tg=async(method,payload)=>{__sent.push({method,payload});return{ok:true,result:{message_id:__sent.length}};}; save=()=>{}; downloadPhoto=async()=>({filePath:"synthetic.jpg",url:"/api/expenses/photo/synthetic.jpg"}); readPaymentScreenshot=async()=>__ocr; module.exports.__handlers={callback:handleCallback,message:handleWebhookMessage,blank};',context,{filename:'telegram.js'});
  const handlers=context.module.exports.__handlers,state=handlers.blank();state.userChats.prashant=['123'];state.chats['123']={name:'prashant'};
  const callback=data=>handlers.callback({id:'test-query',data,message:{chat:{id:123}}},state);
  const upload=()=>{const key=Object.keys(state.paymentPrompts).at(-1);assert.ok(key,'payment proof prompt should exist');return handlers.message({chat:{id:123,type:'private'},reply_to_message:{message_id:Number(key.split(':').at(-1))},photo:[{file_id:'synthetic-file',file_unique_id:'synthetic-proof'}]},state);};
  const draft=()=>Object.entries(state.businessDrafts).find(([,d])=>['payment','provisional-payment'].includes(d.kind));
  const finish=async()=>{const [key]=draft();await callback('bp:acct:'+key+':0');await callback('bp:confirm:'+key);};
  return{expense,sent,recorded,state,callback,upload,draft,finish,lastMessage:()=>sent.filter(x=>x.method==='sendMessage').at(-1)?.payload};
}

for(const [status,paidAmount] of [['approved',0],['partially_paid',100]])test(status+' expenses use proof, account buttons and ordinary payment posting',async()=>{
  const h=harness(status,paidAmount),approval=h.expense.approvedAt;
  await h.callback('bp:start:EX-00999');await h.upload();
  const [key,d]=h.draft();assert.equal(d.kind,'payment');assert.equal(d.amount,418-paidAmount);
  const choices=h.lastMessage().reply_markup.inline_keyboard.flat();assert.ok(choices.some(b=>b.text.includes('Prashant Axis 3645')));assert.ok(choices.some(b=>b.text==='Counter Cash'));
  await h.finish();assert.equal(h.recorded.length,1);assert.equal(h.recorded[0].kind,'payment');assert.equal(h.recorded[0].body.account,'Prashant Axis 3645');assert.equal(h.recorded[0].body.amount,418-paidAmount);assert.equal(h.expense.approvedAt,approval);assert.equal(h.state.businessDrafts[key],undefined);
});

test('old pending-payment button continues after approval elsewhere',async()=>{
  const h=harness('approved');await h.callback('bp:startpending:EX-00999');
  assert.equal(Object.values(h.state.paymentPrompts)[0].provisional,false);await h.upload();await h.finish();assert.equal(h.recorded[0].kind,'payment');
});

test('old Approve notification offers payment for an already approved bill',async()=>{
  const h=harness('approved');await h.callback('ba:approve:EX-00999');
  assert.ok(h.lastMessage().reply_markup.inline_keyboard.flat().some(b=>b.callback_data==='bp:start:EX-00999'));assert.equal(h.recorded.length,0);
});

test('approval while waiting for proof switches the upload to an approved payment',async()=>{
  const h=harness('pending');await h.callback('bp:startpending:EX-00999');h.expense.status='approved';h.expense.approvedAt='2026-10-18T10:00:00Z';await h.upload();
  assert.equal(h.draft()[1].kind,'payment');await h.finish();assert.equal(h.recorded[0].kind,'payment');
});

test('approval after proof upload keeps the selected account and posts the same payment normally',async()=>{
  const h=harness('pending');await h.callback('bp:startpending:EX-00999');await h.upload();assert.equal(h.draft()[1].kind,'provisional-payment');
  h.expense.status='approved';h.expense.approvedAt='2026-10-18T10:00:00Z';await h.finish();assert.equal(h.recorded.length,1);assert.equal(h.recorded[0].kind,'payment');assert.equal(h.recorded[0].body.account,'Prashant Axis 3645');assert.doesNotMatch(h.lastMessage().text,/still pending approval/);
});

test('unapproved bills retain provisional posting',async()=>{
  const h=harness('pending');await h.callback('bp:startpending:EX-00999');await h.upload();await h.finish();assert.equal(h.recorded[0].kind,'provisional');assert.match(h.lastMessage().text,/still pending approval/);
});

for(const status of ['paid','rejected'])test(status+' bills cannot start another payment from an older button',async()=>{
  const h=harness(status,status==='paid'?418:0);await h.callback('bp:startpending:EX-00999');assert.equal(Object.keys(h.state.paymentPrompts).length,0);assert.equal(h.recorded.length,0);
});

test('confirmation rechecks entity authorization after the expense changes',async()=>{
  const h=harness('approved',0,['accounting']);await h.callback('bp:start:EX-00999');await h.upload();h.expense.nature='PERSONAL';await h.finish();assert.equal(h.recorded.length,0);assert.match(h.lastMessage().text,/not authorized/);
});

test('approved-payment accounting records partial and final payments once with proof and account validation',()=>{
  const dataDir=fs.mkdtempSync(path.join(__dirname,'.tmp-telegram-approved-')),priorDataPath=process.env.DATA_PATH;
  process.env.DATA_PATH=path.join(dataDir,'data.json');fs.writeFileSync(process.env.DATA_PATH,'{}');fs.writeFileSync(path.join(dataDir,'expenses.json'),'{}');
  try{
    const e=require('../modules/expenses'),owner={username:'gaganlambasanki',roles:['owner']},access={roles:['admin']},date=e.indiaBusinessDate();
    const created=e.telegramApi('POST','/api/expenses',owner,{body:{nature:'SANKI',vendor:'Synthetic approved vendor',vendorPhone:'9999999999',particulars:'Synthetic approved payment',amount:418,date,bill:'printed',billPhoto:'/api/expenses/photo/synthetic-bill.jpg',qrPhoto:'/api/expenses/photo/synthetic-qr.jpg',ledger:'Fuel Expenses',paymentType:'UPI'}});
    assert.equal(created.success,true,JSON.stringify(created));const id=created.expense.id,approved=e.telegramApproveExpense(id,'prashant',{ledger:'Fuel Expenses'});assert.equal(approved.success,true,JSON.stringify(approved));const approvedAt=approved.expense.approvedAt;
    const details={amount:100,date,account:'Prashant Axis 3645',proof:'/api/expenses/photo/synthetic-approved-payment.jpg'};
    assert.equal(e.telegramRecordPayment(id,'prashant',{...details,proof:''},access).success,false);
    assert.equal(e.telegramRecordPayment(id,'prashant',{...details,account:'Axis Bank 3448'},access).success,false);
    const partial=e.telegramRecordPayment(id,'prashant',details,access);assert.equal(partial.success,true);assert.equal(partial.expense.status,'partially_paid');assert.equal(partial.expense.paidAmount,100);
    assert.equal(e.telegramRecordPayment(id,'prashant',{...details,amount:319},access).success,false);
    const final=e.telegramRecordPayment(id,'prashant',{...details,amount:318,proof:'/api/expenses/photo/synthetic-final-payment.jpg'},access);assert.equal(final.success,true);assert.equal(final.expense.status,'paid');assert.equal(final.expense.paidAmount,418);assert.equal(final.expense.approvedAt,approvedAt);assert.equal(final.expense.ledger,'Fuel Expenses');assert.equal(final.expense.payments.length,2);assert.ok(final.expense.payments.every(p=>!p.provisional));
    assert.equal(e.telegramRecordPayment(id,'prashant',{...details,amount:1},access).success,false);
    const ledger=e.telegramApi('GET','/api/expenses/account-ledger',owner,{query:{nature:'SANKI',account:'Prashant Axis 3645',from:date,to:date}});assert.equal(ledger.success,true);const movements=ledger.entries.filter(row=>String(row.id).startsWith(id+'/'));assert.equal(movements.length,2);assert.equal(movements.reduce((sum,row)=>sum+row.debit,0),418);
  }finally{
    if(priorDataPath===undefined)delete process.env.DATA_PATH;else process.env.DATA_PATH=priorDataPath;
    fs.rmSync(dataDir,{recursive:true,force:true});
  }
});

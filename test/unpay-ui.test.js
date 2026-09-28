'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm'),path=require('node:path');
const html=fs.readFileSync(path.join(__dirname,'../public/expenses.html'),'utf8');
function harness({reason='Wrong account',confirm=true,response={success:true,expense:{status:'partially_paid'}},error,blocked='',canApprove=true}={}){
 const calls=[],messages=[],warnings=[];let loads=0;
 const ctx={window:{},cfg:{canApprove,approvalNatures:['SANKI']},expensesById:{'EX-1':{payments:[{id:'PAY-002',unpayBlockedReason:blocked}]}},prompt:()=>reason,confirm:s=>{warnings.push(s);return confirm;},setMsg:(...v)=>messages.push(v),loadList:()=>loads++,api:(...args)=>{calls.push(args);return error?Promise.reject(new Error(error)):Promise.resolve(response);},encodeURIComponent,fmt:String,esc:String,proofGallery:()=>''};
 vm.runInNewContext(html.slice(html.indexOf('    var unpayPending='),html.indexOf('    window.openReimburse =')),ctx);
 vm.runInNewContext(html.slice(html.indexOf('    function paymentHistory('),html.indexOf('    function proofGallery(')),ctx);
 return {ctx,calls,messages,warnings,loads:()=>loads};
}
test('Unpay confirms only the selected payment, requires a reason, and refreshes after success',async()=>{
 const h=harness({reason:'  Wrong account  '});await h.ctx.window.unpayExpense('EX-1','PAY-002');
 assert.equal(h.calls.length,1);assert.equal(h.calls[0][0],'/api/expenses/EX-1/payments/PAY-002');assert.equal(h.calls[0][1].method,'DELETE');assert.deepEqual(JSON.parse(h.calls[0][1].body),{reason:'Wrong account'});
 assert.match(h.warnings[0],/Only this payment/);assert.match(h.warnings[0],/expense, other payments, payment proofs and audit history will remain/);assert.equal(h.loads(),1);
});
test('blank reason and cancellation never send a removal',async()=>{
 for(const opts of [{reason:null},{reason:'   '},{confirm:false},{blocked:'Reconciliation must be corrected'}]){const h=harness(opts);await h.ctx.window.unpayExpense('EX-1','PAY-002');assert.equal(h.calls.length,0);}
});
test('server conflicts and network errors are visible and allow retry',async()=>{
 for(const opts of [{response:{success:false,error:'Reconciled payment'}},{error:'Offline'}]){const h=harness(opts);await h.ctx.window.unpayExpense('EX-1','PAY-002');assert.equal(h.loads(),0);assert.equal(h.messages[0][1],false);assert.match(h.messages[0][0],/Reconciled|Offline/);await h.ctx.window.unpayExpense('EX-1','PAY-002');assert.equal(h.calls.length,2);}
});
test('duplicate clicks submit once while a request is pending',async()=>{
 const h=harness();const pending=h.ctx.window.unpayExpense('EX-1','PAY-002');h.ctx.window.unpayExpense('EX-1','PAY-002');await pending;assert.equal(h.calls.length,1);
});
test('individual payment controls respect role, entity and reconciliation protection',()=>{
 const e={id:'EX-1',nature:'SANKI',payments:[{id:'PAY-001',amount:20},{id:'PAY-002',amount:30,unpayBlockedReason:'Correct reconciliation first'}]};
 const h=harness(),render=h.ctx.paymentHistory(e);assert.match(render,/unpayExpense\('EX-1','PAY-001'\)/);assert.match(render,/disabled>Unpay/);assert.match(render,/Correct reconciliation first/);assert.doesNotMatch(render,/unpayExpense\('EX-1','PAY-002'\)/);
 assert.doesNotMatch(harness({canApprove:false}).ctx.paymentHistory(e),/Unpay/);assert.doesNotMatch(h.ctx.paymentHistory({...e,nature:'PERSONAL'}),/Unpay/);
});

const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const vm=require('node:vm');
const path=require('node:path');
const html=fs.readFileSync(path.join(__dirname,'../public/expenses.html'),'utf8');
const code=html.slice(html.indexOf('    window.correctSaleAllocation=async function'),html.indexOf('    window.correctSaleAllocation=async function')+html.slice(html.indexOf('    window.correctSaleAllocation=async function')).indexOf('\n    };')+7);
function setup(answers,api){
 const alerts=[],calls=[];
 const ctx={window:{},ledgerEditRows:{'SHOPIFY/1':{account:'Counter Cash',row:{gross:11296,cashAmount:11300}}},prompt:()=>answers.shift(),alert:m=>alerts.push(m),setMsg:()=>{},fmt:n=>'₹'+n,api:async(...args)=>{calls.push(args);return api(...args);},loadCash:async()=>{},loadAccountLedger:async()=>{},loadConfig:async()=>{},syncApprovalsTab:()=>{}};
 vm.runInNewContext(code,ctx);return {ctx,alerts,calls};
}
test('over-total cash shows immediate error and can retry without saving invalid amount',async()=>{
 const x=setup(['11300','11,000','Correct receipt'],()=>({success:true,approved:true,allocation:{cashAmount:11000,nonCashAmount:296}}));
 await x.ctx.window.correctSaleAllocation('SHOPIFY/1');
 assert.match(x.alerts[0],/exceeds.*₹4/);assert.equal(x.calls.length,1);assert.equal(JSON.parse(x.calls[0][1].body).cashAmount,11000);assert.match(x.alerts[1],/saved/);
});
test('cancel after invalid cash makes no save',async()=>{
 const x=setup(['11300',null],()=>{throw Error('Unexpected save');});await x.ctx.window.correctSaleAllocation('SHOPIFY/1');assert.equal(x.calls.length,0);
});
test('failed server save is visible immediately',async()=>{
 const x=setup(['11000','Correction'],()=>({success:false,error:'Reopen reconciliation'}));await x.ctx.window.correctSaleAllocation('SHOPIFY/1');assert.equal(x.alerts[0],'Reopen reconciliation');
});

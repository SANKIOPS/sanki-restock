'use strict';
const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const tempDir=fs.mkdtempSync(path.join(__dirname,'.tmp-salary-accounts-'));
process.env.DATA_PATH=path.join(tempDir,'data.json');
fs.writeFileSync(path.join(tempDir,'expenses.json'),'{}');
const salary=require('../modules/salary').router,expenses=require('../modules/expenses').router;
test.after(()=>fs.rmSync(tempDir,{recursive:true,force:true}));
function invoke(router,method,routePath,{body={},params={},query={}}={}){
  const layer=router.stack.find(item=>item.route&&item.route.path===routePath&&item.route.methods[method]);assert.ok(layer);
  let status=200,result,index=0;const req={body,params,query,method:method.toUpperCase(),path:routePath,route:{path:routePath},headers:{},get(){return'';},ip:'127.0.0.1',user:{username:'owner',role:'owner',roles:['owner']}},res={status(value){status=value;return this;},json(value){result=value;return this;},end(){return this;}};
  const next=error=>{if(error)throw error;const handler=layer.route.stack[index++];if(handler)handler.handle(req,res,next);};next();return{status,body:result};
}
test('3448 salary payment debits its bank ledger once without moving any amount through 3645',()=>{
  const employee=invoke(salary,'post','/api/salary/employees',{body:{name:'Bank Source Test',salary:42500,joiningDate:'2099-01-01'}}).body.employee;
  invoke(salary,'post','/api/salary/row/:ym',{params:{ym:'2099-01'},body:{empId:employee.id,paidDays:30}});
  const balances=()=>invoke(expenses,'get','/api/expenses/balances',{query:{nature:'SANKI'}}).body.accounts;
  const before=balances(),balance=(rows,name)=>rows.find(item=>item.name===name).balance;
  const paid=invoke(salary,'post','/api/salary/payments/batch',{body:{ym:'2099-01',date:'2099-01-31',account:'Axis Bank 3448',proofs:['/salary-proof.jpg'],reference:'SALARY-3448-TEST',items:[{empId:employee.id,amount:42500}]}});assert.equal(paid.status,200);
  const after=balances();assert.equal(balance(after,'Axis Bank 3448'),balance(before,'Axis Bank 3448')-42500);assert.equal(balance(after,'Prashant Axis 3645'),balance(before,'Prashant Axis 3645'));
  const entries=invoke(expenses,'get','/api/expenses/account-ledger',{query:{nature:'SANKI',account:'Axis Bank 3448'}}).body.entries.filter(item=>item.id===paid.body.batchId+'-001');assert.equal(entries.length,1);assert.equal(entries[0].debit,42500);
  const salaryView=invoke(salary,'get','/api/salary/month/:ym',{params:{ym:'2099-01'}}).body.rows.find(item=>item.id===employee.id);assert.equal(salaryView.paid,42500);assert.equal(salaryView.balance,0);
});

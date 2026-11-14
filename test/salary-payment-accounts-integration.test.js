'use strict';
const test=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const tempDir=fs.mkdtempSync(path.join(__dirname,'.tmp-salary-accounts-'));
process.env.DATA_PATH=path.join(tempDir,'data.json');
fs.writeFileSync(path.join(tempDir,'expenses.json'),'{}');
const salaryModule=require('../modules/salary'),salary=salaryModule.router,expenses=require('../modules/expenses').router;
test.after(()=>fs.rmSync(tempDir,{recursive:true,force:true}));
function invoke(router,method,routePath,{body={},params={},query={}}={}){
  const layer=router.stack.find(item=>item.route&&item.route.path===routePath&&item.route.methods[method]);assert.ok(layer);
  let status=200,result,index=0;const req={body,params,query,method:method.toUpperCase(),path:routePath,route:{path:routePath},headers:{},get(){return'';},ip:'127.0.0.1',user:{username:'owner',role:'owner',roles:['owner']}},res={status(value){status=value;return this;},json(value){result=value;return this;},end(){return this;}};
  const next=error=>{if(error)throw error;const handler=layer.route.stack[index++];if(handler)handler.handle(req,res,next);};next();return{status,body:result};
}
function salaryApi(entity,method,routePath,{body={},params={}}={}){
  return salaryModule.telegramApi(method,routePath,{username:'owner',roles:['owner']},{entity,body,params});
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

test('personal salary sources debit the correct physical account balances while SAMAST 7883 remains SAMAST',()=>{
  const cases=[
    {entity:'SANKI',account:'IndusInd Bank 7883',nature:'PERSONAL'},
    {entity:'SANKI',account:'ICICI Bank 0993',nature:'PERSONAL'},
    {entity:'SANKI',account:'ICICI Bank 0992',nature:'PERSONAL'},
    {entity:'SAMAST',account:'IndusInd Bank 7883',nature:'SAMAST'},
    {entity:'SAMAST',account:'ICICI Bank 0993',nature:'PERSONAL'},
    {entity:'SAMAST',account:'ICICI Bank 0992',nature:'PERSONAL'}
  ];
  cases.forEach((item,index)=>{
    const amount=100+index,ym='2099-0'+(index<3?'2':'3');
    const balancesBefore=invoke(expenses,'get','/api/expenses/balances',{query:{nature:item.nature}}).body.accounts;
    const before=balancesBefore.find(row=>row.name===item.account);assert.ok(before);
    const created=salaryApi(item.entity,'post','/api/salary/employees',{body:{name:item.entity+' ledger source '+index,salary:amount,joiningDate:ym+'-01'}});
    assert.equal(created.status,200);
    assert.equal(salaryApi(item.entity,'post','/api/salary/row/:ym',{params:{ym},body:{empId:created.employee.id,paidDays:30}}).status,200);
    const paid=salaryApi(item.entity,'post','/api/salary/payments/batch',{body:{ym,date:ym+'-28',account:item.account,proofs:['/salary-proof.jpg'],reference:'SALARY-NATURE-'+index,items:[{empId:created.employee.id,amount}]}});
    assert.equal(paid.status,200,paid.error);
    const file=path.join(tempDir,item.entity==='SAMAST'?'salary-samast.json':'salary.json');
    const stored=JSON.parse(fs.readFileSync(file,'utf8')).salaryPayments.find(row=>row.batchId===paid.batchId);
    assert.equal(stored.payingNature,item.nature);
    const balancesAfter=invoke(expenses,'get','/api/expenses/balances',{query:{nature:item.nature}}).body.accounts;
    const after=balancesAfter.find(row=>row.name===item.account);assert.ok(after);
    assert.equal(after.spent,before.spent+amount,item.entity+' '+item.account+' should record one '+item.nature+' outflow');
  });
});

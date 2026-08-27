'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'sanki-salary-'));
process.env.DATA_PATH = path.join(tempDir, 'data.json');
const { router } = require('../modules/salary');
test.after(() => fs.rmSync(tempDir, { recursive:true, force:true }));

function invoke(method, routePath, { body={}, params={}, query={}, role='admin' }={}) {
  const layer=router.stack.find(x=>x.route&&x.route.path===routePath&&x.route.methods[method.toLowerCase()]);
  assert.ok(layer, 'route exists: '+method+' '+routePath); let status=200,result;
  const req={body,params,query,user:{username:'tester',role,roles:[role]}};
  const res={status(n){status=n;return this;},json(v){result=v;return this;}};
  let i=0; const next=()=>{const h=layer.route.stack[i++];if(h)h.handle(req,res,next);}; next();
  return {status,body:result};
}

test('salary advances require proof, summarize balances and recover oldest first', () => {
  const emp=invoke('POST','/api/salary/employees',{body:{name:'Employee A',salary:30000,channel:'Shared'}}).body.employee;
  const missing=invoke('POST','/api/salary/advances',{body:{empId:emp.id,amount:1000,date:'2026-08-22',account:'Axis Bank 3448'}});
  assert.equal(missing.status,400); assert.match(missing.body.error,/proof/i);
  [1000,2000,2000].forEach((amount,i)=>{
    const made=invoke('POST','/api/salary/advances',{body:{empId:emp.id,amount,date:'2026-08-'+String(22+i).padStart(2,'0'),account:'Axis Bank 3448',proof:'/proof-'+i+'.jpg',recoveryStartMonth:'2026-08'}});
    assert.equal(made.status,200);
  });
  let list=invoke('GET','/api/salary/advances',{query:{summaryMonth:'2026-08'}}).body;
  const summary=list.summary.find(x=>x.empId===emp.id); assert.equal(summary.total,5000); assert.equal(summary.thisMonth,5000); assert.equal(summary.outstanding,5000);
  const recovered=invoke('POST','/api/salary/recoveries/:ym',{params:{ym:'2026-08'},body:{empId:emp.id,amount:2500}});
  assert.equal(recovered.status,200);
  list=invoke('GET','/api/salary/advances').body; const own=list.advances.filter(x=>x.empId===emp.id).sort((a,b)=>a.date.localeCompare(b.date));
  assert.equal(own[0].status,'Recovered'); assert.equal(own[1].recovered,1500); assert.equal(own[1].status,'Partially recovered'); assert.equal(list.summary.find(x=>x.empId===emp.id).outstanding,2500);
  const payroll=invoke('GET','/api/salary/month/:ym',{params:{ym:'2026-08'}}).body.rows.find(x=>x.id===emp.id);
  assert.equal(payroll.loggedAdvanceRecovery,2500); assert.equal(payroll.outstandingAdvance,2500);
  const tooMuch=invoke('POST','/api/salary/recoveries/:ym',{params:{ym:'2026-08'},body:{empId:emp.id,amount:6000}}); assert.equal(tooMuch.status,400);
});

test('advance UI is collapsed by default and exposes account posting and audit concepts', () => {
  const html=fs.readFileSync(path.join(__dirname,'..','public','salary.html'),'utf8');
  assert.match(html,/data-v="advances"/); assert.match(html,/<details class="card"><summary>Employee advance summary/); assert.match(html,/Payment proof \(required\)/); assert.match(html,/saveRecovery/); assert.match(html,/oldest eligible advance first/);
  assert.match(html,/S\.No\./); assert.match(html,/\(index\+1\)/);
});

test('every salary employee list is returned alphabetically A to Z', () => {
  invoke('POST','/api/salary/employees',{body:{name:'zulu employee',salary:10000}});
  invoke('POST','/api/salary/employees',{body:{name:'Alpha employee',salary:10000}});
  const employees=invoke('GET','/api/salary/employees').body.employees.map(x=>x.name);
  assert.deepEqual(employees,employees.slice().sort((a,b)=>a.localeCompare(b,'en',{sensitivity:'base',numeric:true})));
  const payroll=invoke('GET','/api/salary/month/:ym',{params:{ym:'2026-08'}}).body.rows.map(x=>x.name);
  assert.deepEqual(payroll,employees);
  const summary=invoke('GET','/api/salary/advances').body.summary.map(x=>x.name);
  assert.deepEqual(summary,summary.slice().sort((a,b)=>a.localeCompare(b,'en',{sensitivity:'base',numeric:true})));
});

test('assigned weekly off converts an absent mark only on that weekday', () => {
  const created=invoke('POST','/api/salary/employees',{body:{name:'Weekly Off Test',salary:12000,weekOffDay:'Sunday'}}).body.employee;
  assert.equal(created.weekOffDay,'Sunday');
  const sunday=invoke('POST','/api/salary/attendance/:ym',{params:{ym:'2026-08'},body:{empId:created.id,day:'23',mark:'A'}});
  const monday=invoke('POST','/api/salary/attendance/:ym',{params:{ym:'2026-08'},body:{empId:created.id,day:'24',mark:'A'}});
  assert.equal(sunday.body.mark,'WO'); assert.equal(monday.body.mark,'A');
  const month=invoke('GET','/api/salary/month/:ym',{params:{ym:'2026-08'}}).body;
  assert.equal(month.attendance[created.id]['23'],'WO'); assert.equal(month.attendance[created.id]['24'],'A');
  const html=fs.readFileSync(path.join(__dirname,'..','public','salary.html'),'utf8');
  assert.match(html,/Weekly off day/); assert.match(html,/cal-sunday/); assert.match(html,/cal-weekoff/); assert.match(html,/attendanceCellInfo/);
});

test('bulk attendance validates every employee before replacing supplied days',()=>{
  const a=invoke('POST','/api/salary/employees',{body:{name:'Bulk Alpha',salary:12000,weekOffDay:'Sunday'}}).body.employee;
  const b=invoke('POST','/api/salary/employees',{body:{name:'Bulk Beta',salary:12000}}).body.employee;
  const bad=invoke('POST','/api/salary/attendance/:ym/batch',{params:{ym:'2026-08'},body:{items:[{employeeName:a.name,marks:['P','A']},{employeeName:b.name,marks:['INVALID']}]}});
  assert.equal(bad.status,400);
  let month=invoke('GET','/api/salary/month/:ym',{params:{ym:'2026-08'}}).body;
  assert.equal((month.attendance[a.id]||{})['01'],undefined,'invalid batch saves nothing');
  const good=invoke('POST','/api/salary/attendance/:ym/batch',{params:{ym:'2026-08'},body:{items:[{employeeName:a.name,marks:['P','HD','']},{employeeName:b.name,marks:['A','P','P']}]}});
  assert.equal(good.status,200);assert.equal(good.body.employees,2);assert.equal(good.body.cells,6);
  month=invoke('GET','/api/salary/month/:ym',{params:{ym:'2026-08'}}).body;
  assert.equal(month.attendance[a.id]['01'],'P');assert.equal(month.attendance[a.id]['02'],'H');assert.equal(month.attendance[a.id]['03'],undefined);
  const html=fs.readFileSync(path.join(__dirname,'..','public','salary.html'),'utf8');assert.match(html,/Bulk attendance import/);assert.match(html,/Import this month atomically/);
});

test('bulk attendance skips supplied marks outside employment dates',()=>{
  const emp=invoke('POST','/api/salary/employees',{body:{name:'Bulk Joiner',salary:12000,joiningDate:'2026-08-24'}}).body.employee;
  const made=invoke('POST','/api/salary/attendance/:ym/batch',{params:{ym:'2026-08'},body:{items:[{empId:emp.id,marks:Array(27).fill('A')}]}});
  assert.equal(made.status,200);
  const month=invoke('GET','/api/salary/month/:ym',{params:{ym:'2026-08'}}).body;
  assert.equal(month.attendance[emp.id]['23'],undefined);assert.equal(month.attendance[emp.id]['24'],'A');
});

test('joining and leaving dates limit calendar-month weekly offs and paid days', () => {
  const emp=invoke('POST','/api/salary/employees',{body:{name:'Mid Month Joiner',salary:12000,weekOffDay:'Sunday',joiningDate:'2026-08-20'}}).body.employee;
  assert.equal(invoke('POST','/api/salary/attendance/:ym',{params:{ym:'2026-08'},body:{empId:emp.id,day:'16',mark:'A'}}).status,400);
  assert.equal(invoke('POST','/api/salary/attendance/:ym',{params:{ym:'2026-08'},body:{empId:emp.id,day:'23',mark:'A'}}).body.mark,'WO');
  assert.equal(invoke('POST','/api/salary/attendance/:ym',{params:{ym:'2026-08'},body:{empId:emp.id,day:'30',mark:'A'}}).body.mark,'WO');
  const month=invoke('GET','/api/salary/month/:ym',{params:{ym:'2026-08'}}).body, row=month.rows.find(x=>x.id===emp.id);
  assert.equal(row.computedPaidDays,2,'only the two eligible Sundays count');
  const invalid=invoke('POST','/api/salary/employees',{body:{name:'Invalid Dates',joiningDate:'2026-08-20',lastWorkingDate:'2026-08-19'}});
  assert.equal(invalid.status,400);
  const html=fs.readFileSync(path.join(__dirname,'..','public','salary.html'),'utf8');
  assert.match(html,/Joining date/); assert.match(html,/Last working date/); assert.match(html,/not-employed/);
});

test('payroll posting creates employee salary ledgers once and advances remain bank-backed',()=>{
  const emp=invoke('POST','/api/salary/employees',{body:{name:'Ledger Employee',salary:30000}}).body.employee;
  invoke('POST','/api/salary/row/:ym',{params:{ym:'2026-09'},body:{empId:emp.id,paidDays:30,paid:25000}});
  invoke('POST','/api/salary/advances',{body:{empId:emp.id,amount:5000,date:'2026-09-10',account:'Axis Bank 3448',proof:'/axis-advance.jpg',reference:'UTR5000'}});
  invoke('POST','/api/salary/recoveries/:ym',{params:{ym:'2026-09'},body:{empId:emp.id,amount:5000}});
  assert.equal(invoke('POST','/api/salary/post/:ym',{params:{ym:'2026-09'}}).status,200);
  assert.equal(invoke('POST','/api/salary/post/:ym',{params:{ym:'2026-09'}}).status,409);
  const ledger=invoke('GET','/api/salary/ledgers').body.ledgers.find(x=>x.empId===emp.id);
  assert.equal(ledger.ledgerName,'Ledger Employee — Salary');assert.equal(ledger.balance,0);assert.equal(ledger.status,'Settled');
  assert.ok(ledger.entries.some(x=>x.kind==='advance'&&x.reference==='UTR5000'));assert.ok(ledger.entries.some(x=>x.kind==='salary_earned'));
  const salaryHtml=fs.readFileSync(path.join(__dirname,'..','public','salary.html'),'utf8'),expensesHtml=fs.readFileSync(path.join(__dirname,'..','public','expenses.html'),'utf8');
  assert.match(salaryHtml,/accountsByNature/);assert.match(salaryHtml,/salary earned in ledgers/);assert.match(expensesHtml,/data-t="salaryledgers"/);assert.match(expensesHtml,/function renderSalaryLedgers/);
  assert.match(salaryHtml,/fd\.append\('photo',file\)/,'advance proof uses the server upload field');
  assert.doesNotMatch(salaryHtml,/fd\.append\('file',file\)/);
  assert.match(salaryHtml,/Payment proof upload failed/);
});

test('one salary batch posts multiple employees atomically from the payroll table',()=>{
  const a=invoke('POST','/api/salary/employees',{body:{name:'Batch A',salary:30000}}).body.employee,b=invoke('POST','/api/salary/employees',{body:{name:'Batch B',salary:15000}}).body.employee;
  invoke('POST','/api/salary/row/:ym',{params:{ym:'2026-10'},body:{empId:a.id,paidDays:30}});invoke('POST','/api/salary/row/:ym',{params:{ym:'2026-10'},body:{empId:b.id,paidDays:30}});
  const batch=invoke('POST','/api/salary/payments/batch',{body:{ym:'2026-10',date:'2026-10-31',account:'Axis Bank 3448',proof:'/batch.jpg',reference:'BATCH-UTR',items:[{empId:a.id,amount:30000},{empId:b.id,amount:15000}]}});
  assert.equal(batch.status,200);assert.equal(batch.body.count,2);assert.equal(batch.body.total,45000);
  const month=invoke('GET','/api/salary/month/:ym',{params:{ym:'2026-10'}}).body;assert.equal(month.rows.find(x=>x.id===a.id).transactionPaid,30000);assert.equal(month.rows.find(x=>x.id===b.id).balance,0);
  const bad=invoke('POST','/api/salary/payments/batch',{body:{ym:'2026-10',date:'2026-10-31',account:'Axis Bank 3448',proof:'/batch.jpg',items:[{empId:a.id,amount:1},{empId:b.id,amount:99999}]}});assert.equal(bad.status,400);
  const again=invoke('GET','/api/salary/month/:ym',{params:{ym:'2026-10'}}).body;assert.equal(again.rows.find(x=>x.id===a.id).transactionPaid,30000,'invalid batch posts nothing');
});

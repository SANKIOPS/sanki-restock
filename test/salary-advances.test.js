'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');

const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'sanki-salary-'));
process.env.DATA_PATH = path.join(tempDir, 'data.json');
const { router, _july2026Import, _providedAdvanceImport, _finalJuly2026Payroll, _finalAugust2026Advances, _julyImportedMarks, _findImportedEmployee, _ensureHistoricalGuard, _repairGuardSunnyCollision } = require('../modules/salary');
test.after(() => fs.rmSync(tempDir, { recursive:true, force:true }));

function invoke(method, routePath, { body={}, params={}, query={}, role='admin',username='tester' }={}) {
  const layer=router.stack.find(x=>x.route&&x.route.path===routePath&&x.route.methods[method.toLowerCase()]);
  assert.ok(layer, 'route exists: '+method+' '+routePath); let status=200,result;
  const req={body,params,query,user:{username,role,roles:[role]}};
  const res={status(n){status=n;return this;},json(v){result=v;return this;}};
  let i=0; const next=()=>{const h=layer.route.stack[i++];if(h)h.handle(req,res,next);}; next();
  return {status,body:result};
}

function postAdvance(emp,body={}){
  const request=invoke('POST','/api/salary/advances',{body:Object.assign({empId:emp.id,amount:1000,date:'2026-08-22',account:'Axis Bank 3448',recoveryStartMonth:'2026-08'},body),role:'admin'}).body.request;
  assert.ok(request&&request.id);assert.equal(invoke('POST','/api/salary/advance-requests/:id/approve',{params:{id:request.id},role:'owner'}).status,200);
  return invoke('POST','/api/salary/advance-requests/:id/post',{params:{id:request.id},body:{proofs:[body.proof||'/proof.jpg']},role:'admin'}).body.advance;
}

test('salary advances require owner approval and proof-backed posting, then recover oldest first', () => {
  const emp=invoke('POST','/api/salary/employees',{body:{name:'Employee A',salary:30000,channel:'Shared'}}).body.employee;
  const request=invoke('POST','/api/salary/advances',{body:{empId:emp.id,amount:1000,date:'2026-08-22',account:'Axis Bank 3448'}}).body.request;
  assert.equal(invoke('GET','/api/salary/advances').body.advances.filter(x=>x.empId===emp.id).length,0);
  assert.equal(invoke('POST','/api/salary/advance-requests/:id/post',{params:{id:request.id},body:{proof:'/early.jpg'}}).status,400);
  assert.equal(invoke('POST','/api/salary/advance-requests/:id/approve',{params:{id:request.id},role:'accounting'}).status,403);
  assert.equal(invoke('POST','/api/salary/advance-requests/:id/approve',{params:{id:request.id},role:'owner'}).status,200);
  const missing=invoke('POST','/api/salary/advance-requests/:id/post',{params:{id:request.id},body:{}});assert.equal(missing.status,400);assert.match(missing.body.error,/proof/i);
  assert.equal(invoke('POST','/api/salary/advance-requests/:id/post',{params:{id:request.id},body:{proof:'/proof-first.jpg'}}).status,200);
  [2000,2000].forEach((amount,i)=>{
    const made=postAdvance(emp,{amount,date:'2026-08-'+String(23+i).padStart(2,'0'),account:'Axis Bank 3448',proof:'/proof-'+i+'.jpg',recoveryStartMonth:'2026-08'});
    assert.ok(made.id);
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

test('historical advances assigned to an earlier payroll month remain editable',()=>{
  const emp=invoke('POST','/api/salary/employees',{body:{name:'Historical Recovery',salary:30000}}).body.employee;
  const made=postAdvance(emp,{amount:3000,date:'2026-12-07',account:'Axis Bank 3448',proof:'/historical.jpg',recoveryStartMonth:'2026-11'});
  assert.equal(invoke('POST','/api/salary/recoveries/:ym',{params:{ym:'2026-11'},body:{empId:emp.id,amount:3000}}).status,200);
  let row=invoke('GET','/api/salary/month/:ym',{params:{ym:'2026-11'}}).body.rows.find(x=>x.id===emp.id);
  assert.equal(row.loggedAdvanceRecovery,3000);
  assert.equal(invoke('POST','/api/salary/recoveries/:ym',{params:{ym:'2026-11'},body:{empId:emp.id,amount:1000}}).status,200);
  row=invoke('GET','/api/salary/month/:ym',{params:{ym:'2026-11'}}).body.rows.find(x=>x.id===emp.id);
  assert.equal(row.loggedAdvanceRecovery,1000);assert.equal(row.netPayable,-1000);
  const updated=invoke('GET','/api/salary/advances').body.advances.find(x=>x.id===made.id);assert.equal(updated.outstanding,2000);
});

test('advance UI merges employee history and exposes approval and proof-backed posting', () => {
  const html=fs.readFileSync(path.join(__dirname,'..','public','salary.html'),'utf8');
  assert.match(html,/data-v="advances"/); assert.match(html,/Employee advance summary · closing/); assert.match(html,/Advance approval queue/);assert.match(html,/Submit for Owner approval/);assert.match(html,/Upload proof & post/); assert.match(html,/saveRecovery/); assert.match(html,/oldest-first/);
  assert.match(html,/S\.No\./); assert.match(html,/\(index\+1\)/);
});

test('payroll always follows attendance instead of a stale paid-days override',()=>{
  const emp=invoke('POST','/api/salary/employees',{body:{name:'Attendance Source',salary:30000,monthlyPaidLeaveAllowance:0}}).body.employee;
  assert.equal(invoke('POST','/api/salary/row/:ym',{params:{ym:'2026-08'},body:{empId:emp.id,paidDays:24}}).status,200);
  const marks=Array(31).fill('');
  for(let i=0;i<26;i++)marks[i]='P';
  marks[26]='H';
  const imported=invoke('POST','/api/salary/attendance/:ym/batch',{params:{ym:'2026-08'},body:{items:[{empId:emp.id,marks}]}});
  assert.equal(imported.status,200);
  const row=invoke('GET','/api/salary/month/:ym',{params:{ym:'2026-08'}}).body.rows.find(x=>x.id===emp.id);
  assert.equal(row.computedPaidDays,25.5);
  assert.equal(row.paidDays,25.5);
  assert.equal(row.salaryAmt,25500);
  const override=invoke('POST','/api/salary/row/:ym',{params:{ym:'2026-08'},body:{empId:emp.id,paidDays:24}});
  assert.equal(override.status,409);
  assert.match(override.body.error,/Attendance tab/i);
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

test('adding employees never reuses a stale ID or overwrites an existing employee',()=>{
  const before=invoke('GET','/api/salary/employees').body.employees,protectedEmployee=before[0];
  const salaryPath=path.join(tempDir,'salary.json'),stored=JSON.parse(fs.readFileSync(salaryPath,'utf8'));stored.seq=0;fs.writeFileSync(salaryPath,JSON.stringify(stored));
  const added=invoke('POST','/api/salary/employees',{body:{name:'Unique New Employee',post:'Tester',salary:10000}});
  assert.equal(added.status,200);assert.notEqual(added.body.employee.id,protectedEmployee.id);
  const after=invoke('GET','/api/salary/employees').body.employees;assert.equal(after.length,before.length+1);assert.ok(after.some(x=>x.id===protectedEmployee.id&&x.name===protectedEmployee.name));
  const duplicate=invoke('POST','/api/salary/employees',{body:{name:'unique new employee',post:'Tester',salary:10000}});assert.equal(duplicate.status,409);assert.equal(duplicate.body.duplicate,true);
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
  assert.match(html,/MARK_NEXT\s*=\s*\{\s*'':'P',\s*'P':'H',\s*'H':'A',\s*'A':'',\s*'WO':''\s*\}/);
  assert.match(html,/Week-off is assigned automatically/);
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
  assert.equal(row.computedPaidDays,1,'two eligible Sundays are followed by the 31-day payroll adjustment');
  const invalid=invoke('POST','/api/salary/employees',{body:{name:'Invalid Dates',joiningDate:'2026-08-20',lastWorkingDate:'2026-08-19'}});
  assert.equal(invalid.status,400);
  const html=fs.readFileSync(path.join(__dirname,'..','public','salary.html'),'utf8');
  assert.match(html,/Joining date/); assert.match(html,/Last working date/); assert.match(html,/not-employed/);
});

test('zero paid-leave allowance converts legacy PL to absence and deducts salary',()=>{
  const emp=invoke('POST','/api/salary/employees',{body:{name:'Zero Leave Employee',salary:30000,monthlyPaidLeaveAllowance:0}}).body.employee,marks=Array(30).fill('P').concat('PL');
  assert.equal(invoke('POST','/api/salary/attendance/:ym/batch',{params:{ym:'2026-08'},body:{items:[{empId:emp.id,marks}]}}).status,200);
  const month=invoke('GET','/api/salary/month/:ym',{params:{ym:'2026-08'}}).body,row=month.rows.find(x=>x.id===emp.id);
  assert.equal(month.attendance[emp.id]['31'],'A');assert.equal(row.computedPaidDays,29);assert.equal(row.salaryAmt,29000);
  const html=fs.readFileSync(path.join(__dirname,'..','public','salary.html'),'utf8');assert.doesNotMatch(html,/m-PL|>PL</);assert.match(html,/when it is 0, every A or WO deducts salary/);
});

test('payroll posting creates employee salary ledgers once and advances remain bank-backed',()=>{
  const emp=invoke('POST','/api/salary/employees',{body:{name:'Ledger Employee',salary:30000}}).body.employee;
  invoke('POST','/api/salary/row/:ym',{params:{ym:'2026-09'},body:{empId:emp.id,paidDays:30,paid:25000}});
  postAdvance(emp,{amount:5000,date:'2026-09-10',account:'Axis Bank 3448',proof:'/axis-advance.jpg',reference:'UTR5000',recoveryStartMonth:'2026-09'});
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
  const batch=invoke('POST','/api/salary/payments/batch',{body:{ym:'2026-10',date:'2026-10-31',account:'Gagan Sir Cash',proof:'/batch.jpg',reference:'BATCH-UTR',items:[{empId:a.id,amount:30000},{empId:b.id,amount:15000}]}});
  assert.equal(batch.status,200);assert.equal(batch.body.count,2);assert.equal(batch.body.total,45000);
  const month=invoke('GET','/api/salary/month/:ym',{params:{ym:'2026-10'}}).body;assert.equal(month.rows.find(x=>x.id===a.id).transactionPaid,30000);assert.equal(month.rows.find(x=>x.id===b.id).balance,0);
  const bad=invoke('POST','/api/salary/payments/batch',{body:{ym:'2026-10',date:'2026-10-31',account:'Counter Cash',proof:'/batch.jpg',items:[{empId:a.id,amount:1},{empId:b.id,amount:99999}]}});assert.equal(bad.status,400);
  const again=invoke('GET','/api/salary/month/:ym',{params:{ym:'2026-10'}}).body;assert.equal(again.rows.find(x=>x.id===a.id).transactionPaid,30000,'invalid batch posts nothing');
  const html=fs.readFileSync(path.join(__dirname,'..','public','salary.html'),'utf8');assert.match(html,/Select all payable/);assert.match(html,/Clear selection/);assert.match(html,/Only checked employees will be paid/);assert.match(html,/Partially paid/);
});

test('positive and negative balances carry forward once and payroll respects employment months',()=>{
  const carryEmp=invoke('POST','/api/salary/employees',{body:{name:'Carry Forward Employee',salary:30000}}).body.employee;
  invoke('POST','/api/salary/row/:ym',{params:{ym:'2026-05'},body:{empId:carryEmp.id,paidDays:0,advance:5000}});
  invoke('POST','/api/salary/row/:ym',{params:{ym:'2026-06'},body:{empId:carryEmp.id,paidDays:30}});
  let june=invoke('GET','/api/salary/month/:ym',{params:{ym:'2026-06'}}).body.rows.find(x=>x.id===carryEmp.id);
  assert.equal(june.openingBalanceCarry,-5000);assert.equal(june.openingAdvanceCarry,5000);assert.equal(june.openingPayableCarry,0);assert.equal(june.netPayable,25000);assert.equal(june.carryForwardAdvance,0);
  const payableEmp=invoke('POST','/api/salary/employees',{body:{name:'Positive Carry Employee',salary:30000}}).body.employee;
  invoke('POST','/api/salary/row/:ym',{params:{ym:'2026-05'},body:{empId:payableEmp.id,paidDays:30,paid:29000}});
  invoke('POST','/api/salary/row/:ym',{params:{ym:'2026-06'},body:{empId:payableEmp.id,paidDays:30}});
  june=invoke('GET','/api/salary/month/:ym',{params:{ym:'2026-06'}}).body.rows.find(x=>x.id===payableEmp.id);
  assert.equal(june.openingBalanceCarry,1000);assert.equal(june.openingAdvanceCarry,0);assert.equal(june.openingPayableCarry,1000);assert.equal(june.netPayable,31000);
  const joiner=invoke('POST','/api/salary/employees',{body:{name:'August Joiner',salary:18000,joiningDate:'2026-08-15'}}).body.employee;
  const leaver=invoke('POST','/api/salary/employees',{body:{name:'July Leaver',salary:18000,lastWorkingDate:'2026-07-20'}}).body.employee;
  const leaverWithBalance=invoke('POST','/api/salary/employees',{body:{name:'January Leaver With Balance',salary:18000,lastWorkingDate:'2027-01-20'}}).body.employee;
  invoke('POST','/api/salary/row/:ym',{params:{ym:'2027-01'},body:{empId:leaverWithBalance.id,paidDays:0,advance:2400}});
  const july=invoke('GET','/api/salary/month/:ym',{params:{ym:'2026-07'}}).body.rows,august=invoke('GET','/api/salary/month/:ym',{params:{ym:'2026-08'}}).body.rows;
  assert.equal(july.some(x=>x.id===joiner.id),false);assert.equal(august.some(x=>x.id===joiner.id),true);
  assert.equal(july.some(x=>x.id===leaver.id),true);assert.equal(august.some(x=>x.id===leaver.id),false);
  const february=invoke('GET','/api/salary/month/:ym',{params:{ym:'2027-02'}}).body.rows;
  assert.equal(february.find(x=>x.id===leaverWithBalance.id).openingAdvanceCarry,2400,'a former employee remains visible until their balance is settled');
  assert.equal(invoke('POST','/api/salary/row/:ym',{params:{ym:'2026-07'},body:{empId:joiner.id,paidDays:1}}).status,400);
  const invalidAccount=invoke('POST','/api/salary/payments/batch',{body:{ym:'2026-06',date:'2026-06-30',account:'Axis Bank 3448',proof:'/proof.jpg',items:[{empId:carryEmp.id,amount:1}]}});
  assert.equal(invalidAccount.status,400);assert.match(invalidAccount.body.error,/Gagan Sir Cash|Counter Cash/);
  const html=fs.readFileSync(path.join(__dirname,'..','public','salary.html'),'utf8');assert.match(html,/Salary paying cash/);assert.match(html,/last month deduction/);assert.match(html,/last month payable/);assert.match(html,/positive balances remain payable/);
});

test('July 2026 historical attendance prepares payroll with paid-off and 31-day rules',()=>{
  assert.equal(_findImportedEmployee({employees:{x:{id:'x',name:'Arshpreet Singh Arora',post:'Manager'}}},'ARSHPREET SINGH','MANAGER').id,'x','longer employee-master name is matched safely by post');
  assert.equal(_findImportedEmployee({employees:{x:{id:'x',name:'Nandini',post:'Sales Executive'}}},'NANDANI','SALES EXECUTIVE').id,'x','Nandini production spelling matches the supplied NANDANI row');
  assert.equal(_findImportedEmployee({employees:{x:{id:'x',name:'Pradeep',post:'Executive'}}},'PARDEEP','EXECUTIVE').id,'x','Pradeep production spelling matches the supplied PARDEEP row');
  const missingGuard={employees:{},seq:0},guardMonth={rows:{}};const restoredGuard=_ensureHistoricalGuard(missingGuard,guardMonth);assert.equal(restoredGuard.name,'Guard');assert.equal(restoredGuard.salary,15000);assert.equal(guardMonth.rows[restoredGuard.id].paidDays,30);
  const collision={seq:22,employees:{E022:{id:'E022',name:'Guard',post:'Security',salary:15000}},advances:{a:{empId:'E022',employeeName:'SUNNY SHARMA',historicalImport:true}}};assert.equal(_repairGuardSunnyCollision(collision),true);assert.equal(collision.employees.E022.name,'SUNNY SHARMA');const safeGuard=_ensureHistoricalGuard(collision,{rows:{}});assert.equal(safeGuard.id,'E023');assert.equal(collision.employees.E022.name,'SUNNY SHARMA');
  assert.ok(_july2026Import.every(x=>x[2].length===31),'every supplied employee has exactly 31 source cells');
  const month=invoke('GET','/api/salary/month/:ym',{params:{ym:'2026-07'}}).body;
  const expected=new Map(_july2026Import.map(x=>{const emp={monthlyPaidLeaveAllowance:String(x[0]).toLowerCase()==='suraj'?1:4,joiningDate:x[4]||''};return[x[0].toLowerCase()+'|'+x[1].toLowerCase(),{paidDays:_julyImportedMarks(emp,x[2]).paidDays}];}));
  const imported=month.rows.filter(r=>expected.has(String(r.name).replace(/\s*\([^)]*\)\s*/g,'').trim().toLowerCase()+'|'+String(r.post).toLowerCase()));
  assert.equal(imported.length,19);
  const finalRows=new Map(_finalJuly2026Payroll.map(x=>[String(x[0]).replace(/\s*\([^)]*\)\s*/g,'').trim().toLowerCase()+'|'+String(x[1]).toLowerCase(),{paidDays:x[3],paid:x[4]}]));
  imported.forEach(r=>{const key=String(r.name).replace(/\s*\([^)]*\)\s*/g,'').trim().toLowerCase()+'|'+String(r.post).toLowerCase(),x=finalRows.get(key);assert.ok(x,r.name+' has a final July row');assert.equal(r.paidDays,x.paidDays,r.name);assert.equal(r.paid,x.paid,r.name+' historical payment');});
  assert.equal(_providedAdvanceImport.reduce((n,x)=>n+x[3],0),125067);
  assert.equal(month.totals.advance,125067,'every supplied July and August advance is recovered in July payroll');
  const allHistorical=invoke('GET','/api/salary/advances').body.advances.filter(x=>x.historicalImport),history=allHistorical.filter(x=>String(x.sourceKey).startsWith('provided-advance-sheet-')),augustHistory=allHistorical.filter(x=>String(x.sourceKey).startsWith('final-august-advance-sheet-'));
  assert.equal(history.length,22);assert.equal(history.reduce((n,x)=>n+x.amount,0),125067);assert.ok(history.every(x=>x.status==='Recovered'&&!x.account&&!x.proof));
  assert.equal(augustHistory.length,7);assert.equal(augustHistory.reduce((n,x)=>n+x.amount,0),12000);assert.ok(augustHistory.every(x=>x.status==='Recovered'&&x.recoveryStartMonth==='2026-08'));
  assert.equal(_finalAugust2026Advances.reduce((n,x)=>n+x[3],0),12000);
  assert.equal(month.totals.paid,291000);assert.equal(Math.round(imported.reduce((n,r)=>n+r.balance,0)*100)/100,-36658.65);
  assert.equal(month.rows.find(r=>r.name==='Pooja').paidDays,10);
  assert.equal(month.rows.find(r=>r.name==='Ravi').paidDays,9);
  const sunny=month.rows.find(r=>r.name==='SUNNY SHARMA'),guard=month.rows.find(r=>r.name==='Guard'),suraj=month.rows.find(r=>/^Suraj/i.test(r.name));
  assert.equal(sunny.salary,24000);assert.equal(sunny.paidDays,22);assert.equal(sunny.advance,20000);assert.equal(sunny.netPayable,-2400);
  assert.equal(guard.advance,3000);assert.equal(guard.netPayable,12000);
  assert.equal(suraj.paidDays,25);assert.equal(suraj.advance,1500);assert.equal(suraj.netPayable,13500);
  const employeeMaster=invoke('GET','/api/salary/employees').body.employees;assert.equal(employeeMaster.find(e=>e.id===suraj.id).monthlyPaidLeaveAllowance,1);assert.equal(employeeMaster.find(e=>e.id===sunny.id).monthlyPaidLeaveAllowance,4);
  const arshpreet=month.rows.find(r=>/^Arshpreet/i.test(r.name)),ravi=month.rows.find(r=>r.name==='Ravi');
  const august=invoke('GET','/api/salary/month/:ym',{params:{ym:'2026-08'}}).body.rows,finalNames=new Set(_finalJuly2026Payroll.map(x=>String(x[0]).replace(/\s*\([^)]*\)\s*/g,'').trim().toLowerCase()+'|'+String(x[1]).toLowerCase())),finalAugust=august.filter(r=>finalNames.has(String(r.name).replace(/\s*\([^)]*\)\s*/g,'').trim().toLowerCase()+'|'+String(r.post).toLowerCase()));
  assert.equal(august.find(r=>r.id===arshpreet.id).advance,29500);assert.equal(august.find(r=>r.id===ravi.id).advance,10200);assert.equal(august.find(r=>r.id===suraj.id).advance,1500);
  assert.equal(august.find(r=>r.name==='PIYUSH').openingPayableCarry,266.67);
  assert.equal(Math.round(finalAugust.reduce((n,r)=>n+r.netPayable,0)*100)/100,-48658.65,'signed July balances and ₹12,000 August advances combine once');
  const sundayOff=_julyImportedMarks({weekOffDay:'Sunday'},'A'.repeat(31));
  assert.equal(sundayOff.attendance['05'],'WO','an absent weekly-off date stays visibly marked WO');
  assert.equal(sundayOff.attendance['01'],'A','ordinary absence remains visibly marked A');
  const twoLeaves=_julyImportedMarks({},'P'.repeat(29)+'AA'),fiveLeaves=_julyImportedMarks({},'P'.repeat(26)+'AAAAA');
  assert.equal(twoLeaves.paidDays,32,'two unused allowance days become extra paid days after the July adjustment');
  assert.equal(fiveLeaves.paidDays,29,'a fifth leave reduces the normal 30 paid days by one');
});

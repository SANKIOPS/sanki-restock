'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const XLSX = require('xlsx');

const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'sanki-salary-'));
process.env.DATA_PATH = path.join(tempDir, 'data.json');
const { router, _july2026Import, _providedAdvanceImport, _finalJuly2026Payroll, _finalAugust2026Advances, _julyImportedMarks, _findImportedEmployee, _ensureHistoricalGuard, _repairGuardSunnyCollision, _removeHistoricalAdvancesV16, _salarySheetChanges, _applySalarySheetChanges, _advanceSheetRows, _applyAdvanceSheetRows, _advanceSourceSheet, _finalAugustPlan, _applyFinalAugustPlan, _repairFinalAugustImportedRoster, _linkFinalAugustSourceSheetRows } = require('../modules/salary');
test.after(() => fs.rmSync(tempDir, { recursive:true, force:true }));

function invoke(method, routePath, { body={}, params={}, query={}, role='admin',username='tester' }={}) {
  const layer=router.stack.find(x=>x.route&&x.route.path===routePath&&x.route.methods[method.toLowerCase()]);
  assert.ok(layer, 'route exists: '+method+' '+routePath); let status=200,result;
  const req={body,params,query,method:method.toUpperCase(),path:routePath,route:{path:routePath},user:{username,role,roles:[role]}};
  const res={status(n){status=n;return this;},json(v){result=v;return this;}};
  let i=0; const next=()=>{const h=layer.route.stack[i++];if(h)h.handle(req,res,next);}; next();
  return {status,body:result};
}

function postAdvance(emp,body={}){
  const request=invoke('POST','/api/salary/advances',{body:Object.assign({empId:emp.id,amount:1000,date:'2026-08-22',account:'Axis Bank 3448',recoveryStartMonth:'2026-08'},body),role:'admin'}).body.request;
  assert.ok(request&&request.id);assert.equal(invoke('POST','/api/salary/advance-requests/:id/approve',{params:{id:request.id},role:'owner'}).status,200);
  return invoke('POST','/api/salary/advance-requests/:id/post',{params:{id:request.id},body:{payoutDate:body.payoutDate||body.date||'2026-08-22',proofs:[body.proof||'/proof.jpg']},role:'admin'}).body.advance;
}

test('salary Excel preview updates existing employees and audits attendance-derived paid-day overrides',()=>{
  const s={employees:{E001:{id:'E001',name:'Excel Employee',salary:30000,active:true,joiningDate:'2099-01-01'}},months:{'2099-05':{rows:{E001:{paidDays:30}},attendance:{}}},divisor:30,salaryPayments:[],advances:{}};
  const workbook=XLSX.utils.book_new();XLSX.utils.book_append_sheet(workbook,XLSX.utils.json_to_sheet([{'Employee ID':'E001','Employee Name':'Excel Employee','Paid Days':30,'Final Salary Amount':28500,'Remarks':'Verified sheet'}]),'Salary');
  const file={originalname:'salary.xlsx',buffer:XLSX.write(workbook,{type:'buffer',bookType:'xlsx'})};
  const preview=_salarySheetChanges(s,'2099-05',file);assert.equal(preview.changes.length,1);assert.equal(preview.changes[0].finalAmount,28500);assert.equal(preview.changes[0].previousFinalAmount,30000);assert.match(preview.hash,/^[a-f0-9]{64}$/);
  assert.equal(s.months['2099-05'].rows.E001.finalSalaryAmount,undefined,'preview does not mutate payroll');
  _applySalarySheetChanges(s,'2099-05',preview,'Approved revised salary sheet','prashant','salary.xlsx');
  assert.equal(s.months['2099-05'].rows.E001.finalSalaryAmount,28500);assert.equal(s.finalSalaryAudit[0].by,'prashant');assert.equal(s.salarySheetAudit[0].fileHash,preview.hash);assert.equal(s.salaryPayments.length,0);
  s.months['2099-05'].attendance.E001={'01':'P'};
  const locked=XLSX.utils.book_new();XLSX.utils.book_append_sheet(locked,XLSX.utils.json_to_sheet([{'Employee ID':'E001','Paid Days':29}]),'Salary');
  const dayChange=_salarySheetChanges(s,'2099-05',{originalname:'salary.xlsx',buffer:XLSX.write(locked,{type:'buffer',bookType:'xlsx'})});
  assert.equal(dayChange.changes[0].attendanceDerived,true);assert.equal(dayChange.changes[0].status,'Update existing');
  _applySalarySheetChanges(s,'2099-05',dayChange,'Revised paid-day count','prashant','salary.xlsx');
  assert.equal(s.months['2099-05'].rows.E001.sheetPaidDaysOverride,29);assert.equal(s.months['2099-05'].attendance.E001['01'],'P');
});

test('final August workbook reconciles historical advances without a second salary or bank payment',()=>{
  const amounts=[27000,4900,2700,100,100,67,333,500,500,500,6000,2500,1000,1000,650,5000,17000,10000,1000,1000];
  assert.equal(amounts.reduce((n,x)=>n+x,0),81850);
  const book=XLSX.utils.book_new(),salary=[['S.NO.','NAME','POST','SALARY','PAID DAYS','MONTH DAY','SALARY AMT','ADVANCE SALARY','BILLING AMT','NET PAYABLE',null,null,null,'PAIDSALARY',null,'Paid in Cash','Paid by 3645']],attendance=[['NAME','POST',...Array.from({length:31},(_,i)=>new Date(Date.UTC(2026,7,i+1)))]],advances=[['Date','Particular','Amount','Account']];
  const state={employees:{},months:{'2026-08':{rows:{},attendance:{}}},divisor:30,salaryPayments:[],advances:{},advanceSeq:0,oneTimeMigrations:{}};
  for(let i=0;i<23;i++){
    const id='E'+i,name='Employee '+i,monthlySalary=200000,advance=amounts[i]||0,cash=i===0?171000:0,bank=i===1?102703:0;
    state.employees[id]={id,name,salary:monthlySalary,active:true};salary.push([i+1,name,'Staff',monthlySalary,30,30,monthlySalary,advance,0,monthlySalary-advance,null,null,null,cash+bank,null,cash,bank]);attendance.push([name,'Staff',...Array(31).fill('P')]);
    if(i<20)advances.push([new Date(Date.UTC(2026,7,12+i%20)),name,advance,'']);
    if(cash||bank)state.salaryPayments.push({empId:id,ym:'2026-08',amount:cash?cash:bank-.33,account:cash?'Gagan Sir Cash':'Prashant Axis 3645',proof:'/proof.jpg',active:true});
  }
  state.employees.EXTRA={id:'EXTRA',name:'Not on August sheet',salary:5000,active:true};
  state.salaryPayments.push({empId:'E2',ym:'2026-08',date:'2026-09-16',amount:650,account:'Prashant Axis 3645',proof:'/later-fraction.jpg',active:true});
  for(const [name,rows] of [['SALARY(Up.)',salary],['Attendance(Up.)',attendance],['ADV',advances]])XLSX.utils.book_append_sheet(book,XLSX.utils.aoa_to_sheet(rows),name);
  const file={originalname:'final.xlsb',buffer:XLSX.write(book,{type:'buffer',bookType:'xlsx'})},plan=_finalAugustPlan(state,file),paymentCount=state.salaryPayments.length;
  assert.equal(plan.salaryRows.length,23);assert.equal(plan.advances.length,20);assert.equal(plan.extras.length,1);
  assert.equal(plan.totals.bank,102703);assert.equal(plan.totals.supplemental,650);assert.equal(plan.totals.actualPaid,274352.67);
  state.advances.CONFLICT={id:'CONFLICT',empId:'E0',employeeName:'Employee 0',date:'2026-08-31',amount:1000,proof:'/real-payout.jpg',recoveries:[],active:true};
  const conflicted=_finalAugustPlan(state,file);assert.equal(conflicted.unmatchedAdvances.length,1);
  assert.throws(()=>_applyFinalAugustPlan(state,conflicted,'owner',file.originalname),/Existing advances disagree/);
  assert.equal(state.advances.CONFLICT.proof,'/real-payout.jpg');delete state.advances.CONFLICT;
  const result=_applyFinalAugustPlan(state,plan,'owner',file.originalname);
  assert.equal(result.rows.length,23);assert.equal(result.totals.actualPaid,274352.67);assert.equal(state.salaryPayments.length,paymentCount);assert.equal(Object.keys(state.advances).length,20);
  assert.ok(Object.values(state.advances).every(a=>a.historicalOpening&&a.proof===''&&a.recoveries[0].ym==='2026-08'));
  assert.equal(result.rows.reduce((n,r)=>n+r.loggedAdvanceRecovery,0),81850);
  state.employees.LATE={id:'LATE',name:'Historical carry not on sheet',salary:5000,active:true};
  state.months['2026-08'].rows.LATE={paidDays:30};
  assert.equal(_repairFinalAugustImportedRoster(state),true);
  assert.equal(state.months['2026-08'].rows.LATE.sheetExcluded,true);
  assert.equal(state.oneTimeMigrations.final_august_sheet_roster_carry_repair_v1.excluded[0].name,'Historical carry not on sheet');
  state.advanceSourceSheets=[{hash:'source-sheet',items:plan.advances.map((a,i)=>({row:i+2,employeeName:a.name,requestDate:a.date,amount:a.amount}))}];
  assert.equal(_linkFinalAugustSourceSheetRows(state),true);
  assert.equal(state.oneTimeMigrations.final_august_sheet_source_links_v1.linked.length,20);
  assert.ok(state.advanceSourceSheets[0].items.every(x=>state.advances[x.linkedAdvanceId]));
  assert.equal(_linkFinalAugustSourceSheetRows(state),false,'migration is idempotent');
  state.salaryPayments[0].amount++;assert.throws(()=>_finalAugustPlan(state,file),/paid amounts differ/);
});

test('advances Excel preview imports requests but never posts payments',()=>{
  const s={employees:{E001:{id:'E001',name:'Excel Employee'}},advanceRequests:{},advanceRequestSeq:0,advances:{}};
  const book=XLSX.utils.book_new();XLSX.utils.book_append_sheet(book,XLSX.utils.json_to_sheet([{'Employee ID':'E001','Advance Amount':5000,'Request Date':'2026-09-16','Paying Account':'Prashant Axis 3645','Recovery Start Month':'2026-10','Note':'Employee request'}]),'Advances');
  const file={originalname:'advances.xlsx',buffer:XLSX.write(book,{type:'buffer',bookType:'xlsx'})};
  const preview=_advanceSheetRows(s,file);assert.equal(preview.changes.length,1);assert.equal(preview.changes[0].amount,5000);assert.deepEqual(s.advanceRequests,{});
  const applied=_applyAdvanceSheetRows(s,preview,'Import test',{user:{username:'prashant'}},file.originalname);
  assert.equal(applied.ids.length,1);assert.equal(s.advanceRequests[applied.ids[0]].status,'Pending approval');assert.equal(s.advanceRequests[applied.ids[0]].account,'Prashant Axis 3645');assert.deepEqual(s.advances,{});
  const unchanged=_advanceSheetRows(s,file);assert.equal(unchanged.changes[0].status,'Unchanged');
  const changedBook=XLSX.utils.book_new();XLSX.utils.book_append_sheet(changedBook,XLSX.utils.json_to_sheet([{'Employee ID':'E001','Advance Amount':6000,'Request Date':'2026-09-16','Paying Account':'Prashant Axis 3645','Recovery Start Month':'2026-10','Note':'Employee request'}]),'Advances');
  const changed=_advanceSheetRows(s,{originalname:'revised.xlsx',buffer:XLSX.write(changedBook,{type:'buffer',bookType:'xlsx'})});assert.equal(changed.changes[0].status,'Update existing request');
  const update=_applyAdvanceSheetRows(s,changed,'Revised amount',{user:{username:'prashant'}},'revised.xlsx');assert.equal(update.counts.updated,1);assert.equal(Object.keys(s.advanceRequests).length,1);assert.equal(s.advanceRequests[applied.ids[0]].amount,6000);
  s.advances.ADV001={id:'ADV001',empId:'E001',employeeName:'Excel Employee',amount:7000,date:'2026-09-20',account:'Prashant Axis 3645',recoveryStartMonth:'2026-09',note:'',reference:'',active:true};
  const postedBook=XLSX.utils.book_new();XLSX.utils.book_append_sheet(postedBook,XLSX.utils.json_to_sheet([{'Advance ID':'ADV001','Employee ID':'E001','Advance Amount':8000,'Request Date':'2026-09-20','Paying Account':'Prashant Axis 3645','Recovery Start Month':'2026-09'}]),'Advances');
  const posted=_advanceSheetRows(s,{originalname:'posted.xlsx',buffer:XLSX.write(postedBook,{type:'buffer',bookType:'xlsx'})});assert.equal(posted.changes[0].status,'Needs individual correction');assert.throws(()=>_applyAdvanceSheetRows(s,posted,'No silent payment edit',{user:{username:'prashant'}},'posted.xlsx'),/individual/);assert.equal(s.advances.ADV001.amount,7000);
});

test('formatted Excel dates and named recovery months are read without creating carried-forward payments',()=>{
  const s={employees:{E001:{id:'E001',name:'Excel Employee'}},advanceRequests:{},advances:{}};
  const sheet=XLSX.utils.json_to_sheet([{'Employee Name':'Excel Employee','Advance Amount':27000,'Request Date':new Date(Date.UTC(2026,7,1)),'Paying Account':'N/A','Recovery Start Month':'August','Note':'Last Month Advance carried forward'}]);
  sheet.C2.z='yyyy/mm/dd';const book=XLSX.utils.book_new();XLSX.utils.book_append_sheet(book,sheet,'Advances');const file={originalname:'advances.xlsx',buffer:XLSX.write(book,{type:'buffer',bookType:'xlsx'})};
  const preview=_advanceSheetRows(s,file);assert.equal(preview.changes[0].date,'2026-08-01');assert.equal(preview.changes[0].recoveryStartMonth,'2026-08');assert.equal(preview.changes[0].status,'Source row — review only');assert.throws(()=>_applyAdvanceSheetRows(s,preview,'Import',{user:{username:'owner'}},file.originalname),/Save the exact sheet/);
  const source=_advanceSourceSheet(file);assert.equal(source.items[0].amount,27000);assert.equal(source.items[0].note,'Last Month Advance carried forward');assert.deepEqual(s.advances,{});
});

test('uploaded advance rows can be edited, linked, or proof-posted without duplicate payments',()=>{
  const employee=invoke('POST','/api/salary/employees',{body:{name:'Sheet Editor',salary:20000,channel:'Shared'}}).body.employee;
  const salPath=path.join(tempDir,'salary.json'),state=JSON.parse(fs.readFileSync(salPath,'utf8'));
  state.advanceSourceSheets=[{hash:'sheet-test-hash',fileName:'advances.xlsx',items:[
    {row:2,advanceId:'',employeeId:employee.id,employeeName:employee.name,amount:1200,requestDate:'2026-09-16',payingAccount:'3645',recoveryStartMonth:'2026-10',note:'New advance',reference:'sheet-ref'},
    {row:3,advanceId:'',employeeId:employee.id,employeeName:employee.name,amount:900,requestDate:'2026-08-01',payingAccount:'N/A',recoveryStartMonth:'August',note:'Last Month Advance carried forward',reference:'',reviewedAt:'2026-09-16T00:00:00.000Z'}
  ],total:2100}];fs.writeFileSync(salPath,JSON.stringify(state));
  assert.equal(invoke('POST','/api/salary/advances/source-sheet/rows/:row/post',{params:{row:'2'},body:{hash:'sheet-test-hash',account:'Prashant Axis 3645',proofs:['/proof.jpg']},role:'owner'}).status,409,'unreviewed sheet row cannot be posted');
  const edited=invoke('PATCH','/api/salary/advances/source-sheet/rows/:row',{params:{row:'2'},body:{hash:'sheet-test-hash',amount:1250,note:'Corrected from worksheet'},role:'owner'});
  assert.equal(edited.status,200);assert.equal(edited.body.item.amount,1250);assert.ok(edited.body.item.reviewedAt);
  assert.equal(invoke('POST','/api/salary/advances/source-sheet/rows/:row/post',{params:{row:'3'},body:{hash:'sheet-test-hash',account:'Prashant Axis 3645',proofs:['/proof.jpg']},role:'owner'}).status,400,'carry-forward cannot create a bank debit');
  assert.equal(invoke('POST','/api/salary/advances/source-sheet/rows/:row/post',{params:{row:'2'},body:{hash:'sheet-test-hash',account:'Prashant Axis 3645'},role:'owner'}).status,400,'proof required');
  const posted=invoke('POST','/api/salary/advances/source-sheet/rows/:row/post',{params:{row:'2'},body:{hash:'sheet-test-hash',account:'Prashant Axis 3645',proofs:['/proof.jpg']},role:'owner'});
  assert.equal(posted.status,200);assert.equal(posted.body.advance.amount,1250);
  assert.equal(invoke('POST','/api/salary/advances/source-sheet/rows/:row/post',{params:{row:'2'},body:{hash:'sheet-test-hash',account:'Prashant Axis 3645',proofs:['/proof.jpg']},role:'owner'}).status,409,'cannot repost linked row');
  const view=invoke('GET','/api/salary/advances',{role:'owner'}).body;
  assert.equal(view.sourceSheet.items[0].linkedAdvanceId,posted.body.advance.id);assert.equal(view.sourceSheet.total,2150);
  assert.equal(view.summary.find(x=>x.empId===employee.id).outstanding,1250,'posted row is counted in employee advances');
  assert.equal(invoke('PATCH','/api/salary/advances/source-sheet/rows/:row',{params:{row:'2'},body:{hash:'sheet-test-hash',amount:1300},role:'owner'}).status,409,'posted amount cannot silently change');
  assert.equal(view.advances.filter(a=>a.id===posted.body.advance.id).length,1);
  const html=fs.readFileSync(path.join(__dirname,'..','public','salary.html'),'utf8');assert.match(html,/src_amount_.*type="number"/);assert.match(html,/src_date_.*type="date"/);assert.match(html,/src_account_/);assert.match(html,/Save.*Post/);
});

test('salary advances require owner approval and proof-backed posting, then recover oldest first', () => {
  const emp=invoke('POST','/api/salary/employees',{body:{name:'Employee A',salary:30000,channel:'Shared'}}).body.employee;
  const request=invoke('POST','/api/salary/advances',{body:{empId:emp.id,amount:1000,date:'2026-08-22',account:'Axis Bank 3448'}}).body.request;
  let beforePosting=invoke('GET','/api/salary/advances').body;
  assert.equal(beforePosting.advances.filter(x=>x.empId===emp.id).length,0);
  assert.deepEqual(beforePosting.summary.find(x=>x.empId===emp.id).requests.map(x=>x.id),[request.id]);
  assert.equal(invoke('POST','/api/salary/advance-requests/:id/post',{params:{id:request.id},body:{proof:'/early.jpg'}}).status,400);
  assert.equal(invoke('POST','/api/salary/advance-requests/:id/approve',{params:{id:request.id},role:'accounting'}).status,403);
  assert.equal(invoke('POST','/api/salary/advance-requests/:id/approve',{params:{id:request.id},role:'owner'}).status,200);
  const missing=invoke('POST','/api/salary/advance-requests/:id/post',{params:{id:request.id},body:{}});assert.equal(missing.status,400);assert.match(missing.body.error,/proof/i);
  assert.equal(invoke('POST','/api/salary/advance-requests/:id/post',{params:{id:request.id},body:{payoutDate:'2026-08-22',proof:'/proof-first.jpg'}}).status,200);
  const afterPosting=invoke('GET','/api/salary/advances').body.summary.find(x=>x.empId===emp.id);
  assert.equal(afterPosting.requests.some(x=>x.id===request.id),false);
  assert.equal(afterPosting.transactions.some(x=>x.requestId===request.id),true);
  [2000,2000].forEach((amount,i)=>{
    const made=postAdvance(emp,{amount,date:'2026-08-'+String(23+i).padStart(2,'0'),account:'Axis Bank 3448',proof:'/proof-'+i+'.jpg',recoveryStartMonth:'2026-08'});
    assert.ok(made.id);
  });
  let list=invoke('GET','/api/salary/advances',{query:{summaryMonth:'2026-08'}}).body;
  const summary=list.summary.find(x=>x.empId===emp.id); assert.equal(summary.total,5000); assert.equal(summary.thisMonth,5000); assert.equal(summary.outstanding,5000);
  const recovered=invoke('POST','/api/salary/recoveries/:ym',{params:{ym:'2026-08'},body:{empId:emp.id,amount:2500,reason:'Deduct half this cycle'},role:'owner'});
  assert.equal(recovered.status,200);
  list=invoke('GET','/api/salary/advances').body; const own=list.advances.filter(x=>x.empId===emp.id).sort((a,b)=>a.date.localeCompare(b.date));
  assert.equal(own[0].status,'Recovered'); assert.equal(own[1].recovered,1500); assert.equal(own[1].status,'Partially recovered'); assert.equal(list.summary.find(x=>x.empId===emp.id).outstanding,2500);
  assert.equal(own[0].recoveries[0].payrollMonth,'2026-08');assert.equal(own[0].recoveries[0].deductedFrom,'2026-08 salary');assert.equal(own[0].recoveries[0].deductionDate,'');assert.equal(own[0].recoveries[0].reference,'SALARY-RECOVERY-2026-08');assert.equal(own[0].recoveries[0].remainingAfter,0);assert.match(own[0].recoveries[0].recordedOn,/^\d{4}-\d{2}-\d{2}$/);
  const payroll=invoke('GET','/api/salary/month/:ym',{params:{ym:'2026-08'}}).body.rows.find(x=>x.id===emp.id);
  assert.equal(payroll.loggedAdvanceRecovery,2500); assert.equal(payroll.outstandingAdvance,2500);
  const tooMuch=invoke('POST','/api/salary/recoveries/:ym',{params:{ym:'2026-08'},body:{empId:emp.id,amount:6000},role:'owner'}); assert.equal(tooMuch.status,400);
});

test('advance recovery starts from its actual payout month, not a proposed recovery month',()=>{
  const emp=invoke('POST','/api/salary/employees',{body:{name:'Historical Recovery',salary:30000}}).body.employee;
  const made=postAdvance(emp,{amount:3000,date:'2026-12-07',account:'Axis Bank 3448',proof:'/historical.jpg',recoveryStartMonth:'2026-11'});
  assert.equal(made.date,'2026-12-07');assert.equal(made.recoveryStartMonth,'2026-12');
  assert.equal(invoke('POST','/api/salary/recoveries/:ym',{params:{ym:'2026-11'},body:{empId:emp.id,amount:3000},role:'owner'}).status,400);
  assert.equal(invoke('POST','/api/salary/recoveries/:ym',{params:{ym:'2026-12'},body:{empId:emp.id,amount:1000,reason:'Partial recovery'},role:'owner'}).status,200);
  const row=invoke('GET','/api/salary/month/:ym',{params:{ym:'2026-12'}}).body.rows.find(x=>x.id===emp.id);
  assert.equal(row.loggedAdvanceRecovery,1000);
  const updated=invoke('GET','/api/salary/advances').body.advances.find(x=>x.id===made.id);assert.equal(updated.outstanding,2000);
});

test('owner can post a proof-backed employee advance immediately without approval',()=>{
  const emp=invoke('POST','/api/salary/employees',{body:{name:'Owner Direct Advance',salary:24000}}).body.employee;
  const missingProof=invoke('POST','/api/salary/advances',{role:'owner',username:'owner',body:{empId:emp.id,amount:2500,date:'2026-09-13',account:'Axis Bank 3448',recoveryStartMonth:'2026-09'}});
  assert.equal(missingProof.status,400);assert.match(missingProof.body.error,/proof/i);
  const posted=invoke('POST','/api/salary/advances',{role:'owner',username:'owner',body:{empId:emp.id,amount:2500,date:'2026-09-13',account:'Axis Bank 3448',recoveryStartMonth:'2026-09',note:'Emergency advance',proofs:['/owner-proof.jpg']}});
  assert.equal(posted.status,200);assert.equal(posted.body.directPost,true);assert.equal(posted.body.request,undefined);assert.equal(posted.body.advance.outstanding,2500);assert.equal(posted.body.advance.directOwnerPost,true);
  const list=invoke('GET','/api/salary/advances',{role:'owner'}).body;
  assert.equal(list.permissions.canDirectPost,true);assert.equal(list.summary.find(x=>x.empId===emp.id).outstanding,2500);
});

test('advance UI merges employee history and exposes approval and proof-backed posting', () => {
  const html=fs.readFileSync(path.join(__dirname,'..','public','salary.html'),'utf8');
  assert.match(html,/data-v="advances"/); assert.match(html,/Employee advance register · closing/); assert.doesNotMatch(html,/Advance approval queue/);assert.match(html,/Requests, approvals, posted advances/);assert.match(html,/Submit for Owner approval/);assert.match(html,/Upload proof & post/); assert.match(html,/saveRecovery/); assert.match(html,/oldest-first/);assert.match(html,/Company owes/);assert.match(html,/editAdvance/);
  assert.match(html,/S\.No\./); assert.match(html,/\(index\+1\)/);assert.match(html,/Connected advance history/);assert.match(html,/Advance given/);assert.match(html,/Amount deducted/);assert.match(html,/Date not recorded/);assert.match(html,/deductionDate/);assert.match(html,/salaryPaymentReference/);assert.match(html,/Fully deducted/);assert.match(html,/remainingAfter/);
  assert.match(html,/0992\|0993\|7883/);assert.match(html,/personalOwnerAccounts/);
});

test('salary UI and storage keep SANKI and Samast payrolls independent',()=>{
  const html=fs.readFileSync(path.join(__dirname,'..','public','salary.html'),'utf8'),source=fs.readFileSync(path.join(__dirname,'..','modules','salary.js'),'utf8');
  assert.match(html,/SANKI Salary/);assert.match(html,/Samast Salary/);assert.match(html,/state\.entity/);assert.match(html,/entity='\+encodeURIComponent\(state\.entity\)/);
  assert.match(source,/salary-samast\.json/);assert.match(source,/AsyncLocalStorage/);assert.match(source,/entity==='SANKI'&&applyJuly2026AttendanceAndPayroll/);
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

test('salary increments are effective-dated and preserve earlier payroll salary',()=>{
  const emp=invoke('POST','/api/salary/employees',{body:{name:'Increment History Employee',salary:30000}}).body.employee;
  invoke('POST','/api/salary/row/:ym',{params:{ym:'2026-09'},body:{empId:emp.id,paidDays:30}});
  invoke('POST','/api/salary/row/:ym',{params:{ym:'2026-10'},body:{empId:emp.id,paidDays:30}});
  const added=invoke('POST','/api/salary/increments',{body:{empId:emp.id,updatedSalary:36000,effectiveMonth:'2026-10'}});
  assert.equal(added.status,200);assert.equal(added.body.increment.previousSalary,30000);assert.equal(added.body.increment.increase,6000);
  assert.equal(invoke('GET','/api/salary/month/:ym',{params:{ym:'2026-09'}}).body.rows.find(x=>x.id===emp.id).salary,30000);
  const october=invoke('GET','/api/salary/month/:ym',{params:{ym:'2026-10'}}).body.rows.find(x=>x.id===emp.id);assert.equal(october.salary,36000);assert.equal(october.salaryAmt,36000);
  assert.equal(invoke('GET','/api/salary/month/:ym',{params:{ym:'2026-11'}}).body.rows.find(x=>x.id===emp.id).salary,36000);
  const listed=invoke('GET','/api/salary/employees').body.employees.find(x=>x.id===emp.id);assert.equal(listed.salaryHistory.length,1);assert.equal(listed.salaryHistory[0].effectiveMonth,'2026-10');
  assert.equal(invoke('POST','/api/salary/increments',{body:{empId:emp.id,updatedSalary:37000,effectiveMonth:'2026-10'}}).status,409);
  assert.equal(invoke('POST','/api/salary/employees',{body:{id:emp.id,salary:40000}}).status,400);
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

test('joining and leaving dates limit attendance and earned paid-leave days', () => {
  const emp=invoke('POST','/api/salary/employees',{body:{name:'Mid Month Joiner',salary:12000,weekOffDay:'Sunday',joiningDate:'2026-08-20'}}).body.employee;
  assert.equal(invoke('POST','/api/salary/attendance/:ym',{params:{ym:'2026-08'},body:{empId:emp.id,day:'16',mark:'A'}}).status,400);
  assert.equal(invoke('POST','/api/salary/attendance/:ym',{params:{ym:'2026-08'},body:{empId:emp.id,day:'23',mark:'A'}}).body.mark,'WO');
  assert.equal(invoke('POST','/api/salary/attendance/:ym',{params:{ym:'2026-08'},body:{empId:emp.id,day:'30',mark:'A'}}).body.mark,'WO');
  const month=invoke('GET','/api/salary/month/:ym',{params:{ym:'2026-08'}}).body, row=month.rows.find(x=>x.id===emp.id);
  assert.equal(row.computedPaidDays,0,'paid leave is not earned before six full present days');
  const invalid=invoke('POST','/api/salary/employees',{body:{name:'Invalid Dates',joiningDate:'2026-08-20',lastWorkingDate:'2026-08-19'}});
  assert.equal(invalid.status,400);
  const html=fs.readFileSync(path.join(__dirname,'..','public','salary.html'),'utf8');
  assert.match(html,/Joining date/); assert.match(html,/Last working date/); assert.match(html,/not-employed/);
});

test('paid-leave allowance follows the final salary-sheet attendance bands',()=>{
  const expected=new Map([[5,4],[6,6],[8,8],[9,9.5],[11,11.5],[12,13],[14,15],[15,16.5],[17,18.5],[18,20],[20,22],[21,23.5],[23,25.5],[24,27],[30,33]]);
  expected.forEach((paidDays,presentDays)=>{
    const emp=invoke('POST','/api/salary/employees',{body:{name:'Band '+presentDays,salary:30000,monthlyPaidLeaveAllowance:4}}).body.employee;
    const marks=Array(31).fill('A');for(let i=0;i<presentDays;i++)marks[i]='P';
    assert.equal(invoke('POST','/api/salary/attendance/:ym/batch',{params:{ym:'2026-08'},body:{items:[{empId:emp.id,marks}]}}).status,200);
    const row=invoke('GET','/api/salary/month/:ym',{params:{ym:'2026-08'}}).body.rows.find(x=>x.id===emp.id);
    assert.equal(row.computedPaidDays,paidDays,'present days '+presentDays);
  });
  const capped=invoke('POST','/api/salary/employees',{body:{name:'One Day Cap',salary:30000,monthlyPaidLeaveAllowance:1}}).body.employee;
  const marks=Array(31).fill('A');for(let i=0;i<24;i++)marks[i]='P';
  invoke('POST','/api/salary/attendance/:ym/batch',{params:{ym:'2026-08'},body:{items:[{empId:capped.id,marks}]}});
  const cappedRow=invoke('GET','/api/salary/month/:ym',{params:{ym:'2026-08'}}).body.rows.find(x=>x.id===capped.id);
  assert.equal(cappedRow.computedPaidDays,24,'employee-specific maximum remains authoritative');
});

test('a joiner working on the 31st receives that one paid day',()=>{
  const emp=invoke('POST','/api/salary/employees',{body:{name:'Last Day Joiner',salary:30000,joiningDate:'2026-08-31'}}).body.employee;
  assert.equal(invoke('POST','/api/salary/attendance/:ym',{params:{ym:'2026-08'},body:{empId:emp.id,day:'31',mark:'P'}}).status,200);
  const row=invoke('GET','/api/salary/month/:ym',{params:{ym:'2026-08'}}).body.rows.find(x=>x.id===emp.id);
  assert.equal(row.computedPaidDays,1);assert.equal(row.salaryAmt,1000);
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
  invoke('POST','/api/salary/recoveries/:ym',{params:{ym:'2026-09'},body:{empId:emp.id,amount:5000},role:'owner'});
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
  const bad=invoke('POST','/api/salary/payments/batch',{body:{ym:'2026-10',date:'2026-10-31',account:'Counter Cash',proof:'/batch.jpg',items:[{empId:a.id,amount:1},{empId:b.id,amount:99999}]}});assert.equal(bad.status,400);assert.match(bad.body.error,/Batch A.*no salary is payable/);assert.equal(bad.body.remaining,0);
  const again=invoke('GET','/api/salary/month/:ym',{params:{ym:'2026-10'}}).body;assert.equal(again.rows.find(x=>x.id===a.id).transactionPaid,30000,'invalid batch posts nothing');
  const html=fs.readFileSync(path.join(__dirname,'..','public','salary.html'),'utf8');assert.match(html,/Select all payable/);assert.match(html,/Clear selection/);assert.match(html,/Only checked employees will be paid/);assert.match(html,/Partially paid/);
});

test('a noted extra salary payment records the actual cash and carries the excess forward',()=>{
  const emp=invoke('POST','/api/salary/employees',{body:{name:'Extra Paid Employee',salary:30000}}).body.employee;
  invoke('POST','/api/salary/row/:ym',{params:{ym:'2099-03'},body:{empId:emp.id,paidDays:30}});
  const payload={ym:'2099-03',date:'2099-03-31',account:'Gagan Sir Cash',proof:'/cash-proof.jpg',items:[{empId:emp.id,amount:31000}]};
  const missingNote=invoke('POST','/api/salary/payments/batch',{body:payload});
  assert.equal(missingNote.status,400);assert.match(missingNote.body.error,/Enter why/);
  payload.items[0].modificationReason='₹1,000 extra cash salary paid with this batch';
  const posted=invoke('POST','/api/salary/payments/batch',{body:payload});assert.equal(posted.status,200);
  const march=invoke('GET','/api/salary/month/:ym',{params:{ym:'2099-03'}}).body.rows.find(x=>x.id===emp.id);
  assert.equal(march.transactionPaid,31000);assert.equal(march.balance,-1000);
  const april=invoke('GET','/api/salary/month/:ym',{params:{ym:'2099-04'}}).body.rows.find(x=>x.id===emp.id);
  assert.equal(april.openingBalanceCarry,-1000);
});

test('payroll presents a prominent salary payment action without offering zero-balance payments',()=>{
  const html=fs.readFileSync(path.join(__dirname,'..','public','salary.html'),'utf8');
  assert.match(html,/payableEmployees=d\.rows\.filter\(function\(r\)\{return Number\(r\.balance\)>0\.005;\}\)/);
  assert.match(html,/id="quickPayEmployee"/);
  assert.match(html,/id="quickPayButton"/);
  assert.match(html,/No salary balance is currently payable for this month/);
  assert.match(html,/openSalaryPay\(id\)/);
});

test('Prashant Axis 3645 is available for advances and full or partial salary payments',()=>{
  const config=invoke('GET','/api/salary/employees').body;
  assert.ok(config.salaryPayingAccounts.includes('Prashant Axis 3645'));
  assert.ok(config.salaryPayingAccounts.includes('IndusInd Bank 8181'));
  const emp=invoke('POST','/api/salary/employees',{body:{name:'Axis Salary Employee',salary:20000}}).body.employee;
  invoke('POST','/api/salary/row/:ym',{params:{ym:'2098-09'},body:{empId:emp.id,paidDays:30}});
  const partial=invoke('POST','/api/salary/payments/batch',{body:{ym:'2098-09',date:'2098-09-30',account:'Prashant Axis 3645',proof:'/axis-partial.jpg',items:[{empId:emp.id,amount:5000,modificationReason:'Fraction salary payment'}]}});
  assert.equal(partial.status,200);assert.equal(partial.body.total,5000);
  const fullBalance=invoke('POST','/api/salary/payments/batch',{body:{ym:'2098-09',date:'2098-09-30',account:'Prashant Axis 3645',proof:'/axis-balance.jpg',items:[{empId:emp.id,amount:15000}]}});
  assert.equal(fullBalance.status,200);assert.equal(fullBalance.body.total,15000);
  const advance=invoke('POST','/api/salary/advances',{role:'owner',username:'owner',body:{empId:emp.id,amount:1000,date:'2098-10-01',account:'Prashant Axis 3645',recoveryStartMonth:'2098-10',proof:'/axis-advance.jpg'}});
  assert.equal(advance.status,200);assert.equal(advance.body.advance.account,'Prashant Axis 3645');
  const html=fs.readFileSync(path.join(__dirname,'..','public','salary.html'),'utf8');assert.match(html,/Salary paying account \/ cash/);assert.match(html,/ed\.salaryPayingAccounts/);
});

test('partial salary payment requires a reason and preserves the remaining balance with its own proof',()=>{
  const emp=invoke('POST','/api/salary/employees',{body:{name:'Partial Pay Employee',salary:30000}}).body.employee;
  invoke('POST','/api/salary/row/:ym',{params:{ym:'2098-10'},body:{empId:emp.id,paidDays:30}});
  const refused=invoke('POST','/api/salary/payments/batch',{body:{ym:'2098-10',date:'2098-10-31',account:'Gagan Sir Cash',proof:'/partial.jpg',items:[{empId:emp.id,amount:5500}]}});assert.equal(refused.status,400);assert.match(refused.body.error,/why/i);
  const paid=invoke('POST','/api/salary/payments/batch',{body:{ym:'2098-10',date:'2098-10-31',account:'Gagan Sir Cash',proof:'/partial.jpg',items:[{empId:emp.id,amount:5500,modificationReason:'First partial salary payment'}]}});assert.equal(paid.status,200);
  const row=invoke('GET','/api/salary/month/:ym',{params:{ym:'2098-10'}}).body.rows.find(x=>x.id===emp.id);assert.equal(row.paid,5500);assert.equal(row.balance,24500);
  const stored=JSON.parse(fs.readFileSync(path.join(tempDir,'salary.json'),'utf8')).salaryPayments.find(x=>x.empId===emp.id&&x.ym==='2098-10');assert.equal(stored.modificationReason,'First partial salary payment');assert.equal(stored.remainingBeforePayment,30000);assert.equal(stored.balanceAfterPayment,24500);assert.equal(stored.proof,'/partial.jpg');
  const html=fs.readFileSync(path.join(__dirname,'..','public','salary.html'),'utf8');assert.match(html,/Salary adjustment/);assert.match(html,/Advance due/);assert.match(html,/Deduct now/);assert.match(html,/Balance to pay/);assert.match(html,/Comment \/ reason for different amount/);assert.match(html,/submitSingleSalaryPayment/);
});

test('salary payments upload and preserve multiple screenshots as one payment',()=>{
  const emp=invoke('POST','/api/salary/employees',{body:{name:'Multi Proof Salary',salary:12000}}).body.employee;
  invoke('POST','/api/salary/row/:ym',{params:{ym:'2098-12'},body:{empId:emp.id,paidDays:30}});
  const proofs=['/salary-part-2000.jpg','/salary-part-8000.jpg'];
  const paid=invoke('POST','/api/salary/payments/batch',{body:{ym:'2098-12',date:'2098-12-31',account:'Gagan Sir Cash',proofs,items:[{empId:emp.id,amount:10000,modificationReason:'Paid in two immediate transfers'}]}});
  assert.equal(paid.status,200);assert.equal(paid.body.proofCount,2);
  const stored=JSON.parse(fs.readFileSync(path.join(tempDir,'salary.json'),'utf8')).salaryPayments.find(x=>x.empId===emp.id&&x.ym==='2098-12');
  assert.deepEqual(stored.proofs,proofs);assert.equal(stored.proof,proofs[0]);assert.equal(stored.amount,10000);
  const html=fs.readFileSync(path.join(__dirname,'..','public','salary.html'),'utf8');
  assert.match(html,/id="one_proof"[^>]*multiple/);assert.match(html,/id="sp_proof"[^>]*multiple/);assert.match(html,/proofs:proofs/);
});

test('staff advance edits require owner approval and every correction remains audited',()=>{
  const emp=invoke('POST','/api/salary/employees',{body:{name:'Editable Advance Employee',salary:20000}}).body.employee,advance=postAdvance(emp,{amount:2000,date:'2098-11-01',account:'Axis Bank 3448',proof:'/advance-edit.jpg'});
  const proposed=invoke('PATCH','/api/salary/advances/:id',{params:{id:advance.id},body:{amount:2100,date:'2098-11-02',account:'Axis Bank 3448',reason:'Correction'}});
  assert.equal(proposed.status,200);assert.equal(proposed.body.pendingApproval,true);
  assert.equal(invoke('GET','/api/salary/advances').body.advances.find(x=>x.id===advance.id).amount,2000);
  assert.equal(invoke('POST','/api/salary/advance-edit-requests/:id/decision',{params:{id:proposed.body.request.id},body:{decision:'approve'}}).status,403);
  assert.equal(invoke('POST','/api/salary/advance-edit-requests/:id/decision',{params:{id:proposed.body.request.id},role:'owner',body:{decision:'approve'}}).status,200);
  const edited=invoke('PATCH','/api/salary/advances/:id',{params:{id:advance.id},role:'owner',body:{amount:2200,date:'2098-11-02',account:'Axis Bank 3448',note:'Corrected advance',reason:'Proof showed corrected amount'}});assert.equal(edited.status,200);assert.equal(edited.body.advance.amount,2200);assert.equal(edited.body.advance.date,'2098-11-02');
  const saved=JSON.parse(fs.readFileSync(path.join(tempDir,'salary.json'),'utf8'));assert.ok(saved.advanceAudit.some(x=>x.action==='EDITED'&&x.advanceId===advance.id&&x.details.reason==='Proof showed corrected amount'));
});

test('historical-import cleanup deletes only imported advances and retains an audit',()=>{
  const store={advances:{
    historical:{id:'ADV-HIST',employeeName:'Imported',amount:50000,date:'2026-08-01',reference:'HIST-1',historicalImport:true,recoveries:[{amount:23000}]},
    appRequest:{id:'ADV-APP',employeeName:'App request',amount:1000,requestId:'ADVR-1',proof:'/proof.jpg',recoveries:[]},
    olderApp:{id:'ADV-OLD',employeeName:'Older app entry',amount:2500,proof:'/proof-old.jpg',recoveries:[]}
  },advanceAudit:[],oneTimeMigrations:{}};
  assert.equal(_removeHistoricalAdvancesV16(store),true);
  assert.deepEqual(Object.keys(store.advances).sort(),['appRequest','olderApp']);
  assert.equal(store.advanceAudit.at(-1).action,'HISTORICAL_IMPORTS_DELETED');
  assert.equal(store.advanceAudit.at(-1).details.count,1);
  assert.equal(store.advanceAudit.at(-1).details.totals.outstanding,27000);
  assert.equal(_removeHistoricalAdvancesV16(store),false,'cleanup is idempotent');
});

test('positive and negative balances carry forward once and payroll respects employment months',()=>{
  const carryEmp=invoke('POST','/api/salary/employees',{body:{name:'Carry Forward Employee',salary:30000}}).body.employee;
  invoke('POST','/api/salary/row/:ym',{params:{ym:'2026-05'},body:{empId:carryEmp.id,paidDays:0,advance:5000}});
  invoke('POST','/api/salary/row/:ym',{params:{ym:'2026-06'},body:{empId:carryEmp.id,paidDays:30}});
  let june=invoke('GET','/api/salary/month/:ym',{params:{ym:'2026-06'}}).body.rows.find(x=>x.id===carryEmp.id);
  assert.equal(june.openingBalanceCarry,-5000);assert.equal(june.openingAdvanceCarry,5000);assert.equal(june.openingPayableCarry,0);assert.equal(june.netPayable,25000);assert.equal(june.carryForwardAdvance,0);
  assert.equal(june.deductionAdjustment,-5000);assert.ok(june.adjustmentDetails.some(x=>x.kind==='previous_overpayment'&&x.amount===-5000));
  const payableEmp=invoke('POST','/api/salary/employees',{body:{name:'Positive Carry Employee',salary:30000}}).body.employee;
  invoke('POST','/api/salary/row/:ym',{params:{ym:'2026-05'},body:{empId:payableEmp.id,paidDays:30,paid:29000}});
  invoke('POST','/api/salary/row/:ym',{params:{ym:'2026-06'},body:{empId:payableEmp.id,paidDays:30}});
  june=invoke('GET','/api/salary/month/:ym',{params:{ym:'2026-06'}}).body.rows.find(x=>x.id===payableEmp.id);
  assert.equal(june.openingBalanceCarry,1000);assert.equal(june.openingAdvanceCarry,0);assert.equal(june.openingPayableCarry,1000);assert.equal(june.netPayable,31000);
  assert.equal(june.deductionAdjustment,1000);assert.ok(june.adjustmentDetails.some(x=>x.kind==='previous_payable'&&x.amount===1000));
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
  assert.equal(invalidAccount.status,400);assert.match(invalidAccount.body.error,/authorized salary paying account/);
  const html=fs.readFileSync(path.join(__dirname,'..','public','salary.html'),'utf8'),source=fs.readFileSync(path.join(__dirname,'..','modules','salary.js'),'utf8');assert.match(html,/Salary paying account \/ cash/);assert.match(source,/Extra salary paid earlier/);assert.match(source,/Salary left unpaid earlier/);assert.match(html,/positive balances remain payable/);
});

test('August historical closing offsets are removed without fabricating payments or closing the month',()=>{
  const month=invoke('GET','/api/salary/month/:ym',{params:{ym:'2026-08'},role:'owner'}).body;
  assert.equal(month.finalized,false);
  assert.ok(month.rows.every(x=>!x.historicalCloseAdjustment));
  const salary=JSON.parse(fs.readFileSync(path.join(tempDir,'salary.json'),'utf8'));
  assert.ok(salary.oneTimeMigrations.reopen_august_2026_payroll_for_payments_v18);
  assert.ok(salary.salaryPaymentAudit.some(x=>x.action==='AUGUST_PAYROLL_REOPENED'));
  assert.equal(salary.salaryPayments.filter(x=>x.ym==='2026-08').length,0);
  const reopened=salary.oneTimeMigrations.reopen_august_2026_payroll_for_payments_v18.removedOffsets.length;
  invoke('GET','/api/salary/month/:ym',{params:{ym:'2026-08'},role:'owner'});
  assert.equal(JSON.parse(fs.readFileSync(path.join(tempDir,'salary.json'),'utf8')).oneTimeMigrations.reopen_august_2026_payroll_for_payments_v18.removedOffsets.length,reopened);
});

test('Prashant can pay August salary with proof; a changed amount requires a note, not a reopening reason',()=>{
  const who={role:'claimant',username:'prashant'},month=invoke('GET','/api/salary/month/:ym',{...who,params:{ym:'2026-08'}}).body;
  const row=month.rows.find(x=>x.balance>1000&&x.paid===0);
  assert.ok(row,'an August salary is payable');
  const amount=Math.round((row.balance-100)*100)/100;
  const body={ym:'2026-08',date:'2026-09-16',account:'Prashant Axis 3645',proof:'/august-proof.jpg',items:[{empId:row.id,amount}]};
  assert.equal(invoke('POST','/api/salary/payments/batch',{...who,body}).status,400);
  body.items[0].modificationReason='Paying the remaining ₹100 separately';
  const result=invoke('POST','/api/salary/payments/batch',{...who,body});
  assert.equal(result.status,200);
  const payment=invoke('GET','/api/salary/payments/:ym',{...who,params:{ym:'2026-08'}}).body.payments.find(x=>x.empId===row.id);
  assert.equal(payment.amount,amount);assert.equal(payment.modificationReason,body.items[0].modificationReason);assert.equal(payment.proof,'/august-proof.jpg');
});

test('owner can correct a proof-backed salary payment and correction is audited',()=>{
  const emp=invoke('POST','/api/salary/employees',{body:{name:'Correction employee',salary:30000},role:'owner'}).body.employee;
  invoke('POST','/api/salary/row/:ym',{params:{ym:'2099-01'},body:{empId:emp.id,paidDays:30},role:'owner'});
  const made=invoke('POST','/api/salary/payments/batch',{body:{ym:'2099-01',date:'2099-01-31',account:'Prashant Axis 3645',proof:'/first.jpg',items:[{empId:emp.id,amount:10000,modificationReason:'First tranche'}]},role:'owner'});
  assert.equal(made.status,200);
  const id=made.body.batchId+'-001';
  const edited=invoke('PATCH','/api/salary/payments/:id',{params:{id},body:{amount:9000,date:'2099-01-30',account:'Gagan Sir Cash',proofs:['/corrected.jpg'],reference:'CASH-1',reason:'Actual cash receipt'},role:'owner'});
  assert.equal(edited.status,200);
  assert.equal(edited.body.payment.amount,9000);
  assert.equal(edited.body.payment.account,'Gagan Sir Cash');
  const excess={amount:31000,date:'2099-01-30',account:'Gagan Sir Cash',proofs:['/corrected.jpg'],reference:'CASH-1',reason:'Proof shows additional cash; carry excess'};
  assert.equal(invoke('PATCH','/api/salary/payments/:id',{params:{id},body:excess,role:'owner'}).status,400);
  assert.equal(invoke('PATCH','/api/salary/payments/:id',{params:{id},body:{...excess,allowOverpayment:true},role:'accounting'}).status,403);
  assert.equal(invoke('PATCH','/api/salary/payments/:id',{params:{id},body:{...excess,allowOverpayment:true},role:'owner'}).status,200);
  assert.equal(invoke('PATCH','/api/salary/payments/:id',{params:{id},body:{amount:8000,date:'2099-01-30',account:'Gagan Sir Cash',proofs:['/corrected.jpg'],reason:'No'},role:'accounting'}).status,403);
  const view=invoke('GET','/api/salary/payments/:ym',{params:{ym:'2099-01'},role:'owner'}).body;
  assert.equal(view.payments.find(x=>x.id===id).proof,'/corrected.jpg');
  assert.equal(view.audit.find(x=>x.paymentId===id).overpayment,1000);
  assert.equal(view.audit.find(x=>x.paymentId===id&&x.before.amount===10000).before.amount,10000);
  assert.equal(invoke('GET','/api/salary/month/:ym',{params:{ym:'2099-01'},role:'owner'}).body.rows.find(x=>x.id===emp.id).paid,31000);
});

test('owner links one finalized 3645 debit to salary without a second account-ledger debit',()=>{
  const emp=invoke('POST','/api/salary/employees',{body:{name:'Linked debit employee',salary:12000},role:'owner'}).body.employee;
  invoke('POST','/api/salary/row/:ym',{params:{ym:'2099-03'},body:{empId:emp.id,paidDays:30},role:'owner'});
  fs.writeFileSync(path.join(tempDir,'expenses.json'),JSON.stringify({bankStatements:{'Prashant Axis 3645':{imports:[{id:'BST-1',reconciliationRows:[{id:'row-1',bank:{date:'2099-03-31',debit:12000,reference:'UTR-SALARY',description:'Salary paid'},ledger:{id:'EX-1/PAY-1',date:'2099-03-31',debit:12000,description:'Employee salary'}}]}]}}}));
  const candidates=invoke('GET','/api/salary/existing-3645-debits',{role:'owner'}).body.candidates;
  assert.equal(candidates.length,1);
  const linked=invoke('POST','/api/salary/payments/link-existing',{role:'owner',body:{ym:'2099-03',empId:emp.id,ledgerEntryId:'EX-1/PAY-1',proofs:['/salary-proof.jpg'],reason:'Existing bank payment for this employee'}});
  assert.equal(linked.status,200);assert.equal(linked.body.noAdditionalLedgerDebit,true);
  assert.equal(linked.body.payment.linkedLedgerEntryId,'EX-1/PAY-1');
  assert.equal(invoke('GET','/api/salary/month/:ym',{params:{ym:'2099-03'}}).body.rows.find(x=>x.id===emp.id).balance,0);
  assert.equal(invoke('GET','/api/salary/existing-3645-debits',{role:'owner'}).body.candidates.length,0);
  assert.equal(invoke('POST','/api/salary/payments/link-existing',{role:'owner',body:{ym:'2099-03',empId:emp.id,ledgerEntryId:'EX-1/PAY-1',proofs:['/salary-proof.jpg'],reason:'Duplicate'}}).status,409);
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
  assert.equal(month.totals.advance,0,'historical spreadsheet advances no longer affect payroll');
  const allHistorical=invoke('GET','/api/salary/advances').body.advances.filter(x=>x.historicalImport),history=allHistorical.filter(x=>String(x.sourceKey).startsWith('provided-advance-sheet-')),augustHistory=allHistorical.filter(x=>String(x.sourceKey).startsWith('final-august-advance-sheet-'));
  assert.equal(history.length,0);assert.equal(augustHistory.length,0);
  assert.equal(_finalAugust2026Advances.reduce((n,x)=>n+x[3],0),12000);
  assert.equal(month.totals.paid,291000);assert.equal(Math.round(imported.reduce((n,r)=>n+r.balance,0)*100)/100,85408.35,'salary balances are recalculated without deleted historical advance deductions');
  assert.equal(month.rows.find(r=>r.name==='Pooja').paidDays,10);
  assert.equal(month.rows.find(r=>r.name==='Ravi').paidDays,9);
  const sunny=month.rows.find(r=>r.name==='SUNNY SHARMA'),guard=month.rows.find(r=>r.name==='Guard'),suraj=month.rows.find(r=>/^Suraj/i.test(r.name));
  assert.equal(sunny.salary,24000);assert.equal(sunny.paidDays,22);assert.equal(sunny.advance,0);
  assert.equal(guard.advance,0);
  assert.equal(suraj.paidDays,25);assert.equal(suraj.advance,0);
  const employeeMaster=invoke('GET','/api/salary/employees').body.employees;assert.equal(employeeMaster.find(e=>e.id===suraj.id).monthlyPaidLeaveAllowance,1);assert.equal(employeeMaster.find(e=>e.id===sunny.id).monthlyPaidLeaveAllowance,4);
  const arshpreet=month.rows.find(r=>/^Arshpreet/i.test(r.name)),ravi=month.rows.find(r=>r.name==='Ravi');
  const august=invoke('GET','/api/salary/month/:ym',{params:{ym:'2026-08'}}).body.rows,finalNames=new Set(_finalJuly2026Payroll.map(x=>String(x[0]).replace(/\s*\([^)]*\)\s*/g,'').trim().toLowerCase()+'|'+String(x[1]).toLowerCase())),finalAugust=august.filter(r=>finalNames.has(String(r.name).replace(/\s*\([^)]*\)\s*/g,'').trim().toLowerCase()+'|'+String(r.post).toLowerCase()));
  assert.equal(august.find(r=>r.id===arshpreet.id).advance,0);assert.equal(august.find(r=>r.id===arshpreet.id).outstandingAdvance,0);assert.equal(august.find(r=>r.id===ravi.id).advance,0);assert.equal(august.find(r=>r.id===suraj.id).advance,0);
  assert.equal(august.find(r=>r.name==='PIYUSH').openingPayableCarry,266.67);assert.ok(finalAugust.every(r=>!r.historicalCloseAdjustment),'August historical closing offsets are removed');
  const september=invoke('GET','/api/salary/month/:ym',{params:{ym:'2026-09'}}).body.rows;assert.ok(finalAugust.every(r=>september.find(x=>x.id===r.id).openingBalanceCarry===r.balance),'September carries remaining August balance');
  assert.ok(finalAugust.every(r=>r.outstandingAdvance===0),'no spreadsheet-imported advance survives the cleanup');
  const sundayOff=_julyImportedMarks({weekOffDay:'Sunday'},'A'.repeat(31));
  assert.equal(sundayOff.attendance['05'],'WO','an absent weekly-off date stays visibly marked WO');
  assert.equal(sundayOff.attendance['01'],'A','ordinary absence remains visibly marked A');
  const twoLeaves=_julyImportedMarks({},'P'.repeat(29)+'AA'),fiveLeaves=_julyImportedMarks({},'P'.repeat(26)+'AAAAA');
  assert.equal(twoLeaves.paidDays,32,'two unused allowance days become extra paid days after the July adjustment');
  assert.equal(fiveLeaves.paidDays,29,'a fifth leave reduces the normal 30 paid days by one');
});

test('owner final salary correction requires a reason and updates earned payroll without recording a payment',()=>{
  const emp=invoke('POST','/api/salary/employees',{body:{name:'Final Salary Correction Test',salary:30000,joiningDate:'2099-01-01'}}).body.employee;
  const ym='2099-01',params={ym,empId:emp.id};
  assert.equal(invoke('POST','/api/salary/row/:ym',{params:{ym},body:{empId:emp.id,paidDays:20}}).status,200);
  assert.equal(invoke('POST','/api/salary/post/:ym',{params:{ym}}).status,200);
  assert.equal(invoke('PATCH','/api/salary/final-amount/:ym/:empId',{params,role:'admin',body:{amount:19000,reason:'Approved correction'}}).status,403);
  assert.equal(invoke('PATCH','/api/salary/final-amount/:ym/:empId',{params,role:'owner',body:{amount:19000}}).status,400);
  assert.equal(invoke('PATCH','/api/salary/final-amount/:ym/:empId',{params,role:'owner',body:{amount:'invalid',reason:'Test'}}).status,400);
  const correction=invoke('PATCH','/api/salary/final-amount/:ym/:empId',{params,role:'owner',username:'owner',body:{amount:19000,reason:'Approved attendance correction'}});
  assert.equal(correction.status,200);
  const month=invoke('GET','/api/salary/month/:ym',{params:{ym},role:'owner'}).body,row=month.rows.find(x=>x.id===emp.id);
  assert.equal(row.calculatedSalaryAmt,20000);assert.equal(row.salaryAmt,19000);assert.equal(row.finalSalaryReason,'Approved attendance correction');
  assert.equal(row.paid,0);assert.equal(row.balance,19000);
  assert.equal(month.finalSalaryAudit.find(x=>x.empId===emp.id).previousAmount,20000);
  const salary=JSON.parse(fs.readFileSync(path.join(tempDir,'salary.json'),'utf8'));
  assert.equal(salary.payrollPostings[ym].rows.find(x=>x.empId===emp.id).salaryAmt,19000);
  assert.equal(salary.salaryPayments.filter(x=>x.empId===emp.id&&x.ym===ym).length,0);
  const html=fs.readFileSync(path.join(__dirname,'..','public','salary.html'),'utf8');
  assert.match(html,/Salary adjustment/);assert.match(html,/openFinalSalary/);assert.match(html,/saveFinalSalary/);
});

test('signed salary adjustment and partial advance deduction stay separate to the paisa',()=>{
  const emp=invoke('POST','/api/salary/employees',{body:{name:'Signed Salary Example',salary:7600}}).body.employee;
  invoke('POST','/api/salary/row/:ym',{params:{ym:'2098-06'},body:{empId:emp.id,paidDays:30}});
  postAdvance(emp,{amount:1000,date:'2098-06-02',payoutDate:'2098-06-02',recoveryStartMonth:'2098-06'});
  const adjust=invoke('PATCH','/api/salary/final-amount/:ym/:empId',{params:{ym:'2098-06',empId:emp.id},role:'owner',body:{adjustment:400,reason:'Approved extra salary'}});
  assert.equal(adjust.status,200);
  const recover=invoke('POST','/api/salary/recoveries/:ym',{params:{ym:'2098-06'},role:'owner',body:{empId:emp.id,amount:500,reason:'Recover half now'}});
  assert.equal(recover.status,200);
  const row=invoke('GET','/api/salary/month/:ym',{params:{ym:'2098-06'}}).body.rows.find(x=>x.id===emp.id);
  assert.equal(row.calculatedSalaryAmt,7600);assert.equal(row.salaryAdjustment,400);assert.equal(row.salaryAmt,8000);
  assert.equal(row.advanceDueBeforeDeduction,1000);assert.equal(row.loggedAdvanceRecovery,500);assert.equal(row.outstandingAdvance,500);assert.equal(row.netPayable,7500);
  const revised=invoke('PATCH','/api/salary/final-amount/:ym/:empId',{params:{ym:'2098-06',empId:emp.id},role:'owner',body:{adjustment:-400.25,reason:'Approved deduction'}});
  assert.equal(revised.status,200);
  const row2=invoke('GET','/api/salary/month/:ym',{params:{ym:'2098-06'}}).body.rows.find(x=>x.id===emp.id);
  assert.equal(row2.salaryAmt,7199.75);assert.equal(row2.netPayable,6699.75);
});

test('Prashant can edit salary and record proof-backed payments from authorized salary accounts',()=>{
  const emp=invoke('POST','/api/salary/employees',{body:{name:'Prashant Payment Access Test',salary:20000,joiningDate:'2099-02-01'}}).body.employee,ym='2099-02';
  assert.equal(invoke('POST','/api/salary/row/:ym',{params:{ym},body:{empId:emp.id,paidDays:20}}).status,200);
  const who={role:'claimant',username:'prashant'},accounts=invoke('GET','/api/salary/employees',who).body.salaryPayingAccounts;
  assert.deepEqual(accounts,['Prashant Axis 3645','IndusInd Bank 8181','Prashant Cash','Gagan Sir Cash','Counter Cash']);
  assert.equal(invoke('POST','/api/salary/row/:ym',{...who,params:{ym},body:{empId:emp.id,paidDays:15}}).status,200);
  assert.equal(invoke('PATCH','/api/salary/final-amount/:ym/:empId',{...who,params:{ym,empId:emp.id},body:{amount:10000,reason:'Confirmed final amount'}}).status,200);
  assert.equal(invoke('GET','/api/salary/month/:ym',{...who,params:{ym}}).body.permissions.canModifyPayroll,true);
  const payload={ym,date:'2099-02-20',account:'Gagan Sir Cash',proofs:['/api/expenses/photo/no.jpg'],items:[{empId:emp.id,amount:1000,modificationReason:'Partial'}]};
  const withoutComment={...payload,items:[{empId:emp.id,amount:1000}]};
  assert.equal(invoke('POST','/api/salary/payments/batch',{...who,body:withoutComment}).status,400);
  const made=invoke('POST','/api/salary/payments/batch',{...who,body:payload});assert.equal(made.status,200);
  const payment=invoke('GET','/api/salary/payments/:ym',{...who,params:{ym}}).body.payments.find(x=>x.empId===emp.id);
  assert.ok(payment);assert.equal(payment.amount,1000);assert.equal(payment.modificationReason,'Partial');assert.equal(payment.createdBy,'prashant');
  assert.equal(invoke('POST','/api/salary/payments/:id/proofs',{...who,params:{id:payment.id},body:{proofs:['/api/expenses/photo/more.jpg']}}).status,200);
  const after=invoke('GET','/api/salary/payments/:ym',{...who,params:{ym}}).body.payments.find(x=>x.id===payment.id);
  assert.deepEqual(after.proofs,['/api/expenses/photo/no.jpg','/api/expenses/photo/more.jpg']);
  const from8181=invoke('POST','/api/salary/payments/batch',{...who,body:{...payload,account:'IndusInd Bank 8181',date:'2099-02-21',proofs:['/api/expenses/photo/8181.jpg']}});assert.equal(from8181.status,200);assert.equal(from8181.body.total,1000);
  assert.equal(invoke('GET','/api/salary/month/:ym',{...who,params:{ym}}).body.rows.find(x=>x.id===emp.id).paid,2000);
  assert.equal(invoke('POST','/api/salary/payments/:id/proofs',{...who,params:{id:payment.id},body:{proofs:['https://other.test/not-proof']}}).status,400);
  const html=fs.readFileSync(path.join(__dirname,'..','public','salary.html'),'utf8');
  assert.match(html,/Amount actually paid ₹ — editable/);
  assert.match(html,/Comment \/ reason for different amount/);
  assert.match(html,/p\.modificationReason\|\|'—'/);
});

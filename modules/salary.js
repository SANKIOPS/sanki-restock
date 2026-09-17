'use strict';
// Salary + attendance — the payroll side of in-app accounting.
// Model (replicates the founder's "JUL SALARY" sheet exactly):
//   Salary Amt  = monthly salary ÷ 30 × Paid Days   (30 is fixed, not calendar days)
//   Net Payable = Salary Amt − Advance
//   Balance     = Net Payable − Paid                 (over/under-payment carries)
// Paid Days can EXCEED 30 (overtime / extra days) and is editable per month.
// Attendance (manager-marked) auto-suggests Paid Days but the payroll row can
// always override it. Salary is CONFIDENTIAL → every route is admin/accounting only.
// Salary Amt (earned) feeds the P&L Fixed Cost row, channel-tagged (POS/Website/
// Shared), Shared split by revenue share like other shared costs.
const express = require('express');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { AsyncLocalStorage } = require('async_hooks');
const multer = require('multer');
const XLSX = require('xlsx');
const router = express.Router();
const salarySheetUpload=multer({storage:multer.memoryStorage(),limits:{fileSize:5*1024*1024}}).single('file');

const DATA_DIR = process.env.DATA_PATH ? path.dirname(process.env.DATA_PATH) : path.join(__dirname, '..');
const SAL_PATH = path.join(DATA_DIR, 'salary.json');
const SAMAST_SAL_PATH = path.join(DATA_DIR, 'salary-samast.json');
const EXP_PATH = path.join(DATA_DIR, 'expenses.json');
const salaryContext = new AsyncLocalStorage();
function salaryEntity(value){return String(value||'SANKI').toUpperCase()==='SAMAST'?'SAMAST':'SANKI';}
function activeSalaryEntity(){const context=salaryContext.getStore();return salaryEntity(context&&context.entity);}
function activeSalaryPath(){return activeSalaryEntity()==='SAMAST'?SAMAST_SAL_PATH:SAL_PATH;}

function num(v) { const n = parseFloat(v); return isNaN(n) ? 0 : n; }
function round0(n) { return Math.round(n); }
function round2(n) { return Math.round(n * 100) / 100; }
function byEmployeeName(a, b) { return String(a.name || a.employeeName || '').localeCompare(String(b.name || b.employeeName || ''), 'en', { sensitivity:'base', numeric:true }); }
function salaryHistoryOf(e) { return Array.isArray(e.salaryHistory) ? e.salaryHistory.slice().sort((a,b)=>String(a.effectiveMonth||'').localeCompare(String(b.effectiveMonth||''))||String(a.recordedAt||'').localeCompare(String(b.recordedAt||''))) : []; }
function salaryForMonth(e, ym) {
  let salary=num(e.baseSalary != null ? e.baseSalary : e.salary);
  salaryHistoryOf(e).forEach(x=>{ if(String(x.effectiveMonth||'')<=ym)salary=num(x.updatedSalary); });
  return round2(salary);
}

const CHANNELS = ['POS', 'Website', 'Shared'];
const SALARY_PAYING_ACCOUNTS = ['Prashant Axis 3645', 'IndusInd Bank 8181', 'Prashant Cash', 'Gagan Sir Cash', 'Counter Cash'];
const SOURCE_SHEET_POSTING_ACCOUNTS = [...SALARY_PAYING_ACCOUNTS,'Axis Bank 3448','IndusInd Bank 7883','ICICI Bank 0992','ICICI Bank 0993','Gagan Personal Cash'];
const WEEK_DAYS = ['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'];
// Paid-day value per attendance mark: Present 1, Half 0.5, Paid-leave 1,
// Week-off 1 (paid), Absent 0.
const MARKS = { P: 1, H: 0.5, PL: 1, WO: 1, A: 0 };

function blank() { return { employees: {}, months: {}, divisor: 30, seq: 0, advances: {}, advanceSeq: 0, advanceAudit: [], advanceRequests:{}, advanceRequestSeq:0, advanceRequestAudit:[], payrollPostings:{}, salaryPayments:[], salaryPaymentBatchSeq:0, salaryPaymentAudit:[], finalSalaryAudit:[], oneTimeMigrations:{} }; }
function load() {
  try {
    const entity=activeSalaryEntity(),s=Object.assign(blank(), JSON.parse(fs.readFileSync(activeSalaryPath(), 'utf8')));
    const julyImported=entity==='SANKI'&&applyJuly2026AttendanceAndPayroll(s);
    const employeeRepair=entity==='SANKI'&&applySunnyGuardAndSurajRepair(s);
    const correctedJuly=entity==='SANKI'&&applyCorrectedJulyAttendanceV7(s);
    const normalizedLeaveMarks=entity==='SANKI'&&applyStoredLeaveAllowancesV11(s);
    const finalJulyPayroll=entity==='SANKI'&&applyFinalJuly2026PayrollV13(s);
    const correctedAshpreetAdvance=entity==='SANKI'&&repairAshpreetOutstandingAdvanceV14(s);
    const allocatedAshpreetSalary=entity==='SANKI'&&allocateAshpreetSalaryRecoveryV15(s);
    const removedHistoricalAdvances=removeHistoricalAdvancesV16(s);
    const closedHistoricalPayroll=entity==='SANKI'&&closeHistoricalPayrollCarryV17(s);
    const reopenedAugustPayroll=entity==='SANKI'&&reopenAugustPayrollForPaymentsV18(s);
    if(julyImported||employeeRepair||correctedJuly||normalizedLeaveMarks||finalJulyPayroll||correctedAshpreetAdvance||allocatedAshpreetSalary||removedHistoricalAdvances||closedHistoricalPayroll||reopenedAugustPayroll) save(s);
    return s;
  } catch { return blank(); }
}

// Owner-authorized cleanup: advances imported from spreadsheets/history are not
// part of the app advance ledger. Keep every app-created request and posted
// advance (including older proof-backed advances that predate request IDs).
function removeHistoricalAdvancesV16(s){
  const key='remove_historical_import_advances_v16';s.oneTimeMigrations=s.oneTimeMigrations||{};
  if(s.oneTimeMigrations[key])return false;
  const now=new Date().toISOString(),removed=[];
  Object.entries(s.advances||{}).forEach(([id,a])=>{
    if(a.historicalImport!==true)return;
    removed.push({id,employeeName:a.employeeName||'',date:a.date||'',amount:round2(a.amount),recovered:advanceRecovered(a),outstanding:advanceOutstanding(a),reference:a.reference||''});
    delete s.advances[id];
  });
  const totals={amount:round2(removed.reduce((n,a)=>n+a.amount,0)),recovered:round2(removed.reduce((n,a)=>n+a.recovered,0)),outstanding:round2(removed.reduce((n,a)=>n+a.outstanding,0))};
  s.advanceAudit=s.advanceAudit||[];
  s.advanceAudit.push({at:now,by:'Owner-authorized cleanup',action:'HISTORICAL_IMPORTS_DELETED',advanceId:'',details:{count:removed.length,totals,advanceIds:removed.map(a=>a.id)}});
  s.oneTimeMigrations[key]={appliedAt:now,count:removed.length,totals,advanceIds:removed.map(a=>a.id),rule:'Only advances explicitly marked historicalImport were deleted; app-created requests and advances were preserved.'};
  return true;
}

// The Owner also confirmed that payroll before September 2026 is closed. Once
// spreadsheet advances are removed, their former salary deductions must not
// reappear as company payables. Record a visible non-cash closing adjustment
// on August instead of fabricating salary-payment transactions.
function closeHistoricalPayrollCarryV17(s){
  const key='close_historical_payroll_carry_through_2026_08_v17';s.oneTimeMigrations=s.oneTimeMigrations||{};
  if(s.oneTimeMigrations[key])return false;
  const ym='2026-08',mo=ensureMonth(s,ym),closed=[];
  computeMonth(s,ym).forEach(row=>{
    if(Math.abs(num(row.balance))<.005)return;
    mo.rows[row.id]=Object.assign({},mo.rows[row.id],{historicalCloseAdjustment:round2(-row.balance)});
    closed.push({empId:row.id,employeeName:row.name,balanceBefore:round2(row.balance),adjustment:round2(-row.balance)});
  });
  const now=new Date().toISOString(),total=round2(closed.reduce((n,x)=>n+x.adjustment,0));
  s.oneTimeMigrations[key]={appliedAt:now,throughMonth:ym,count:closed.length,totalAdjustment:total,entries:closed,rule:'Historical payroll is closed without creating salary-payment transactions; September 2026 starts with zero pre-app salary carry.'};
  return true;
}
// August must remain payable until actual proof-backed salary payments are
// recorded. The v17 offsets were bookkeeping placeholders, never payments.
function reopenAugustPayrollForPaymentsV18(s){
  const key='reopen_august_2026_payroll_for_payments_v18';s.oneTimeMigrations=s.oneTimeMigrations||{};
  if(s.oneTimeMigrations[key])return false;
  const ym='2026-08',mo=ensureMonth(s,ym),reopened=[];
  Object.entries(mo.rows||{}).forEach(([empId,row])=>{
    const offset=round2(num(row.historicalCloseAdjustment));
    if(!offset)return;
    delete row.historicalCloseAdjustment;
    reopened.push({empId,employeeName:(s.employees[empId]||{}).name||'',removedOffset:offset});
  });
  const wasFinalized=!!mo.finalized;
  mo.finalized=false;
  const now=new Date().toISOString();
  s.salaryPaymentAudit=s.salaryPaymentAudit||[];
  s.salaryPaymentAudit.push({at:now,by:'Owner-authorized August reopening',action:'AUGUST_PAYROLL_REOPENED',ym,removedOffsets:reopened.length,wasFinalized,details:'Removed non-cash closing offsets. Existing recorded payments were preserved; no new payments were created.'});
  s.oneTimeMigrations[key]={appliedAt:now,ym,removedOffsets:reopened,wasFinalized,rule:'August 2026 is open for proof-backed payments; changed payment amounts still require a note.'};
  return true;
}
function save(s) { const target=activeSalaryPath(),tmp = target + '.tmp-' + process.pid + '-' + Date.now(); fs.writeFileSync(tmp, JSON.stringify(s)); fs.renameSync(tmp, target); }

function salarySheetChanges(s,ym,file){
  if(!/^\d{4}-(0[1-9]|1[0-2])$/.test(ym))throw new Error('Choose a valid payroll month.');
  if(!file||!file.buffer||!/^\.(xlsx|xls|csv)$/i.test(path.extname(file.originalname||'')))throw new Error('Choose an Excel or CSV salary sheet (.xlsx, .xls, .csv).');
  let workbook,rows;
  try{workbook=XLSX.read(file.buffer,{type:'buffer',cellDates:false});rows=XLSX.utils.sheet_to_json(workbook.Sheets[workbook.SheetNames[0]],{defval:'',raw:false});}
  catch{throw new Error('This salary sheet could not be read. Use the downloadable template.');}
  if(!rows.length||rows.length>250)throw new Error('The sheet must contain 1 to 250 employee rows.');
  const norm=x=>String(x||'').trim().toLowerCase().replace(/[^a-z0-9]/g,'');
  const aliases={id:['employeeid','empid'],name:['employeename','employee','name'],paidDays:['paiddays','dayspaid'],finalAmount:['finalsalaryamount','finalsalary','salaryamount','salaryamt'],remarks:['remarks','comment','notes']};
  const headers=Object.keys(rows[0]).map(norm);
  if(!aliases.id.concat(aliases.name).some(x=>headers.includes(x))||!aliases.paidDays.concat(aliases.finalAmount,aliases.remarks).some(x=>headers.includes(x)))throw new Error('Use Employee ID or Employee Name, plus Paid Days, Final Salary Amount or Remarks. Download the template if needed.');
  const get=(r,keys)=>{const key=Object.keys(r).find(k=>keys.includes(norm(k)));return key==null?'':String(r[key]??'').trim();};
  const amount=(raw,label)=>{const cleaned=raw.replace(/[₹,\s]/g,'');const value=Number(cleaned);if(!cleaned||!Number.isFinite(value))throw new Error(label+' must be a valid number.');return round2(value);};
  const existing=computeMonth(s,ym),seen=new Set(),changes=[];
  rows.forEach((r,index)=>{
    const id=get(r,aliases.id),name=get(r,aliases.name);if(!id&&!name)return;
    const matches=id?[s.employees[id]].filter(Boolean):Object.values(s.employees).filter(e=>norm(e.name)===norm(name));
    if(matches.length!==1)throw new Error('Row '+(index+2)+': '+(name||id)+' does not match exactly one existing employee.');
    const emp=matches[0],before=existing.find(x=>x.id===emp.id);if(!before||!employeeInPayrollMonth(emp,ym))throw new Error('Row '+(index+2)+': '+emp.name+' is not employed in '+ym+'.');
    if(seen.has(emp.id))throw new Error('Row '+(index+2)+': '+emp.name+' appears more than once.');seen.add(emp.id);
    const daysText=get(r,aliases.paidDays),finalText=get(r,aliases.finalAmount),remarks=get(r,aliases.remarks);
    let paidDays=null,finalAmount=null;
    if(daysText){paidDays=amount(daysText,'Row '+(index+2)+' Paid Days');if(paidDays<0||paidDays>50)throw new Error('Row '+(index+2)+': Paid Days must be between 0 and 50.');}
    if(finalText){finalAmount=amount(finalText,'Row '+(index+2)+' Final Salary Amount');if(finalAmount<0||finalAmount>100000000)throw new Error('Row '+(index+2)+': Final Salary Amount is out of range.');}
    if(paidDays==null&&finalAmount==null&&!remarks)throw new Error('Row '+(index+2)+': no salary or remark value was supplied.');
    const previousRemarks=before.remarks||'',changesDays=paidDays!=null&&Math.abs(paidDays-num(before.paidDays))>.001,changesAmount=finalAmount!=null&&Math.abs(finalAmount-before.salaryAmt)>.001,changesRemarks=!!remarks&&remarks!==previousRemarks;
    changes.push({empId:emp.id,name:emp.name,previousPaidDays:before.paidDays,paidDays,previousFinalAmount:before.salaryAmt,finalAmount,previousRemarks,remarks,attendanceDerived:before.computedPaidDays!=null,status:changesDays||changesAmount||changesRemarks?'Update existing':'Unchanged'});
  });
  if(!changes.length)throw new Error('No employee rows were found in the sheet.');
  const hash=crypto.createHash('sha256').update(file.buffer).digest('hex');return {changes,hash,previewToken:crypto.createHash('sha256').update(hash+JSON.stringify(changes)).digest('hex')};
}
function applySalarySheetChanges(s,ym,result,reason,by,fileName){
  const mo=ensureMonth(s,ym),at=new Date().toISOString();
  result.changes.forEach(change=>{
    const row=mo.rows[change.empId]=mo.rows[change.empId]||{};
    if(change.paidDays!=null&&Math.abs(num(change.previousPaidDays)-change.paidDays)>.001){row.sheetPaidDaysOverride=change.paidDays;row.sheetPaidDaysReason=reason;row.sheetPaidDaysEditedAt=at;row.sheetPaidDaysEditedBy=by;}
    if(change.finalAmount!=null&&Math.abs(change.previousFinalAmount-change.finalAmount)>.001){row.finalSalaryAmount=change.finalAmount;row.finalSalaryReason=reason;row.finalSalaryEditedAt=at;row.finalSalaryEditedBy=by;s.finalSalaryAudit=s.finalSalaryAudit||[];s.finalSalaryAudit.push({ym,empId:change.empId,employeeName:change.name,at,by,previousAmount:change.previousFinalAmount,finalAmount:change.finalAmount,reason,source:'salary_excel_import'});}
    if(change.remarks)row.remarks=change.remarks;
  });
  const posting=(s.payrollPostings||{})[ym];if(posting){posting.rows=posting.rows||[];const current=computeMonth(s,ym);result.changes.forEach(change=>{const after=current.find(x=>x.id===change.empId);let posted=posting.rows.find(x=>x.empId===change.empId);if(!posted){posted={empId:change.empId,employeeName:change.name};posting.rows.push(posted);}Object.assign(posted,{salaryAmt:after.salaryAmt,netPayable:after.netPayable,paid:after.paid,advanceRecovery:after.loggedAdvanceRecovery,legacyAdvance:after.legacyAdvance,legacyPaid:after.legacyPaid});});posting.correctedAt=at;posting.correctedBy=by;}
  s.salarySheetAudit=s.salarySheetAudit||[];s.salarySheetAudit.push({at,by,ym,fileName:path.basename(fileName||''),fileHash:result.hash,reason,employeeIds:result.changes.map(x=>x.empId),changes:result.changes.filter(x=>x.status!=='Unchanged')});
}
function advanceSheetRows(s,file){
  if(!file||!file.buffer||!/^\.(xlsx|xls|csv)$/i.test(path.extname(file.originalname||'')))throw new Error('Choose an Excel or CSV advances sheet.');
  let rows;try{const book=XLSX.read(file.buffer,{type:'buffer',cellDates:false});rows=XLSX.utils.sheet_to_json(book.Sheets[book.SheetNames[0]],{defval:'',raw:false});}catch{throw new Error('Could not read the advances sheet. Use the template.');}
  if(!rows.length||rows.length>250)throw new Error('The sheet must contain 1 to 250 advances.');
  const norm=x=>String(x||'').trim().toLowerCase().replace(/[^a-z0-9]/g,''),get=(r,keys)=>{const key=Object.keys(r).find(k=>keys.includes(norm(k)));return key==null?'':String(r[key]??'').trim();};
  const sheetDate=value=>{const text=String(value||'').trim();if(/^\d{4}[-/]\d{1,2}[-/]\d{1,2}$/.test(text)){const [year,month,day]=text.split(/[-/]/).map(Number);const iso=[year,String(month).padStart(2,'0'),String(day).padStart(2,'0')].join('-');const parsed=new Date(iso+'T00:00:00Z');return !Number.isNaN(parsed.getTime())&&parsed.toISOString().slice(0,10)===iso?iso:'';}if(/^\d{5}$/.test(text)){const parts=XLSX.SSF.parse_date_code(Number(text));if(parts){const iso=[parts.y,String(parts.m).padStart(2,'0'),String(parts.d).padStart(2,'0')].join('-');return sheetDate(iso);}}return '';};
  const sheetMonth=(value,date)=>{const text=String(value||'').trim();if(!text)return date.slice(0,7);if(/^\d{4}-(0[1-9]|1[0-2])$/.test(text))return text;const match=text.match(/^([A-Za-z]+)(?:\s+(\d{4}))?$/);if(match){const months=['january','february','march','april','may','june','july','august','september','october','november','december'],index=months.indexOf(match[1].toLowerCase());if(index>=0)return String(match[2]||date.slice(0,4))+'-'+String(index+1).padStart(2,'0');}return '';};
  const changes=[],seen=new Set(),matched=new Set();
  rows.forEach((r,index)=>{
    const label='Row '+(index+2),empId=get(r,['employeeid','empid']),name=get(r,['employeename','employee','name']),matches=empId?[s.employees[empId]].filter(Boolean):Object.values(s.employees).filter(e=>norm(e.name)===norm(name));
    if(matches.length!==1)throw new Error(label+': employee must match exactly one existing employee by ID or name.');
    const emp=matches[0],advanceId=get(r,['advanceid','requestid']),forceNew=norm(advanceId)==='new',raw=get(r,['advanceamount','amount']).replace(/[₹,\s]/g,''),amount=Number(raw),date=sheetDate(get(r,['requestdate','proposeddate','date'])),account=get(r,['payingaccount','account']),recoveryStartMonth=sheetMonth(get(r,['recoverystartmonth','recoverymonth']),date),note=get(r,['note','remarks','reason']),reference=get(r,['reference','ref']);
    if(!raw||!Number.isFinite(amount)||amount<=0||amount>100000000)throw new Error(label+': enter a valid advance amount.');
    const parsedDate=new Date(date+'T00:00:00Z');if(!/^\d{4}-\d{2}-\d{2}$/.test(date)||Number.isNaN(parsedDate.getTime())||parsedDate.toISOString().slice(0,10)!==date)throw new Error(label+': use YYYY-MM-DD for Request Date.');
    if(!account)throw new Error(label+': paying account is required.');
    if(!/^\d{4}-(0[1-9]|1[0-2])$/.test(recoveryStartMonth))throw new Error(label+': recovery start month must be YYYY-MM.');
    const key=[emp.id,date,amount,account,reference].join('|');if(seen.has(key))throw new Error(label+': duplicate advance row.');seen.add(key);
    const all=[...Object.values(s.advanceRequests||{}).filter(x=>x.status!=='Posted').map(x=>({record:x,kind:'request'})),...Object.values(s.advances||{}).filter(x=>x.active!==false).map(x=>({record:x,kind:'posted'}))].filter(x=>x.record.empId===emp.id&&x.record.status!=='Rejected');
    let candidates=forceNew?[]:advanceId?all.filter(x=>x.record.id===advanceId):reference?all.filter(x=>x.record.reference===reference):[];
    if(!advanceId&&!candidates.length)candidates=all.filter(x=>x.record.date===date&&x.record.account===account);
    if(!advanceId&&!candidates.length&&all.some(x=>String(x.record.date||'').slice(0,7)===date.slice(0,7)&&round2(num(x.record.amount))===round2(amount)))throw new Error(label+': a similar advance already exists for '+emp.name+' this month. Enter its Advance ID to update it, or put NEW in Advance ID to confirm a separate advance.');
    if(candidates.length>1)throw new Error(label+': more than one existing advance matches. Enter its Advance ID in the sheet.');
    if(advanceId&&!forceNew&&!candidates.length)throw new Error(label+': Advance ID '+advanceId+' was not found for '+emp.name+'.');
    const match=candidates[0],existing=match&&match.record;if(existing&&matched.has(existing.id))throw new Error(label+': the same existing advance appears twice in the sheet.');if(existing)matched.add(existing.id);
    const next={empId:emp.id,employeeName:emp.name,amount:round2(amount),date,account,recoveryStartMonth,note,reference},previous=existing?{amount:round2(num(existing.amount)),date:existing.date,account:existing.account||'',recoveryStartMonth:existing.recoveryStartMonth||'',note:existing.note||'',reference:existing.reference||''}:null;
    const different=!!previous&&Object.keys(previous).some(k=>String(previous[k])!==String(next[k]));
    const historicalCarry=/last\s+month.*carry|carried\s+forward/i.test(note),nonPaymentSource=/^(?:n\/?a|recharge|sir\s+psnl\s+a\/?c)$/i.test(account);
    changes.push(Object.assign({advanceId:existing&&existing.id||'',status:historicalCarry||nonPaymentSource?'Source row — review only':!existing?'New request':!different?'Unchanged':match.kind==='posted'||existing.status==='Posted'?'Needs individual correction':'Update existing request',previous,existingStatus:existing&&existing.status||''},next));
  });
  const hash=crypto.createHash('sha256').update(file.buffer).digest('hex');return {changes,hash,previewToken:crypto.createHash('sha256').update(hash+JSON.stringify(changes)).digest('hex')};
}
function applyAdvanceSheetRows(s,result,reason,req,fileName){
  if(result.changes.some(x=>x.status==='Source row — review only'))throw new Error('This sheet contains carried-forward advances or non-payment sources. Save the exact sheet for review; do not import these as new payments.');
  if(result.changes.some(x=>x.status==='Needs individual correction'))throw new Error('A posted advance differs from the sheet. Correct that advance individually before applying this sheet.');
  s.advanceRequests=s.advanceRequests||{};const ids=[],at=new Date().toISOString(),counts={created:0,updated:0,unchanged:0};
  result.changes.forEach(row=>{if(row.status==='Unchanged'){counts.unchanged++;return;}const fields={empId:row.empId,employeeName:row.employeeName,amount:row.amount,date:row.date,account:row.account,note:row.note,reference:row.reference,recoveryStartMonth:row.recoveryStartMonth};
    if(row.status==='Update existing request'){const request=s.advanceRequests[row.advanceId],before=JSON.parse(JSON.stringify(request));Object.assign(request,fields,{status:'Pending approval',updatedBy:req.user&&req.user.username||'admin',updatedAt:at,importReason:reason,importHash:result.hash});delete request.approvedBy;delete request.approvedAt;delete request.approvalNote;auditAdvanceRequest(s,req,'SHEET_UPDATED',request.id,{before,after:fields,fileName:path.basename(fileName||''),reason});ids.push(request.id);counts.updated++;return;}
    s.advanceRequestSeq=(s.advanceRequestSeq||0)+1;const id='ADVR-'+String(s.advanceRequestSeq).padStart(5,'0');s.advanceRequests[id]=Object.assign({id,status:'Pending approval',createdBy:req.user&&req.user.username||'admin',createdAt:at,importReason:reason,importHash:result.hash},fields);auditAdvanceRequest(s,req,'IMPORTED',id,{fileName:path.basename(fileName||''),reason,amount:row.amount});ids.push(id);counts.created++;
  });
  return {ids,counts};
}
function advanceSourceSheet(file){
  if(!file||!file.buffer||!/^\.(xlsx|xls|csv)$/i.test(path.extname(file.originalname||'')))throw new Error('Choose an Excel or CSV advances sheet.');
  let rows;try{const book=XLSX.read(file.buffer,{type:'buffer',cellDates:false});rows=XLSX.utils.sheet_to_json(book.Sheets[book.SheetNames[0]],{defval:'',raw:false});}catch{throw new Error('Could not read the advances sheet.');}
  if(!rows.length||rows.length>250)throw new Error('The sheet must contain 1 to 250 advances.');
  const norm=x=>String(x||'').trim().toLowerCase().replace(/[^a-z0-9]/g,''),get=(r,keys)=>{const key=Object.keys(r).find(k=>keys.includes(norm(k)));return key==null?'':String(r[key]??'').trim();};
  const items=rows.map((row,index)=>{const amountText=get(row,['advanceamount','amount']).replace(/[₹,\s]/g,''),amount=Number(amountText),dateText=get(row,['requestdate','proposeddate','date']),date=/^\d{4}[-/]\d{1,2}[-/]\d{1,2}$/.test(dateText)?dateText.replace(/\//g,'-'):dateText;
    if(!get(row,['employeename','employee','name'])||!amountText||!Number.isFinite(amount)||amount<=0||!/^\d{4}-\d{2}-\d{2}$/.test(date))throw new Error('Row '+(index+2)+': employee, positive amount and valid request date are required.');
    return {row:index+2,advanceId:get(row,['advanceid','requestid']),employeeId:get(row,['employeeid','empid']),employeeName:get(row,['employeename','employee','name']),amount:round2(amount),requestDate:date,payingAccount:get(row,['payingaccount','account']),recoveryStartMonth:get(row,['recoverystartmonth','recoverymonth']),note:get(row,['note','remarks','reason']),reference:get(row,['reference','ref'])};
  });
  return {hash:crypto.createHash('sha256').update(file.buffer).digest('hex'),items,total:round2(items.reduce((sum,item)=>sum+item.amount,0))};
}

// Owner-confirmed historical import. It is deliberately idempotent and only
// changes July 2026. Historical advances have no proof/account, so they remain
// payroll adjustments and never create bank/cash ledger transactions.
const JULY_2026_IMPORT = [
  ['ARSHPREET SINGH','MANAGER','PPAPPPPPPPAPPPPPAPPPPPPAPPPPPPP',2000],
  ['SUNNY SHARMA','SALES EXECUTIVE','PPPPAPPPPAAAPPPPPPPPPAPPPPPPAAA',17000],
  ['PARDEEP','EXECUTIVE','PPPPPPPPPPPPPAAAAAAAAAAAAHPPPPP',0],
  ['NANDANI','SALES EXECUTIVE','PPPPPAAPPPPPPPPPAPPPPPPAPPPPPPA',0],
  ['SHIVAM','SALES EXECUTIVE','PPPPPPPPPAPPPPPAPPPPAPPPPPAPPPP',1000],
  ['ISHA','SALES EXECUTIVE','PPPPPPPPPAPPPPPAAPPPPPPPPPPPAPP',100],
  ['HITESH','PHOTOGRAPHER','PPPPAAAPPPPAPPPPPPAPPAPPPAPPPPP',0],
  ['AJAY','VIDEO EDITOR','PPPPPPAPPPPAPPPAPPPAPPPPPAPPPPP',0],
  ['PIYUSH','TAILOR','PPPP AAPPPPAPPPPPPPPPP AAP PAPPPPP'.replace(/ /g,''),0],
  ['UMAIR','DRIVER','PPPPAPPPPPPAPPAPPPAAAHPPPPPPPPP',17000],
  ['TUSHAR','PACKING HELPER','PPPPAAPPPPPPPPPPPPPPPPPPPAPPPPP',0],
  ['NIDA','LOGISTIC','PPPPAPPPPPPAPPPPPPAAPPPPPAPPPPP',0],
  ['GAURAV','EDITOR','PPPPPPPPPPPAPPHPPAAPPAPPPAPPPPP',0],
  ['PRASHANT','ACCOUNTS','PPPPPPPHPPPAPHHPA PPPPPPPPAPPPPP'.replace(/ /g,''),7467],
  ['Suraj','Office Boy','PPP AAPPPPPPPPPAAAA PPPPPPPPPPPPP'.replace(/ /g,''),1500],
  ['TUSHAR','Model','PPPPPPPPPPPAPPPPPPAPPPPPAPPPPPP',0],
  ['Indervir','ACCOUNTS','PPPPAPAAPPPPPPPPPHAPPPPAPPPPPPP',3000],
  ['Pooja','Model','--------------------PPPPPAPPPPP',0,'2026-07-21'],
  ['Ravi','DRIVER','---------------------PPPPPPPPPA',6000,'2026-07-22']
];

const PROVIDED_ADVANCE_IMPORT = [
  ['2026-07-01','Isha','SALES EXECUTIVE',100],
  ['2026-07-01','ARSHPREET SINGH','MANAGER',2000],
  ['2026-07-01','Prashant','ACCOUNTS',7467],
  ['2026-07-01','Sunny','SALES EXECUTIVE',7000],
  ['2026-07-18','Suraj','Office Boy',500],
  ['2026-07-23','Indervir','ACCOUNTS',3000],
  ['2026-07-23','Suraj','Office Boy',500],
  ['2026-07-25','Ravi','DRIVER',2000],
  ['2026-07-26','Shivam','SALES EXECUTIVE',1000],
  ['2026-07-29','Suraj','Office Boy',500],
  ['2026-07-29','Umair','DRIVER',17000],
  ['2026-07-30','Ravi','DRIVER',4000],
  ['2026-07-30','Sunny','SALES EXECUTIVE',10000],
  ['2026-08-01','Sunny','SALES EXECUTIVE',2000],
  ['2026-08-01','ARSHPREET SINGH','MANAGER',50000],
  ['2026-08-03','Prashant','ACCOUNTS',10000],
  ['2026-08-07','Indervir','ACCOUNTS',1000],
  ['2026-08-07','Sunny','SALES EXECUTIVE',1000],
  ['2026-08-07','Ravi','DRIVER',500],
  ['2026-08-07','Guard','Security',3000],
  ['2026-08-09','TUSHAR','PACKING HELPER',2000],
  ['2026-08-09','Shivam','SALES EXECUTIVE',500]
];

// Owner-confirmed July payroll. Paid days and paid amounts are frozen to the
// supplied historical sheet; advances remain individually dated above.
const FINAL_JULY_2026_PAYROLL = [
  ['ARSHPREET SINGH','MANAGER',30000,30,5000,'2000 Last Month + 50000 This Month'],
  ['Guard','Security',15000,30,12000,''],
  ['SUNNY SHARMA','SALES EXECUTIVE',24000,22,0,'7000 Last Month'],
  ['PARDEEP','EXECUTIVE',19000,21.5,13500,''],
  ['NANDANI','SALES EXECUTIVE',17000,29,16500,''],
  ['SHIVAM','SALES EXECUTIVE',17000,30,15500,''],
  ['ISHA','SALES EXECUTIVE',18000,30,18000,''],
  ['HITESH','PHOTOGRAPHER',20000,27,18000,''],
  ['AJAY','VIDEO EDITOR',18000,29,17500,'Salary Delay'],
  ['PIYUSH','TAILOR',18500,28,17000,''],
  ['UMAIR','DRIVER',28000,27.5,9000,''],
  ['TUSHAR','PACKING HELPER',15000,31,13500,''],
  ['NIDA','LOGISTIC',25000,29,24000,''],
  ['GAURAV','EDITOR',18500,28.5,17500,''],
  ['PRASHANT','ACCOUNTS',26000,29.5,13000,'7467 Last Month + 10000 This Month'],
  ['Suraj','Office Boy',18000,25,13500,''],
  ['TUSHAR','Model',30000,30,30000,''],
  ['Indervir','ACCOUNTS',17000,28.5,12000,''],
  ['CSH','MARKETING',15000,30,15000,'Final: 30 paid days'],
  ['Ravi','DRIVER',26000,9,4000,''],
  ['Pooja','Model',20000,10,6500,'']
];

// Complete owner-confirmed August recovery. The `knownExisting` rows are
// matched to their live transactions; only the other four should be created
// on production. A fresh data set still receives the complete final history.
const FINAL_AUGUST_2026_ADVANCES = [
  ['2026-08-12','Ravi','DRIVER',500,false],
  ['2026-08-14','Suraj','Office Boy',500,false],
  ['2026-08-14','Shivam','SALES EXECUTIVE',500,false],
  ['2026-08-19','Ravi','DRIVER',6000,false],
  ['2026-08-26','ARSHPREET SINGH','MANAGER',2500,true],
  ['2026-08-27','Ravi','DRIVER',1000,true],
  ['2026-08-27','Suraj','Office Boy',1000,true]
];

function findImportedEmployee(s,name,post){
  const canonicalName=value=>{
    const normalized=String(value||'').replace(/\s*\([^)]*\)\s*/g,'').trim().toLowerCase();
    return ({pardeep:'pradeep',nandani:'nandini'})[normalized]||normalized;
  };
  const target=canonicalName(name);
  const sameName=Object.values(s.employees||{}).filter(e=>{const live=canonicalName(e.name);return live===target||live.startsWith(target+' ')||target.startsWith(live+' ');});
  return sameName.find(e=>String(e.post||'').localeCompare(post,'en',{sensitivity:'base'})===0)||sameName[0];
}
function nextEmployeeId(s){
  let n=Math.max(num(s.seq),...Object.keys(s.employees||{}).map(id=>num(String(id).replace(/^E/i,''))));let id;
  do{id='E'+String(++n).padStart(3,'0');}while(s.employees[id]);s.seq=n;return id;
}
function repairGuardSunnyCollision(s){
  const sunny=findImportedEmployee(s,'Sunny','SALES EXECUTIVE'),guard=findImportedEmployee(s,'Guard','Security');
  if(sunny||!guard)return false;
  const hasSunnyHistory=Object.values(s.advances||{}).some(a=>a.empId===guard.id&&/^sunny/i.test(String(a.employeeName||'')));
  if(!hasSunnyHistory)return false;
  s.employees[guard.id]=Object.assign({},guard,{name:'SUNNY SHARMA',post:'SALES EXECUTIVE',salary:24000,channel:'POS',note:'Restored after historical Guard import ID collision',active:true});
  return true;
}
function ensureHistoricalGuard(s,mo){
  let guard=findImportedEmployee(s,'Guard','Security');
  if(!guard){
    const id=nextEmployeeId(s);
    guard=s.employees[id]={id,name:'Guard',post:'Security',salary:15000,channel:'Shared',weekOffDay:'',joiningDate:'',lastWorkingDate:'',note:'Restored from supplied historical payroll advances',active:true,createdAt:new Date().toISOString()};
  }
  mo.rows[guard.id]=Object.assign({},mo.rows[guard.id],{paidDays:30,advance:0,paid:0,remarks:'Historical July payroll row restored for supplied Guard advance'});
  return guard;
}
function julyImportedMarks(emp,encoded){
  const attendance={},raw=String(encoded||''); let worked=0,presentDays=0;
  for(let i=0;i<31;i++){
    const source=raw[i]||'',day=String(i+1).padStart(2,'0');
    if(source==='-'||!source)continue;
    if(source==='P'){attendance[day]='P';worked++;presentDays++;continue;}
    if(source==='H'){attendance[day]='H';worked+=.5;continue;}
    if(source==='A'){
      const date='2026-07-'+day,weekday=WEEK_DAYS[new Date(date+'T00:00:00Z').getUTCDay()],isWeekOff=!!emp.weekOffDay&&emp.weekOffDay===weekday;
      attendance[day]=isWeekOff?'WO':'A';
    }
  }
  return {attendance,paidDays:Math.max(0,round2(worked+paidLeaveAllowanceForMonth(emp,'2026-07',presentDays)-1))};
}
function applyCorrectedJulyAttendanceV7(s){
  const key='corrected_july_attendance_fixed_leave_allowance_v10';s.oneTimeMigrations=s.oneTimeMigrations||{};
  if(s.oneTimeMigrations[key]||((s.payrollPostings||{})['2026-07'])||(((s.months||{})['2026-07']||{}).finalized))return false;
  const mo=ensureMonth(s,'2026-07'),updated=[];
  JULY_2026_IMPORT.forEach(([name,post,marks,,joiningDate])=>{
    const emp=findImportedEmployee(s,name,post);if(!emp)return;if(joiningDate)emp.joiningDate=joiningDate;else if(emp.joiningDate&&emp.joiningDate>'2026-07-01')emp.joiningDate='2026-07-01';if(emp.lastWorkingDate&&emp.lastWorkingDate<'2026-07-31')emp.lastWorkingDate='2026-07-31';emp.monthlyPaidLeaveAllowance=String(name).toLowerCase()==='suraj'?1:4;
    const calculated=julyImportedMarks(emp,marks);mo.attendance[emp.id]=calculated.attendance;mo.rows[emp.id]=Object.assign({},mo.rows[emp.id],{paidDays:calculated.paidDays,advance:0,paid:0,remarks:'Corrected July attendance · A/WO retained · fixed paid-leave allowance · 31-day adjustment -1'});updated.push({empId:emp.id,name:emp.name,paidDays:calculated.paidDays});
  });
  const now=new Date().toISOString(),advanceIds=[];s.advances=s.advances||{};s.advanceSeq=num(s.advanceSeq);
  PROVIDED_ADVANCE_IMPORT.forEach(([date,name,post,amount],index)=>{
    const emp=findImportedEmployee(s,name,post);if(!emp)return;const sourceKey='provided-advance-sheet-'+date+'-'+index;
    let advance=Object.values(s.advances).find(a=>a.sourceKey===sourceKey);
    if(!advance){s.advanceSeq++;const id='ADV-'+String(s.advanceSeq).padStart(5,'0');advance=s.advances[id]={id,createdAt:now,createdBy:'System migration'};}
    Object.assign(advance,{empId:emp.id,employeeName:emp.name,amount:round2(amount),date,account:'',proof:'',note:'Historical advance imported from supplied payroll sheet',reference:'HIST-'+date.replace(/-/g,'')+'-'+String(index+1).padStart(2,'0'),sourceKey,recoveryStartMonth:'2026-07',recoveries:[{ym:'2026-07',amount:round2(amount),by:'System migration',at:now}],active:true,historicalImport:true});advanceIds.push(advance.id);
  });
  s.oneTimeMigrations[key]={appliedAt:now,month:'2026-07',employees:updated,advanceIds,advanceTotal:round2(PROVIDED_ADVANCE_IMPORT.reduce((n,x)=>n+num(x[3]),0)),rules:{attendanceLabelsRetained:true,fixedMonthlyAllowance:true,partialMonthAllowanceLimited:true,calendar31Deduction:1,allProvidedAdvancesRecoveredInJuly:true}};
  return true;
}

function applyFinalJuly2026PayrollV13(s){
  const key='final_july_payroll_and_august_advances_v13';s.oneTimeMigrations=s.oneTimeMigrations||{};
  if(s.oneTimeMigrations[key])return false;
  const now=new Date().toISOString(),mo=ensureMonth(s,'2026-07'),updatedEmployees=[];
  mo.rows=mo.rows||{};

  FINAL_JULY_2026_PAYROLL.forEach(([name,post,salary,paidDays,paid,remarks])=>{
    const emp=findImportedEmployee(s,name,post);if(!emp)return;
    emp.salary=round2(salary);
    const row=mo.rows[emp.id]=Object.assign({},mo.rows[emp.id]);
    row.historicalPaidDays=round2(paidDays);
    row.paid=round2(paid);
    row.advance=0;
    if(remarks)row.remarks=remarks;
    updatedEmployees.push({empId:emp.id,name:emp.name,salary:emp.salary,paidDays:row.historicalPaidDays,paid:row.paid});
  });

  // July recovered only the advances listed in the confirmed July summary.
  // Rebuild those recoveries deterministically instead of adding another one.
  let julyRecoveryTotal=0;
  PROVIDED_ADVANCE_IMPORT.forEach(([date,name,post,amount],index)=>{
    const emp=findImportedEmployee(s,name,post),sourceKey='provided-advance-sheet-'+date+'-'+index;
    if(!emp)return;
    let advance=Object.values(s.advances||{}).find(a=>a.sourceKey===sourceKey);
    if(!advance){
      s.advanceSeq=num(s.advanceSeq)+1;const id='ADV-'+String(s.advanceSeq).padStart(5,'0');
      advance=s.advances[id]={id,createdAt:now,createdBy:'System migration'};
    }
    Object.assign(advance,{empId:emp.id,employeeName:emp.name,amount:round2(amount),date,account:'',proof:'',proofs:[],note:'Historical advance imported from supplied July payroll sheet',reference:'HIST-'+date.replace(/-/g,'')+'-'+String(index+1).padStart(2,'0'),sourceKey,recoveryStartMonth:'2026-07',recoveries:[{ym:'2026-07',amount:round2(amount),by:'System migration',at:now}],active:true,historicalImport:true});
    julyRecoveryTotal+=num(amount);
  });

  const augustAdvanceIds=[],createdAugustAdvanceIds=[];
  FINAL_AUGUST_2026_ADVANCES.forEach(([date,name,post,amount,knownExisting],index)=>{
    const emp=findImportedEmployee(s,name,post);if(!emp)return;
    const finalPayrollSourceKey='final-august-payroll-'+date+'-'+String(index+1).padStart(2,'0');
    let advance=Object.values(s.advances||{}).find(a=>a.finalPayrollSourceKey===finalPayrollSourceKey)||Object.values(s.advances||{}).find(a=>a.active!==false&&a.empId===emp.id&&String(a.date)===date&&Math.abs(num(a.amount)-num(amount))<.001);
    if(!advance){
      do{s.advanceSeq=num(s.advanceSeq)+1;}while(s.advances['ADV-'+String(s.advanceSeq).padStart(5,'0')]);
      const id='ADV-'+String(s.advanceSeq).padStart(5,'0');advance=s.advances[id]={id,empId:emp.id,employeeName:emp.name,amount:round2(amount),date,account:'',proof:'',proofs:[],note:'Historical August advance imported from confirmed payroll sheet',reference:'HIST-AUG-'+date.replace(/-/g,'')+'-'+String(index+1).padStart(2,'0'),sourceKey:'final-august-advance-sheet-'+date+'-'+String(index+1).padStart(2,'0'),active:true,historicalImport:true,createdAt:now,createdBy:'System migration'};createdAugustAdvanceIds.push(id);
    }
    advance.finalPayrollSourceKey=finalPayrollSourceKey;advance.recoveryStartMonth='2026-08';
    const otherRecoveries=(advance.recoveries||[]).filter(r=>r.ym!=='2026-08'),previouslyRecovered=round2(otherRecoveries.reduce((n,r)=>n+num(r.amount),0)),augustRecovery=round2(Math.max(0,num(advance.amount)-previouslyRecovered));
    advance.recoveries=otherRecoveries.concat(augustRecovery?[{ym:'2026-08',amount:augustRecovery,by:'System migration',at:now}]:[]);
    augustAdvanceIds.push(advance.id);
  });

  // A historical payroll posting records salary earned and the sheet's paid
  // amounts in employee ledgers only. No salaryPayments or cash entries exist.
  const postingRows=computeMonth(s,'2026-07').filter(r=>r.salaryAmt||r.advance||r.paid).map(r=>({empId:r.id,employeeName:r.name,salaryAmt:r.salaryAmt,advanceRecovery:r.loggedAdvanceRecovery,legacyAdvance:r.legacyAdvance,legacyPaid:r.legacyPaid,paid:r.paid,netPayable:r.netPayable}));
  s.payrollPostings=s.payrollPostings||{};
  const existingPosting=s.payrollPostings['2026-07']||{};
  s.payrollPostings['2026-07']=Object.assign({},existingPosting,{ym:'2026-07',rows:postingRows,postedAt:existingPosting.postedAt||now,postedBy:existingPosting.postedBy||'System migration',historicalImport:true,correctedAt:now});
  s.oneTimeMigrations[key]={appliedAt:now,month:'2026-07',updatedEmployees,julyRecoveryTotal:round2(julyRecoveryTotal),julyPaidTotal:round2(FINAL_JULY_2026_PAYROLL.reduce((n,x)=>n+num(x[4]),0)),augustAdvanceIds,createdAugustAdvanceIds,augustRecoveryTotal:round2(FINAL_AUGUST_2026_ADVANCES.reduce((n,x)=>n+num(x[3]),0)),rules:{signedPayrollCarry:true,noBankOrCashPosting:true,existingAugustAdvancesMatchedNotDuplicated:true,pendingRequestsUntouched:true}};
  return true;
}

// The ₹50,000 paid to Ashpreet on 1 August was imported with the July payroll
// sheet, but it was not recovered from salary. Earlier import code incorrectly
// created a same-value System migration recovery. Remove only that generated
// recovery; do not touch the separate ₹2,000 or ₹2,500 recovered advances.
function repairAshpreetOutstandingAdvanceV14(s){
  const key='ashpreet_50000_advance_outstanding_v14';s.oneTimeMigrations=s.oneTimeMigrations||{};
  if(s.oneTimeMigrations[key])return false;
  const advance=Object.values(s.advances||{}).find(a=>a.active!==false&&a.historicalImport&&num(a.amount)===50000&&String(a.date)==='2026-08-01'&&/^arshpreet/i.test(String(a.employeeName||'')));
  const now=new Date().toISOString();
  if(!advance){s.oneTimeMigrations[key]={appliedAt:now,result:'matching advance not found'};return true;}
  const before=(advance.recoveries||[]).map(x=>Object.assign({},x));
  advance.recoveries=before.filter(r=>!(String(r.by)==='System migration'&&String(r.ym)==='2026-07'&&num(r.amount)===50000));
  const removed=round2(before.reduce((n,r)=>n+num(r.amount),0)-advance.recoveries.reduce((n,r)=>n+num(r.amount),0));
  if(removed){s.advanceAudit=s.advanceAudit||[];s.advanceAudit.push({at:now,by:'System correction',action:'REMOVED_INCORRECT_RECOVERY',advanceId:advance.id,details:{amount:removed,reason:'Owner confirmed the ₹50,000 Ashpreet advance remains fully unrecovered.'}});}
  s.oneTimeMigrations[key]={appliedAt:now,advanceId:advance.id,removedRecovery:removed,outstandingAfter:advanceOutstanding(advance)};
  return true;
}

// Ashpreet earned ₹30,000 and was paid ₹5,000. The withheld ₹25,000 clears
// the older ₹2,000 advance first and then recovers ₹23,000 from this ₹50,000
// advance, leaving the owner-confirmed outstanding balance of ₹27,000.
function allocateAshpreetSalaryRecoveryV15(s){
  const key='ashpreet_50000_salary_recovery_23000_v15';s.oneTimeMigrations=s.oneTimeMigrations||{};
  if(s.oneTimeMigrations[key])return false;
  const advance=Object.values(s.advances||{}).find(a=>a.active!==false&&a.historicalImport&&num(a.amount)===50000&&String(a.date)==='2026-08-01'&&/^arshpreet/i.test(String(a.employeeName||'')));
  const now=new Date().toISOString();
  if(!advance){s.oneTimeMigrations[key]={appliedAt:now,result:'matching advance not found'};return true;}
  const before=(advance.recoveries||[]).map(x=>Object.assign({},x));
  advance.recoveries=before.filter(r=>String(r.by)!=='System salary allocation').concat([{ym:'2026-07',amount:23000,by:'System salary allocation',at:now,note:'₹25,000 salary withheld: ₹2,000 older advance + ₹23,000 of this advance'}]);
  s.advanceAudit=s.advanceAudit||[];s.advanceAudit.push({at:now,by:'System correction',action:'ALLOCATED_WITHHELD_SALARY',advanceId:advance.id,details:{salaryEarned:30000,salaryPaid:5000,totalWithheld:25000,olderAdvanceRecovery:2000,thisAdvanceRecovery:23000,outstandingAfter:advanceOutstanding(advance),reason:'Owner confirmed the remaining advance balance is ₹27,000.'}});
  s.oneTimeMigrations[key]={appliedAt:now,advanceId:advance.id,recoveryAllocated:23000,outstandingAfter:advanceOutstanding(advance)};
  return true;
}

function normalizedAbsentMark(e,ym,day){
  const date=ym+'-'+String(day).padStart(2,'0'),weekday=WEEK_DAYS[new Date(date+'T00:00:00Z').getUTCDay()];
  return e&&e.weekOffDay===weekday?'WO':'A';
}
function applyStoredLeaveAllowancesV11(s){
  const key='normalize_legacy_paid_leave_marks_v11';s.oneTimeMigrations=s.oneTimeMigrations||{};if(s.oneTimeMigrations[key])return false;
  let converted=0,recalculated=0;
  Object.entries(s.months||{}).forEach(([ym,mo])=>{
    mo.attendance=mo.attendance||{};mo.rows=mo.rows||{};
    Object.entries(mo.attendance).forEach(([empId,att])=>{
      const emp=s.employees[empId];if(!emp)return;
      Object.keys(att||{}).forEach(day=>{if(att[day]==='PL'){att[day]=normalizedAbsentMark(emp,ym,day);converted++;}});
      if(!mo.finalized&&!((s.payrollPostings||{})[ym])){const calculated=attPaidDays(employmentAttendance(att,emp,ym),emp,ym);if(calculated!=null){mo.rows[empId]=Object.assign({},mo.rows[empId],{paidDays:calculated});recalculated++;}}
    });
  });
  s.oneTimeMigrations[key]={appliedAt:new Date().toISOString(),converted,recalculated,rule:'PL removed; stored monthly allowance controls compensation; zero means every absence deducts salary'};return true;
}
function applySunnyGuardAndSurajRepair(s){
  const key='sunny_guard_suraj_payroll_repair_v6';s.oneTimeMigrations=s.oneTimeMigrations||{};
  if(s.oneTimeMigrations[key])return false;
  const sunny=findImportedEmployee(s,'Sunny Sharma','SALES EXECUTIVE')||findImportedEmployee(s,'Sunny','SALES EXECUTIVE');
  const guard=findImportedEmployee(s,'Guard','Security');
  const suraj=findImportedEmployee(s,'Suraj','Office Boy');
  if(!sunny||!guard||!suraj)return false;
  Object.assign(sunny,{name:'SUNNY SHARMA',post:'SALES EXECUTIVE',salary:24000,channel:'POS',active:true});
  Object.assign(guard,{name:'Guard',post:'Security',salary:15000,channel:'Shared',active:true});
  suraj.monthlyPaidLeaveAllowance=1;
  Object.values(s.advances||{}).filter(a=>a.historicalImport).forEach(a=>{
    const n=String(a.employeeName||'').toLowerCase();
    if(n.startsWith('sunny')){a.empId=sunny.id;a.employeeName=sunny.name;}
    if(n==='guard'){a.empId=guard.id;a.employeeName=guard.name;}
  });
  const mo=ensureMonth(s,'2026-07'),sunnyImport=JULY_2026_IMPORT.find(x=>x[0]==='SUNNY SHARMA'),surajImport=JULY_2026_IMPORT.find(x=>String(x[0]).toLowerCase()==='suraj');
  if(sunnyImport){const calculated=julyImportedMarks(sunny,sunnyImport[2]);mo.attendance[sunny.id]=calculated.attendance;mo.rows[sunny.id]=Object.assign({},mo.rows[sunny.id],{paidDays:calculated.paidDays,advance:0,paid:0,remarks:'July 2026 attendance import · first 4 ordinary absences paid · 31-day adjustment -1'});}
  if(surajImport){const calculated=julyImportedMarks(suraj,surajImport[2]);mo.attendance[suraj.id]=calculated.attendance;mo.rows[suraj.id]=Object.assign({},mo.rows[suraj.id],{paidDays:calculated.paidDays,advance:0,paid:0,remarks:'July 2026 attendance import · 1 paid leave monthly · 31-day adjustment -1'});}
  mo.rows[guard.id]=Object.assign({},mo.rows[guard.id],{paidDays:30,advance:0,paid:0,remarks:'Historical July payroll row restored for supplied Guard advance'});
  s.oneTimeMigrations[key]={appliedAt:new Date().toISOString(),sunnyEmployeeId:sunny.id,guardEmployeeId:guard.id,surajEmployeeId:suraj.id,surajMonthlyPaidLeaveAllowance:1};
  return true;
}
function applyJuly2026AttendanceAndPayroll(s){
  const key='july_2026_attendance_payroll_v5';s.oneTimeMigrations=s.oneTimeMigrations||{};
  if(s.oneTimeMigrations[key]||((s.payrollPostings||{})['2026-07'])||(((s.months||{})['2026-07']||{}).finalized))return false;
  const mo=ensureMonth(s,'2026-07');mo.attendance=mo.attendance||{};mo.rows=mo.rows||{};
  const repairedGuardSunnyCollision=repairGuardSunnyCollision(s);
  const guard=ensureHistoricalGuard(s,mo);
  const imported=[];
  for(const [name,post,marks,advance,joiningDate] of JULY_2026_IMPORT){
    const emp=findImportedEmployee(s,name,post);if(!emp)continue;
    if(joiningDate)emp.joiningDate=joiningDate;
    const calculated=julyImportedMarks(emp,marks);
    mo.attendance[emp.id]=calculated.attendance;
    mo.rows[emp.id]=Object.assign({},mo.rows[emp.id],{paidDays:calculated.paidDays,advance:0,paid:0,remarks:'July 2026 attendance import · first 4 offs paid · 31-day adjustment -1'});
    imported.push({empId:emp.id,name:emp.name,paidDays:calculated.paidDays});
  }
  // Replace old summary-only estimates with individually dated advance records.
  for(const [id,row] of Object.entries(mo.rows))if(!imported.some(x=>x.empId===id)){row.advance=0;row.paid=0;}
  s.advances=s.advances||{};s.advanceSeq=num(s.advanceSeq);const advanceIds=[];
  PROVIDED_ADVANCE_IMPORT.forEach(([date,name,post,amount],index)=>{
    const emp=findImportedEmployee(s,name,post),sourceKey='provided-advance-sheet-'+date+'-'+index;
    if(!emp||Object.values(s.advances).some(a=>a.sourceKey===sourceKey))return;
    s.advanceSeq++;const id='ADV-'+String(s.advanceSeq).padStart(5,'0'),now=new Date().toISOString();
    s.advances[id]={id,empId:emp.id,employeeName:emp.name,amount:round2(amount),date,account:'',proof:'',note:'Historical advance imported from supplied payroll sheet',reference:'HIST-'+date.replace(/-/g,'')+'-'+String(index+1).padStart(2,'0'),sourceKey,recoveryStartMonth:'2026-07',recoveries:[{ym:'2026-07',amount:round2(amount),by:'System migration',at:now}],active:true,historicalImport:true,createdBy:'System migration',createdAt:now};
    advanceIds.push(id);
  });
  Object.values(s.advances).filter(a=>a.historicalImport&&String(a.employeeName||'').toLowerCase()==='guard').forEach(a=>{a.empId=guard.id;a.employeeName=guard.name;});
  const advanceTotal=round2(PROVIDED_ADVANCE_IMPORT.reduce((n,x)=>n+num(x[3]),0));
  s.oneTimeMigrations[key]={appliedAt:new Date().toISOString(),month:'2026-07',employees:imported,guardEmployeeId:guard.id,repairedGuardSunnyCollision,advanceIds,advanceTotal,rules:{firstOffsPaid:4,calendar31Deduction:1,allProvidedAdvancesRecoveredInJuly:true}};
  return true;
}

// ── Access: salary is sensitive → admin or accounting only ──
function rolesOf(req) { return (req.user && (req.user.roles || (req.user.role ? [req.user.role] : []))) || []; }
function advanceUsername(req) { return String(req.user && req.user.username || '').trim().toLowerCase(); }
function canRequestOrPostAdvance(req) { return rolesOf(req).includes('admin') || rolesOf(req).includes('owner') || advanceUsername(req) === 'prashant'; }
function canApproveAdvance(req) { return rolesOf(req).includes('owner') || advanceUsername(req) === 'prashant'; }
function isOwner(req) { return rolesOf(req).includes('owner'); }
function canReviewAugustSalary(req) { return canApproveAdvance(req); }
function guard(req, res, next) {
  if(advanceUsername(req)==='prashant')return next();
  const r = rolesOf(req);
  if (r.includes('admin') || r.includes('accounting') || r.includes('owner')) return next();
  return res.status(403).json({ success: false, error: 'Salary is admin/accounting only.' });
}

function daysInMonth(ym) { const p = String(ym).split('-').map(Number); return new Date(p[0], p[1], 0).getDate(); }
function attPaidDays(att,e,ym) {
  if (!att) return null;
  let worked=0,presentDays=0,any=false;
  Object.keys(att).forEach(d=>{const mark=att[d];if(MARKS[mark]!=null){any=true;if(mark==='P'){worked++;presentDays++;}else if(mark==='H')worked+=.5;}});
  if(!any)return null;
  const grossPaidDays=round2(worked+paidLeaveAllowanceForMonth(e,ym,presentDays));
  // The supplied attendance sheet subtracts one day in a 31-day month after
  // paid offs are earned. Preserve a genuine single-day new joiner.
  const calendarAdjustment=daysInMonth(ym)===31&&grossPaidDays>1?1:0;
  return Math.max(0,round2(grossPaidDays-calendarAdjustment));
}
function employmentAttendance(att, e, ym) {
  if (!att) return att;
  const filtered={}; Object.keys(att).forEach(day=>{const date=ym+'-'+String(day).padStart(2,'0');if(e.joiningDate&&date<e.joiningDate)return;if(e.lastWorkingDate&&date>e.lastWorkingDate)return;filtered[day]=att[day];}); return filtered;
}
function employeeInPayrollMonth(e,ym){
  const joinMonth=String(e.joiningDate||'').slice(0,7),leaveMonth=String(e.lastWorkingDate||'').slice(0,7);
  return !(joinMonth&&ym<joinMonth)&&!(leaveMonth&&ym>leaveMonth);
}
function paidLeaveAllowanceForMonth(e,ym,presentDays){
  const base=e.monthlyPaidLeaveAllowance==null?4:Math.max(0,num(e.monthlyPaidLeaveAllowance)),present=Math.max(0,num(presentDays));
  if(!base||!employeeInPayrollMonth(e,ym))return 0;
  // Final payroll-sheet rule: paid leave is earned from full present days.
  // 0–5: 0, 6–8: 1, 9–11: 1.5, 12–14: 2, 15–17: 2.5,
  // 18–20: 3, 21–23: 3.5, and 24+: 4. An employee-specific
  // allowance remains a cap (for example, Suraj is capped at one day).
  const earned=present<=5?0:present<=8?1:present<=11?1.5:present<=14?2:present<=17?2.5:present<=20?3:present<=23?3.5:4;
  return Math.min(base,earned);
}
function ensureMonth(s, ym) { if (!s.months[ym]) s.months[ym] = { finalized: false, rows: {}, attendance: {} }; return s.months[ym]; }
function advanceRecovered(a) { return round2((a.recoveries || []).reduce((n, x) => n + num(x.amount), 0)); }
function advanceOutstanding(a) { return round2(Math.max(0, num(a.amount) - advanceRecovered(a))); }
function advanceStatus(a) { const r = advanceRecovered(a); return r <= 0 ? 'Outstanding' : (r + .001 >= num(a.amount) ? 'Recovered' : 'Partially recovered'); }
function advanceView(a,s) {
  let remaining=round2(num(a.amount));
  const recoveries=(a.recoveries||[]).map((r,index)=>Object.assign({ _index:index },r)).sort((x,y)=>String((x.ym||'')+(x.at||'')+x._index).localeCompare(String((y.ym||'')+(y.at||'')+y._index))).map(r=>{
    remaining=round2(Math.max(0,remaining-num(r.amount)));
    const recordedOn=String(r.recordedOn||r.at||'').slice(0,10);
    const payrollMonth=r.payrollMonth||r.ym||'',salaryPayment=s&&(s.salaryPayments||[]).filter(p=>p.active!==false&&p.empId===a.empId&&p.ym===payrollMonth&&p.date).sort((x,y)=>String(x.date).localeCompare(String(y.date)))[0];
    const view=Object.assign({},r,{payrollMonth,recordedOn,deductionDate:r.deductionDate||(salaryPayment&&salaryPayment.date)||'',deductedFrom:payrollMonth?payrollMonth+' salary':'Salary deduction',salaryPaymentReference:salaryPayment&&(salaryPayment.reference||salaryPayment.batchId||salaryPayment.id)||'',reference:r.reference||('SALARY-RECOVERY-'+String(r.ym||'UNKNOWN')),remainingAfter:remaining});
    delete view._index;return view;
  });
  return Object.assign({}, a, { recoveries, recovered: advanceRecovered(a), outstanding: advanceOutstanding(a), status: a.active === false ? 'Cancelled' : advanceStatus(a) });
}
function monthRecovery(s, empId, ym) { return round2(Object.values(s.advances || {}).filter(a => a.active !== false && a.empId === empId).reduce((n, a) => n + (a.recoveries || []).filter(r => r.ym === ym).reduce((m, r) => m + num(r.amount), 0), 0)); }
function auditAdvance(s, req, action, advanceId, details) { s.advanceAudit = s.advanceAudit || []; s.advanceAudit.push({ at: new Date().toISOString(), by: req.user && req.user.username || 'admin', action, advanceId, details: details || {} }); }
function auditAdvanceRequest(s, req, action, requestId, details) { s.advanceRequestAudit = s.advanceRequestAudit || []; s.advanceRequestAudit.push({ at:new Date().toISOString(), by:req.user&&req.user.username||'system', action, requestId, details:details||{} }); }

// Compute a month's payroll rows for every employee.
function employeeMonthBase(s,e,ym){
  const mo = s.months[ym] || { rows: {}, attendance: {} };
  const div = num(s.divisor) || 30;
  const row=(mo.rows||{})[e.id]||{},computed=attPaidDays(employmentAttendance((mo.attendance||{})[e.id],e,ym),e,ym),historicalPaidDays=row.historicalPaidDays!=null?num(row.historicalPaidDays):null,paidDays=row.sheetPaidDaysOverride!=null?num(row.sheetPaidDaysOverride):(historicalPaidDays!=null?historicalPaidDays:(computed!=null?computed:(row.paidDays!=null?num(row.paidDays):null)));
  const monthlySalary=row.sheetMonthlySalaryOverride==null?salaryForMonth(e,ym):round2(num(row.sheetMonthlySalaryOverride)),calculatedSalaryAmt=round2(paidDays!=null?(monthlySalary/div*paidDays):0),salaryAdjustment=row.salaryAdjustment==null?null:round2(num(row.salaryAdjustment)),salaryAmt=salaryAdjustment==null?(row.finalSalaryAmount==null?calculatedSalaryAmt:round2(num(row.finalSalaryAmount))):round2(calculatedSalaryAmt+salaryAdjustment),legacyAdvance=num(row.advance),loggedAdvanceRecovery=monthRecovery(s,e.id,ym),currentAdvance=round2(legacyAdvance+loggedAdvanceRecovery),historicalCloseAdjustment=num(row.historicalCloseAdjustment);
  const legacyPaid=num(row.paid),transactionPaid=round2((s.salaryPayments||[]).filter(p=>p.empId===e.id&&p.ym===ym&&p.active!==false).reduce((n,p)=>n+num(p.amount),0)),paid=round2(legacyPaid+transactionPaid);
  return {row,computed,historicalPaidDays,paidDays,monthlySalary,calculatedSalaryAmt,salaryAdjustment,salaryAmt,legacyAdvance,loggedAdvanceRecovery,currentAdvance,historicalCloseAdjustment,legacyPaid,transactionPaid,paid};
}
function payrollBalanceCarryIn(s,e,ym){
  let carry=0;
  Object.keys(s.months||{}).filter(m=>m<ym).sort().forEach(m=>{
    if(!employeeInPayrollMonth(e,m))return;
    const x=employeeMonthBase(s,e,m),monthRow=(s.months[m]&&s.months[m].rows||{})[e.id]||{},opening=monthRow.sheetOpeningCarryOverride==null?carry:num(monthRow.sheetOpeningCarryOverride),net=x.salaryAmt-x.currentAdvance+opening+x.historicalCloseAdjustment,balance=net-x.paid;
    carry=round2(balance);
  });
  return carry;
}
function computeMonth(s, ym) {
  return Object.values(s.employees).filter(e=>((employeeInPayrollMonth(e,ym)||Math.abs(payrollBalanceCarryIn(s,e,ym))>=.005)&&!((s.months[ym]&&s.months[ym].rows||{})[e.id]||{}).sheetExcluded)).sort(byEmployeeName).map(e => {
    const x=employeeMonthBase(s,e,ym),openingBalanceCarry=x.row.sheetOpeningCarryOverride==null?payrollBalanceCarryIn(s,e,ym):round2(num(x.row.sheetOpeningCarryOverride)),openingAdvanceCarry=round2(Math.max(0,-openingBalanceCarry)),openingPayableCarry=round2(Math.max(0,openingBalanceCarry)),advance=round2(x.currentAdvance+openingAdvanceCarry),netPayable=x.salaryAmt-x.currentAdvance+openingBalanceCarry+x.historicalCloseAdjustment;
    return {
      id: e.id, name: e.name, post: e.post, channel: e.channel, weekOffDay: e.weekOffDay || '', joiningDate:e.joiningDate||'', lastWorkingDate:e.lastWorkingDate||'', active: e.active !== false,
      salary: x.monthlySalary, paidDays:x.paidDays, computedPaidDays:x.computed, historicalPaidDays:x.historicalPaidDays, sheetPaidDaysOverride:x.row.sheetPaidDaysOverride==null?null:num(x.row.sheetPaidDaysOverride),sheetPaidDaysReason:x.row.sheetPaidDaysReason||'',
      salaryAmt: round2(x.salaryAmt), calculatedSalaryAmt:x.calculatedSalaryAmt, salaryAdjustment:x.salaryAdjustment==null?round2(x.salaryAmt-x.calculatedSalaryAmt):x.salaryAdjustment, finalSalaryAmount:x.row.finalSalaryAmount==null?null:round2(num(x.row.finalSalaryAmount)), finalSalaryReason:x.row.finalSalaryReason||'', finalSalaryEditedAt:x.row.finalSalaryEditedAt||'', finalSalaryEditedBy:x.row.finalSalaryEditedBy||'', advance, currentAdvance:x.currentAdvance, openingBalanceCarry, openingAdvanceCarry, openingPayableCarry, legacyAdvance:x.legacyAdvance, loggedAdvanceRecovery:x.loggedAdvanceRecovery, historicalCloseAdjustment:x.historicalCloseAdjustment, netPayable: round2(netPayable),
      deductionAdjustment:round2(openingBalanceCarry-x.currentAdvance+x.historicalCloseAdjustment), adjustmentDetails:[
        ...(x.loggedAdvanceRecovery?[{kind:'advance_recovery',amount:-x.loggedAdvanceRecovery,description:'Salary advance recovered in '+ym}]:[]),
        ...(x.legacyAdvance?[{kind:'historical_deduction',amount:-x.legacyAdvance,description:'Historical salary deduction recorded for '+ym}]:[]),
        ...(openingAdvanceCarry?[{kind:'previous_overpayment',amount:-openingAdvanceCarry,description:'Extra salary paid earlier; carried forward as a deduction'}]:[]),
        ...(openingPayableCarry?[{kind:'previous_payable',amount:openingPayableCarry,description:'Salary left unpaid earlier; added this month'}]:[]),
        ...(x.historicalCloseAdjustment?[{kind:'historical_payroll_unverified',amount:x.historicalCloseAdjustment,description:'Unverified historical offset — not evidence of payment. Owner must review actual 3645/cash payments.'}]:[])
      ],
      outstandingAdvance: round2(Object.values(s.advances || {}).filter(a => a.active !== false && a.empId === e.id).reduce((n, a) => n + advanceOutstanding(a), 0)),
      advanceDueBeforeDeduction:round2(Object.values(s.advances||{}).filter(a=>a.active!==false&&a.empId===e.id&&String(a.recoveryStartMonth||a.date||'').slice(0,7)<=ym).reduce((n,a)=>n+advanceOutstanding(a)+(a.recoveries||[]).filter(r=>r.ym===ym).reduce((m,r)=>m+num(r.amount),0),0)),
      paid:x.paid, legacyPaid:x.legacyPaid, transactionPaid:x.transactionPaid, balance:round2(netPayable-x.paid), carryForwardAdvance:round2(Math.max(0,-(netPayable-x.paid))),carryForwardPayable:round2(Math.max(0,netPayable-x.paid)),remarks:x.row.remarks||''
    };
  });
}

// ── P&L feed: earned salary (Salary Amt) by channel for a date range ──
// A month contributes its salary pro-rated by how many of its calendar days fall
// inside [from,to] (full month = 100%). Shared is returned separately so pl.js can
// split it by revenue share, exactly like other shared fixed costs.
function summaryForPL(from, to) {
  const s = load();
  const buckets = { POS: 0, Website: 0, Shared: 0 };
  Object.keys(s.months || {}).forEach(ym => {
    const dim = daysInMonth(ym);
    const mStart = ym + '-01', mEnd = ym + '-' + String(dim).padStart(2, '0');
    const lo = (from && from > mStart) ? from : mStart;
    const hi = (to && to < mEnd) ? to : mEnd;
    if (lo > hi) return;
    const overlapDays = (Date.parse(hi) - Date.parse(lo)) / 86400000 + 1;
    const frac = Math.max(0, Math.min(1, overlapDays / dim));
    if (frac <= 0) return;
    computeMonth(s, ym).forEach(r => {
      const ch = CHANNELS.includes(r.channel) ? r.channel : 'Shared';
      buckets[ch] += r.salaryAmt * frac;
    });
  });
  return { POS: round0(buckets.POS), Website: round0(buckets.Website), Shared: round0(buckets.Shared) };
}

// Every salary API runs inside an entity-local storage context. Existing salary.json
// remains SANKI; SAMAST starts independently in salary-samast.json.
router.use('/api/salary',(req,res,next)=>salaryContext.run({entity:salaryEntity(req.query&&req.query.entity)},next));

// ── Employee master ──
router.get('/api/salary/employees', guard, (req, res) => {
  const s = load();
  const currentMonth=new Date().toISOString().slice(0,7),employees=Object.values(s.employees).sort(byEmployeeName).map(e=>Object.assign({},e,{salaryHistory:salaryHistoryOf(e),effectiveSalary:salaryForMonth(e,currentMonth)}));
  res.json({ success: true, entity:activeSalaryEntity(), employees, currentMonth, divisor: num(s.divisor) || 30, channels: CHANNELS, weekDays: WEEK_DAYS, salaryPayingAccounts:SALARY_PAYING_ACCOUNTS });
});
router.post('/api/salary/employees', guard, (req, res) => {
  const s = load(); const b = req.body || {};
  let id = b.id;
  if(id&&!s.employees[id])return res.status(404).json({success:false,error:'Employee record not found. Reload the page and try again.'});
  const requestedName=String(b.name||'').trim(),duplicate=!id&&Object.values(s.employees).find(e=>e.active!==false&&String(e.name||'').trim().localeCompare(requestedName,'en',{sensitivity:'base'})===0);
  if(duplicate)return res.status(409).json({success:false,duplicate:true,existingEmployee:{id:duplicate.id,name:duplicate.name,post:duplicate.post},error:duplicate.name+' already exists. Edit or reactivate the existing employee instead.'});
  if (!id) id=nextEmployeeId(s);
  const cur = s.employees[id] || {};
  if(cur.id&&b.salary!==undefined&&round2(num(b.salary))!==round2(num(cur.salary)))return res.status(400).json({success:false,error:'Use Salary increments to change an existing employee salary so previous payroll months remain unchanged.'});
  const joiningDate=b.joiningDate!==undefined?String(b.joiningDate||'').slice(0,10):(cur.joiningDate||''),lastWorkingDate=b.lastWorkingDate!==undefined?String(b.lastWorkingDate||'').slice(0,10):(cur.lastWorkingDate||'');
  if((joiningDate&&!/^\d{4}-\d{2}-\d{2}$/.test(joiningDate))||(lastWorkingDate&&!/^\d{4}-\d{2}-\d{2}$/.test(lastWorkingDate)))return res.status(400).json({success:false,error:'Use valid joining and last-working dates.'});
  if(joiningDate&&lastWorkingDate&&lastWorkingDate<joiningDate)return res.status(400).json({success:false,error:'Last working date cannot be before the joining date.'});
  s.employees[id] = {
    id,
    name: String(b.name != null ? b.name : cur.name || '').trim(),
    post: String(b.post != null ? b.post : cur.post || '').trim(),
    salary: b.salary != null ? num(b.salary) : num(cur.salary),
    baseSalary: cur.baseSalary != null ? num(cur.baseSalary) : (b.salary != null ? num(b.salary) : num(cur.salary)),
    salaryHistory: salaryHistoryOf(cur),
    channel: CHANNELS.includes(b.channel) ? b.channel : (cur.channel || 'Shared'),
    weekOffDay: b.weekOffDay !== undefined ? (WEEK_DAYS.includes(b.weekOffDay) ? b.weekOffDay : '') : (cur.weekOffDay || ''),
    monthlyPaidLeaveAllowance: b.monthlyPaidLeaveAllowance !== undefined ? Math.max(0,num(b.monthlyPaidLeaveAllowance)) : (cur.monthlyPaidLeaveAllowance == null ? 4 : Math.max(0,num(cur.monthlyPaidLeaveAllowance))),
    joiningDate, lastWorkingDate,
    note: b.note != null ? String(b.note) : (cur.note || ''),
    active: b.active != null ? !!b.active : (cur.active !== false),
    createdAt: cur.createdAt || new Date().toISOString()
  };
  save(s);
  res.json({ success: true, employee: s.employees[id] });
});
router.post('/api/salary/increments', guard, (req,res)=>{
  const s=load(),b=req.body||{},emp=s.employees[b.empId],effectiveMonth=String(b.effectiveMonth||''),updatedSalary=round2(num(b.updatedSalary));
  if(!emp)return res.status(400).json({success:false,error:'Select an employee.'});
  if(!/^\d{4}-\d{2}$/.test(effectiveMonth)||Number(effectiveMonth.slice(5,7))<1||Number(effectiveMonth.slice(5,7))>12)return res.status(400).json({success:false,error:'Select a valid effective salary month.'});
  if(!(updatedSalary>0))return res.status(400).json({success:false,error:'Enter a valid updated monthly salary.'});
  const history=salaryHistoryOf(emp);
  if(history.some(x=>x.effectiveMonth===effectiveMonth))return res.status(409).json({success:false,error:'A salary change already exists for this employee in '+effectiveMonth+'.'});
  const previousSalary=salaryForMonth(emp,effectiveMonth);
  if(updatedSalary<=previousSalary)return res.status(400).json({success:false,error:'The incremented salary must be greater than the ₹'+previousSalary+' salary effective for that month.'});
  if(emp.baseSalary==null)emp.baseSalary=num(emp.salary);
  const at=new Date().toISOString(),entry={id:'INC-'+String(Date.now())+'-'+emp.id,effectiveMonth,previousSalary,updatedSalary,increase:round2(updatedSalary-previousSalary),recordedAt:at,recordedBy:req.user&&req.user.username||'admin'};
  emp.salaryHistory=history.concat(entry).sort((a,b)=>String(a.effectiveMonth).localeCompare(String(b.effectiveMonth))||String(a.recordedAt).localeCompare(String(b.recordedAt)));
  emp.salary=emp.salaryHistory.reduce((latest,x)=>num(x.updatedSalary),num(emp.baseSalary));
  save(s);res.json({success:true,employee:Object.assign({},emp,{effectiveSalary:salaryForMonth(emp,new Date().toISOString().slice(0,7))}),increment:entry});
});
router.delete('/api/salary/employees/:id', guard, (req, res) => {
  const s = load();
  if (s.employees[req.params.id]) { s.employees[req.params.id].active = false; save(s); }
  res.json({ success: true });
});

router.post('/api/salary/post/:ym', guard, (req,res)=>{
  const s=load(),ym=req.params.ym;if(!/^\d{4}-\d{2}$/.test(ym))return res.status(400).json({success:false,error:'Invalid payroll month.'});
  s.payrollPostings=s.payrollPostings||{};if(s.payrollPostings[ym])return res.status(409).json({success:false,error:'This month is already posted to salary ledgers.'});
  const rows=computeMonth(s,ym).filter(r=>r.salaryAmt||r.advance||r.paid).map(r=>({empId:r.id,employeeName:r.name,salaryAmt:r.salaryAmt,advanceRecovery:r.loggedAdvanceRecovery,legacyAdvance:r.legacyAdvance,legacyPaid:r.legacyPaid,paid:r.paid,netPayable:r.netPayable}));
  s.payrollPostings[ym]={ym,rows,postedAt:new Date().toISOString(),postedBy:req.user&&req.user.username||'admin'};save(s);res.json({success:true,posting:s.payrollPostings[ym]});
});
router.post('/api/salary/payments/batch',guard,(req,res)=>{
  const s=load(),b=req.body||{},ym=String(b.ym||''),date=String(b.date||''),account=String(b.account||'').trim(),proofs=Array.from(new Set([].concat(Array.isArray(b.proofs)?b.proofs:[],b.proof||[]).map(x=>String(x||'').trim()).filter(Boolean))),proof=proofs[0]||'',items=Array.isArray(b.items)?b.items:[];
  const allowedAccounts=SALARY_PAYING_ACCOUNTS;
  if(!/^\d{4}-\d{2}$/.test(ym)||!/^\d{4}-\d{2}-\d{2}$/.test(date)||!allowedAccounts.includes(account)||!proofs.length||!items.length)return res.status(400).json({success:false,error:'Choose employees, date, an authorized salary paying account or cash source, and at least one payment proof.'});
  const rows=computeMonth(s,ym),seen=new Set(),prepared=[];for(const x of items){const row=rows.find(r=>r.id===x.empId),amount=round2(num(x.amount)),remaining=round2(Math.max(0,row&&row.balance||0)),modificationReason=String(x.modificationReason||'').trim();if(!row)return res.status(400).json({success:false,error:'One selected employee was not found. Refresh the salary page before retrying.'});if(seen.has(x.empId))return res.status(400).json({success:false,error:row.name+' was selected more than once.'});if(!(amount>0))return res.status(400).json({success:false,error:row.name+': Pay Now must be greater than ₹0.'});if(!(remaining>0))return res.status(400).json({success:false,error:row.name+': no salary is payable on this row. Review the salary amount before recording a payment.',empId:row.id,payNow:amount,remaining});if(Math.abs(amount-remaining)>.001&&!modificationReason)return res.status(400).json({success:false,error:'Enter why '+row.name+' is being paid '+amount+' instead of the full balance '+remaining+'.'});seen.add(x.empId);prepared.push({row,amount,modificationReason,remainingBeforePayment:remaining});}
  s.salaryPaymentBatchSeq=(s.salaryPaymentBatchSeq||0)+1;const batchId='SALB-'+String(s.salaryPaymentBatchSeq).padStart(5,'0'),now=new Date().toISOString();s.salaryPayments=s.salaryPayments||[];prepared.forEach((x,i)=>s.salaryPayments.push({id:batchId+'-'+String(i+1).padStart(3,'0'),batchId,ym,empId:x.row.id,employeeName:x.row.name,amount:x.amount,date,account,proof,proofs:proofs.slice(),reference:String(b.reference||'').trim(),note:String(b.note||'').trim(),modificationReason:x.modificationReason,remainingBeforePayment:x.remainingBeforePayment,balanceAfterPayment:round2(x.remainingBeforePayment-x.amount),active:true,createdBy:req.user&&req.user.username||'admin',createdAt:now}));save(s);res.json({success:true,batchId,count:prepared.length,total:round2(prepared.reduce((n,x)=>n+x.amount,0)),proofCount:proofs.length});
});
router.post('/api/salary/payments/:id/proofs',guard,(req,res)=>{
  const s=load(),p=(s.salaryPayments||[]).find(x=>x.id===req.params.id&&x.active!==false),proofs=Array.isArray((req.body||{}).proofs)?req.body.proofs.map(x=>String(x||'').trim()):[];
  if(!p)return res.status(404).json({success:false,error:'Recorded salary payment not found.'});
  if(!proofs.length||proofs.some(x=>!/^\/api\/expenses\/photo\/[A-Za-z0-9._-]+$/.test(x)))return res.status(400).json({success:false,error:'Upload at least one valid payment proof.'});
  const before=(p.proofs&&p.proofs.length?p.proofs:[p.proof]).filter(Boolean),added=proofs.filter(x=>!before.includes(x));
  if(!added.length)return res.status(400).json({success:false,error:'These proofs are already attached.'});
  p.proofs=before.concat(added);p.proof=p.proofs[0];p.proofsUpdatedAt=new Date().toISOString();p.proofsUpdatedBy=req.user&&req.user.username||'admin';
  s.salaryPaymentAudit=s.salaryPaymentAudit||[];s.salaryPaymentAudit.push({at:p.proofsUpdatedAt,by:p.proofsUpdatedBy,action:'PAYMENT_PROOFS_ADDED',paymentId:p.id,ym:p.ym,empId:p.empId,account:p.account,proofsAdded:added.slice()});
  save(s);res.json({success:true,paymentId:p.id,proofCount:p.proofs.length});
});
router.get('/api/salary/payments/:ym',guard,(req,res)=>{
  const ym=String(req.params.ym||'');if(!/^\d{4}-\d{2}$/.test(ym))return res.status(400).json({success:false,error:'Invalid payroll month.'});
  const s=load(),payments=(s.salaryPayments||[]).filter(p=>p.active!==false&&p.ym===ym).slice().sort((a,b)=>String(b.date+b.id).localeCompare(String(a.date+a.id)));
  res.json({success:true,payments,audit:(s.salaryPaymentAudit||[]).filter(x=>x.ym===ym).slice().reverse()});
});
router.patch('/api/salary/payments/:id',guard,(req,res)=>{
  if(!canApproveAdvance(req))return res.status(403).json({success:false,error:'Only a salary manager can correct a salary payment.'});
  const s=load(),p=(s.salaryPayments||[]).find(x=>x.id===req.params.id&&x.active!==false),b=req.body||{};
  if(!p)return res.status(404).json({success:false,error:'Salary payment not found.'});
  if(p.linkedLedgerEntryId)return res.status(409).json({success:false,error:'This salary payment is linked to an existing ledger debit. Correct that original ledger entry before changing its amount, date, or account.'});
  const amount=round2(num(b.amount)),date=String(b.date||''),account=String(b.account||'').trim(),reason=String(b.reason||'').trim();
  const proofs=Array.from(new Set([].concat(Array.isArray(b.proofs)?b.proofs:[],b.proof||[]).map(x=>String(x||'').trim()).filter(Boolean)));
  if(!(amount>0)||!/^\d{4}-\d{2}-\d{2}$/.test(date)||!SALARY_PAYING_ACCOUNTS.includes(account)||!proofs.length||!reason)return res.status(400).json({success:false,error:'Amount, date, paying account, at least one proof, and correction reason are required.'});
  const before={amount:p.amount,date:p.date,account:p.account,proofs:p.proofs||[p.proof].filter(Boolean),reference:p.reference||''};
  const changed=amount!==round2(num(p.amount))||date!==p.date||account!==p.account||JSON.stringify(proofs)!==JSON.stringify(before.proofs)||String(b.reference||'').trim()!==before.reference;
  if(!changed)return res.status(400).json({success:false,error:'No payment details changed.'});
  const otherPaid=(s.salaryPayments||[]).filter(x=>x!==p&&x.active!==false&&x.empId===p.empId&&x.ym===p.ym).reduce((n,x)=>n+num(x.amount),0);
  const row=computeMonth(s,p.ym).find(x=>x.id===p.empId),available=row?round2(row.netPayable-num((s.months[p.ym]&&s.months[p.ym].rows[p.empId]||{}).paid)-otherPaid):0;
  const overpayment=round2(Math.max(0,amount-available));
  if(overpayment>.001&&!(isOwner(req)&&b.allowOverpayment===true))return res.status(400).json({success:false,error:'Corrected amount exceeds this employee’s available salary balance. Only the Owner may explicitly approve a proof-backed overpayment.'});
  Object.assign(p,{amount,date,account,proof:proofs[0],proofs,reference:String(b.reference||'').trim(),editedAt:new Date().toISOString(),editedBy:req.user.username});
  s.salaryPaymentAudit=s.salaryPaymentAudit||[];s.salaryPaymentAudit.push({at:p.editedAt,by:req.user.username,action:'PAYMENT_CORRECTED',paymentId:p.id,ym:p.ym,empId:p.empId,before,after:{amount,date,account,proofs,reference:p.reference},overpayment,reason});
  save(s);res.json({success:true,payment:p});
});
function existing3645DebitCandidates(s){
  let store;try{store=JSON.parse(fs.readFileSync(EXP_PATH,'utf8'));}catch{return[];}
  const book=(store.bankStatements||{})['Prashant Axis 3645'],used=new Set((s.salaryPayments||[]).filter(p=>p.active!==false&&p.linkedLedgerEntryId).map(p=>p.linkedLedgerEntryId)),seen=new Set(),out=[];
  for(const record of book&&book.imports||[])for(const row of [].concat(record.reconciliationRows||[],record.carriedReconciliationRows||[])){
    const bank=row.bank||{},ledger=row.ledger||{},ledgerId=String(ledger.id||'');
    if(!ledgerId||!num(bank.debit)||!num(ledger.debit)||Math.abs(num(bank.debit)-num(ledger.debit))>.01||used.has(ledgerId)||seen.has(ledgerId)||ledgerId.startsWith('SALB-'))continue;
    seen.add(ledgerId);out.push({ledgerEntryId:ledgerId,recordId:record.id,rowId:row.id,date:bank.date,amount:round2(num(bank.debit)),reference:String(bank.reference||''),description:String(bank.description||''),ledgerDescription:String(ledger.description||''),ledgerProof:String(ledger.proof||'')});
  }
  return out.sort((a,b)=>String(b.date+b.ledgerEntryId).localeCompare(String(a.date+a.ledgerEntryId)));
}
router.get('/api/salary/existing-3645-debits',guard,(req,res)=>{
  if(!canApproveAdvance(req))return res.status(403).json({success:false,error:'Only a salary manager can link existing salary payments.'});
  if(activeSalaryEntity()!=='SANKI')return res.status(400).json({success:false,error:'3645 belongs to SANKI payroll.'});
  res.json({success:true,candidates:existing3645DebitCandidates(load())});
});
router.post('/api/salary/payments/link-existing',guard,(req,res)=>{
  if(!canApproveAdvance(req))return res.status(403).json({success:false,error:'Only a salary manager can link existing salary payments.'});
  if(activeSalaryEntity()!=='SANKI')return res.status(400).json({success:false,error:'3645 belongs to SANKI payroll.'});
  const s=load(),b=req.body||{},ym=String(b.ym||''),emp=s.employees[String(b.empId||'')],ledgerEntryId=String(b.ledgerEntryId||''),reason=String(b.reason||'').trim();
  const proofs=Array.from(new Set([].concat(Array.isArray(b.proofs)?b.proofs:[],b.proof||[]).map(x=>String(x||'').trim()).filter(Boolean)));
  if(!/^\d{4}-\d{2}$/.test(ym)||!emp||!ledgerEntryId||!reason||!proofs.length)return res.status(400).json({success:false,error:'Choose employee, payroll month, existing 3645 debit, reason, and payment proof.'});
  const candidate=existing3645DebitCandidates(s).find(x=>x.ledgerEntryId===ledgerEntryId);if(!candidate)return res.status(409).json({success:false,error:'The 3645 debit is no longer eligible or has already been linked.'});
  const mo=ensureMonth(s,ym),stored=mo.rows[emp.id]||{},offset=ym==='2026-08'?num(stored.historicalCloseAdjustment):0;
  if(offset)delete stored.historicalCloseAdjustment;mo.rows[emp.id]=stored;
  const row=computeMonth(s,ym).find(x=>x.id===emp.id),remaining=round2(num(row&&row.balance));
  if(!row||candidate.amount>remaining+.001){if(offset)stored.historicalCloseAdjustment=offset;return res.status(400).json({success:false,error:'This debit exceeds the reopened salary balance. Review the employee and amount before linking.'});}
  s.salaryPaymentBatchSeq=(s.salaryPaymentBatchSeq||0)+1;const id='SALB-'+String(s.salaryPaymentBatchSeq).padStart(5,'0')+'-001',now=new Date().toISOString();
  const payment={id,batchId:id.slice(0,-4),ym,empId:emp.id,employeeName:emp.name,amount:candidate.amount,date:candidate.date,account:'Prashant Axis 3645',proof:proofs[0],proofs,reference:candidate.reference||ledgerEntryId,linkedLedgerEntryId:ledgerEntryId,linkedBankRecordId:candidate.recordId,linkedBankRowId:candidate.rowId,modificationReason:reason,remainingBeforePayment:remaining,balanceAfterPayment:round2(remaining-candidate.amount),active:true,createdBy:req.user.username,createdAt:now};
  s.salaryPayments=s.salaryPayments||[];s.salaryPayments.push(payment);s.salaryPaymentAudit=s.salaryPaymentAudit||[];s.salaryPaymentAudit.push({at:now,by:req.user.username,action:'EXISTING_3645_DEBIT_LINKED',paymentId:id,ym,empId:emp.id,ledgerEntryId,bankRecordId:candidate.recordId,bankRowId:candidate.rowId,historicalOffsetReopened:round2(offset),reason});save(s);
  res.json({success:true,payment,noAdditionalLedgerDebit:true});
});
router.post('/api/salary/historical-offset/:ym/:empId/reopen',guard,(req,res)=>{
  if(!canReviewAugustSalary(req))return res.status(403).json({success:false,error:'Only the Owner or Prashant can review August salary balances.'});
  const ym=String(req.params.ym||''),empId=String(req.params.empId||''),reason=String(req.body&&req.body.reason||'').trim();
  if(ym!=='2026-08'||!reason)return res.status(400).json({success:false,error:'Choose August 2026 and enter a reason.'});
  const s=load(),mo=s.months[ym],row=mo&&mo.rows&&mo.rows[empId],employee=s.employees[empId];
  if(!employee||!row||!num(row.historicalCloseAdjustment))return res.status(404).json({success:false,error:'No unverified historical offset for this employee.'});
  const before=round2(num(row.historicalCloseAdjustment));delete row.historicalCloseAdjustment;
  s.salaryPaymentAudit=s.salaryPaymentAudit||[];s.salaryPaymentAudit.push({at:new Date().toISOString(),by:req.user.username,action:'HISTORICAL_OFFSET_REOPENED',ym,empId,employeeName:employee.name,beforeOffset:before,reason});
  save(s);res.json({success:true,employeeName:employee.name,reopenedAmount:round2(-before),row:computeMonth(s,ym).find(x=>x.id===empId)});
});
router.get('/api/salary/ledgers',guard,(req,res)=>{
  const s=load(),by={};const ensure=(id,name)=>by[id]||(by[id]={empId:id,name,ledgerName:(name||id)+' — Salary',entries:[]});
  Object.values(s.advances||{}).filter(a=>a.active!==false&&!a.historicalOpening).forEach(a=>ensure(a.empId,a.employeeName).entries.push({id:a.id,date:a.date,kind:'advance',description:'Salary advance paid'+(a.note?' · '+a.note:''),debit:num(a.amount),credit:0,proof:a.proof||'',reference:a.reference||a.id}));
  (s.salaryPayments||[]).filter(p=>p.active!==false).forEach(p=>ensure(p.empId,p.employeeName).entries.push({id:p.id,date:p.date,kind:'salary_paid',description:p.ym+' salary payment'+(p.note?' · '+p.note:''),debit:num(p.amount),credit:0,proof:p.proof||'',proofs:(p.proofs&&p.proofs.length?p.proofs:[p.proof]).filter(Boolean),reference:p.reference||p.batchId}));
  Object.values(s.payrollPostings||{}).forEach(p=>(p.rows||[]).forEach(r=>{const l=ensure(r.empId,r.employeeName),date=p.ym+'-'+String(daysInMonth(p.ym)).padStart(2,'0');if(num(r.salaryAmt))l.entries.push({id:p.ym+'/'+r.empId+'/EARNED',date,kind:'salary_earned',description:p.ym+' salary earned',debit:0,credit:num(r.salaryAmt),reference:p.ym});if(num(r.legacyPaid))l.entries.push({id:p.ym+'/'+r.empId+'/LEGACY-PAID',date,kind:'legacy_salary_paid',description:p.ym+' legacy paid amount',debit:num(r.legacyPaid),credit:0,reference:p.ym});}));
  const ledgers=Object.values(by).map(l=>{l.entries.sort((a,b)=>String(a.date+a.id).localeCompare(String(b.date+b.id)));let balance=0;l.entries.forEach(e=>{balance+=num(e.credit)-num(e.debit);e.balance=round2(balance);});l.balance=round2(balance);l.status=balance>0?'Company owes':balance<0?'Employee owes':'Settled';l.lastPostingDate=l.entries.at(-1)&&l.entries.at(-1).date||'';l.outstandingAdvance=round2(Object.values(s.advances||{}).filter(a=>a.active!==false&&a.empId===l.empId).reduce((n,a)=>n+advanceOutstanding(a),0));l.entries=l.entries.slice().reverse();return l;}).sort(byEmployeeName);
  res.json({success:true,ledgers});
});

// Salary advances are recoverable employee balances, not salary/P&L expenses.
router.get('/api/salary/advances', guard, (req, res) => {
  const s = load(), q = req.query || {};
  let rows = Object.values(s.advances || {}).map(a=>advanceView(a,s));
  if (q.employee) rows = rows.filter(a => a.empId === q.employee);
  if (q.month) rows = rows.filter(a => String(a.date || '').slice(0, 7) === q.month);
  if (q.status) rows = rows.filter(a => a.status === q.status);
  if (q.account) rows = rows.filter(a => a.account === q.account);
  rows.sort((a, b) => String(b.date + b.id).localeCompare(String(a.date + a.id)));
  const requests=Object.values(s.advanceRequests||{}).sort((a,b)=>String(b.createdAt||'').localeCompare(String(a.createdAt||''))),payrollRows=new Map(computeMonth(s,q.summaryMonth||new Date().toISOString().slice(0,7)).map(x=>[x.id,x])),summary = Object.values(s.employees).sort(byEmployeeName).map(e => {
    const all = Object.values(s.advances || {}).filter(a => a.active !== false && a.empId === e.id);
    const total = all.reduce((n, a) => n + num(a.amount), 0), recovered = all.reduce((n, a) => n + advanceRecovered(a), 0);
    const payroll=payrollRows.get(e.id),transactions=all.map(a=>advanceView(a,s)).sort((a,b)=>String(b.date+b.id).localeCompare(String(a.date+a.id))),employeeRequests=requests.filter(r=>r.empId===e.id&&r.status!=='Posted'),activityDates=transactions.map(x=>x.date).concat(employeeRequests.map(x=>x.payoutDate||x.date)).filter(Boolean).sort().reverse();return { empId: e.id, name: e.name, thisMonth: all.filter(a => String(a.date).slice(0, 7) === (q.summaryMonth || new Date().toISOString().slice(0, 7))).reduce((n, a) => n + num(a.amount), 0), total: round2(total), recovered: round2(recovered), outstanding: round2(total - recovered), companyOwes:round2(Math.max(0,payroll&&payroll.balance||0)),lastActivity:activityDates[0]||'',transactions,requests:employeeRequests };
  }).filter(x => x.total || x.recovered || x.companyOwes || x.requests.length);
  const totals = summary.reduce((t, x) => ({ total: t.total + x.total, recovered: t.recovered + x.recovered, outstanding: t.outstanding + x.outstanding }), { total: 0, recovered: 0, outstanding: 0 });
  res.json({ success: true, advances: rows, summary, totals, requests, editRequests:Object.values(s.advanceEditRequests||{}).filter(x=>x.status==='Pending owner approval'), sourceSheet:(s.advanceSourceSheets||[]).at(-1)||null, sourceSheetPostingAccounts:SOURCE_SHEET_POSTING_ACCOUNTS, permissions:{canRequest:canRequestOrPostAdvance(req),canDirectPost:isOwner(req),canApprove:isOwner(req),canPostProof:canRequestOrPostAdvance(req),canEdit:canRequestOrPostAdvance(req),canCancel:isOwner(req)}, audit: (s.advanceAudit || []).slice().reverse().slice(0, 500), requestAudit:(s.advanceRequestAudit||[]).slice().reverse().slice(0,500) });
});
router.post('/api/salary/advances/source-sheet',guard,receiveSalarySheet,(req,res)=>{
  if(!canApproveAdvance(req))return res.status(403).json({success:false,error:'Only a salary manager can save an advances source sheet.'});
  try{const parsed=advanceSourceSheet(req.file),s=load();s.advanceSourceSheets=s.advanceSourceSheets||[];const existing=s.advanceSourceSheets.find(x=>x.hash===parsed.hash);if(existing)return res.json({success:true,alreadySaved:true,sourceSheet:existing});const sourceSheet={hash:parsed.hash,fileName:path.basename(req.file.originalname||''),savedAt:new Date().toISOString(),savedBy:req.user&&req.user.username||'admin',items:parsed.items,total:parsed.total};s.advanceSourceSheets.push(sourceSheet);save(s);res.json({success:true,alreadySaved:false,sourceSheet});}catch(err){res.status(400).json({success:false,error:err.message});}
});
function currentAdvanceSheet(s,hash,row){
  const sheet=(s.advanceSourceSheets||[]).at(-1);
  if(!sheet||sheet.hash!==hash)throw new Error('This sheet is no longer the latest. Refresh the advances page.');
  const item=(sheet.items||[]).find(x=>x.row===Number(row));
  if(!item)throw new Error('Sheet row not found. Refresh the advances page.');
  return {sheet,item};
}
function sheetEmployee(s,item){
  return (item.employeeId&&s.employees[item.employeeId])||findImportedEmployee(s,item.employeeName,'');
}
function sourceCandidates(s,item){
  const employee=sheetEmployee(s,item);
  if(!employee)return [];
  return Object.values(s.advances||{}).filter(a=>a.active!==false&&a.empId===employee.id&&Math.abs(num(a.amount)-num(item.amount))<.005);
}
router.patch('/api/salary/advances/source-sheet/rows/:row',guard,(req,res)=>{
  if(!canApproveAdvance(req))return res.status(403).json({success:false,error:'Only a salary manager can edit the source sheet.'});
  try{
    const s=load(),b=req.body||{},{sheet,item}=currentAdvanceSheet(s,String(b.hash||''),req.params.row);
    const fields=['advanceId','employeeId','employeeName','amount','requestDate','payingAccount','recoveryStartMonth','note','reference'];
    const next=Object.assign({},item);
    fields.forEach(key=>{if(Object.prototype.hasOwnProperty.call(b,key))next[key]=key==='amount'?round2(Number(b[key])):String(b[key]??'').trim();});
    if(!next.employeeName||!(next.amount>0)||!/^[0-9]{4}-[0-9]{2}-[0-9]{2}$/.test(next.requestDate)||isNaN(Date.parse(next.requestDate+'T00:00:00Z')))return res.status(400).json({success:false,error:'Employee, positive amount and a real payment date are required.'});
    const employee=sheetEmployee(s,next);if(!employee)return res.status(400).json({success:false,error:'Select an existing employee before saving this row.'});
    next.employeeId=employee.id;next.employeeName=employee.name;
    if(item.linkedAdvanceId&&['employeeId','amount','requestDate','payingAccount'].some(k=>String(next[k])!==String(item[k])))return res.status(409).json({success:false,error:'This row is linked to a posted advance. Edit the posted advance separately before changing its employee, amount, date or paying account.'});
    const before=Object.assign({},item);Object.assign(item,next);item.reviewedAt=new Date().toISOString();item.reviewedBy=req.user.username;
    sheet.total=round2(sheet.items.reduce((sum,x)=>sum+num(x.amount),0));sheet.edits=sheet.edits||[];sheet.edits.push({at:new Date().toISOString(),by:req.user.username,row:item.row,before,after:Object.assign({},item)});
    save(s);res.json({success:true,item,total:sheet.total});
  }catch(err){res.status(409).json({success:false,error:err.message});}
});
router.post('/api/salary/advances/source-sheet/rows/:row/link',guard,(req,res)=>{
  if(!canApproveAdvance(req))return res.status(403).json({success:false,error:'Only a salary manager can link advances.'});
  try{
    const s=load(),b=req.body||{},{item}=currentAdvanceSheet(s,String(b.hash||''),req.params.row),a=(s.advances||{})[String(b.advanceId||'')];
    if(item.linkedAdvanceId)return res.status(409).json({success:false,error:'This sheet row is already linked.'});
    if(!a||a.active===false||!sourceCandidates(s,item).some(x=>x.id===a.id))return res.status(400).json({success:false,error:'Choose a posted advance for the same employee and amount.'});
    if(Object.values((s.advanceSourceSheets||[]).at(-1).items).some(x=>x.row!==item.row&&x.linkedAdvanceId===a.id))return res.status(409).json({success:false,error:'That advance is already linked to another sheet row.'});
    item.linkedAdvanceId=a.id;item.linkedAt=new Date().toISOString();item.linkedBy=req.user.username;
    auditAdvance(s,req,'SOURCE_SHEET_LINKED',a.id,{sheetHash:b.hash,row:item.row,sourceDate:item.requestDate,postedDate:a.date});save(s);res.json({success:true,advanceId:a.id});
  }catch(err){res.status(409).json({success:false,error:err.message});}
});
router.post('/api/salary/advances/source-sheet/rows/:row/post',guard,(req,res)=>{
  if(!isOwner(req))return res.status(403).json({success:false,error:'Only the Owner can post a source-sheet advance directly.'});
  try{
    const s=load(),b=req.body||{},{item}=currentAdvanceSheet(s,String(b.hash||''),req.params.row),employee=sheetEmployee(s,item);
    if(item.linkedAdvanceId)return res.status(409).json({success:false,error:'Already linked. No second payment was posted.'});
    if(!item.reviewedAt)return res.status(409).json({success:false,error:'Save and review this sheet row before posting it.'});
    if(!employee)return res.status(400).json({success:false,error:'Choose an existing employee first.'});
    if(/carried forward/i.test(item.note)||!item.payingAccount||/^(n\/?a|recharge)$/i.test(item.payingAccount))return res.status(400).json({success:false,error:'This looks like a carried-forward balance or unclear source, not a new payment. Confirm and correct it before posting.'});
    if(sourceCandidates(s,item).some(a=>a.date===item.requestDate||a.id===item.advanceId))return res.status(409).json({success:false,error:'A posted advance for this employee, date and amount already exists. Link it instead of posting another debit.'});
    const proofs=Array.from(new Set([].concat(b.proofs||[]).map(x=>String(x||'').trim()).filter(Boolean)));
    if(!proofs.length)return res.status(400).json({success:false,error:'Attach at least one payment proof.'});
    const account=String(b.account||'').trim(),allowed=new Set(SOURCE_SHEET_POSTING_ACCOUNTS);
    if(!allowed.has(account))return res.status(400).json({success:false,error:'Choose a valid salary paying account or cash source.'});
    const recoveryStartMonth=/^\d{4}-\d{2}$/.test(item.recoveryStartMonth)?item.recoveryStartMonth:item.requestDate.slice(0,7);
    s.advanceSeq=(s.advanceSeq||0)+1;const id='ADV-'+String(s.advanceSeq).padStart(5,'0'),now=new Date().toISOString();
    s.advances[id]={id,empId:employee.id,employeeName:employee.name,amount:item.amount,date:item.requestDate,payoutDate:item.requestDate,account,proof:proofs[0],proofs,note:item.note,reference:item.reference,recoveryStartMonth,recoveries:[],active:true,createdBy:req.user.username,createdAt:now,approvedBy:req.user.username,approvedAt:now,sourceSheetHash:b.hash,sourceSheetRow:item.row};
    item.linkedAdvanceId=id;item.linkedAt=now;item.linkedBy=req.user.username;auditAdvance(s,req,'SOURCE_SHEET_POSTED',id,{sheetHash:b.hash,row:item.row,account,proofCount:proofs.length});save(s);res.json({success:true,advance:advanceView(s.advances[id],s)});
  }catch(err){res.status(409).json({success:false,error:err.message});}
});
router.get('/api/salary/advances/import/template',guard,(req,res)=>{
  if(!canApproveAdvance(req))return res.status(403).json({success:false,error:'Only a salary manager can import advances.'});
  const sheet=XLSX.utils.aoa_to_sheet([['Advance ID','Employee ID','Employee Name','Advance Amount','Request Date','Paying Account','Recovery Start Month','Note','Reference']]);const workbook=XLSX.utils.book_new();XLSX.utils.book_append_sheet(workbook,sheet,'Advances');res.setHeader('Content-Type','application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');res.setHeader('Content-Disposition','attachment; filename="advances-import-template.xlsx"');res.send(XLSX.write(workbook,{type:'buffer',bookType:'xlsx'}));
});
router.post('/api/salary/advances/import/preview',guard,receiveSalarySheet,(req,res)=>{
  if(!canApproveAdvance(req))return res.status(403).json({success:false,error:'Only a salary manager can import advances.'});
  try{const result=advanceSheetRows(load(),req.file);res.json({success:true,changes:result.changes,hash:result.hash,previewToken:result.previewToken});}catch(err){res.status(400).json({success:false,error:err.message});}
});
router.post('/api/salary/advances/import/apply',guard,receiveSalarySheet,(req,res)=>{
  if(!canApproveAdvance(req))return res.status(403).json({success:false,error:'Only a salary manager can import advances.'});
  const reason=String((req.body||{}).reason||'').trim();if(!reason)return res.status(400).json({success:false,error:'Enter a reason for the advances import.'});
  try{const s=load(),result=advanceSheetRows(s,req.file);if(result.previewToken!==String((req.body||{}).previewToken||''))return res.status(409).json({success:false,error:'The sheet or advance records changed after preview. Preview it again.'});const applied=applyAdvanceSheetRows(s,result,reason,req,req.file.originalname);if(applied.ids.length)save(s);res.json({success:true,requests:applied.ids.length,ids:applied.ids,counts:applied.counts});}catch(err){res.status(400).json({success:false,error:err.message});}
});

router.patch('/api/salary/advances/:id',guard,(req,res)=>{
  const s=load(),a=(s.advances||{})[req.params.id],b=req.body||{},reason=String(b.reason||'').trim(),amount=round2(num(b.amount)),date=String(b.date||'').slice(0,10),account=String(b.account||'').trim();
  if(!a||a.active===false)return res.status(404).json({success:false,error:'Advance not found.'});if(!reason||!(amount>0)||amount+0.001<advanceRecovered(a)||!/^\d{4}-\d{2}-\d{2}$/.test(date)||!account)return res.status(400).json({success:false,error:'Reason, valid amount/date/account are required, and amount cannot be below recovered value.'});
  const changes={amount,date,payoutDate:date,account,note:String(b.note||'').trim()};
  if(!isOwner(req)){
    s.advanceEditRequests=s.advanceEditRequests||{};s.advanceEditRequestSeq=(s.advanceEditRequestSeq||0)+1;
    const id='ADVE-'+String(s.advanceEditRequestSeq).padStart(5,'0');s.advanceEditRequests[id]={id,advanceId:a.id,changes,reason,status:'Pending owner approval',createdBy:req.user&&req.user.username||'admin',createdAt:new Date().toISOString()};
    auditAdvance(s,req,'EDIT_REQUESTED',a.id,{requestId:id,changes,reason});save(s);return res.json({success:true,pendingApproval:true,request:s.advanceEditRequests[id]});
  }
  const before=JSON.parse(JSON.stringify(a));Object.assign(a,changes,{editedBy:req.user.username,editedAt:new Date().toISOString()});auditAdvance(s,req,'EDITED',a.id,{before,after:a,reason});save(s);res.json({success:true,advance:advanceView(a,s)});
});

router.post('/api/salary/advance-edit-requests/:id/decision',guard,(req,res)=>{
  if(!isOwner(req))return res.status(403).json({success:false,error:'Only the Owner can decide an advance edit.'});
  const s=load(),request=(s.advanceEditRequests||{})[req.params.id],decision=String((req.body||{}).decision||''),note=String((req.body||{}).note||'').trim();
  if(!request||request.status!=='Pending owner approval')return res.status(404).json({success:false,error:'Pending advance edit not found.'});
  if(!['approve','reject'].includes(decision))return res.status(400).json({success:false,error:'Choose approve or reject.'});
  if(decision==='reject'&&!note)return res.status(400).json({success:false,error:'Give a reason for rejection.'});
  const a=(s.advances||{})[request.advanceId];if(!a||a.active===false)return res.status(409).json({success:false,error:'The original advance is no longer active.'});
  if(decision==='approve'){
    if(request.changes.amount+0.001<advanceRecovered(a))return res.status(409).json({success:false,error:'Advance has since been recovered beyond the proposed amount.'});
    const before=JSON.parse(JSON.stringify(a));Object.assign(a,request.changes,{editedBy:req.user.username,editedAt:new Date().toISOString()});auditAdvance(s,req,'EDIT_APPROVED',a.id,{requestId:request.id,before,after:a,reason:request.reason,note});
  }else auditAdvance(s,req,'EDIT_REJECTED',a.id,{requestId:request.id,reason:note});
  request.status=decision==='approve'?'Approved':'Rejected';request.decidedBy=req.user.username;request.decidedAt=new Date().toISOString();request.decisionNote=note;save(s);res.json({success:true,request,advance:advanceView(a,s)});
});

router.post('/api/salary/advances', guard, (req, res) => {
  const s = load(), b = req.body || {}, emp = s.employees[b.empId], amount = num(b.amount);
  if (!canRequestOrPostAdvance(req)) return res.status(403).json({success:false,error:'Only the Owner, Admin, or Prashant can record an advance.'});
  if (!emp) return res.status(400).json({ success: false, error: 'Select an employee.' });
  if (!(amount > 0)) return res.status(400).json({ success: false, error: 'Enter a valid advance amount.' });
  if (!/^\d{4}-\d{2}-\d{2}$/.test(String(b.date || ''))) return res.status(400).json({ success: false, error: 'Select the payment date.' });
  if (!String(b.account || '').trim()) return res.status(400).json({ success: false, error: 'Select the paying account.' });
  const recoveryStartMonth = String(b.recoveryStartMonth || b.date.slice(0, 7));
  if (!/^\d{4}-\d{2}$/.test(recoveryStartMonth)) return res.status(400).json({ success: false, error: 'Select a recovery start month.' });
  if (isOwner(req)) {
    const proofs=Array.from(new Set([].concat(Array.isArray(b.proofs)?b.proofs:[],b.proof||[]).map(x=>String(x||'').trim()).filter(Boolean)));
    if(!proofs.length)return res.status(400).json({success:false,error:'Payment proof is required to post the advance.'});
    s.advanceSeq=(s.advanceSeq||0)+1;const id='ADV-'+String(s.advanceSeq).padStart(5,'0'),now=new Date().toISOString(),payoutDate=String(b.date);
    s.advances[id]={id,empId:emp.id,employeeName:emp.name,amount:round2(amount),date:payoutDate,payoutDate,account:String(b.account).trim(),proof:proofs[0],proofs,note:String(b.note||'').trim(),reference:String(b.reference||'').trim(),recoveryStartMonth,recoveries:[],active:true,createdBy:req.user&&req.user.username||'owner',createdAt:now,approvedBy:req.user&&req.user.username||'owner',approvedAt:now,directOwnerPost:true};
    auditAdvance(s,req,'OWNER_DIRECT_POST',id,{amount:round2(amount),account:String(b.account).trim(),payoutDate,proofCount:proofs.length});save(s);
    return res.json({success:true,directPost:true,advance:advanceView(s.advances[id],s)});
  }
  s.advanceRequests=s.advanceRequests||{};s.advanceRequestSeq=(s.advanceRequestSeq||0)+1;const id='ADVR-'+String(s.advanceRequestSeq).padStart(5,'0'),now=new Date().toISOString();
  s.advanceRequests[id]={id,empId:emp.id,employeeName:emp.name,amount:round2(amount),date:String(b.date),account:String(b.account).trim(),note:String(b.note||'').trim(),reference:String(b.reference||'').trim(),recoveryStartMonth,status:'Pending approval',createdBy:req.user&&req.user.username||'admin',createdAt:now};
  auditAdvanceRequest(s,req,'SUBMITTED',id,{amount:round2(amount),account:String(b.account).trim()});save(s);
  res.json({success:true,request:s.advanceRequests[id]});
});

router.post('/api/salary/advance-requests/:id/approve',guard,(req,res)=>{
  if(!isOwner(req))return res.status(403).json({success:false,error:'Only the Owner can approve an advance.'});
  const s=load(),r=(s.advanceRequests||{})[req.params.id];if(!r)return res.status(404).json({success:false,error:'Advance request not found.'});
  if(r.status!=='Pending approval')return res.status(400).json({success:false,error:'Only a pending request can be approved.'});
  r.status='Approved – proof required';r.approvedBy=req.user&&req.user.username||'owner';r.approvedAt=new Date().toISOString();r.approvalNote=String((req.body||{}).note||'').trim();auditAdvanceRequest(s,req,'APPROVED',r.id,{note:r.approvalNote});save(s);res.json({success:true,request:r});
});

router.post('/api/salary/advance-requests/:id/reject',guard,(req,res)=>{
  if(!isOwner(req))return res.status(403).json({success:false,error:'Only the Owner can reject an advance.'});
  const s=load(),r=(s.advanceRequests||{})[req.params.id],reason=String((req.body||{}).reason||'').trim();if(!r)return res.status(404).json({success:false,error:'Advance request not found.'});
  if(r.status!=='Pending approval')return res.status(400).json({success:false,error:'Only a pending request can be rejected.'});if(!reason)return res.status(400).json({success:false,error:'A rejection reason is required.'});
  r.status='Rejected';r.rejectedBy=req.user&&req.user.username||'owner';r.rejectedAt=new Date().toISOString();r.rejectionReason=reason;auditAdvanceRequest(s,req,'REJECTED',r.id,{reason});save(s);res.json({success:true,request:r});
});

router.post('/api/salary/advance-requests/:id/post',guard,(req,res)=>{
  if(!canRequestOrPostAdvance(req))return res.status(403).json({success:false,error:'Only Admin or Prashant can upload proof and post an approved advance.'});
  const s=load(),r=(s.advanceRequests||{})[req.params.id],b=req.body||{},proofs=Array.from(new Set([].concat(Array.isArray(b.proofs)?b.proofs:[],b.proof||[]).map(x=>String(x||'').trim()).filter(Boolean)));
  if(!r)return res.status(404).json({success:false,error:'Advance request not found.'});if(r.status!=='Approved – proof required')return res.status(400).json({success:false,error:'The Owner must approve this request before it can be posted.'});if(!proofs.length)return res.status(400).json({success:false,error:'Payment proof is required before posting.'});
  const payoutDate=String(b.payoutDate||'').trim();if(!/^\d{4}-\d{2}-\d{2}$/.test(payoutDate)||isNaN(Date.parse(payoutDate+'T00:00:00Z')))return res.status(400).json({success:false,error:'Actual payout date is required before posting.'});
  s.advanceSeq=(s.advanceSeq||0)+1;const id='ADV-'+String(s.advanceSeq).padStart(5,'0'),now=new Date().toISOString();s.advances[id]={id,requestId:r.id,empId:r.empId,employeeName:r.employeeName,amount:r.amount,date:payoutDate,payoutDate,proposedDate:r.date,account:r.account,proof:proofs[0],proofs,note:r.note,reference:r.reference,recoveryStartMonth:payoutDate.slice(0,7),recoveries:[],active:true,createdBy:req.user&&req.user.username||'admin',createdAt:now,approvedBy:r.approvedBy,approvedAt:r.approvedAt};
  r.status='Posted';r.advanceId=id;r.payoutDate=payoutDate;r.proof=proofs[0];r.proofs=proofs;r.postedBy=req.user&&req.user.username||'admin';r.postedAt=now;auditAdvanceRequest(s,req,'POSTED',r.id,{advanceId:id,payoutDate,proofCount:proofs.length});auditAdvance(s,req,'CREATED',id,{amount:r.amount,account:r.account,requestId:r.id,payoutDate});save(s);res.json({success:true,request:r,advance:advanceView(s.advances[id],s)});
});

router.post('/api/salary/recoveries/:ym', guard, (req, res) => {
  if(!canApproveAdvance(req))return res.status(403).json({success:false,error:'Only a salary manager can change deductions.'});
  const s = load(), b = req.body || {}, ym = req.params.ym, amount = Number(b.amount), reason=String(b.reason||'').trim();
  if (!s.employees[b.empId] || !/^\d{4}-\d{2}$/.test(ym) || !Number.isFinite(amount) || amount < 0 || round2(amount)!==amount) return res.status(400).json({ success: false, error: 'Invalid employee, month or amount (maximum two decimal places).' });
  const employeeAdvances=Object.values(s.advances||{}).filter(a=>a.active!==false&&a.empId===b.empId);
  const previous=monthRecovery(s,b.empId,ym);
  if(round2(previous)!==round2(amount)&&!reason)return res.status(400).json({success:false,error:'Explain why this advance deduction amount was changed.'});
  employeeAdvances.forEach(a=>{a.recoveries=(a.recoveries||[]).filter(r=>r.ym!==ym);});
  // recoveryStartMonth is the payroll authority. This intentionally supports
  // owner-approved imports paid later but recovered in an earlier payroll run.
  const eligible = employeeAdvances.filter(a => a.recoveryStartMonth <= ym).sort((a,b)=>String(a.date+a.id).localeCompare(String(b.date+b.id)));
  const available = eligible.reduce((n, a) => n + advanceOutstanding(a), 0);
  if (amount > available + .001) return res.status(400).json({ success: false, error: 'Recovery cannot exceed the eligible outstanding advance of ₹' + round2(available) + '.' });
  let left = amount; eligible.forEach(a => { if (left <= 0) return; const take = Math.min(left, advanceOutstanding(a)); if (take > 0) { const at=new Date().toISOString();a.recoveries.push({ ym, payrollMonth:ym, amount: round2(take), by: req.user && req.user.username || 'admin', at, recordedOn:at.slice(0,10), reference:'SALARY-RECOVERY-'+ym,note:reason }); left = round2(left - take); } });
  auditAdvance(s, req, 'RECOVERY_SET', '', { empId: b.empId, ym, previous, amount, reason }); save(s); res.json({ success: true, amount: round2(amount) });
});

router.post('/api/salary/advances/:id/cancel', guard, (req, res) => {
  if(!isOwner(req))return res.status(403).json({success:false,error:'Only the Owner can cancel an advance.'});
  const s = load(), a = (s.advances || {})[req.params.id], reason = String((req.body || {}).reason || '').trim();
  if (!a || a.active === false) return res.status(404).json({ success: false, error: 'Advance not found.' });
  if (!reason) return res.status(400).json({ success: false, error: 'A cancellation reason is required.' });
  if (advanceRecovered(a) > 0) return res.status(400).json({ success: false, error: 'Reverse its payroll recoveries before cancelling this advance.' });
  a.active = false; a.cancelledAt = new Date().toISOString(); a.cancelledBy = req.user && req.user.username || 'admin'; a.cancelReason = reason; auditAdvance(s, req, 'CANCELLED', a.id, { reason }); save(s); res.json({ success: true });
});

// ── A month: computed rows + attendance + totals ──
router.get('/api/salary/month/:ym', guard, (req, res) => {
  const ym = req.params.ym; const s = load();
  const rows = computeMonth(s, ym);
  const mo = s.months[ym] || { finalized: false, attendance: {} };
  const totals = rows.reduce((t, r) => {
    t.salary += r.salary; t.salaryAmt += r.salaryAmt; t.advance += r.advance;
    t.netPayable += r.netPayable; t.paid += r.paid; t.balance += r.balance; return t;
  }, { salary: 0, salaryAmt: 0, advance: 0, netPayable: 0, paid: 0, balance: 0 });
  Object.keys(totals).forEach(k => totals[k] = round2(totals[k]));
  res.json({ success: true, ym, divisor: num(s.divisor) || 30, daysInMonth: daysInMonth(ym), finalized: !!mo.finalized, rows, attendance: mo.attendance || {}, totals, finalSalaryAudit:(s.finalSalaryAudit||[]).filter(x=>x.ym===ym), finalAugustImported:(s.finalAugustSheetAudit||[]).some(x=>x.hash), permissions:{canModifyPayroll:canApproveAdvance(req),canReviewAugust:canReviewAugustSalary(req),canApplyFinalAugust:isOwner(req),canPay:true,paymentOnly:false} });
});

function receiveSalarySheet(req,res,next){salarySheetUpload(req,res,err=>err?res.status(400).json({success:false,error:err.code==='LIMIT_FILE_SIZE'?'Salary sheet exceeds the 5 MB limit.':'Could not upload the salary sheet.'}):next());}
// The founder's final August workbook is a historical payroll source, not a
// payment instruction. Its P/Q columns must match already proof-backed debits.
const AUGUST_NAME_ALIASES={arshpreetsingh:'arshpreetsingharora',arshpreetsir:'arshpreetsingharora',nandani:'nandini',tushari:'tusharinv',tusharm:'tusharmod',suraj:'surajob',ravidriver:'ravi'};
function augustEmployee(s,name){
  const normalize=x=>String(x||'').toLowerCase().replace(/[^a-z0-9]/g,'');
  const target=AUGUST_NAME_ALIASES[normalize(name)]||normalize(name),found=Object.values(s.employees||{}).filter(e=>normalize(e.name)===target);
  if(found.length!==1)throw new Error('August sheet employee '+name+' must match exactly one app employee.');
  return found[0];
}
function finalAugustPlan(s,file){
  if(!file||!file.buffer||!/^\.xlsb$/i.test(path.extname(file.originalname||'')))throw new Error('Choose the original final August .xlsb workbook.');
  const book=XLSX.read(file.buffer,{type:'buffer',cellDates:false});
  if(!['SALARY(Up.)','Attendance(Up.)','ADV'].every(name=>book.Sheets[name]))throw new Error('The August workbook must include Salary, Attendance and ADV tabs.');
  const salary=XLSX.utils.sheet_to_json(book.Sheets['SALARY(Up.)'],{header:1,defval:null,raw:true});
  const attendance=XLSX.utils.sheet_to_json(book.Sheets['Attendance(Up.)'],{header:1,defval:null,raw:true});
  const advance=XLSX.utils.sheet_to_json(book.Sheets.ADV,{header:1,defval:null,raw:true});
  const rows=salary.slice(1).filter(x=>Number.isInteger(x[0])&&x[1]),items=advance.slice(1).filter(x=>typeof x[0]==='number'&&x[1]&&x[2]!=null);
  if(rows.length!==23||items.length!==20)throw new Error('Expected 23 salary rows and 20 advance entries; this is not the confirmed final workbook.');
  const amount=(value,label)=>{const n=Number(value);if(!Number.isFinite(n)||n<0)throw new Error(label+' must be a nonnegative number.');return round2(n);};
  const attendanceByName=new Map(attendance.slice(1).filter(x=>x[0]&&x[1]&&x.slice(2,33).some(v=>v!=null)).map(x=>[augustEmployee(s,x[0]).id,x]));
  const seen=new Set(),salaryRows=rows.map((source,index)=>{
    const emp=augustEmployee(s,source[1]);if(seen.has(emp.id))throw new Error('Duplicate employee in August salary sheet: '+emp.name);seen.add(emp.id);
    const marks=attendanceByName.get(emp.id);if(!marks)throw new Error('Missing August attendance for '+emp.name);
    const daily={};for(let day=1;day<=31;day++){const mark=String(marks[day+1]||'').trim().toUpperCase();if(!{P:1,A:1,HD:1}[mark])throw new Error('Invalid attendance on August '+day+' for '+emp.name);daily[day]=mark==='HD'?'H':mark;}
    const monthlySalary=amount(source[3],emp.name+' salary'),paidDays=amount(source[4],emp.name+' paid days'),advanceDeduction=amount(source[7]||0,emp.name+' advance'),net=amount(source[9],emp.name+' net salary'),cash=amount(source[15]||0,emp.name+' cash'),bank=amount(source[16]||0,emp.name+' 3645');
    const earned=round2(monthlySalary/30*paidDays),adjustment=round2(net+advanceDeduction-earned);
    const actual=(s.salaryPayments||[]).filter(p=>p.active!==false&&p.ym==='2026-08'&&p.empId===emp.id);
    const actualCash=round2(actual.filter(p=>p.account==='Gagan Sir Cash').reduce((n,p)=>n+num(p.amount),0)),actualBank=round2(actual.filter(p=>p.account==='Prashant Axis 3645').reduce((n,p)=>n+num(p.amount),0));
    if(actual.length&&actual.some(p=>!p.proof&&!((p.proofs||[]).length)))throw new Error(emp.name+' has a salary payment without proof; review it before importing.');
    const additionalPaid=round2(actualBank-bank),laterBank=actual.filter(p=>p.account==='Prashant Axis 3645'&&p.date>'2026-09-12'),laterBankTotal=round2(laterBank.reduce((n,p)=>n+num(p.amount),0));
    if(Math.abs(actualCash-cash)>.005||additionalPaid<-.005||actual.some(p=>!['Gagan Sir Cash','Prashant Axis 3645'].includes(p.account))||(additionalPaid>.005&&(Math.abs(laterBankTotal-additionalPaid)>.005||additionalPaid>Math.max(0,round2(net-cash-bank))+.005)))throw new Error(emp.name+' paid amounts differ from the existing cash/3645 payments. Do not duplicate or rewrite payments.');
    return {empId:emp.id,name:emp.name,monthlySalary,paidDays,earned,adjustment,advanceDeduction,net,cash,bank,additionalPaid,attendance:daily,sourceRow:index+2};
  });
  const sum=(list,key)=>round2(list.reduce((n,x)=>n+num(x[key]),0));
  if(sum(salaryRows,'cash')!==171000||sum(salaryRows,'bank')!==102703||sum(salaryRows,'advanceDeduction')!==81850)throw new Error('The final workbook must show ₹1,71,000 cash, ₹1,02,703 from 3645 and ₹81,850 advances.');
  const advances=items.map((source,index)=>{
    const emp=augustEmployee(s,source[1]),dateParts=XLSX.SSF.parse_date_code(Number(source[0]));if(!dateParts)throw new Error('Invalid advance date in row '+(index+2));
    const date=[dateParts.y,String(dateParts.m).padStart(2,'0'),String(dateParts.d).padStart(2,'0')].join('-');
    return {empId:emp.id,name:emp.name,date,amount:amount(source[2],emp.name+' advance'),account:source[3]==null?'':String(source[3]).trim(),sourceRow:index+2};
  });
  if(sum(advances,'amount')!==81850)throw new Error('The ADV tab must total ₹81,850.');
  for(const row of salaryRows){const allocated=sum(advances.filter(a=>a.empId===row.empId),'amount');if(allocated!==row.advanceDeduction)throw new Error(row.name+' advance deduction does not equal their ADV rows.');}
  // Existing proof-backed advances are evidence of actual payouts. A workbook
  // cannot silently replace their dates or amounts, or create a second payout.
  const unmatchedAdvances=Object.values(s.advances||{}).filter(a=>a.active!==false&&a.date>='2026-08-01'&&a.date<='2026-09-12'&&!advances.some(x=>x.empId===a.empId&&x.date===a.date&&Math.abs(num(a.amount)-x.amount)<.005)).map(a=>({id:a.id,name:(s.employees[a.empId]||{}).name||a.employeeName,date:a.date,amount:round2(a.amount),recovered:advanceRecovered(a),proofBacked:!!(a.proof||(a.proofs||[]).length)}));
  const extras=Object.values(s.employees).filter(e=>employeeInPayrollMonth(e,'2026-08')&&!seen.has(e.id)).map(e=>({empId:e.id,name:e.name,existingPayments:(s.salaryPayments||[]).filter(p=>p.active!==false&&p.ym==='2026-08'&&p.empId===e.id).length}));
  if(extras.some(x=>x.existingPayments))throw new Error('An employee missing from the workbook has recorded August payments; review before importing.');
  const hash=crypto.createHash('sha256').update(file.buffer).digest('hex');
  const stateHash=crypto.createHash('sha256').update(JSON.stringify({aug:s.months['2026-08'],advances:s.advances,payments:(s.salaryPayments||[]).filter(p=>p.ym==='2026-08')})).digest('hex');
  const supplemental=sum(salaryRows,'additionalPaid');
  return {hash,stateHash,salaryRows,advances,extras,unmatchedAdvances,totals:{cash:171000,bank:102703,supplemental,actualBank:round2(102703+supplemental),actualPaid:round2(273703+supplemental),advance:81850,net:sum(salaryRows,'net')},previewToken:crypto.createHash('sha256').update(hash+stateHash).digest('hex')};
}
function applyFinalAugustPlan(s,plan,by,fileName){
  if(plan.unmatchedAdvances.length)throw new Error('Existing advances disagree with the final sheet: '+plan.unmatchedAdvances.map(a=>a.name+' ₹'+a.amount+' on '+a.date+' ('+a.id+')').join('; ')+'. No changes saved; review the original payment proof before correcting these records.');
  const ym='2026-08',mo=ensureMonth(s,ym),at=new Date().toISOString(),before=JSON.parse(JSON.stringify({rows:mo.rows,attendance:mo.attendance,advances:s.advances}));
  s.advances=s.advances||{};
  for(const a of plan.advances){
    const sourceKey='final-aug-2026:'+plan.hash+':ADV:'+a.sourceRow;
    let matches=Object.values(s.advances).filter(x=>x.active!==false&&(x.sourceKey===sourceKey||(x.empId===a.empId&&x.date===a.date&&Math.abs(num(x.amount)-a.amount)<.005)));
    if(matches.length>1)throw new Error('Ambiguous existing advance for '+a.name+' on '+a.date);
    let record=matches[0];
    if(!record){s.advanceSeq=(s.advanceSeq||0)+1;const id='ADV-'+String(s.advanceSeq).padStart(5,'0');record=s.advances[id]={id,empId:a.empId,employeeName:a.name,amount:a.amount,date:a.date,account:a.account,proof:'',proofs:[],sourceKey,historicalImport:true,historicalOpening:true,reference:'AUG-2026-SHEET-'+a.sourceRow,note:'Historical advance from final August workbook; no new account debit',recoveryStartMonth:ym,recoveries:[],active:true,createdAt:at,createdBy:by};}
    else if(Math.abs(num(record.amount)-a.amount)>.005)throw new Error('Existing advance amount changed for '+a.name);
    const recovered=advanceRecovered(record);if(recovered>a.amount+.005)throw new Error('Existing advance over-recovered for '+a.name);
    if(recovered<a.amount-.005)record.recoveries.push({ym,amount:round2(a.amount-recovered),by,at,source:'final_august_workbook',sourceRow:a.sourceRow});
  }
  for(const x of plan.salaryRows){
    const row=mo.rows[x.empId]=mo.rows[x.empId]||{};
    Object.assign(row,{sheetMonthlySalaryOverride:x.monthlySalary,sheetPaidDaysOverride:x.paidDays,sheetPaidDaysReason:'Final August workbook · attendance and paid-off rule',sheetOpeningCarryOverride:0,salaryAdjustment:x.adjustment,finalSalaryReason:'Final August workbook · signed addition/deduction',advance:0,paid:0,remarks:row.remarks||'Final August workbook'});
    delete row.finalSalaryAmount;delete row.historicalPaidDays;delete row.historicalCloseAdjustment;delete row.sheetExcluded;
    mo.attendance[x.empId]=x.attendance;
  }
  for(const x of plan.extras){const row=mo.rows[x.empId]=mo.rows[x.empId]||{};Object.assign(row,{sheetExcluded:true,sheetOpeningCarryOverride:0,sheetPaidDaysOverride:0,salaryAdjustment:0,advance:0,paid:0});delete row.historicalCloseAdjustment;delete row.finalSalaryAmount;}
  mo.finalized=false;
  const actual=computeMonth(s,ym),mismatch=plan.salaryRows.find(x=>{const r=actual.find(y=>y.id===x.empId);return !r||Math.abs(r.netPayable-x.net)>.015||Math.abs(r.transactionPaid-(x.cash+x.bank+x.additionalPaid))>.005;});
  if(mismatch)throw new Error('August payroll did not reconcile for '+mismatch.name+'; no changes saved.');
  const posting=(s.payrollPostings||{})[ym];if(posting){posting.rows=actual.map(r=>({empId:r.id,employeeName:r.name,salaryAmt:r.salaryAmt,netPayable:r.netPayable,paid:r.paid,advanceRecovery:r.loggedAdvanceRecovery,legacyAdvance:0,legacyPaid:0}));posting.correctedAt=at;posting.correctedBy=by;}
  s.finalAugustSheetAudit=s.finalAugustSheetAudit||[];s.finalAugustSheetAudit.push({at,by,fileName:path.basename(fileName),hash:plan.hash,totals:plan.totals,before,employeeIds:plan.salaryRows.map(x=>x.empId),source:'Owner-confirmed August workbook; proof-backed salary debits retained'});
  return {rows:actual,totals:plan.totals};
}
router.post('/api/salary/final-august/preview',guard,receiveSalarySheet,(req,res)=>{
  if(!isOwner(req))return res.status(403).json({success:false,error:'Only the Owner can apply the final historical workbook.'});
  try{const plan=finalAugustPlan(load(),req.file);res.json({success:true,previewToken:plan.previewToken,salaryRows:plan.salaryRows.map(({attendance,...row})=>row),advances:plan.advances,extras:plan.extras,unmatchedAdvances:plan.unmatchedAdvances,totals:plan.totals});}catch(err){res.status(400).json({success:false,error:err.message});}
});
router.post('/api/salary/final-august/apply',guard,receiveSalarySheet,(req,res)=>{
  if(!isOwner(req))return res.status(403).json({success:false,error:'Only the Owner can apply the final historical workbook.'});
  try{const s=load(),plan=finalAugustPlan(s,req.file);if(plan.previewToken!==String(req.body&&req.body.previewToken||''))return res.status(409).json({success:false,error:'Workbook or live payroll changed after preview. Preview again.'});
    if((s.finalAugustSheetAudit||[]).some(x=>x.hash===plan.hash))return res.status(409).json({success:false,error:'This exact final workbook was already applied.'});
    const copy=JSON.parse(JSON.stringify(s)),result=applyFinalAugustPlan(copy,plan,req.user.username,req.file.originalname);save(copy);res.json({success:true,employees:result.rows.length,advances:plan.advances.length,totals:result.totals,noNewBankOrCashDebits:true});
  }catch(err){res.status(400).json({success:false,error:err.message});}
});
router.get('/api/salary/import/template',guard,(req,res)=>{
  const sheet=XLSX.utils.aoa_to_sheet([['Employee ID','Employee Name','Paid Days','Final Salary Amount','Remarks']]);
  const workbook=XLSX.utils.book_new();XLSX.utils.book_append_sheet(workbook,sheet,'Salary');
  res.setHeader('Content-Type','application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
  res.setHeader('Content-Disposition','attachment; filename="salary-import-template.xlsx"');
  res.send(XLSX.write(workbook,{type:'buffer',bookType:'xlsx'}));
});
router.post('/api/salary/import/:ym/preview',guard,receiveSalarySheet,(req,res)=>{
  try{const result=salarySheetChanges(load(),req.params.ym,req.file);res.json({success:true,ym:req.params.ym,changes:result.changes,hash:result.hash,previewToken:result.previewToken});}
  catch(err){res.status(400).json({success:false,error:err.message});}
});
router.post('/api/salary/import/:ym/apply',guard,receiveSalarySheet,(req,res)=>{
  const reason=String((req.body||{}).reason||'').trim();if(!reason)return res.status(400).json({success:false,error:'Enter a reason for importing or changing the salary sheet.'});
  try{
    const s=load(),result=salarySheetChanges(s,req.params.ym,req.file);
    if(result.previewToken!==String((req.body||{}).previewToken||''))return res.status(409).json({success:false,error:'The sheet or current payroll changed after preview. Preview it again before applying.'});
    applySalarySheetChanges(s,req.params.ym,result,reason,req.user&&req.user.username||'admin',req.file.originalname);
    save(s);res.json({success:true,employees:result.changes.length,ym:req.params.ym});
  }catch(err){res.status(400).json({success:false,error:err.message});}
});

// Correct the earned amount for one employee/month; never invent a payment.
router.patch('/api/salary/final-amount/:ym/:empId', guard, (req, res) => {
  if(!canApproveAdvance(req))return res.status(403).json({success:false,error:'Only a salary manager can change the final salary amount.'});
  const ym=String(req.params.ym||''),empId=String(req.params.empId||''),b=req.body||{},reason=String(b.reason||'').trim(),usingAdjustment=b.adjustment!==undefined,raw=usingAdjustment?b.adjustment:b.amount,amount=Number(raw);
  if(!/^\d{4}-(0[1-9]|1[0-2])$/.test(ym))return res.status(400).json({success:false,error:'Choose a valid payroll month.'});
  if(raw==null||String(raw).trim()===''||!Number.isFinite(amount)||Math.abs(amount)>100000000||(!usingAdjustment&&amount<0))return res.status(400).json({success:false,error:'Enter a valid signed adjustment or final salary amount.'});
  if(!reason)return res.status(400).json({success:false,error:'Explain why the final salary was changed.'});
  const s=load(),emp=s.employees[empId];
  if(!emp||!employeeInPayrollMonth(emp,ym))return res.status(404).json({success:false,error:'Employee is not in this payroll month.'});
  const before=computeMonth(s,ym).find(x=>x.id===empId),mo=ensureMonth(s,ym),row=mo.rows[empId]=mo.rows[empId]||{},at=new Date().toISOString(),by=req.user&&req.user.username||'Owner';
  if(usingAdjustment&&round2(before.calculatedSalaryAmt+amount)<0)return res.status(400).json({success:false,error:'Adjusted salary cannot be negative.'});
  row.salaryAdjustment=usingAdjustment?round2(amount):round2(amount-before.calculatedSalaryAmt);delete row.finalSalaryAmount;row.finalSalaryReason=reason;row.finalSalaryEditedAt=at;row.finalSalaryEditedBy=by;
  const after=computeMonth(s,ym).find(x=>x.id===empId);
  s.finalSalaryAudit=s.finalSalaryAudit||[];
  s.finalSalaryAudit.push({ym,empId,employeeName:emp.name,at,by,previousAmount:before.salaryAmt,calculatedAmount:after.calculatedSalaryAmt,adjustment:after.salaryAdjustment,finalAmount:after.salaryAmt,reason});
  const posting=(s.payrollPostings||{})[ym];
  if(posting){
    posting.rows=posting.rows||[];
    let posted=posting.rows.find(x=>x.empId===empId);
    if(!posted){posted={empId,employeeName:emp.name,advanceRecovery:after.loggedAdvanceRecovery,legacyAdvance:after.legacyAdvance,legacyPaid:after.legacyPaid,paid:after.paid};posting.rows.push(posted);}
    posted.salaryAmt=after.salaryAmt;posted.netPayable=after.netPayable;posting.correctedAt=at;posting.correctedBy=by;
  }
  save(s);res.json({success:true,row:after});
});

// Mark one attendance cell. mark='' clears it.
router.post('/api/salary/attendance/:ym', guard, (req, res) => {
  const s = load(); const mo = ensureMonth(s, req.params.ym); const b = req.body || {};
  if (b.empId && b.day) {
    mo.attendance[b.empId] = mo.attendance[b.empId] || {};
    let mark = b.mark;
    const emp=s.employees[b.empId],dateText=req.params.ym+'-'+String(b.day).padStart(2,'0'),date=new Date(dateText+'T00:00:00Z'),weekday=!isNaN(date)?WEEK_DAYS[date.getUTCDay()]:'';
    if(emp&&((emp.joiningDate&&dateText<emp.joiningDate)||(emp.lastWorkingDate&&dateText>emp.lastWorkingDate)))return res.status(400).json({success:false,error:'This date is outside the employee’s employment period.'});
    if((mark==='A'||mark==='PL')&&emp)mark=normalizedAbsentMark(emp,req.params.ym,b.day);
    if (mark && MARKS[mark] != null) mo.attendance[b.empId][b.day] = mark;
    else delete mo.attendance[b.empId][b.day];
    if(mo.rows[b.empId]){delete mo.rows[b.empId].paidDays;delete mo.rows[b.empId].sheetPaidDaysOverride;}
    b.savedMark=mark;
  }
  save(s);
  res.json({ success: true, mark:b.savedMark || '' });
});

// Atomically replace a supplied attendance range for several employees.
// Every item is validated before salary.json is changed, preventing partial imports.
router.post('/api/salary/attendance/:ym/batch', guard, (req, res) => {
  const ym=String(req.params.ym||''),b=req.body||{},items=Array.isArray(b.items)?b.items:[];
  if(!/^\d{4}-\d{2}$/.test(ym)||!items.length)return res.status(400).json({success:false,error:'Choose a valid month and at least one employee.'});
  const s=load(),dim=daysInMonth(ym),seen=new Set(),prepared=[];
  for(const item of items){
    const requestedName=String(item.employeeName||'').trim();
    const emp=item.empId?s.employees[item.empId]:Object.values(s.employees).find(e=>String(e.name||'').localeCompare(requestedName,'en',{sensitivity:'base'})===0);
    if(!emp||seen.has(emp.id))return res.status(400).json({success:false,error:'An employee is missing or duplicated: '+(requestedName||item.empId||'unknown')});
    const marks=Array.isArray(item.marks)?item.marks:[];
    if(marks.length>dim)return res.status(400).json({success:false,error:'Too many attendance days for '+emp.name+'.'});
    const normalized={};
    for(let i=0;i<marks.length;i++){
      const day=String(i+1).padStart(2,'0'),dateText=ym+'-'+day,raw=String(marks[i]||'').trim().toUpperCase(),mark=raw==='HD'?'H':raw;
      if(mark&&!Object.prototype.hasOwnProperty.call(MARKS,mark))return res.status(400).json({success:false,error:'Invalid mark for '+emp.name+' on '+dateText+'.'});
      if((emp.joiningDate&&dateText<emp.joiningDate)||(emp.lastWorkingDate&&dateText>emp.lastWorkingDate)){normalized[day]='';continue;}
      const weekday=WEEK_DAYS[new Date(dateText+'T00:00:00Z').getUTCDay()];
      normalized[day]=(mark==='A'||mark==='PL')?normalizedAbsentMark(emp,ym,day):mark;
    }
    seen.add(emp.id);prepared.push({emp,normalized});
  }
  const mo=ensureMonth(s,ym);
  prepared.forEach(({emp,normalized})=>{mo.attendance[emp.id]=mo.attendance[emp.id]||{};Object.entries(normalized).forEach(([day,mark])=>{if(mark)mo.attendance[emp.id][day]=mark;else delete mo.attendance[emp.id][day];});if(mo.rows[emp.id]){delete mo.rows[emp.id].paidDays;delete mo.rows[emp.id].sheetPaidDaysOverride;}});
  save(s);
  res.json({success:true,employees:prepared.length,cells:prepared.reduce((n,x)=>n+Object.keys(x.normalized).length,0),rows:computeMonth(s,ym).filter(r=>seen.has(r.id)).map(r=>({empId:r.id,name:r.name,paidDays:r.computedPaidDays}))});
});

// Edit a payroll row. Paid days may be entered manually only when attendance is absent.
router.post('/api/salary/row/:ym', guard, (req, res) => {
  const s = load(); const mo = ensureMonth(s, req.params.ym); const b = req.body || {};
  if (!b.empId) return res.status(400).json({ success: false, error: 'empId required' });
  if(!s.employees[b.empId]||!employeeInPayrollMonth(s.employees[b.empId],req.params.ym))return res.status(400).json({success:false,error:'This employee is outside the selected payroll month.'});
  const row = mo.rows[b.empId] = mo.rows[b.empId] || {};
  if (b.paidDays !== undefined) {
    const computed=attPaidDays(employmentAttendance((mo.attendance||{})[b.empId],s.employees[b.empId],req.params.ym),s.employees[b.empId],req.params.ym);
    if(computed!=null)return res.status(409).json({success:false,error:'Paid days come from attendance for this employee. Update the Attendance tab instead.'});
    row.paidDays = (b.paidDays === '' || b.paidDays === null) ? null : num(b.paidDays);delete row.sheetPaidDaysOverride;
  }
  if (b.advance !== undefined) row.advance = num(b.advance);
  if (b.paid !== undefined) row.paid = num(b.paid);
  if (b.remarks !== undefined) row.remarks = String(b.remarks);
  save(s);
  res.json({ success: true });
});

router.post('/api/salary/divisor', guard, (req, res) => {
  const s = load(); const d = num((req.body || {}).divisor);
  if (d > 0) s.divisor = d;
  save(s);
  res.json({ success: true, divisor: s.divisor });
});

// ── One-time seed from the founder's JUL SALARY sheet (21 staff + July run) ──
// Runs only when no salary.json exists yet, so it never clobbers real edits.
function seedIfEmpty() {
  if (fs.existsSync(SAL_PATH)) return;
  // [name, post, salary, channel, paidDays, advance, paid, remarks]  (July 2026)
  const SEED = [
    ['ARSHPREET SINGH', 'MANAGER', 30000, 'Shared', 30, 52000, 5000, '2000 Last Month + 50000 This Month'],
    ['Guard', 'Security', 15000, 'Shared', 30, 3000, 12000, ''],
    ['SUNNY SHARMA', 'SALES EXECUTIVE', 24000, 'POS', 22, 20000, 0, '7000 Last Month'],
    ['PARDEEP', 'EXECUTIVE', 19000, 'Shared', 21.5, 0, 13500, ''],
    ['NANDANI', 'SALES EXECUTIVE', 17000, 'POS', 29, 0, 16500, ''],
    ['SHIVAM', 'SALES EXECUTIVE', 17000, 'POS', 30, 1500, 15500, ''],
    ['ISHA', 'SALES EXECUTIVE', 18000, 'POS', 30, 100, 18000, ''],
    ['HITESH', 'PHOTOGRAPHER', 20000, 'Website', 27, 0, 18000, ''],
    ['AJAY', 'VIDEO EDITOR', 18000, 'Website', 29, 0, 17500, 'Salary Delay'],
    ['PIYUSH', 'TAILOR', 18500, 'Shared', 28, 0, 17000, ''],
    ['UMAIR', 'DRIVER', 28000, 'Shared', 27.5, 17000, 9000, ''],
    ['TUSHAR(INV)', 'PACKING HELPER', 15000, 'Shared', 31, 2000, 13500, ''],
    ['NIDA', 'LOGISTIC', 25000, 'Shared', 29, 0, 24000, ''],
    ['GAURAV', 'EDITOR', 18500, 'Website', 28.5, 0, 17500, ''],
    ['PRASHANT', 'ACCOUNTS', 26000, 'Shared', 29.5, 17467, 13000, '7467 Last Month + 10000 This Month'],
    ['Suraj(OB)', 'Office Boy', 18000, 'Shared', 25, 1500, 13500, ''],
    ['TUSHAR(MOD)', 'Model', 30000, 'Website', 30, 0, 30000, ''],
    ['Indervir', 'ACCOUNTS', 17000, 'Shared', 28.5, 4000, 12000, ''],
    ['CSH', 'MARKETING', 15000, 'Website', 30, 0, 15000, 'Final: 30 paid days'],
    ['Ravi', 'DRIVER', 26000, 'Shared', 9, 6500, 4000, ''],
    ['Pooja', 'MODEL', 20000, 'Website', 10, 0, 6500, '']
  ];
  const s = blank();
  const now = new Date().toISOString();
  const rows = {};
  SEED.forEach((r, i) => {
    s.seq = i + 1;
    const id = 'E' + String(s.seq).padStart(3, '0');
    s.employees[id] = { id, name: r[0], post: r[1], salary: r[2], channel: r[3], note: r[7] || '', active: true, createdAt: now };
    rows[id] = { paidDays: r[4], advance: r[5], paid: r[6], remarks: r[7] || '' };
  });
  s.months['2026-07'] = { finalized: false, rows, attendance: {} };
  try { save(s); } catch (_) { /* volume not ready → skip; endpoints will seed lazily via load() default */ }
}
seedIfEmpty();

module.exports = { router, summaryForPL, _july2026Import:JULY_2026_IMPORT, _providedAdvanceImport:PROVIDED_ADVANCE_IMPORT, _finalJuly2026Payroll:FINAL_JULY_2026_PAYROLL, _finalAugust2026Advances:FINAL_AUGUST_2026_ADVANCES, _julyImportedMarks:julyImportedMarks, _findImportedEmployee:findImportedEmployee, _ensureHistoricalGuard:ensureHistoricalGuard, _repairGuardSunnyCollision:repairGuardSunnyCollision, _applySunnyGuardAndSurajRepair:applySunnyGuardAndSurajRepair, _removeHistoricalAdvancesV16:removeHistoricalAdvancesV16, _closeHistoricalPayrollCarryV17:closeHistoricalPayrollCarryV17, _salarySheetChanges:salarySheetChanges, _applySalarySheetChanges:applySalarySheetChanges, _advanceSheetRows:advanceSheetRows, _applyAdvanceSheetRows:applyAdvanceSheetRows, _advanceSourceSheet:advanceSourceSheet, _finalAugustPlan:finalAugustPlan, _applyFinalAugustPlan:applyFinalAugustPlan };

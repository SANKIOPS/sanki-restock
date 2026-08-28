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
const router = express.Router();

const DATA_DIR = process.env.DATA_PATH ? path.dirname(process.env.DATA_PATH) : path.join(__dirname, '..');
const SAL_PATH = path.join(DATA_DIR, 'salary.json');

function num(v) { const n = parseFloat(v); return isNaN(n) ? 0 : n; }
function round0(n) { return Math.round(n); }
function round2(n) { return Math.round(n * 100) / 100; }
function byEmployeeName(a, b) { return String(a.name || a.employeeName || '').localeCompare(String(b.name || b.employeeName || ''), 'en', { sensitivity:'base', numeric:true }); }

const CHANNELS = ['POS', 'Website', 'Shared'];
const SALARY_PAYING_ACCOUNTS = ['Gagan Sir Cash', 'Counter Cash'];
const WEEK_DAYS = ['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'];
// Paid-day value per attendance mark: Present 1, Half 0.5, Paid-leave 1,
// Week-off 1 (paid), Absent 0.
const MARKS = { P: 1, H: 0.5, PL: 1, WO: 1, A: 0 };

function blank() { return { employees: {}, months: {}, divisor: 30, seq: 0, advances: {}, advanceSeq: 0, advanceAudit: [], payrollPostings:{}, salaryPayments:[], salaryPaymentBatchSeq:0, oneTimeMigrations:{} }; }
function load() {
  try {
    const s=Object.assign(blank(), JSON.parse(fs.readFileSync(SAL_PATH, 'utf8')));
    const julyImported=applyJuly2026AttendanceAndPayroll(s);
    const employeeRepair=applySunnyGuardAndSurajRepair(s);
    const correctedJuly=applyCorrectedJulyAttendanceV7(s);
    if(julyImported||employeeRepair||correctedJuly) save(s);
    return s;
  } catch { return blank(); }
}
function save(s) { const tmp = SAL_PATH + '.tmp-' + process.pid + '-' + Date.now(); fs.writeFileSync(tmp, JSON.stringify(s)); fs.renameSync(tmp, SAL_PATH); }

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

function findImportedEmployee(s,name,post){
  const target=String(name||'').replace(/\s*\([^)]*\)\s*/g,'').trim().toLowerCase();
  const sameName=Object.values(s.employees||{}).filter(e=>{const live=String(e.name||'').replace(/\s*\([^)]*\)\s*/g,'').trim().toLowerCase();return live===target||live.startsWith(target+' ')||target.startsWith(live+' ');});
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
  const attendance={},raw=String(encoded||''); let worked=0;
  for(let i=0;i<31;i++){
    const source=raw[i]||'',day=String(i+1).padStart(2,'0');
    if(source==='-'||!source)continue;
    if(source==='P'){attendance[day]='P';worked++;continue;}
    if(source==='H'){attendance[day]='H';worked+=.5;continue;}
    if(source==='A'){
      const date='2026-07-'+day,weekday=WEEK_DAYS[new Date(date+'T00:00:00Z').getUTCDay()],isWeekOff=!!emp.weekOffDay&&emp.weekOffDay===weekday;
      attendance[day]=isWeekOff?'WO':'A';
    }
  }
  return {attendance,paidDays:Math.max(0,round2(worked+paidLeaveAllowanceForMonth(emp,'2026-07')-1))};
}
function applyCorrectedJulyAttendanceV7(s){
  const key='corrected_july_attendance_fixed_leave_allowance_v9';s.oneTimeMigrations=s.oneTimeMigrations||{};
  if(s.oneTimeMigrations[key]||((s.payrollPostings||{})['2026-07'])||(((s.months||{})['2026-07']||{}).finalized))return false;
  const mo=ensureMonth(s,'2026-07'),updated=[];
  JULY_2026_IMPORT.forEach(([name,post,marks,,joiningDate])=>{
    const emp=findImportedEmployee(s,name,post);if(!emp)return;if(joiningDate)emp.joiningDate=joiningDate;else if(emp.joiningDate&&emp.joiningDate>'2026-07-01')emp.joiningDate='2026-07-01';emp.monthlyPaidLeaveAllowance=String(name).toLowerCase()==='suraj'?1:4;
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
function guard(req, res, next) {
  const r = rolesOf(req);
  if (r.includes('admin') || r.includes('accounting') || r.includes('owner')) return next();
  return res.status(403).json({ success: false, error: 'Salary is admin/accounting only.' });
}

function daysInMonth(ym) { const p = String(ym).split('-').map(Number); return new Date(p[0], p[1], 0).getDate(); }
function attPaidDays(att,e,ym) {
  if (!att) return null;
  let worked=0,any=false;
  Object.keys(att).forEach(d=>{const mark=att[d];if(MARKS[mark]!=null){any=true;if(mark==='P')worked++;else if(mark==='H')worked+=.5;}});
  return any?round2(worked+paidLeaveAllowanceForMonth(e,ym)):null;
}
function employmentAttendance(att, e, ym) {
  if (!att) return att;
  const filtered={}; Object.keys(att).forEach(day=>{const date=ym+'-'+String(day).padStart(2,'0');if(e.joiningDate&&date<e.joiningDate)return;if(e.lastWorkingDate&&date>e.lastWorkingDate)return;filtered[day]=att[day];}); return filtered;
}
function employeeInPayrollMonth(e,ym){
  const joinMonth=String(e.joiningDate||'').slice(0,7),leaveMonth=String(e.lastWorkingDate||'').slice(0,7);
  return !(joinMonth&&ym<joinMonth)&&!(leaveMonth&&ym>leaveMonth);
}
function paidLeaveAllowanceForMonth(e,ym){
  const base=e.monthlyPaidLeaveAllowance==null?4:Math.max(0,num(e.monthlyPaidLeaveAllowance)),dim=daysInMonth(ym);
  if(!base||!employeeInPayrollMonth(e,ym))return 0;
  const monthStart=ym+'-01',monthEnd=ym+'-'+String(dim).padStart(2,'0'),start=e.joiningDate&&e.joiningDate>monthStart?e.joiningDate:monthStart,end=e.lastWorkingDate&&e.lastWorkingDate<monthEnd?e.lastWorkingDate:monthEnd;
  if(start===monthStart&&end===monthEnd)return base;
  if(start>end)return 0;
  if(e.weekOffDay){let count=0;for(let day=1;day<=dim;day++){const date=ym+'-'+String(day).padStart(2,'0');if(date<start||date>end)continue;if(WEEK_DAYS[new Date(date+'T00:00:00Z').getUTCDay()]===e.weekOffDay)count++;}return Math.min(base,count);}
  const employedDays=Math.floor((Date.parse(end)-Date.parse(start))/86400000)+1;return Math.min(base,Math.floor(base*employedDays/dim));
}
function ensureMonth(s, ym) { if (!s.months[ym]) s.months[ym] = { finalized: false, rows: {}, attendance: {} }; return s.months[ym]; }
function advanceRecovered(a) { return round2((a.recoveries || []).reduce((n, x) => n + num(x.amount), 0)); }
function advanceOutstanding(a) { return round2(Math.max(0, num(a.amount) - advanceRecovered(a))); }
function advanceStatus(a) { const r = advanceRecovered(a); return r <= 0 ? 'Outstanding' : (r + .001 >= num(a.amount) ? 'Recovered' : 'Partially recovered'); }
function advanceView(a) { return Object.assign({}, a, { recovered: advanceRecovered(a), outstanding: advanceOutstanding(a), status: a.active === false ? 'Cancelled' : advanceStatus(a) }); }
function monthRecovery(s, empId, ym) { return round2(Object.values(s.advances || {}).filter(a => a.active !== false && a.empId === empId).reduce((n, a) => n + (a.recoveries || []).filter(r => r.ym === ym).reduce((m, r) => m + num(r.amount), 0), 0)); }
function auditAdvance(s, req, action, advanceId, details) { s.advanceAudit = s.advanceAudit || []; s.advanceAudit.push({ at: new Date().toISOString(), by: req.user && req.user.username || 'admin', action, advanceId, details: details || {} }); }

// Compute a month's payroll rows for every employee.
function employeeMonthBase(s,e,ym){
  const mo = s.months[ym] || { rows: {}, attendance: {} };
  const div = num(s.divisor) || 30;
  const row=(mo.rows||{})[e.id]||{},computed=attPaidDays(employmentAttendance((mo.attendance||{})[e.id],e,ym),e,ym),paidDays=row.paidDays!=null?num(row.paidDays):computed;
  const salaryAmt=paidDays!=null?(num(e.salary)/div*paidDays):0,legacyAdvance=num(row.advance),loggedAdvanceRecovery=monthRecovery(s,e.id,ym),currentAdvance=round2(legacyAdvance+loggedAdvanceRecovery);
  const legacyPaid=num(row.paid),transactionPaid=round2((s.salaryPayments||[]).filter(p=>p.empId===e.id&&p.ym===ym&&p.active!==false).reduce((n,p)=>n+num(p.amount),0)),paid=round2(legacyPaid+transactionPaid);
  return {row,computed,paidDays,salaryAmt,legacyAdvance,loggedAdvanceRecovery,currentAdvance,legacyPaid,transactionPaid,paid};
}
function payrollCarryIn(s,e,ym){
  let carry=0;
  Object.keys(s.months||{}).filter(m=>m<ym).sort().forEach(m=>{
    if(!employeeInPayrollMonth(e,m))return;
    const x=employeeMonthBase(s,e,m),net=x.salaryAmt-x.currentAdvance-carry,balance=net-x.paid;
    carry=round2(Math.max(0,-balance));
  });
  return carry;
}
function computeMonth(s, ym) {
  return Object.values(s.employees).filter(e=>employeeInPayrollMonth(e,ym)).sort(byEmployeeName).map(e => {
    const x=employeeMonthBase(s,e,ym),openingAdvanceCarry=payrollCarryIn(s,e,ym),advance=round2(x.currentAdvance+openingAdvanceCarry),netPayable=x.salaryAmt-advance;
    return {
      id: e.id, name: e.name, post: e.post, channel: e.channel, weekOffDay: e.weekOffDay || '', joiningDate:e.joiningDate||'', lastWorkingDate:e.lastWorkingDate||'', active: e.active !== false,
      salary: num(e.salary), paidDays:x.paidDays, computedPaidDays:x.computed,
      salaryAmt: round2(x.salaryAmt), advance, currentAdvance:x.currentAdvance, openingAdvanceCarry, legacyAdvance:x.legacyAdvance, loggedAdvanceRecovery:x.loggedAdvanceRecovery, netPayable: round2(netPayable),
      outstandingAdvance: round2(Object.values(s.advances || {}).filter(a => a.active !== false && a.empId === e.id).reduce((n, a) => n + advanceOutstanding(a), 0)),
      paid:x.paid, legacyPaid:x.legacyPaid, transactionPaid:x.transactionPaid, balance:round2(netPayable-x.paid), carryForwardAdvance:round2(Math.max(0,-(netPayable-x.paid))),remarks:x.row.remarks||''
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

// ── Employee master ──
router.get('/api/salary/employees', guard, (req, res) => {
  const s = load();
  res.json({ success: true, employees: Object.values(s.employees).sort(byEmployeeName), divisor: num(s.divisor) || 30, channels: CHANNELS, weekDays: WEEK_DAYS, salaryPayingAccounts:SALARY_PAYING_ACCOUNTS });
});
router.post('/api/salary/employees', guard, (req, res) => {
  const s = load(); const b = req.body || {};
  let id = b.id;
  if(id&&!s.employees[id])return res.status(404).json({success:false,error:'Employee record not found. Reload the page and try again.'});
  const requestedName=String(b.name||'').trim(),duplicate=!id&&Object.values(s.employees).find(e=>e.active!==false&&String(e.name||'').trim().localeCompare(requestedName,'en',{sensitivity:'base'})===0);
  if(duplicate)return res.status(409).json({success:false,duplicate:true,existingEmployee:{id:duplicate.id,name:duplicate.name,post:duplicate.post},error:duplicate.name+' already exists. Edit or reactivate the existing employee instead.'});
  if (!id) id=nextEmployeeId(s);
  const cur = s.employees[id] || {};
  const joiningDate=b.joiningDate!==undefined?String(b.joiningDate||'').slice(0,10):(cur.joiningDate||''),lastWorkingDate=b.lastWorkingDate!==undefined?String(b.lastWorkingDate||'').slice(0,10):(cur.lastWorkingDate||'');
  if((joiningDate&&!/^\d{4}-\d{2}-\d{2}$/.test(joiningDate))||(lastWorkingDate&&!/^\d{4}-\d{2}-\d{2}$/.test(lastWorkingDate)))return res.status(400).json({success:false,error:'Use valid joining and last-working dates.'});
  if(joiningDate&&lastWorkingDate&&lastWorkingDate<joiningDate)return res.status(400).json({success:false,error:'Last working date cannot be before the joining date.'});
  s.employees[id] = {
    id,
    name: String(b.name != null ? b.name : cur.name || '').trim(),
    post: String(b.post != null ? b.post : cur.post || '').trim(),
    salary: b.salary != null ? num(b.salary) : num(cur.salary),
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
  const s=load(),b=req.body||{},ym=String(b.ym||''),date=String(b.date||''),account=String(b.account||'').trim(),proof=String(b.proof||'').trim(),items=Array.isArray(b.items)?b.items:[];
  if(!/^\d{4}-\d{2}$/.test(ym)||!/^\d{4}-\d{2}-\d{2}$/.test(date)||!SALARY_PAYING_ACCOUNTS.includes(account)||!proof||!items.length)return res.status(400).json({success:false,error:'Choose employees, date, Gagan Sir Cash or Counter Cash, and payment proof.'});
  const rows=computeMonth(s,ym),seen=new Set(),prepared=[];for(const x of items){const row=rows.find(r=>r.id===x.empId),amount=round2(num(x.amount));if(!row||seen.has(x.empId)||!(amount>0)||amount>Math.max(0,row.balance)+.001)return res.status(400).json({success:false,error:'A payment is invalid or exceeds the employee’s remaining payable balance.'});seen.add(x.empId);prepared.push({row,amount});}
  s.salaryPaymentBatchSeq=(s.salaryPaymentBatchSeq||0)+1;const batchId='SALB-'+String(s.salaryPaymentBatchSeq).padStart(5,'0'),now=new Date().toISOString();s.salaryPayments=s.salaryPayments||[];prepared.forEach((x,i)=>s.salaryPayments.push({id:batchId+'-'+String(i+1).padStart(3,'0'),batchId,ym,empId:x.row.id,employeeName:x.row.name,amount:x.amount,date,account,proof,reference:String(b.reference||'').trim(),note:String(b.note||'').trim(),active:true,createdBy:req.user&&req.user.username||'admin',createdAt:now}));save(s);res.json({success:true,batchId,count:prepared.length,total:round2(prepared.reduce((n,x)=>n+x.amount,0))});
});
router.get('/api/salary/ledgers',guard,(req,res)=>{
  const s=load(),by={};const ensure=(id,name)=>by[id]||(by[id]={empId:id,name,ledgerName:(name||id)+' — Salary',entries:[]});
  Object.values(s.advances||{}).filter(a=>a.active!==false).forEach(a=>ensure(a.empId,a.employeeName).entries.push({id:a.id,date:a.date,kind:'advance',description:'Salary advance paid'+(a.note?' · '+a.note:''),debit:num(a.amount),credit:0,proof:a.proof||'',reference:a.reference||a.id}));
  (s.salaryPayments||[]).filter(p=>p.active!==false).forEach(p=>ensure(p.empId,p.employeeName).entries.push({id:p.id,date:p.date,kind:'salary_paid',description:p.ym+' salary payment'+(p.note?' · '+p.note:''),debit:num(p.amount),credit:0,proof:p.proof||'',reference:p.reference||p.batchId}));
  Object.values(s.payrollPostings||{}).forEach(p=>(p.rows||[]).forEach(r=>{const l=ensure(r.empId,r.employeeName),date=p.ym+'-'+String(daysInMonth(p.ym)).padStart(2,'0');if(num(r.salaryAmt))l.entries.push({id:p.ym+'/'+r.empId+'/EARNED',date,kind:'salary_earned',description:p.ym+' salary earned',debit:0,credit:num(r.salaryAmt),reference:p.ym});if(num(r.legacyPaid))l.entries.push({id:p.ym+'/'+r.empId+'/LEGACY-PAID',date,kind:'legacy_salary_paid',description:p.ym+' legacy paid amount',debit:num(r.legacyPaid),credit:0,reference:p.ym});}));
  const ledgers=Object.values(by).map(l=>{l.entries.sort((a,b)=>String(a.date+a.id).localeCompare(String(b.date+b.id)));let balance=0;l.entries.forEach(e=>{balance+=num(e.credit)-num(e.debit);e.balance=round2(balance);});l.balance=round2(balance);l.status=balance>0?'Company owes':balance<0?'Employee owes':'Settled';l.lastPostingDate=l.entries.at(-1)&&l.entries.at(-1).date||'';l.outstandingAdvance=round2(Object.values(s.advances||{}).filter(a=>a.active!==false&&a.empId===l.empId).reduce((n,a)=>n+advanceOutstanding(a),0));l.entries=l.entries.slice().reverse();return l;}).sort(byEmployeeName);
  res.json({success:true,ledgers});
});

// Salary advances are recoverable employee balances, not salary/P&L expenses.
router.get('/api/salary/advances', guard, (req, res) => {
  const s = load(), q = req.query || {};
  let rows = Object.values(s.advances || {}).map(advanceView);
  if (q.employee) rows = rows.filter(a => a.empId === q.employee);
  if (q.month) rows = rows.filter(a => String(a.date || '').slice(0, 7) === q.month);
  if (q.status) rows = rows.filter(a => a.status === q.status);
  if (q.account) rows = rows.filter(a => a.account === q.account);
  rows.sort((a, b) => String(b.date + b.id).localeCompare(String(a.date + a.id)));
  const summary = Object.values(s.employees).sort(byEmployeeName).map(e => {
    const all = Object.values(s.advances || {}).filter(a => a.active !== false && a.empId === e.id);
    const total = all.reduce((n, a) => n + num(a.amount), 0), recovered = all.reduce((n, a) => n + advanceRecovered(a), 0);
    return { empId: e.id, name: e.name, thisMonth: all.filter(a => String(a.date).slice(0, 7) === (q.summaryMonth || new Date().toISOString().slice(0, 7))).reduce((n, a) => n + num(a.amount), 0), total: round2(total), recovered: round2(recovered), outstanding: round2(total - recovered), transactions: all.map(advanceView).sort((a,b)=>String(b.date+b.id).localeCompare(String(a.date+a.id))) };
  }).filter(x => x.total || x.recovered);
  const totals = summary.reduce((t, x) => ({ total: t.total + x.total, recovered: t.recovered + x.recovered, outstanding: t.outstanding + x.outstanding }), { total: 0, recovered: 0, outstanding: 0 });
  res.json({ success: true, advances: rows, summary, totals, audit: (s.advanceAudit || []).slice().reverse().slice(0, 500) });
});

router.post('/api/salary/advances', guard, (req, res) => {
  const s = load(), b = req.body || {}, emp = s.employees[b.empId], amount = num(b.amount);
  if (!emp) return res.status(400).json({ success: false, error: 'Select an employee.' });
  if (!(amount > 0)) return res.status(400).json({ success: false, error: 'Enter a valid advance amount.' });
  if (!/^\d{4}-\d{2}-\d{2}$/.test(String(b.date || ''))) return res.status(400).json({ success: false, error: 'Select the payment date.' });
  if (!String(b.account || '').trim()) return res.status(400).json({ success: false, error: 'Select the paying account.' });
  if (!String(b.proof || '').trim()) return res.status(400).json({ success: false, error: 'Payment proof is required.' });
  const recoveryStartMonth = String(b.recoveryStartMonth || b.date.slice(0, 7));
  if (!/^\d{4}-\d{2}$/.test(recoveryStartMonth)) return res.status(400).json({ success: false, error: 'Select a recovery start month.' });
  s.advanceSeq = (s.advanceSeq || 0) + 1; const id = 'ADV-' + String(s.advanceSeq).padStart(5, '0'), now = new Date().toISOString();
  s.advances[id] = { id, empId: emp.id, employeeName: emp.name, amount: round2(amount), date: b.date, account: String(b.account).trim(), proof: String(b.proof).trim(), note: String(b.note || '').trim(), reference: String(b.reference || '').trim(), recoveryStartMonth, recoveries: [], active: true, createdBy: req.user && req.user.username || 'admin', createdAt: now };
  auditAdvance(s, req, 'CREATED', id, { amount, account: b.account }); save(s);
  res.json({ success: true, advance: advanceView(s.advances[id]) });
});

router.post('/api/salary/recoveries/:ym', guard, (req, res) => {
  const s = load(), b = req.body || {}, ym = req.params.ym, amount = num(b.amount);
  if (!s.employees[b.empId] || !/^\d{4}-\d{2}$/.test(ym) || amount < 0) return res.status(400).json({ success: false, error: 'Invalid employee, month or amount.' });
  const employeeAdvances=Object.values(s.advances||{}).filter(a=>a.active!==false&&a.empId===b.empId);
  employeeAdvances.forEach(a=>{a.recoveries=(a.recoveries||[]).filter(r=>r.ym!==ym);});
  // recoveryStartMonth is the payroll authority. This intentionally supports
  // owner-approved imports paid later but recovered in an earlier payroll run.
  const eligible = employeeAdvances.filter(a => a.recoveryStartMonth <= ym).sort((a,b)=>String(a.date+a.id).localeCompare(String(b.date+b.id)));
  const available = eligible.reduce((n, a) => n + advanceOutstanding(a), 0);
  if (amount > available + .001) return res.status(400).json({ success: false, error: 'Recovery cannot exceed the eligible outstanding advance of ₹' + round2(available) + '.' });
  let left = amount; eligible.forEach(a => { if (left <= 0) return; const take = Math.min(left, advanceOutstanding(a)); if (take > 0) { a.recoveries.push({ ym, amount: round2(take), by: req.user && req.user.username || 'admin', at: new Date().toISOString() }); left = round2(left - take); } });
  auditAdvance(s, req, 'RECOVERY_SET', '', { empId: b.empId, ym, amount }); save(s); res.json({ success: true, amount: round2(amount) });
});

router.post('/api/salary/advances/:id/cancel', guard, (req, res) => {
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
  res.json({ success: true, ym, divisor: num(s.divisor) || 30, daysInMonth: daysInMonth(ym), finalized: !!mo.finalized, rows, attendance: mo.attendance || {}, totals });
});

// Mark one attendance cell. mark='' clears it.
router.post('/api/salary/attendance/:ym', guard, (req, res) => {
  const s = load(); const mo = ensureMonth(s, req.params.ym); const b = req.body || {};
  if (b.empId && b.day) {
    mo.attendance[b.empId] = mo.attendance[b.empId] || {};
    let mark = b.mark;
    const emp=s.employees[b.empId],dateText=req.params.ym+'-'+String(b.day).padStart(2,'0'),date=new Date(dateText+'T00:00:00Z'),weekday=!isNaN(date)?WEEK_DAYS[date.getUTCDay()]:'';
    if(emp&&((emp.joiningDate&&dateText<emp.joiningDate)||(emp.lastWorkingDate&&dateText>emp.lastWorkingDate)))return res.status(400).json({success:false,error:'This date is outside the employee’s employment period.'});
    if(mark==='A'&&emp&&emp.weekOffDay&&emp.weekOffDay===weekday)mark='WO';
    if (mark && MARKS[mark] != null) mo.attendance[b.empId][b.day] = mark;
    else delete mo.attendance[b.empId][b.day];
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
      normalized[day]=mark==='A'&&emp.weekOffDay===weekday?'WO':mark;
    }
    seen.add(emp.id);prepared.push({emp,normalized});
  }
  const mo=ensureMonth(s,ym);
  prepared.forEach(({emp,normalized})=>{mo.attendance[emp.id]=mo.attendance[emp.id]||{};Object.entries(normalized).forEach(([day,mark])=>{if(mark)mo.attendance[emp.id][day]=mark;else delete mo.attendance[emp.id][day];});});
  save(s);
  res.json({success:true,employees:prepared.length,cells:prepared.reduce((n,x)=>n+Object.keys(x.normalized).length,0),rows:computeMonth(s,ym).filter(r=>seen.has(r.id)).map(r=>({empId:r.id,name:r.name,paidDays:r.computedPaidDays}))});
});

// Edit a payroll row (paidDays override / advance / paid / remarks).
router.post('/api/salary/row/:ym', guard, (req, res) => {
  const s = load(); const mo = ensureMonth(s, req.params.ym); const b = req.body || {};
  if (!b.empId) return res.status(400).json({ success: false, error: 'empId required' });
  if(!s.employees[b.empId]||!employeeInPayrollMonth(s.employees[b.empId],req.params.ym))return res.status(400).json({success:false,error:'This employee is outside the selected payroll month.'});
  const row = mo.rows[b.empId] = mo.rows[b.empId] || {};
  if (b.paidDays !== undefined) row.paidDays = (b.paidDays === '' || b.paidDays === null) ? null : num(b.paidDays);
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
    ['ARSHPREET SINGH', 'MANAGER', 30000, 'Shared', 30, 52000, 0, '2000 Last Month + 50000 This Month'],
    ['Guard', 'Security', 15000, 'Shared', 30, 3000, 12000, ''],
    ['SUNNY SHARMA', 'SALES EXECUTIVE', 24000, 'POS', 22, 20000, 0, '7000 Last Month'],
    ['PARDEEP', 'EXECUTIVE', 19000, 'Shared', 21.5, 0, 13500, ''],
    ['NANDANI', 'SALES EXECUTIVE', 17000, 'POS', 29, 0, 16500, ''],
    ['SHIVAM', 'SALES EXECUTIVE', 17000, 'POS', 30, 1500, 15500, ''],
    ['ISHA', 'SALES EXECUTIVE', 18000, 'POS', 30, 100, 18000, ''],
    ['HITESH', 'PHOTOGRAPHER', 20000, 'Website', 27, 0, 18000, ''],
    ['AJAY', 'VIDEO EDITOR', 18000, 'Website', 29, 0, 17500, 'Salary Delay'],
    ['PIYUSH', 'TAILOR', 18500, 'Shared', 28, 0, 0, ''],
    ['UMAIR', 'DRIVER', 28000, 'Shared', 27.5, 17000, 9000, ''],
    ['TUSHAR(INV)', 'PACKING HELPER', 15000, 'Shared', 31, 2000, 13500, ''],
    ['NIDA', 'LOGISTIC', 25000, 'Shared', 29, 0, 24000, ''],
    ['GAURAV', 'EDITOR', 18500, 'Website', 28.5, 0, 17500, ''],
    ['PRASHANT', 'ACCOUNTS', 26000, 'Shared', 29.5, 17467, 13000, '7467 Last Month + 10000 This Month'],
    ['Suraj(OB)', 'Office Boy', 18000, 'Shared', 25, 1500, 13500, ''],
    ['TUSHAR(MOD)', 'Model', 28000, 'Website', 30, 0, 28000, ''],
    ['Indervir', 'ACCOUNTS', 17000, 'Shared', 28.5, 4000, 12000, ''],
    ['CSH', 'MARKETING', 15000, 'Website', null, 0, 0, 'Confirmation'],
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

module.exports = { router, summaryForPL, _july2026Import:JULY_2026_IMPORT, _providedAdvanceImport:PROVIDED_ADVANCE_IMPORT, _julyImportedMarks:julyImportedMarks, _findImportedEmployee:findImportedEmployee, _ensureHistoricalGuard:ensureHistoricalGuard, _repairGuardSunnyCollision:repairGuardSunnyCollision, _applySunnyGuardAndSurajRepair:applySunnyGuardAndSurajRepair };

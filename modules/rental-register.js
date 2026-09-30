'use strict';
const {randomUUID}=require('node:crypto');
const money=n=>Math.round(Number(n)*100)/100, positive=n=>Number.isFinite(n)&&n>0;
const validMonth=m=>/^\d{4}-(0[1-9]|1[0-2])$/.test(String(m));
const validDate=d=>/^\d{4}-\d{2}-\d{2}$/.test(String(d))&&!Number.isNaN(Date.parse(d))&&new Date(d).toISOString().slice(0,10)===d;
const indiaDate=(now=new Date())=>new Date(now.getTime()+19800000).toISOString().slice(0,10);
const daysInMonth=m=>new Date(Date.UTC(+m.slice(0,4),+m.slice(5,7),0)).getUTCDate();
const nextMonth=m=>new Date(Date.UTC(+m.slice(0,4),+m.slice(5,7),1)).toISOString().slice(0,7);
const daysBetween=(a,b)=>Math.round((Date.parse(b)-Date.parse(a))/86400000);
const active=rows=>(rows||[]).filter(x=>!x.reversedAt&&!x.voidedAt), sum=rows=>money(rows.reduce((n,x)=>n+Number(x.amount||0),0)), id=p=>p+'-'+randomUUID();
const fields=['baseRent','dueDay','cgst','sgst','tds','escalationDate','escalationPercent','escalationEveryMonths'];
const seeds=()=>[
  ['fraganote-3','Fraganote','3rd',79800,'2026-08-31'],['fraganote-1','Fraganote','1st',105000,'2026-08-31'],
  ['suraagna-top','Suraagna','Top',60000,''],['lujo-2','Lujo Fab','2nd',89250,'2026-10-12'],
  ['lujo-basement','Lujo Fab','Basement',69500,''],['amty-4','AMTY','4th',83790,'']
].map(([id,tenant,floor,baseRent,endDate])=>({id,tenant,floor,baseRent,endDate,property:'Kirti Nagar',startMonth:'2026-09',dueDay:null,cgst:9,sgst:9,tds:10,history:[],note:id==='suraagna-top'?'Status after April needs confirmation.':id==='amty-4'?'₹79,800 before July 2026; ₹83,790 from July.':''}));
// Reference evidence only: never overwrite live tenancies or assume opening dues.
const sourceReview=[
  {floor:'Top Floor',tenant:'Ajaybir Singh',baseRent:58000,security:116000,startDate:'2026-10-01'},
  {floor:'3rd Floor',tenant:'Amty Global',baseRent:76000,security:150000,startDate:'2024-07-10',issue:'5% increase from Jul 2025; lease end and five-year term conflict.'},
  {floor:'2nd Floor',tenant:'Only Supplements',baseRent:81000,security:null,startDate:'2026-11-11'},
  {floor:'1st Floor',tenant:'Luj O Fab',baseRent:85000,security:170000,startDate:'2024-10-07',issue:'Lease end, term and remarks conflict; increased-rent field contains a date.'},
  {floor:'Upper Ground',tenant:'Nvik Beauty Private Ltd.',baseRent:106500,security:200000,startDate:'2026-09-15'},
  {floor:'Basement',tenant:'Lujo Fab',baseRent:72975,security:139000,startDate:'',issue:'Rent start and lease dates missing.'}
];
function state(s){const r=s.rentals||{tenancies:seeds()};for(const k of ['invoices','deposits','allocations','credits','reminders','activity'])if(!Array.isArray(r[k]))r[k]=[];r.revision=r.revision||0;r.automation=r.automation||{};return r;}
function depositBalance(r,tid){return money(sum(active(r.deposits).filter(x=>x.tenancyId===tid))-sum(active(r.allocations).filter(x=>x.tenancyId===tid&&x.kind==='security')));}
function outstanding(r,i){return i.voidedAt?0:money(i.bankDue-sum(active(r.allocations).filter(x=>x.invoiceId===i.id)));}
function creditBalance(r,c){return c.reversedAt?0:money(c.amount-sum(active(r.allocations).filter(x=>x.creditId===c.id)));}
function receiptUsage(r,rid){return money(sum(active(r.allocations).filter(x=>x.kind==='receipt'&&x.receiptId===rid))+sum(active(r.credits).filter(x=>x.receiptId===rid)));}
function securityTimelineValid(r,tid){
  const events=[...active(r.deposits).filter(x=>x.tenancyId===tid).map(x=>({date:x.date,amount:x.amount})),...active(r.allocations).filter(x=>x.tenancyId===tid&&x.kind==='security').map(x=>({date:x.date,amount:-x.amount}))];
  const daily={};for(const e of events)daily[e.date]=(daily[e.date]||0)+e.amount;
  let held=0;for(const date of Object.keys(daily).sort()){held=money(held+daily[date]);if(held<0)return false;}return true;
}
function termsAt(t,month){
  const versions=(t.versions||[]).filter(v=>v.effectiveMonth<=month).sort((a,b)=>a.effectiveMonth.localeCompare(b.effectiveMonth)),v=versions.length?versions[versions.length-1]:t;
  let base=Number(v.baseRent),date=v.escalationDate;
  if(validDate(date)&&date.slice(8)==='01'&&date.slice(0,7)<=month&&v.escalationPercent>0){
    const diff=(+month.slice(0,4)- +date.slice(0,4))*12+ +month.slice(5)- +date.slice(5,7),count=v.escalationEveryMonths?Math.floor(diff/v.escalationEveryMonths)+1:1;
    for(let n=0;n<count;n++)base=money(base*(1+v.escalationPercent/100));
  }
  return {...v,baseRent:base};
}
function manualMonth(t,m){
  if(t.startDate&&t.startDate.slice(0,7)===m&&t.startDate.slice(8)!=='01')return 'First partial month needs an agreed charge.';
  if(t.endDate&&t.endDate.slice(0,7)===m&&+t.endDate.slice(8)<daysInMonth(m))return 'Final partial month needs an agreed charge.';
  const v=termsAt(t,m);
  if(validDate(v.escalationDate)&&v.escalationDate.slice(8)!=='01'&&v.escalationDate.slice(0,7)<=m)return 'Mid-month increase needs reviewed rent terms before automation resumes.';
  return '';
}
function makeInvoice(r,t,month,base,reason,now,automatic=false){
  const v=termsAt(t,month),cgst=money(base*v.cgst/100),sgst=money(base*v.sgst/100),tds=money(base*v.tds/100);
  const i={id:id('RENT'),tenancyId:t.id,month,baseRent:base,cgst,sgst,tds,gross:money(base+cgst+sgst),bankDue:money(base+cgst+sgst-tds),dueDate:v.dueDay?month+'-'+String(Math.min(v.dueDay,daysInMonth(month))).padStart(2,'0'):'',reason,createdAt:now,automatic};r.invoices.push(i);return i;
}
function generate(r,today,now){
  let count=0;
  for(const t of r.tenancies){
    if(!t.confirmedAt||!t.autoEnabled||!validMonth(t.autoFrom)||['paused','ended'].includes(t.status))continue;
    for(let m=t.autoFrom,guard=0;m<=today.slice(0,7)&&guard<120;m=nextMonth(m),guard++){
      if(m<t.trackingFrom||(t.startDate&&m<t.startDate.slice(0,7))||(t.endDate&&m>t.endDate.slice(0,7)))continue;
      const v=termsAt(t,m);
      if(!v.dueDay||manualMonth(t,m)||r.invoices.some(i=>i.tenancyId===t.id&&i.month===m&&!i.opening))continue;
      makeInvoice(r,t,m,v.baseRent,'Automatically generated from confirmed terms.',now,true);count++;
    }
  }return count;
}
function snapshot(s,today=indiaDate()){
  const r=state(s),month=today.slice(0,7),alerts=[],followups=[];
  const invoices=active(r.invoices).map(i=>{
    const left=outstanding(r,i),cash=sum(active(r.allocations).filter(a=>a.invoiceId===i.id&&['receipt','credit'].includes(a.kind))),security=sum(active(r.allocations).filter(a=>a.invoiceId===i.id&&a.kind==='security'));
    const status=left<=0?'Paid':!i.dueDate?'Due date not set':i.dueDate<today?(cash+security>0?'Part-paid · overdue':'Overdue'):cash+security>0?'Part-paid':'Due';
    return {...i,outstanding:left,cashReceived:cash,securityAdjusted:security,status};
  });
  for(const i of invoices){
    const t=r.tenancies.find(t=>t.id===i.tenancyId);
    if(!t||i.outstanding<=0||!i.dueDate||daysBetween(today,i.dueDate)>3||(i.pauseUntil&&i.pauseUntil>=today))continue;
    const overdue=Math.max(0,daysBetween(i.dueDate,today));
    if(overdue>0&&overdue<=Number(t.graceDays||0))continue;
    const last=r.reminders.filter(x=>x.invoiceId===i.id&&x.kind==='contact').sort((a,b)=>b.date.localeCompare(a.date))[0];
    if(last&&daysBetween(last.date,today)<3)continue;
    followups.push({invoiceId:i.id,tenancyId:t.id,tenant:t.tenant,floor:t.floor,month:i.month,dueDate:i.dueDate,amount:i.outstanding,overdue,action:overdue>=7?'Call tenant':overdue?'Follow up':'Upcoming rent',phone:t.phone||'',email:t.email||'',message:`Hello ${t.tenant}, a reminder for ${t.property||'your property'} / ${t.floor}, rent ${i.month}. The remaining bank payment is ₹${i.outstanding.toLocaleString('en-IN')} (due ${i.dueDate}). Please transfer the balance and share the payment reference. Thank you.`});
  }
  const tenancies=r.tenancies.map(t=>{
    const issues=[];
    if(!t.confirmedAt)issues.push('Confirm tenant and terms.');if(!t.dueDay)issues.push('Rent due day missing.');if(!t.phone&&!t.email)issues.push('Tenant contact missing.');if(t.openingConfirmed!==true)issues.push('Opening balance not confirmed.');
    if(t.autoEnabled&&validMonth(t.autoFrom))for(let m=t.autoFrom,g=0;m<=month&&g++<120;m=nextMonth(m)){if((t.startDate&&m<t.startDate.slice(0,7))||(t.endDate&&m>t.endDate.slice(0,7)))continue;const note=manualMonth(t,m);if(note&&!r.invoices.some(i=>i.tenancyId===t.id&&i.month===m))issues.push(m+': '+note);}
    if(t.endDate&&daysBetween(today,t.endDate)<=Math.max(60,Number(t.noticeDays)||0)&&daysBetween(today,t.endDate)>=-30)alerts.push({tenancyId:t.id,tenant:t.tenant,kind:'Lease',date:t.endDate,text:!t.confirmedAt?'Verify the recorded lease end against the current agreement.':t.endDate<today?'Lease end date has passed. Review renewal or closure.':'Lease ends soon. Review renewal and notice deadline.'});
    if(t.endDate&&t.noticeDays){const d=new Date(Date.parse(t.endDate)-t.noticeDays*86400000).toISOString().slice(0,10);if(daysBetween(today,d)<=30&&t.endDate>=today)alerts.push({tenancyId:t.id,tenant:t.tenant,kind:'Notice',date:d,text:'Review contractual notice deadline.'});}
    const v=termsAt(t,month);let ed=v.escalationDate;
    if(validDate(ed)&&v.escalationEveryMonths){let guard=0;while(ed<today&&guard++<120){const ym=new Date(Date.UTC(+ed.slice(0,4),+ed.slice(5,7)-1+v.escalationEveryMonths,1)).toISOString().slice(0,7);ed=ym+'-'+String(Math.min(+ed.slice(8),daysInMonth(ym))).padStart(2,'0');}}
    if(validDate(ed)&&daysBetween(today,ed)<=30&&daysBetween(today,ed)>=-30)alerts.push({tenancyId:t.id,tenant:t.tenant,kind:'Increase',date:ed,text:'Scheduled rent increase: '+v.escalationPercent+'%.'});
    return {...t,currentRent:v.baseRent,securityRecorded:r.deposits.some(d=>d.tenancyId===t.id),securityBalance:depositBalance(r,t.id),creditBalance:sum(active(r.credits).filter(c=>c.tenancyId===t.id).map(c=>({amount:creditBalance(r,c)}))),issues};
  });
  const receipts=(s.receipts||[]).filter(x=>x.nature==='PERSONAL'&&!x.accountingExcluded).map(x=>({id:x.id,date:x.date,account:x.account,source:x.source,amount:x.amount,allocated:receiptUsage(r,x.id),available:money(x.amount-receiptUsage(r,x.id))})),integrity=[];
  for(const rid of new Set([...active(r.credits),...active(r.allocations).filter(a=>a.kind==='receipt')].map(x=>x.receiptId))){const rec=receipts.find(x=>x.id===rid);if(!rec||rec.available<0)integrity.push('Linked receipt '+rid+' is missing, excluded or smaller than its allocations. Review Accounts before treating balances as final.');}
  const current=invoices.filter(i=>i.month===month&&!i.opening);
  return {success:true,...r,today,tenancies,invoices,voidedInvoices:r.invoices.filter(i=>i.voidedAt),allocations:active(r.allocations),deposits:active(r.deposits),credits:active(r.credits).map(c=>({...c,available:creditBalance(r,c)})),receipts,followups,alerts,integrity,sourceReview,summary:{month,due:sum(current.map(i=>({amount:i.bankDue}))),received:sum(current.map(i=>({amount:i.cashReceived}))),security:sum(current.map(i=>({amount:i.securityAdjusted}))),outstanding:sum(current.map(i=>({amount:i.outstanding}))),olderArrears:sum(invoices.filter(i=>i.month<month||i.opening).map(i=>({amount:i.outstanding}))),overdue:sum(invoices.filter(i=>i.dueDate&&i.dueDate<today).map(i=>({amount:i.outstanding}))),needsReview:tenancies.filter(t=>t.issues.length).length}};
}
function register(router,{loadStore,saveStore,isOwner,audit,clock=()=>new Date()}){
  const deny=(res,error,status=400)=>res.status(status).json({success:false,error});
  function tick(){const now=clock().toISOString(),today=indiaDate(clock()),s=loadStore(),r=state(s),count=generate(r,today,now);if(count||r.automation.lastCheckedDate!==today){r.automation={lastCheckedDate:today,lastSuccessAt:now,error:null};if(count)r.activity.push({at:now,by:'system',action:'automatic_dues',detail:count+' monthly charge(s) generated.'});r.revision++;s.rentals=r;saveStore(s);}return count;}
  let timer;
  function start(){if(timer)return;const run=()=>{try{tick();}catch(e){console.error('[rentals] scheduler failed:',e.message);try{const s=loadStore(),r=state(s);r.automation.error='Automatic dues could not be saved. Retry from Rental income.';s.rentals=r;saveStore(s);}catch{}}};run();timer=setInterval(run,15*60*1000);timer.unref();}
  router.get('/api/expenses/rentals',(req,res)=>{if(!isOwner(req))return deny(res,'Owner only.',403);res.json(snapshot(loadStore(),indiaDate(clock())));});
  router.get('/api/expenses/rentals/summary',(req,res)=>{if(!isOwner(req))return deny(res,'Owner only.',403);const d=snapshot(loadStore(),indiaDate(clock()));res.json({success:true,today:d.today,summary:d.summary,followupCount:d.followups.length,alertCount:d.alerts.length,error:d.automation.error||d.integrity[0]||null});});
  router.post('/api/expenses/rentals',(req,res)=>{
    if(!isOwner(req))return deny(res,'Owner only.',403);
    const s=loadStore(),r=state(s),b=req.body||{},now=clock().toISOString(),today=indiaDate(clock());
    if(b.revision!=null&&b.revision!==r.revision)return deny(res,'The register changed. Refresh and try again.',409);
    let t=r.tenancies.find(x=>x.id===b.tenancyId);
    const amount=money(b.amount),reason=String(b.reason||'').trim(),actor=req.user.username,requireDate=()=>validDate(String(b.date||''))&&b.date<=today,receiptFor=rid=>(s.receipts||[]).find(x=>x.id===rid&&x.nature==='PERSONAL'&&!x.accountingExcluded);
    if(b.action==='run'){generate(r,today,now);r.automation={lastCheckedDate:today,lastSuccessAt:now,error:null};}
    else if(b.action==='add_tenant'){
      if(!String(b.tenant||'').trim()||!String(b.floor||'').trim()||!String(b.property||'').trim())return deny(res,'Property, unit / floor and tenant name are required.');
      t={id:id('TEN'),tenant:String(b.tenant).trim(),floor:String(b.floor).trim(),property:String(b.property).trim(),baseRent:0,startMonth:today.slice(0,7),dueDay:null,cgst:0,sgst:0,tds:0,endDate:'',history:[]};r.tenancies.push(t);
    }else{
      if(!t)return deny(res,'Choose a tenant / floor.');
      if(b.action==='terms'){
        const base=money(b.baseRent),due=b.dueDay===''||b.dueDay==null?null:Number(b.dueDay),effective=String(b.startMonth||''),end=String(b.endDate||''),startDate=String(b.startDate||''),rates=['cgst','sgst','tds'].map(k=>Number(b[k]));
        if(!positive(base)||!validMonth(effective)||(due!==null&&(!Number.isInteger(due)||due<1||due>31))||(end&&!validDate(end))||(startDate&&!validDate(startDate))||(end&&startDate&&end<startDate))return deny(res,'Enter valid rent, effective month, due day and lease dates.');
        if(rates.some(n=>!Number.isFinite(n)||n<0||n>100))return deny(res,'Enter valid tax rates.');
        const ed=String(b.escalationDate||''),ep=Number(b.escalationPercent||0),em=Number(b.escalationEveryMonths||0),grace=Number(b.graceDays||0),notice=Number(b.noticeDays||0);
        if((ed&&!validDate(ed))||!Number.isFinite(ep)||ep<0||ep>100||!Number.isInteger(em)||em<0||em>120||!Number.isInteger(grace)||grace<0||grace>60||!Number.isInteger(notice)||notice<0||notice>730)return deny(res,'Check increase, grace-period and notice settings.');
        if((ed&&!ep)||(!ed&&ep))return deny(res,'Provide both the increase date and percentage.');
        if(ed&&ed.slice(0,7)<effective)return deny(res,'Increase must be on or after the effective month. Use the revised base for past increases.');
        const enabled=b.autoEnabled===true,autoFrom=String(b.autoFrom||t.autoFrom||today.slice(0,7)),confirmed=b.confirmed===true||!!t.confirmedAt;
        if(enabled&&(!confirmed||!due||!(b.openingConfirmed===true||t.openingConfirmed)))return deny(res,'Confirm tenancy, due day and opening balance before enabling automatic dues.');
        if(enabled&&(!validMonth(autoFrom)||(!t.autoFrom&&autoFrom<today.slice(0,7))||(t.autoFrom&&autoFrom!==t.autoFrom)))return deny(res,'Start new automation this month or later. Once set, the automation start month stays fixed. Enter reviewed historical dues separately.');
        const status=String(b.status||t.status||'active');if(!['active','upcoming','paused','ended'].includes(status))return deny(res,'Choose a valid tenancy status.');
        if(enabled&&end&&end<autoFrom+'-01')return deny(res,'Tenancy ends before automation starts. Leave automatic dues off.');
        if(t.autoEnabled&&effective<today.slice(0,7)&&!(effective===t.startMonth&&base===t.baseRent&&due===t.dueDay&&rates.every((n,j)=>n===t[['cgst','sgst','tds'][j]])&&ed===(t.escalationDate||'')&&ep===(t.escalationPercent||0)&&em===(t.escalationEveryMonths||0)))return deny(res,'Change automated terms this month or later; correct historical charges individually.');
        if(b.email&&!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(b.email))return deny(res,'Enter a valid email address.');
        if(b.phone&&!/^\+?\d{8,15}$/.test(b.phone))return deny(res,'Use country code and digits for the phone number.');
        t.history=t.history||[];t.history.push({at:now,by:actor,before:{...t,history:undefined,versions:undefined},effectiveMonth:effective});
        t.versions=t.versions||[{effectiveMonth:t.startMonth,...Object.fromEntries(fields.map(k=>[k,t[k]]))}];
        Object.assign(t,{tenant:String(b.tenant||t.tenant).trim(),floor:String(b.floor||t.floor).trim(),property:String(b.property||t.property||'Kirti Nagar').trim(),phone:String(b.phone||'').trim(),email:String(b.email||'').trim(),baseRent:base,dueDay:due,startMonth:effective,trackingFrom:t.trackingFrom||t.startMonth,endDate:end,startDate,cgst:rates[0],sgst:rates[1],tds:rates[2],escalationDate:ed,escalationPercent:ep,escalationEveryMonths:em,graceDays:grace,noticeDays:notice,status,autoEnabled:enabled,autoFrom:enabled?autoFrom:t.autoFrom,openingConfirmed:b.openingConfirmed===true||t.openingConfirmed===true,reviewNote:String(b.reviewNote||''),confirmedAt:confirmed?(t.confirmedAt||now):null});
        t.versions=t.versions.filter(v=>v.effectiveMonth!==effective);t.versions.push({effectiveMonth:effective,...Object.fromEntries(fields.map(k=>[k,t[k]]))});
        if(enabled)generate(r,today,now);
      }else if(b.action==='invoice'){
        const month=String(b.month||'');if(!validMonth(month)||month<(t.trackingFrom||t.startMonth)||(t.endDate&&month>t.endDate.slice(0,7)))return deny(res,'Month is outside this tenancy / tracking period.');
        if(active(r.invoices).some(i=>i.tenancyId===t.id&&i.month===month&&!i.opening))return deny(res,'This month already has a rent charge.');
        const base=b.baseRent===''||b.baseRent==null?termsAt(t,month).baseRent:money(b.baseRent);if(!positive(base))return deny(res,'Enter the agreed base rent.');
        if((manualMonth(t,month)||(t.endDate&&month===t.endDate.slice(0,7)))&&!reason)return deny(res,'For a partial / final month or mid-month increase, explain the agreed rent.');
        makeInvoice(r,t,month,base,reason,now);
      }else if(b.action==='opening'){
        if(b.amount===''||b.amount==null||!Number.isFinite(amount)||amount<0||!requireDate()||!reason||b.date>=String(t.autoFrom||t.startMonth)+'-01')return deny(res,'Enter a confirmed nonnegative opening balance, date before tracking, and note.');
        if(active(r.invoices).some(i=>i.tenancyId===t.id&&i.opening)||(t.openingRecordedAt&&!r.invoices.some(i=>i.tenancyId===t.id&&i.opening&&i.voidedAt)))return deny(res,'Opening balance already recorded. Correct its charge rather than entering twice.');
        if(amount)r.invoices.push({id:id('OPEN'),tenancyId:t.id,month:b.date.slice(0,7),dueDate:b.date,bankDue:amount,opening:true,reason,createdAt:now});t.openingConfirmed=true;t.openingRecordedAt=now;t.openingNote=reason;
      }else if(['deposit','refund_deposit'].includes(b.action)){
        if(!positive(amount)||!requireDate()||!reason)return deny(res,'Enter a positive security amount, actual date and reference.');
        if(b.action==='refund_deposit'&&amount>depositBalance(r,t.id))return deny(res,'Refund exceeds security held.');
        r.deposits.push({id:id('SEC'),tenancyId:t.id,amount:b.action==='deposit'?amount:-amount,date:b.date,reason,createdAt:now});
      }else if(b.action==='advance'){
        const rec=receiptFor(b.receiptId);if(!rec||rec.date>today||!positive(amount)||amount>money(rec.amount-receiptUsage(r,rec.id))||!reason)return deny(res,'Choose an available received receipt, amount and note.');
        r.credits.push({id:id('CREDIT'),tenancyId:t.id,receiptId:rec.id,amount,date:rec.date,reason,createdAt:now});
      }else if(['security','receipt','credit','split_receipt'].includes(b.action)){
        const pieces=b.action==='split_receipt'?b.splits:[{invoiceId:b.invoiceId,amount}];
        if(!Array.isArray(pieces)||!pieces.length||pieces.length>60||!requireDate()||!reason)return deny(res,'Provide allocations, valid settlement date and remark.');
        const kind=b.action==='split_receipt'?'receipt':b.action,rec=kind==='receipt'?receiptFor(b.receiptId):null,c=kind==='credit'?active(r.credits).find(x=>x.id===b.creditId&&x.tenancyId===t.id):null,total=sum(pieces.map(x=>({amount:money(x.amount)})));
        if(!positive(total))return deny(res,'Enter positive allocation amounts.');
        if(kind==='receipt'&&(!rec||total>money(rec.amount-receiptUsage(r,rec.id))||b.date<rec.date))return deny(res,'Receipt unavailable, allocation exceeds its balance, or settlement precedes receipt.');
        if(kind==='credit'&&(!c||total>creditBalance(r,c)||b.date<c.date))return deny(res,'Insufficient advance credit on this date.');
        if(kind==='security'){const held=money(sum(active(r.deposits).filter(x=>x.tenancyId===t.id&&x.date<=b.date))-sum(active(r.allocations).filter(x=>x.tenancyId===t.id&&x.kind==='security')));if(total>held)return deny(res,'Insufficient security deposit balance on this date.');}
        const seen=new Set();for(const p of pieces){const i=active(r.invoices).find(x=>x.id===p.invoiceId&&(b.action==='split_receipt'||x.tenancyId===t.id)),amt=money(p.amount);if(!i||seen.has(i.id)||!positive(amt)||amt>outstanding(r,i))return deny(res,'Use distinct charges and amounts within their outstanding balances.');seen.add(i.id);}
        for(const p of pieces){const i=r.invoices.find(x=>x.id===p.invoiceId);r.allocations.push({id:id('RA'),tenancyId:i.tenancyId,invoiceId:i.id,kind,receiptId:rec?rec.id:'',creditId:c?c.id:'',amount:money(p.amount),date:b.date,reason,createdAt:now});}
      }else if(b.action==='reverse'){
        const collection={allocation:r.allocations,credit:r.credits,deposit:r.deposits}[b.kind],x=collection&&active(collection).find(x=>x.id===b.id&&x.tenancyId===t.id);
        if(!x||!reason)return deny(res,'Choose an active entry and provide a correction reason.');if(b.kind==='credit'&&active(r.allocations).some(a=>a.creditId===x.id))return deny(res,'Reverse credit settlements before releasing the advance.');
        x.reversedAt=now;x.reversedBy=actor;x.reversalReason=reason;if(depositBalance(r,t.id)<0)return deny(res,'Reverse security settlements / refunds before removing this deposit.');
      }else if(b.action==='void_invoice'){
        const i=active(r.invoices).find(x=>x.id===b.invoiceId&&x.tenancyId===t.id);if(!i||!reason)return deny(res,'Choose a charge and explain the correction.');if(active(r.allocations).some(a=>a.invoiceId===i.id))return deny(res,'Reverse settlements before voiding this charge.');i.voidedAt=now;i.voidedBy=actor;i.voidReason=reason;
      }else if(['followup','pause','tds'].includes(b.action)){
        const i=active(r.invoices).find(x=>x.id===b.invoiceId&&x.tenancyId===t.id);if(!i)return deny(res,'Choose a rent charge.');
        if(b.action==='pause'){if(b.until&&(!validDate(b.until)||b.until<today))return deny(res,'Choose a review date today or later.');if(!reason)return deny(res,'Explain the pause or resumption.');i.pauseUntil=b.until||'';i.pauseReason=reason;}
        else if(b.action==='tds'){if(!['pending','verified','not_applicable'].includes(b.status)||!reason)return deny(res,'Choose TDS evidence status and reference.');i.tdsStatus=b.status;i.tdsReference=reason;}
        else{if(b.until&&(!validDate(b.until)||b.until<today))return deny(res,'Choose a promised-payment date today or later.');if(!['called','whatsapp','email','other'].includes(b.channel)||!reason)return deny(res,'Choose contact channel and note.');if(r.reminders.some(x=>x.invoiceId===i.id&&x.date===today&&x.channel===b.channel))return deny(res,'This contact is already logged today.');if(b.until){i.pauseUntil=b.until;i.pauseReason=reason;}r.reminders.push({id:id('REM'),invoiceId:i.id,date:today,channel:b.channel,kind:'contact',reason,by:actor,at:now});}
      }else return deny(res,'Unknown rental action.');
    }
    if(t&&!securityTimelineValid(r,t.id))return deny(res,'This correction would leave security negative on an earlier date. Review dated deposits and settlements.');
    r.activity.push({at:now,by:actor,action:b.action,tenancyId:t&&t.id,detail:reason||'Updated rental register.'});r.revision++;s.rentals=r;audit(s,req,'RENTAL_'+String(b.action).toUpperCase(),'rental',t?t.id:'register',{after:b});saveStore(s);res.json({success:true,tenancyId:t&&t.id,revision:r.revision});
  });return {tick,start};
}
module.exports={register,state,depositBalance,outstanding,creditBalance,receiptUsage,snapshot,generate,termsAt,indiaDate};

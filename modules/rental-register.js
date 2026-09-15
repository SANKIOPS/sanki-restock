'use strict';
const money=n=>Math.round(Number(n)*100)/100;
const validDate=d=>/^\d{4}-\d{2}-\d{2}$/.test(d)&&!Number.isNaN(Date.parse(d))&&new Date(d).toISOString().slice(0,10)===d;
const seeds=()=>[
  ['fraganote-3','Fraganote','3rd',79800,'2026-08-31'],['fraganote-1','Fraganote','1st',105000,'2026-08-31'],
  ['suraagna-top','Suraagna','Top',60000,''],['lujo-2','Lujo Fab','2nd',89250,'2026-10-12'],
  ['lujo-basement','Lujo Fab','Basement',69500,''],['amty-4','AMTY','4th',83790,'']
].map(([id,tenant,floor,baseRent,endDate])=>({id,tenant,floor,baseRent,endDate,startMonth:'2026-09',dueDay:null,cgst:9,sgst:9,tds:10,history:[],note:id==='suraagna-top'?'Status after April needs confirmation.':id==='amty-4'?'₹79,800 before July 2026; ₹83,790 from July.':''}));
function state(s){return s.rentals||{tenancies:seeds(),invoices:[],deposits:[],allocations:[]};}
function depositBalance(r,id){return money(r.deposits.filter(x=>x.tenancyId===id).reduce((n,x)=>n+x.amount,0)-r.allocations.filter(x=>x.tenancyId===id&&x.kind==='security').reduce((n,x)=>n+x.amount,0));}
function outstanding(r,invoice){return money(invoice.bankDue-r.allocations.filter(x=>x.invoiceId===invoice.id).reduce((n,x)=>n+x.amount,0));}
function register(router,{loadStore,saveStore,isOwner,audit}){
  const fail=(res,error)=>res.status(400).json({success:false,error});
  router.get('/api/expenses/rentals',(req,res)=>{
    if(!isOwner(req))return res.status(403).json({success:false,error:'Owner only.'});
    const s=loadStore(),r=state(s),today=new Date().toISOString().slice(0,10);
    res.json({success:true,...r,tenancies:r.tenancies.map(t=>({...t,securityBalance:depositBalance(r,t.id)})),invoices:r.invoices.map(i=>{const left=outstanding(r,i);return {...i,outstanding:left,status:left<=0?'Settled':!i.dueDate?'Due date not set':i.dueDate<today?'Overdue':'Due'};}),receipts:(s.receipts||[]).filter(x=>x.nature==='PERSONAL'&&!x.accountingExcluded).map(x=>({id:x.id,date:x.date,account:x.account,source:x.source,amount:x.amount,allocated:r.allocations.filter(a=>a.receiptId===x.id).reduce((n,a)=>n+a.amount,0)}))});
  });
  router.post('/api/expenses/rentals',(req,res)=>{
    if(!isOwner(req))return res.status(403).json({success:false,error:'Owner only.'});
    const s=loadStore(),r=state(s),b=req.body||{},t=r.tenancies.find(x=>x.id===b.tenancyId),now=new Date().toISOString();
    if(!t)return fail(res,'Choose a tenant / floor.');
    const amount=money(b.amount),reason=String(b.reason||'').trim();
    if(b.action==='terms'){
      const base=money(b.baseRent),due=b.dueDay===''||b.dueDay==null?null:Number(b.dueDay),start=String(b.startMonth||''),end=String(b.endDate||'');
      if(!(base>0)||!/^\d{4}-(0[1-9]|1[0-2])$/.test(start)||(due!==null&&(!Number.isInteger(due)||due<1||due>31))||(end&&!validDate(end)))return fail(res,'Enter valid rent, start month, due day and end date.');
      const rates=['cgst','sgst','tds'].map(k=>Number(b[k]));if(rates.some(n=>!Number.isFinite(n)||n<0||n>100))return fail(res,'Tax rates must be between 0 and 100.');
      t.history.push({at:now,by:req.user.username,before:{...t,history:undefined},effectiveMonth:start});
      Object.assign(t,{baseRent:base,dueDay:due,startMonth:start,endDate:end,cgst:rates[0],sgst:rates[1],tds:rates[2]});
    }else if(b.action==='invoice'){
      const month=String(b.month||'');
      if(!/^\d{4}-(0[1-9]|1[0-2])$/.test(month)||month<t.startMonth||(t.endDate&&month>t.endDate.slice(0,7)))return fail(res,'Month is outside this tenancy / tracking period.');
      if(r.invoices.some(i=>i.tenancyId===t.id&&i.month===month))return fail(res,'This month already has a rent charge.');
      const base=b.baseRent===''||b.baseRent==null?t.baseRent:money(b.baseRent);
      if(!(base>0))return fail(res,'Enter the final agreed base rent.');
      if(t.endDate&&month===t.endDate.slice(0,7)&&!reason)return fail(res,'For the final month, enter the agreed rent and explain full-month / prorated treatment.');
      const cgst=money(base*t.cgst/100),sgst=money(base*t.sgst/100),tds=money(base*t.tds/100),days=new Date(Number(month.slice(0,4)),Number(month.slice(5)),0).getDate();
      r.invoices.push({id:'RENT-'+t.id+'-'+month,tenancyId:t.id,month,baseRent:base,cgst,sgst,tds,gross:money(base+cgst+sgst),bankDue:money(base+cgst+sgst-tds),dueDate:t.dueDay?month+'-'+String(Math.min(t.dueDay,days)).padStart(2,'0'):'',reason,createdAt:now});
    }else if(b.action==='deposit'){
      if(!(amount>0)||!validDate(String(b.date||'')))return fail(res,'Enter a positive security amount and actual deposit date.');
      if(!reason)return fail(res,'Enter a deposit reference / note.');
      r.deposits.push({id:'SEC-'+Date.now()+'-'+r.deposits.length,tenancyId:t.id,amount,date:b.date,reason,createdAt:now});
    }else if(['security','receipt'].includes(b.action)){
      const i=r.invoices.find(x=>x.id===b.invoiceId&&x.tenancyId===t.id);
      if(!i||!(amount>0)||amount>outstanding(r,i)||!validDate(String(b.date||''))||!reason)return fail(res,'Choose a rent charge, valid date, amount within outstanding rent, and remark.');
      if(b.action==='security'){
        const held=r.deposits.filter(x=>x.tenancyId===t.id&&x.date<=b.date).reduce((n,x)=>n+x.amount,0)-r.allocations.filter(x=>x.tenancyId===t.id&&x.kind==='security').reduce((n,x)=>n+x.amount,0);
        if(amount>money(held))return fail(res,'Insufficient security deposit balance on this date.');
      }
      if(b.action==='receipt'){
        const receipt=(s.receipts||[]).find(x=>x.id===b.receiptId&&x.nature==='PERSONAL'&&!x.accountingExcluded),used=r.allocations.filter(x=>x.receiptId===b.receiptId).reduce((n,x)=>n+x.amount,0);
        if(!receipt||amount>money(receipt.amount-used))return fail(res,'Choose an available PERSONAL receipt; allocation cannot exceed its amount.');
      }
      r.allocations.push({id:'RA-'+Date.now()+'-'+r.allocations.length,tenancyId:t.id,invoiceId:i.id,kind:b.action,receiptId:b.action==='receipt'?b.receiptId:'',amount,date:b.date,reason,createdAt:now});
    }else return fail(res,'Unknown rental action.');
    s.rentals=r;audit(s,req,'RENTAL_'+String(b.action).toUpperCase(),'rental',t.id,{after:b});saveStore(s);res.json({success:true});
  });
}
module.exports={register,state,depositBalance,outstanding};

'use strict';
const {money}=require('./physical-bank-balance');
const list=v=>Array.isArray(v)?v:Object.values(v||{});
const validMonth=value=>/^\d{4}-(?:0[1-9]|1[0-2])$/.test(String(value||''));
const activeInvoice=invoice=>invoice&&invoice.voidedAt==null&&invoice.reversedAt==null&&!invoice.opening&&validMonth(invoice.month);
function tenancyActiveInMonth(tenancy,month){
 if(!tenancy||tenancy.status==='ended'&&!tenancy.endDate)return false;
 const start=String(tenancy.startDate||((tenancy.trackingFrom||tenancy.startMonth||'')+'-01')).slice(0,7),end=String(tenancy.endDate||'').slice(0,7);
 return (!validMonth(start)||start<=month)&&(!validMonth(end)||end>=month);
}
function rentDueEntries(store,months,receipts){
 const rental=store&&store.rentals,hasRegister=!!rental&&Array.isArray(rental.tenancies)&&Array.isArray(rental.invoices),entries=[],review=new Map();
 for(const month of months){
  const invoices=hasRegister?rental.invoices.filter(invoice=>activeInvoice(invoice)&&invoice.month===month):[];
  const activeTenancies=hasRegister?rental.tenancies.filter(tenancy=>tenancyActiveInMonth(tenancy,month)):[];
  const charged=new Set(invoices.map(invoice=>invoice.tenancyId).filter(Boolean));
  const missing=activeTenancies.filter(tenancy=>!charged.has(tenancy.id));
  const hasReceipt=receipts.some(receipt=>String(receipt.date||'').startsWith(month));
  const complete=hasRegister&&!missing.length&&(activeTenancies.length>0||invoices.length>0||!hasReceipt);
  review.set(month,{complete,missing:missing.map(tenancy=>tenancy.tenant||tenancy.floor||tenancy.id)});
  for(const invoice of invoices){
   const tenant=rental.tenancies.find(item=>item.id===invoice.tenancyId),desiredNet=money(Number(invoice.bankDue)||0);
   const baseDue=money(invoice.taxBreakdownUnknown?desiredNet/1.08:Number(invoice.baseRent)||0);
   const tdsExpected=money(invoice.taxBreakdownUnknown?baseDue*.10:Number(invoice.tds)||0);
   // Tie the rounded tax components exactly to the approved net charge.
   const gstExpected=money(invoice.taxBreakdownUnknown?desiredNet-baseDue+tdsExpected:(Number(invoice.cgst)||0)+(Number(invoice.sgst)||0));
   const tdsVerified=invoice.tdsStatus==='verified'?tdsExpected:0;
   entries.push({id:invoice.id,reference:invoice.id,date:invoice.dueDate||month+'-01',kind:'rent_due',entity:'SAMAST',category:'Approved rent charge',description:(tenant?.tenant||'Tenant')+' · '+month,tenant:tenant?.tenant||'',payer:tenant?.tenant||'',account:'',amount:desiredNet,profitKind:'rent_due',baseRent:0,gst:0,tds:0,received:0,desiredNet,rentDifference:desiredNet,gstExpected,gstPending:gstExpected,tdsExpected,tdsVerified,tdsPending:money(tdsExpected-tdsVerified),tdsEvidenceStatus:invoice.tdsStatus||'pending',tdsReference:invoice.tdsReference||'',expense:0,salary:0,profit:0,gstPaid:0,incomeTaxPaid:0});
  }
 }
 return {entries,review};
}
// Classify only explicit tax remittances, not professional/compliance service fees.
function taxKind(record){
 const category=String(record.ledger||record.category||'').trim(),description=[record.particulars,record.description,record.reconciliationSource?.bankDescription].filter(Boolean).join(' ');
 if(/^(?:gst|goods and services tax)\s*(?:paid|payment|remittance|challan)$/i.test(category)||(/^gst fee$/i.test(category)&&/\bGST\s*\//i.test(description)))return 'gstPaid';
 if(/\b(?:self[ -]?assess(?:ed|ment)\s+i\.?\s*t\.?|income[ -]?tax|self[ -]?assess(?:ed|ment)(?: income)? tax|advance income[ -]?tax)\b/i.test(category)&&!/consult|professional|filing|preparation|service/i.test(category))return 'incomeTaxPaid';
 return '';
}
// Receipt reporting only. Never write back the derived base/GST/TDS to accounting.
function build(store,{from,to,receipts,costs,salary={}}){
 const salaryLinks=new Set(list(salary.salaryPayments).filter(p=>p.active!==false&&!p.accountingExcluded).map(p=>p.linkedLedgerEntryId).filter(Boolean));
 const entries=[];
 const zero={baseRent:0,gst:0,tds:0,received:0,desiredNet:0,rentDifference:0,gstExpected:0,gstPending:0,tdsExpected:0,tdsVerified:0,tdsPending:0,expense:0,salary:0,profit:0,gstPaid:0,incomeTaxPaid:0};
 const selectedMonths=[];
 for(let month=from.slice(0,7);month<=to.slice(0,7);){selectedMonths.push(month);const next=new Date(month+'-01T00:00:00Z');next.setUTCMonth(next.getUTCMonth()+1);month=next.toISOString().slice(0,7);}
 const due=rentDueEntries(store,selectedMonths,receipts);entries.push(...due.entries);
 for(const r of receipts){
  const received=money(Number(r.amount)||0),baseRent=money(received/1.08),tds=money(baseRent*.10);
  // Keep rounded components tied exactly to the authoritative bank receipt.
  const gst=money(received-baseRent+tds);
  entries.push({...zero,id:r.id,reference:r.id,date:r.date,kind:'income',entity:'SAMAST',category:'Rental income',description:r.particulars||r.source||r.payer||'Rent receipt',account:r.account||'',amount:baseRent,profitKind:'rent',baseRent,gst,tds,received,rentDifference:-received,gstPending:-gst,expense:0,salary:0,profit:baseRent,proof:r.proof||r.paymentProof||'',proofs:r.proofs||r.paymentProofs||[],bankReference:r.bankReconciliationEvidence?.reference||r.bankReference||'',bankDescription:r.bankReconciliationEvidence?.description||'',payer:r.payer||r.receivedFrom||r.source||''});
 }
 for(const r of costs){
  const isSalary=r.kind==='salary'||salaryLinks.has(r.id)||[...salaryLinks].some(id=>String(r.id).startsWith(id+'/'))||/\bsalar(?:y|ies)\b/i.test(r.category||'');
  const payroll=list(salary.salaryPayments).find(p=>String(r.id).startsWith(p.id+'/'));
  const amount=money(r.expenses),tax=taxKind(r);
  const profitKind=tax?'tax':isSalary?'salary':'expense';
  entries.push({...r,...zero,proof:r.proof||payroll?.proofs?.[0]||'',proofs:payroll?.proofs||[],vendor:r.vendor||payroll?.employeeName||salary.employees?.[payroll?.empId]?.name||'',kind:'expense',entity:'SAMAST',amount,profitKind,expense:profitKind==='expense'?amount:0,salary:profitKind==='salary'?amount:0,profit:tax?0:-amount,...(tax?{[tax]:amount}:{})});
 }
 const fields=['baseRent','expense','salary','profit','gst','tds','received','desiredNet','rentDifference','gstExpected','gstPending','tdsExpected','tdsVerified','tdsPending','gstPaid','incomeTaxPaid'];
 const total=rows=>Object.fromEntries(fields.map(key=>[key,money(rows.reduce((n,r)=>n+r[key],0))]));
 const months=[];
 for(let month=from.slice(0,7);month<=to.slice(0,7);){
  const start=month+'-01',next=new Date(start+'T00:00:00Z');next.setUTCMonth(next.getUTCMonth()+1);const nextMonth=next.toISOString().slice(0,7),end=new Date(next.getTime()-86400000).toISOString().slice(0,10);
  const values=total(entries.filter(r=>r.date.startsWith(month))),rentReview=due.review.get(month);
  if(!rentReview?.complete)for(const key of ['desiredNet','rentDifference','gstExpected','gstPending','tdsExpected','tdsVerified','tdsPending'])values[key]=null;
  months.push({month,from:from>start?from:start,to:to<end?to:end,partial:from>start||to<end,rentDueComplete:!!rentReview?.complete,rentDueReview:rentReview?.missing||[],...values});month=nextMonth;
 }
 const totals=total(entries),complete=months.every(month=>month.rentDueComplete);
 if(!complete)for(const key of ['desiredNet','rentDifference','gstExpected','gstPending','tdsExpected','tdsVerified','tdsPending'])totals[key]=null;
 return {basis:'Receipts/payments basis · Desired net rent comes only from approved tenant rent charges; missing or partial-month charges show Needs review. Actual rent received comes only from recorded rent bank receipts. Difference = desired net rent − actual rent received (positive is pending; negative is advance/excess). Base rent from actual receipts = bank receipt ÷ 1.08 (18% GST added, 10% TDS deducted). Profit before income tax = actual-receipt base rent − Samast expenses paid − Samast salaries paid. GST expected and TDS expected come from approved charges; GST received comes from actual receipts. TDS credit is confirmed only when its monthly rent-charge evidence is marked Verified. GST remittances and actual income tax paid are separate and excluded from operating expenses. Unpaid rent, personal spending, transfers, refundable deposits and loan principal are excluded.',months,totals,entries:require('./report-expense-entries').enrichMany(store,entries).sort((a,b)=>b.date.localeCompare(a.date))};
}
module.exports={build,taxKind,rentDueEntries};

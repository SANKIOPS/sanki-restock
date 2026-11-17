'use strict';
const {money}=require('./physical-bank-balance');
const list=v=>Array.isArray(v)?v:Object.values(v||{});
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
 const zero={baseRent:0,gst:0,tds:0,received:0,expense:0,salary:0,profit:0,gstPaid:0,incomeTaxPaid:0};
 for(const r of receipts){
  const received=money(Number(r.amount)||0),baseRent=money(received/1.08),tds=money(baseRent*.10);
  // Keep rounded components tied exactly to the authoritative bank receipt.
  const gst=money(received-baseRent+tds);
  entries.push({...zero,id:r.id,reference:r.id,date:r.date,kind:'income',entity:'SAMAST',category:'Rental income',description:r.particulars||r.source||r.payer||'Rent receipt',account:r.account||'',amount:baseRent,profitKind:'rent',baseRent,gst,tds,received,expense:0,salary:0,profit:baseRent,proof:r.proof||r.paymentProof||'',proofs:r.proofs||r.paymentProofs||[],bankReference:r.bankReconciliationEvidence?.reference||r.bankReference||'',bankDescription:r.bankReconciliationEvidence?.description||'',payer:r.payer||r.receivedFrom||r.source||''});
 }
 for(const r of costs){
  const isSalary=r.kind==='salary'||salaryLinks.has(r.id)||[...salaryLinks].some(id=>String(r.id).startsWith(id+'/'))||/\bsalar(?:y|ies)\b/i.test(r.category||'');
  const payroll=list(salary.salaryPayments).find(p=>String(r.id).startsWith(p.id+'/'));
  const amount=money(r.expenses),tax=taxKind(r);
  const profitKind=tax?'tax':isSalary?'salary':'expense';
  entries.push({...r,...zero,proof:r.proof||payroll?.proofs?.[0]||'',proofs:payroll?.proofs||[],vendor:r.vendor||payroll?.employeeName||salary.employees?.[payroll?.empId]?.name||'',kind:'expense',entity:'SAMAST',amount,profitKind,expense:profitKind==='expense'?amount:0,salary:profitKind==='salary'?amount:0,profit:tax?0:-amount,...(tax?{[tax]:amount}:{})});
 }
 const fields=['baseRent','expense','salary','profit','gst','tds','received','gstPaid','incomeTaxPaid'];
 const total=rows=>Object.fromEntries(fields.map(key=>[key,money(rows.reduce((n,r)=>n+r[key],0))]));
 const months=[];
 for(let month=from.slice(0,7);month<=to.slice(0,7);){
  const start=month+'-01',next=new Date(start+'T00:00:00Z');next.setUTCMonth(next.getUTCMonth()+1);const nextMonth=next.toISOString().slice(0,7),end=new Date(next.getTime()-86400000).toISOString().slice(0,10);
  months.push({month,from:from>start?from:start,to:to<end?to:end,partial:from>start||to<end,...total(entries.filter(r=>r.date.startsWith(month)))});month=nextMonth;
 }
 return {basis:'Receipts/payments basis · Base rent = rent bank receipt ÷ 1.08 (18% GST added, 10% TDS deducted). Profit before income tax = base rent − Samast expenses paid − Samast salaries paid. Net rent received = base rent + 18% GST − 10% TDS for every tenant. GST remittances and actual income tax paid are shown separately and excluded from operating expenses. Salaries are read from Samast payroll on each report refresh and grouped by payment date, not payroll month. TDS deducted is not confirmation of tax credit deposited. Unpaid rent, personal spending, transfers, refundable deposits and loan principal are excluded.',months,totals:total(entries),entries:require('./report-expense-entries').enrichMany(store,entries).sort((a,b)=>b.date.localeCompare(a.date))};
}
module.exports={build,taxKind};

'use strict';
const {money}=require('./physical-bank-balance');
const {termsAt}=require('./rental-register');
const list=value=>Array.isArray(value)?value:Object.values(value||{});
const validMonth=value=>/^\d{4}-(?:0[1-9]|1[0-2])$/.test(String(value||''));
const daysInMonth=month=>new Date(Date.UTC(+month.slice(0,4),+month.slice(5,7),0)).getUTCDate();
const activeInvoice=invoice=>invoice&&invoice.voidedAt==null&&invoice.reversedAt==null&&!invoice.opening&&validMonth(invoice.month);
function monthWindow(tenancy,month){
 if(!tenancy||tenancy.status==='ended'&&!tenancy.endDate)return null;
 const start=String(tenancy.startDate||((tenancy.trackingFrom||tenancy.startMonth||'')+'-01')),end=String(tenancy.endDate||''),last=daysInMonth(month);
 if(start.slice(0,7)>month||end&&end.slice(0,7)<month)return null;
 const first=start.slice(0,7)===month?Math.max(1,Math.min(last,Number(start.slice(8))||1)):1;
 const final=end.slice(0,7)===month?Math.max(first,Math.min(last,Number(end.slice(8))||last)):last;
 return {first,final,days:final-first+1,totalDays:last,factor:(final-first+1)/last};
}
function rates(terms){
 let gst=Number(terms.cgst||0)+Number(terms.sgst||0),tds=Number(terms.tds||0);
 // The imported rent sheet supplies base and net amounts but no split rates.
 // Its 1.08 net factor is the configured 18% GST less 10% TDS arrangement.
 if(terms.netRent!=null&&!gst&&!tds){gst=18;tds=10;}
 return {gst,tds};
}
function desiredRentEntries(store,months){
 const rental=store&&store.rentals,tenancies=list(rental&&rental.tenancies),invoices=list(rental&&rental.invoices).filter(activeInvoice),evidence=list(rental&&rental.tdsEvidence),entries=[];
 for(const month of months)for(const tenancy of tenancies){
  const window=monthWindow(tenancy,month);if(!window)continue;
  const terms=termsAt(tenancy,month),invoice=invoices.find(item=>item.tenancyId===tenancy.id&&item.month===month),configured=rates(terms);
  let baseDue,gstExpected,tdsExpected,desiredNet,source;
  if(invoice){
   baseDue=money(Number(invoice.baseRent)||0);tdsExpected=money(baseDue*configured.tds/100);
   gstExpected=money(baseDue*configured.gst/100);desiredNet=money(baseDue+gstExpected-tdsExpected);source='Monthly base-rent charge';
  }else{
   baseDue=money(Number(terms.baseRent||0)*window.factor);tdsExpected=money(baseDue*configured.tds/100);
   gstExpected=money(baseDue*configured.gst/100);desiredNet=money(baseDue+gstExpected-tdsExpected);
   source=window.factor===1?'Tenant base-rent setup':'Tenant base-rent setup · automatic '+window.days+'/'+window.totalDays+' day proration';
  }
  const saved=evidence.find(item=>item.tenancyId===tenancy.id&&item.month===month),status=saved?.status||invoice?.tdsStatus||'pending',reference=saved?.reference||invoice?.tdsReference||'',tdsVerified=status==='verified'?tdsExpected:0;
  entries.push({id:invoice?.id||'RENT-DESIRED-'+tenancy.id+'-'+month,reference:invoice?.id||tenancy.id+'/'+month,date:invoice?.dueDate||month+'-01',kind:'rent_due',entity:'SAMAST',category:'Rent to collect',description:(tenancy.tenant||'Tenant')+' · '+(tenancy.floor||month)+' · '+source,tenant:tenancy.tenant||'',floor:tenancy.floor||'',tenancyId:tenancy.id,payer:tenancy.tenant||'',account:'',amount:desiredNet,profitKind:'rent_due',baseDue,baseRent:0,gst:0,tds:0,received:0,unassignedRent:0,desiredNet,rentDifference:desiredNet,gstExpected,gstPending:gstExpected,tdsExpected,tdsVerified,tdsPending:money(tdsExpected-tdsVerified),tdsEvidenceStatus:status,tdsReference:reference,expense:0,salary:0,profit:0,gstPaid:0,incomeTaxPaid:0});
 }
 return entries;
}
function receiptSplit(store,receipt,due,previousReceived=0){
 const tenancy=list(store&&store.rentals&&store.rentals.tenancies).find(item=>item.id===receipt.tenancyId);if(!tenancy)return null;
 if(!due||due.tenancyId!==tenancy.id)return null;
 const received=money(Number(receipt.amount)||0),net=Number(due.desiredNet)||0;
 if(net<=0)return null;
 // Bank money is evidence of what arrived, never the source of the tax rate.
 // Allocate the agreed base/GST/TDS charge by the portion of its net receivable
 // covered so far. A second receipt continues the same charge; an excess does
 // not invent additional rent or tax.
 const before=Math.min(net,Math.max(0,Number(previousReceived)||0)),after=Math.min(net,before+received),portion=(after-before)/net;
 return {tenancy,received,baseRent:money(due.baseDue*portion),gst:money(due.gstExpected*portion),tds:money(due.tdsExpected*portion),covered:money(after-before),excess:money(received-(after-before)),calculationSource:'Agreed base rent'};
}

function payrollEarnedEntries(salary,months,from,to,zero){
 const compute=require('./salary')._computeMonth,entries=[];
 if(!salary||!salary.employees||!Object.keys(salary.employees).length)return entries;
 for(const month of months){
  const totalDays=daysInMonth(month),start=month+'-01',end=month+'-'+String(totalDays).padStart(2,'0'),lo=from>start?from:start,hi=to<end?to:end;
  if(lo>hi)continue;
  const days=Math.round((Date.parse(hi)-Date.parse(lo))/86400000)+1;
  for(const row of compute(salary,month)){
   const amount=money(Number(row.salaryAmt||0));if(!amount)continue;
   entries.push({...zero,id:'PAYROLL-'+month+'-'+row.id,reference:month+'/'+row.id,date:hi,kind:'expense',entity:'SAMAST',category:'Live Payroll salary',description:row.name+' · '+row.paidDays+' paid days'+(days<totalDays?' · live '+month+' Payroll month':''),vendor:row.name,amount,profitKind:'salary',salary:amount,profit:-amount,payrollMonth:month,paidDays:row.paidDays,monthlySalary:row.salary,salaryAdjustment:row.salaryAdjustment||0,calculationSource:'Attendance-based Payroll'});
  }
 }
 return entries;
}
// Classify only explicit tax remittances, not professional/compliance service fees.
function taxKind(record){
 const category=String(record.ledger||record.category||'').trim(),description=[record.particulars,record.description,record.reconciliationSource?.bankDescription].filter(Boolean).join(' ');
 if(/^(?:gst|goods and services tax)\s*(?:paid|payment|remittance|challan)$/i.test(category)||(/^gst fee$/i.test(category)&&/\bGST\s*\//i.test(description)))return 'gstPaid';
 if(/\b(?:self[ -]?assess(?:ed|ment)\s+i\.?\s*t\.?|income[ -]?tax|self[ -]?assess(?:ed|ment)(?: income)? tax|advance income[ -]?tax)\b/i.test(category)&&!/consult|professional|filing|preparation|service/i.test(category))return 'incomeTaxPaid';
 return '';
}
// Receipt reporting only. Calculated tax components are never written back to accounting.
function build(store,{from,to,receipts,costs,salary={}}){
 const salaryLinks=new Set(list(salary.salaryPayments).filter(payment=>payment.active!==false&&!payment.accountingExcluded).map(payment=>payment.linkedLedgerEntryId).filter(Boolean)),entries=[];
 const zero={baseDue:0,baseRent:0,gst:0,tds:0,received:0,unassignedRent:0,desiredNet:0,rentDifference:0,gstExpected:0,gstPending:0,tdsExpected:0,tdsVerified:0,tdsPending:0,expense:0,salary:0,profit:0,gstPaid:0,incomeTaxPaid:0};
 const selectedMonths=[];for(let month=from.slice(0,7);month<=to.slice(0,7);){selectedMonths.push(month);const next=new Date(month+'-01T00:00:00Z');next.setUTCMonth(next.getUTCMonth()+1);month=next.toISOString().slice(0,7);}
 const rentDues=desiredRentEntries(store,selectedMonths);entries.push(...rentDues);
 const coverage=new Map();
 for(const receipt of receipts.slice().sort((a,b)=>String(a.date+a.id).localeCompare(String(b.date+b.id)))){
  const received=money(Number(receipt.amount)||0),month=String(receipt.rentMonth||receipt.month||receipt.date||'').slice(0,7),key=(receipt.tenancyId||'')+'|'+month,due=rentDues.find(row=>row.tenancyId===receipt.tenancyId&&row.date.startsWith(month)),before=coverage.get(key)||0,split=receiptSplit(store,receipt,due,before),common={...zero,id:receipt.id,reference:receipt.id,date:receipt.date,kind:'income',entity:'SAMAST',category:'Rental income',description:receipt.particulars||receipt.source||receipt.payer||'Rent receipt',account:receipt.account||'',received,rentDifference:-received,proof:receipt.proof||receipt.paymentProof||'',proofs:receipt.proofs||receipt.paymentProofs||[],bankReference:receipt.bankReconciliationEvidence?.reference||receipt.bankReference||'',bankDescription:receipt.bankReconciliationEvidence?.description||'',payer:receipt.payer||receipt.receivedFrom||receipt.source||'',tenancyId:receipt.tenancyId||''};
  if(!split)entries.push({...common,amount:received,profitKind:'rent_unassigned',unassignedRent:received});
  else{coverage.set(key,before+received);entries.push({...common,amount:split.baseRent,profitKind:'rent',tenant:split.tenancy.tenant||'',floor:split.tenancy.floor||'',baseRent:split.baseRent,gst:split.gst,tds:split.tds,gstPending:-split.gst,profit:split.baseRent,excessRentReceipt:split.excess,calculationSource:split.calculationSource});}
 }
 for(const record of costs){
  const isSalary=record.kind==='salary'||salaryLinks.has(record.id)||[...salaryLinks].some(id=>String(record.id).startsWith(id+'/'))||/\bsalar(?:y|ies)\b/i.test(record.category||''),payroll=list(salary.salaryPayments).find(payment=>String(record.id).startsWith(payment.id+'/')),amount=money(record.expenses),tax=taxKind(record),profitKind=tax?'tax':isSalary?'salary':'expense';
  if(isSalary)continue;
  entries.push({...record,...zero,proof:record.proof||payroll?.proofs?.[0]||'',proofs:payroll?.proofs||[],vendor:record.vendor||payroll?.employeeName||salary.employees?.[payroll?.empId]?.name||'',kind:'expense',entity:'SAMAST',amount,profitKind,expense:profitKind==='expense'?amount:0,salary:profitKind==='salary'?amount:0,profit:tax?0:-amount,...(tax?{[tax]:amount}:{})});
 }
 entries.push(...payrollEarnedEntries(salary,selectedMonths,from,to,zero));
 const fields=['baseDue','baseRent','expense','salary','profit','gst','tds','received','unassignedRent','desiredNet','rentDifference','gstExpected','gstPending','tdsExpected','tdsVerified','tdsPending','gstPaid','incomeTaxPaid'];
 const total=rows=>Object.fromEntries(fields.map(key=>[key,money(rows.reduce((sum,row)=>sum+row[key],0))]));
 const gstEvidencePending=rows=>money(rows.filter(row=>row.gstPaid&&!row.proof&&!list(row.proofs).length&&!row.bankReference&&!row.bankReconciliationEvidence?.reference&&!row.reconciliationSource?.bankReference).reduce((sum,row)=>sum+row.gstPaid,0));
 const status=values=>values.tdsExpected<=0?'Not applicable':values.tdsVerified>=values.tdsExpected?'Submitted':values.tdsVerified>0?'Partly submitted':'Pending';
 const months=[];
 for(let month=from.slice(0,7);month<=to.slice(0,7);){
  const start=month+'-01',next=new Date(start+'T00:00:00Z');next.setUTCMonth(next.getUTCMonth()+1);const nextMonth=next.toISOString().slice(0,7),end=new Date(next.getTime()-86400000).toISOString().slice(0,10),values=total(entries.filter(row=>row.date.startsWith(month)));
  if(values.unassignedRent>0)for(const key of ['baseRent','gst','tds','profit'])values[key]=null;
  if(values.gst!=null){values.gstLiability=money(Math.max(0,values.gst-values.gstPaid));values.gstCredit=money(Math.max(0,values.gstPaid-values.gst));values.gstEvidencePending=gstEvidencePending(entries.filter(row=>row.date.startsWith(month)));}
  if(values.tds!=null)values.tdsPending=money(Math.max(0,values.tds-values.tdsVerified));
  months.push({month,from:from>start?from:start,to:to<end?to:end,partial:from>start||to<end,taxSplitComplete:values.unassignedRent===0,tdsStatus:status(values),...values});month=nextMonth;
 }
 const totals=total(entries);if(totals.unassignedRent>0)for(const key of ['baseRent','gst','tds','profit'])totals[key]=null;if(totals.gst!=null){totals.gstLiability=money(Math.max(0,totals.gst-totals.gstPaid));totals.gstCredit=money(Math.max(0,totals.gstPaid-totals.gst));totals.gstEvidencePending=gstEvidencePending(entries);}if(totals.tds!=null)totals.tdsPending=money(Math.max(0,totals.tds-totals.tdsVerified));totals.tdsStatus=status(totals);
 const taxableProfit=totals.profit==null?null:money(Math.max(0,totals.profit));
 return {basis:'Rent to collect is calculated only from agreed base rent: base rent + GST − TDS. Actual rent received is the linked bank credit; its rent/GST/TDS breakdown is allocated from that agreed charge, never reverse-calculated from the bank amount.',months,totals,reconciliation:{gst:{calculatedCollected:totals.gst,paid:totals.gstPaid,outstanding:totals.gstLiability,evidencePending:totals.gstEvidencePending,status:totals.gstLiability||totals.gstEvidencePending?'pending':'reconciled'},tds:{calculatedWithheld:totals.tds,verified:totals.tdsVerified,pending:totals.tdsPending,status:totals.tdsPending?'pending':'reconciled'}},incomeTax:{status:'awaiting_rules',taxableProfit,calculated:null,paid:totals.incomeTaxPaid,outstanding:null,note:'Self-assessed income-tax slabs and calculation rules are awaiting Owner/CA confirmation.'},entries:require('./report-expense-entries').enrichMany(store,entries).sort((a,b)=>b.date.localeCompare(a.date))};
}
module.exports={build,taxKind,desiredRentEntries,receiptSplit,payrollEarnedEntries};

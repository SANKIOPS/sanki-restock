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
   baseDue=money(Number(invoice.baseRent)||0);tdsExpected=money(invoice.taxBreakdownUnknown?baseDue*configured.tds/100:Number(invoice.tds)||0);
   desiredNet=money(Number(invoice.bankDue)||0);gstExpected=money(invoice.taxBreakdownUnknown?desiredNet-baseDue+tdsExpected:(Number(invoice.cgst)||0)+(Number(invoice.sgst)||0));source='Manual monthly override';
  }else{
   baseDue=money(Number(terms.baseRent||0)*window.factor);tdsExpected=money(baseDue*configured.tds/100);
   desiredNet=money((terms.netRent==null?Number(terms.baseRent||0)*(1+(configured.gst-configured.tds)/100):Number(terms.netRent||0))*window.factor);
   gstExpected=money(desiredNet-baseDue+tdsExpected);source=window.factor===1?'Tenant setup':'Tenant setup · automatic '+window.days+'/'+window.totalDays+' day proration';
  }
  const saved=evidence.find(item=>item.tenancyId===tenancy.id&&item.month===month),status=saved?.status||invoice?.tdsStatus||'pending',reference=saved?.reference||invoice?.tdsReference||'',tdsVerified=status==='verified'?tdsExpected:0;
  entries.push({id:invoice?.id||'RENT-DESIRED-'+tenancy.id+'-'+month,reference:invoice?.id||tenancy.id+'/'+month,date:invoice?.dueDate||month+'-01',kind:'rent_due',entity:'SAMAST',category:'Desired rent',description:(tenancy.tenant||'Tenant')+' · '+(tenancy.floor||month)+' · '+source,tenant:tenancy.tenant||'',floor:tenancy.floor||'',tenancyId:tenancy.id,payer:tenancy.tenant||'',account:'',amount:desiredNet,profitKind:'rent_due',baseRent:0,gst:0,tds:0,received:0,unassignedRent:0,desiredNet,rentDifference:desiredNet,gstExpected,gstPending:gstExpected,tdsExpected,tdsVerified,tdsPending:money(tdsExpected-tdsVerified),tdsEvidenceStatus:status,tdsReference:reference,expense:0,salary:0,profit:0,gstPaid:0,incomeTaxPaid:0});
 }
 return entries;
}
function receiptSplit(store,receipt){
 const tenancy=list(store&&store.rentals&&store.rentals.tenancies).find(item=>item.id===receipt.tenancyId);if(!tenancy)return null;
 const terms=termsAt(tenancy,String(receipt.date||'').slice(0,7)),configured=rates(terms),factor=1+(configured.gst-configured.tds)/100;
 if(!Number.isFinite(factor)||factor<=0)return null;
 const received=money(Number(receipt.amount)||0),baseRent=money(received/factor),tds=money(baseRent*configured.tds/100),gst=money(received-baseRent+tds);
 return {tenancy,received,baseRent,gst,tds};
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
 const zero={baseRent:0,gst:0,tds:0,received:0,unassignedRent:0,desiredNet:0,rentDifference:0,gstExpected:0,gstPending:0,tdsExpected:0,tdsVerified:0,tdsPending:0,expense:0,salary:0,profit:0,gstPaid:0,incomeTaxPaid:0};
 const selectedMonths=[];for(let month=from.slice(0,7);month<=to.slice(0,7);){selectedMonths.push(month);const next=new Date(month+'-01T00:00:00Z');next.setUTCMonth(next.getUTCMonth()+1);month=next.toISOString().slice(0,7);}
 entries.push(...desiredRentEntries(store,selectedMonths));
 for(const receipt of receipts){
  const received=money(Number(receipt.amount)||0),split=receiptSplit(store,receipt),common={...zero,id:receipt.id,reference:receipt.id,date:receipt.date,kind:'income',entity:'SAMAST',category:'Rental income',description:receipt.particulars||receipt.source||receipt.payer||'Rent receipt',account:receipt.account||'',received,rentDifference:-received,proof:receipt.proof||receipt.paymentProof||'',proofs:receipt.proofs||receipt.paymentProofs||[],bankReference:receipt.bankReconciliationEvidence?.reference||receipt.bankReference||'',bankDescription:receipt.bankReconciliationEvidence?.description||'',payer:receipt.payer||receipt.receivedFrom||receipt.source||'',tenancyId:receipt.tenancyId||''};
  if(!split)entries.push({...common,amount:received,profitKind:'rent_unassigned',unassignedRent:received});
  else entries.push({...common,amount:split.baseRent,profitKind:'rent',tenant:split.tenancy.tenant||'',floor:split.tenancy.floor||'',baseRent:split.baseRent,gst:split.gst,tds:split.tds,gstPending:-split.gst,profit:split.baseRent});
 }
 for(const record of costs){
  const isSalary=record.kind==='salary'||salaryLinks.has(record.id)||[...salaryLinks].some(id=>String(record.id).startsWith(id+'/'))||/\bsalar(?:y|ies)\b/i.test(record.category||''),payroll=list(salary.salaryPayments).find(payment=>String(record.id).startsWith(payment.id+'/')),amount=money(record.expenses),tax=taxKind(record),profitKind=tax?'tax':isSalary?'salary':'expense';
  entries.push({...record,...zero,proof:record.proof||payroll?.proofs?.[0]||'',proofs:payroll?.proofs||[],vendor:record.vendor||payroll?.employeeName||salary.employees?.[payroll?.empId]?.name||'',kind:'expense',entity:'SAMAST',amount,profitKind,expense:profitKind==='expense'?amount:0,salary:profitKind==='salary'?amount:0,profit:tax?0:-amount,...(tax?{[tax]:amount}:{})});
 }
 const fields=['baseRent','expense','salary','profit','gst','tds','received','unassignedRent','desiredNet','rentDifference','gstExpected','gstPending','tdsExpected','tdsVerified','tdsPending','gstPaid','incomeTaxPaid'];
 const total=rows=>Object.fromEntries(fields.map(key=>[key,money(rows.reduce((sum,row)=>sum+row[key],0))]));
 const status=values=>values.tdsExpected<=0?'Not applicable':values.tdsVerified>=values.tdsExpected?'Submitted':values.tdsVerified>0?'Partly submitted':'Pending';
 const months=[];
 for(let month=from.slice(0,7);month<=to.slice(0,7);){
  const start=month+'-01',next=new Date(start+'T00:00:00Z');next.setUTCMonth(next.getUTCMonth()+1);const nextMonth=next.toISOString().slice(0,7),end=new Date(next.getTime()-86400000).toISOString().slice(0,10),values=total(entries.filter(row=>row.date.startsWith(month)));
  if(values.unassignedRent>0)for(const key of ['baseRent','gst','tds','profit'])values[key]=null;
  months.push({month,from:from>start?from:start,to:to<end?to:end,partial:from>start||to<end,taxSplitComplete:values.unassignedRent===0,tdsStatus:status(values),...values});month=nextMonth;
 }
 const totals=total(entries);if(totals.unassignedRent>0)for(const key of ['baseRent','gst','tds','profit'])totals[key]=null;totals.tdsStatus=status(totals);
 return {basis:'Simple monthly rent view · Desired receipt comes automatically from tenant setup as base rent + configured GST − configured TDS; first and final partial months are prorated by active days, with any manual monthly charge used as an override. Actual receipt is the recorded bank rent credit. Difference = desired − actual. Base, GST and TDS are calculated from actual receipts using the selected tenant’s configured rates. A receipt without a selected tenant keeps its full bank amount but shows Select tenant for the split and profit. TDS status is Submitted only after its month/tenant evidence is verified. Profit before income tax = calculated base rent from tenant-linked actual receipts − Samast expenses paid − Samast salaries paid.',months,totals,entries:require('./report-expense-entries').enrichMany(store,entries).sort((a,b)=>b.date.localeCompare(a.date))};
}
module.exports={build,taxKind,desiredRentEntries,receiptSplit};

'use strict';

// A read-only presentation of the existing Results events. Preserve paid-cost,
// GST, refund and SKU-cost rules; never infer costs from transfers or purchases.
const {money}=require('./physical-bank-balance');
const catalog=require('./sanki-category-plan.json').catalog||[];
const marketing=new Set(catalog.filter(c=>c.group==='Marketing & Promotion').map(c=>c.name.toLowerCase()));
const orderCosts=new Set(['zipper bags expense','courier bags expense','packing materials','packaging','packaging expense','courier charges','courier expense','delivery charges','shipping charges','return shipping','logistics charges','payment gateway charges','payment processing fees','platform fees','marketplace commission','sales incentives','incentive expense','bank charges','paytm charges','gokwik charges']);
const special={interest:new Set(['loan interest','bank interest paid','interest expense','interest paid']),tax:new Set(['income tax','income tax expense','corporate tax']),depreciation:new Set(['depreciation','depreciation expense','amortisation','amortization','amortisation expense','amortization expense'])};
function bucket(row){
  const category=String(row.category||'').trim().toLowerCase();
  for(const [key,names]of Object.entries(special))if(names.has(category)||key==='interest'&&row.kind==='loan_interest')return key;
  if(row.group==='marketing'||marketing.has(category))return 'marketing';
  if(orderCosts.has(category)||row.kind==='incentive')return 'orderCosts';
  return 'operatingExpenses';
}
function build(report,{blocked=false,books={}}={}){
  const receipts=Array.isArray(books.receipts)?books.receipts:Object.values(books.receipts||{});
  const totals=report.totals,values={sales:totals.grossSales,discountsReturns:money(totals.discounts+totals.returns),gst:totals.gst,netSales:totals.netSales,cogs:totals.cogs,grossProfit:totals.grossProfit,otherIncome:totals.otherIncome,orderCosts:0,marketing:0,operatingExpenses:0,interest:0,tax:0,depreciation:0,interestIncome:0};
  let units=0,unitsKnown=true;
  const entries=report.transactions.map(row=>{
    const v={sales:row.grossSales,discountsReturns:money(row.discounts+row.returns),gst:row.gst,netSales:row.netSales,cogs:row.cogs,otherIncome:row.otherIncome};
    if(row.expenses){const key=bucket(row);v[key]=row.expenses;values[key]=money(values[key]+row.expenses);}
    // Interest earned is disclosed but is outside operating earnings.
    if(row.otherIncome&&(receipts.some(r=>r.id===row.reference&&r.receiptType==='bank_interest')||/^(?:bank interest|bank interest received|bank interest earned|interest income)$/i.test(row.category||''))){v.interestIncome=row.otherIncome;v.otherIncome=0;values.interestIncome=money(values.interestIncome+row.otherIncome);}
    if(['sale','return'].includes(row.kind)){
      const items=(row.details||[]).filter(d=>!['shipping','rounding'].includes(d.itemKind));
      if(!items.length||items.some(d=>!(Number(d.qty)>0)))unitsKnown=false;
      else units+=items.reduce((n,d)=>n+Number(d.qty),0)*(row.kind==='return'?-1:1);
    }
    v.grossProfit=money((row.netSales||0)-row.cogs);
    v.ebitda=money(v.grossProfit+row.otherIncome-(v.interestIncome||0)-(v.orderCosts||0)-(v.marketing||0)-(v.operatingExpenses||0));
    v.otherDeductions=money((v.interest||0)+(v.tax||0)+(v.depreciation||0));
    v.netProfit=money(v.grossProfit+row.otherIncome-row.expenses);
    return {...row,kind:row.expenses?'expense':row.otherIncome?'income':row.kind,entity:'SANKI',description:row.particulars,values:v};
  });
  values.otherIncome=money(values.otherIncome-values.interestIncome);
  values.otherDeductions=money(values.interest+values.tax+values.depreciation);
  values.ebitda=totals.complete&&!blocked?money(totals.grossProfit+values.otherIncome-values.orderCosts-values.marketing-values.operatingExpenses):null;
  values.netProfit=blocked?null:totals.netProfit;
  if(blocked)values.grossProfit=null;
  return {values,entries,stats:{orders:totals.salesCount,units:unitsKnown?units:null,averageOrder:totals.netSales!=null&&totals.salesCount>0?money(totals.netSales/totals.salesCount):null,profitPerItem:unitsKnown&&units>0&&values.netProfit!=null?money(values.netProfit/units):null},
    basis:'Management report: completed sales excluding GST and sold-item COGS; expenses follow recorded paid amounts. EBITDA excludes interest, income tax, depreciation and amortisation. Unpaid bills are outside this payment-based view.',
    costSource:report.costSource?.name||'Verified SKU cost'};
}
module.exports={build,bucket};

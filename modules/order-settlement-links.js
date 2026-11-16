'use strict';
const money=v=>Math.round(Number(v||0)*100);
const ref=v=>String(v||'').toUpperCase().replace(/[^A-Z0-9]/g,'');
function bankEvidence(accounting, reference, expected, options={}){
 const target=ref(reference);if(target.length<8)return null;
 const found=[];
 const inspect=(bookKey,book,row,meta={})=>{
  if(options.account&&String(book.account||bookKey).replace(/^SANKI\|/,'')!==options.account)return;
  if(options.date&&(!row.date||String(row.date).slice(0,10)<options.date||String(row.date).slice(0,10)>new Date(Date.parse(options.date)+14*86400000).toISOString().slice(0,10)))return;
  const texts=[row.reference,row.description,row.narration,row.utr].filter(Boolean).map(String);
  const exact=texts.some(text=>ref(text)===target||text.toUpperCase().split(/[^A-Z0-9]+/).includes(target));
  if(!exact||money(row.credit)!==money(expected)||!(Number(row.credit)>0))return;
  const account=book.account||bookKey,date=String(row.date||'').slice(0,10),signature=[account,date,money(row.credit),target].join('|');
  found.push({account,date:row.date,reference,amount:Number(row.credit),transactionId:row.id||meta.rowId,signature,...meta});
 };
 for(const [bookKey,book] of Object.entries(accounting.bankStatements||{}))for(const row of Object.values(book.transactions||{}))inspect(bookKey,book,row);
 if(options.includeDrafts)for(const [draftKey,draft] of Object.entries(accounting.bankReconciliationDrafts||{})){
  if(draft.summary&&draft.summary.validated===false)continue;
  (draft.transactions||[]).forEach((row,index)=>{const rowId='bank-'+index;if((draft.resolutions||{})[rowId])return;inspect(draft.account||draftKey,draft,row,{draftId:draft.id||draftKey,rowId,pending:true});});
 }
 if(found.length===1)return found[0];
 // An overlapping preview can contain the same immutable row as one finalized
 // statement. Prefer that single finalized item, but never collapse two exact
 // finalized bank rows (which must remain ambiguous).
 const finalized=found.filter(item=>!item.pending);
 return finalized.length===1&&found.every(item=>item.signature===finalized[0].signature)?finalized[0]:null;
}
function attachSettlementLinks(orders,accounting,online,finance){
 const groups=new Map();
 for(const row of online){
  if(!row.utr)continue;const key='online|'+row.utr,g=groups.get(key)||{reference:row.utr,net:0,invalid:false};
  let net;
  if(row.source==='gokwik_api_v2'){
   net=Number(row.net);if(!Number.isFinite(net))g.invalid=true;
  }else if(row.credit!=null&&row.reportedGokwikDeduction!=null){
   net=(money(row.credit)-money(row.reportedGokwikDeduction))/100;
   if(net<0||(money(row.debit)!==0&&money(row.debit)!==money(net)))g.invalid=true;
  }else{net=Number(row.net);if(!Number.isFinite(net)||net<0)g.invalid=true;}
  g.net+=Number.isFinite(net)?net:0;groups.set(key,g);
 }
 // COD API provides gross collection, not net batch deductions. Do not infer
 // the net bank payout by summing collections. Only imported payout data can
 // establish an expected COD batch total.
 for(const order of orders){
  order.bankEvidence=[];
  for(const row of order.onlineEvidence||[]){const group=groups.get('online|'+row.utr);const match=group&&!group.invalid&&bankEvidence(accounting,group.reference,group.net,{account:'Tiana Traders IndusInd 0437'});if(match&&!order.bankEvidence.some(x=>x.reference===match.reference))order.bankEvidence.push(match);}
  const links=Object.entries(accounting.paytmOrderLinks||{}).filter(([,v])=>String(v.orderId)===String(order.id));
  order.posClearance=links.map(([transactionId,link])=>{
   const tx=(accounting.paytmReportTransactions||{})[transactionId]||{};
   const posting=(accounting.paytmPayoutPostings||[]).find(p=>String(p.payoutId)===String(tx.payoutId));
   return {transactionId,payoutId:tx.payoutId||'',status:posting?'Posted through existing POS reconciliation':'Linked · bank clearance pending',reference:posting&&posting.utr||'',amount:Number(tx.amount)||null};
  });
  if(order.channel==='POS'&&order.posClearance.length)order.settlementStatus='Existing POS reconciliation linked';
  if(order.bankEvidence.length)order.settlementStatus='Bank credit matched · batch allocation review pending';
 }
 return orders;
}
module.exports={attachSettlementLinks,bankEvidence};

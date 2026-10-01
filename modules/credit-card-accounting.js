'use strict';
const number=v=>Number(v)||0;
const money=v=>Math.round(number(v)*100)/100;
const expenseClasses=['expense','fee','emi_interest','refund'];
function merchantName(narration,bank=''){
  const text=String(narration||'').trim();
  const known=[[/facebook|\bmeta\b/i,'Facebook'],[/linkedin/i,'LinkedIn'],[/shopify/i,'Shopify'],[/amazon/i,'Amazon'],[/swiggy/i,'Swiggy'],[/zomato/i,'Zomato'],[/google/i,'Google'],[/microsoft/i,'Microsoft'],[/apple\.com|\bapple\b/i,'Apple']];
  for(const [pattern,name] of known)if(pattern.test(text))return name;
  if(/membership\s*fee|statement.*rounding|annual.*fee|\bigst\b|\bgst\b|emi.*interest|finance.*charge|payment.*thank|autopay/i.test(text)){
    const issuer=String(bank||'Card issuer').trim();return /hdfc/i.test(issuer+' '+text)?'HDFC Bank':/bank$/i.test(issuer)?issuer:issuer+' Bank';
  }
  if(/\bhdfc\b/i.test(text))return'HDFC Bank';
  const cleaned=text.replace(/\([^)]*ref[^)]*\)/ig,'').replace(/\bhttps?:\/\/\S+|\bwww\.\S+/ig,'').replace(/^(?:EMI|RAZ|POS|UPI|ECOM|IND)[*\s.-]+/i,'').replace(/\b(?:India|Gurugram|Gurgaon|Hyderabad|Delhi|Mumbai|Bangalore|Bengaluru)\b/ig,'').replace(/\b(?:ref|txn|reference)[#:\s]*\w+/ig,'').replace(/[*|]+/g,' ').replace(/\s+/g,' ').trim();
  return cleaned||'Unknown merchant';
}
function activeRow(row){return !row.supersededBy&&!row.duplicateOf&&!['duplicate','link','merge','replace'].includes(row.duplicateResolution);}
function totals(rows){const credit=money(rows.reduce((n,r)=>n+number(r.credit),0)),debit=money(rows.reduce((n,r)=>n+number(r.debit),0));return{credit,debit,netMovement:money(debit-credit)};}
function rowMatches(a,b){
  if(a.date!==b.date||money(a.debit)!==money(b.debit)||money(a.credit)!==money(b.credit))return false;
  const ref=x=>String(x||'').replace(/[^a-z0-9]/ig,'').toLowerCase();
  if(a.reference&&b.reference)return ref(a.reference)===ref(b.reference);
  return merchantName(a.merchant||a.narration).toLowerCase()===merchantName(b.merchant||b.narration).toLowerCase();
}
function matchRows(store,statement){
  const used=new Set();
  const candidates=Object.values(store.statements||{}).filter(s=>s.id!==statement.id&&s.cardId===statement.cardId&&s.status==='finalized').flatMap(s=>(s.rows||[]).filter(activeRow).map(row=>({statement:s,row}))).sort((a,b)=>Number(a.statement.kind==='unbilled')-Number(b.statement.kind==='unbilled'));
  for(const row of statement.rows||[]){
    const match=candidates.find(x=>!used.has(x.statement.id+'/'+x.row.id)&&rowMatches(row,x.row));if(!match)continue;
    used.add(match.statement.id+'/'+match.row.id);
    const replacement=statement.kind!=='unbilled'&&match.statement.kind==='unbilled';
    row[replacement?'replaces':'duplicateOf']={statementId:match.statement.id,rowId:match.row.id};
    if(replacement)for(const key of ['merchant','classification','type','category','nature','channel'])if(!row[key]&&match.row[key])row[key]=match.row[key];
  }
  return statement;
}
function syncPostings(store,expenses){
  expenses.reconciliationExpenses=(expenses.reconciliationExpenses||[]).filter(e=>!e.creditCardStatementId);
  expenses.vendors=expenses.vendors||{};expenses.vendorsByNature=expenses.vendorsByNature||{};
  for(const st of Object.values(store.statements||{}).filter(s=>s.status==='finalized')){
    const card=store.cards[st.cardId];if(!card)continue;
    for(const row of (st.rows||[]).filter(r=>activeRow(r)&&expenseClasses.includes(r.classification))){
      const merchant=merchantName(row.merchant||row.narration,card.issuingBank||card.name);
      expenses.reconciliationExpenses.push({id:'CCE-'+st.id+'-'+row.id,nature:row.nature,date:row.date,amount:row.classification==='refund'?-number(row.amount):number(row.amount),account:card.name+' '+card.last4,category:row.category,type:row.type||(/marketing|advertis/i.test(row.category||'')?'marketing':/fee|charge|subscription/i.test(row.category||'')?'running':'variable'),vendor:merchant,particulars:row.narration,channel:row.channel||'',creditCardId:card.id,ownerOnly:!!card.ownerOnly,creditCardStatementId:st.id,creditCardRowId:row.id,classification:row.classification,unbilled:st.kind==='unbilled',source:'credit_card_statement',createdBy:st.finalizedBy,createdAt:st.finalizedAt});
      const master=row.nature==='SANKI'?expenses.vendors:(expenses.vendorsByNature[row.nature]=expenses.vendorsByNature[row.nature]||{});
      const key=Object.keys(master).find(k=>String(master[k].name||'').toLowerCase()===merchant.toLowerCase())||merchant.toLowerCase();
      const saved=master[key]||{name:merchant,notes:''};saved.tags=Array.from(new Set([...(saved.tags||[]),'Credit-card merchant']));master[key]=saved;
    }
  }
}
function cardExpenseRecords(expenses){return(expenses.reconciliationExpenses||[]).filter(e=>e.creditCardStatementId).map(e=>({...e,vendor:merchantName(e.vendor||e.particulars,e.account),ledger:e.category,status:'paid',paidAmount:number(e.amount),requestedAmount:number(e.amount),paymentType:'Credit',source:'credit_card_statement',statementBacked:true,readOnly:true,bill:'statement',approvedAt:e.createdAt,approvedBy:e.createdBy,claimant:'',payments:[{id:'STATEMENT',date:e.date,amount:number(e.amount),account:e.account,paymentType:'Credit',personalFunds:false,unpayBlockedReason:'Correct this transaction in its credit-card statement.'}],reimbursementStatus:'not_applicable'}));}
module.exports={merchantName,activeRow,totals,rowMatches,matchRows,syncPostings,cardExpenseRecords};

// Mobile current-cycle lists often contain one amount and no running balance.
function parseUnbilledTransactions(raw){
  const text=String(raw||'').replace(/\r/g,''),anchors=Array.from(text.matchAll(/(?:^|\n)\s*(\d{4}[-/.]\d{1,2}[-/.]\d{1,2}|\d{1,2}[-/.]\d{1,2}[-/.]\d{2,4}|\d{1,2}\s+[A-Za-z]{3,9}\s+\d{4})(?=\s|\||$)/g)),rows=[];
  const monthNames=['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec'];
  for(let i=0;i<anchors.length;i++){
    const anchor=anchors[i],dateText=anchor[1];let year,month,day,m;
    if((m=dateText.match(/^(\d{4})[-/.](\d{1,2})[-/.](\d{1,2})$/))){year=+m[1];month=+m[2];day=+m[3];}
    else if((m=dateText.match(/^(\d{1,2})[-/.](\d{1,2})[-/.](\d{2,4})$/))){day=+m[1];month=+m[2];year=+m[3]+(m[3].length===2?2000:0);}
    else{m=dateText.match(/^(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})$/);day=+m[1];month=monthNames.indexOf(m[2].slice(0,3).toLowerCase())+1;year=+m[3];}
    const d=new Date(Date.UTC(year,month-1,day));if(d.getUTCFullYear()!==year||d.getUTCMonth()+1!==month||d.getUTCDate()!==day)continue;
    const block=text.slice(anchor.index+anchor[0].length,i+1<anchors.length?anchors[i+1].index:text.length);
    const amountMatch=block.match(/(?:₹|INR\b|Rs\.?\s*)\s*([+-]?[\d,]+(?:\.\d{1,2})?)(?:\s*(CR|DR))?/i)||block.match(/([+-]?[\d,]+\.\d{2})\s*(CR|DR)\b/i);
    if(!amountMatch)continue;const amount=Math.abs(number(amountMatch[1].replace(/,/g,'')));if(!amount)continue;
    const description=block.slice(0,amountMatch.index).replace(/^\s*\|\s*/,'').replace(/\s+/g,' ').trim();if(!description)continue;
    const isCredit=/CR/i.test(amountMatch[2]||'')||(!/DR/i.test(amountMatch[2]||'')&&(/\brefund\b|\bpayment\b|\bcredit\b/i.test(description)||/^\+/.test(amountMatch[1])));
    rows.push({date:d.toISOString().slice(0,10),description,reference:((description.match(/\b(?:ref|txn)[#:\s]*([a-z0-9-]+)/i)||[])[1]||''),debit:isCredit?0:amount,credit:isCredit?amount:0,balance:0,row:i+1});
  }
  rows.statementSummary={format:'Current-cycle credit-card transactions',accountType:'credit_card',totalDebits:totals(rows).debit,totalCredits:totals(rows).credit};return rows;
}
module.exports.parseUnbilledTransactions=parseUnbilledTransactions;

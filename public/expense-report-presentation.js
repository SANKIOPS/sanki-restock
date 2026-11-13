'use strict';
(function(root,factory){const api=factory();if(typeof module==='object'&&module.exports)module.exports=api;else root.ExpenseReportPresentation=api;})(typeof window==='object'?window:this,function(){
 const string=value=>typeof value==='string'?value.trim():'';
 const normalize=value=>string(value).toLowerCase().replace(/\s+/g,' ');
 function isBankNarration(value,bankDescription=''){
  const text=string(value);if(!text)return false;
  if(/\b(?:UPI|IMPS|NEFT|RTGS|MMT|ACH|NACH|ECS|IFT|TRF)\s*[/|]/i.test(text))return true;
  if(/\b(?:BIL|VPS|IPS|ATM|POS|CMS|PAYTM)\s*[/|]/i.test(text)&&/\d{8,}/.test(text))return true;
  if(/^\s*(?:ACH|NACH|ECS|NEFT|RTGS|IMPS)\b.*\b(?:DR|CR|INW|OUT|REF|PAY|TRANSFER)\b/i.test(text))return true;
  if(/^\s*(?:UPI|IMPS|NEFT|RTGS|NACH|ACH|MMT)[-:]\s*\S/i.test(text)&&/\d{8,}/.test(text))return true;
  return !!string(bankDescription)&&normalize(text)===normalize(bankDescription)&&(/\d{8,}/.test(text)||/\b(?:UTR|RRN|IFSC)\b/i.test(text));
 }
 function clean(value,bankDescription){return isBankNarration(value,bankDescription)?'':string(value);}
 function partyName(value){const text=string(value);if(!isBankNarration(text))return text;const prefix=text.match(/^(.*?)\bUPI\s*\//i),segment=text.match(/\bUPI\s*\/([^/]+)/i),candidate=string(prefix?.[1])||string(segment?.[1]);return candidate&&!/[\/@]|\d{8,}/.test(candidate)&&!isBankNarration(candidate)&&/[\p{L}]/u.test(candidate)&&! /^(?:UPI|MMT|IMPS|NEFT|RTGS)$/i.test(candidate)?candidate:'';}
 function presentReceipt(receipt){const e=receipt||{},bank=e.bankReconciliationEvidence?.description||e.reconciliationSource?.bankDescription||'';
 const payer=[e.payerName,e.payer,e.receivedFrom,e.customerName,e.tenantName,e.source].map(partyName).find(Boolean)||'Not provided';
 const note=/^(?:Historical .*classified from verified bank statement|Captured from Owner Telegram receipt screenshot|Created (?:directly )?from bank reconciliation|Recorded while reconciling)/i.test(string(e.note))?'':e.note;
 const particulars=[e.particulars,e.description,e.narration,e.telegramNarration,note].map(v=>clean(v,bank)).find(Boolean)||'Not provided';
 return {payer,particulars,particularsMissing:particulars==='Not provided',type:'income',category:clean(e.category||e.receiptType,bank)||'Unclassified'};
 }
 function present(expense){const e=expense||{},bank=e.reconciliationSource?.bankDescription||'',choices=[e.productParticulars,e.productDescription,e.particulars,e.description,e.telegramNarration,e.narration],particulars=choices.map(v=>clean(v,bank)).find(Boolean)||'';
  return {vendor:partyName(e.vendor)||'Not provided',particulars:particulars||'Not provided',particularsMissing:!particulars,type:clean(e.productType,bank)||clean(e.expenseType,bank)||clean(e.type,bank)||'Not specified',category:clean(e.ledger||e.category,bank)||'Unclassified'};
 }
 return {isBankNarration,clean,partyName,present,presentReceipt};
});

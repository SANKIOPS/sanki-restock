'use strict';
const crypto=require('crypto');
const ENDPOINT='https://gkx.gokwik.co/v2/settlements/recon';
const day=v=>new Intl.DateTimeFormat('en-CA',{timeZone:'Asia/Kolkata',year:'numeric',month:'2-digit',day:'2-digit'}).format(new Date(v));
const shift=(d,n)=>new Date(Date.parse(d+'T12:00:00Z')+n*86400000).toISOString().slice(0,10);
const hash=v=>crypto.createHash('sha256').update(JSON.stringify(v)).digest('hex');
function units(value,field){
 if(value==null||value==='')return 0;
 const raw=String(value);if(!/^-?\d+(?:\.\d{1,4})?$/.test(raw))throw Error('Invalid GoKwik API amount: '+field);
 const n=Math.round(Number(raw)*10000);if(!Number.isSafeInteger(n))throw Error('GoKwik API amount exceeds safe precision');return n;
}
function id(value){if(value==null)return '';const s=String(value).trim();if(/^\d+(?:\.\d+)?[eE][+-]?\d+$/.test(s)||typeof value==='number'&&!Number.isSafeInteger(value))throw Error('GoKwik API returned an unsafe order identifier');return s;}
function normalize(row,settlementDay){
 const type=String(row.transaction_type||'').trim();if(!type)throw Error('GoKwik API transaction type missing');
 if(row.credit==null||row.debit==null)throw Error('GoKwik API credit/debit fields missing');
 const credit=units(row.credit,'credit'),debit=units(row.debit,'debit');
 if(credit<0||debit<0)throw Error('GoKwik API credit/debit cannot be negative');
 const rawDeductions=row.deductions==null?{}:row.deductions;if(typeof rawDeductions!=='object'||Array.isArray(rawDeductions))throw Error('Invalid GoKwik API split deductions');
 const deductions=Object.fromEntries(Object.entries(rawDeductions).map(([k,v])=>[k,units(v,'deductions')/10000]));
 const split=Object.values(rawDeductions).reduce((n,v)=>n+units(v,'deductions'),0);
 const fees=['fee','tax','additional_fees','additional_tax'].reduce((n,k)=>n+units(row[k],k),0);
 if(row.settlement_date&&day(row.settlement_date)!==settlementDay)throw Error('GoKwik API returned a different settlement date');
 const canonical={paymentId:id(row.payment_id),referenceId:id(row.reference_id),orderId:id(row.merchant_order_id),orderNumber:id(row.platform_order_id),type:type.toLowerCase()==='payment'?'Payment':type,providerOrderId:id(row.order_id),gross:units(row.amount,'amount')/10000,fees:fees/10000,fee:units(row.fee,'fee')/10000,tax:units(row.tax,'tax')/10000,additionalFee:units(row.additional_fees,'additional_fees')/10000,additionalTax:units(row.additional_tax,'additional_tax')/10000,credit:credit/10000,debit:debit/10000,deductions,reportedGokwikDeduction:deductions.gokwik||0,net:(credit-debit-split)/10000,utr:String(row.settlement_utr||'').trim(),date:settlementDay,provider:String(row.settled_by||''),method:String(row.payment_method||row.payment_mode||''),rrn:String(row.transaction_rrn||''),shopifyTransactionId:id(row.shopify_transaction_id),transactionDate:row.transaction_date||null,remark:String(row.remark||''),source:'gokwik_api_v2',bankVerified:false,requiresDeductionReview:false};
 return {...canonical,apiFingerprint:hash(canonical)};
}
async function fetchDay({date,appId,appSecret,fetchImpl=require('node-fetch'),pause=ms=>new Promise(r=>setTimeout(r,ms)),pageSize=1000}){
 if(!appId||!appSecret)throw Error('GoKwik API credentials are not configured');
 const result=[],pages=new Set(),occurrences=new Map();
 for(let page=1;page<=1000;page++){
  if(page>1)await pause(1000);
  const url=new URL(ENDPOINT);for(const [k,v]of Object.entries({from:date,to:date,page_size:pageSize,page_number:page}))url.searchParams.set(k,String(v));
  let response;
  for(let retry=0;retry<3;retry++){
   try{response=await fetchImpl(url.toString(),{headers:{'gk-app-id':appId,'gk-app-secret':appSecret,Accept:'application/json'},timeout:30000,redirect:'error'});}catch(e){if(retry===2)throw Error('GoKwik API network request failed');await pause(2000*(retry+1));continue;}
   if((response.status===429||response.status>=500)&&retry<2){await pause(15000*(retry+1));continue;}break;
  }
  if(!response.ok)throw Error('GoKwik settlement API HTTP '+response.status+(response.status===401||response.status===403?' — verify credentials and API v2 activation':''));
  let body;try{body=await response.json();}catch(e){throw Error('GoKwik API returned invalid JSON');}
  if(!Array.isArray(body.items)||!Number.isInteger(body.count)||body.count!==body.items.length||body.page_number!=null&&Number(body.page_number)!==page)throw Error('Unexpected GoKwik settlement API pagination response');
  if(body.from&&body.from!==date||body.to&&body.to!==date)throw Error('GoKwik API returned a different requested date');
  const rows=body.items.map(r=>normalize(r,date));
  if(rows.length){const signature=hash(rows.map(r=>r.apiFingerprint).sort());if(pages.has(signature))throw Error('GoKwik API repeated a settlement page');pages.add(signature);}
  for(const row of rows){const occurrence=(occurrences.get(row.apiFingerprint)||0)+1;occurrences.set(row.apiFingerprint,occurrence);result.push({...row,apiRowId:row.apiFingerprint+':'+occurrence});}
  if(body.count<pageSize)return result;
 }
 throw Error('GoKwik API pagination limit exceeded');
}
function mergedEvidence(reportRows,apiRows){
 const key=r=>r.paymentId+'|'+String(r.type).toLowerCase()+'|'+r.utr;
 const keys=new Set(apiRows.filter(r=>r.paymentId).map(key));
 return [...reportRows.filter(r=>!keys.has(key(r))),...apiRows];
}
module.exports={fetchDay,normalize,mergedEvidence,day,shift,units};

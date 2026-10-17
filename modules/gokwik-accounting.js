'use strict';

// Report evidence is separate from posting. No inferred rates, opening balance
// or bank closing balance is ever written by this module.
const crypto = require('crypto');
const schema = require('./gokwik-report-schema');
const CLEARING = 'GoKwik Settlement Clearing';
const BANK = 'Tiana Traders IndusInd 0437';
const SUBSCRIPTION = 'Subscription Expense';
const START = '2026-08-22';
const money = value => Math.round(Number(value || 0) * 100);
const amount = value => value / 100;
const text = value => String(value == null ? '' : value).trim();
const reference = value => text(value).toUpperCase().replace(/[^A-Z0-9]/g, '');
const hash = value => crypto.createHash('sha256').update(JSON.stringify(value)).digest('hex');
const date = schema.date;
function parseRows(rows) {
  if (!rows.length) throw Error('The GoKwik report is empty.');
  const required = ['Transaction Type','Payment Id','Order Id','Amount','Currency','Tax','Fee','Additional Fees','Additional Tax','gokwik Deduction','Debit','Credit','Transaction Date','Merchant Order Id','Platform Order Id','Settlement UTR','Settlement Date','Settled By'];
  const optional=['Transaction RRN','Shopify Transaction Id','Payment Method','Payment Mode'];
  rows=rows.map(row=>{const normalized=schema.headers(row);return Object.fromEntries(required.concat(optional).map(key=>[key,normalized[key.toLowerCase()]]));});
  if (required.some(key => rows[0][key]===undefined)) throw Error('Upload the original GoKwik settlement CSV, not a bank or Paytm report. Missing column: '+required.find(key=>rows[0][key]===undefined));
  const seen = new Map();
  for (const [index,row] of rows.entries()) {
    try {
    if (text(row['Transaction Type']).toLowerCase() !== 'payment') throw Error('Unsupported GoKwik transaction type. Refunds/reversals require separate review; nothing was imported.');
    if (text(row.Currency).toUpperCase() !== 'INR') throw Error('Only INR settlements are supported.');
    const values = schema.amounts(row),net=values.net;
    const tx = {paymentId:text(row['Payment Id']),orderId:text(row['Merchant Order Id']),orderNumber:text(row['Platform Order Id']).replace(/^#/, ''),gokwikOrderId:text(row['Order Id']),date:date(row['Transaction Date']),settledDate:date(row['Settlement Date']),utr:text(row['Settlement UTR']),provider:text(row['Settled By']),currency:'INR',gross:amount(values.Amount),fee:amount(values.Fee),tax:amount(values.Tax),additionalFee:amount(values['Additional Fees']),additionalTax:amount(values['Additional Tax']),deduction:amount(values['gokwik Deduction']),credit:amount(values.Credit),net:amount(net),rrn:text(row['Transaction RRN']),shopifyTransactionId:text(row['Shopify Transaction Id']),method:text(row['Payment Method']),paymentMode:text(row['Payment Mode'])};
    if (![tx.paymentId,tx.orderId,tx.orderNumber,tx.gokwikOrderId,tx.utr,tx.provider].every(Boolean)) throw Error('Missing GoKwik payment, order or settlement identifiers.');
    if (tx.date < START || tx.settledDate < tx.date) throw Error('Payment is before 22 August 2026 or settles before collection.');
    if (seen.has(tx.paymentId) && hash(seen.get(tx.paymentId)) !== hash(tx)) throw Error('Conflicting duplicate payment ID: ' + tx.paymentId);
    seen.set(tx.paymentId, tx);
    } catch(error) {throw Error('GoKwik row '+(index+2)+(text(row['Payment Id'])?' ('+text(row['Payment Id'])+')':'')+': '+error.message);}
  }
  batches([...seen.values()]); // reject a UTR reused with conflicting date/provider
  return [...seen.values()];
}
function parseReport(buffer) {
  if(!Buffer.isBuffer(buffer)||!buffer.length)throw Error('The GoKwik CSV is empty.');
  if(buffer[0]===0x50&&buffer[1]===0x4b||buffer.includes(0)&&!(buffer[0]===0xff&&buffer[1]===0xfe))throw Error('Upload CSV text, not an Excel workbook renamed as CSV.');
  if(buffer[0]===0xff&&buffer[1]===0xfe)buffer=Buffer.from(buffer.subarray(2).toString('utf16le'),'utf8');
  const XLSX = require('xlsx'), book = XLSX.read(buffer, {type:'buffer', raw:true});
  if (book.SheetNames.length !== 1) throw Error('Upload a single GoKwik settlement report.');
  const matrix=XLSX.utils.sheet_to_json(book.Sheets[book.SheetNames[0]],{header:1,defval:'',raw:true,blankrows:false});
  const columns=matrix.shift()||[],keys=columns.map(key=>text(key).replace(/\s+/g,' ').toLowerCase());
  if(new Set(keys).size!==keys.length||keys.some(key=>!key))throw Error('GoKwik CSV headers must be nonempty and unique.');
  return parseRows(matrix.filter(row=>row.some(value=>text(value)!=='')).map(row=>{
    if(row.length>columns.length)throw Error('GoKwik CSV row has more fields than its header. Check delimiters and quoted values.');
    return Object.fromEntries(columns.map((key,index)=>[key,row[index]??'']));
  }));
}
function batches(transactions) {
  const groups = new Map();
  for (const tx of transactions) {
    const old = groups.get(tx.utr);
    if (old && (old.settledDate !== tx.settledDate || old.provider !== tx.provider)) throw Error('Settlement UTR has conflicting dates or providers: ' + tx.utr);
    const batch = old || {id:'GKW-' + hash(tx.utr).slice(0,20),utr:tx.utr,settledDate:tx.settledDate,provider:tx.provider,transactions:[],gross:0,fee:0,tax:0,additionalFee:0,additionalTax:0,deduction:0,net:0};
    batch.transactions.push(tx);
    for (const key of ['gross','fee','tax','additionalFee','additionalTax','deduction','net']) batch[key] = amount(money(batch[key]) + money(tx[key]));
    groups.set(tx.utr, batch);
  }
  return [...groups.values()].sort((a,b) => a.settledDate.localeCompare(b.settledDate) || a.utr.localeCompare(b.utr));
}
function routeLocked(store, orderId) {
  const base = 'SHOPIFY/' + orderId, isSource = id => text(id) === base || text(id).startsWith(base + '/');
  if (Object.keys(store.bankDateOverrides || {}).some(isSource)) return true;
  const resolutions = Object.values(store.bankReconciliationDrafts || {}).flatMap(d => Object.values(d.resolutions || {}));
  if (resolutions.some(r => [r.appId,...(r.appIds || [])].some(isSource))) return true;
  return Object.values(store.bankStatements || {}).some(book => (book.imports || []).some(record => [...(record.reconciliationRows || []),...(record.carriedReconciliationRows || [])].some(row => [...(row.linkedRecordIds || []),row.ledger && row.ledger.id].some(isSource))));
}
function linkedOrderId(store,tx) {
  if(!schema.scientificId(tx.orderId))return tx.orderId;
  const link=(store.gokwikOrderLinks||{})[tx.paymentId],review=link&&link.identityReview;
  return review&&review.sourceOrderId===tx.orderId&&review.orderId===link.orderId&&review.reason&&review.by&&schema.roundedIdAgrees(tx.orderId,link.orderId)?link.orderId:'';
}
function identityCandidates(tx,orders) {
  if(!schema.scientificId(tx.orderId))return [];
  return orders.filter(o=>text(o.orderNumber||o.number||o.name).replace(/^#/,'')===tx.orderNumber&&schema.roundedIdAgrees(tx.orderId,o.id)).map(o=>({orderId:text(o.id),orderNumber:tx.orderNumber,total:Number(o.total)}));
}
function validateLink(store, tx, orders, options={}) {
  const rounded=schema.scientificId(tx.orderId),requested=text(options.confirmedOrderId),orderId=requested||linkedOrderId(store,tx);
  if(requested&&(!rounded||!schema.roundedIdAgrees(tx.orderId,requested)))throw Error('Confirmed Shopify ID does not agree with the original merchant order ID. Upload the untouched CSV or review the correct order.');
  if(rounded&&!orderId)throw Error('Merchant Order Id is in rounded scientific notation ('+tx.orderId+'). Upload the untouched CSV, or explicitly review and confirm the full Shopify ID for #'+tx.orderNumber+'. No ID was guessed.');
  const candidates = orders.filter(order => text(order.id) === orderId);
  if (candidates.length !== 1) throw Error('Exact Shopify order ID was not found uniquely. Sync the order first.');
  const order = candidates[0], number = text(order.orderNumber || order.number || order.name).replace(/^#/, '');
  if (number !== tx.orderNumber) throw Error('Shopify order ID and order number disagree.');
  if(rounded&&identityCandidates(tx,orders).length!==1)throw Error('The rounded merchant ID and order number do not identify one unique Shopify order. Resolve the conflicting source first.');
  if (routeLocked(store, orderId)) throw Error('This order already has bank-linked receipt evidence. Review/unmatch the original bank link before moving its receipt to GoKwik.');
  const successful = (order.paymentTransactions || []).filter(p => ['sale','capture'].includes(text(p.kind).toLowerCase()) && (!p.status || text(p.status).toLowerCase() === 'success'));
  const tender = pattern => successful.filter(p=>pattern.test(text(p.gateway || p.payment_gateway))).reduce((sum,p)=>sum+money(p.amount),0);
  const linkedPaytm = Object.values(store.paytmOrderLinks || {}).filter(p => text(p.orderId) === orderId).reduce((sum,p) => sum + money(p.amount),0);
  const allocation=(store.saleAllocationOverrides || {})['SHOPIFY/'+orderId];
  const reserved = Math.max(tender(/^cash$/i),money(allocation ? allocation.cashAmount : order.cashAmount ?? order.cashPaidAmount)) +
    Math.max(tender(/store.?credit|gift.?card/i),money(order.storeCreditAmount ?? order.storeCreditUsed)) +
    Math.max(tender(/paytm/i),linkedPaytm);
  const collected = Object.values(store.gokwikTransactions || {}).filter(p => linkedOrderId(store,p)===orderId||schema.scientificId(p.orderId)&&p.orderNumber===number&&schema.roundedIdAgrees(p.orderId,orderId)).reduce((sum,p) => sum + money(p.gross),0);
  if (collected + reserved > money(order.total)) throw Error('GoKwik collections plus cash, Paytm or store credit exceed the Shopify order total. Review the payment split.');
  return {orderId,orderNumber:tx.orderNumber,gross:tx.gross,orderTotal:Number(order.total),reservedOtherTender:amount(reserved),refundAmount:Number(order.moneyRefunded || order.refundAmount || 0),cancelled:!!order.cancelledAt};
}
function projectSales(rows, store) {
  // Existing finalized/manual bank links never silently change accounts.
  const result = rows.map(row => {
    if (row.account === CLEARING && routeLocked(store,row.orderId)) return {...row,account:row.gokwikOriginalAccount || BANK,gokwikRoutingBlocked:true};
    if (row.account === BANK && row.orderId && /website|online|gokwik|easebuzz/i.test(row.description || '') && !routeLocked(store,row.orderId)) return {...row,account:CLEARING};
    return {...row};
  }).filter(row=>row.account!==CLEARING||text(row.date)>=START);
  const grouped = new Map();
  for (const tx of Object.values(store.gokwikTransactions || {})) {
    const orderId=linkedOrderId(store,tx);
    if (!(store.gokwikOrderLinks || {})[tx.paymentId] || !orderId || routeLocked(store,orderId)) continue;
    const group = grouped.get(orderId) || [];
    group.push(tx); grouped.set(orderId, group);
  }
  for (const [orderId, transactions] of grouped) {
    const existing = result.filter(r => text(r.orderId) === orderId && r.account === CLEARING);
    const collected = transactions.reduce((n,t) => n + money(t.gross),0), previous = existing.reduce((n,r) => n + money(r.amount),0);
    for (let i=result.length-1;i>=0;i--) if (text(result[i].orderId) === orderId && result[i].account === CLEARING) result.splice(i,1);
    if (previous > collected) result.push({...existing[0],id:'GKW-UNREPORTED/'+orderId,amount:amount(previous-collected),description:'GoKwik website receipt · awaiting remaining settlement report',reportPending:true});
    for (const tx of transactions) result.push({id:'GKW-PAYMENT/'+tx.paymentId,orderId,orderNumber:tx.orderNumber,date:tx.date,account:CLEARING,amount:tx.gross,originalAmount:tx.gross,reference:tx.paymentId,gateway:'GoKwik',allocationPart:'gokwik',description:'GoKwik customer payment · #'+tx.orderNumber});
    // A partial-COD advance is not collected a second time by the courier.
    const all = result.filter(r => text(r.orderId) === orderId), gross = money(existing[0] && existing[0].gross || (store.gokwikOrderLinks || {})[transactions[0].paymentId].orderTotal);
    let excess = Math.max(0,all.reduce((n,r) => n+money(r.amount),0)-gross);
    for (const row of all.filter(r => r.account === 'Velocity' && !r.manualCashCollected)) {const reduction=Math.min(excess,money(row.amount));row.amount=amount(money(row.amount)-reduction);excess-=reduction;}
  }
  return result.filter(r => money(r.amount)>0);
}
function bankSignature(row) {return [row.date,money(row.credit),money(row.debit),reference(row.reference),reference(row.description),row.balance==null?'':money(row.balance)].join('|');}
function bankCandidates(store,batch) {
  const found = new Map(), add = (row, locator, resolution) => {
    if (money(row.credit)!==money(batch.net) || money(row.debit)!==0 || row.date!==batch.settledDate || !reference([row.reference,row.description].join(' ')).includes(reference(batch.utr))) return;
    const signature=bankSignature(row), old=found.get(signature), value={...row,signature,locator,resolution:resolution || null};
    // Overlapping uploads can show the same physical bank row. They cannot
    // erase an existing review decision or collapse two rows in one statement.
    if(old&&old.locator.kind===locator.kind&&old.locator.draftId===locator.draftId&&text(old.locator.id || old.locator.rowId)!==text(locator.id || locator.rowId)) {
      found.set(signature+'|duplicate|'+text(locator.id || locator.rowId),value);return;
    }
    if(old&&old.resolution&&!value.resolution)value.resolution=old.resolution;
    if (!old || locator.kind==='official' || value.resolution) found.set(signature,value);
  };
  Object.values(((store.bankStatements || {})[BANK] || {}).transactions || {}).forEach(row => add(row,{kind:'official',id:row.id}));
  for (const draft of Object.values(store.bankReconciliationDrafts || {})) {
    if (draft.account!==BANK || text(draft.nature || 'SANKI')!=='SANKI' || draft.finalizedAt || draft.expiresAt && Date.parse(draft.expiresAt)<Date.now()) continue;
    (draft.transactions || []).forEach((row,index) => add(row,{kind:'draft',draftId:draft.id,rowId:'bank-'+index},(draft.resolutions || {})['bank-'+index]));
  }
  return [...found.values()];
}
function getBatch(store,id) {const batch=batches(Object.values(store.gokwikTransactions || {})).find(b=>b.id===id);if(!batch)throw Error('GoKwik settlement not found.');return batch;}
function protectsMovement(store,id) {
  return Object.values(store.gokwikPostings || {}).some(p=>p.transferId===id||(p.expenseIds || []).some(expenseId=>id===expenseId||text(id).startsWith(expenseId+'/')));
}
function review(store,id,orders) {
  const batch=getBatch(store,id);
  if(!(batch.net>0))throw Error('Zero-net settlements need a separate non-bank review; no zero-value bank transfer will be created.');
  if ((store.gokwikPostings || {})[id]) throw Error('This GoKwik settlement is already posted.');
  const links=batch.transactions.map(tx=>{if (!(store.gokwikOrderLinks || {})[tx.paymentId]) throw Error('Link every GoKwik payment to its Shopify order first.');return validateLink(store,tx,orders);});
  const candidates=bankCandidates(store,batch);
  if (candidates.length!==1) throw Error(candidates.length?'Bank match is ambiguous. Review the 0437 statement.':'Upload the 0437 bank statement: an exact date, UTR and net credit is required.');
  const bank=candidates[0];
  if (bank.resolution) throw Error('This bank row already has a reconciliation decision. Unmatch/review it before posting GoKwik.');
  const imports=(((store.bankStatements || {})[BANK] || {}).imports || []);
  if(imports.some(record=>[...(record.reconciliationRows || []),...(record.carriedReconciliationRows || [])].some(row=>row.bank && bankSignature(row.bank)===bank.signature && (row.linkedRecordIds || []).length))) throw Error('The bank credit already has finalized ledger evidence; do not post it twice.');
  if((store.bankTruthMovements || []).some(row=>row.account===BANK&&(text(row.bankTransactionId)===text(bank.id)&&bank.id||bankSignature(row)===bank.signature)))throw Error('This bank credit is already represented by a bank-truth/exclusion entry. Review that classification before posting GoKwik.');
  if((store.receipts || []).some(row=>!row.accountingExcluded&&row.account===BANK&&reference(row.bankReference || row.reference).includes(reference(batch.utr))))throw Error('A receipt already uses this settlement UTR. Review it before creating a duplicate bank credit.');
  if((store.transfers || []).some(t=>!t.accountingExcluded && t.toAccount===BANK && reference(t.bankReference || t.reference || t.note).includes(reference(batch.utr)))) throw Error('A transfer already uses this settlement UTR. Review the existing transfer instead of creating another.');
  if(Object.values(store.gokwikPostings || {}).some(p=>p.bankSignature===bank.signature))throw Error('This bank credit is already used by another GoKwik settlement.');
  const source=batch.transactions.map(tx=>{const link=(store.gokwikOrderLinks || {})[tx.paymentId];return {tx,link,sourceHash:(store.gokwikImports || {})[tx.importId]?.hash || ''};});
  const fingerprint=hash({batch,links,bank:{signature:bank.signature,locator:bank.locator},source});
  return {batch,links,bank,fingerprint};
}
function view(store,orders) {
  const transactions=Object.values(store.gokwikTransactions || {}).map(tx=>{let issue='';try{validateLink(store,tx,orders);}catch(e){issue=e.message;}return {...tx,link:(store.gokwikOrderLinks || {})[tx.paymentId] || null,issue,requiresIdentityReview:schema.scientificId(tx.orderId),orderCandidates:identityCandidates(tx,orders)};});
  const settlements=batches(transactions).map(batch=>{
    const posting=(store.gokwikPostings || {})[batch.id],approval=(store.gokwikApprovals || {})[batch.id];
    let issue='',ready=false,approvalStale=false;
    if(!posting)try{const checked=review(store,batch.id,orders);approvalStale=!!approval&&approval.fingerprint!==checked.fingerprint;ready=!approvalStale;if(approvalStale)issue='Evidence changed after submission. Withdraw this review and submit the current figures again.';}catch(e){issue=e.message;approvalStale=!!approval;}
    return {...batch,posting:posting || null,approval:approval || null,approvalStale,ready,issue,bankCandidates:bankCandidates(store,batch).map(b=>({date:b.date,credit:b.credit,reference:b.reference,description:b.description,locator:b.locator})),status:posting?'posted':approval&&!approvalStale?'awaiting_owner':ready?'ready':'review_required'};
  });
  return {account:CLEARING,bankAccount:BANK,subscriptionCategory:SUBSCRIPTION,subscriptionVendor:'GoKwik',transactions,settlements,imports:Object.values(store.gokwikImports || {}).map(({id,name,hash,at,by,count})=>({id,name,hash,at,by,count})),previews:Object.values(store.gokwikPreviews || {}).map(({id,name,at,transactions})=>({id,name,at,count:transactions.length})),counts:{payments:transactions.length,settlements:settlements.length,posted:settlements.filter(s=>s.status==='posted').length,awaitingOwner:settlements.filter(s=>s.status==='awaiting_owner').length,reviewRequired:settlements.filter(s=>s.status==='review_required').length}};
}
module.exports={CLEARING,BANK,SUBSCRIPTION,START,money,hash,date,parseRows,parseReport,batches,validateLink,routeLocked,projectSales,bankCandidates,bankSignature,getBatch,protectsMovement,review,view};

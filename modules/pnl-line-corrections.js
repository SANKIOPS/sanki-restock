'use strict';
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const key = (orderId, lineId) => String(orderId) + '/' + String(lineId);
const hash = line => crypto.createHash('sha256').update(JSON.stringify([String(line.id),line.sku||'',line.title||'',line.qty,line.price,line.discount,line.taxable,line.taxLines])).digest('hex');
const orderHash = order => crypto.createHash('sha256').update(JSON.stringify([String(order.id),order.name||'',order.createdAt||''])).digest('hex');
const manualHash = receipts => crypto.createHash('sha256').update(JSON.stringify(receipts.map(r=>[r.id,r.manualSaleId||'',r.date,r.amount,r.source||'',r.receiptType,r.nature,r.accountingExcluded||false]).sort((a,b)=>String(a[0]).localeCompare(String(b[0]))))).digest('hex');
const validDay = d => /^\d{4}-\d{2}-\d{2}$/.test(String(d||''))&&!Number.isNaN(Date.parse(d+'T00:00:00Z'))&&new Date(d+'T00:00:00Z').toISOString().slice(0,10)===d;
const canonicalSku = sku => String(sku || '').trim().toUpperCase();
const dateOf = value => /^\d{4}-\d{2}-\d{2}$/.test(String(value||'')) ? value : Number.isFinite(Date.parse(value)) ? new Date(Date.parse(value)+19800000).toISOString().slice(0,10) : '';
const fail = message => { throw Object.assign(Error(message), {status:409}); };
const list = value => Array.isArray(value)?value:Object.values(value||{});
const money = value => Math.round((Number(value)+Number.EPSILON)*100)/100;
const activeExpenseDraft = (record,books) => !!(record&&(record.bankReconciliationDraft&&books.bankReconciliationDrafts?.[record.bankReconciliationDraft]||record.reconciliationDraft&&books.bankReconciliationDrafts?.[record.reconciliationDraft]));
const expenseConfirmationKey = id => key('expense:'+id,'$expense');
// Separate from the legacy order/line hashes: existing private annotations
// retain their original fingerprints. The expense snapshot is evidence, not
// permission to restore its excluded payment or to post money to an account.
const expenseHash = expense => crypto.createHash('sha256').update(JSON.stringify([expense.id,expense.date,expense.nature,expense.amount,expense.paidAmount,expense.status,expense.accountingExcluded||false,expense.ownerOnly||false,expense.nonExpenseReimbursement||false,expense.salaryAdvanceId||'',expense.inventoryPurchase||false,expense.procurementPoId||'',expense.purchaseOrderId||'',expense.reconciliationDraft||'',expense.bankReconciliationDraft||'',(expense.payments||[]).map(p=>[p.id,p.amount,p.date||p.paidAt,p.account||'',p.voided||false,p.accountingExcluded||false,p.bankReconciliationDraft||'']).sort((a,b)=>String(a[0]).localeCompare(String(b[0]))),(expense.vendorAdvanceApplications||[]).map(p=>[p.id,p.amount,p.date,p.vendorAdvanceId,p.voided||false,p.accountingExcluded||false])])).digest('hex');
function expenseGuard(expense,books) {
 if(!expense||!['SANKI','A3'].includes(String(expense.nature||'').toUpperCase())||expense.ownerOnly||expense.accountingExcluded||expense.nonExpenseReimbursement||expense.salaryAdvanceId||expense.inventoryPurchase||expense.procurementPoId||expense.purchaseOrderId||!['approved','partially_paid','paid'].includes(expense.status)||!Number.isFinite(Number(expense.amount))||!Number.isFinite(Number(expense.paidAmount))||!(Number(expense.amount)>0)||!(Number(expense.paidAmount)>0)||!validDay(expense.date)||expense.date<'2026-08-22')fail('Choose an existing approved, paid business operating expense. Private, excluded, unpaid, inventory and balance-sheet entries cannot be confirmed here.');
 if(activeExpenseDraft(expense,books)||(expense.payments||[]).some(p=>activeExpenseDraft(p,books))||(expense.vendorAdvanceApplications||[]).some(p=>activeExpenseDraft(list(books.vendorAdvances).find(a=>a.id===p.vendorAdvanceId),books)))fail('This expense still has provisional reconciliation evidence. Complete its existing approval workflow; a Results confirmation cannot bypass it.');
}
function eligibleExpensePaidAmount(expense,books={}) {
 const regular=(expense.payments||[]).filter(p=>!p.voided&&!p.accountingExcluded&&!activeExpenseDraft(p,books)).map((p,i)=>({...p,id:p.id||'PAY-LEGACY/'+i}));
 const advances=(expense.vendorAdvanceApplications||[]).map((p,i)=>({...p,id:'ADV-APPLICATION/'+(p.id||(p.vendorAdvanceId||'LEGACY')+'/'+(p.batchPaymentId||p.appliedAt||i))})).filter(p=>!p.voided&&!p.accountingExcluded&&!activeExpenseDraft(list(books.vendorAdvances).find(a=>a.id===p.vendorAdvanceId)||{},books));
 const payments=[...regular,...advances].sort((a,b)=>dateOf(a.date||a.paidAt).localeCompare(dateOf(b.date||b.paidAt))||String(a.id).localeCompare(String(b.id))),seen=new Set();
 let remaining=Math.max(0,Number(expense.amount)||0),recognized=0;const today=dateOf(new Date().toISOString());
 for(const p of payments){if(seen.has(p.id)||!Number.isFinite(Number(p.amount))||!(Number(p.amount)>0))continue;seen.add(p.id);const included=Math.min(remaining,Number(p.amount));remaining-=included;const date=dateOf(p.date||p.paidAt);if(validDay(date)&&date>='2026-08-22'&&date<=today)recognized+=included;}
 return money(recognized);
}
function resolveExpenseConfirmation(expense,sources={}) {
 const entry=sources.lineCorrections?.entries?.[expenseConfirmationKey(expense.id)];
 if(!entry)return {entry:null,amount:0,issues:[]};
 const issues=[],bad=message=>({entry,amount:0,issues:[message]});
 try{expenseGuard(expense,sources.expenses||{});}catch(error){return bad(error.message);}
 if(entry.type!=='expense_confirmation'||entry.expenseId!==expense.id||!Number.isFinite(entry.amount)||!(entry.amount>0)||money(entry.amount)!==entry.amount)return bad('Results expense confirmation is invalid; the original financial records were not changed.');
 const eligible=eligibleExpensePaidAmount(expense,sources.expenses||{});
 // Source payments have priority across ALL dates, not merely this selected
 // report period. A later restored payment supersedes the annotation, rather
 // than appearing once on each date or in two different monthly reports.
 if(eligible>=entry.amount)return {entry,amount:0,issues,superseded:true};
 if(entry.fingerprint!==expenseHash(expense))return bad('Results expense confirmation no longer matches its source expense/payment evidence. Preview it again; financial records remain unchanged.');
 if(entry.amount>Math.min(Number(expense.amount),Number(expense.paidAmount))||!validDay(entry.paymentDate)||entry.paymentDate<expense.date||entry.paymentDate<'2026-08-22'||entry.paymentDate>dateOf(new Date().toISOString()))return bad('Results expense amount or actual payment date conflicts with the existing paid expense. Review the confirmation.');
 return {entry,amount:money(Math.max(0,entry.amount-eligible)),date:entry.paymentDate,eligiblePaidAmount:eligible,issues};
}
const costMatches = (sources, sku) => (sources.shopifyCosts?.items||[]).filter(i=>i&&canonicalSku(i.sku)===canonicalSku(sku));
const costCurrency = item => item.unitCostCurrency||item.currency||'INR';
const duplicateSku = items => new Set(items.map(i=>String(i.id||'')+'\0'+i.unitCost+'\0'+costCurrency(i))).size>1;
function verifiedCostItem(items, inventoryItemId) {
 const selected=inventoryItemId===undefined?items:items.filter(i=>String(i.id)===String(inventoryItemId));
 const distinct=new Map(selected.map(i=>[String(i.id),i]));
 if(distinct.size!==1||!selected[0]?.id||!Number.isFinite(Number(selected[0].unitCost))||!(Number(selected[0].unitCost)>0)||costCurrency(selected[0])!=='INR'||selected.some(i=>Number(i.unitCost)!==Number(selected[0].unitCost)||costCurrency(i)!=='INR'))fail('Select exactly one Shopify item with a positive INR cost. Refresh Shopify costs or resolve the item identity first.');
 return selected[0];
}
function reviewRows(sources, from, to) {
 const rows=[];
 for(const order of Object.values(sources.orders?.orders || {})) {
  const date=dateOf(order.processedAt || order.createdAt);
  if(date < from || date > to || order.accountingExcluded) continue;
  for(const line of order.lineItems || []) {
   const sourceSku=canonicalSku(line.sku),matches=sourceSku?costMatches(sources,sourceSku):[];
   if(!line.id || sourceSku&&!duplicateSku(matches)) continue;
   const candidates=[...new Map(matches.map(i=>[String(i.id),i])).values()].map(i=>({inventoryItemId:String(i.id||''),sku:canonicalSku(i.sku),variant:i.variant||'',productTitle:i.product?.title||'',unitCost:i.unitCost,currency:costCurrency(i)}));
   rows.push({orderId:String(order.id),lineId:String(line.id),reference:order.name||String(order.id),date,title:line.title||'Custom item',sourceSku,candidates,qty:line.qty,price:line.price,discount:line.discount||0,fingerprint:hash(line),current:sources.lineCorrections?.entries?.[key(order.id,line.id)] || null});
  }
 }
 return rows.sort((a,b)=>b.date.localeCompare(a.date)||a.reference.localeCompare(b.reference)||a.lineId.localeCompare(b.lineId));
}
function prepare(sources, items, requireFingerprint=false) {
 if(!Array.isArray(items)||!items.length||items.length>100) fail('Choose between 1 and 100 item corrections.');
 const seen=new Set();
 return items.map(input=>{
  if(input.type==='expense_confirmation'){
   const reference=String(input.reference||input.expenseId||'').trim(),books=sources.expenses||{},matches=list(books.expenses).filter(e=>String(e.id||'').toUpperCase()===reference.toUpperCase());
   if(!reference||matches.length!==1)fail('Enter the exact existing expense reference.');
   const expense=matches[0],id=expenseConfirmationKey(expense.id);expenseGuard(expense,books);
   if(seen.has(id))fail('The same expense confirmation was selected twice.');seen.add(id);
   const amount=Number(input.amount),paymentDate=String(input.paymentDate||''),adjustmentReference=String(input.adjustmentReference||'').trim(),fingerprint=expenseHash(expense);
   if(!Number.isFinite(amount)||amount<=0||amount>10000000||money(amount)!==amount||amount>Math.min(Number(expense.amount),Number(expense.paidAmount)))fail('Confirm a positive amount with at most two decimals, no greater than the existing bill and source paid total.');
   if(!validDay(paymentDate)||paymentDate<expense.date||paymentDate<'2026-08-22'||paymentDate>dateOf(new Date().toISOString()))fail('Confirm the actual reimbursement/payment date, not before the expense, accounting start, or after today.');
   if(adjustmentReference.length>200)fail('The optional user-reported adjustment reference must be at most 200 characters.');
   if(requireFingerprint&&input.fingerprint!==fingerprint)fail('Expense or source payment evidence changed since preview. Preview the confirmation again.');
   const eligiblePaidAmount=eligibleExpensePaidAmount(expense,books),reportOnlyAmount=money(Math.max(0,amount-eligiblePaidAmount));
   if(!reportOnlyAmount)fail('Existing eligible source payments already cover this amount; no additional Results expense is needed.');
   return {orderId:'expense:'+expense.id,lineId:'$expense',type:'expense_confirmation',expenseId:expense.id,reference:expense.id,title:expense.particulars||expense.vendor||'Existing paid expense',amount,paymentDate,adjustmentReference,adjustmentReferenceBasis:'User-reported only; not verified bank or adjustment evidence',fingerprint,expectedExpenseAmount:Number(expense.amount),sourcePaidAmount:Number(expense.paidAmount),eligiblePaidAmount,reportOnlyAmount};
  }
  if(input.type==='manual_cost') {
   const reference=String(input.reference||input.manualId||'').trim();
   const receipts=Object.values(sources.expenses?.receipts||[]).filter(r=>r.receiptType==='product_sale'&&!r.orderId&&!r.shopifyOrderId&&!r.accountingExcluded&&['SANKI','A3'].includes(r.nature)&&String(r.manualSaleId||r.id)===reference);
   if(!reference||!receipts.length||receipts.some(r=>!validDay(r.date)||r.date<'2026-08-22'||!(Number(r.amount)>0)||r.reconciliationDraft&&sources.expenses?.bankReconciliationDrafts?.[r.reconciliationDraft]))fail('Choose an existing posted manual-sale receipt within the accounting period.');
   if((sources.manualSales?.sales||[]).some(s=>s.id===reference))fail('This manual sale has its own source allocation; review that source instead.');
   const id=key('manual:'+reference,'$manual'),fingerprint=manualHash(receipts),qty=Number(input.qty),unitCost=Number(input.unitCost);
   if(seen.has(id)||!Number.isInteger(qty)||qty<=0||qty>10000||!Number.isFinite(unitCost)||unitCost<=0||unitCost>10000000||Math.abs(unitCost*100-Math.round(unitCost*100))>.00001)fail('Enter a positive whole quantity and a positive per-unit cost with at most two decimals.');
   if(requireFingerprint&&input.fingerprint!==fingerprint)fail('Manual-sale receipts changed since preview. Preview them again.');
   const entry={orderId:'manual:'+reference,lineId:'$manual',type:'manual_cost',manualId:reference,reference,fingerprint,qty,unitCost,title:receipts[0].source||'Manual product sale',expectedAmount:receipts.reduce((s,r)=>s+Number(r.amount),0)};
   if(input.taxRate!==undefined&&input.taxRate!==null&&input.taxRate!=='') {
    if(!Number.isFinite(Number(input.taxRate))||Number(input.taxRate)<0||Number(input.taxRate)>1||typeof input.taxesIncluded!=='boolean')fail('Confirm the GST fraction and inclusive/exclusive basis together, or leave tax unresolved.');
    entry.taxRate=Number(input.taxRate);entry.taxesIncluded=input.taxesIncluded;
   }
   seen.add(id);return entry;
  }
  if(['cancelled','source_status','delivered'].includes(input.type)) {
   const matches=Object.values(sources.orders?.orders||{}).filter(o=>input.orderId?String(o.id)===String(input.orderId):String(o.name||'').toLowerCase()===String(input.reference||'').trim().toLowerCase());
   if(matches.length!==1)fail('Enter the exact existing order reference, including #, or reload the preview.');
   const order=matches[0],id=key(order.id,'$order');
   if(seen.has(id)||!dateOf(order.processedAt||order.createdAt)||dateOf(order.processedAt||order.createdAt)<'2026-08-22'||order.accountingExcluded)fail('Duplicate, excluded, undated or pre-accounting-start orders cannot be corrected here.');
   if(requireFingerprint&&input.fingerprint!==orderHash(order))fail('Order identity changed since preview. Preview it again before saving.');
   seen.add(id);
   const entry={orderId:String(order.id),lineId:'$order',type:input.type,reference:order.name||String(order.id),fingerprint:orderHash(order),createdDate:dateOf(order.createdAt),title:input.type==='cancelled'?'Confirmed cancelled; exclude sale from Results':'Use original source completion status'};
   if(input.type==='delivered'){
    if(!validDay(input.deliveryDate)||input.deliveryDate<dateOf(order.createdAt||order.processedAt)||input.deliveryDate<dateOf(order.processedAt||order.createdAt)||input.deliveryDate>dateOf(new Date().toISOString())||order.cancelledAt)fail('Delivery date must be real, not before the order, not in the future, and not on a cancelled source order.');
    entry.deliveryDate=input.deliveryDate;entry.title='Confirmed delivered '+input.deliveryDate+'; recognize Results on actual delivery';
   }
   return entry;
  }
  const order=sources.orders?.orders?.[String(input.orderId)];
  const line=order?.lineItems?.find(l=>String(l.id)===String(input.lineId));
  const id=key(input.orderId,input.lineId);
  if(!order || !line || seen.has(id) || canonicalSku(line.sku)&&input.type!=='sku_identity') fail('An item is missing, already has a source SKU, or was selected twice. Reload the review.');
  seen.add(id);
  if(!dateOf(order.processedAt || order.createdAt)||dateOf(order.processedAt || order.createdAt)<'2026-08-22') fail('Pre-accounting-start or undated orders cannot be corrected here.');
  if(!Number.isFinite(Number(input.expectedPrice)) || Number(input.expectedPrice)!==Number(line.price) || Number(input.expectedQty)!==Number(line.qty) || !(Number(line.qty)>0)) fail('The original item price or quantity changed. Reload and review the exact line.');
  if(requireFingerprint && input.fingerprint!==hash(line)) fail('Source item changed since preview. Preview it again before saving.');
  const entry={orderId:String(order.id),lineId:String(line.id),type:input.type,expectedPrice:Number(line.price),expectedQty:Number(line.qty),fingerprint:hash(line)};
  if(input.type==='sku') {
   entry.sku=canonicalSku(input.sku);
   if(!entry.sku)fail('Enter the exact Shopify SKU.');
   verifiedCostItem(costMatches(sources,entry.sku));
  } else if(input.type==='sku_identity') {
   entry.sku=canonicalSku(line.sku);
   const matches=costMatches(sources,entry.sku);
   if(!entry.sku||!duplicateSku(matches)||!String(input.inventoryItemId||'').trim())fail('Choose an existing duplicate-SKU source line and its exact Shopify item.');
   const item=verifiedCostItem(matches,input.inventoryItemId);
   entry.inventoryItemId=String(item.id);
  } else if(input.type==='bundle') {
   if(!Array.isArray(input.components)||!input.components.length||input.components.length>100)fail('Enter the exact SKU and quantity for each bundle component.');
   const componentSkus=new Set();
   entry.components=input.components.map(component=>{
    const sku=canonicalSku(component?.sku),qty=Number(component?.qty);
    if(!sku||componentSkus.has(sku)||!Number.isInteger(qty)||qty<=0||qty>10000)fail('Bundle SKUs must be unique with positive whole quantities. Combine repeated units under the same SKU.');
    const item=verifiedCostItem(costMatches(sources,sku));componentSkus.add(sku);
    return {sku,qty,inventoryItemId:String(item.id)};
   });
   if(entry.components.reduce((n,c)=>n+c.qty,0)!==Number(line.qty))fail('Bundle component quantities must exactly match the original source line quantity.');
  } else if(input.type==='cost') {
   const cost=Number(input.unitCost);
   if(!Number.isFinite(cost)||cost<=0||cost>10000000||Math.abs(cost*100-Math.round(cost*100))>.00001) fail('Enter a positive per-unit custom-item cost with at most two decimals.');
   entry.unitCost=cost;
  } else if(!['shipping','shipping_no_tax','rounding'].includes(input.type)) fail('Choose SKU mapping, exact item identity, bundle, custom-item cost, shipping fee, or rounding.');
  return {...entry,reference:order.name||String(order.id),title:line.title||'Custom item'};
 });
}
function read(file) {
 try { const value=JSON.parse(fs.readFileSync(file,'utf8'));if(value.schemaVersion!==1||!value.entries||Array.isArray(value.entries)||typeof value.entries!=='object'||!Array.isArray(value.history)) fail('The Results item-correction store requires review; it was not replaced.');return value; }
 catch(error){if(error.code==='ENOENT') return {schemaVersion:1,entries:{},history:[]};throw error;}
}
function save(file, sources, items, actor, note, at=new Date().toISOString()) {
 if(!actor || !String(note||'').trim() || String(note).length>1000) fail('A reason is required for the audit trail.');
 const entries=prepare(sources,items,true),store=read(file),next={...store,entries:{...store.entries},history:[...store.history]};
 for(const entry of entries) { const id=key(entry.orderId,entry.lineId),after={...entry,updatedBy:actor,updatedAt:at,note:String(note).trim()};next.history.push({id,before:next.entries[id]||null,after,actor,at,note:String(note).trim()});next.entries[id]=after; }
 const temp=file+'.'+crypto.randomUUID()+'.tmp';
 try { fs.writeFileSync(temp,JSON.stringify(next),{mode:0o600,flag:'wx'});fs.renameSync(temp,file); }
 catch(error){try{fs.unlinkSync(temp);}catch(_){}throw error;}
 return {count:entries.length,historyCount:next.history.length};
}
function resolveOrder(order, store={}) {
 const issues=[];
 const status=store.entries?.[key(order.id,'$order')];
 let pnlCancelled=false,pnlDeliveryDate='';
 if(status) {
  if(status.fingerprint!==orderHash(order)||!['cancelled','source_status','delivered'].includes(status.type))issues.push('Results order-status correction no longer matches the source order. Review it again.');
  else if(status.type==='delivered') {
   if(!validDay(status.deliveryDate)||status.deliveryDate<dateOf(order.createdAt||order.processedAt)||order.cancelledAt)issues.push('Results delivery confirmation conflicts with the source order. Review it again.');
   else pnlDeliveryDate=status.deliveryDate;
  } else pnlCancelled=status.type==='cancelled';
 }
 const lineItems=(order.lineItems||[]).map(line=>{
  const entry=store.entries?.[key(order.id,line.id)];if(!entry)return line;
  if(entry.fingerprint!==hash(line)||canonicalSku(line.sku)&&entry.type!=='sku_identity'){issues.push('Results correction no longer matches source line '+line.id+'. Review it again; source data was not overridden.');return line;}
  if(entry.type==='sku_identity'&&canonicalSku(line.sku)&&canonicalSku(entry.sku)===canonicalSku(line.sku)&&String(entry.inventoryItemId||''))return {...line,pnlInventoryItemId:String(entry.inventoryItemId),pnlCorrection:entry};
  if(entry.type==='sku' && canonicalSku(entry.sku))return {...line,sku:canonicalSku(entry.sku),pnlCorrection:entry};
  if(entry.type==='bundle'&&Array.isArray(entry.components)&&entry.components.length&&entry.components.every(c=>canonicalSku(c.sku)&&Number.isInteger(c.qty)&&c.qty>0&&String(c.inventoryItemId||''))&&new Set(entry.components.map(c=>canonicalSku(c.sku))).size===entry.components.length&&entry.components.reduce((n,c)=>n+c.qty,0)===Number(line.qty))return {...line,pnlComponents:entry.components.map(c=>({...c,sku:canonicalSku(c.sku)})),pnlCorrection:entry};
  if(entry.type==='cost' && Number.isFinite(entry.unitCost)&&entry.unitCost>0)return {...line,pnlUnitCost:entry.unitCost,pnlCorrection:entry};
  if(entry.type==='shipping')return {...line,pnlKind:'shipping',pnlUnitCost:0,pnlCorrection:entry};
  if(entry.type==='shipping_no_tax')return {...line,pnlKind:'shipping',pnlShippingTaxZero:true,pnlUnitCost:0,pnlCorrection:entry};
  if(entry.type==='rounding')return {...line,pnlKind:'rounding',pnlUnitCost:0,pnlCorrection:entry};
  issues.push('Invalid Results correction for source line '+line.id+'.');return line;
 });
 return {order:{...order,lineItems,pnlCancelled,pnlDeliveryDate,pnlStatusCorrection:pnlCancelled||pnlDeliveryDate?status:null},issues};
}
module.exports={key,hash,orderHash,manualHash,expenseHash,expenseConfirmationKey,eligibleExpensePaidAmount,resolveExpenseConfirmation,reviewRows,prepare,read,save,resolveOrder};

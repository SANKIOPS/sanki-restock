'use strict';
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const key = (orderId, lineId) => String(orderId) + '/' + String(lineId);
const hash = line => crypto.createHash('sha256').update(JSON.stringify([String(line.id),line.sku||'',line.title||'',line.qty,line.price,line.discount,line.taxable,line.taxLines])).digest('hex');
const orderHash = order => crypto.createHash('sha256').update(JSON.stringify([String(order.id),order.name||'',order.createdAt||''])).digest('hex');
const canonicalSku = sku => String(sku || '').trim().toUpperCase();
const dateOf = value => /^\d{4}-\d{2}-\d{2}$/.test(String(value||'')) ? value : Number.isFinite(Date.parse(value)) ? new Date(Date.parse(value)+19800000).toISOString().slice(0,10) : '';
const fail = message => { throw Object.assign(Error(message), {status:409}); };
function reviewRows(sources, from, to) {
 const rows=[];
 for(const order of Object.values(sources.orders?.orders || {})) {
  const date=dateOf(order.processedAt || order.createdAt);
  if(date < from || date > to || order.accountingExcluded) continue;
  for(const line of order.lineItems || []) {
   if(canonicalSku(line.sku) || !line.id) continue;
   rows.push({orderId:String(order.id),lineId:String(line.id),reference:order.name||String(order.id),date,title:line.title||'Custom item',qty:line.qty,price:line.price,discount:line.discount||0,fingerprint:hash(line),current:sources.lineCorrections?.entries?.[key(order.id,line.id)] || null});
  }
 }
 return rows.sort((a,b)=>b.date.localeCompare(a.date)||a.reference.localeCompare(b.reference)||a.lineId.localeCompare(b.lineId));
}
function prepare(sources, items, requireFingerprint=false) {
 if(!Array.isArray(items)||!items.length||items.length>100) fail('Choose between 1 and 100 item corrections.');
 const seen=new Set();
 return items.map(input=>{
  if(['cancelled','source_status'].includes(input.type)) {
   const matches=Object.values(sources.orders?.orders||{}).filter(o=>input.orderId?String(o.id)===String(input.orderId):String(o.name||'').toLowerCase()===String(input.reference||'').trim().toLowerCase());
   if(matches.length!==1)fail('Enter the exact existing order reference, including #, or reload the preview.');
   const order=matches[0],id=key(order.id,'$order');
   if(seen.has(id)||!dateOf(order.processedAt||order.createdAt)||dateOf(order.processedAt||order.createdAt)<'2026-08-22'||order.accountingExcluded)fail('Duplicate, excluded, undated or pre-accounting-start orders cannot be corrected here.');
   if(requireFingerprint&&input.fingerprint!==orderHash(order))fail('Order identity changed since preview. Preview it again before saving.');
   seen.add(id);
   return {orderId:String(order.id),lineId:'$order',type:input.type,reference:order.name||String(order.id),fingerprint:orderHash(order),createdDate:dateOf(order.createdAt),title:input.type==='cancelled'?'Confirmed cancelled; exclude sale from P&L':'Use original source completion status'};
  }
  const order=sources.orders?.orders?.[String(input.orderId)];
  const line=order?.lineItems?.find(l=>String(l.id)===String(input.lineId));
  const id=key(input.orderId,input.lineId);
  if(!order || !line || canonicalSku(line.sku) || seen.has(id)) fail('An item is missing, already has a source SKU, or was selected twice. Reload the review.');
  seen.add(id);
  if(!dateOf(order.processedAt || order.createdAt)||dateOf(order.processedAt || order.createdAt)<'2026-08-22') fail('Pre-accounting-start or undated orders cannot be corrected here.');
  if(!Number.isFinite(Number(input.expectedPrice)) || Number(input.expectedPrice)!==Number(line.price) || Number(input.expectedQty)!==Number(line.qty) || !(Number(line.qty)>0)) fail('The original item price or quantity changed. Reload and review the exact line.');
  if(requireFingerprint && input.fingerprint!==hash(line)) fail('Source item changed since preview. Preview it again before saving.');
  const entry={orderId:String(order.id),lineId:String(line.id),type:input.type,expectedPrice:Number(line.price),expectedQty:Number(line.qty),fingerprint:hash(line)};
  if(input.type==='sku') {
   entry.sku=canonicalSku(input.sku);
   const matches=(sources.shopifyCosts?.items||[]).filter(i=>i && canonicalSku(i.sku)===entry.sku);
   const distinct=new Map(matches.map(i=>[i.id,i]));
   if(!entry.sku || distinct.size!==1 || !matches[0]?.id || !(Number(matches[0].unitCost)>0) || !Number.isFinite(Number(matches[0].unitCost)) || (matches[0].unitCostCurrency||matches[0].currency||'INR')!=='INR' || matches.some(i=>Number(i.unitCost)!==Number(matches[0].unitCost))) fail('Mapped SKU must identify one Shopify item with a positive INR cost. Refresh Shopify costs or resolve duplicates first.');
  } else if(input.type==='cost') {
   const cost=Number(input.unitCost);
   if(!Number.isFinite(cost)||cost<=0||cost>10000000||Math.abs(cost*100-Math.round(cost*100))>.00001) fail('Enter a positive per-unit custom-item cost with at most two decimals.');
   entry.unitCost=cost;
  } else if(input.type!=='shipping') fail('Choose SKU mapping, custom-item cost, or shipping fee.');
  return {...entry,reference:order.name||String(order.id),title:line.title||'Custom item'};
 });
}
function read(file) {
 try { const value=JSON.parse(fs.readFileSync(file,'utf8'));if(value.schemaVersion!==1||!value.entries||Array.isArray(value.entries)||typeof value.entries!=='object'||!Array.isArray(value.history)) fail('The P&L item-correction store requires review; it was not replaced.');return value; }
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
 let pnlCancelled=false;
 if(status) {
  if(status.fingerprint!==orderHash(order)||!['cancelled','source_status'].includes(status.type))issues.push('P&L order-status correction no longer matches the source order. Review it again.');
  else pnlCancelled=status.type==='cancelled';
 }
 const lineItems=(order.lineItems||[]).map(line=>{
  const entry=store.entries?.[key(order.id,line.id)];if(!entry)return line;
  if(entry.fingerprint!==hash(line)||canonicalSku(line.sku)){issues.push('P&L correction no longer matches source line '+line.id+'. Review it again; source data was not overridden.');return line;}
  if(entry.type==='sku' && canonicalSku(entry.sku))return {...line,sku:canonicalSku(entry.sku),pnlCorrection:entry};
  if(entry.type==='cost' && Number.isFinite(entry.unitCost)&&entry.unitCost>0)return {...line,pnlUnitCost:entry.unitCost,pnlCorrection:entry};
  if(entry.type==='shipping')return {...line,pnlKind:'shipping',pnlUnitCost:0,pnlCorrection:entry};
  issues.push('Invalid P&L correction for source line '+line.id+'.');return line;
 });
 return {order:{...order,lineItems,pnlCancelled,pnlStatusCorrection:pnlCancelled?status:null},issues};
}
module.exports={key,hash,orderHash,reviewRows,prepare,read,save,resolveOrder};

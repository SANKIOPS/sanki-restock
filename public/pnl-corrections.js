(function(){
 'use strict';
 const el=id=>document.getElementById(id),panel=el('pnlCorrections');
 if(!panel)return;
 const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 let rows=[],preview=null,editable=false,busy=false;
 const status=message=>{el('pnlCorrectionStatus').textContent=message;};
 function invalidate(){preview=null;el('pnlCorrectionSave').disabled=true;el('pnlCorrectionPreview').textContent='';}
 function treatment(r){
  if(!r)return 'Unresolved';
  if(r.type==='sku')return r.sku;
  if(r.type==='cost')return 'COGS ₹'+r.unitCost+' per unit';
  if(r.type==='bundle')return r.components.map(c=>c.sku+' × '+c.qty).join(' + ');
  if(r.type==='sku_identity'){
   const candidate=rows.find(s=>s.orderId===r.orderId&&s.lineId===r.lineId)?.candidates?.find(c=>c.inventoryItemId===r.inventoryItemId);
   return r.sku+' · '+(candidate?candidate.productTitle+' · '+candidate.variant+' · Shopify cost ₹'+candidate.unitCost:r.inventoryItemId);
  }
  return r.type==='rounding'?'Rounding · zero inventory COGS and GST; amount retained':r.type==='shipping_no_tax'?'Shipping fee · confirmed zero GST and inventory COGS':'Shipping fee · zero inventory COGS; source GST retained';
 }
 function bundle(value){
  if(!value.trim())throw Error('Enter every bundle SKU and quantity, for example SKU-A:1, SKU-B:1.');
  return value.split(',').map(part=>{const m=part.trim().match(/^([^,:=*\s]+)\s*[:=*]\s*([1-9]\d*)$/);if(!m)throw Error('Use SKU:quantity for each bundle component, separated by commas.');return {sku:m[1].toUpperCase(),qty:Number(m[2])};});
 }
 function controls(select){
  const index=select.dataset.correctionAction,value=el('pnlCorrectionRows').querySelector('[data-correction-value="'+index+'"]'),identity=el('pnlCorrectionRows').querySelector('[data-correction-inventory="'+index+'"]');
  value.hidden=!['sku','cost','bundle'].includes(select.value);identity.hidden=select.value!=='sku_identity';
  value.placeholder=select.value==='bundle'?'SKU-A:1, SKU-B:1':select.value==='cost'?'INR cost per unit':'Exact Shopify SKU';
 }
 async function request(url,body){const r=await fetch(url,{method:body?'POST':'GET',headers:body?{'Content-Type':'application/json'}:{},body:body?JSON.stringify(body):undefined,cache:'no-store'});const d=await r.json();if(!r.ok||!d.success)throw Error(d.error||'Item corrections could not be read safely.');return d;}
 el('pnlCorrectionLoad').onclick=async()=>{
  if(busy)return;busy=true;invalidate();el('pnlCorrectionLoad').disabled=true;
  try{
   const q=new URLSearchParams({from:el('from').disabled?'2026-08-22':el('from').value||'2026-08-22',to:el('to').value});
   const d=await request('/api/pl/report/line-corrections?'+q);rows=d.rows;editable=d.canEdit;
   el('pnlCorrectionRows').innerHTML='<div class="scroll"><table><thead><tr><th>Order / item</th><th>Original price × quantity</th><th>Current correction</th><th>New treatment</th><th>SKU, components, variant or unit cost</th></tr></thead><tbody>'+rows.map((r,i)=>'<tr data-correction-search="'+esc((r.reference+' '+r.title+' '+r.lineId+' '+(r.sourceSku||'')).toLowerCase())+'"><td>'+esc(r.reference)+' · '+esc(r.date)+'<br>'+esc(r.title)+'<br><small>Line '+esc(r.lineId)+(r.sourceSku?' · Source SKU '+esc(r.sourceSku):'')+'</small></td><td>₹'+esc(r.price)+' × '+esc(r.qty)+'<br><small>Discount ₹'+esc(r.discount)+'</small></td><td>'+esc(treatment(r.current))+'</td><td><select data-correction-action="'+i+'" aria-label="Treatment for '+esc(r.reference)+' line '+esc(r.lineId)+'"'+(!editable?' disabled':'')+'><option value="">Leave unchanged</option>'+(r.sourceSku?'<option value="sku_identity">Choose exact Shopify variant</option>':'<option value="sku">Map to Shopify SKU</option><option value="bundle">Split combined SKU line</option><option value="cost">Custom-item COGS</option><option value="shipping">Shipping fee · retain source GST</option><option value="shipping_no_tax">Shipping fee · confirmed no GST</option><option value="rounding">Rounding · no inventory COGS or GST</option>')+'</select></td><td><input hidden data-correction-value="'+i+'" aria-label="SKU or unit cost for '+esc(r.reference)+' line '+esc(r.lineId)+'" placeholder="Exact Shopify SKU"'+(!editable?' disabled':'')+'><select hidden data-correction-inventory="'+i+'" aria-label="Shopify variant for '+esc(r.reference)+' line '+esc(r.lineId)+'"'+(!editable?' disabled':'')+'><option value="">Choose the verified variant</option>'+(r.candidates||[]).map(c=>'<option value="'+esc(c.inventoryItemId)+'">'+esc(c.productTitle+' · '+c.variant+' · ₹'+c.unitCost+' '+c.currency)+'</option>').join('')+'</select></td></tr>').join('')+'</tbody></table></div>';
   el('pnlCorrectionCheck').disabled=!editable||!rows.length;
   status(rows.length+' source lines needing item review · '+d.historyCount+' audited correction(s). '+(editable?'Choose only the lines you want to correct. Bundle format: SKU-A:1, SKU-B:1.':'Only Admin or Owner may save corrections.'));
  }catch(e){status(e.message);}finally{busy=false;el('pnlCorrectionLoad').disabled=false;}
 };
 el('pnlCorrectionRows').addEventListener('input',invalidate);el('pnlCorrectionRows').addEventListener('change',event=>{invalidate();if(event.target.matches('[data-correction-action]'))controls(event.target);});
 el('pnlCorrectionSearch').addEventListener('input',()=>{const q=el('pnlCorrectionSearch').value.trim().toLowerCase();el('pnlCorrectionRows').querySelectorAll('[data-correction-search]').forEach(r=>{r.hidden=!r.dataset.correctionSearch.includes(q);});});
 el('pnlCorrectionCheck').onclick=async()=>{
  if(busy||!editable)return;busy=true;invalidate();
  try{
   const items=Array.from(el('pnlCorrectionRows').querySelectorAll('[data-correction-action]')).filter(s=>s.value).map(s=>{const r=rows[Number(s.dataset.correctionAction)],v=el('pnlCorrectionRows').querySelector('[data-correction-value="'+s.dataset.correctionAction+'"]').value.trim(),inventoryItemId=el('pnlCorrectionRows').querySelector('[data-correction-inventory="'+s.dataset.correctionAction+'"]').value;return {orderId:r.orderId,lineId:r.lineId,type:s.value,expectedPrice:r.price,expectedQty:r.qty,...(s.value==='sku'?{sku:v}:s.value==='cost'?{unitCost:v}:s.value==='bundle'?{components:bundle(v)}:s.value==='sku_identity'?{inventoryItemId}:{})};});
   const d=await request('/api/pl/report/line-corrections/preview',{items});preview=d.items;
   el('pnlCorrectionPreview').innerHTML='<strong>Review '+preview.length+' Results-only correction(s)</strong><ul>'+preview.map(r=>'<li>'+esc(r.reference)+' · '+esc(r.title)+' · ₹'+esc(r.expectedPrice)+' × '+esc(r.expectedQty)+' → '+esc(treatment(r))+'</li>').join('')+'</ul>';
   el('pnlCorrectionSave').disabled=false;status('Preview validated against the exact source lines. Enter a reason, then save.');
  }catch(e){status(e.message);}finally{busy=false;}
 };
 el('pnlCorrectionSave').onclick=async()=>{
  if(busy||!preview||!editable)return;busy=true;el('pnlCorrectionSave').disabled=true;
  try{const d=await request('/api/pl/report/line-corrections',{items:preview,note:el('pnlCorrectionNote').value});status('Saved '+d.count+' Results-only correction(s). Original orders, invoices and stock were not changed. Reloading report…');location.reload();}
  catch(e){status(e.message);el('pnlCorrectionSave').disabled=false;}finally{busy=false;}
 };
})();

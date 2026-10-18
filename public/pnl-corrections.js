(function(){
 'use strict';
 const el=id=>document.getElementById(id),panel=el('pnlCorrections');
 if(!panel)return;
 const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 let rows=[],preview=null,editable=false,busy=false;
 const status=message=>{el('pnlCorrectionStatus').textContent=message;};
 function invalidate(){preview=null;el('pnlCorrectionSave').disabled=true;el('pnlCorrectionPreview').textContent='';}
 async function request(url,body){const r=await fetch(url,{method:body?'POST':'GET',headers:body?{'Content-Type':'application/json'}:{},body:body?JSON.stringify(body):undefined,cache:'no-store'});const d=await r.json();if(!r.ok||!d.success)throw Error(d.error||'Item corrections could not be read safely.');return d;}
 el('pnlCorrectionLoad').onclick=async()=>{
  if(busy)return;busy=true;invalidate();el('pnlCorrectionLoad').disabled=true;
  try{
   const q=new URLSearchParams({from:el('from').disabled?'2026-08-22':el('from').value||'2026-08-22',to:el('to').value});
   const d=await request('/api/pl/report/line-corrections?'+q);rows=d.rows;editable=d.canEdit;
   el('pnlCorrectionRows').innerHTML='<div class="scroll"><table><thead><tr><th>Order / item</th><th>Original price × quantity</th><th>Current correction</th><th>New treatment</th><th>SKU or per-unit cost</th></tr></thead><tbody>'+rows.map((r,i)=>'<tr data-correction-search="'+esc((r.reference+' '+r.title+' '+r.lineId).toLowerCase())+'"><td>'+esc(r.reference)+' · '+esc(r.date)+'<br>'+esc(r.title)+'<br><small>Line '+esc(r.lineId)+'</small></td><td>₹'+esc(r.price)+' × '+esc(r.qty)+'<br><small>Discount ₹'+esc(r.discount)+'</small></td><td>'+esc(r.current?(r.current.type==='sku'?r.current.sku:r.current.type==='cost'?'COGS ₹'+r.current.unitCost:'Shipping fee'):'Unresolved')+'</td><td><select data-correction-action="'+i+'" aria-label="Treatment for '+esc(r.reference)+' line '+esc(r.lineId)+'"'+(!editable?' disabled':'')+'><option value="">Leave unchanged</option><option value="sku">Map to Shopify SKU</option><option value="cost">Custom-item COGS</option><option value="shipping">Shipping fee · no inventory COGS</option></select></td><td><input data-correction-value="'+i+'" aria-label="SKU or unit cost for '+esc(r.reference)+' line '+esc(r.lineId)+'" placeholder="SKU, or INR cost per unit"'+(!editable?' disabled':'')+'></td></tr>').join('')+'</tbody></table></div>';
   el('pnlCorrectionCheck').disabled=!editable||!rows.length;
   status(rows.length+' source lines without SKUs · '+d.historyCount+' audited correction(s). '+(editable?'Choose only the lines you want to correct.':'Only Admin or Owner may save corrections.'));
  }catch(e){status(e.message);}finally{busy=false;el('pnlCorrectionLoad').disabled=false;}
 };
 el('pnlCorrectionRows').addEventListener('input',invalidate);el('pnlCorrectionRows').addEventListener('change',invalidate);
 el('pnlCorrectionSearch').addEventListener('input',()=>{const q=el('pnlCorrectionSearch').value.trim().toLowerCase();el('pnlCorrectionRows').querySelectorAll('[data-correction-search]').forEach(r=>{r.hidden=!r.dataset.correctionSearch.includes(q);});});
 el('pnlCorrectionCheck').onclick=async()=>{
  if(busy||!editable)return;busy=true;invalidate();
  try{
   const items=Array.from(el('pnlCorrectionRows').querySelectorAll('[data-correction-action]')).filter(s=>s.value).map(s=>{const r=rows[Number(s.dataset.correctionAction)],v=el('pnlCorrectionRows').querySelector('[data-correction-value="'+s.dataset.correctionAction+'"]').value.trim();return {orderId:r.orderId,lineId:r.lineId,type:s.value,expectedPrice:r.price,expectedQty:r.qty,...(s.value==='sku'?{sku:v}:s.value==='cost'?{unitCost:v}:{})};});
   const d=await request('/api/pl/report/line-corrections/preview',{items});preview=d.items;
   el('pnlCorrectionPreview').innerHTML='<strong>Review '+preview.length+' P&L-only correction(s)</strong><ul>'+preview.map(r=>'<li>'+esc(r.reference)+' · '+esc(r.title)+' · ₹'+esc(r.expectedPrice)+' × '+esc(r.expectedQty)+' → '+esc(r.type==='sku'?r.sku:r.type==='cost'?'COGS ₹'+r.unitCost+' per unit':'Shipping fee; zero inventory COGS; invoice tax retained')+'</li>').join('')+'</ul>';
   el('pnlCorrectionSave').disabled=false;status('Preview validated against the exact source lines. Enter a reason, then save.');
  }catch(e){status(e.message);}finally{busy=false;}
 };
 el('pnlCorrectionSave').onclick=async()=>{
  if(busy||!preview||!editable)return;busy=true;el('pnlCorrectionSave').disabled=true;
  try{const d=await request('/api/pl/report/line-corrections',{items:preview,note:el('pnlCorrectionNote').value});status('Saved '+d.count+' P&L-only correction(s). Original orders, invoices and stock were not changed. Reloading report…');location.reload();}
  catch(e){status(e.message);el('pnlCorrectionSave').disabled=false;}finally{busy=false;}
 };
})();

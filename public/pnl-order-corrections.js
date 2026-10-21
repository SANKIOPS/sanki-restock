(function(){
 'use strict';
 const el=id=>document.getElementById(id);
 if(!el('pnlOrderCorrections'))return;
 let preview=null,busy=false;
 const message=text=>{el('pnlOrderPreview').textContent=text;};
 const reset=()=>{preview=null;el('pnlOrderSave').disabled=true;message('Preview the exact order before saving.');};
 ['pnlOrderReference','pnlOrderTreatment','pnlOrderDate'].forEach(id=>{el(id)?.addEventListener('input',reset);el(id)?.addEventListener('change',reset);});
 async function post(path,body){const r=await fetch('/api/pl/report/line-corrections'+path,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});const d=await r.json();if(!r.ok||!d.success)throw Error(d.error||'The Results order treatment could not be saved.');return d;}
 el('pnlOrderPreviewButton').onclick=async()=>{
  if(busy)return;busy=true;reset();el('pnlOrderPreviewButton').disabled=true;
  try{const d=await post('/preview',{items:[{reference:el('pnlOrderReference').value.trim(),type:el('pnlOrderTreatment').value,deliveryDate:el('pnlOrderDate')?.value||''}]});preview=d.items;const p=preview[0];message(p.reference+' · Created '+p.createdDate+' · '+p.title+'. Results only; customer collections and refunds are unchanged. Enter the confirmation reason to save.');el('pnlOrderSave').disabled=false;}
  catch(e){message(e.message);}finally{busy=false;el('pnlOrderPreviewButton').disabled=false;}
 };
 el('pnlOrderSave').onclick=async()=>{
  if(busy||!preview)return;busy=true;el('pnlOrderSave').disabled=true;
  try{await post('',{items:preview,note:el('pnlOrderNote').value});message('Saved Results-only order treatment. Reloading report…');location.reload();}
  catch(e){message(e.message);el('pnlOrderSave').disabled=false;}finally{busy=false;}
 };
 if(!el('pnlManualCorrections'))return;
 let manualPreview=null,manualBusy=false;
 const manualMessage=text=>{el('pnlManualPreview').textContent=text;};
 const manualReset=()=>{manualPreview=null;el('pnlManualSave').disabled=true;manualMessage('Preview the exact existing manual-sale reference before saving.');};
 ['pnlManualReference','pnlManualQty','pnlManualCost','pnlManualTax','pnlManualBasis'].forEach(id=>{el(id).addEventListener('input',manualReset);el(id).addEventListener('change',manualReset);});
 el('pnlManualPreviewButton').onclick=async()=>{
  if(manualBusy)return;manualBusy=true;manualReset();
  try{const tax=el('pnlManualTax').value,basis=el('pnlManualBasis').value;if((tax==='')!==(basis===''))throw Error('Confirm both GST rate and price basis, or leave both blank.');const item={type:'manual_cost',reference:el('pnlManualReference').value.trim(),qty:Number(el('pnlManualQty').value),unitCost:Number(el('pnlManualCost').value)};if(tax!==''){item.taxRate=Number(tax)/100;item.taxesIncluded=basis==='inclusive';}const d=await post('/preview',{items:[item]});manualPreview=d.items;const p=manualPreview[0];manualMessage(p.reference+' · '+p.title+' · existing receipt amount ₹'+p.expectedAmount+' · COGS '+p.qty+' × ₹'+p.unitCost+'. '+(p.taxRate===undefined?'GST remains unresolved; no zero-tax assumption.':'Confirmed GST '+Number((p.taxRate*100).toFixed(2))+'%, '+(p.taxesIncluded?'inclusive':'exclusive')+'.'));el('pnlManualSave').disabled=false;}
  catch(e){manualMessage(e.message);}finally{manualBusy=false;}
 };
 el('pnlManualSave').onclick=async()=>{
  if(manualBusy||!manualPreview)return;manualBusy=true;el('pnlManualSave').disabled=true;
  try{await post('',{items:manualPreview,note:el('pnlManualNote').value});location.reload();}
  catch(e){manualMessage(e.message);el('pnlManualSave').disabled=false;}finally{manualBusy=false;}
 };
})();

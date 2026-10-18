(function(){
 'use strict';
 const el=id=>document.getElementById(id);
 if(!el('pnlOrderCorrections'))return;
 let preview=null,busy=false;
 const message=text=>{el('pnlOrderPreview').textContent=text;};
 const reset=()=>{preview=null;el('pnlOrderSave').disabled=true;message('Preview the exact order before saving.');};
 ['pnlOrderReference','pnlOrderTreatment'].forEach(id=>{el(id).addEventListener('input',reset);el(id).addEventListener('change',reset);});
 async function post(path,body){const r=await fetch('/api/pl/report/line-corrections'+path,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});const d=await r.json();if(!r.ok||!d.success)throw Error(d.error||'The P&L order treatment could not be saved.');return d;}
 el('pnlOrderPreviewButton').onclick=async()=>{
  if(busy)return;busy=true;reset();el('pnlOrderPreviewButton').disabled=true;
  try{const d=await post('/preview',{items:[{reference:el('pnlOrderReference').value.trim(),type:el('pnlOrderTreatment').value}]});preview=d.items;const p=preview[0];message(p.reference+' · Created '+p.createdDate+' · '+p.title+'. P&L only; customer collections and refunds are unchanged. Enter the confirmation reason to save.');el('pnlOrderSave').disabled=false;}
  catch(e){message(e.message);}finally{busy=false;el('pnlOrderPreviewButton').disabled=false;}
 };
 el('pnlOrderSave').onclick=async()=>{
  if(busy||!preview)return;busy=true;el('pnlOrderSave').disabled=true;
  try{await post('',{items:preview,note:el('pnlOrderNote').value});message('Saved P&L-only order treatment. Reloading report…');location.reload();}
  catch(e){message(e.message);el('pnlOrderSave').disabled=false;}finally{busy=false;}
 };
})();

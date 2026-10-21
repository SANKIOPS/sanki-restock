(function(){
 'use strict';
 const el=id=>document.getElementById(id),panel=el('pnlExpenseCorrections');
 if(!panel)return;
 const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
 const fields=['pnlExpenseReference','pnlExpenseAmount','pnlExpenseDate','pnlExpenseAdjustment'];
 let preview=null,busy=false,revision=0;
 const status=value=>{el('pnlExpenseStatus').textContent=value;};
 function invalidate(){revision++;preview=null;el('pnlExpenseSave').disabled=true;el('pnlExpensePreview').textContent='';}
 async function request(url,body){const response=await fetch(url,{method:'POST',headers:{'Content-Type':'application/json'},cache:'no-store',body:JSON.stringify(body)}),data=await response.json();if(!response.ok||!data.success)throw Error(data.error||'Results confirmation could not be saved safely. No financial records were changed.');return data;}
 fields.forEach(id=>{el(id).addEventListener('input',invalidate);el(id).addEventListener('change',invalidate);});
 el('pnlExpenseCheck').onclick=async()=>{
  if(busy)return;invalidate();const requestedRevision=revision;busy=true;el('pnlExpenseCheck').disabled=true;
  try{
   const item={type:'expense_confirmation',reference:el('pnlExpenseReference').value.trim(),amount:el('pnlExpenseAmount').value,paymentDate:el('pnlExpenseDate').value,adjustmentReference:el('pnlExpenseAdjustment').value.trim()};
   const data=await request('/api/pl/report/line-corrections/preview',{items:[item]});
   if(requestedRevision!==revision){status('The form changed during preview. Preview the current confirmation again.');return;}
   preview=data.items;
   const fact=preview[0];
   el('pnlExpensePreview').innerHTML='<strong>'+esc(fact.reference)+' · '+esc(fact.title)+'</strong><p>Confirmed total paid expense: ₹'+esc(fact.amount)+'; eligible source payments: ₹'+esc(fact.eligiblePaidAmount)+'. Add only ₹'+esc(fact.reportOnlyAmount)+' to Results on '+esc(fact.paymentDate)+'.</p><p>Basis: <strong>Results-only paid-expense confirmation</strong>. Original payments, exclusions, account balances and reconciliations will not change. No bank posting is created.</p>'+(fact.adjustmentReference?'<p>Related reference: '+esc(fact.adjustmentReference)+' — <strong>user-reported only, not verified bank or adjustment evidence</strong>.</p>':'');
   el('pnlExpenseSave').disabled=false;status('Preview validated against the existing paid expense. Enter the confirmation reason before saving.');
  }catch(error){status(error.message);}finally{busy=false;el('pnlExpenseCheck').disabled=false;}
 };
 el('pnlExpenseSave').onclick=async()=>{
  if(busy||!preview)return;busy=true;el('pnlExpenseSave').disabled=true;el('pnlExpenseCheck').disabled=true;fields.forEach(id=>{el(id).disabled=true;});
  try{const data=await request('/api/pl/report/line-corrections',{items:preview,note:el('pnlExpenseNote').value});status('Saved '+data.count+' Results-only confirmation. Source payments, bank balances and reconciliations were not changed. Reloading report…');location.reload();}
  catch(error){status(error.message);el('pnlExpenseSave').disabled=false;}finally{busy=false;el('pnlExpenseCheck').disabled=false;fields.forEach(id=>{el(id).disabled=false;});}
 };
})();

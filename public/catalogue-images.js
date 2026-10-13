'use strict';
const $=selector=>document.querySelector(selector);
const esc=value=>{const d=document.createElement('div');d.textContent=String(value||'');return d.innerHTML};
let state=null;

async function api(url,options){
  const response=await fetch(url,options),body=await response.json().catch(()=>({}));
  if(!response.ok)throw Error(body.error||'Request failed');
  return body;
}
function viewLabel(type){
  return ({front:'Product front',alternate:'Alternate angle',detail:'Detail',female:'Female front',male:'Male front','model-front':'Model front','model-side':'Three-quarter'})[type]||type;
}
function qaText(draft){
  if(draft.accepted)return 'Approved for this trio';
  if(draft.qa?.status==='manual-review')return 'Returned from Codex · inspect before approval';
  return 'Needs review';
}
function draftCard(item,draft){
  return `<div class="shot draft" data-draft-type="${esc(draft.type)}"><img src="${esc(draft.url)}" alt=""><b>${esc(viewLabel(draft.type))}</b><span class="qa">${esc(qaText(draft))}</span>${draft.accepted?'':`<button class="approve-view" data-approve-view="${esc(draft.type)}">Approve view</button>`}</div>`;
}
function card(item){
  const images=item.currentImages.map(image=>`<label class="shot"><input type="radio" name="source-${item.productId}" value="${esc(image.src)}" ${image.src===item.sourceUrl?'checked':''}><img src="${esc(image.src)}" alt=""><small>${esc(image.name||'Current image')}</small></label>`).join('');
  const drafts=(item.drafts||[]).sort((a,b)=>item.requiredViews.indexOf(a.type)-item.requiredViews.indexOf(b.type)).map(draft=>draftCard(item,draft)).join('');
  const activeBatch=state?.pilot?.activeCodexBatchId||'';
  const returnBar=activeBatch?`<div class="returnbar"><label>Returned view<select data-return-type>${item.requiredViews.map(type=>`<option value="${esc(type)}">${esc(viewLabel(type))}</option>`).join('')}</select></label><label class="upload">Return Codex image<input data-codex-return type="file" accept="image/jpeg,image/png,image/webp"></label><span class="meta">Batch ${esc(activeBatch)}</span></div>`:`<p class="meta">Prepare the Codex batch before returning generated images.</p>`;
  return `<article class="card" data-product="${item.productId}"><div class="cardhead"><div><h2>${esc(item.title)}</h2><div class="meta">SKU ${esc(item.sku||'—')} · ${esc(item.productType)} · ${esc(item.colour)} · ${item.currentImages.length} existing image(s)</div></div><span class="badge ${esc(item.status)}">${esc(item.status)}</span></div><div class="setup"><label>Audience<select data-audience>${['Women','Men','Unisex'].map(value=>`<option ${value===item.audience?'selected':''}>${value}</option>`).join('')}</select></label><button data-save>Save audience & source</button><span class="meta">Required: ${item.requiredViews.map(viewLabel).join(' · ')}</span></div><div class="sectionlabel">Select the actual product reference</div><div class="current">${images}</div>${returnBar}${drafts?`<div class="sectionlabel">Returned Codex trio</div><div class="drafts">${drafts}</div>`:''}<div class="cardfoot"><p>${esc(item.message)}</p>${item.status==='passed'?'<button class="publish" data-publish>Publish trio to positions 1–3</button>':''}${item.status==='published'?'<a href="https://sanki.online/products/'+encodeURIComponent(item.handle)+'" target="_blank">Open live product</a>':''}</div></article>`;
}
function render(){
  const items=state?.pilot?.items||[];
  $('#pilotState').textContent=state?.pilot?.status||'Not prepared';
  $('#selected').textContent=items.length;
  $('#passed').textContent=items.filter(item=>item.status==='passed').length;
  $('#published').textContent=items.filter(item=>item.status==='published').length;
  $('#message').textContent=state?.pilot?.message||'Prepare 4 Women, 4 Men and 4 Unisex products. Incomplete and reference-file galleries are prioritised.';
  $('#products').innerHTML=items.map(card).join('');
  $('#codexBatch').disabled=!items.length;
  $('#prepare').disabled=false;
  document.querySelectorAll('[data-save]').forEach(button=>button.onclick=()=>saveSetup(button.closest('.card')));
  document.querySelectorAll('[data-publish]').forEach(button=>button.onclick=()=>publish(button.closest('.card')));
  document.querySelectorAll('[data-codex-return]').forEach(input=>input.onchange=()=>returnCodexImage(input.closest('.card'),input));
  document.querySelectorAll('[data-approve-view]').forEach(button=>button.onclick=()=>approveView(button.closest('.card'),button.dataset.approveView));
}
async function load(){
  try{state=await api('/api/catalogue-images');render()}catch(error){$('#message').textContent=error.message}
}
async function saveSetup(card){
  const id=card.dataset.product,audience=card.querySelector('[data-audience]').value,source=card.querySelector('input[type=radio]:checked');
  if(!source)return alert('Select one current image as the product reference.');
  try{state=await api('/api/catalogue-images/pilot/items/'+id,{method:'PATCH',headers:{'Content-Type':'application/json'},body:JSON.stringify({audience,sourceUrl:source.value})});render()}catch(error){alert(error.message)}
}
function downloadBatch(batch){
  const blob=new Blob([JSON.stringify(batch,null,2)],{type:'application/json'}),url=URL.createObjectURL(blob),a=document.createElement('a');
  a.href=url;a.download='catalogue-codex-batch-'+batch.id+'.json';a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);
}
async function prepareCodexBatch(){
  const button=$('#codexBatch');button.disabled=true;
  try{
    const data=await api('/api/catalogue-images/pilot/codex-batch',{method:'POST',headers:{'Content-Type':'application/json'},body:'{}'});
    state=data;render();downloadBatch(data.batch);
    alert('Codex batch '+data.batch.id+' is ready. No paid Railway AI call was made. Generate the files in Codex and return them here for review.');
  }catch(error){alert(error.message);load()}finally{button.disabled=false}
}
async function returnCodexImage(card,input){
  const file=input.files&&input.files[0],batchId=state?.pilot?.activeCodexBatchId,type=card.querySelector('[data-return-type]').value;
  if(!file||!batchId)return;
  const form=new FormData();form.append('type',type);form.append('image',file);
  input.disabled=true;
  try{state=await api('/api/catalogue-images/pilot/codex-batch/'+encodeURIComponent(batchId)+'/items/'+encodeURIComponent(card.dataset.product)+'/results',{method:'POST',body:form});render()}catch(error){alert(error.message);input.disabled=false;input.value=''}
}
async function approveView(card,type){
  if(!confirm('Approve this '+viewLabel(type)+' image after checking the garment, audience, outfit and framing?'))return;
  try{state=await api('/api/catalogue-images/pilot/items/'+encodeURIComponent(card.dataset.product)+'/drafts/'+encodeURIComponent(type)+'/review',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({accepted:true})});render()}catch(error){alert(error.message)}
}
async function publish(card){
  const item=state.pilot.items.find(value=>value.productId===card.dataset.product);
  if(!confirm('Publish the three approved Codex images for '+item.sku+' to live Shopify positions 1–3? Earlier images will remain after them.'))return;
  try{state=await api('/api/catalogue-images/pilot/items/'+item.productId+'/publish',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({confirm:true})});render()}catch(error){alert(error.message);load()}
}

$('#prepare').onclick=async()=>{
  if(state?.pilot&&!confirm('Replace the current pilot queue with a fresh 12-product mixed pilot? Saved generated files remain in audit history, but this queue will change.'))return;
  try{$('#message').textContent='Reading active in-stock Shopify products…';state=await api('/api/catalogue-images/pilot/prepare',{method:'POST',headers:{'Content-Type':'application/json'},body:'{}'});render()}catch(error){alert(error.message);load()}
};
$('#codexBatch').onclick=prepareCodexBatch;
$('#refresh').onclick=load;
load();

'use strict';
(() => {
  const $=id=>document.getElementById(id), esc=value=>String(value??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const labels={queued:'Queued',processing:'Creating images',ready:'Ready to review',publishing:'Publishing',publish_failed:'Publishing needs retry',published:'Published',unreviewed:'Not reviewed',correct:'Images correct',awaiting_originals:'Awaiting originals',ready_for_correction:'Ready for correction',awaiting_approval:'Awaiting approval',approved_for_upload:'Approved for upload',website_changed:'Website changed'};
  let items=[],offset=0,total=0,selected=null,busy=false,initial=true,request=0,canPublish=false;
  const bulkSelection=new Map();let bulkPlan=[],bulkRunning=false,bulkPreparing=false,bulkStop=false;
  const bulkEligible=i=>window.ImageReviewBulk.eligible(i);
  async function api(url,options={}){const r=await fetch('/api/image-review'+url,options);const d=await r.json();if(!r.ok||!d.success){const error=Error(d.error||'Could not load Image Review.');error.item=d.item;throw error;}return d;}
  function message(text,error=false){$('message').textContent=text;$('message').className='message'+(error?' error':'');}
  const outputOrder=['front','detail','angle','back','product'],viewNames={front:'Model front',detail:'Detail close-up',angle:'Model 45°',back:'Model back',product:'Product-only front'};
  const revisionDrafts={};let viewerItems=[],viewerIndex=0,swipeStart=null;
  let zoom=1,panX=0,panY=0,fitWidth=0,fitHeight=0;
  const stage=$('zoomStage'),zoomImage=$('enlargedImage'),pointers=new Map();
  function paintZoom(){
    const limitX=Math.max(0,(fitWidth*zoom-stage.clientWidth)/2),limitY=Math.max(0,(fitHeight*zoom-stage.clientHeight)/2);
    panX=Math.max(-limitX,Math.min(limitX,panX));panY=Math.max(-limitY,Math.min(limitY,panY));
    zoomImage.style.width=fitWidth+'px';zoomImage.style.height=fitHeight+'px';zoomImage.style.transform=`translate(-50%,-50%) translate(${panX}px,${panY}px) scale(${zoom})`;
    const original=$('viewerOriginalImage'),originalStage=$('originalStage');
    if(!$('viewerOriginalPane').hidden&&original.naturalWidth&&originalStage.clientHeight){const ratio=Math.min(originalStage.clientWidth/original.naturalWidth,originalStage.clientHeight/original.naturalHeight),w=original.naturalWidth*ratio,h=original.naturalHeight*ratio,lx=Math.max(0,(w*zoom-originalStage.clientWidth)/2),ly=Math.max(0,(h*zoom-originalStage.clientHeight)/2);original.style.width=w+'px';original.style.height=h+'px';original.style.transform=`translate(-50%,-50%) translate(${Math.max(-lx,Math.min(lx,panX))}px,${Math.max(-ly,Math.min(ly,panY))}px) scale(${zoom})`;}
    $('zoomLevel').value=Math.round(zoom*100)+'%';$('zoomIn').disabled=zoom>=8;$('zoomOut').disabled=zoom<=1;
  }
  function fitImage(){if(!zoomImage.naturalWidth||!stage.clientWidth)return;const ratio=Math.min(stage.clientWidth/zoomImage.naturalWidth,stage.clientHeight/zoomImage.naturalHeight);fitWidth=zoomImage.naturalWidth*ratio;fitHeight=zoomImage.naturalHeight*ratio;zoom=1;panX=panY=0;paintZoom();}
  function changeZoom(value,x=stage.clientWidth/2,y=stage.clientHeight/2){const next=Math.max(1,Math.min(8,value)),ratio=next/zoom;panX=(panX-(x-stage.clientWidth/2))*ratio+(x-stage.clientWidth/2);panY=(panY-(y-stage.clientHeight/2))*ratio+(y-stage.clientHeight/2);zoom=next;paintZoom();}
  function showViewer(index){
    viewerIndex=(index+viewerItems.length)%viewerItems.length;const current=viewerItems[viewerIndex];
    pointers.clear();zoom=1;panX=panY=0;
    $('viewerOriginalPane').hidden=!current.originalSrc;$('viewerPair').classList.toggle('paired',!!current.originalSrc);
    $('viewerOriginalImage').onload=paintZoom;if(current.originalSrc)$('viewerOriginalImage').src=current.originalSrc;
    $('viewerResultLabel').textContent=current.view?'AI result'+(current.originalSrc?'':' · Created from uploaded references'):'Original / website photo';
    zoomImage.onload=fitImage;zoomImage.src=current.src;
    $('viewerCaption').textContent=`${viewerIndex+1} / ${viewerItems.length} · ${current.label}`;
    $('imagePrevious').disabled=$('imageNext').disabled=viewerItems.length<2;
    const editable=!!current.view&&['ready','publish_failed'].includes(selected?.review.job?.status);
    $('viewerRevision').hidden=!editable;$('viewerFeedback').value=current.view?revisionDrafts[current.view]||'':'';fitImage();
  }
  function enlarge(list,index){viewerItems=list;if(!$('imageDialog').open)$('imageDialog').showModal();showViewer(index);}
  $('imagePrevious').onclick=()=>showViewer(viewerIndex-1);$('imageNext').onclick=()=>showViewer(viewerIndex+1);
  $('viewerFeedback').addEventListener('input',()=>{const view=viewerItems[viewerIndex]?.view;if(view){revisionDrafts[view]=$('viewerFeedback').value;const field=$('reviewBody').querySelector(`[data-view-feedback="${view}"]`);if(field)field.value=revisionDrafts[view];}});
  $('imageDialog').addEventListener('keydown',e=>{if(e.target.matches('input,textarea'))return;if(e.key==='ArrowLeft'&&zoom===1){e.preventDefault();showViewer(viewerIndex-1);}if(e.key==='ArrowRight'&&zoom===1){e.preventDefault();showViewer(viewerIndex+1);}});
  $('zoomIn').addEventListener('click',()=>changeZoom(zoom*1.5));$('zoomOut').addEventListener('click',()=>changeZoom(zoom/1.5));$('zoomFit').addEventListener('click',fitImage);
  $('viewerPair').addEventListener('wheel',e=>{e.preventDefault();const r=(e.target.closest('#originalStage')||stage).getBoundingClientRect();changeZoom(zoom*Math.exp(-e.deltaY*0.002),e.clientX-r.left,e.clientY-r.top);},{passive:false});
  $('viewerPair').addEventListener('dblclick',e=>{const r=stage.getBoundingClientRect();if(zoom>1)fitImage();else changeZoom(3,e.clientX-r.left,e.clientY-r.top);});
  $('viewerPair').addEventListener('pointerdown',e=>{if(pointers.size===0)swipeStart={x:e.clientX,y:e.clientY,id:e.pointerId};else swipeStart=null;stage.setPointerCapture(e.pointerId);pointers.set(e.pointerId,{x:e.clientX,y:e.clientY});});
  $('viewerPair').addEventListener('pointermove',e=>{if(!pointers.has(e.pointerId))return;const before=[...pointers.values()],old=pointers.get(e.pointerId);pointers.set(e.pointerId,{x:e.clientX,y:e.clientY});const after=[...pointers.values()];if(before.length===2){const distance=p=>Math.hypot(p[0].x-p[1].x,p[0].y-p[1].y),r=(e.target.closest('#originalStage')||stage).getBoundingClientRect(),prior=distance(before);if(prior)changeZoom(zoom*distance(after)/prior,(after[0].x+after[1].x)/2-r.left,(after[0].y+after[1].y)/2-r.top);}else{panX+=e.clientX-old.x;panY+=e.clientY-old.y;paintZoom();}});
  $('viewerPair').addEventListener('pointerup',e=>{if(swipeStart?.id===e.pointerId&&pointers.size===1&&zoom===1){const dx=e.clientX-swipeStart.x,dy=e.clientY-swipeStart.y;if(Math.abs(dx)>55&&Math.abs(dx)>Math.abs(dy)*1.3)showViewer(viewerIndex+(dx<0?1:-1));}swipeStart=null;pointers.delete(e.pointerId);});
  for(const name of['pointercancel','lostpointercapture'])$('viewerPair').addEventListener(name,e=>{swipeStart=null;pointers.delete(e.pointerId);});
  $('viewerPair').addEventListener('keydown',e=>{if(['+','=','-','0','ArrowLeft','ArrowRight','ArrowUp','ArrowDown'].includes(e.key)){e.preventDefault();if(e.key==='0')fitImage();else if(e.key==='+'||e.key==='=')changeZoom(zoom*1.5);else if(e.key==='-')changeZoom(zoom/1.5);else{panX+=e.key==='ArrowLeft'?60:e.key==='ArrowRight'?-60:0;panY+=e.key==='ArrowUp'?60:e.key==='ArrowDown'?-60:0;paintZoom();}}});
  new ResizeObserver(()=>{if($('imageDialog').open)fitImage();}).observe(stage);
  const picture=(image,extra='',view='',label='Garment image',originalView='')=>`<button class="image-button ${extra}" data-image="${esc(image.src||image.url)}" data-result-view="${esc(view)}" data-original-view="${esc(originalView)}" data-image-label="${esc(label)}" aria-label="Enlarge garment image"><img src="${esc(image.src||image.url)}" alt="${esc(image.alt||image.view||'Garment image')}" loading="lazy"></button>`;
  function wirePictures(root){const buttons=[...root.querySelectorAll('[data-image]')];buttons.forEach(b=>b.addEventListener('click',()=>{
    const view=b.dataset.resultView||b.dataset.originalView;
    if(view&&selected?.review.job&&latest(selected.review.proposals,view)){
      const list=outputOrder.filter(v=>latest(selected.review.proposals,v)).map(v=>({view:v,src:latest(selected.review.proposals,v).src||latest(selected.review.proposals,v).url,originalSrc:latest(selected.review.originals,v)?.src||latest(selected.review.originals,v)?.url,label:viewNames[v]}));
      enlarge(list,list.findIndex(x=>x.view===view));return;
    }
    const group=[...(b.closest('.review-section')||root).querySelectorAll('[data-image]')].filter(x=>!x.dataset.resultView);const list=group.map(x=>({src:x.dataset.image,label:x.dataset.imageLabel||'Original / website photo'}));enlarge(list,group.indexOf(b));
  }));}
  function fillSelect(id,values){const select=$(id),old=select.value;select.replaceChildren(new Option(id==='collection'?'All collections':id==='gender'?'All genders':'All categories',''),...values.map(v=>new Option(v,v)));select.value=values.includes(old)?old:'';return old!==select.value;}
  async function load(){const ticket=++request;message('Loading Shopify galleries…');try{
    const params=new URLSearchParams({offset,limit:24});for(const k of['collection','gender','category','status','search'])if($(k).value)params.set(k,$(k).value);
    const d=await api('/catalogue?'+params);if(ticket!==request)return;
    let changed=false;for(const k of['collection','gender','category'])changed=fillSelect(k,d.facets[k==='collection'?'collections':k==='gender'?'genders':'categories'])||changed;
    if(changed)return load();
    if(initial){initial=false;let reload=false;if(d.facets.collections.includes('Casuals')){$('collection').value='Casuals';reload=true;}if(d.facets.genders.includes('Women')){$('gender').value='Women';reload=true;}if(reload)return load();}
    canPublish=d.canPublish===true;items=d.items;total=d.total;if(!canPublish){const exportLink=document.querySelector('a[href="/api/image-review/export"]');if(exportLink)exportLink.hidden=true;}
    $('summary').innerHTML=Object.entries(d.summary).map(([status,count])=>`<span><strong>${count}</strong>${esc(labels[status]||status)}</span>`).join('');
    $('products').innerHTML=items.length?items.map(i=>`<article class="product">${canPublish&&bulkEligible(i)?`<label class="bulk-select"><input type="checkbox" data-bulk-select="${esc(i.id)}" ${bulkSelection.has(i.id)?'checked':''} aria-label="Select ${esc(i.title)} ${esc(i.colour)}"> Select for approval</label>`:''}${i.images[0]?picture(i.images[0],'product-cover'):'<div class="product-cover">No website images</div>'}<div class="product-info"><h3>${esc(i.title)}</h3><p class="meta">${esc(i.colour)} · ${esc(i.category)} · ${esc(i.shopifyStatus)}${i.publishedAt?' · Published':' · Not published'}</p><p class="meta">${esc(i.variants.map(v=>v.sku).filter(Boolean).join(', ')||'SKU missing')}</p><span class="badge ${esc(i.status)}">${esc(labels[i.status])}</span><div class="strip">${i.images.map(m=>picture(m)).join('')}</div><button class="primary" data-review="${esc(i.id)}">Review ${i.images.length} website images</button></div></article>`).join(''):'<div class="empty">No products match these filters.</div>';
    $('products').querySelectorAll('[data-bulk-select]').forEach(input=>input.onchange=()=>{const item=items.find(i=>i.id===input.dataset.bulkSelect);if(input.checked)bulkSelection.set(item.id,item);else bulkSelection.delete(item.id);updateBulk();});
    updateBulk();wirePictures($('products'));$('products').querySelectorAll('[data-review]').forEach(b=>b.addEventListener('click',()=>open(items.find(i=>i.id===b.dataset.review))));
    $('page').textContent=total?`${offset+1}–${Math.min(offset+24,total)} of ${total} product colours`:'0 product colours';$('previous').disabled=offset===0;$('next').disabled=offset+24>=total;message('Shopify images read '+new Date(d.syncedAt).toLocaleString()+'. No upload means not reviewed.');
  }catch(e){message(e.message,true);}}
  function latest(list,view){return [...list].reverse().find(f=>f.view===view);}
  function slot(view){const file=latest(selected.review.originals,view),job=selected.review.job,active=!selected.review.pendingViews||selected.review.pendingViews.includes(view);return `<div class="photo-slot"><h4>${view[0].toUpperCase()+view.slice(1)}</h4><div class="slot-picture">${file?picture(file,'','',view+' · Your original',view):'No correction requested for this view'}</div><label>${file?'Replace uploaded':'Upload correct'} ${view} photo<input type="file" accept="image/jpeg,image/png,image/webp" data-kind="original" data-view="${view}"></label><small>${file?esc(file.width)+' × '+esc(file.height)+' · Saved '+esc(new Date(file.uploadedAt).toLocaleDateString()):'Optional · JPEG, PNG or WebP · up to 20 MB'}</small>${file&&active?`<label class="target-label">Website photo to replace<select data-target="${view}"><option value="">Choose photo number above</option><option value="add" ${job?.targets?.[view]==='add'?'selected':''}>Add as a new photo</option>${selected.images.map((image,n)=>`<option value="${esc(image.id)}" ${job?.targets?.[view]===image.id?'selected':''}>Photo ${n+1}</option>`).join('')}</select></label>`:''}</div>`;}
  function resultPanel(){
    const job=selected.review.job;if(!job)return '';const done=job.status==='published',ready=['ready','publish_failed'].includes(job.status),missing=outputOrder.filter(view=>!latest(selected.review.proposals,view));
    return `<section class="review-section job-panel"><h3>${esc(labels[job.status]||job.status)}</h3><p>${job.status==='queued'?'Saved to the queue. Tell Codex “Process the image queue” in the chat.':job.status==='processing'?'Codex is creating the requested views. Results will appear here.':done?'Approved photos were published. Other gallery images were retained.':ready?'Compare the five views below. Write changes under the exact photo, then choose Revise images.':'Publishing the approved views…'}</p>${job.error?`<p class="error">${esc(job.error)}</p>`:''}
      <div class="photo-grid result-grid">${outputOrder.map((view,index)=>{
        const original=latest(selected.review.originals,view),result=latest(selected.review.proposals,view);
        return `<div class="photo-slot"><h4>${index+1}. ${esc(viewNames[view])}</h4><div class="comparison ${original?'':'ai-only'}">${original?`<div><small>Your original</small><div class="slot-picture">${picture(original,'','',viewNames[view]+' · Your original',view)}</div></div>`:''}<div><small>AI result</small><div class="slot-picture">${result?picture(result,'',view,viewNames[view]+' · AI result'):'Waiting for this view'}</div></div></div>${ready?`<label class="photo-feedback">Changes to this photo<textarea data-view-feedback="${view}" rows="2" placeholder="Leave empty if this photo is correct">${esc(revisionDrafts[view]||'')}</textarea></label>`:''}</div>`;
      }).join('')}</div>${ready?`${missing.length?'<p class="section-intro">This earlier job has missing views. Create them to complete the five-photo set.</p>':canPublish?'<button class="primary" id="publishJob">Approve & publish</button>':'<p>Ready for the Owner to approve and publish.</p>'}<button id="reviseJob">${missing.length?'Create missing views / revise images':'Revise images'}</button><p class="section-intro">Only photos with comments are revised; the other results stay. Missing views are created automatically when you submit.</p>`:''}</section>`;
  }
  function open(item){for(const key of Object.keys(revisionDrafts))delete revisionDrafts[key];selected=item;renderReview();if(!$('reviewDialog').open)$('reviewDialog').showModal();}
  function renderReview(){const i=selected;$('reviewTitle').textContent=i.title;$('reviewMeta').textContent=[i.colour,i.variants.map(v=>v.sku).join(', '),labels[i.status]].join(' · ');
    $('reviewBody').innerHTML=`<section class="review-section"><h3>Current website gallery</h3><p class="section-intro">All current gallery images are shown. Check the selected colour in every view.</p><div class="gallery">${i.images.map((m,n)=>'<figure>'+picture(m)+'<figcaption>Photo '+(n+1)+'</figcaption></figure>').join('')||'<p>No website images.</p>'}</div></section><section class="review-section"><h3>Upload photos for the views that need fixing</h3><p class="section-intro">Upload a genuine photo of this exact garment and colour only where a correction is needed. Front, Back and Detail are optional: one photo is enough to start. These three originals guide all five finished views: model front, detail, 45°, model back and product-only. No extra raw photos are needed for 45° or product-only. Each upload saves automatically.</p><div class="photo-grid">${['front','back','detail'].map(v=>slot(v)).join('')}</div></section>${resultPanel()}<section class="review-section"><details id="recreatePanel"><summary>Recreate all images</summary><p>Create all five views using a correct product-only photo, or upload original photos above. Existing website photos stay until you approve and publish.</p><label>Reference photo<select id="recreateSource"><option value="">Use uploaded original photos</option>${i.images.map((m,n)=>`<option value="${esc(m.id)}">Website photo ${n+1}</option>`).join('')}</select></label><p id="recreatePreview"></p><label class="reference-confirm"><input type="checkbox" id="referenceConfirmed"> I checked that the reference shows the correct garment and colour.</label><p><button id="recreateQueue">Queue all five images</button></p></details></section><label class="notes">Review notes<textarea id="notes" rows="3" placeholder="For example: wrong collar, pocket shape or sold components">${esc(i.review.notes)}</textarea></label><p id="reviewMessage" class="review-message" role="status"></p><div class="decisions"><button class="primary" id="queueJob">Create images</button><button data-decision="correct">All website images correct</button><button data-decision="needs_correction">Needs correction / save notes</button><button data-decision="unreviewed">Return to not reviewed</button></div>`;
    const recreateBlocked=()=>busy||['queued','processing','publishing','publish_failed'].includes(selected.review.job?.status);
    $('recreateQueue').disabled=recreateBlocked();
    $('recreateSource').addEventListener('change',()=>{const image=i.images.find(m=>m.id===$('recreateSource').value);$('recreatePreview').innerHTML=image?picture(image):'Upload Front, Back or Detail above if no original photos have been saved.';$('referenceConfirmed').checked=false;wirePictures($('recreatePreview'));});
    $('recreateQueue').addEventListener('click',()=>{if(recreateBlocked())return;if(!$('referenceConfirmed').checked){$('reviewMessage').textContent='Check the reference confirmation first.';return;}const sourceImageId=$('recreateSource').value;if(!sourceImageId&&!i.review.originals.length){$('reviewMessage').textContent='Select a correct website photo or upload an original photo above.';return;}mutate('/queue',json({...base(),recreateAll:true,fullSet:true,sourceImageId}));});
    if(i.review.job){$('queueJob').hidden=true;$('reviewBody').querySelectorAll('[data-decision],[data-target]').forEach(e=>{if(e.dataset.decision)e.hidden=true;else e.disabled=true;});}
    $('queueJob').disabled=!!i.review.job&&['queued','processing','publishing','publish_failed'].includes(i.review.job.status)||!i.review.originals.length||i.review.pendingViews?.length===0;
    $('queueJob').addEventListener('click',()=>{
      const targets={},selects=[...$('reviewBody').querySelectorAll('[data-target]')],used=new Set();
      for(const select of selects){
        select.removeAttribute('aria-invalid');
        const view=select.dataset.target;
        if(!select.value||select.value!=='add'&&used.has(select.value)){
          select.setAttribute('aria-invalid','true');
          const text=!select.value?`Choose a website photo to replace for ${view}, or choose “Add as a new photo”.`:'Each view needs a different website photo. Choose another photo or “Add as a new photo”.';
          $('reviewMessage').textContent=text;$('reviewMessage').classList.add('error');
          select.parentElement.querySelector('.target-error')?.remove();
          const hint=document.createElement('small');hint.className='target-error error';hint.textContent=text;select.after(hint);
          select.scrollIntoView({block:'center',behavior:'smooth'});select.focus({preventScroll:true});return;
        }
        targets[view]=select.value;if(select.value!=='add')used.add(select.value);
      }
      $('reviewMessage').textContent='Submitting to the image queue…';$('reviewMessage').classList.remove('error');
      mutate('/queue',json({...base(),targets,fullSet:true}));
    });
    $('reviewBody').querySelectorAll('[data-target]').forEach(select=>select.addEventListener('change',()=>{select.removeAttribute('aria-invalid');select.parentElement.querySelector('.target-error')?.remove();}));
    if($('publishJob'))$('publishJob').addEventListener('click',()=>mutate('/publish',json({...base(),jobId:selected.review.job.id})));
    if($('reviseJob'))$('reviseJob').addEventListener('click',()=>{const viewFeedback={};$('reviewBody').querySelectorAll('[data-view-feedback]').forEach(field=>{if(field.value.trim())viewFeedback[field.dataset.viewFeedback]=field.value.trim();});mutate('/revise',json({...base(),jobId:selected.review.job.id,viewFeedback,fullSet:true}));});
    $('reviewBody').querySelectorAll('[data-view-feedback]').forEach(field=>field.addEventListener('input',()=>revisionDrafts[field.dataset.viewFeedback]=field.value));
    wirePictures($('reviewBody'));$('reviewBody').querySelectorAll('[data-decision]').forEach(b=>b.addEventListener('click',()=>decision(b.dataset.decision)));$('reviewBody').querySelectorAll('input[type=file]').forEach(input=>input.addEventListener('change',()=>upload(input)));

  }
  function setBusy(value){busy=value;$('reviewBody').querySelectorAll('button,input,textarea').forEach(e=>e.disabled=value);$('closeReview').disabled=value;if(!value&&selected?.review.job)$('reviewBody').querySelectorAll('[data-target]').forEach(e=>e.disabled=true);}
  async function mutate(path,options){if(busy)return;setBusy(true);try{const d=await api('/items/'+encodeURIComponent(selected.id)+path,options);selected=d.item;if(path==='/revise')for(const key of Object.keys(revisionDrafts))delete revisionDrafts[key];renderReview();$('reviewMessage').textContent=selected.review.job?.status==='published'?'Published and verified in Shopify.':path==='/revise'?'Corrections saved to the queue. Tell Codex “Process the image queue”.':path==='/queue'?'Added to the queue. Tell Codex “Process the image queue” to start generation.':'Saved.';}catch(e){if(e.item){selected=e.item;renderReview();}$('reviewMessage').textContent=e.message;$('reviewMessage').classList.add('error');}finally{setBusy(false);if($('recreateQueue'))$('recreateQueue').disabled=['queued','processing','publishing','publish_failed'].includes(selected.review.job?.status);const j=selected.review.job;if($('queueJob'))$('queueJob').disabled=!!j&&['queued','processing','publishing','publish_failed'].includes(j.status)||!selected.review.originals.length||selected.review.pendingViews?.length===0;}}
  const json=body=>({method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
  const base=()=>({revision:selected.review.revision,fingerprint:selected.fingerprint,notes:$('notes').value});
  function decision(value){mutate('',{method:'PATCH',headers:{'Content-Type':'application/json'},body:JSON.stringify({...base(),decision:value,notes:$('notes').value})});}
  function upload(input){const file=input.files[0];if(!file)return;const form=new FormData();form.append('image',file);form.append('kind',input.dataset.kind);form.append('view',input.dataset.view);form.append('revision',selected.review.revision);form.append('fingerprint',selected.fingerprint);form.append('notes',$('notes').value);mutate('/photos',{method:'POST',body:form});}
  $('closeReview').addEventListener('click',()=>{if(!busy){$('reviewDialog').close();load();}});$('reviewDialog').addEventListener('cancel',e=>{if(busy)e.preventDefault();else load();});$('closeImage').addEventListener('click',()=>$('imageDialog').close());
  for(const k of['collection','gender','category','status'])$(k).addEventListener('change',()=>{offset=0;bulkSelection.clear();load();});let timer;$('search').addEventListener('input',()=>{clearTimeout(timer);timer=setTimeout(()=>{offset=0;bulkSelection.clear();load();},250);});


  function updateBulk(){
    $('bulkActions').hidden=!canPublish;
    $('selectionCount').textContent=bulkSelection.size+' selected';
    $('approveSelected').disabled=!bulkSelection.size||bulkRunning||bulkPreparing;
    for(const id of ['selectPage','clearSelection','approveFiltered'])$(id).disabled=bulkRunning||bulkPreparing;
    $('approveFiltered').textContent=$('category').value?'Approve all ready in '+$('category').value:'Approve all ready in filters';
    $('products').querySelectorAll('[data-bulk-select]').forEach(e=>{e.checked=bulkSelection.has(e.dataset.bulkSelect);e.disabled=bulkRunning||bulkPreparing;});
  }
  async function prepareBulk(mode){
    if(!canPublish||bulkRunning||bulkPreparing)return;
    bulkPreparing=true;updateBulk();
    try{
      const scope=['collection','gender','category','search'].map(k=>$(k).value).filter(Boolean).join(' / ')||'All products';
      let candidates=[];
      if(mode==='selected'){
        for(const id of bulkSelection.keys()){const d=await api('/items/'+encodeURIComponent(id));candidates.push(d.item);}
      }else{
        const params=new URLSearchParams({limit:48,status:'ready'});
        for(const k of ['collection','gender','category','search'])if($(k).value)params.set(k,$(k).value);
        if($('status').value&&$('status').value!=='ready')throw Error('Choose Ready to review or All statuses before approving all filtered products.');
        let position=0,count=Infinity;
        while(position<count){params.set('offset',position);const d=await api('/catalogue?'+params);count=d.total;candidates.push(...d.items);if(!d.items.length)break;position+=d.items.length;}
      }
      bulkPlan=window.ImageReviewBulk.plan(candidates);
      if(!bulkPlan.length){message('No complete five-image sets are ready for approval in this selection.',true);return;}
      $('bulkScope').textContent=bulkPlan.length+' product colours · '+(mode==='selected'?'Your selected products':scope);
      $('bulkList').innerHTML=bulkPlan.map(i=>'<li>'+esc(i.title)+' · '+esc(i.colour)+' · '+esc(i.variants.map(v=>v.sku).join(', '))+'</li>').join('');
      $('bulkProgress').textContent='Review this list, then approve and publish.';
      $('confirmBulk').hidden=false;$('confirmBulk').disabled=false;$('confirmBulk').textContent='Approve & publish '+bulkPlan.length+' products';$('stopBulk').hidden=true;$('closeBulk').disabled=false;
      $('bulkDialog').showModal();
    }catch(e){message(e.message,true);}finally{bulkPreparing=false;updateBulk();}
  }
  $('selectPage').onclick=()=>{for(const i of items)if(bulkEligible(i))bulkSelection.set(i.id,i);updateBulk();};
  $('clearSelection').onclick=()=>{bulkSelection.clear();updateBulk();};
  $('approveSelected').onclick=()=>prepareBulk('selected');$('approveFiltered').onclick=()=>prepareBulk('filtered');
  $('closeBulk').onclick=()=>{if(!bulkRunning)$('bulkDialog').close();};
  $('bulkDialog').addEventListener('cancel',e=>{if(bulkRunning)e.preventDefault();});
  $('stopBulk').onclick=()=>{bulkStop=true;$('stopBulk').disabled=true;$('bulkProgress').textContent+=' Stopping after this product…';};
  $('confirmBulk').onclick=async()=>{
    if(!canPublish||bulkRunning)return;bulkRunning=true;bulkStop=false;updateBulk();$('confirmBulk').disabled=true;$('closeBulk').disabled=true;$('stopBulk').hidden=false;$('stopBulk').disabled=false;
    const results=await window.ImageReviewBulk.publish(bulkPlan,async i=>{
      return api('/items/'+encodeURIComponent(i.id)+'/publish',json({revision:i.review.revision,fingerprint:i.fingerprint,jobId:i.review.job.id,notes:i.review.notes||''}));
    },()=>bulkStop,(done,total,item,result)=>{
      if(result.ok)bulkSelection.delete(item.id);
      $('bulkProgress').textContent=done+' / '+total+' processed · '+item.title+' · '+(result.ok?'Published':result.error);
    });
    bulkRunning=false;$('closeBulk').disabled=false;$('stopBulk').hidden=true;$('confirmBulk').hidden=true;
    const passed=results.filter(r=>r.ok).length,failed=results.filter(r=>!r.ok);
    $('bulkProgress').textContent=passed+' published · '+failed.length+' failed · '+(bulkPlan.length-results.length)+' not processed.';
    if(failed.length)$('bulkList').innerHTML=failed.map(r=>'<li>'+esc(r.item.title)+' · '+esc(r.error)+'</li>').join('');
    await load();updateBulk();
  };

  if(new URLSearchParams(location.search).get('worker')==='1'){
    const panel=document.createElement('section');panel.id='workerPanel';panel.className='job-panel';document.querySelector('main').prepend(panel);
    async function workerLoad(){try{const d=await api('/queue');panel.innerHTML='<h2>Codex work queue</h2><p>Use this owner session to download references and return generated images. Publishing stays in the product review.</p><button id="workerRefresh">Refresh queue</button>'+d.items.map(i=>`<article class="photo-slot" data-job="${esc(i.id)}"><h3>${esc(i.title)} · ${esc(i.colour)}</h3><p>${esc(i.variants.map(v=>v.sku).join(', '))} · ${esc(labels[i.review.job.status])}</p><p>${esc(i.review.job.notes)} ${esc(i.review.job.feedback||'')}</p><button data-claim="${esc(i.id)}">Claim job</button> <a class="button" href="/api/image-review/items/${encodeURIComponent(i.id)}/packet">Download work packet</a>${i.review.job.status==='processing'?(i.review.job.generationViews||i.review.job.outputViews||i.review.job.references.map(ref=>ref.view)).map(view=>`<label>Return generated ${esc(view)}<input type="file" accept="image/png,image/jpeg,image/webp" data-result="${esc(i.id)}" data-view="${view}"></label>`).join('')+`<label>Visual QA notes<textarea data-qa="${esc(i.id)}" placeholder="Reference fidelity, colour, construction and view checks"></textarea></label><button data-complete="${esc(i.id)}">Send for owner review</button>`:''}</article>`).join('');$('workerRefresh').onclick=workerLoad;
      panel.querySelectorAll('[data-claim]').forEach(b=>b.onclick=async()=>{const i=d.items.find(i=>i.id===b.dataset.claim);try{await api('/items/'+i.id+'/claim',json({revision:i.review.revision,fingerprint:i.fingerprint,jobId:i.review.job.id}));await workerLoad();}catch(e){message(e.message,true);}});
      panel.querySelectorAll('[data-result]').forEach(input=>input.onchange=async()=>{const i=d.items.find(i=>i.id===input.dataset.result),file=input.files[0];if(!file)return;const form=new FormData();for(const [k,v] of Object.entries({kind:'proposal',view:input.dataset.view,revision:i.review.revision,fingerprint:i.fingerprint,jobId:i.review.job.id,lease:i.review.job.lease}))form.append(k,v);form.append('image',file);try{await api('/items/'+i.id+'/photos',{method:'POST',body:form});await workerLoad();}catch(e){message(e.message,true);}});
      panel.querySelectorAll('[data-complete]').forEach(b=>b.onclick=async()=>{const i=d.items.find(i=>i.id===b.dataset.complete);try{await api('/items/'+i.id+'/complete',json({revision:i.review.revision,fingerprint:i.fingerprint,jobId:i.review.job.id,lease:i.review.job.lease,qa:panel.querySelector('[data-qa="'+i.id+'"]').value}));await workerLoad();}catch(e){message(e.message,true);}});
    }catch(e){panel.textContent=e.message;}}workerLoad();
  }
  setInterval(async()=>{if(!selected||!$('reviewDialog').open||busy||!['queued','processing'].includes(selected.review.job?.status))return;try{const d=await api('/items/'+encodeURIComponent(selected.id));if(d.item.review.revision!==selected.review.revision&&!$('notes')?.matches(':focus')){selected=d.item;renderReview();}}catch{}},8000);
  $('previous').addEventListener('click',()=>{offset=Math.max(0,offset-24);load();});$('next').addEventListener('click',()=>{offset+=24;load();});$('sync').addEventListener('click',async()=>{if(busy)return;$('sync').disabled=true;message('Refreshing current Shopify galleries…');try{await api('/sync',{method:'POST'});offset=0;await load();}catch(e){message(e.message,true);}finally{$('sync').disabled=false;}});load();
})();

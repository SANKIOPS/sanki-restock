'use strict';
window.MetaLinkedWorkspace=(()=>{
  const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const filters={destination:'',collection:'',creative:''};
  const options=(values,selected)=>'<option value="">All</option>'+values.map(v=>'<option '+(v===selected?'selected':'')+' value="'+esc(v)+'">'+esc(v)+'</option>').join('');
  const safeUrl=v=>{try{const u=new URL(v);return ['https:','http:'].includes(u.protocol)?u.href:'';}catch{return '';}};
  function creative(ad){
    let source={};try{source=JSON.parse(ad.setupSource||'{}');}catch{}
    const link=(ad.mediaUrls||[]).map(safeUrl).find(Boolean)||'';
    const thumb=Object.entries(source).filter(([k])=>/thumbnail|image.*url|image.*link/i.test(k)).map(([,v])=>safeUrl(v)).find(Boolean)||(ad.mediaUrls||[]).find(v=>/\.(png|jpe?g|webp)(\?|$)/i.test(v)&&safeUrl(v))||'';
    return link?'<a class="lw-creative-link" href="'+esc(link)+'" target="_blank" rel="noopener noreferrer">'+(thumb?'<img loading="lazy" referrerpolicy="no-referrer" src="'+esc(thumb)+'" alt="Creative thumbnail">':'<span class="lw-no-thumbnail">Preview<br>thumbnail unavailable</span>')+'<small>Open creative ↗</small></a>':'<small>URL not supplied</small>';
  }
  const locations=set=>(set.locations||[]).map(l=>esc(l.name)+(l.radiusKm==null?'':' +'+esc(l.radiusKm)+' km')).join('<br>')||'Not supplied';
  const budget=set=>set.budgetDaily==null?'Not supplied':'₹'+Number(set.budgetDaily).toLocaleString('en-IN',{maximumFractionDigits:2})+'<small>Ad set / day</small>';
  function render(root,state,callbacks){
    const rows=state.ads.map(ad=>({ad,set:state.adSets.find(s=>s.id===ad.adSetId)})).map(r=>({...r,campaign:state.campaigns.find(c=>c.id===r.set?.campaignId)})).filter(r=>r.set&&r.campaign&&!r.ad.archived&&!r.set.archived&&!r.campaign.archived);
    const types=[...new Set(['Aesthetic','Vocal','UGC','Other',...state.ads.flatMap(a=>a.creativeTypes||[])])];
    root.innerHTML='<div class="linked-filters"><label>Destination<select id="lwDestination">'+options(['Instagram profile','WhatsApp','Google Maps','Website','Messenger','Other'],filters.destination)+'</select></label><label>Collection<select id="lwCollection">'+options(['Femme','Homme','Studio','Both','Other','Unclassified'],filters.collection)+'</select></label><label>Creative type<select id="lwCreative">'+options(types.concat('Unclassified'),filters.creative)+'</select></label><button id="lwClear">Clear filters</button></div><div id="lwRows"></div><details id="lwPerformance" '+(state.insights?.date?'open':'')+'><summary>Performance and next actions</summary><div id="linkedPerformance"></div></details>';
    let editing=null, saving=false;
    function update(){
      const visible=rows.filter(r=>(!filters.destination||r.set.destination===filters.destination)&&(!filters.collection||(r.ad.collection||r.set.collection||'Unclassified')===filters.collection)&&(!filters.creative||(filters.creative==='Unclassified'?!(r.ad.creativeTypes||[]).length:(r.ad.creativeTypes||[]).includes(filters.creative))));
      root.querySelector('#lwRows').innerHTML='<div class="table-wrap"><table><thead><tr><th>On / off · Edit</th><th>Campaign</th><th>Ad set</th><th>Ad / creative</th><th>Destination</th><th>Collection</th><th>Creative type</th><th>Budget</th><th>Creative URL</th><th>Locations</th><th>Interests</th><th>Age / gender</th></tr></thead><tbody>'+visible.map(r=>'<tr><td><input type="checkbox" data-select-campaign="'+esc(r.campaign.id)+'" aria-label="Select '+esc(r.campaign.name)+'" '+(callbacks.isSelected?.(r.campaign.id)?'checked':'')+'><span class="state-switch '+(['active','reported_running'].includes(r.campaign.status)?'on':'')+'" role="img" aria-label="Campaign status: '+esc(r.campaign.status)+'"></span><button class="lw-pencil" data-tag-ad="'+esc(r.ad.id)+'" aria-label="Edit row for '+esc(r.ad.name)+'">✎</button>'+(editing===r.ad.id?'<div class="lw-edit-actions"><button data-open-campaign="'+esc(r.campaign.id)+'">Campaign</button><button data-edit-set="'+esc(r.set.id)+'">Ad set</button><button data-open-ad="'+esc(r.ad.id)+'">Creative</button><button data-save-tags>Save tags</button><button data-cancel-tags>Cancel</button><small id="lwInlineError" role="alert"></small></div>':'')+'</td><td><button class="row-title" data-open-campaign="'+esc(r.campaign.id)+'">'+esc(r.campaign.name)+'</button><small>'+esc(r.campaign.businessGoal||r.campaign.objective||'Objective not supplied')+'</small></td><td><button class="row-title" data-open-set="'+esc(r.set.id)+'">'+esc(r.set.name)+'</button></td><td><button class="row-title" data-open-ad="'+esc(r.ad.id)+'">'+esc(r.ad.name)+'</button><small>'+esc(r.ad.format||'Type not supplied')+'</small></td><td>'+esc(r.set.destination||'Unconfirmed')+'<small>'+esc(r.ad.whatsappNumber||r.set.whatsappNumber||'')+'</small></td><td>'+(editing===r.ad.id?'<select aria-label="Collection for '+esc(r.ad.name)+'" id="lwInlineCollection"><option value="">Unclassified</option>'+['Femme','Homme','Studio','Both','Other'].map(v=>'<option '+(v===(r.ad.collection||r.set.collection)?'selected':'')+'>'+v+'</option>').join('')+'</select>':esc(r.ad.collection||r.set.collection||'Unclassified'))+'</td><td>'+(editing===r.ad.id?'<select aria-label="Creative type for '+esc(r.ad.name)+'" id="lwInlineTypes"><option value="">Unclassified</option>'+types.map(v=>'<option '+((r.ad.creativeTypes||[]).includes(v)?'selected':'')+'>'+esc(v)+'</option>').join('')+'</select><input id="lwInlineCustom" aria-label="Custom creative type" placeholder="Add custom type">':esc((r.ad.creativeTypes||[]).join(', ')||'Unclassified'))+'</td><td>'+budget(r.set)+'</td><td>'+creative(r.ad)+'</td><td>'+locations(r.set)+'</td><td>'+esc((r.set.interests||[]).join(', ')||(r.set.interestsConfirmed?'None':'Not supplied'))+((r.set.behaviors||[]).length?'<small>Behaviours: '+esc(r.set.behaviors.join(', '))+'</small>':'')+'</td><td>'+esc(r.set.ageMin==null||r.set.ageMax==null?'Not supplied':r.set.ageMin+'–'+r.set.ageMax)+'<small>'+esc(r.set.gender||'Gender not supplied')+'</small></td></tr>').join('')+'</tbody></table></div>'+(!visible.length?'<p class="report-note">No matching ads. Clear filters, upload setup or tag your creatives.</p>':'');
      const ids=new Set(visible.map(r=>r.ad.id));
      window.MetaInsightView.render(root.querySelector('#linkedPerformance'),{...state,insights:{...state.insights,scoped:true,reportCreatives:state.insights?.creatives||[],creatives:(state.insights?.creatives||[]).filter(r=>ids.has(r.adId))}},callbacks);
      for(const key of ['collection','creative'])root.querySelector('#linkedPerformance [data-filter="'+key+'"]').closest('label').hidden=true;
    }
    for(const [id,key] of [['lwDestination','destination'],['lwCollection','collection'],['lwCreative','creative']])root.querySelector('#'+id).addEventListener('change',e=>{filters[key]=e.target.value;update();});
    root.querySelector('#lwClear').addEventListener('click',()=>{Object.keys(filters).forEach(k=>filters[k]='');render(root,state,callbacks);});
    root.querySelector('#lwRows').addEventListener('change',e=>{if(e.target.dataset.selectCampaign)callbacks.selectCampaign?.(e.target.dataset.selectCampaign,e.target.checked);});
    root.querySelector('#lwRows').addEventListener('click',e=>{
      const b=e.target.closest('button');if(!b)return;
      if(b.dataset.openCampaign)callbacks.edit('campaigns',b.dataset.openCampaign);
      if(b.dataset.editSet)callbacks.edit('adSets',b.dataset.editSet);
      if(b.dataset.openSet)callbacks.details(b.dataset.openSet);
      if(b.dataset.openAd)callbacks.edit('ads',b.dataset.openAd);
      if(b.dataset.tagAd){editing=b.dataset.tagAd;update();}
      if(b.hasAttribute('data-cancel-tags')&&!saving){editing=null;update();}
      if(b.hasAttribute('data-save-tags')&&!saving){
        const fields={collection:root.querySelector('#lwInlineCollection').value,creativeTypes:[...new Set([...root.querySelector('#lwInlineTypes').selectedOptions].map(o=>o.value).filter(Boolean).concat(root.querySelector('#lwInlineCustom').value.trim()||[]))]};
        saving=true;b.disabled=true;
        callbacks.saveTags(editing,fields).catch(error=>{root.querySelector('#lwInlineError').textContent=error.message;b.disabled=false;}).finally(()=>{saving=false;});
      }
    });
    update();
  }
  return {render};
})();

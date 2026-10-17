'use strict';
window.MetaLinkedWorkspace=(()=>{
  const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const expanded=new Set();
  let dateMode='saved';
  const filters={destination:'',collection:'',creative:''};
  const options=(values,selected)=>'<option value="">All</option>'+values.map(v=>'<option '+(v===selected?'selected':'')+' value="'+esc(v)+'">'+esc(v)+'</option>').join('');
  const safeUrl=v=>{try{const u=new URL(v);return ['https:','http:'].includes(u.protocol)?u.href:'';}catch{return '';}};
  function creative(ad){
    let source={};try{source=JSON.parse(ad.setupSource||'{}');}catch{}
    const link=(ad.mediaUrls||[]).map(safeUrl).find(Boolean)||'';
    const thumb=Object.entries(source).filter(([k])=>/thumbnail|image.*url|image.*link/i.test(k)).map(([,v])=>safeUrl(v)).find(Boolean)||(ad.mediaUrls||[]).find(v=>/\.(png|jpe?g|webp)(\?|$)/i.test(v)&&safeUrl(v))||'';
    return link?'<a class="lw-creative-link" href="'+esc(link)+'" target="_blank" rel="noopener noreferrer">'+(thumb?'<img loading="lazy" referrerpolicy="no-referrer" src="'+esc(thumb)+'" alt="Creative thumbnail">':'')+'<small>Open creative ↗</small></a>':'<small>URL not supplied</small>';
  }
  const shortList=values=>{const list=[...new Set(values.filter(Boolean))];if(!list.length)return 'Not supplied';return esc(list[0])+(list.length>1?'<details class="lw-short-list"><summary>Show all ('+list.length+')</summary><p>'+list.map(esc).join('<br>')+'</p></details>':'');};
  const locations=set=>shortList((set.locations||[]).map(l=>l.name+(l.radiusKm==null?'':' +'+l.radiusKm+' km')));
  const budget=set=>set.budgetDaily==null?'Not supplied':'₹'+Number(set.budgetDaily).toLocaleString('en-IN',{maximumFractionDigits:2})+'<small>Ad set / day</small>';
  function statusFor(...records){
    if(records.some(r=>r?.archived||['archived','paused'].includes(r?.status)))return {on:false,label:'Off'};
    if(records.every(r=>r&&['active','reported_running'].includes(r.status)))return {on:true,label:'On'};
    return {on:false,label:'Unconfirmed'};
  }
  const switchFor=(...records)=>{const status=statusFor(...records);return '<span class="state-switch '+(status.on?'on':'')+'" role="img" title="'+esc(status.label)+' · saved export status" aria-label="Row status: '+esc(status.label)+'"></span><small>'+esc(status.label)+'</small>';};
  const setCounts=(campaign,sets)=>{const counts={On:0,Off:0,Unconfirmed:0};for(const set of sets)counts[statusFor(campaign,set).label]++;return sets.length+' ad sets<small>'+counts.On+' on · '+counts.Off+' off'+(counts.Unconfirmed?' · '+counts.Unconfirmed+' unconfirmed':'')+'</small>';};
  function render(root,state,callbacks){
    const rows=state.ads.map(ad=>({ad,set:state.adSets.find(s=>s.id===ad.adSetId)})).map(r=>({...r,campaign:state.campaigns.find(c=>c.id===r.set?.campaignId)})).filter(r=>r.set&&r.campaign&&!r.ad.archived&&!r.set.archived&&!r.campaign.archived);
    const types=[...new Set(['Aesthetic','Vocal','UGC','Other',...state.ads.flatMap(a=>a.creativeTypes||[])])];
    root.innerHTML='<div class="linked-filters"><label>Destination<select id="lwDestination">'+options(['Instagram profile','WhatsApp','Google Maps','Website','Messenger','Other'],filters.destination)+'</select></label><label>Collection<select id="lwCollection">'+options(['Femme','Homme','Studio','Both','Other','Unclassified'],filters.collection)+'</select></label><label>Creative type<select id="lwCreative">'+options(types.concat('Unclassified'),filters.creative)+'</select></label><button id="lwClear">Clear filters</button></div><div id="lwMetricsControls"></div><div id="lwRows"></div>';
    let editing=null, saving=false;
    const report=state.insights||{},items=report.creatives||[],periods=report.periods||[];
    const goals=[...new Set(items.map(r=>r.goal))];
    root.querySelector('#lwMetricsControls').innerHTML='<label>Reporting period<select id="lwReportPeriod" '+(!periods.length?'disabled':'')+'>'+(!periods.length?'<option>No performance report saved</option>':'')+(report.selectedPeriod&&!periods.some(p=>p.key===report.selectedPeriod)?'<option value="'+esc(report.selectedPeriod)+'" selected>'+esc(report.date)+'</option>':'')+periods.map(p=>'<option value="'+esc(p.key)+'" '+(p.key===report.selectedPeriod?'selected':'')+'>'+esc(p.start+' – '+p.end)+'</option>').join('')+'</select></label><details><summary>Cost targets</summary>'+goals.map(g=>'<label>'+esc(g)+' (₹)<input type="number" min="0" step="0.01" data-cost-goal="'+esc(g)+'" value="'+esc(MetaInsightView.getTarget(g))+'" placeholder="Set target"></label>').join('')+'</details><button id="lwViewReport">View report</button><dialog id="lwReportDialog"><button id="lwCloseReport">Close</button><h2>Uploaded performance · '+esc(report.date||'No report')+'</h2>'+items.map(r=>'<p>'+esc(r.ad)+' · '+esc(r.goal)+' · '+(r.count==null?'Results unavailable':esc(r.count)+' results')+' · '+(r.spend==null?'Spend unavailable':'₹'+esc(r.spend))+'</p>').join('')+'</dialog>';
    const dateControls=document.createElement('div');dateControls.className='lw-date-range';
    const presets=[['saved','Uploaded period'],['1','Yesterday'],['2','Last 2 days'],['3','Last 3 days'],['5','Last 5 days'],['7','Last 7 days'],['30','Last 30 days'],['custom','Single day / custom range']];
    dateControls.innerHTML='<label>Date view<select id="lwDateMode">'+presets.map(([v,l])=>'<option value="'+v+'" '+(v===dateMode?'selected':'')+'>'+l+'</option>').join('')+'</select></label><label>From<input type="date" id="lwRangeStart" value="'+esc(report.periodStart||'')+'"></label><label>To<input type="date" id="lwRangeEnd" value="'+esc(report.periodEnd||'')+'"></label><button id="lwApplyRange">Apply dates</button><span class="muted">Last N days ends yesterday. For one day, set From and To to the same date.</span><p class="lw-coverage" role="status">'+esc(report.coverageNote||'Upload performance to see results.')+'</p>';
    root.querySelector('#lwMetricsControls').prepend(dateControls);
    const rangeStart=root.querySelector('#lwRangeStart'),rangeEnd=root.querySelector('#lwRangeEnd');
    root.querySelector('#lwDateMode').addEventListener('change',e=>{dateMode=e.target.value;if(dateMode==='saved')return;const days=Number(dateMode);if(days){const today=new Date().toLocaleDateString('sv-SE',{timeZone:'Asia/Kolkata'}),end=new Date(Date.parse(today)-86400000).toISOString().slice(0,10),start=new Date(Date.parse(end)-(days-1)*86400000).toISOString().slice(0,10);callbacks.period(start+'~'+end);}});
    root.querySelector('#lwApplyRange').addEventListener('click',()=>{if(!rangeStart.value||!rangeEnd.value||rangeStart.value>rangeEnd.value||Date.parse(rangeEnd.value)-Date.parse(rangeStart.value)>365*86400000){dateControls.querySelector('.lw-coverage').textContent='Choose valid From and To dates, up to 366 days.';return;}dateMode='custom';callbacks.period(rangeStart.value+'~'+rangeEnd.value);});
    root.querySelector('#lwReportPeriod').addEventListener('change',e=>{dateMode='saved';callbacks.period(e.target.value);});
    root.querySelector('#lwViewReport').disabled=!items.length;
    root.querySelector('#lwViewReport').addEventListener('click',()=>root.querySelector('#lwReportDialog').showModal());
    root.querySelector('#lwCloseReport').addEventListener('click',()=>root.querySelector('#lwReportDialog').close());
    root.querySelectorAll('[data-cost-goal]').forEach(input=>input.addEventListener('change',e=>{MetaInsightView.setTarget(e.target.dataset.costGoal,e.target.value);update();}));
    const money=v=>v==null?'Unavailable':'₹'+Number(v).toLocaleString('en-IN',{maximumFractionDigits:2});
    function performance(ads){
      const ids=new Set(ads.map(a=>a.id)),matches=items.filter(r=>ids.has(r.adId));
      if(!matches.length)return '<td>No coverage</td><td>Unavailable</td><td>Upload report for selected dates</td>';
      const groups=new Map();for(const r of matches){const k=r.goal+'|'+r.attribution;if(!groups.has(k))groups.set(k,[]);groups.get(k).push(r);}
      const metrics=[...groups.values()].map(rs=>{const first=rs[0],sum=k=>rs.every(r=>r[k]!=null)?rs.reduce((n,r)=>n+r[k],0):null,count=rs.length>1&&first.metric==='reach'?null:sum('count'),spend=sum('spend');return {...first,grouped:rs.length>1,count,spend,cost:count>0&&spend!=null?spend/count*first.costFactor:null,impressions:sum('impressions'),previousCost:rs.length>1?null:first.previousCost};});
      const spend=matches.every(r=>r.spend!=null)?matches.reduce((n,r)=>n+r.spend,0):null;
      return '<td>'+metrics.map(r=>(r.count==null?'Unavailable':Number(r.count).toLocaleString('en-IN'))+'<small>'+esc(r.goal)+'</small>').join('<hr>')+(matches.length<ads.length?'<small>Partial report: '+matches.length+'/'+ads.length+' ads</small>':'')+'</td><td>'+money(spend)+'<small>Spend</small>'+metrics.map(r=>money(r.cost)+'<small>'+esc(r.goal)+(r.costFactor===1000?' / 1,000 reached':' / result')+'</small>').join('<hr>')+'</td><td>'+metrics.map(r=>{const a=MetaInsightView.assess(r),peers=items.filter(p=>p.goal===r.goal&&p.attribution===r.attribution&&p.cost!=null&&p.count>=5),best=!r.grouped&&r.count>=5&&peers.length>1&&r.cost===Math.min(...peers.map(p=>p.cost));return '<strong>'+esc(a.name)+'</strong><small>'+esc(a.action)+'</small>'+(best?'<small class="lw-cost-rank">Lowest cost · same goal</small>':'')+'<details><summary>Why?</summary>'+esc(a.reason)+'<small>Different audiences can affect comparisons. Profile visits and enquiries do not prove store footfall.</small></details>';}).join('<hr>')+'</td>';
    }

    function update(){
      const visible=rows.filter(r=>(!filters.destination||r.set.destination===filters.destination)&&(!filters.collection||(r.ad.collection||r.set.collection||'Unclassified')===filters.collection)&&(!filters.creative||(filters.creative==='Unclassified'?!(r.ad.creativeTypes||[]).length:(r.ad.creativeTypes||[]).includes(filters.creative))));
      root.querySelector('#lwRows').innerHTML='<div class="table-wrap"><table><thead><tr><th>Status<br>Edit</th><th>Campaign</th><th>Ad set</th><th>Ad / creative</th><th>Destination</th><th>Collection</th><th>Creative type</th><th>Model name</th><th>Budget</th><th>Creative URL</th><th>Locations</th><th>Interests</th><th>Age / gender</th><th>Results</th><th>Spend<br>Cost / result</th><th>Assessment</th></tr></thead><tbody>'+visible.map(r=>'<tr><td><input type="checkbox" data-select-campaign="'+esc(r.campaign.id)+'" aria-label="Select '+esc(r.campaign.name)+'" '+(callbacks.isSelected?.(r.campaign.id)?'checked':'')+'>'+switchFor(r.campaign,r.set,r.ad)+'<button class="lw-pencil" data-tag-ad="'+esc(r.ad.id)+'" aria-label="Edit row for '+esc(r.ad.name)+'">✎</button>'+(editing===r.ad.id?'<div class="lw-edit-actions"><button data-open-campaign="'+esc(r.campaign.id)+'">Campaign</button><button data-edit-set="'+esc(r.set.id)+'">Ad set</button><button data-open-ad="'+esc(r.ad.id)+'">Creative</button><button data-save-tags>Save</button><button data-cancel-tags>Cancel</button><small id="lwInlineError" role="alert"></small></div>':'')+'</td><td><button class="row-title" data-open-campaign="'+esc(r.campaign.id)+'">'+esc(r.campaign.name)+'</button><small>'+esc(r.campaign.businessGoal||r.campaign.objective||'Objective not supplied')+'</small></td><td><button class="row-title" data-open-set="'+esc(r.set.id)+'">'+esc(r.set.name)+'</button><small>Ad set: '+esc(statusFor(r.campaign,r.set).label)+'</small></td><td><button class="row-title" data-open-ad="'+esc(r.ad.id)+'">'+esc(r.ad.name)+'</button><small>'+esc(r.ad.format||'Type not supplied')+'</small></td><td>'+esc(r.set.destination||'Unconfirmed')+'<small>'+esc(r.ad.whatsappNumber||r.set.whatsappNumber||'')+'</small></td><td>'+(editing===r.ad.id?'<select aria-label="Collection for '+esc(r.ad.name)+'" id="lwInlineCollection"><option value="">Unclassified</option>'+['Femme','Homme','Studio','Both','Other'].map(v=>'<option '+(v===(r.ad.collection||r.set.collection)?'selected':'')+'>'+v+'</option>').join('')+'</select>':esc(r.ad.collection||r.set.collection||'Unclassified'))+'</td><td>'+(editing===r.ad.id?'<select aria-label="Creative type for '+esc(r.ad.name)+'" id="lwInlineTypes"><option value="">Unclassified</option>'+types.map(v=>'<option '+((r.ad.creativeTypes||[]).includes(v)?'selected':'')+'>'+esc(v)+'</option>').join('')+'</select><input id="lwInlineCustom" aria-label="Custom creative type" placeholder="Add custom type">':esc((r.ad.creativeTypes||[]).join(', ')||'Unclassified'))+'</td><td>'+(editing===r.ad.id?'<input id="lwInlineModel" aria-label="Model name" maxlength="160" placeholder="Model name" value="'+esc(r.ad.model||'')+'">':esc(r.ad.model||'Not supplied'))+'</td><td>'+budget(r.set)+'</td><td>'+creative(r.ad)+'</td><td>'+locations(r.set)+'</td><td>'+shortList(r.set.interests||[])+((r.set.behaviors||[]).length?'<small>Behaviours: '+shortList(r.set.behaviors)+'</small>':'')+'</td><td>'+esc(r.set.ageMin==null||r.set.ageMax==null?'Not supplied':r.set.ageMin+'–'+r.set.ageMax)+'<small>'+esc(r.set.gender||'Gender not supplied')+'</small></td>'+performance([r.ad])+'</tr>').join('')+'</tbody></table></div>'+(!visible.length?'<p class="report-note">No matching ads. Clear filters, upload setup or tag your creatives.</p>':'');
      if(!state.showLinkedDetails){
        const unique=values=>[...new Set(values.filter(Boolean))].join(', ')||'Not supplied';
        const summaries=state.campaigns.filter(c=>!c.archived&&!expanded.has(c.id)).map(c=>{
          const sets=state.adSets.filter(s=>s.campaignId===c.id&&!s.archived);
          const ads=visible.filter(r=>r.campaign.id===c.id).map(r=>r.ad);
          const known=sets.filter(s=>s.budgetDaily!=null);
          return '<tr><td><input type="checkbox" data-select-campaign="'+esc(c.id)+'" aria-label="Select '+esc(c.name)+'" '+(callbacks.isSelected?.(c.id)?'checked':'')+'><div class="lw-status-edit"><span class="state-switch '+(['active','reported_running'].includes(c.status)?'on':'')+'" role="img" aria-label="Campaign status: '+esc(c.status)+'"></span><button data-open-campaign="'+esc(c.id)+'" aria-label="Edit campaign '+esc(c.name)+'">✎</button></div></td><td><button class="row-title" data-expand-campaign="'+esc(c.id)+'">'+esc(c.name)+'</button><small>'+esc(c.businessGoal||c.objective||'Objective not supplied')+'</small></td><td><button class="row-title" data-expand-campaign="'+esc(c.id)+'">'+setCounts(c,sets)+'</button></td><td><button class="row-title" data-expand-campaign="'+esc(c.id)+'">'+ads.length+' creatives</button></td><td>'+esc(unique(sets.map(s=>s.destination)))+'</td><td>'+esc(unique(ads.map(a=>a.collection||sets.find(s=>s.id===a.adSetId)?.collection)))+'</td><td>'+esc(unique(ads.flatMap(a=>a.creativeTypes||[])))+'</td><td>'+esc(unique(ads.map(a=>a.model)))+'</td><td>'+(known.length?'₹'+known.reduce((n,s)=>n+s.budgetDaily,0).toLocaleString('en-IN'):'Not supplied')+'<small>Total ad-set / day'+(known.length<sets.length?' · some missing':'')+'</small></td><td><button class="row-title" data-expand-campaign="'+esc(c.id)+'">View '+ads.length+' creatives ↗</button></td><td>'+shortList(sets.flatMap(s=>(s.locations||[]).map(l=>l.name+(l.radiusKm==null?'':' +'+l.radiusKm+' km'))))+'</td><td>'+shortList(sets.flatMap(s=>s.interests||[]))+'</td><td>'+esc(unique(sets.map(s=>s.ageMin==null||s.ageMax==null?'':s.ageMin+'–'+s.ageMax)))+'<small>'+esc(unique(sets.map(s=>s.gender)))+'</small></td>'+performance(ads)+'</tr>';
        });
        const body=root.querySelector('#lwRows tbody');
        [...body.children].forEach((tr,i)=>{if(!expanded.has(visible[i].campaign.id))tr.remove();});
        body.insertAdjacentHTML('afterbegin',summaries.join(''));
        if(expanded.size)root.querySelector('#lwRows').insertAdjacentHTML('afterbegin','<button id="lwCollapse">Back to campaign summary</button>');
      }

    }
    for(const [id,key] of [['lwDestination','destination'],['lwCollection','collection'],['lwCreative','creative']])root.querySelector('#'+id).addEventListener('change',e=>{filters[key]=e.target.value;update();});
    root.querySelector('#lwClear').addEventListener('click',()=>{Object.keys(filters).forEach(k=>filters[k]='');render(root,state,callbacks);});
    root.querySelector('#lwRows').addEventListener('change',e=>{if(e.target.dataset.selectCampaign)callbacks.selectCampaign?.(e.target.dataset.selectCampaign,e.target.checked);});
    root.querySelector('#lwRows').addEventListener('click',e=>{
      const b=e.target.closest('button');if(!b)return;
      if(b.id==='lwCollapse'){expanded.clear();update();}
      if(b.dataset.expandCampaign){expanded.add(b.dataset.expandCampaign);update();}
      if(b.dataset.openCampaign)callbacks.edit('campaigns',b.dataset.openCampaign);
      if(b.dataset.editSet)callbacks.edit('adSets',b.dataset.editSet);
      if(b.dataset.openSet)callbacks.details(b.dataset.openSet);
      if(b.dataset.openAd)callbacks.edit('ads',b.dataset.openAd);
      if(b.dataset.tagAd){editing=b.dataset.tagAd;update();}
      if(b.hasAttribute('data-cancel-tags')&&!saving){editing=null;update();}
      if(b.hasAttribute('data-save-tags')&&!saving){
        const fields={model:root.querySelector('#lwInlineModel').value.trim(),collection:root.querySelector('#lwInlineCollection').value,creativeTypes:[...new Set([...root.querySelector('#lwInlineTypes').selectedOptions].map(o=>o.value).filter(Boolean).concat(root.querySelector('#lwInlineCustom').value.trim()||[]))]};
        saving=true;b.disabled=true;
        callbacks.saveTags(editing,fields).catch(error=>{root.querySelector('#lwInlineError').textContent=error.message;b.disabled=false;}).finally(()=>{saving=false;});
      }
    });
    update();
  }
  return {render,statusFor};
})();

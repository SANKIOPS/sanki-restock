'use strict';
window.MetaLinkedWorkspace=(()=>{
  const esc=v=>String(v??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const scope={campaign:null,set:null};
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
  const switchFor=(...records)=>{const status=statusFor(...records);if(status.label==='Unconfirmed')return '<span class="status-unknown" role="status">Unconfirmed</span>';return '<span class="state-switch '+(status.on?'on':'')+'" role="img" title="'+esc(status.label)+' · saved app status, not live Meta" aria-label="Row status: '+esc(status.label)+'"></span><small>'+esc(status.label)+'</small>';};
  const setCounts=(campaign,sets)=>{const counts={On:0,Off:0,Unconfirmed:0};for(const set of sets)counts[statusFor(set).label]++;if(!sets.length)return 'Not supplied';return sets.length+' ad sets<small title="Ad-set switches, independent of the campaign switch">'+counts.On+' on · '+counts.Off+' off'+(counts.Unconfirmed?' · '+counts.Unconfirmed+' unconfirmed':'')+'</small>';};
  const canEdit=(level,owner)=>level===owner||(level==='ads'&&owner==='results');
  function render(root,state,callbacks){
    const rows=state.ads.map(ad=>({ad,set:state.adSets.find(s=>s.id===ad.adSetId)})).map(r=>({...r,campaign:state.campaigns.find(c=>c.id===r.set?.campaignId)})).filter(r=>r.set&&r.campaign&&!r.ad.archived&&!r.set.archived&&!r.campaign.archived);
    const types=[...new Set(['Aesthetic','Vocal','UGC','Other',...state.ads.flatMap(a=>a.creativeTypes||[])])];
    root.innerHTML='<div class="linked-filters"><label>Destination<select id="lwDestination">'+options(['Instagram profile','WhatsApp','Google Maps','Website','Messenger','Other'],filters.destination)+'</select></label><label>Collection<select id="lwCollection">'+options(['Femme','Homme','Studio','Both','Other','Unclassified'],filters.collection)+'</select></label><label>Creative type<select id="lwCreative">'+options(types.concat('Unclassified'),filters.creative)+'</select></label><button id="lwClear">Clear filters</button></div><div id="lwMetricsControls"></div><div id="lwRows"></div>';
    let editing=null, saving=false, copying=null;
    const report=state.insights||{},items=report.creatives||[],periods=report.periods||[];
    const goals=[...new Set(items.map(r=>r.goal))];
    root.querySelector('#lwMetricsControls').innerHTML='<label>Reporting period<select id="lwReportPeriod" '+(!periods.length?'disabled':'')+'>'+(!periods.length?'<option>No performance report saved</option>':'')+(report.selectedPeriod&&!periods.some(p=>p.key===report.selectedPeriod)?'<option value="'+esc(report.selectedPeriod)+'" selected>'+esc(report.date)+'</option>':'')+periods.map(p=>'<option value="'+esc(p.key)+'" '+(p.key===report.selectedPeriod?'selected':'')+'>'+esc(p.start+' – '+p.end)+'</option>').join('')+'</select></label><details><summary>Cost targets</summary>'+goals.map(g=>'<label>'+esc(g)+' (₹)<input type="number" min="0" step="0.01" data-cost-goal="'+esc(g)+'" value="'+esc(MetaInsightView.getTarget(g))+'" placeholder="Set target"></label>').join('')+'</details><button id="lwViewReport">View report</button><dialog id="lwReportDialog"><button id="lwCloseReport">Close</button><h2>Uploaded performance · '+esc(report.date||'No report')+'</h2>'+items.map(r=>'<p>'+esc(r.ad)+' · '+esc(r.goal)+' · '+(r.count==null?'Results unavailable':esc(r.count)+' results')+' · '+(r.spend==null?'Spend unavailable':'₹'+esc(r.spend))+'</p>').join('')+'</dialog>';
    const dateControls=document.createElement('div');dateControls.className='lw-date-range';
    const presets=[['saved','Uploaded period'],['1','Yesterday'],['2','Last 2 days'],['3','Last 3 days'],['5','Last 5 days'],['7','Last 7 days'],['30','Last 30 days'],['180','Last 180 days (~6 months)'],['custom','Single day / custom range']];
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

    const unique=values=>[...new Set(values.filter(Boolean))].join(', ')||'Not supplied';
    const groupFields={
      campaignName:[['name','Campaign name'],['objective','Objective']],
      setName:[['name','Ad-set name'],['performanceGoal','Performance goal']],
      adName:[['name','Ad name'],['format','Format'],['primaryText','Primary text','area'],['cta','Call to action']],
      destination:[['destination','Destination','select',['Instagram profile','WhatsApp','Google Maps','Website','Messenger','Other']],['destinationRef','Destination URL'],['whatsappNumber','WhatsApp number']],
      collection:[['collection','Collection','select',['Femme','Homme','Studio','Both','Other']]],
      creativeTypes:[['creativeTypes','Creative types','list']],model:[['model','Model name']],
      budget:[['budgetDaily','Daily budget ₹','number']],mediaUrls:[['mediaUrls','Creative URLs','list']],
      locations:[['locations','Locations | radius km','locations']],
      interests:[['interests','Interests (empty = broad)','list'],['behaviors','Behaviours (empty = none)','list'],['placements','Placements (Advantage+ or list)','list']],
      age:[['ageMin','Minimum age','number'],['ageMax','Maximum age','number'],['gender','Gender','select',['all','women','men']]],
      assessment:[['assessment','Your assessment / next action','area']]
    };
    const refs=new Map();
    const record=(type,id)=>state[type].find(x=>x.id===id);
    function editor(ref){
      return '<div class="lw-cell-editor">'+ref.fields.map(([key,label,type,values])=>{
        let value=ref.record[key]??'';if(type==='list')value=(value||[]).join('\n');if(type==='locations')value=(value||[]).map(x=>x.name+' | '+(x.radiusKm??'')).join('\n');
        const attr=' data-inline-key="'+key+'" aria-label="'+esc(label)+'"';
        return '<label>'+esc(label)+(type==='select'?'<select'+attr+'><option value="">Not supplied</option>'+[...new Set([...(values||[]),value].filter(Boolean))].map(v=>'<option value="'+esc(v)+'" '+(value===v?'selected':'')+'>'+esc(v)+'</option>').join('')+'</select>':['list','locations','area'].includes(type)?'<textarea'+attr+' rows="3">'+esc(value)+'</textarea>':'<input'+attr+' type="'+(type==='number'?'number':'text')+'" '+(type==='number'?'min="0" step="any"':'')+' value="'+esc(value)+'">')+'</label>';
      }).join('')+(ref.result?'<small>Correction for '+esc(report.periodStart)+' – '+esc(report.periodEnd)+'. Saved in history; a later report may replace it.</small>':'')+'<button type="button" data-inline-save>Save</button><button type="button" data-inline-cancel>Cancel</button><small class="lw-inline-error" role="alert"></small></div>';
    }
    function editable(type,r,group,html,extra={}){
      if(!r)return html;const key=type+':'+r.id+':'+group;
      const ref={type,id:r.id,record:r,fields:groupFields[group],...extra};refs.set(key,ref);
      return editing===key?editor(ref):'<button class="lw-cell-value" type="button" data-inline-edit="'+esc(key)+'" title="Click to edit">'+(html||'Not supplied')+' <span class="lw-edit-mark">✎</span></button>';
    }
    function drill(id,label,setId){return '<button class="row-title" data-drill-campaign="'+esc(id)+'" '+(setId?'data-drill-set="'+esc(setId)+'"':'')+'>'+label+'</button>';}
    function statusCell(type,r){const label=statusFor(r).label;return '<button type="button" class="lw-toggle" data-toggle-type="'+type+'" data-toggle-id="'+esc(r.id)+'" aria-label="'+esc(label+' status for '+r.name)+'" title="Click to record '+(label==='On'?'Off':'On')+' in this app">'+switchFor(r)+'</button>';}
    function metricEditor(ads,field){
      if(ads.length!==1||!report.periodStart||!report.periodEnd)return '';
      const ad=ads[0],item=items.find(x=>x.adId===ad.id);if(!item)return '';
      const exact=state.results.find(r=>!r.archived&&r.adId===ad.id&&r.periodStart===report.periodStart&&r.periodEnd===report.periodEnd);
      const metric=item.metric||'reportedResults';
      const source=state.results.find(r=>r.adId===ad.id)||{};
      const r=exact||{id:'manual-'+ad.id+'-'+report.periodStart+'-'+report.periodEnd,adId:ad.id,adSetId:ad.adSetId,periodStart:report.periodStart,periodEnd:report.periodEnd,reportGoal:source.reportGoal,reportDestination:source.reportDestination,resultIndicator:source.resultIndicator,attributionSetting:item.attribution,spend:item.spend,impressions:item.impressions,[metric]:item.count,source:'Manual table correction'};
      return editable('results',r,'figures-'+field,field==='spend'?'Edit spend':'Edit results',{fields:field==='spend'?[['spend','Spend ₹','number']]:[[metric,item.goal||'Results','number']],result:true,isNew:!exact});
    }
    function perfCells(ads,row){
      const atAd=canEdit(row.kind,'results');
      const markup=performance(ads),tail=atAd?metricEditor(ads,'count'):(ads.length?drill(row.campaign.id,'View ad figures',row.set?.id):'');
      const manual=atAd&&ads.length===1?editable('ads',ads[0],'assessment',esc(ads[0].assessment||'Add your assessment')):ads.map(a=>a.assessment).filter(Boolean).map(esc).join('<br>');
      let index=0;return markup.replace(/<\/td>/g,()=>{index++;return (index===1?tail:index===2?(atAd?metricEditor(ads,'spend'):''):manual)+'</td>';});
    }
    function scopedRows(){
      if(scope.campaign&&!state.campaigns.some(c=>c.id===scope.campaign))scope.campaign=scope.set=null;
      if(scope.set&&!state.adSets.some(s=>s.id===scope.set&&s.campaignId===scope.campaign))scope.set=null;
      if(scope.campaign){
        const c=record('campaigns',scope.campaign);
        if(scope.set)return rows.filter(r=>r.campaign.id===c.id&&r.set.id===scope.set).map(r=>({...r,kind:'ads',sets:[r.set],ads:[r.ad]}));
        return state.adSets.filter(s=>!s.archived&&s.campaignId===c.id&&(!state.hasSetupFilters||rows.some(r=>r.set.id===s.id))).map(set=>({kind:'adSets',campaign:c,set,sets:[set],ads:rows.filter(r=>r.set.id===set.id).map(r=>r.ad)}));
      }
      if(state.showLinkedDetails)return rows.map(r=>({...r,kind:'ads',sets:[r.set],ads:[r.ad]}));
      return state.campaigns.filter(c=>!c.archived).map(c=>({kind:'campaigns',campaign:c,sets:state.adSets.filter(s=>!s.archived&&s.campaignId===c.id),ads:rows.filter(r=>r.campaign.id===c.id).map(r=>r.ad)}));
    }
    function update(){
      refs.clear();const displayed=scopedRows();
      const c=scope.campaign&&record('campaigns',scope.campaign),set=scope.set&&record('adSets',scope.set);
      root.querySelector('#lwRows').innerHTML='<nav class="lw-breadcrumb"><button data-back-campaigns>All campaigns</button>'+(c?' <span>›</span> <button data-drill-campaign="'+esc(c.id)+'">'+esc(c.name)+'</button>':'')+(set?' <span>›</span> '+esc(set.name):'')+'<small>'+displayed.length+' '+(scope.set?'ads':c?'ad sets':state.showLinkedDetails?'matching ads':'campaigns')+' · '+(scope.set||state.showLinkedDetails?'Edit creative fields here; audience settings belong to the ad set.':c?'Edit audience settings here; open an ad set to edit its creatives.':'Campaign summaries: open a campaign to edit its ad sets, then an ad set to edit its ads.')+' Switches update app records only.</small></nav><div class="lw-inline-status" role="status"></div><div class="table-wrap"><table><thead><tr>'+['Status','Campaign','Ad set','Ad / creative','Destination','Collection','Creative type','Model name','Budget','Creative URL','Locations','Interests / placements','Age / gender','Results','Spend / cost','Assessment'].map(h=>'<th>'+h+'</th>').join('')+'</tr></thead><tbody>'+displayed.map(row=>{
        const {campaign,sets,ads}=row,s=row.set,a=row.ad|| (ads.length===1?ads[0]:null),own=row.kind==='campaigns'?campaign:row.kind==='adSets'?s:row.ad;
        const jump=label=>drill(campaign.id,label,s?.id);
        const setup=(group,value)=>canEdit(row.kind,'adSets')?editable('adSets',s,group,value):row.kind==='ads'?'<span title="Saved on this ad set; edit it in the ad-set view">'+value+'</span>':jump(value);
        const creativeField=(group,value)=>canEdit(row.kind,'ads')?editable('ads',a,group,value):jump(value);
        return '<tr><td><input type="checkbox" data-select-campaign="'+esc(campaign.id)+'" aria-label="Select '+esc(campaign.name)+'" '+(callbacks.isSelected?.(campaign.id)?'checked':'')+'>'+statusCell(row.kind,own)+'</td><td>'+drill(campaign.id,esc(campaign.name))+(canEdit(row.kind,'campaigns')?editable('campaigns',campaign,'campaignName','Rename / objective'):'')+'</td><td>'+(s?drill(campaign.id,esc(s.name),s.id)+(canEdit(row.kind,'adSets')?editable('adSets',s,'setName','Rename / goal')+'<button type="button" data-copy-settings="'+esc(s.id)+'">Copy settings</button>':''):drill(campaign.id,setCounts(campaign,sets)))+'</td><td>'+(row.ad?editable('ads',row.ad,'adName',esc(row.ad.name)):jump(ads.length+' creatives'))+'</td><td>'+setup('destination',esc(unique(sets.map(x=>x.destination)))+(s?.whatsappNumber?'<small>'+esc(s.whatsappNumber)+'</small>':''))+'</td><td>'+creativeField('collection',esc(unique(ads.map(x=>x.collection||s?.collection))))+'</td><td>'+creativeField('creativeTypes',esc(unique(ads.flatMap(x=>x.creativeTypes||[]))))+'</td><td>'+creativeField('model',esc(unique(ads.map(x=>x.model))))+'</td><td>'+setup('budget',s?budget(s):'₹'+sets.reduce((n,x)=>n+(x.budgetDaily||0),0).toLocaleString('en-IN')+'<small>Total ad-set / day</small>')+'</td><td>'+(canEdit(row.kind,'ads')?creative(a)+editable('ads',a,'mediaUrls','Edit URL'):jump('View '+ads.length+' creatives'))+'</td><td>'+setup('locations',esc(unique(sets.flatMap(x=>(x.locations||[]).map(l=>l.name+(l.radiusKm==null?'':' +'+l.radiusKm+' km'))))))+'</td><td>'+setup('interests',esc(unique(sets.flatMap(x=>x.interestsConfirmed?(x.interests?.length?x.interests:['Broad / no interests']):[])))+'<small>'+esc(unique(sets.flatMap(x=>x.placements||[])))+'</small>')+'</td><td>'+setup('age',esc(unique(sets.map(x=>x.ageMin==null||x.ageMax==null?'':x.ageMin+'–'+x.ageMax)))+'<small>'+esc(unique(sets.map(x=>x.gender)))+'</small>')+'</td>'+perfCells(ads,row)+'</tr>';
      }).join('')+'</tbody></table></div>'+(!displayed.length?'<p>No matching records in this campaign or ad set.</p>':'');
      if(copying){
        const source=record('adSets',copying),targets=state.adSets.filter(s=>!s.archived&&s.campaignId===source.campaignId&&s.id!==source.id);
        const panel=document.createElement('section');panel.className='lw-cell-editor';panel.innerHTML='<strong>Copy settings from '+esc(source.name)+'</strong><p>Choose settings and target ad sets. Selected values replace existing values. Blank source fields are skipped. Creative tags apply to all ads in the target sets; creative URLs remain separate.</p><div>'+[['destination','Destination / goal'],['budget','Budget'],['locations','Locations'],['interests','Interests / behaviours / placements'],['age','Age / gender'],['collection','Collection'],['creativeTypes','Creative type'],['model','Model']].map(([key,label])=>'<label><input type="checkbox" data-copy-group="'+key+'" checked> '+label+'</label>').join('')+'</div><strong>Copy to</strong>'+targets.map(t=>'<label><input type="checkbox" data-copy-target="'+esc(t.id)+'" checked> '+esc(t.name)+'</label>').join('')+(targets.length?'':'<p>No other ad sets in this campaign.</p>')+'<button type="button" data-copy-apply>Copy selected settings</button><button type="button" data-copy-cancel>Cancel</button><small class="lw-inline-error" role="alert"></small>';
        root.querySelector('#lwRows').prepend(panel);
      }
      callbacks.scope?.(c,set,displayed.length);
    }
    root.querySelector('#lwRows').addEventListener('change',e=>{if(e.target.dataset.selectCampaign)callbacks.selectCampaign?.(e.target.dataset.selectCampaign,e.target.checked);});
    root.querySelector('#lwRows').addEventListener('click',async e=>{
      const b=e.target.closest('button');if(!b||saving)return;
      if(b.dataset.copySettings){copying=b.dataset.copySettings;editing=null;update();root.querySelector('#lwRows section')?.scrollIntoView({block:'nearest',behavior:'smooth'});return;}
      if(b.hasAttribute('data-copy-cancel')){copying=null;update();return;}
      if(b.hasAttribute('data-copy-apply')){
        const panel=b.closest('section'),targets=[...panel.querySelectorAll('[data-copy-target]:checked')].map(x=>x.dataset.copyTarget),groups=[...panel.querySelectorAll('[data-copy-group]:checked')].map(x=>x.dataset.copyGroup);
        if(!targets.length||!groups.length){panel.querySelector('.lw-inline-error').textContent='Choose at least one target and setting.';return;}
        saving=true;b.disabled=true;try{await callbacks.copySettings({source:copying,targets,groups});copying=null;}catch(error){panel.querySelector('.lw-inline-error').textContent=error.message;}finally{saving=false;b.disabled=false;}return;
      }
      if(b.hasAttribute('data-back-campaigns')){scope.campaign=scope.set=null;editing=null;update();return;}
      if(b.dataset.drillCampaign){scope.campaign=b.dataset.drillCampaign;scope.set=b.dataset.drillSet||null;editing=null;update();return;}
      if(b.dataset.inlineEdit){editing=b.dataset.inlineEdit;update();return;}
      if(b.hasAttribute('data-inline-cancel')){editing=null;update();return;}
      if(b.dataset.toggleId){
        const r=record(b.dataset.toggleType,b.dataset.toggleId);saving=true;b.disabled=true;
        try{await callbacks.patch(b.dataset.toggleType,r.id,{status:statusFor(r).label==='On'?'paused':'reported_running'});}catch(error){root.querySelector('.lw-inline-status').textContent=error.message;}finally{saving=false;b.disabled=false;}return;
      }
      if(b.hasAttribute('data-inline-save')){
        const ref=refs.get(editing);if(!ref)return;const container=b.closest('.lw-cell-editor'),patch={};
        try{
          for(const [key,,type] of ref.fields){const value=container.querySelector('[data-inline-key="'+key+'"]').value.trim();
            if(type==='number'){if(value!==''&&(!Number.isFinite(Number(value))||Number(value)<0))throw new Error('Enter a valid non-negative number.');patch[key]=value===''?null:Number(value);}
            else if(type==='list')patch[key]=value.split('\n').map(x=>x.trim()).filter(Boolean);
            else if(type==='locations')patch[key]=value.split('\n').map(x=>x.trim()).filter(Boolean).map(line=>{const parts=line.split('|');if(parts.length>2)throw new Error('Use location | radius km.');const radius=(parts[1]||'').trim();if(radius!==''&&(!Number.isFinite(Number(radius))||Number(radius)<0||Number(radius)>1000))throw new Error('Use a radius from 0 to 1000 km.');return {name:parts[0].trim(),radiusKm:radius===''?null:Number(radius)};});
            else patch[key]=value||null;
          }
          if(Object.hasOwn(patch,'interests')){patch.interestsConfirmed=true;patch.behaviorsConfirmed=true;patch.placementsConfirmed=patch.placements.length>0;}
          saving=true;b.disabled=true;
          const save=ref.isNew?{...ref.record,...patch}:patch;
          await callbacks.patch(ref.type,ref.id,save,!!ref.isNew);editing=null;
        }catch(error){container.querySelector('.lw-inline-error').textContent=error.message;}
        finally{saving=false;b.disabled=false;}
      }
    });
    update();
  }
  return {render,statusFor,switchFor,setCounts,canEdit,resetScope:()=>{scope.campaign=scope.set=null;}};
})();

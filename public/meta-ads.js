'use strict';
(() => {
  const $ = id => document.getElementById(id);
  const apiBase = '/api/marketing/meta';
  let setupRevision=null, setupFile=null;
  async function uploadSetup(preview) {
    const file=$('setupFile').files[0];
    if(!state||!file)return message('Choose a configuration file after records load.',true);
    if(!preview && (file!==setupFile||setupRevision===null))return message('Preview this file first.',true);
    $('setupPreview').disabled=true;$('setupSave').disabled=true;
    try {
      const body=new FormData();body.append('file',file);body.append('preview',String(preview));body.append('revision',String(preview?state.revision:setupRevision));
      const response=await fetch(apiBase+'/import-setup',{method:'POST',credentials:'same-origin',body});const data=await response.json();
      if(!response.ok||!data.success)throw new Error(data.error||'Upload failed.');
      if(preview){setupRevision=data.revision;setupFile=file;$('setupSummary').textContent=`${data.records.campaigns.length} campaign(s), ${data.records.adSets.length} ad set(s), ${data.records.ads.length} ad(s).\n${data.counts.created} new; ${data.counts.updated} updated; ${data.counts.unchanged} unchanged.\n`+data.records.campaigns.map(x=>x.name).join('\n')+'\n'+data.warnings.join('\n');$('setupSave').hidden=false;}
      else {$('setupSave').hidden=true;setupRevision=null;$('setupSummary').textContent='Setup saved. Daily performance upload is the next step.';await load('Campaign setup imported.');}
    }catch(error){setupRevision=null;$('setupSave').hidden=true;$('setupSummary').textContent=error.message;}
    finally{$('setupPreview').disabled=false;$('setupSave').disabled=false;}
  }
  $('setupPreview').addEventListener('click',()=>uploadSetup(true));
  $('setupSave').addEventListener('click',()=>uploadSetup(false));
  $('setupFile').addEventListener('change',()=>{setupRevision=null;setupFile=null;$('setupSave').hidden=true;$('setupSummary').textContent='';});
  let performanceRevision=null,performanceFile=null,insightsPeriod="";
  async function uploadPerformance(preview){
    const file=$('performanceFile').files[0];if(!state||!file)return message('Choose a performance file.',true);
    if(!preview&&(file!==performanceFile||performanceRevision===null))return message('Preview this file first.',true);
    $('performancePreview').disabled=true;$('performanceSave').disabled=true;
    try{const body=new FormData();body.append('file',file);body.append('preview',String(preview));body.append('revision',String(preview?state.revision:performanceRevision));
      const response=await fetch(apiBase+'/import-performance',{method:'POST',credentials:'same-origin',body});const data=await response.json();if(!response.ok||!data.success)throw new Error(data.error||'Performance upload failed.');
      if(preview){performanceRevision=data.revision;performanceFile=file;$('performanceSummary').textContent=`${data.report.length} ad/period rows: ${data.counts.created} new, ${data.counts.updated} updated, ${data.counts.unchanged} unchanged.\n`+data.report.map(r=>`${r.periodStart} – ${r.periodEnd} · ${r.adSet} · ${r.ad} · ${r.spend==null?'spend unavailable':'₹'+r.spend} · ${r.action}`).join('\n');$('performanceSave').hidden=false;}
      else{performanceRevision=null;$('performanceSave').hidden=true;$('performancePanel').hidden=true;$('performanceSummary').textContent='Daily performance saved. See Campaigns for findings and next actions.';insightsPeriod="";view='campaigns';await load('Daily performance imported.');}
    }catch(e){performanceRevision=null;$('performanceSave').hidden=true;$('performanceSummary').textContent=e.message;}finally{$('performancePreview').disabled=false;$('performanceSave').disabled=false;}
  }
  $('performancePreview').addEventListener('click',()=>uploadPerformance(true));$('performanceSave').addEventListener('click',()=>uploadPerformance(false));
  $('performanceFile').addEventListener('change',()=>{performanceRevision=null;performanceFile=null;$('performanceSave').hidden=true;$('performanceSummary').textContent='';});
  $('openPerformance').addEventListener('click',()=>{$('performancePanel').hidden=!$('performancePanel').hidden;});
  $('insightsPanel').addEventListener('change',event=>{if(event.target.id==='insightPeriodSelect'){insightsPeriod=event.target.value;load('Reporting period selected.');}});
  const paths = { campaigns: 'campaigns', adSets: 'adsets', ads: 'ads', results: 'results' };
  const labels = { campaigns: 'Campaigns', adSets: 'Ad sets', ads: 'Ads', results: 'Results', changes: 'Changes',insights:'Insights' };
  const singular = { campaigns: 'campaign', adSets: 'ad set', ads: 'ad', results: 'result' };
  const statusLabels = { draft: 'Draft', reported_running: 'Reported running', delivery_unconfirmed: 'Reported · verify delivery', active: 'Verified active', paused: 'Paused', completed: 'Completed', archived: 'Archived', unknown: 'Unconfirmed', pending: 'Pending' };
  const statuses = Object.entries(statusLabels);
  let state = null, view = 'campaigns', busy = false, editing = null, sort = null;
  const selection={campaigns:new Set(),adSets:new Set(),ads:new Set(),results:new Set(),changes:new Set()};
  const esc = value => String(value == null ? '' : value).replace(/[&<>"']/g, c => ({ '&':'&amp;', '<':'&lt;', '>':'&gt;', '"':'&quot;', "'":'&#39;' }[c]));
  const missing = value => value === null || value === undefined || value === '';
  const pending = () => '<span class="pending">Pending</span>';
  const display = value => missing(value) ? pending() : esc(value);
  const integer = value => missing(value) ? pending() : Number(value).toLocaleString('en-IN');
  const money = value => missing(value) ? pending() : Number(value).toLocaleString('en-IN', { style:'currency', currency:'INR', maximumFractionDigits:2 });
  const ratio = (numerator, denominator, format) => missing(numerator) || missing(denominator) ? pending() : denominator === 0 ? '<span class="cell-note">—</span>' : format(numerator / denominator);
  const fixed = value => value.toFixed(2);
  const dateDisplay = value => missing(value) ? 'Pending' : new Date(value.length === 10 ? value + 'T00:00:00' : value).toLocaleDateString('en-IN', {day:'2-digit',month:'short',year:'numeric'});
  const badge = record => '<span class="badge ' + esc(record.archived ? 'archived' : record.status) + '">' + esc(statusLabels[record.archived ? 'archived' : record.status] || record.status || 'Pending') + '</span>';
  const note = value => value ? '<span class="cell-note">' + esc(value) + '</span>' : '';
  const find = (type, id) => state[type].find(row => row.id === id);
  const adSetFor = (type, row) => type === 'adSets' ? row : type === 'ads' || type === 'results' ? find('adSets', row.adSetId) : null;
  const campaignFor = (type, row) => type === 'campaigns' ? row : find('campaigns', (adSetFor(type, row) || {}).campaignId);
  const shortId = value => value.length > 18 ? value.slice(0, 18) + '…' : value;
  const nameCell = (row, name, subtitle) => '<button class="row-title" '+(['campaigns','adSets'].includes(view)?'data-drill':'data-edit')+'="' + esc(row.id) + '">' + esc(name || row.name || row.id) + '</button>' + note(subtitle);
  const lines = values => values.length ? '<div class="list-lines">' + values.map(esc).join('<br>') + '</div>' : pending();
  const locationText = row => (row.locations || []).map(x => x.name + (missing(x.radiusKm) ? ' · radius pending' : ' +' + x.radiusKm + ' km'));
  const goalText=value=>({REACH:'Maximise reach',PROFILE_VISIT:'Instagram profile visits',LINK_CLICKS:'Maximise link clicks',CONVERSATIONS:'Messaging conversations'}[value]||value);
  const setPhones=row=>[...new Set([row.whatsappNumber,...state.ads.filter(a=>a.adSetId===row.id&&!a.archived).map(a=>a.whatsappNumber)].filter(Boolean))];
  const genderText = value => ({women:'Women', men:'Men', all:'All genders'}[value] || value);
  const campaignSets = row => state.adSets.filter(x => x.campaignId === row.id && !x.archived);
  const setBudget = rows => rows.reduce((sum, row) => sum + (row.budgetDaily || 0), 0);
  const captions = {
    campaigns:'One row per campaign. Configured budget is the sum of its current ad-set budgets.',
    adSets:'Select an ad-set name to view its ads. Use Edit record for settings.',
    ads:'One row per ad. Keep its audience and reporting linked to the correct ad set.',
    results:'One row per ad set or creative, for a stated date range. Enter 0 only when confirmed.',
    changes:'Saved field changes, with the person, time and reason. History cannot be edited.'
  };
  const col = (key, label, render, number = false, value) => ({key,label,render,number,value:value || (row => row[key])});
  function columns() {
    if($('columnMode').value==='overview' && ['campaigns','adSets','ads'].includes(view)) {
      const delivery=col('status','Delivery',r=>'<span class="delivery-dot '+esc(r.status)+'"></span>'+badge(r));
      const action=col('action','Action',r=>'<button class="table-edit" data-edit="'+esc(r.id)+'">Edit record</button>');
      if(view==='campaigns')return [col('name','Campaign',r=>nameCell(r)),delivery,action,col('objective','Objective',r=>display(r.objective)),col('budgetDaily','Budget',r=>money(setBudget(campaignSets(r)))+note('Daily · ad-set budgets'),true,r=>setBudget(campaignSets(r))),col('adSets','Ad sets',r=>integer(campaignSets(r).length),true,r=>campaignSets(r).length)];
      if(view==='adSets')return [col('name','Ad set',r=>nameCell(r)),delivery,action,col('performanceGoal','Performance goal',r=>display(r.performanceGoal)),col('budgetDaily','Budget',r=>money(r.budgetDaily)+note('Daily'),true),col('destination','Destination',r=>display(r.destination)),col('locations','Audience locations',r=>r.locations.length?display(locationText(r)[0])+note(r.locations.length>1?'+'+(r.locations.length-1)+' more locations · Full setup':''):pending()),col('ageMin','Age / gender',r=>display(missing(r.ageMin)||missing(r.ageMax)?null:r.ageMin+'–'+r.ageMax)+note(genderText(r.gender)))];
      return [col('name','Ad',r=>nameCell(r)),delivery,action,col('adSetId','Ad set',r=>display((adSetFor(view,r)||{}).name)),col('format','Format',r=>display(r.format)),col('primaryText','Ad text',r=>display(r.primaryText)),col('cta','Call to action',r=>display(r.cta)),col('media','Creative',r=>r.mediaUrls.length?r.mediaUrls.map((url,i)=>'<a class="creative-link" href="'+esc(url)+'" target="_blank" rel="noopener noreferrer">Preview '+(i+1)+'</a>').join(' '):pending())];
    }
    if (view === 'campaigns') return [
      col('name','Campaign', r => nameCell(r)),
      col('objective','Objective',r => display(r.objective)),
      col('businessGoal','Business goal',r => esc(r.businessGoal||r.objective||'Not supplied in export')),
      col('adSets','Ad sets',r => integer(campaignSets(r).length),true,r=>campaignSets(r).length),
      col('budgetDaily','Configured / day',r => money(setBudget(campaignSets(r))) + note(campaignSets(r).some(s=>missing(s.budgetDaily)) ? 'Some budgets pending' : 'Ad-set budgets'),true,r=>setBudget(campaignSets(r))),
      col('status','Status',badge),
      col('adAccountId','Ad account',r => display(r.adAccountId)),
      col('notes','Source / notes',r => display(r.notes))
    ];
    if (view === 'adSets') return [
      col('name','Ad set',r => nameCell(r,r.name,r.collection)),
      col('setup','Full details',r=>'<button class="table-edit" data-setup="'+esc(r.id)+'">View setup</button>'),
      col('campaign','Campaign',r => display((campaignFor(view,r)||{}).name),false,r=>(campaignFor(view,r)||{}).name),
      col('performanceGoal','Purpose / optimisation',r => display(goalText(r.performanceGoal)) + note(r.conversionLocation || r.destination || 'Conversion location pending')),
      col('budgetDaily','Configured / day',r => money(r.budgetDaily),true),
      col('destination','Destination / WhatsApp number',r => display(r.destination) + (setPhones(r).length?lines(setPhones(r)):r.destination==='WhatsApp'?note('Number not supplied in saved export'):note(r.instagramProfile ? '@'+r.instagramProfile.replace(/^@/,'') : ''))),
      col('locations','Locations',r => lines(locationText(r)),false,r=>locationText(r).join(' ')),
      col('ageMin','Age / gender',r => (missing(r.ageMin)||missing(r.ageMax) ? pending() : esc(r.ageMin + '–' + r.ageMax)) + note(genderText(r.gender)) + note(r.audienceMode || 'Controls vs suggestions pending')),
      col('interests','Interests',r => r.interestsConfirmed ? (r.interests.length ? lines(r.interests) : '<span class="cell-note">No interests selected</span>') : pending(),false,r=>r.interests.join(' ')),
      col('behaviors','Behaviours',r=>r.behaviorsConfirmed?((r.behaviors||[]).length?lines(r.behaviors):note('No behaviours selected')):pending(),false,r=>(r.behaviors||[]).join(' ')),
      col('facebookPage','Identities',r => display(r.facebookPage) + note(r.instagramProfile ? 'Instagram: @'+r.instagramProfile.replace(/^@/,'') : 'Instagram identity pending')),
      col('frequencyCount','Frequency setting',r => missing(r.frequencyCount)||missing(r.frequencyDays) ? pending() : esc(r.frequencyCount + ' / ' + r.frequencyDays + ' day(s)') + note(r.frequencyMode || 'Cap vs target pending')),
      col('placements','Placements / language',r => (r.placementsConfirmed ? lines(r.placements) : pending()) + note(r.languages.length ? r.languages.join(', ') : 'Languages pending'),false,r=>r.placements.join(' ')),
      col('status','Status',badge),
      col('confirmedOn','Last reported',r => esc(dateDisplay(r.confirmedOn)))
    ];
    if (view === 'ads') return [
      col('name','Creative / ad',r => nameCell(r)),
      col('adSetId','Ad set',r => display((adSetFor(view,r)||{}).name),false,r=>(adSetFor(view,r)||{}).name),
      col('collection','Collection',r => display((adSetFor(view,r)||{}).collection),false,r=>(adSetFor(view,r)||{}).collection),
      col('model','Model',r => display(r.model)),
      col('description','Creative concept',r => display(r.description) + note(r.format)),
      col('mediaCount','Selected media',r => integer(r.mediaCount) + note(r.mediaUrls.length ? r.mediaUrls.length+' linked file(s)' : 'File links pending'),true),
      col('cta','Call to action',r => display(r.cta)),
      col('status','Status',badge),
      col('notes','Notes',r=>display(r.notes))
    ];
    if (view === 'results') return [
      col('periodStart','Reporting period',r=>nameCell(r,dateDisplay(r.periodStart)+' – '+dateDisplay(r.periodEnd),r.source || 'Source pending')),
      col('adSetId','Ad set / scope',r=>display((adSetFor(view,r)||{}).name)+note(r.adId ? (find('ads',r.adId)||{}).name : 'Whole ad set'),false,r=>(adSetFor(view,r)||{}).name),
      col('spend','Spend',r=>money(r.spend),true),
      col('impressions','Impressions',r=>integer(r.impressions),true),
      col('reach','Reach',r=>integer(r.reach),true),
      col('frequency','Frequency',r=>ratio(r.impressions,r.reach,fixed),true,r=>missing(r.impressions)||!r.reach?null:r.impressions/r.reach),
      col('profileVisits','Profile visits',r=>integer(r.profileVisits),true),
      col('cpv','₹ / profile visit',r=>ratio(r.spend,r.profileVisits,money),true,r=>missing(r.spend)||!r.profileVisits?null:r.spend/r.profileVisits),
      col('messagingStarts','Message starts',r=>integer(r.messagingStarts),true),
      col('cpmessages','₹ / message start',r=>ratio(r.spend,r.messagingStarts,money),true,r=>missing(r.spend)||!r.messagingStarts?null:r.spend/r.messagingStarts),
      col('qualifiedEnquiries','Qualified enquiries',r=>integer(r.qualifiedEnquiries),true),
      col('orders','Orders',r=>integer(r.orders),true),
      col('metaPurchases','Meta purchases',r=>integer(r.metaPurchases),true),
      col('metaPurchaseValue','Meta purchase value',r=>money(r.metaPurchaseValue),true),
      col('metaROAS','Meta ROAS',r=>ratio(r.metaPurchaseValue,r.spend,x=>fixed(x)+'×'),true,r=>missing(r.metaPurchaseValue)||!r.spend?null:r.metaPurchaseValue/r.spend),
      col('videoPlays3s','3-second video plays',r=>integer(r.videoPlays3s),true),
      col('thruPlays','ThruPlays',r=>integer(r.thruPlays),true),
      col('attributionSetting','Attribution',r=>display(r.attributionSetting)),
      col('revenue','Revenue',r=>money(r.revenue),true),
      col('roas','ROAS',r=>ratio(r.revenue,r.spend,x=>fixed(x)+'×'),true,r=>missing(r.revenue)||!r.spend?null:r.revenue/r.spend),
      col('storeVisits','Store visits',r=>integer(r.storeVisits),true),
      col('linkClicks','Link clicks',r=>integer(r.linkClicks),true),
      col('cpc','₹ / link click',r=>ratio(r.spend,r.linkClicks,money),true,r=>missing(r.spend)||!r.linkClicks?null:r.spend/r.linkClicks),
      col('ctr','Link CTR',r=>ratio(r.linkClicks,r.impressions,x=>fixed(x*100)+'%'),true,r=>missing(r.linkClicks)||!r.impressions?null:r.linkClicks/r.impressions),
      col('notes','Notes',r=>display(r.notes))
    ];
    return [
      col('at','Changed',r=>esc(new Date(r.at).toLocaleString('en-IN')) + note('Revision '+r.revision)),
      col('entityId','Record',r=>display(historyRecord(r).name || r.entityId)+note(r.entityType+' · '+r.entityId)),
      col('field','Field / action',r=>display(r.field)+note(r.action)),
      col('before','Previous value',r=>'<div class="history-value">'+historyValue(r.before)+'</div>'),
      col('after','New value',r=>'<div class="history-value">'+historyValue(r.after)+'</div>'),
      col('actor','Changed by',r=>display(r.actor)),
      col('reason','Reason',r=>display(r.reason))
    ];
  }
  function historyValue(value) {
    if (value === null || value === undefined) return pending();
    if (typeof value === 'object') return esc(JSON.stringify(value,null,2));
    return esc(String(value));
  }
  function historyType(row) { return ({campaign:'campaigns',adSet:'adSets',ad:'ads',result:'results'}[row.entityType] || row.entityType); }
  function historyRecord(row) { return (state[historyType(row)] || []).find(x=>x.id===row.entityId) || {}; }
  function isArchived(type,row) {
    return !!(row.archived || (adSetFor(type,row)||{}).archived || (campaignFor(type,row)||{}).archived);
  }
  function filteredRows() {
    const campaign = $('campaignFilter').value, collection = $('collectionFilter').value;
    const status = $('statusFilter').value, destination = $('destinationFilter').value;
    const query = $('search').value.toLowerCase().trim();
    const creative=$('creativeTypeFilter').value;
    const matchingAds=state.ads.filter(a=>!isArchived('ads',a)&&(!collection||(a.collection||adSetFor('ads',a)?.collection||'Unclassified')===collection)&&(!destination||adSetFor('ads',a)?.destination===destination)&&(!creative||(a.creativeTypes||[]).includes(creative)));
    const matchingSets=new Set(matchingAds.map(a=>a.adSetId));
    let rows = state[view].filter(row => {
      const type = view === 'changes' ? historyType(row) : view;
      const record = view === 'changes' ? historyRecord(row) : row;
      const set = adSetFor(type,record), cmp = campaignFor(type,record);
      if(view!=='campaigns' && selection.campaigns.size && (!cmp||!selection.campaigns.has(cmp.id)))return false;
      if(['ads','results'].includes(view) && selection.adSets.size && (!set||!selection.adSets.has(set.id)))return false;
      if((collection||destination||creative)&&!((type==='campaigns'?campaignSets(record).some(x=>matchingSets.has(x.id)):type==='adSets'?matchingSets.has(record.id):matchingAds.some(a=>a.id===(type==='ads'?record.id:record.adId)))))return false;
      if (campaign && (!cmp || cmp.id !== campaign)) return false;
      if (view !== 'changes') {
        if (status === 'archived' ? !isArchived(type,record) : isArchived(type,record)) return false;
        if (status && status !== 'archived' && record.status !== status && !(type === 'results' && set && set.status === status)) return false;
      }
      if (query && !JSON.stringify([row,cmp&&cmp.name,set&&set.name]).toLowerCase().includes(query)) return false;
      if (view === 'results' && (($('periodStartFilter').value && row.periodStart < $('periodStartFilter').value) || ($('periodEndFilter').value && row.periodEnd > $('periodEndFilter').value))) return false;
      return true;
    });
    if (view === 'changes') rows = rows.slice();
    if (sort) {
      const column = columns().find(c=>c.key===sort.key);
      if (column) rows.sort((a,b)=>{
        const x=column.value(a),y=column.value(b);
        if (missing(x)) return missing(y)?0:1;
        if (missing(y)) return -1;
        return (typeof x==='number'&&typeof y==='number' ? x-y : String(x).localeCompare(String(y),undefined,{numeric:true})) * sort.direction;
      });
    }
    return rows;
  }
  function render() {
    if (!state) return;
    const insightView=view==='insights';
    $('insightsPanel').hidden=!insightView;
    for(const selector of ['.manager-toolbar','.filters','.view-heading','.manager-search','.manager-views','#tableFooter','#stats'])document.querySelector(selector).hidden=insightView;
    $('records').parentElement.hidden=insightView;
    const creativeValue=$('creativeTypeFilter').value;
    $('creativeTypeFilter').innerHTML='<option value="">All creative types</option>'+[...new Set(['Aesthetic','Vocal','UGC','Other',...state.ads.flatMap(a=>a.creativeTypes||[])])].map(t=>'<option '+(t===creativeValue?'selected':'')+'>'+esc(t)+'</option>').join('');
    const s=state.summary;
    $('records').classList.toggle('full-setup',$('columnMode').value==='setup');
    for(const type of Object.keys(selection))for(const id of selection[type])if(!state[type].some(x=>x.id===id))selection[type].delete(id);
    $('pageTitle').textContent=labels[view];
    const nc=selection.campaigns.size,ns=selection.adSets.size;
    $('campaignTabLabel').textContent='▣ Campaigns'+(nc?' · '+nc+' selected':'');
    $('adSetTabLabel').textContent='▦ Ad sets'+(nc?' for '+nc+' campaign'+(nc>1?'s':''):'');
    $('adTabLabel').textContent='▤ Ads'+(ns?' for '+ns+' ad set'+(ns>1?'s':''):nc?' for '+nc+' campaign'+(nc>1?'s':''):'');
    $('scopeSummary').textContent=nc?[...selection.campaigns].map(id=>find('campaigns',id)?.name).join(', '):'All campaigns';
    $('editSelected').disabled=busy||view==='changes'||!selection[view].size;
    $('editSelected').textContent=selection[view].size>1?'Edit '+selection[view].size+' records':'Edit record';
    $('removeSelected').disabled=busy||view==='changes'||!selection[view].size;
    $('viewChildren').hidden=!['campaigns','adSets'].includes(view);
    $('viewChildren').disabled=busy||!selection[view].size;
    $('viewChildren').textContent=view==='campaigns'?'View ad sets':'View ads';
    $('columnMode').disabled=!['campaigns','adSets','ads'].includes(view);
    $('allRecords').classList.toggle('active',!$('statusFilter').value);
    $('runningRecords').classList.toggle('active',$('statusFilter').value==='reported_running');
    $('stats').innerHTML=[
      ['Configured / day',money(s.configuredDailyBudget),s.pendingBudgetCount ? s.pendingBudgetCount+' budget(s) pending' : 'All current ad-set budgets'],
      ['Campaigns',integer(s.campaignCount),'Marketing campaign records'],
      ['Ad sets / creatives',integer(s.adSetCount)+' / '+integer(s.adCount),'Audience and creative records'],
      ['Reporting periods',integer(s.resultCount),'Dated results logged']
    ].map(x=>'<article class="stat"><span class="stat-label">'+x[0]+'</span><strong class="stat-value">'+x[1]+'</strong><span class="stat-detail">'+esc(x[2])+'</span></article>').join('');
    document.querySelectorAll('[data-tab]').forEach(tab=>{
      tab.setAttribute('aria-selected',String(tab.dataset.tab===view));
      const type=tab.dataset.tab;
      if(type==='insights'){tab.querySelector('span').textContent=state.insights?.actions.length||0;return;}
      tab.querySelector('span').textContent=state[type].filter(row=>{const cmp=campaignFor(type,row),set=adSetFor(type,row);return (type==='changes'||($('statusFilter').value==='archived'?isArchived(type,row):!isArchived(type,row)))&&(type==='campaigns'||type==='changes'||!nc||cmp&&selection.campaigns.has(cmp.id))&&(type!=='ads'||!ns||set&&selection.adSets.has(set.id));}).length;
    });
    $('recordView').setAttribute('aria-labelledby','tab-'+view);
    $('viewTitle').textContent=labels[view];$('viewHelp').textContent=captions[view];
    $('resultFilters').hidden=view!=='results';
    $('add').hidden=view==='changes';$('add').textContent='+ '+(view==='results'?'Log result':'Add '+(singular[view]||'record'));
    const cols=columns(), rows=filteredRows();
    $('rowCount').textContent=rows.length+' row'+(rows.length===1?'':'s');
    const selectable=view!=='changes';
    $('records').classList.toggle('selectable',selectable);
    const head=selectable?'<th class="selection-cell"><input type="checkbox" id="selectAllRows" aria-label="Select all visible rows" '+(rows.length&&rows.every(r=>selection[view].has(r.id))?'checked':'')+'></th><th class="record-state">On / off</th>':'';
    $('records').querySelector('thead').innerHTML='<tr>'+head+cols.map(c=>'<th scope="col" class="'+(c.number?'number':'')+'" data-sort="'+esc(c.key)+'" tabindex="0" aria-sort="'+(sort&&sort.key===c.key?(sort.direction===1?'ascending':'descending'):'none')+'">'+esc(c.label)+(sort&&sort.key===c.key?(sort.direction===1?' ↑':' ↓'):' ↕')+'</th>').join('')+'</tr>';
    $('records').querySelector('tbody').innerHTML=rows.map(r=>'<tr class="'+(selection[view].has(r.id)?'selected-row':'')+'">'+(selectable?'<td class="selection-cell"><input type="checkbox" data-select="'+esc(r.id)+'" aria-label="Select '+esc(r.name||r.id)+'" '+(selection[view].has(r.id)?'checked':'')+'></td><td class="record-state"><span class="state-switch '+(['active','reported_running'].includes(r.status)&&!r.archived?'on':'')+'" title="Recorded status only; update it with Edit record" role="img" aria-label="Recorded status: '+esc(statusLabels[r.status]||'Unconfirmed')+'"></span></td>':'')+cols.map(c=>'<td class="'+(c.number?'number':'')+'">'+c.render(r)+'</td>').join('')+'</tr>').join('');
    $('tableFooter').textContent=rows.length+' '+labels[view].toLowerCase()+' · '+selection[view].size+' selected';
    $('empty').hidden=rows.length>0;
    $('empty').innerHTML='<strong>'+(view==='results'&&!state.results.length?'No results logged yet':view==='changes'&&!state.changes.length?'No changes recorded yet':'No matching records')+'</strong>'+(view==='results'?'Log a dated report to compare spend, visits, enquiries and orders.':view==='changes'?'Your initial setup is saved. Future edits will appear here.':'Adjust the filters or add a record.');
    $('export').disabled=rows.length===0||busy;
    $('add').disabled=busy;
    $('creativeTypeFilter').closest('label').hidden=!['campaigns','adSets','ads'].includes(view);
    $('insightsPanel').hidden=view!=='campaigns';
    if(view==='campaigns'&&window.MetaLinkedWorkspace){
      const campaignIds=new Set(rows.map(r=>r.id));
      const sets=state.adSets.filter(r=>campaignIds.has(r.campaignId));
      const ads=state.ads.filter(a=>sets.some(r=>r.id===a.adSetId)&&(!$('collectionFilter').value||(a.collection||sets.find(r=>r.id===a.adSetId)?.collection||'Unclassified')===$('collectionFilter').value)&&(!$('destinationFilter').value||sets.find(r=>r.id===a.adSetId)?.destination===$('destinationFilter').value)&&(!$('creativeTypeFilter').value||(a.creativeTypes||[]).includes($('creativeTypeFilter').value)));
      MetaLinkedWorkspace.render($('insightsPanel'),{...state,ads,adSets:sets},{period:value=>{insightsPeriod=value;load('Reporting period selected.');},classify:id=>{const old=view;view='ads';openEditor(id);view=old;},edit:(type,id)=>{const old=view;view=type;openEditor(id);view=old;},details:openSetupDetails,saveTags:async(id,fields)=>{await request('/ads/'+encodeURIComponent(id),'PATCH',{...fields,revision:state.revision,reason:'Classify creative'});await load('Creative tags saved.');}});
      $('insightsPanel').querySelector('.linked-filters').hidden=true;
    }
  }
  async function request(path='',method='GET',body) {
    const response=await fetch(apiBase+path,{method,credentials:'same-origin',headers:body?{'Content-Type':'application/json'}:{},body:body?JSON.stringify(body):undefined});
    let result;try{result=await response.json();}catch{throw new Error(response.status===401?'Sign in again to load marketing records.':'The server returned an unreadable response.');}
    if (!response.ok||result.success===false) {const error=new Error(result.error||result.message||'Unable to save the record.');error.status=response.status;throw error;}
    return result;
  }
  function message(text,error=false) {$('message').textContent=text;$('message').classList.toggle('error',error);}
  function setBusy(value) {busy=value;$('refresh').disabled=value;$('add').disabled=value;$('export').disabled=value;document.querySelectorAll('#recordForm button').forEach(b=>b.disabled=value || (!!editing && !!editing.conflicted && (b.type==='submit' || b.id==='archive')));}
  async function load(notice) {
    setBusy(true);
    try {
      state=await request(insightsPeriod?"?period="+encodeURIComponent(insightsPeriod):"");
      const selected=$('campaignFilter').value;
      $('campaignFilter').innerHTML='<option value="">All campaigns</option>'+state.campaigns.map(c=>'<option value="'+esc(c.id)+'">'+esc(c.name)+(c.archived?' (archived)':'')+'</option>').join('');
      $('campaignFilter').value=selected;
      const latestReport=state.adSets.map(x=>x.confirmedOn).filter(Boolean).sort().pop();
      message(notice || 'Owner’s records · last reported '+dateDisplay(latestReport)+' · revision '+state.revision);
      render();
    } catch(error){message(error.message,true);}finally{setBusy(false);if(state)render();}
  }
  const f=(key,label,type='text',extra={})=>({key,label,type,...extra});
  const section=label=>({section:label});
  function fields(type) {
    const common=[f('status','Delivery / record status','select',{options:statuses}),f('notes','Notes / evidence','textarea',{wide:true,max:5000})];
    if(type==='campaigns')return [
      f('name','Campaign name','text',{required:true,max:200,wide:true}),f('objective','Meta campaign objective','text',{max:120}),
      f('adAccountId','Meta ad account ID','text',{max:120}),f('businessGoal','Business goal','textarea',{wide:true,max:500,help:'Business ambition can differ from the selected optimization goal.'}),...common
    ];
    if(type==='adSets')return [
      f('campaignId','Campaign','select',{required:true,options:state.campaigns.filter(x=>!x.archived).map(x=>[x.id,x.name])}),
      f('name','Ad-set name','text',{required:true,max:200}),f('collection','Collection','select',{options:[['','Pending'],['Femme','Femme'],['Homme','Homme'],['Studio','Studio'],['Other','Other']]}),
      f('budgetDaily','Configured daily budget (₹)','number',{step:'0.01',help:'Includes paused records in the configured total; actual spend is logged separately.'}),
      f('conversionLocation','Conversion location','text',{max:160}),f('performanceGoal','Performance goal','text',{max:250}),
      section('Destination & identities'),
      f('destination','Destination','select',{options:[['','Pending'],['Instagram profile','Instagram profile'],['WhatsApp','WhatsApp'],['Google Maps','Google Maps'],['Website','Website'],['Messenger','Messenger'],['Other','Other']]}),
      f('destinationRef','Destination URL / reference','text',{max:2048}),f('facebookPage','Facebook Page','text',{max:200}),f('instagramProfile','Instagram profile','text',{max:160,placeholder:'sanki.femme'}),f('whatsappNumber','WhatsApp number','tel',{max:40,placeholder:'+91 95405 41212'}),
      section('Audience'),
      f('locations','Locations & radii','locations',{wide:true,help:'One location per line: Delhi | 17. Leave radius blank when unconfirmed.'}),
      f('ageMin','Minimum age','number',{min:13,max:100,step:'1'}),f('ageMax','Maximum age','number',{min:13,max:100,step:'1'}),
      f('gender','Gender','select',{options:[['','Pending'],['women','Women'],['men','Men'],['all','All genders']]}),
      f('audienceMode','Audience controls / suggestions','text',{max:160,help:'For example, strict controls or Advantage+ suggestions. Leave blank until checked.'}),
      f('interests','Detailed targeting interests','list',{wide:true,help:'One interest per line.'}),f('interestsConfirmed','Interest selections checked','check',{help:'Unchecked means unconfirmed, rather than no interests.'}),
      f('behaviors','Targeting behaviours','list',{wide:true,help:'One behaviour per line.'}),f('behaviorsConfirmed','Behaviour selections checked','check'),
      f('languages','Languages','list',{help:'One per line. Use All languages only when confirmed.'}),
      f('placements','Placements','list',{help:'One per line.'}),f('placementsConfirmed','Placements checked','check'),
      section('Frequency & verification'),
      f('frequencyCount','Frequency count','number',{step:'1'}),f('frequencyDays','Every N days','number',{step:'1'}),
      f('frequencyMode','Frequency setting type','select',{options:[['','Pending'],['cap','Cap'],['target','Target']]}),
      f('confirmedOn','Last reported / checked','date'),...common
    ];
    if(type==='ads')return [
      f('collection','Collection','select',{options:[['','Unconfirmed'],['Femme','Femme'],['Homme','Homme'],['Studio','Studio'],['Both','Both'],['Other','Other']]}),
      f('creativeTypes','Creative types','list',{wide:true,help:'One type per line. Examples: Aesthetic, Vocal, UGC, Store walkthrough. Add any type you need; confirm from the actual creative.'}),
      f('adSetId','Ad set','select',{required:true,wide:true,options:state.adSets.filter(x=>!isArchived('adSets',x)).map(x=>[x.id,x.name])}),
      f('name','Ad name','text',{required:true,max:200}),f('model','Model / creator','text',{max:160}),f('format','Creative format','text',{max:100}),
      f('mediaCount','Number of selected media assets','number',{step:'1',help:'Leave blank until confirmed in Meta.'}),
      f('description','Creative concept','textarea',{wide:true,max:5000}),f('mediaUrls','Video / creative file URLs','list',{wide:true,help:'One absolute http(s) link per line. File uploads are not sent to Meta.'}),
      f('cta','Call to action','text',{max:150}),f('primaryText','Primary ad text','textarea',{wide:true,max:10000}),...common
    ];
    return [
      f('adSetId','Ad set','select',{required:true,options:state.adSets.map(x=>[x.id,x.name])}),f('adId','Reporting scope','select',{options:[['','Whole ad set']]}),
      f('periodStart','Reporting period starts','date',{required:true}),f('periodEnd','Reporting period ends','date',{required:true}),
      f('source','Source / report','text',{max:200,wide:true,placeholder:'Meta Ads Manager, 9–11 Oct 2026'}),
      section('Delivery & actions'),
      f('spend','Amount spent (₹)','number',{step:'0.01'}),f('impressions','Impressions','number',{step:'1'}),f('reach','Reach','number',{step:'1'}),f('linkClicks','Link clicks','number',{step:'1'}),f('profileVisits','Instagram profile visits','number',{step:'1'}),f('messagingStarts','Messaging conversations started','number',{step:'1'}),
      section('Business outcomes'),
      f('qualifiedEnquiries','Qualified enquiries','number',{step:'1',help:'A useful buying enquiry, rather than only “hi”.'}),f('orders','Confirmed orders','number',{step:'1'}),f('revenue','Attributed revenue (₹)','number',{step:'0.01'}),f('storeVisits','Attributed store visits','number',{step:'1'}),
      f('notes','Attribution / report notes','textarea',{wide:true,max:5000,help:'Explain how orders, revenue and store visits were attributed. Unknown metrics stay blank.'})
    ];
  }
  function inputMarkup(field,value) {
    const id='field-'+field.key, attr=' id="'+id+'" name="'+field.key+'"'+(field.required?' required':'')+(field.max&&field.type!=='number'?' maxlength="'+field.max+'"':'');
    let control;
    if(field.type==='select'){
      let options=field.options.slice();
      if(!missing(value)&&!options.some(x=>String(x[0])===String(value)))options.push([value,value]);
      control='<select'+attr+'>'+options.map(x=>'<option value="'+esc(x[0])+'"'+(String(x[0])===String(value==null?'':value)?' selected':'')+'>'+esc(x[1])+'</option>').join('')+'</select>';
    }else if(field.type==='check')control='<input type="checkbox"'+attr+(value===true?' checked':'')+'>';
    else if(['list','locations','textarea'].includes(field.type)){
      const text=field.type==='list'?(value||[]).join('\n'):field.type==='locations'?(value||[]).map(x=>x.name+' | '+(missing(x.radiusKm)?'':x.radiusKm)).join('\n'):value;
      control='<textarea'+attr+(field.placeholder?' placeholder="'+esc(field.placeholder)+'"':'')+'>'+esc(text)+'</textarea>';
    }else control='<input type="'+field.type+'"'+attr+' value="'+esc(value)+'"'+(field.type==='number'?' min="'+(field.min==null?0:field.min)+'"'+(field.max?' max="'+field.max+'"':'')+' step="'+(field.step||'1')+'"':'')+(field.placeholder?' placeholder="'+esc(field.placeholder)+'"':'')+'>';
    return '<label for="'+id+'" class="'+(field.wide?'wide ':'')+(field.type==='check'?'check-label':'')+'">'+(field.type==='check'?control+' '+esc(field.label):esc(field.label)+control)+(field.help?'<span class="field-help">'+esc(field.help)+'</span>':'')+'</label>';
  }
  function openEditor(id) {
    if(busy||view==='changes')return;
    const record=id?find(view,id):{};
    editing={type:view,id:id||null,conflicted:false};
    document.querySelectorAll('#recordForm button').forEach(b=>b.disabled=false);
    $('editorTitle').textContent=(id?'Edit ':'Add ')+singular[view];
    $('formFields').innerHTML=fields(view).map(field=>field.section?'<div class="form-section">'+esc(field.section)+'</div>':inputMarkup(field,record[field.key])).join('');
    if(view==='results')$('formFields').insertAdjacentHTML('afterbegin','<p class="form-tip">Use the date range and account timezone from your report. Blank means unknown; enter 0 only for a confirmed zero. Saving results does not create an accounting expense.</p>');
    $('recordForm').elements.reason.value='';
    $('reasonWrap').hidden=!id;
    $('recordForm').elements.reason.required=!!id;
    $('archive').hidden=!id;
    $('archive').textContent=record.archived?'Restore record':'Archive record';
    $('recordForm').querySelector('.form-error').textContent='';
    if(view==='results'){
      updateScope(record.adId);
      $('field-adSetId').addEventListener('change',()=>updateScope(null));
    }
    $('editor').showModal();
  }
  function updateScope(adId){
    const setId=$('field-adSetId').value;
    $('field-adId').innerHTML='<option value="">Whole ad set</option>'+state.ads.filter(x=>x.adSetId===setId).map(x=>'<option value="'+esc(x.id)+'">'+esc(x.name)+'</option>').join('');
    $('field-adId').value=adId||'';
  }
  function formRecord(){
    const data={revision:state.revision,reason:$('recordForm').elements.reason.value.trim()||'New marketing record'};
    fields(editing.type).filter(f=>!f.section).forEach(field=>{
      const input=$('recordForm').elements[field.key],value=input.value.trim();
      if(field.type==='check')data[field.key]=input.checked;
      else if(field.type==='number')data[field.key]=value===''?null:Number(value);
      else if(field.type==='list')data[field.key]=value.split('\n').map(x=>x.trim()).filter(Boolean);
      else if(field.type==='locations')data[field.key]=value.split('\n').map(x=>x.trim()).filter(Boolean).map(line=>{
        const parts=line.split('|');if(parts.length>2)throw new Error('Use one location and one radius per line.');
        const radius=(parts[1]||'').trim(),numericRadius=radius===''?null:Number(radius);
        if(radius!=='' && (!Number.isFinite(numericRadius)||numericRadius<0||numericRadius>1000))throw new Error('Enter a radius from 0 to 1000 km, or leave it blank when unconfirmed.');
        return {name:parts[0].trim(),radiusKm:numericRadius};
      });
      else data[field.key]=value||null;
    });
    return data;
  }
  async function saveRecord(event){
    event.preventDefault();if(busy || (editing && editing.conflicted))return;
    const errorBox=$('recordForm').querySelector('.form-error');errorBox.textContent='';
    try{
      const data=formRecord();setBusy(true);
      await request('/'+paths[editing.type]+(editing.id?'/'+encodeURIComponent(editing.id):''),editing.id?'PATCH':'POST',data);
      $('editor').close();await load('Record saved. Changes are recorded in the history.');
    }catch(error){
      if(error.status===409){editing.conflicted=true;await load('Records changed. Close the editor and reopen the record to review the latest values.');errorBox.textContent=error.message+' Your entries are kept here for reference. Close and reopen this record before making another change.';}
      else errorBox.textContent=error.message;
    }finally{setBusy(false);}
  }
  async function archiveRecord(){
    if(busy||!editing.id||editing.conflicted)return;
    const form=$('recordForm'),reason=form.elements.reason.value.trim();
    if(!reason){form.querySelector('.form-error').textContent='Enter a reason before archiving or restoring this record.';form.elements.reason.focus();return;}
    const record=find(editing.type,editing.id),restore=record.archived;
    try{
      setBusy(true);
      const data={revision:state.revision,archived:!restore,reason};
      if(editing.type!=='results')data.status=restore?'draft':'archived';
      await request('/'+paths[editing.type]+'/'+encodeURIComponent(editing.id),'PATCH',data);
      $('editor').close();await load(restore?'Record restored as draft. Verify its current status.':'Record archived. Its reporting history is preserved.');
    }catch(error){if(error.status===409){editing.conflicted=true;await load('Records changed. Close the editor and reopen the record to review the latest values.');}form.querySelector('.form-error').textContent=error.message+(error.status===409?' Close and reopen this record before making another change.':'');}finally{setBusy(false);}
  }
  function csvCell(value){
    let text=missing(value)?'':typeof value==='object'?JSON.stringify(value):String(value);
    if(/^[\s]*[=+@\-\t\r]/.test(text))text="'"+text;
    return '"'+text.replace(/"/g,'""')+'"';
  }
  function exportRows(){
    if(!state||busy)return;
    const rows=filteredRows();if(!rows.length)return;
    const keys=[...new Set(rows.flatMap(row=>Object.keys(row)))];
    const csv='\ufeff'+[keys.map(csvCell).join(','),...rows.map(row=>keys.map(key=>csvCell(row[key])).join(','))].join('\r\n');
    const url=URL.createObjectURL(new Blob([csv],{type:'text/csv;charset=utf-8'})),link=document.createElement('a');
    link.href=url;link.download='SANKI_Meta_'+view+'_'+new Date().toISOString().slice(0,10)+'.csv';link.click();URL.revokeObjectURL(url);
  }
  document.querySelectorAll('[data-tab]').forEach(tab=>tab.addEventListener('click',()=>{
    view=tab.dataset.tab;sort=null;render();
  }));
  $('records').addEventListener('click',event=>{
    const details=event.target.closest('[data-setup]');if(details){openSetupDetails(details.dataset.setup);return;}
    const drill=event.target.closest('[data-drill]');if(drill){selection[view].clear();selection[view].add(drill.dataset.drill);if(view==='campaigns')selection.adSets.clear();view=view==='campaigns'?'adSets':'ads';sort=null;render();return;}
    const edit=event.target.closest('[data-edit]');if(edit){openEditor(edit.dataset.edit);return;}
    const heading=event.target.closest('[data-sort]');if(heading){sort={key:heading.dataset.sort,direction:sort&&sort.key===heading.dataset.sort?-sort.direction:1};render();}
  });
  function openSetupDetails(id){
    const r=find('adSets',id);if(!r)return;let raw={};try{raw=JSON.parse(r.setupDetails||'{}');}catch{}
    const renderValue=v=>Array.isArray(v)?v.length?lines(v):note('None recorded'):display(v);
    const row=(label,value)=>'<div class="setup-detail-row"><dt>'+esc(label)+'</dt><dd>'+renderValue(value)+'</dd></div>';
    const target=(values,confirmed)=>confirmed?(values||[]).length?values:'None selected':'Unconfirmed';
    $('setupDetailsTitle').textContent=r.name;
    $('setupDetailsBody').innerHTML='<dl>'+row('Campaign',campaignFor('adSets',r)?.name)+row('Campaign objective',campaignFor('adSets',r)?.objective)+row('Purpose / optimisation',goalText(r.performanceGoal))+row('Destination',r.destination)+row('Destination link',r.destinationRef)+row('WhatsApp number(s)',setPhones(r).length?setPhones(r):'Not supplied in saved export')+row('Daily budget',missing(r.budgetDaily)?null:'₹'+r.budgetDaily)+row('All locations and radii',locationText(r))+row('Exported age limits',missing(r.ageMin)||missing(r.ageMax)?null:r.ageMin+'–'+r.ageMax)+row('Separate audience age range',raw['Age Range'])+row('Gender',genderText(r.gender))+row('Interests',target(r.interests,r.interestsConfirmed))+row('Behaviours',target(r.behaviors,r.behaviorsConfirmed))+row('Audience controls and expansion',r.audienceMode)+row('Placements',r.placementsConfirmed?r.placements:'Unconfirmed')+row('Frequency setting',r.frequencyCount==null||r.frequencyDays==null?null:r.frequencyCount+' impressions / '+r.frequencyDays+' day(s) · '+(r.frequencyMode||'type unconfirmed'))+row('Languages / exported locale codes',r.languages?.length?r.languages:raw.Locales)+row('Attribution',raw['Attribution Spec'])+row('Custom audiences',raw['Custom Audiences'])+row('Excluded custom audiences',raw['Excluded Custom Audiences'])+row('Excluded locations',['Excluded Countries','Excluded Cities','Excluded Regions','Excluded Addresses','Excluded Zip'].map(k=>raw[k]).filter(Boolean))+row('Notes',r.notes)+'</dl>';
    const ads=state.ads.filter(a=>a.adSetId===r.id&&!a.archived);
    $('setupDetailsBody').innerHTML+='<h3>Ads and creative details</h3>'+ads.map(a=>{let source={};try{source=JSON.parse(a.setupSource||'{}');}catch{}return '<section class="creative-details"><h3>'+esc(a.name)+'</h3><dl>'+row('Creative type',a.format)+row('Call to action',a.cta)+row('WhatsApp number',a.whatsappNumber)+row('Ad text',a.primaryText)+'</dl>'+a.mediaUrls.map((url,i)=>'<a class="creative-link" href="'+esc(url)+'" target="_blank" rel="noopener noreferrer">Open creative preview '+(i+1)+'</a>').join('')+(Object.keys(source).length?'<details><summary>All exported settings for this ad</summary><dl>'+Object.entries(source).map(([label,value])=>row(label,value)).join('')+'</dl></details>':'')+'</section>';}).join('');
    $('setupDetailsDialog').showModal();
  }
  $('closeSetupDetails').addEventListener('click',()=>$('setupDetailsDialog').close());
  $('records').addEventListener('change',event=>{if(event.target.id==='selectAllRows'){for(const r of filteredRows())event.target.checked?selection[view].add(r.id):selection[view].delete(r.id);}else if(event.target.dataset.select){event.target.checked?selection[view].add(event.target.dataset.select):selection[view].delete(event.target.dataset.select);}else return;if(view==='campaigns')selection.adSets.clear();render();});
  $('clearScope').addEventListener('click',()=>{Object.values(selection).forEach(s=>s.clear());$('campaignFilter').value='';render();});
  let bulkContext=null;
  function openBulk(action){
    if(!state||busy||!selection[view]?.size)return;
    bulkContext={action,type:view,ids:[...selection[view]],revision:state.revision};$('bulkForm').reset();$('bulkError').textContent='';
    $('bulkTitle').textContent=action==='remove'?'Remove '+bulkContext.ids.length+' selected records':'Edit '+bulkContext.ids.length+' selected records';
    $('bulkHelp').textContent=action==='remove'?'These records and linked child records will be removed from the current view. Their history is retained. Re-uploading setup restores matching campaigns, ad sets and ads; performance can be uploaded again separately.':'Only the settings entered below change. Other fields stay as they are. This updates app records only.';
    $('bulkFields').hidden=action==='remove';$('bulkStatusWrap').hidden=view==='results';$('bulkBudgetWrap').hidden=view!=='adSets';$('bulkSubmit').textContent=action==='remove'?'Remove records':'Apply changes';$('bulkDialog').showModal();
  }
  $('editSelected').addEventListener('click',()=>{if(selection[view].size===1)openEditor([...selection[view]][0]);else openBulk('edit');});
  $('removeSelected').addEventListener('click',()=>openBulk('remove'));
  for(const id of ['bulkClose','bulkCancel'])$(id).addEventListener('click',()=>{if(!$('bulkSubmit').disabled)$('bulkDialog').close();});
  $('bulkDialog').addEventListener('cancel',e=>{if($('bulkSubmit').disabled)e.preventDefault();});
  $('bulkForm').addEventListener('submit',async e=>{
    e.preventDefault();if(!bulkContext||$('bulkSubmit').disabled)return;
    const fields={};if(bulkContext.action==='edit'){if($('bulkStatus').value&&bulkContext.type!=='results')fields.status=$('bulkStatus').value;if($('bulkBudget').value!==''&&bulkContext.type==='adSets')fields.budgetDaily=Number($('bulkBudget').value);if($('bulkNotes').value.trim())fields.notes=$('bulkNotes').value.trim();}
    $('bulkSubmit').disabled=true;
    try{const data=await request('/bulk','POST',{...bulkContext,fields,reason:$('bulkReason').value});$('bulkDialog').close();Object.values(selection).forEach(s=>s.clear());await load(data.changedRecords+' records '+(bulkContext.action==='remove'?'removed. Re-upload setup when ready.':'updated.'));}catch(error){$('bulkError').textContent=error.message;}finally{$('bulkSubmit').disabled=false;}
  });
  $('viewChildren').addEventListener('click',()=>{if(!selection[view].size)return;view=view==='campaigns'?'adSets':'ads';sort=null;render();});
  $('columnMode').addEventListener('change',()=>{sort=null;render();});
  $('openUpload').addEventListener('click',()=>{$('uploadPanel').hidden=!$('uploadPanel').hidden;});
  $('allRecords').addEventListener('click',()=>{$('statusFilter').value='';render();});
  $('runningRecords').addEventListener('click',()=>{$('statusFilter').value='reported_running';render();});
  $('records').addEventListener('keydown',event=>{if((event.key==='Enter'||event.key===' ')&&event.target.matches('[data-sort]')){event.preventDefault();event.target.click();}});
  ['campaignFilter','collectionFilter','statusFilter','destinationFilter','creativeTypeFilter','periodStartFilter','periodEndFilter'].forEach(id=>$(id).addEventListener('change',render));
  $('search').addEventListener('input',render);
  $('refresh').addEventListener('click',()=>load('Records refreshed.'));
  $('export').addEventListener('click',exportRows);$('add').addEventListener('click',()=>openEditor());
  document.querySelectorAll('[data-close]').forEach(button=>button.addEventListener('click',()=>{if(!busy)$('editor').close();}));
  $('editor').addEventListener('cancel',event=>{if(busy)event.preventDefault();});
  $('recordForm').addEventListener('submit',saveRecord);$('archive').addEventListener('click',archiveRecord);
  load();
})();

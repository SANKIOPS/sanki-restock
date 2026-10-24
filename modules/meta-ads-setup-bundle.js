'use strict';
const {parseSetup}=require('./meta-ads-import');
function mergeSetupFiles(files){
  const maps={campaigns:new Map(),adSets:new Map(),ads:new Map()},warnings=[];
  for(const file of files){const parsed=parseSetup(file.buffer);for(const [type,records] of Object.entries(parsed.records))for(const row of records){const prior=maps[type].get(row.id)||{};const merged={...prior};for(const [key,value] of Object.entries(row)){if(value==null||value===''||value==='unknown'||value==='delivery_unconfirmed'){if(merged[key]==null)merged[key]=value;continue;}if(['campaignId','adSetId'].includes(key)&&merged[key]&&merged[key]!==value)throw new Error('Conflicting parent IDs across files for '+row.name);if(key==='status'&&merged.status&&['paused','reported_running','archived'].includes(merged.status)&&merged.status!==value&&['paused','reported_running','archived'].includes(value))throw new Error('Conflicting status across files for '+row.name+'. Export matching snapshots.');merged[key]=value;}maps[type].set(row.id,merged);}warnings.push(...parsed.warnings);}
  warnings.push('Linked setup files are merged by Meta ID. Only supplied fields update. Absent records are not assumed deleted from a partial bundle.');
  return {level:'bundle',records:Object.fromEntries(Object.entries(maps).map(([k,v])=>[k,[...v.values()]])),warnings:[...new Set(warnings)]};
}
function setupCoverage(state){return state.campaigns.filter(c=>!c.archived).map(c=>{const sets=state.adSets.filter(s=>s.campaignId===c.id&&!s.archived),ads=state.ads.filter(a=>sets.some(s=>s.id===a.adSetId)&&!a.archived),missing=[];if(!c.objective)missing.push('campaign objective');if(!sets.length)missing.push('ad sets');if(!ads.length||sets.some(s=>!ads.some(a=>a.adSetId===s.id)))missing.push('creatives');if(sets.some(s=>s.budgetDaily==null))missing.push('budget');if(sets.some(s=>!s.destination))missing.push('destination');if(sets.some(s=>!s.locations?.length))missing.push('locations');if(sets.some(s=>s.ageMin==null||s.ageMax==null||!s.gender))missing.push('age / gender');if(sets.some(s=>s.interestsConfirmed!==true))missing.push('audience confirmation');if(ads.some(a=>!a.mediaUrls?.length))missing.push('creative URLs');if([c,...sets,...ads].some(r=>!['paused','reported_running','active','completed','archived'].includes(r.status)))missing.push('confirmed on/off status');return {campaignId:c.id,adSets:sets.length,ads:ads.length,complete:!missing.length,missing};});}
function missingDetails(state,campaignId){
  const known=r=>['paused','reported_running','active','completed','archived'].includes(r.status);
  const groups=[];
  function add(type,r,keys){if(keys.length)groups.push({type,id:r.id,name:r.name,keys});}
  for(const c of state.campaigns.filter(c=>!c.archived&&(!campaignId||c.id===campaignId))){
    add('campaigns',c,[...(!c.objective?['objective']:[]),...(!known(c)?['status']:[])]);
    for(const s of state.adSets.filter(s=>s.campaignId===c.id&&!s.archived)){
      const keys=[];for(const k of ['budgetDaily','destination','performanceGoal','ageMin','ageMax','gender'])if(s[k]==null||s[k]==='')keys.push(k);
      if(!known(s))keys.push('status');if(!s.locations?.length)keys.push('locations');
      if(s.destination==='WhatsApp'&&!s.whatsappNumber)keys.push('whatsappNumber');
      if(s.interestsConfirmed!==true)keys.push('interests','interestsConfirmed');
      if(s.behaviorsConfirmed!==true)keys.push('behaviors','behaviorsConfirmed');
      if(s.placementsConfirmed!==true||!s.placements?.length)keys.push('placements','placementsConfirmed');
      add('adSets',s,keys);
      for(const a of state.ads.filter(a=>a.adSetId===s.id&&!a.archived))add('ads',a,[...(!a.collection?['collection']:[]),...(!a.creativeTypes?.length?['creativeTypes']:[]),...(!a.model?['model']:[]),...(!a.mediaUrls?.length?['mediaUrls']:[]),...(!known(a)?['status']:[])]);
    }
  }
  return groups;
}
module.exports={mergeSetupFiles,setupCoverage,missingDetails};

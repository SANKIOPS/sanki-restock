'use strict';
const XLSX = require('xlsx');
function parseSetup(buffer) {
  const book = XLSX.read(buffer, {type:'buffer', raw:true});
  for(const name of book.SheetNames)for(const row of XLSX.utils.sheet_to_json(book.Sheets[name],{defval:'',raw:true}))for(const key of ['Campaign ID','Ad Set ID','Ad set ID','Ad ID'])if(typeof row[key]==='number'&&!Number.isSafeInteger(row[key]))throw new Error(key+' was rounded in Excel. Export CSV directly from Meta.');
  const rows = book.SheetNames.flatMap(name => XLSX.utils.sheet_to_json(book.Sheets[name], {defval:'', raw:false}));
  if (!rows.length || rows.length > 2000) throw new Error('Upload a configuration export containing 1–2000 ads.');
  const records = {campaigns:new Map(), adSets:new Map(), ads:new Map()}, warnings = new Set();
  const text = (r,k) => String(r[k] || '').trim();
  const metaId = (r,k,prefix) => {const value=text(r,k).replace(/^[a-z]+:/i,'');if(!/^\d+$/.test(value))throw new Error(`${k} is missing or invalid. Use the configuration export with IDs.`);return prefix+value;};
  const status = v => ({ACTIVE:'reported_running',PAUSED:'paused',INACTIVE:'paused',ARCHIVED:'archived',DELETED:'archived'}[v.toUpperCase()] || 'unknown');
  const number = (r,k) => {const v=text(r,k);if(!v)return undefined;const n=Number(v);if(!Number.isFinite(n))throw new Error(`Invalid ${k}.`);return n;};
  function put(type, record) {const prior=records[type].get(record.id);if(prior && JSON.stringify(prior)!==JSON.stringify(record))throw new Error(`Conflicting settings for ${record.name}. Export a consistent setup.`);records[type].set(record.id,record);}
  if(rows.some(r=>Object.hasOwn(r,'Campaign name'))&&!rows.some(r=>Object.hasOwn(r,'Campaign Name'))){
    const level=rows.some(r=>Object.hasOwn(r,'Ad ID'))?'ads':rows.some(r=>Object.hasOwn(r,'Ad set ID'))?'adSets':'campaigns';
    const deliveryStatus=v=>({active:'reported_running',paused:'paused',off:'paused',archived:'archived',deleted:'archived'}[v.toLowerCase()]||'delivery_unconfirmed');
    for(const r of rows){if(!text(r,'Campaign ID')&&!text(r,'Campaign name'))continue;
      const campaign={id:metaId(r,'Campaign ID','meta-campaign-'),name:text(r,'Campaign name')};if(!campaign.name)throw new Error('Campaign name is required.');if(text(r,'Campaign delivery'))campaign.status=deliveryStatus(text(r,'Campaign delivery'));if(text(r,'Campaign objective'))campaign.objective=text(r,'Campaign objective');put('campaigns',campaign);
      if(level==='campaigns')continue;
      const set={id:metaId(r,'Ad set ID','meta-adset-'),campaignId:campaign.id,name:text(r,'Ad set name')};if(!set.name)throw new Error('Include Ad set name in this table export.');if(text(r,'Ad set delivery'))set.status=deliveryStatus(text(r,'Ad set delivery'));const daily=text(r,'Ad set budget type').toLowerCase()==='daily'?number(r,'Ad set budget'):undefined;if(daily!==undefined)set.budgetDaily=daily;put('adSets',set);
      if(level==='ads'){const ad={id:metaId(r,'Ad ID','meta-ad-'),adSetId:set.id,name:text(r,'Ad name')};if(!ad.name)throw new Error('Include Ad name in this table export.');if(text(r,'Ad delivery'))ad.status=deliveryStatus(text(r,'Ad delivery'));put('ads',ad);}
    }
    warnings.add('Partial '+level+' table: only supplied names, IDs, delivery and explicit daily budgets update. Missing targeting, creatives and objectives remain unchanged; delivery does not always establish on/off status.');
    return {level,records:Object.fromEntries(Object.entries(records).map(([k,v])=>[k,[...v.values()]])),warnings:[...warnings]};
  }
  for(const r of rows) {
    const campaignId=metaId(r,'Campaign ID','meta-campaign-'), adSetId=metaId(r,'Ad Set ID','meta-adset-'), adId=metaId(r,'Ad ID','meta-ad-');
    put('campaigns',{id:campaignId,name:text(r,'Campaign Name'),objective:text(r,'Campaign Objective'),status:status(text(r,'Campaign Status'))});
    const audienceColumns=['Countries','Cities','Regions','Addresses','Zip','Excluded Countries','Excluded Cities','Excluded Regions','Excluded Addresses','Excluded Zip','Location Types','Unified Interests','Flexible Inclusions','Flexible Exclusions','Behaviors','Custom Audiences','Excluded Custom Audiences','Locales','Advantage Audience','Individual Setting','Age Range','Targeting Optimization','Targeting Relaxation','Attribution Spec','Billing Event','Ad Set Bid Strategy'];
    const set={id:adSetId,campaignId,name:text(r,'Ad Set Name'),status:status(text(r,'Ad Set Run Status')),performanceGoal:text(r,'Optimization Goal'),setupDetails:JSON.stringify(Object.fromEntries(audienceColumns.map(k=>[k,text(r,k)])))};
    for(const [key,column] of [['budgetDaily','Ad Set Daily Budget'],['ageMin','Age Min'],['ageMax','Age Max']]) {const v=number(r,column);if(v!==undefined)set[key]=v;}
    if(text(r,'Gender'))set.gender=text(r,'Gender').toLowerCase();
    const destination={INSTAGRAM_PROFILE:'Instagram profile',WHATSAPP:'WhatsApp',WEBSITE:'Website',MESSENGER:'Messenger'}[text(r,'Destination Type')];
    if(destination)set.destination=destination;
    if(text(r,'Link')){set.destinationRef=text(r,'Link');if(/maps\.app|google.*maps/.test(set.destinationRef))set.destination='Google Maps';}
    const locations=['Cities','Countries','Regions','Addresses','Zip'].flatMap(k=>text(r,k).split(';').filter(Boolean)).map(v=>{const match=v.trim().match(/^(.*?)\s*\+([\d.]+)km$/i);return {name:match?match[1]:v.trim(),radiusKm:match?Number(match[2]):null};});
    if(locations.length)set.locations=locations;else warnings.add(`${set.name}: location and radius not supplied; existing locations retained.`);
    let targeting=[];
    if(text(r,'Flexible Inclusions')){try{targeting=JSON.parse(text(r,'Flexible Inclusions'));if(!Array.isArray(targeting))throw new Error();}catch{throw new Error(`Cannot read audience settings for ${set.name}.`);}}
    if(text(r,'Flexible Inclusions')){set.interests=targeting.flatMap(g=>(g.interests||[]).map(x=>x.name).filter(Boolean));set.interestsConfirmed=true;set.behaviors=targeting.flatMap(g=>(g.behaviors||[]).map(x=>x.name).filter(Boolean));set.behaviorsConfirmed=true;}else warnings.add(`${set.name}: blank targeting needs confirmation; existing selections retained.`);
    set.audienceMode=`Advantage audience: ${text(r,'Advantage Audience')||'unconfirmed'}; age range: ${text(r,'Age Range')||'not supplied'}; settings: ${text(r,'Individual Setting')||'not supplied'}; expansion: ${text(r,'Targeting Optimization')||'unconfirmed'}`;
    const placements=['Publisher Platforms','Facebook Positions','Instagram Positions','Audience Network Positions','Messenger Positions'].flatMap(k=>text(r,k)?[`${k}: ${text(r,k)}`]:[]);
    if(placements.length){set.placements=placements;set.placementsConfirmed=true;}
    const frequency=text(r,'Frequency Control').match(/Max Frequency=(\d+), Interval Days=(\d+), Type=(CAP|TARGET)/);
    if(frequency){set.frequencyCount=Number(frequency[1]);set.frequencyDays=Number(frequency[2]);set.frequencyMode=frequency[3].toLowerCase();}
    if(text(r,'Locales'))warnings.add(`${set.name}: language codes are retained in the source export; names require confirmation.`);
    put('adSets',set);
    const ad={id:adId,adSetId,name:text(r,'Ad Name'),status:status(text(r,'Ad Status')),format:text(r,'Creative Type'),primaryText:text(r,'Body'),cta:text(r,'Call to Action')};
    ad.setupSource=JSON.stringify(Object.fromEntries(Object.keys(r).filter(k=>text(r,k)).map(k=>[k,text(r,k)])));
    if(text(r,'Call to Action WhatsApp Number'))ad.whatsappNumber=text(r,'Call to Action WhatsApp Number');
    const urls=['Instagram Preview Link','Permalink','Preview Link'].map(k=>text(r,k)).filter(Boolean);if(urls.length)ad.mediaUrls=urls;
    put('ads',ad);
  }
  return {records:Object.fromEntries(Object.entries(records).map(([k,v])=>[k,[...v.values()]])),warnings:[...warnings]};
}
module.exports={parseSetup};

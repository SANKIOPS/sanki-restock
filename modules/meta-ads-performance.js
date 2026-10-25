'use strict';
const XLSX=require('xlsx');
function parsePerformance(buffer) {
  const book=XLSX.read(buffer,{type:'buffer',raw:true,cellDates:true});
  const rows=book.SheetNames.flatMap(name=>XLSX.utils.sheet_to_json(book.Sheets[name],{defval:null,raw:true}));
  if(!rows.length||rows.length>2000)throw new Error('Upload 1–2000 ad-level performance rows.');
  if(!Object.hasOwn(rows[0],'Reporting starts')||!Object.hasOwn(rows[0],'Reporting ends'))throw new Error('This file does not contain performance reporting dates. Export table results rather than campaign configuration.');
  const ids=(value,label)=>{if(typeof value==='number'&&!Number.isSafeInteger(value))throw new Error(`${label} was rounded in Excel. Export the same report as CSV directly from Meta.`);const s=String(value??'').trim();if(!/^\d+$/.test(s))throw new Error(`${label} is missing or invalid. Add campaign, ad-set and ad IDs to your report.`);return s;};
  const date=value=>{const s=value instanceof Date?value.toISOString().slice(0,10):String(value??'').trim();if(!/^\d{4}-\d{2}-\d{2}$/.test(s)||!Number.isFinite(Date.parse(s))||new Date(s).toISOString().slice(0,10)!==s)throw new Error('Reporting dates must be valid YYYY-MM-DD dates.');return s;};
  const numeric=(r,k)=>{const v=r[k];if(v==null||v===''||v==='-'||v==='—')return null;const n=Number(v);if(!Number.isFinite(n)||n<0)throw new Error(`Invalid ${k}.`);return n;};
  const configuredStatus=(r,columns)=>{
    const value=columns.map(k=>r[k]).find(v=>v!=null&&String(v).trim()!=='');if(value==null)return {};
    const status=String(value).trim().toUpperCase();
    if(['ACTIVE','ON','TRUE','1'].includes(status))return {status:'reported_running'};
    if(['PAUSED','OFF','FALSE','0','INACTIVE'].includes(status))return {status:'paused'};
    throw new Error('Unrecognised configured on/off status: '+status);
  };
  const seen=new Set();
  return rows.map(r=>{
    const campaignMetaId='meta-campaign-'+ids(r['Campaign ID'],'Campaign ID'),setMetaId='meta-adset-'+ids(r['Ad set ID'],'Ad set ID'),adMetaId='meta-ad-'+ids(r['Ad ID'],'Ad ID');
    const periodStart=date(r['Reporting starts']),periodEnd=date(r['Reporting ends']);
    if(periodStart>periodEnd)throw new Error('Reporting start must not be after reporting end.');
    const key=adMetaId+'-'+periodStart+'-'+periodEnd;if(seen.has(key))throw new Error('Repeated ad/period rows found. Remove audience or placement breakdowns for this upload.');seen.add(key);
    if(!Object.hasOwn(r,'Amount spent (INR)')||!Object.hasOwn(r,'Impressions')||!Object.hasOwn(r,'Reach'))throw new Error('Include Amount spent (INR), Impressions and Reach.');
    const resultIndicator=String(r['Result indicator']??''),reportedResults=numeric(r,'Results');
    return {setup:{campaigns:{...configuredStatus(r,['Campaign status','Campaign Status','Campaign on/off']),name:r['Campaign name'],objective:r['Objective']||r['Campaign objective']},adSets:{...configuredStatus(r,['Ad set status','Ad Set Run Status','Ad set on/off']),name:r['Ad set name'],...(String(r['Ad set budget type']||'').toLowerCase()==='daily'?{budgetDaily:numeric(r,'Ad set budget')}:{})},ads:{...configuredStatus(r,['Ad status','Ad Status','Ad on/off']),name:r['Ad name']}},campaignMetaId,setMetaId,adMetaId,adName:String(r['Ad name']??''),campaignName:String(r['Campaign name']??''),setName:String(r['Ad set name']??''),record:{periodStart,periodEnd,source:'Meta Ads Manager performance upload',spend:numeric(r,'Amount spent (INR)'),impressions:numeric(r,'Impressions'),reach:numeric(r,'Reach'),linkClicks:numeric(r,'Link clicks'),profileVisits:resultIndicator==='profile_visit_view'?reportedResults:numeric(r,'Instagram profile visits'),messagingStarts:numeric(r,'Messaging conversations started'),metaPurchases:numeric(r,'Purchases'),metaPurchaseValue:numeric(r,'Purchases conversion value'),videoPlays3s:numeric(r,'3-second video plays'),thruPlays:numeric(r,'15-second ThruPlays'),instagramFollows:numeric(r,'Instagram follows'),reportedResults,resultIndicator,attributionSetting:String(r['Attribution setting']??''),reportedDelivery:String(r['Ad delivery']??'')}};
  });
}
module.exports={parsePerformance};

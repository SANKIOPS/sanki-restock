'use strict';
const XLSX=require('xlsx');
function parsePerformance(buffer) {
  const book=XLSX.read(buffer,{type:'buffer',raw:true,cellDates:true});
  const rows=book.SheetNames.flatMap(name=>XLSX.utils.sheet_to_json(book.Sheets[name],{defval:null,raw:true}));
  if(!rows.length||rows.length>2000)throw new Error('Upload 1–2000 ad-level daily rows.');
  const ids=(value,label)=>{if(typeof value==='number'&&!Number.isSafeInteger(value))throw new Error(`${label} was rounded in Excel. Export the same report as CSV directly from Meta.`);const s=String(value??'').trim();if(!/^\d+$/.test(s))throw new Error(`${label} is missing or invalid. Add campaign, ad-set and ad IDs to your report.`);return s;};
  const date=value=>{const s=value instanceof Date?value.toISOString().slice(0,10):String(value??'').trim();if(!/^\d{4}-\d{2}-\d{2}$/.test(s))throw new Error('Reporting dates must use YYYY-MM-DD. Export Yesterday or a daily breakdown.');return s;};
  const numeric=(r,k)=>{const v=r[k];if(v==null||v===''||v==='-'||v==='—')return null;const n=Number(v);if(!Number.isFinite(n)||n<0)throw new Error(`Invalid ${k}.`);return n;};
  const seen=new Set();
  return rows.map(r=>{
    const campaignMetaId='meta-campaign-'+ids(r['Campaign ID'],'Campaign ID'),setMetaId='meta-adset-'+ids(r['Ad set ID'],'Ad set ID'),adMetaId='meta-ad-'+ids(r['Ad ID'],'Ad ID');
    const periodStart=date(r['Reporting starts']),periodEnd=date(r['Reporting ends']);
    if(periodStart!==periodEnd)throw new Error('Use Yesterday, or export a daily breakdown. Multi-day totals cannot be mixed with daily history.');
    const key=adMetaId+'-'+periodStart;if(seen.has(key))throw new Error('Repeated ad/date rows found. Remove audience or placement breakdowns for the daily upload.');seen.add(key);
    if(!Object.hasOwn(r,'Amount spent (INR)')||!Object.hasOwn(r,'Impressions')||!Object.hasOwn(r,'Reach'))throw new Error('Include Amount spent (INR), Impressions and Reach.');
    const resultIndicator=String(r['Result indicator']??''),reportedResults=numeric(r,'Results');
    return {campaignMetaId,setMetaId,adMetaId,adName:String(r['Ad name']??''),record:{periodStart,periodEnd,source:'Meta Ads Manager daily upload',spend:numeric(r,'Amount spent (INR)'),impressions:numeric(r,'Impressions'),reach:numeric(r,'Reach'),linkClicks:numeric(r,'Link clicks'),profileVisits:resultIndicator==='profile_visit_view'?reportedResults:numeric(r,'Instagram profile visits'),messagingStarts:numeric(r,'Messaging conversations started'),metaPurchases:numeric(r,'Purchases'),metaPurchaseValue:numeric(r,'Purchases conversion value'),videoPlays3s:numeric(r,'3-second video plays'),thruPlays:numeric(r,'15-second ThruPlays'),reportedResults,resultIndicator,attributionSetting:String(r['Attribution setting']??''),reportedDelivery:String(r['Ad delivery']??'')}};
  });
}
module.exports={parsePerformance};

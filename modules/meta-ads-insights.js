'use strict';
const metrics=['spend','impressions','reach','linkClicks','profileVisits','messagingStarts','qualifiedEnquiries','orders','revenue','storeVisits','metaPurchases','metaPurchaseValue','videoPlays3s','thruPlays','reportedResults'];
const nextDay=d=>new Date(Date.parse(d)+86400000).toISOString().slice(0,10);
function selectRange(reports,start,end){
  const selected=[],missing=[];
  for(const id of new Set(reports.map(r=>r.adId))){
    const candidates=reports.filter(r=>r.adId===id&&r.periodStart>=start&&r.periodEnd<=end);
    const exact=candidates.filter(r=>r.periodStart===start&&r.periodEnd===end).sort((a,b)=>(b.updatedAt||'').localeCompare(a.updatedAt||''))[0];
    if(exact){selected.push(exact);continue;}
    const groups=new Map();for(const r of candidates){const key=JSON.stringify([r.reportGoal,r.reportDestination,r.attributionSetting,r.resultIndicator]);if(!groups.has(key))groups.set(key,[]);groups.get(key).push(r);}
    const covers=[];
    for(const rows of groups.values()){
      const memo=new Map();function cover(day){if(day>end)return [];if(memo.has(day))return memo.get(day);memo.set(day,null);for(const row of rows.filter(r=>r.periodStart===day).sort((a,b)=>a.periodEnd.localeCompare(b.periodEnd))){const tail=cover(nextDay(row.periodEnd));if(tail){const result=[row,...tail];memo.set(day,result);return result;}}return null;}
      const parts=cover(start);if(parts)covers.push(parts);
    }
    covers.sort((a,b)=>Math.max(...b.map(r=>Date.parse(r.updatedAt)||0))-Math.max(...a.map(r=>Date.parse(r.updatedAt)||0)));
    if(!covers.length){missing.push(id);continue;}
    const parts=covers[0],row={...parts[0],periodStart:start,periodEnd:end,combinedRows:parts.length};
    for(const k of metrics)row[k]=k==='reach'?null:parts.every(r=>r[k]!=null)?parts.reduce((n,r)=>n+r[k],0):null;
    selected.push(row);
  }
  return {selected,missing};
}
function insights(state,selectedPeriod) {
  const reports=state.results.filter(r=>!r.archived&&r.adId);
  const periodKey=r=>r.periodStart+'~'+r.periodEnd;
  const duration=r=>Math.round((Date.parse(r.periodEnd)-Date.parse(r.periodStart))/86400000)+1;
  const ranges=new Map();for(const r of reports){const key=periodKey(r),old=ranges.get(key);ranges.set(key,{key,start:r.periodStart,end:r.periodEnd,days:duration(r),updatedAt:[old?.updatedAt||'',r.updatedAt||''].sort().at(-1)});}
  const periods=[...ranges.values()].sort((a,b)=>b.updatedAt.localeCompare(a.updatedAt)||b.end.localeCompare(a.end)||a.start.localeCompare(b.start));
  const match=typeof selectedPeriod==='string'&&selectedPeriod.match(/^(\d{4}-\d{2}-\d{2})~(\d{4}-\d{2}-\d{2})$/);
  const valid=match&&match[1]<=match[2]&&[match[1],match[2]].every(d=>Number.isFinite(Date.parse(d))&&new Date(Date.parse(d)).toISOString().slice(0,10)===d)&&duration({periodStart:match[1],periodEnd:match[2]})<=366;
  const period=valid?{key:selectedPeriod,start:match[1],end:match[2],days:duration({periodStart:match[1],periodEnd:match[2]})}:periods[0];
  if(!period)return {date:null,days:0,periods:[],spend:null,groups:[],actions:[],creatives:[],note:'Upload a performance report to see findings and next actions.'};
  const date=period.start===period.end?period.start:period.start+' – '+period.end;
  const coverage=selectRange(reports,period.start,period.end);
  const latest=coverage.selected, groups=new Map(),actions=[],creatives=[];
  let spend=0,missingSpend=false;
  for(const r of latest){
    if(r.spend===null||r.spend===undefined)missingSpend=true;else spend+=r.spend;
    const ad=state.ads.find(a=>a.id===r.adId),set=state.adSets.find(s=>s.id===r.adSetId);if(!ad||!set)continue;
    const goal=r.reportGoal||set.performanceGoal,destination=r.reportDestination||set.destination;
    const metric=destination==='WhatsApp'?'messagingStarts':/PROFILE_VISIT/i.test(goal||'')?'profileVisits':/PURCHASE/i.test((goal||'')+' '+(r.resultIndicator||''))?'metaPurchases':/REACH/i.test(goal||'')?'reach':/LINK_CLICKS/i.test(goal||'')?'linkClicks':null;
    const label={messagingStarts:'WhatsApp enquiries',profileVisits:'Profile visits',metaPurchases:'Website sales',reach:'Store awareness',linkClicks:'Link clicks'}[metric]||'Other';
    const costFactor=metric==='reach'?1000:1;
    const count=metric?r[metric]:null,cost=count>0&&r.spend!=null?r.spend/count*costFactor:null;
    const prior=reports.filter(x=>x.adId===r.adId&&x.periodEnd<r.periodStart&&duration(x)===duration(r)&&x.attributionSetting===r.attributionSetting&&(x.reportGoal||goal)===goal).sort((a,b)=>b.periodEnd.localeCompare(a.periodEnd))[0];
    const previousCost=prior&&metric&&prior[metric]>0&&prior.spend!=null?prior.spend/prior[metric]*costFactor:null;
    const campaign=state.campaigns?.find(c=>c.id===set.campaignId);
    const item={ad:ad.name,adSet:set.name,adSetId:set.id,campaignId:set.campaignId,campaign:campaign?.name||'',adId:ad.id,goal:label,metric,costFactor,attribution:r.attributionSetting,collection:ad.collection||set.collection||'',creativeTypes:ad.creativeTypes||[],locations:set.locations||[],interests:set.interests||[],behaviors:set.behaviors||[],spend:r.spend,count,cost,previousDate:prior?(prior.periodStart===prior.periodEnd?prior.periodStart:prior.periodStart+' – '+prior.periodEnd):null,previousCost,impressions:r.impressions,reach:r.reach,videoPlays3s:r.videoPlays3s,thruPlays:r.thruPlays,reportedDelivery:r.reportedDelivery,storeVisits:r.storeVisits,orders:r.orders,revenue:r.revenue};
    creatives.push(item);
    if(metric&&cost!==null){const key=label+' · '+(r.attributionSetting||'attribution unconfirmed')+' · '+(goal||'goal unconfirmed');if(!groups.has(key))groups.set(key,[]);groups.get(key).push(item);}
    if(r.impressions===0&&r.spend===0)actions.push({priority:3,title:'No delivery to judge',ad:ad.name,reason:'This report has zero spend and zero impressions.',next:'Check delivery in Meta if this ad was intended to run. Keep it unranked until it delivers.'});
    if(destination==='WhatsApp'&&goal==='LINK_CLICKS')actions.push({priority:1,title:'Review the WhatsApp optimisation goal',ad:ad.name+' · '+set.name,reason:`The saved setup optimises link clicks. ${r.linkClicks==null?'Clicks unreported':r.linkClicks+' link clicks'}; ${r.messagingStarts==null?'conversations unreported':r.messagingStarts+' conversations'} in this report.`,next:'If the goal is enquiries, check whether a messaging conversation goal is available in Meta. Judge conversations and their quality before raising the budget.'});
    if(previousCost!==null&&cost!==null&&cost>previousCost*1.25)actions.push({priority:2,title:'Cost increased — investigate',ad:ad.name,reason:`Cost per ${label.toLowerCase()} rose more than 25% versus ${prior.periodStart}. This is a review flag, not proof of fatigue.`,next:'Compare the same reporting window and check audience, placements and recent edits. Do not stop the ad on this change alone.'});
  }
  if(latest.length&&latest.every(r=>r.metaPurchases==null&&r.orders==null))actions.push({priority:1,title:'Sales are not established',ad:'All ads',reason:'Purchase and confirmed-order fields are unavailable in this upload.',next:'Use your enquiry and order records to judge buying quality. Blank purchase fields do not mean zero sales.'});
  const rankings=[...groups].map(([label,items])=>({label,items:items.sort((a,b)=>a.cost-b.cost),winner:items.length>1?items[0]:null}));
  for(const g of rankings)if(g.winner)actions.push({priority:4,title:'Keep testing the lower-cost option',ad:g.winner.ad,reason:`Lowest observed cost in the ${g.label} comparison for ${date}. Audiences and delivery can differ.`,next:'Keep collecting daily results and confirm business quality. This ranking alone does not justify increasing budget.'});
  actions.sort((a,b)=>a.priority-b.priority);
  return {date,days:period.days,periodStart:period.start,periodEnd:period.end,selectedPeriod:period.key,periods,missingAdIds:coverage.missing,coverageNote:coverage.missing.length?'No complete, compatible coverage for '+coverage.missing.length+' ad(s). Upload daily rows or a report matching '+date+'.':'Results use exact reports or complete non-overlapping report ranges.',spend:missingSpend||!latest.length?null:spend,groups:rankings,actions:actions.slice(0,5),creatives,note:'Findings use only this reporting period. Overlapping reports are stored separately and never added together. Period totals cannot establish individual daily results. Rankings compare observed outcomes, not causal creative quality.'};
}
module.exports={insights};

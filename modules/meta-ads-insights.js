'use strict';
function insights(state,selectedPeriod) {
  const reports=state.results.filter(r=>!r.archived&&r.adId);
  const periodKey=r=>r.periodStart+'~'+r.periodEnd;
  const duration=r=>Math.round((Date.parse(r.periodEnd)-Date.parse(r.periodStart))/86400000)+1;
  const ranges=new Map();for(const r of reports){const key=periodKey(r),old=ranges.get(key);ranges.set(key,{key,start:r.periodStart,end:r.periodEnd,days:duration(r),updatedAt:[old?.updatedAt||'',r.updatedAt||''].sort().at(-1)});}
  const periods=[...ranges.values()].sort((a,b)=>b.updatedAt.localeCompare(a.updatedAt)||b.end.localeCompare(a.end)||a.start.localeCompare(b.start));
  const period=periods.find(p=>p.key===selectedPeriod)||periods[0];
  if(!period)return {date:null,days:0,periods:[],spend:null,groups:[],actions:[],creatives:[],note:'Upload a performance report to see findings and next actions.'};
  const date=period.start===period.end?period.start:period.start+' – '+period.end;
  const latest=reports.filter(r=>periodKey(r)===period.key), groups=new Map(),actions=[],creatives=[];
  let spend=0,missingSpend=false;
  for(const r of latest){
    if(r.spend===null||r.spend===undefined)missingSpend=true;else spend+=r.spend;
    const ad=state.ads.find(a=>a.id===r.adId),set=state.adSets.find(s=>s.id===r.adSetId);if(!ad||!set)continue;
    const goal=r.reportGoal||set.performanceGoal,destination=r.reportDestination||set.destination;
    const metric=destination==='WhatsApp'?'messagingStarts':/PROFILE_VISIT/i.test(goal||'')?'profileVisits':/PURCHASE/i.test(goal||'')?'metaPurchases':null;
    const label={messagingStarts:'Messaging conversations',profileVisits:'Instagram profile visits',metaPurchases:'Meta-attributed purchases'}[metric]||'Delivery';
    const count=metric?r[metric]:null,cost=count>0&&r.spend!=null?r.spend/count:null;
    const prior=reports.filter(x=>x.adId===r.adId&&x.periodEnd<r.periodStart&&duration(x)===duration(r)&&x.attributionSetting===r.attributionSetting&&(x.reportGoal||goal)===goal).sort((a,b)=>b.periodEnd.localeCompare(a.periodEnd))[0];
    const previousCost=prior&&metric&&prior[metric]>0&&prior.spend!=null?prior.spend/prior[metric]:null;
    const item={ad:ad.name,adSet:set.name,adId:ad.id,goal:label,spend:r.spend,count,cost,previousDate:prior?(prior.periodStart===prior.periodEnd?prior.periodStart:prior.periodStart+' – '+prior.periodEnd):null,previousCost,impressions:r.impressions,videoPlays3s:r.videoPlays3s,thruPlays:r.thruPlays,reportedDelivery:r.reportedDelivery};
    creatives.push(item);
    if(metric&&cost!==null){const key=label+' · '+(r.attributionSetting||'attribution unconfirmed')+' · '+(goal||'goal unconfirmed');if(!groups.has(key))groups.set(key,[]);groups.get(key).push(item);}
    if(r.impressions===0&&r.spend===0)actions.push({priority:3,title:'No delivery to judge',ad:ad.name,reason:'This report has zero spend and zero impressions.',next:'Check delivery in Meta if this ad was intended to run. Keep it unranked until it delivers.'});
    if(destination==='WhatsApp'&&goal==='LINK_CLICKS')actions.push({priority:1,title:'Review the WhatsApp optimisation goal',ad:ad.name+' · '+set.name,reason:`The saved setup optimises link clicks. ${r.linkClicks==null?'Clicks unreported':r.linkClicks+' link clicks'}; ${r.messagingStarts==null?'conversations unreported':r.messagingStarts+' conversations'} in this report.`,next:'If the goal is enquiries, check whether a messaging conversation goal is available in Meta. Judge conversations and their quality before raising the budget.'});
    if(previousCost!==null&&cost!==null&&cost>previousCost*1.25)actions.push({priority:2,title:'Cost increased — investigate',ad:ad.name,reason:`Cost per ${label.toLowerCase()} rose more than 25% versus ${prior.periodStart}. This is a review flag, not proof of fatigue.`,next:'Compare the same reporting window and check audience, placements and recent edits. Do not stop the ad on this change alone.'});
  }
  if(latest.every(r=>r.metaPurchases==null&&r.orders==null))actions.push({priority:1,title:'Sales are not established',ad:'All ads',reason:'Purchase and confirmed-order fields are unavailable in this upload.',next:'Use your enquiry and order records to judge buying quality. Blank purchase fields do not mean zero sales.'});
  const rankings=[...groups].map(([label,items])=>({label,items:items.sort((a,b)=>a.cost-b.cost),winner:items.length>1?items[0]:null}));
  for(const g of rankings)if(g.winner)actions.push({priority:4,title:'Keep testing the lower-cost option',ad:g.winner.ad,reason:`Lowest observed cost in the ${g.label} comparison for ${date}. Audiences and delivery can differ.`,next:'Keep collecting daily results and confirm business quality. This ranking alone does not justify increasing budget.'});
  actions.sort((a,b)=>a.priority-b.priority);
  return {date,days:period.days,periodStart:period.start,periodEnd:period.end,selectedPeriod:period.key,periods,spend:missingSpend?null:spend,groups:rankings,actions:actions.slice(0,5),creatives,note:'Findings use only this reporting period. Overlapping reports are stored separately and never added together. Period totals cannot establish individual daily results. Rankings compare observed outcomes, not causal creative quality.'};
}
module.exports={insights};

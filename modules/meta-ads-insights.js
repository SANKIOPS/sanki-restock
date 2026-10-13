'use strict';
function insights(state) {
  const daily=state.results.filter(r=>!r.archived&&r.adId&&r.periodStart===r.periodEnd);
  const dates=[...new Set(daily.map(r=>r.periodStart))].sort(),date=dates.at(-1);
  if(!date)return {date:null,days:0,spend:null,groups:[],actions:[],creatives:[],note:'Upload a daily performance report to see findings and next actions.'};
  const latest=daily.filter(r=>r.periodStart===date), groups=new Map(),actions=[],creatives=[];
  let spend=0,missingSpend=false;
  for(const r of latest){
    if(r.spend===null||r.spend===undefined)missingSpend=true;else spend+=r.spend;
    const ad=state.ads.find(a=>a.id===r.adId),set=state.adSets.find(s=>s.id===r.adSetId);if(!ad||!set)continue;
    const goal=r.reportGoal||set.performanceGoal,destination=r.reportDestination||set.destination;
    const metric=destination==='WhatsApp'?'messagingStarts':/PROFILE_VISIT/i.test(goal||'')?'profileVisits':/PURCHASE/i.test(goal||'')?'metaPurchases':null;
    const label={messagingStarts:'Messaging conversations',profileVisits:'Instagram profile visits',metaPurchases:'Meta-attributed purchases'}[metric]||'Delivery';
    const count=metric?r[metric]:null,cost=count>0&&r.spend!=null?r.spend/count:null;
    const prior=daily.filter(x=>x.adId===r.adId&&x.periodStart<date&&x.attributionSetting===r.attributionSetting&&(x.reportGoal||goal)===goal).sort((a,b)=>b.periodStart.localeCompare(a.periodStart))[0];
    const previousCost=prior&&metric&&prior[metric]>0&&prior.spend!=null?prior.spend/prior[metric]:null;
    const item={ad:ad.name,adSet:set.name,adId:ad.id,goal:label,spend:r.spend,count,cost,previousDate:prior?.periodStart||null,previousCost,impressions:r.impressions,videoPlays3s:r.videoPlays3s,thruPlays:r.thruPlays,reportedDelivery:r.reportedDelivery};
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
  return {date,days:dates.length,spend:missingSpend?null:spend,groups:rankings,actions:actions.slice(0,5),creatives,note:'Findings use the latest uploaded day. Rankings compare reported outcomes and attribution windows, not causal creative quality. No live ad changes are made.'};
}
module.exports={insights};

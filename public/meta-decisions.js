'use strict';
(function(root){
  const defaults={profile:{increaseCost:15,keepBelow:21,visitMax:1,minSpend:500,minDays:3},messages:{increaseCost:null,keepBelow:null,minSpend:500,minDays:3},sales:{increaseCost:null,keepBelow:null,minRoas:null,minSpend:500,minDays:3},store:{increaseCost:null,keepBelow:null,minSpend:500,minDays:3}};
  function assess(m,goal,settings,days){
    const t=settings[goal]||defaults[goal],out=(action,reason)=>({action,reason});
    if(m.spend==null||!days||days<t.minDays||m.spend<t.minSpend)return out('Needs data','Needs '+t.minDays+' covered days and ₹'+t.minSpend+' spend.');
    if(t.increaseCost==null||t.keepBelow==null||(goal==='sales'&&t.minRoas==null))return out('Set targets','Save targets for this goal first.');
    const count=m[{profile:'instagramFollows',messages:'messagingStarts',sales:'metaPurchases',store:'storeVisits'}[goal]];
    if(count==null)return out('Needs data','Required outcome count is not supplied.');
    const cost=count>0?m.spend/count:Infinity;
    if(cost>=t.keepBelow)return out('Pause','Cost per '+{profile:'follow',messages:'conversation',sales:'purchase',store:'verified visit'}[goal]+' is '+(Number.isFinite(cost)?'₹'+cost.toFixed(2):'unavailable: zero outcomes')+'; pause threshold is ₹'+t.keepBelow+'.');
    if(goal==='sales'&&(m.metaPurchaseValue==null||m.spend<=0))return out('Needs data','Revenue is needed to check ROAS.');
    if(goal==='sales'&&m.metaPurchaseValue/m.spend<t.minRoas)return out('Keep','ROAS is below the increase target; do not increase.');
    if(cost<=t.increaseCost){
      if(goal==='profile'){
        if(m.profileVisits==null||m.profileVisits<=0)return out('Needs data','Profile visits are needed before increasing.');
        if(m.spend/m.profileVisits>t.visitMax)return out('Keep','Follower cost qualifies, but visit cost exceeds ₹'+t.visitMax+'.');
      }
      return out('Increase','Increase targets met. Review recent changes before raising budget.');
    }
    return out('Keep','Outcome cost is below the pause threshold; leave budget unchanged.');
  }
  const api={defaults,assess};if(typeof module==='object'&&module.exports)module.exports=api;else root.MetaDecisions=api;
})(typeof window==='object'?window:this);

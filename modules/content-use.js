'use strict';
const STATUSES=['not_used','active','previously_used'];
function resolve(item,ads=[]){const linked=ads.filter(a=>(item.adIds||[]).includes(a.id)),known=linked.map(a=>String(a.archived?'archived':a.effectiveStatus||a.status||'').trim().toLowerCase()),automatic=known.some(s=>['active','running','learning'].includes(s))?'active':known.some(s=>['paused','inactive','completed','archived','deleted'].includes(s))?'previously_used':'not_used';return {useStatus:STATUSES.includes(item.useStatus)?item.useStatus:automatic,useCampaign:item.useCampaign||linked.map(a=>a.name).filter(Boolean).join(', '),useAutomatic:!STATUSES.includes(item.useStatus),useLinked:linked.length>0};}
module.exports={STATUSES,resolve};

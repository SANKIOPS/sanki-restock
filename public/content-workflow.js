(function(root,factory){if(typeof module==='object'&&module.exports)module.exports=factory();else root.ContentWorkflow=factory();})(typeof window==='object'?window:this,function(){
 'use strict';
 const personName=v=>String(v||'').trim().toLowerCase().replace(/(^|[\s’'-])\p{L}/gu,c=>c.toUpperCase());
 const modelKey=v=>personName(v).toLowerCase();
 function modelList(items,genders={}){const names=new Map();for(const i of items){if(i.archived)continue;const key=modelKey(i.model);if(!key||key==='all')continue;const row=names.get(key)||{key,name:personName(i.model),count:0,gender:genders[key]||''};if(i.shootStatus==='shot'&&isVideo(i)&&!['Delivery Videos','My Journey'].includes(i.source?.tab))row.count++;names.set(key,row);}return [...names.values()].sort((a,b)=>b.count-a.count||a.name.localeCompare(b.name));}
 const TYPES=['Model aesthetics','UGC','Funny video','Product only'];
 const videoType=t=>TYPES.includes(t)?t:({'Model showcase':'Model aesthetics','Funny':'Funny video'}[t]||'');
 function isPosted(i,publications,account){
  if(i.postingOverride==='not_posted')return false;
  if(i.postingOverride==='posted')return !account||!i.accounts.length||i.accounts.includes(account);
  if(publications.some(p=>p.contentId===i.id&&(!account||p.account===account)))return true;
  return /^posted$/i.test(String(i.source?.posted||'').trim())&&(!account||!i.accounts.length||i.accounts.includes(account));
 }
 function stage(i,publications=[]){
  if(i.accounts.length?i.accounts.every(a=>isPosted(i,publications,a)):isPosted(i,publications))return 'posted';
  if(i.shootStatus!=='shot')return 'shoot';
  if(i.editStatus!=='done')return 'edit';
  return i.finalUrl?'post':'file';
 }
 const isVideo=i=>!/^image|photo|carousel$/i.test(i.contentFormat||'');
 function summary(items,publications=[],includeLibrary=false){const videos=items.filter(i=>(includeLibrary||!['Delivery Videos','My Journey'].includes(i.source?.tab))&&!i.archived&&i.shootStatus==='shot'&&isVideo(i));return {total:videos.length,posted:videos.filter(i=>stage(i,publications)==='posted').length,ready:videos.filter(i=>stage(i,publications)==='post').length,notEdited:videos.filter(i=>stage(i,publications)==='edit').length};}
 function teamActivity(items,from,to,team=[]){
  const groups=new Map(),missing={shot:[],edited:[]};
  const group=n=>{n=personName(n)||'Unassigned';if(!groups.has(n))groups.set(n,{name:n,shot:[],editing:[],edited:[],pending:[],lastUpdate:''});return groups.get(n);};team.forEach(group);
  const inRange=d=>!!d&&d>=from&&d<=to;
  for(const i of items){if(i.archived||i.shootStatus!=='shot'||!isVideo(i)||['Delivery Videos','My Journey'].includes(i.source?.tab))continue;
   const shot=group(i.shotBy||i.shooter||i.source?.shooter),edit=group(i.editedBy||i.editor||i.source?.editor),editedDate=i.editedDate||i.source?.editedDate;
   if(inRange(i.shootDate))shot.shot.push(i.id);if(!i.shootDate)missing.shot.push(i.id);
   if(i.editStatus==='editing')edit.editing.push(i.id);else if(i.editStatus==='done'){if(inRange(editedDate))edit.edited.push(i.id);if(!editedDate)missing.edited.push(i.id);}else edit.pending.push(i.id);
   for(const g of [shot,edit])if((i.updatedAt||'')>g.lastUpdate)g.lastUpdate=i.updatedAt;
  }
  return {people:[...groups.values()].sort((a,b)=>{const ai=team.map(personName).indexOf(a.name),bi=team.map(personName).indexOf(b.name);return (ai<0?999:ai)-(bi<0?999:bi)||a.name.localeCompare(b.name);}),missing};
 }
 return {teamActivity,personName,modelKey,modelList,isPosted,stage,isVideo,summary,TYPES,videoType};
});

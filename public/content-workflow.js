(function(root,factory){if(typeof module==='object'&&module.exports)module.exports=factory();else root.ContentWorkflow=factory();})(typeof window==='object'?window:this,function(){
 'use strict';
 function inDateRange(value,from='',to=''){if(from&&to&&from>to)return false;if(!from&&!to)return true;return !!value&&(!from||value>=from)&&(!to||value<=to);}
 function contentDate(i,basis='latest'){return basis==='shoot'?i.shootDate||'':basis==='edited'?i.editedDate||i.source?.editedDate||'':basis==='poa'?i.poaDate||'':i.editedDate||i.source?.editedDate||i.shootDate||'';}
 const personName=v=>String(v||'').trim().toLowerCase().replace(/(^|[\s’'-])\p{L}/gu,c=>c.toUpperCase());
 const modelKey=v=>personName(v).toLowerCase();
 function modelList(items,genders={}){const names=new Map();for(const i of items){if(i.archived)continue;const key=modelKey(i.model);if(!key||key==='all')continue;const row=names.get(key)||{key,name:personName(i.model),count:0,gender:genders[key]||''};if(isShot(i)&&isVideo(i)&&!['Delivery Videos','My Journey'].includes(i.source?.tab))row.count++;names.set(key,row);}return [...names.values()].sort((a,b)=>b.count-a.count||a.name.localeCompare(b.name));}
 const contentKind=i=>i.contentKind==='story'?'story':'video';
 const needsCorrection=i=>['reshoot','reedit'].includes(i.correctionStatus);
 const editStatus=i=>needsCorrection(i)?(i.editStatus==='editing'?'editing':'pending'):i.finalUrl?'done':i.editStatus;
 const isShot=i=>i.shootStatus==='shot'||!!i.finalUrl;
 const withFinalStatus=i=>i.finalUrl&&!needsCorrection(i)?{...i,editStatus:'done',shootStatus:'shot'}:i;
 const TYPES=['Model aesthetics','UGC','Funny video','Product only','Gasolina','Product showcasing'];
 const contentType=i=>/gasolina/i.test(i.title||'')?'Gasolina':videoType(i.type);
 const withContentType=i=>/gasolina/i.test(i.title||'')?{...i,type:'Gasolina'}:i;
 const videoType=t=>TYPES.includes(t)?t:({'Model showcase':'Model aesthetics','Funny':'Funny video'}[t]||'');
 function isPosted(i,publications=[],account){
  if(i.postingByAccount&&Object.keys(i.postingByAccount).length){if(!account)return i.accounts.length>0&&i.accounts.every(a=>isPosted(i,publications,a));if(Object.prototype.hasOwnProperty.call(i.postingByAccount,account))return i.postingByAccount[account]==='posted';}
  if(i.postingOverride==='not_posted')return false;
  if(i.postingOverride==='posted')return !account||!i.accounts.length||i.accounts.includes(account);
  if(publications.some(p=>p.contentId===i.id&&(!account||p.account===account)))return true;
  return /^posted$/i.test(String(i.source?.posted||'').trim())&&(!account||!i.accounts.length||i.accounts.includes(account));
 }
 function postedOn(i,publications=[],account){if(!isPosted(i,publications,account))return '';if(i.postedOnByAccount?.[account])return i.postedOnByAccount[account];return publications.filter(p=>p.contentId===i.id&&p.account===account&&p.date).map(p=>p.date).sort().pop()||'';}
 function istMinute(now=new Date()){return now.toLocaleString('sv-SE',{timeZone:'Asia/Kolkata'}).slice(0,16);}
 function postingState(i,publications=[],account,now=istMinute()){
  const p=i.postingPlans?.[account],valid=p&&/^\d{4}-\d{2}-\d{2}$/.test(p.date||'')&&/^([01]\d|2[0-3]):[0-5]\d$/.test(p.time||'');
  const status=isPosted(i,publications,account)?'posted':!valid?'not_planned':p.date+' '+p.time<now?'overdue':'planned';
  return {status,label:{posted:'Posted',not_planned:'Not planned',overdue:'Overdue',planned:'Planned'}[status],date:valid?p.date:'',time:valid?p.time:'',owner:valid?p.owner||'':'',blocked:needsCorrection(i)};
 }
 function planTaskVisible(t,username,postingOverview=false){const people=t.participants||t.owners||[];return (postingOverview&&t.kind==='post')||!people.length||people.includes(username);}
 function postingQueue(items,publications=[],focus='today',today=istMinute().slice(0,10),now=istMinute()){
  return items.filter(i=>!i.archived).flatMap(i=>(i.accounts||[]).map(account=>{const p=postingState(i,publications,account,now);return {contentId:i.id,account,...p};})).filter(p=>p.date&&p.status!=='posted'&&(focus==='today'?p.date===today:focus==='overdue'?p.status==='overdue':false));
 }
 function stage(i,publications=[]){
  if(needsCorrection(i))return i.correctionStatus;
  if(i.accounts.length?i.accounts.every(a=>isPosted(i,publications,a)):isPosted(i,publications))return 'posted';
  if(i.finalUrl)return 'post';
  if(i.shootStatus!=='shot')return 'shoot';
  if(i.editStatus!=='done')return 'edit';
  return i.finalUrl?'post':'file';
 }
 const isVideo=i=>!/^image|photo|carousel$/i.test(i.contentFormat||'');
 function summary(items,publications=[],includeLibrary=false,kind='video',account=''){const videos=items.filter(i=>(includeLibrary||!['Delivery Videos','My Journey'].includes(i.source?.tab))&&!i.archived&&contentKind(i)===kind&&isShot(i)&&(kind==='story'||isVideo(i)));return {total:videos.length,posted:videos.filter(i=>account?!needsCorrection(i)&&isPosted(i,publications,account):stage(i,publications)==='posted').length,ready:videos.filter(i=>account?!needsCorrection(i)&&!!i.finalUrl&&!isPosted(i,publications,account):stage(i,publications)==='post').length,notEdited:videos.filter(i=>stage(i,publications)==='edit').length};}
 function teamActivity(items,from,to,team=[]){
  const groups=new Map(),missing={shot:[],edited:[]};
  const group=n=>{n=personName(n)||'Unassigned';if(!groups.has(n))groups.set(n,{name:n,shot:[],editing:[],edited:[],pending:[],lastUpdate:''});return groups.get(n);};team.forEach(group);
  const inRange=d=>!!d&&d>=from&&d<=to;
  for(const i of items){if(i.archived||!isShot(i)||(contentKind(i)!=='story'&&!isVideo(i))||['Delivery Videos','My Journey'].includes(i.source?.tab))continue;
   const shot=group(i.shotBy||i.shooter||i.source?.shooter),edit=group(i.editedBy||i.editor||i.source?.editor),editedDate=i.editedDate||i.source?.editedDate;
   if(inRange(i.shootDate))shot.shot.push(i.id);if(!i.shootDate)missing.shot.push(i.id);
   if(editStatus(i)==='editing')edit.editing.push(i.id);else if(editStatus(i)==='done'){if(inRange(editedDate))edit.edited.push(i.id);if(!editedDate)missing.edited.push(i.id);}else edit.pending.push(i.id);
   for(const g of [shot,edit])if((i.updatedAt||'')>g.lastUpdate)g.lastUpdate=i.updatedAt;
  }
  return {people:[...groups.values()].sort((a,b)=>{const ai=team.map(personName).indexOf(a.name),bi=team.map(personName).indexOf(b.name);return (ai<0?999:ai)-(bi<0?999:bi)||a.name.localeCompare(b.name);}),missing};
 }
 function editorTeamActivity(items,from,to){const names=['Ishnoor','Karan','Lokesh'],data=teamActivity(items,from,to,names),ids=new Map(items.map(i=>[i.id,i]));data.people=names.map(n=>data.people.find(p=>p.name===n));data.missing.shot=data.missing.shot.filter(id=>{const i=ids.get(id);return names.includes(personName(i.shotBy||i.shooter||i.source?.shooter));});data.missing.edited=data.missing.edited.filter(id=>{const i=ids.get(id);return names.includes(personName(i.editedBy||i.editor||i.source?.editor));});return data;}
 return {postedOn,planTaskVisible,postingQueue,istMinute,postingState,needsCorrection,editorTeamActivity,inDateRange,contentDate,contentKind,contentType,withContentType,editStatus,isShot,withFinalStatus,teamActivity,personName,modelKey,modelList,isPosted,stage,isVideo,summary,TYPES,videoType};
});

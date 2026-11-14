(function(root,factory){if(typeof module==='object'&&module.exports)module.exports=factory();else root.ContentWorkflow=factory();})(typeof window==='object'?window:this,function(){
 'use strict';
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
 return {isPosted,stage,isVideo,summary,TYPES,videoType};
});

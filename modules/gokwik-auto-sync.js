'use strict';
const fs=require('fs'),path=require('path'),api=require('./gokwik-api');
let active=null;
const read=file=>{try{return JSON.parse(fs.readFileSync(file,'utf8'));}catch(e){if(e.code==='ENOENT')return {days:{}};throw e;}};
function status(file){const s=read(file);return {configured:!!(process.env.GOKWIK_APP_ID&&process.env.GOKWIK_APP_SECRET),running:!!active,lastSync:s.lastSync||null,lastAttempt:s.lastAttempt||null,error:s.error||null,coveredThrough:s.coveredThrough||null,earliestDate:s.earliestDate||null,daysSynced:Object.keys(s.days||{}).length,rows:Object.values(s.days||{}).reduce((n,r)=>n+r.length,0),backfillPending:!!s.backfillPending};}
function sync({file,orders,write,force=false,now=new Date(),maxDays=400,preferredDates=[],fetchDay=api.fetchDay,pause=ms=>new Promise(r=>setTimeout(r,ms))}){
 if(active)return active;
 active=(async()=>{
  let state=read(file);state.days||={};
  if(!force&&state.nextAttemptAt&&Date.parse(state.nextAttemptAt)>now.getTime())return {skipped:true};
  if(!process.env.GOKWIK_APP_ID||!process.env.GOKWIK_APP_SECRET)throw Error('GoKwik API credentials are not configured');
  // Provider's documented example excludes T-2 itself, so use completed T-3 days.
  const through=api.shift(api.day(now),-3);
  const dates=Object.values(orders).filter(o=>o.channel!=='POS'&&o.createdAt).map(o=>api.day(o.createdAt)).sort();
  const earliest=process.env.GOKWIK_SETTLEMENT_START_DATE||dates[0];
  if(!earliest||!/^\d{4}-\d{2}-\d{2}$/.test(earliest)||earliest>through)return {skipped:true};
  const recent=Array.from({length:7},(_,i)=>api.shift(through,-i)).filter(d=>d>=earliest),missing=[];
  for(let date=earliest;date<=through;date=api.shift(date,1))if(!Object.hasOwn(state.days,date))missing.push(date);
  const priority=preferredDates.filter(d=>/^\d{4}-\d{2}-\d{2}$/.test(d)&&d>=earliest&&d<=through);
  const todo=[...new Set([...priority,...recent,...missing])];
  state={...state,earliestDate:earliest,lastAttempt:now.toISOString(),error:null,backfillPending:missing.length>0};write(file,JSON.stringify(state));
  try{
   for(const date of todo.slice(0,maxDays)){
    const rows=await fetchDay({date,appId:process.env.GOKWIK_APP_ID,appSecret:process.env.GOKWIK_APP_SECRET});
    if((state.days[date]||[]).length&&!rows.length)throw Error('GoKwik returned no rows for a previously populated settlement date '+date);
    state.days[date]=rows;state.lastSync=new Date().toISOString();state.error=null;
    state.coveredThrough=Object.keys(state.days).sort().at(-1)||null;
    // Commit a date only after all its pages were fetched and validated.
    write(file,JSON.stringify(state));await pause(1000);
   }
   state.backfillPending=missing.some(d=>!Object.hasOwn(state.days,d));state.nextAttemptAt=new Date(Date.now()+(state.backfillPending?60000:6*3600000)).toISOString();write(file,JSON.stringify(state));
   return {success:true,days:Object.keys(state.days).length,rows:Object.values(state.days).reduce((n,r)=>n+r.length,0),backfillPending:state.backfillPending};
  }catch(e){state.error=e.message;state.nextAttemptAt=new Date(Date.now()+30*60000).toISOString();write(file,JSON.stringify(state));throw e;}
 })().finally(()=>{active=null;});return active;
}
module.exports={sync,status,read};

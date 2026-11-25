'use strict';
const {rolesOf,isPrashant}=require('./auth-users');
const USERS=['karan','ishnoor'],TIMES=['12:00','13:30','15:00','16:30','18:00','19:30'];
const manager=u=>rolesOf(u).includes('owner')||isPrashant(u);
const name=u=>String(u?.username||'').toLowerCase();
const fail=(msg,status=400)=>{throw Object.assign(Error(msg),{status});};
function attach({load,change,now=()=>new Date()}){
 const clock=()=>new Intl.DateTimeFormat('sv-SE',{timeZone:'Asia/Kolkata',year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',hourCycle:'h23'}).format(now()).replace(',','');
 function view(u,requested){const current=clock(),today=current.slice(0,10),time=current.slice(-5),date=requested||today;if(!/^\d{4}-\d{2}-\d{2}$/.test(date)||!Number.isFinite(Date.parse(date))||new Date(date).toISOString().slice(0,10)!==date)fail('Choose a valid date.');const supervisor=manager(u),username=name(u);if(!supervisor&&!USERS.includes(username))return {enabled:false};const saved=load().uploadChecks?.[date]||{},users=supervisor?USERS:[username],rows=users.flatMap(user=>TIMES.map(slot=>({user,slot,...(saved[user]?.[slot]||{}),due:date<today||date===today&&slot<=time}))),active=date===today&&time>='12:00'&&time<'21:00',latest=active?TIMES.filter(t=>t<=time).at(-1):'';return {enabled:true,supervisor,date,today,users,rows,dueSlot:!supervisor&&latest&&!saved[username]?.[latest]?latest:'',revision:load().revision};}
 function save(b,u){const username=name(u);if(!USERS.includes(username))fail('Only Karan and Ishnoor can confirm their own upload checks.',403);const current=clock(),today=current.slice(0,10),time=current.slice(-5);if(b.date!==today||!TIMES.includes(b.slot)||b.slot>time)fail('Choose a due check for today.');if(!['done','pending'].includes(b.status))fail('Choose completed or pending.');const reason=String(b.reason||'').trim().slice(0,1000);if(b.status==='pending'&&!reason)fail('Add a reason for pending uploads or entries.');if(b.status==='done'&&(!Array.isArray(b.checks)||!['uploads','entries','links'].every(k=>b.checks.includes(k))))fail('Tick all three checks before confirming.');return change(b.revision,u.username,s=>{s.uploadChecks=s.uploadChecks||{};const day=s.uploadChecks[today]||(s.uploadChecks[today]={}),person=day[username]||(day[username]={});person[b.slot]={status:b.status,reason:b.status==='pending'?reason:'',checks:b.status==='done'?['uploads','entries','links']:[],by:u.username,at:now().toISOString()};return {action:'upload_check_'+b.status,id:today+':'+username+':'+b.slot};});}
 return {view,save};
}
module.exports={attach,USERS,TIMES};

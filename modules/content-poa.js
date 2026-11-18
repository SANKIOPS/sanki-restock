'use strict';
const crypto=require('crypto'),w=require('../public/content-workflow');
const {rolesOf,hasUserPermission}=require('./auth-users');
const canPlan=u=>rolesOf(u).includes('owner')||hasUserPermission(u,'contentPOA');
const owns=u=>rolesOf(u).includes('owner');
const fail=(m,status=400)=>{throw Object.assign(Error(m),{status});};
const text=(v,n=1000)=>String(v??'').trim().slice(0,n);
const SOP={lighting:'Check lighting before recording. Avoid unwanted shadows, flickering lights and reflections.',transitions:'Match the action across transitions. Keep transitions smooth and synchronise cuts with the music’s beat.',funny:'Use suitable sound effects and highlight facial reactions to strengthen the humour.'};
function progress(task,items,publications=[]){const rows=items.filter(i=>i.poaId===task.id&&!i.archived);return {linked:rows.length,shot:rows.filter(i=>i.rawUrl||w.isShot(i)).length,editing:rows.filter(i=>w.editStatus(i)==='editing').length,edited:rows.filter(i=>!!i.finalUrl).length,posted:rows.filter(i=>w.stage(i,publications)==='posted').length,ids:rows.map(i=>i.id)};}
function assigned(t,u){return [...t.shooters,t.editor,t.publisher].includes(u.username);}
function visible(t,u){return canPlan(u)||assigned(t,u);}
function attach({load,change,users,catalog}){
 function save(b,u,id){if(!canPlan(u))fail('POA creation permission is required.',403);return change(b.revision,u.username,s=>{s.poas=s.poas||[];const old=id?s.poas.find(t=>t.id===id):null;if(id&&!old)fail('POA not found.',404);if(old&&b.expectedUpdatedAt&&b.expectedUpdatedAt!==old.updatedAt)fail('This POA was changed by someone else. Close and reopen it to review the latest version.',409);const body={...old,...b};
 const date=text(body.date,10);if(!/^\d{4}-\d{2}-\d{2}$/.test(date)||!Number.isFinite(Date.parse(date))||new Date(date).toISOString().slice(0,10)!==date)fail('Choose a valid work date.');
 if(!Array.isArray(body.accounts)||!body.accounts.length||body.accounts.some(a=>!['Femme','Homme','Fashion'].includes(a)))fail('Select posting accounts.');
 if(!['video','story'].includes(body.contentKind)||!w.TYPES.includes(body.type))fail('Choose a format and content type.');
 const quantity=Number(body.quantity);if(!Number.isInteger(quantity)||quantity<1||quantity>50)fail('Quantity must be from 1 to 50.');
 const user=v=>{v=text(v,100);const found=users(v);if(!v||!found||!rolesOf(found).some(r=>['owner','content_team'].includes(r)))fail('Select an existing content team user.');return found.username;};
 if(!Array.isArray(body.shooters)||!body.shooters.length||body.shooters.length>6)fail('Assign at least one person to shoot.');const shooters=[...new Set(body.shooters.map(user))],editor=user(body.editor),publisher=body.publisher?user(body.publisher):'';
 if(!Array.isArray(body.productIds)||body.productIds.length>30)fail('Select up to 30 products.');const valid=new Set(catalog().map(p=>p.id));if(body.productIds.some(p=>!valid.has(p)&&!old?.productIds.includes(p)))fail('Choose products from the app.');
 const scriptMode=body.scriptMode;if(!['text','link','na'].includes(scriptMode))fail('Choose script text, a file link, or N/A.');let script=scriptMode==='na'?'N/A':text(body.script,12000);if(!script)fail('Add the script or choose N/A.');if(scriptMode==='link'){try{const url=new URL(script);if(!['https:','http:'].includes(url.protocol)||url.username||url.password)throw Error();script=url.href;}catch{fail('Use a complete http or https script file link.');}}
 const title=text(body.title,200);if(!title)fail('Give the task a name.');const fields=Object.fromEntries(['outfits','props','cast','cameraSetup'].map(k=>{const value=text(body[k],2000);if(!value)fail('Complete '+({outfits:'outfits',props:'props',cast:'cast',cameraSetup:'camera setup'}[k])+' or enter N/A.');return [k,value];}));
 const now=new Date().toISOString(),t={...old,id:old?.id||crypto.randomUUID(),date,title,accounts:[...new Set(body.accounts)],contentKind:body.contentKind,type:body.type,quantity,shooters,editor,publisher,productIds:[...new Set(body.productIds)],scriptMode,script,...fields,instructions:text(body.instructions,4000),createdAt:old?.createdAt||now,createdBy:old?.createdBy||u.username,updatedAt:now,updatedBy:u.username,archived:old?.archived||false};
 if(progress(t,s.items,s.publications).linked>quantity)fail('Quantity cannot be less than the number of linked entries.');
 t.history=[...(old?.history||[]),{at:now,by:u.username,action:old?'updated':'created'}];if(old)Object.assign(old,t);else s.poas.push(t);return {action:old?'poa_update':'poa_create',id:t.id};});}
 function view(u){const s=load();return {tasks:(s.poas||[]).filter(t=>visible(t,u)).map(t=>({...t,progress:progress(t,s.items,s.publications),canRecord:!t.archived&&(canPlan(u)||assigned(t,u))})),canCreate:canPlan(u),sops:SOP};}
 function archive(b,u,id){if(!canPlan(u))fail('POA creation permission is required.',403);return change(b.revision,u.username,s=>{const t=(s.poas||[]).find(t=>t.id===id);if(!t)fail('POA not found.',404);t.archived=!!b.archived;t.updatedBy=u.username;t.updatedAt=new Date().toISOString();t.history.push({at:t.updatedAt,by:u.username,action:t.archived?'cancelled':'reopened'});return {action:t.archived?'poa_cancel':'poa_reopen',id};});}
 return {save,view,archive};
}
module.exports={attach,canPlan,progress,assigned,visible,SOP};

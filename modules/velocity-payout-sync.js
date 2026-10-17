'use strict';
// Fixed, read-only MCP allowlist. Never forwards arbitrary tools or credentials to report hosts.
const fs=require('fs'),crypto=require('crypto');
const URL='https://shazam.velocity.in/api/v1/mcp';
const ALLOWED=new Set(['list_cod_settlements','list_wallet_recharges','cod_settlement_metrics']);
const cents=v=>{if(v==null||v===''||!Number.isFinite(Number(v)))throw Error('Missing or invalid Velocity amount');return Math.round(Number(v)*100);};
const day=v=>new Date(Number(v)*1000+19800000).toISOString().slice(0,10);
function client({token,uid,fetchImpl=require('node-fetch')}){
 let session='',id=0;
 async function rpc(method,params){
  const headers={'Content-Type':'application/json',Accept:'application/json, text/event-stream','access-token':token,client:'mcp',uid};if(session)headers['Mcp-Session-Id']=session;
  const r=await fetchImpl(URL,{method:'POST',headers,body:JSON.stringify({jsonrpc:'2.0',id:++id,method,params}),redirect:'error',timeout:45000});
  if(!r.ok)throw Error('Velocity payout connection returned HTTP '+r.status);
  session=r.headers?.get('mcp-session-id')||session;const text=await r.text();let body;
  if(text.trim().startsWith('{'))body=JSON.parse(text);else{const events=text.split(/\r?\n/).filter(x=>x.startsWith('data:')).map(x=>JSON.parse(x.slice(5).trim()));body=events.find(x=>x.id===id);}
  if(!body||body.error||body.result?.isError)throw Error('Velocity payout request failed: '+(body?.error?.code||method));
  return body.result;
 }
 return {init:()=>rpc('initialize',{protocolVersion:'2024-11-05',capabilities:{},clientInfo:{name:'SANKI-reconciliation',version:'1.0'}}),call:async(name,args={})=>{if(!ALLOWED.has(name))throw Error('Read-only Velocity tool not allowed');const r=await rpc('tools/call',{name,arguments:args});if(r.structuredContent)return r.structuredContent;return JSON.parse((r.content||[]).find(c=>c.type==='text')?.text||'null');}};
}
async function listAll(api,name,args={}){
 const rows=[],seen=new Set();let total=null;
 for(let page=1;page<=1000;page++){
  const r=await api.call(name,{...args,page,per_page:10});
  if(!Array.isArray(r?.data)||!Number.isInteger(r.meta?.total)||r.meta.total<0||r.meta.current_page!==page)throw Error('Invalid Velocity payout pagination');
  if(total!==null&&total!==r.meta.total)throw Error('Velocity payout list changed during sync; retry needed');total=r.meta.total;
  for(const row of r.data){if(!row.id||seen.has(row.id))throw Error('Duplicate Velocity payout page');seen.add(row.id);rows.push(row);}
  if(rows.length===total)return rows;
  if(!r.data.length||rows.length>total)throw Error('Incomplete Velocity payout list');
 }
 throw Error('Velocity payout pagination limit exceeded');
}
function csv(text){
 const rows=[];let row=[],cell='',quoted=false;
 for(let i=0;i<text.length;i++){const c=text[i];if(c==='"'){if(quoted&&text[i+1]==='"'){cell+='"';i++;}else quoted=!quoted;}else if(c===','&&!quoted){row.push(cell);cell='';}else if((c==='\n'||c==='\r')&&!quoted){if(c==='\r'&&text[i+1]==='\n')i++;row.push(cell);if(row.some(Boolean))rows.push(row);row=[];cell='';}else cell+=c;}
 if(quoted)throw Error('Invalid Velocity settlement CSV');if(cell||row.length){row.push(cell);rows.push(row);}const headers=(rows.shift()||[]).map(v=>v.replace(/^\uFEFF/,'').trim());
 const required=['AWB Number','Order ID','Total Order Amount','UTR'];if(required.some(h=>!headers.includes(h)))throw Error('Velocity settlement CSV columns changed');
 return rows.map(values=>Object.fromEntries(headers.map((h,i)=>[h,values[i]||''])));
}
function allocations(text,batch){
 const rows=csv(text),seen=new Set();let total=0;
 const entries=rows.map(r=>{const awb=String(r['AWB Number']).trim(),type=String(r['Transaction Type']||'Credit').trim().toLowerCase(),amount=cents(r['Total Order Amount']);
  if(!awb||!['credit','debit'].includes(type)||amount<0||String(r.UTR).trim()!==batch.utr)throw Error('Invalid Velocity settlement allocation');
  const key=awb+'|'+type;if(seen.has(key))throw Error('Duplicate AWB in Velocity settlement');seen.add(key);total+=type==='debit'?-amount:amount;
  return {awb,orderReference:r['Order ID'].trim(),amount:amount/100,type,courier:r['Courier Partner']||'',deliveryDate:r['Delivery Date & Time']||''};});
 if(total!==cents(batch.gross))throw Error('Velocity settlement allocations do not equal batch collection');
 return entries;
}
function normalize(row,offsets){
 const a=row.attributes||{},gross=cents(a.amount),net=cents(a.settlement_amount),deduction=cents(a.total_deductions),credits=cents(a.credits??0);
 if(gross<0||net<0||deduction<0||gross-deduction+credits!==net||!a.unique_id||!a.utr||!Number.isFinite(a.transaction_date))throw Error('Velocity batch totals or identifiers require review');
 const matches=offsets.filter(x=>x.attributes?.status==='success'&&x.attributes.gateway==='cod_offset'&&String(x.attributes.notes||'').includes('#'+a.unique_id));
 const walletOffset=matches.reduce((n,x)=>n+cents(x.attributes.amount),0);
 if(walletOffset>deduction)throw Error('Velocity wallet offset exceeds payout deduction');
 return {id:row.id,batchId:a.unique_id,utr:a.utr,date:day(a.transaction_date),gross:gross/100,net:net/100,deduction:deduction/100,credits:credits/100,walletOffset:walletOffset/100,otherDeduction:(deduction-walletOffset)/100,deductionLabel:walletOffset===deduction&&deduction>0?'Velocity wallet recovery':'Velocity deduction',status:a.status,remarks:a.remarks||'',walletReferences:matches.map(x=>x.attributes.unique_id),source:'velocity_mcp',bankVerified:false};
}
async function fetchPayouts({token,uid,previous=[],fetchImpl=require('node-fetch')}){
 if(!token||!uid)throw Error('Velocity payout credentials are not configured');
 const api=client({token,uid,fetchImpl});await api.init();
 const raw=await listAll(api,'list_cod_settlements'),offsets=await listAll(api,'list_wallet_recharges',{gateway:['cod_offset']});const result=[];
 for(const row of raw){
  const batch=normalize(row,offsets),old=previous.find(x=>x.id===batch.id);batch.fingerprint=crypto.createHash('sha256').update(JSON.stringify(batch)).digest('hex');
  if(old?.fingerprint===batch.fingerprint&&old.allocationsComplete){batch.entries=old.entries;batch.allocationsComplete=true;}
  else try{
   const url=new global.URL(row.attributes.settlement_report);if(url.protocol!=='https:'||url.hostname!=='velocity-shazam-prod.s3.ap-south-1.amazonaws.com')throw Error('Unexpected Velocity report host');
   const response=await fetchImpl(url.href,{redirect:'error',timeout:45000,size:5*1024*1024});if(!response.ok)throw Error('Velocity settlement file returned HTTP '+response.status);
   batch.entries=allocations(await response.text(),batch);batch.allocationsComplete=true;
  }catch(e){batch.entries=[];batch.allocationsComplete=false;batch.allocationError=e.message;}
  result.push(batch);
 }
 return {batches:result};
}
let active=null;
function read(file){try{return JSON.parse(fs.readFileSync(file,'utf8'));}catch(e){if(e.code==='ENOENT')return {batches:[]};throw e;}}
function status(file){const s=read(file);return {configured:!!(process.env.VELOCITY_MCP_ACCESS_TOKEN&&process.env.VELOCITY_MCP_UID),running:!!active,lastSync:s.lastSync||null,lastAttempt:s.lastAttempt||null,error:s.error||null,batches:(s.batches||[]).length,allocationIssues:(s.batches||[]).filter(b=>!b.allocationsComplete).length,intervalMinutes:60};}
function sync({file,write,fetcher=fetchPayouts}){if(active)return active;active=(async()=>{const before=read(file),lastAttempt=new Date().toISOString();try{const data=await fetcher({token:process.env.VELOCITY_MCP_ACCESS_TOKEN,uid:process.env.VELOCITY_MCP_UID,previous:before.batches||[]});if((before.batches||[]).some(b=>!data.batches.some(x=>x.id===b.id)))throw Error('Velocity omitted previously known payouts; last successful data retained');write(file,JSON.stringify({...data,lastAttempt,lastSync:new Date().toISOString(),error:null}));return status(file);}catch(e){write(file,JSON.stringify({...before,lastAttempt,error:e.message}));throw e;}})().finally(()=>{active=null;});return active;}
module.exports={client,listAll,csv,allocations,normalize,fetchPayouts,sync,status,read};

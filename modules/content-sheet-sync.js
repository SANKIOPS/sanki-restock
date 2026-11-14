'use strict';
const fs=require('fs'),path=require('path'),crypto=require('crypto');
const SHEET_ID='1E_T8TbWXQTFF1nC_C52fqMnSi5Of_rFDdT6zq6fnPww';
const SOURCE='https://docs.google.com/spreadsheets/d/'+SHEET_ID+'/edit';
const EXPORT='https://docs.google.com/spreadsheets/d/'+SHEET_ID+'/export?format=xlsx';
const INTERVAL=5*60*1000;
function createSync(store,options={}){
 const file=options.file||path.join(process.env.DATA_PATH?path.dirname(process.env.DATA_PATH):path.join(__dirname,'..'),'content-sheet-sync.json');
 const now=options.now||(()=>new Date()),download=options.download||async function(){const r=await require('node-fetch')(EXPORT,{timeout:30000,size:10*1024*1024});if(!r.ok)throw Error('Google Sheet could not be read ('+r.status+').');const b=await r.buffer();if(b[0]!==0x50||b[1]!==0x4b)throw Error('Google did not return a workbook. Check sheet access.');return b;};
 let busy=false,timer;
 function read(){try{return JSON.parse(fs.readFileSync(file,'utf8'));}catch(e){if(e.code==='ENOENT')return {enabled:true};return {enabled:false,error:'Saved sync settings could not be read.'};}}
 function save(s){fs.mkdirSync(path.dirname(file),{recursive:true});const tmp=file+'.tmp-'+crypto.randomUUID();try{fs.writeFileSync(tmp,JSON.stringify(s),{mode:0o600});fs.renameSync(tmp,file);}finally{if(fs.existsSync(tmp))fs.unlinkSync(tmp);}}
 function status(){const s=read();return {enabled:s.enabled===true,running:busy,sourceUrl:SOURCE,intervalMinutes:5,lastAttempt:s.lastAttempt||'',lastSuccess:s.lastSuccess||'',error:s.error||'',added:s.added||0,updated:s.updated||0};}
 function configure(enabled){if(typeof enabled!=='boolean')throw Error('Choose whether sync is enabled.');const s=read();s.enabled=enabled;save(s);return status();}
 async function run(manual=false){if(busy)return status();if(!manual&&!read().enabled)return status();busy=true;const started=now().toISOString();try{const buffer=await download(),hash=crypto.createHash('sha256').update(buffer).digest('hex');const parsed=require('./content-production-import').parse(buffer);if(!parsed.records.length)throw Error('The sheet returned no content rows; saved records were preserved.');if(parsed.skipped||parsed.warnings.length)throw Error('Sheet has duplicate or ambiguous file rows. Resolve these before syncing.');if(!manual&&!read().enabled)return status();const before=read();let report={count:0,updated:0};if(before.hash!==hash){const actor={username:'Google Sheet sync',roles:['owner']};for(let attempt=0;attempt<2;attempt++){try{report=store.importSheet(buffer,store.load().revision,actor,true,true);if(report.count+report.updated)report=store.importSheet(buffer,store.load().revision,actor,false,true);break;}catch(e){if(e.status!==409||attempt)throw e;}}}const s=read();Object.assign(s,{lastAttempt:started,lastSuccess:now().toISOString(),error:'',hash,added:report.count,updated:report.updated});save(s);}catch(e){const s=read();Object.assign(s,{lastAttempt:started,error:String(e.message||'Sheet refresh failed.').slice(0,300)});save(s);}finally{busy=false;}return status();}
 function start(){if(timer)return;const initial=setTimeout(()=>run(),10000);initial.unref?.();timer=setInterval(()=>run(),INTERVAL);timer.unref?.();}
 function stop(){if(timer)clearInterval(timer);timer=null;}
 return {status,configure,run,start,stop};
}
module.exports={createSync,SOURCE,EXPORT,INTERVAL};

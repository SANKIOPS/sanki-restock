'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('fs'),os=require('os'),path=require('path'),express=require('express'),sharp=require('sharp');
const {createImageReviewRouter}=require('../modules/image-review');
test('queue bridges exact references/results, revisions and approval to recoverable Shopify replacement',async t=>{
 const root=fs.mkdtempSync(path.join(os.tmpdir(),'review-queue-')),png=await sharp({create:{width:80,height:100,channels:3,background:'#bbaacc'}}).png().toBuffer();
 const p={id:11,title:'Women top',handle:'top',tags:'SANKI Collection: SANKI Casuals, SANKI Gender: Women',options:[{name:'Color'}],variants:[{id:1,title:'Blue',option1:'Blue',sku:'BLUE'}],images:[{id:9,src:'https://cdn.shopify.com/old.png',alt:'old front',position:1,variant_ids:[1]},{id:10,src:'https://cdn.shopify.com/keep.png',alt:'keep back',position:2,variant_ids:[]}]};let nextId=20,failDelete=true,writes=[];
 const client={request:async(url,opts={})=>{const method=opts.method||'GET';let body;if(method==='GET')body=/products\/11\.json/.test(url)?{product:p}:{products:[p]};else{writes.push(method);const data=opts.body?JSON.parse(opts.body):null;if(method==='POST'){const image={id:nextId++,src:'https://cdn.shopify.com/generated.png',position:p.images.length+1,alt:data.image.alt,variant_ids:[]};p.images.push(image);body={image};}if(method==='PUT'){const id=Number(url.match(/images\/(\d+)/)[1]),image=p.images.find(i=>i.id===id);Object.assign(image,data.image);body={image};}if(method==='DELETE'){if(failDelete){failDelete=false;return{ok:false,status:503,json:async()=>({})};}const id=Number(url.match(/images\/(\d+)/)[1]);p.images=p.images.filter(i=>i.id!==id);body={};}}return{ok:true,status:method==='DELETE'?204:200,headers:{get:()=>null},json:async()=>structuredClone(body)};}};
 const app=express();app.use(express.json());app.use((req,res,next)=>{req.user={username:'owner',roles:[req.headers['x-role']||'owner']};next();});app.use(createImageReviewRouter({root,store:'test.myshopify.com',client,fetchImage:async()=>({ok:true,buffer:async()=>png})}));const server=app.listen(0,'127.0.0.1');await new Promise(r=>server.once('listening',r));t.after(()=>{server.close();fs.rmSync(root,{recursive:true,force:true});});const base='http://127.0.0.1:'+server.address().port+'/api/image-review';
 let item=(await(await fetch(base+'/catalogue')).json()).items[0];
 async function post(action,extra={}){const response=await fetch(base+'/items/'+item.id+'/'+action,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({fullSet:false,revision:item.review.revision,fingerprint:item.fingerprint,jobId:item.review.job?.id,lease:item.review.job?.lease,...extra})}),body=await response.json();if(body.item)item=body.item;return{status:response.status,body};}
 async function reload(){item=(await(await fetch(base+'/items/'+item.id)).json()).item;}
 async function upload(kind,extra={}){const form=new FormData();for(const[k,v]of Object.entries({kind,view:'front',revision:item.review.revision,fingerprint:item.fingerprint,jobId:item.review.job?.id||'',lease:item.review.job?.lease||'',...extra}))form.append(k,v);form.append('image',new Blob([png],{type:'image/png'}),'photo.png');const r=await fetch(base+'/items/'+item.id+'/photos',{method:'POST',body:form}),d=await r.json();if(d.item)item=d.item;return r.status;}
 assert.equal((await post('queue',{targets:{front:'9'}})).status,409);
 await upload('original');assert.equal((await post('queue',{targets:{front:'missing'}})).status,409);
 assert.equal((await post('queue',{targets:{front:'9'}})).status,200);assert.equal(item.status,'queued');assert.equal(writes.length,0);
 const packet=await(await fetch(base+'/items/'+item.id+'/packet')).json();assert.deepEqual(Buffer.from(packet.originals[0].base64,'base64'),png);assert.equal(packet.job.targets.front,'9');
 assert.equal((await post('publish')).status,409);assert.equal(await upload('proposal'),409);
 assert.equal((await post('claim')).status,200);assert.equal((await post('claim')).status,409);assert.equal(await upload('proposal',{lease:'wrong'}),409);
 assert.equal((await post('complete',{qa:'checked'})).status,409);assert.equal(await upload('proposal'),200);assert.equal((await post('complete',{qa:''})).status,409);assert.equal((await post('complete',{qa:'Checked colour and construction against front reference.'})).status,200);assert.equal(item.status,'ready');
 assert.equal((await post('revise',{feedback:'Preserve the front pocket shape'})).status,200);assert.equal(item.status,'queued');assert.equal(item.review.proposals.length,0);await post('claim');await upload('proposal');await post('complete',{qa:'Pocket corrected and reference checked'});
 const published=await post('publish');assert.equal(published.status,409);await reload();assert.equal(item.status,'publish_failed');assert.equal(writes.filter(m=>m==='POST').length,1);assert.equal(await upload('original'),409);
 const retry=await post('publish');assert.equal(retry.status,200,JSON.stringify(retry.body));assert.equal(item.status,'published');assert.equal(writes.filter(m=>m==='POST').length,1);assert.ok(p.images.some(i=>i.id===10));assert.ok(!p.images.some(i=>i.id===9));assert.deepEqual(p.images.find(i=>i.id===20).variant_ids,["1"]);
 const backup=item.review.job.receipts[0].backup;assert.deepEqual(Buffer.from(await(await fetch(base+'/files/'+backup.file)).arrayBuffer()),png);
 assert.equal((await post('publish')).status,409);assert.equal((await(await fetch(base+'/queue')).json()).items.length,0);
 assert.equal(await upload('original'),200);assert.equal(item.review.job,null);assert.deepEqual(item.review.pendingViews,['front']);
 assert.equal((await fetch(base+'/queue',{headers:{'x-role':'admin'}})).status,403);
 // A website change after generation blocks approval before any new write.
 await post('queue',{targets:{front:'20'}});await post('claim');await upload('proposal');await post('complete',{qa:'Checked reference'});const count=writes.length;p.images[1].src='https://cdn.shopify.com/externally-changed.png';assert.equal((await post('publish')).status,409);assert.equal(writes.length,count);
});

test('five-view jobs use three raw references and preserve uncommented results on per-photo revision',async t=>{
 const root=fs.mkdtempSync(path.join(os.tmpdir(),'review-five-')),png=await sharp({create:{width:80,height:100,channels:3,background:'#765544'}}).png().toBuffer();
 const p={id:71,title:'Brown trousers',handle:'brown',tags:'SANKI Collection: SANKI Casuals, SANKI Gender: Women',options:[{name:'Color'}],variants:[{id:2,title:'Brown',option1:'Brown',sku:'BROWN'}],images:[]};let writes=0;
 const client={request:async(url,opts={})=>{if(opts.method)writes++;return{ok:true,status:200,headers:{get:()=>null},json:async()=>/products\/71\.json/.test(url)?{product:p}:{products:[p]}};}};
 const app=express();app.use(express.json());app.use((req,res,next)=>{req.user={username:'owner',roles:['owner']};next();});app.use(createImageReviewRouter({root,store:'test.myshopify.com',client}));const server=app.listen(0,'127.0.0.1');await new Promise(r=>server.once('listening',r));t.after(()=>{server.close();fs.rmSync(root,{recursive:true,force:true});});const base='http://127.0.0.1:'+server.address().port+'/api/image-review';
 let item=(await(await fetch(base+'/catalogue')).json()).items[0];
 async function post(action,extra={}){const r=await fetch(base+'/items/'+item.id+'/'+action,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({revision:item.review.revision,fingerprint:item.fingerprint,jobId:item.review.job?.id,lease:item.review.job?.lease,...extra})});const d=await r.json();if(d.item)item=d.item;return {status:r.status,body:d};}
 async function upload(kind,view){const form=new FormData();for(const [k,v]of Object.entries({kind,view,revision:item.review.revision,fingerprint:item.fingerprint,jobId:item.review.job?.id||'',lease:item.review.job?.lease||''}))form.append(k,v);form.append('image',new Blob([png]),'image.png');const r=await fetch(base+'/items/'+item.id+'/photos',{method:'POST',body:form});const d=await r.json();if(d.item)item=d.item;return r.status;}
 for(const view of ['front','back','detail'])assert.equal(await upload('original',view),200);
 assert.equal(await upload('original','angle'),400);
 assert.equal((await post('queue',{targets:{front:'add',back:'add',detail:'add'}})).status,200);
 assert.deepEqual(item.review.job.outputViews,['front','detail','angle','back','product']);assert.equal(item.review.job.references.length,3);
 await post('claim');for(const view of ['front','back','detail'])assert.equal(await upload('proposal',view),200);
 assert.equal((await post('complete',{qa:'checked'})).status,409);
 for(const view of ['angle','product'])assert.equal(await upload('proposal',view),200);
 assert.equal((await post('complete',{qa:'All five checked against raw references'})).status,200);
 const kept=item.review.proposals.filter(f=>f.view!=='detail').map(f=>f.id);
 assert.equal((await post('revise',{viewFeedback:{detail:'Crop to waistband and top of thighs only'},fullSet:true})).status,200);
 assert.deepEqual(item.review.job.generationViews,['detail']);assert.equal(item.review.job.feedbackByView.detail,'Crop to waistband and top of thighs only');assert.deepEqual(item.review.proposals.map(f=>f.id),kept);
 const packet=await(await fetch(base+'/items/'+item.id+'/packet')).json();assert.equal(packet.originals.length,3);assert.deepEqual(packet.job.generationViews,['detail']);
 await post('claim');assert.equal((await post('complete',{qa:'checked'})).status,409);await upload('proposal','detail');assert.equal((await post('complete',{qa:'Tight detail corrected'})).status,200);assert.equal(item.review.proposals.length,5);assert.equal(writes,0);
 assert.equal((await post('revise',{viewFeedback:{unknown:'change'}})).status,400);
 assert.equal((await post('revise',{viewFeedback:{}})).status,409);
 // Earlier three-view jobs can be expanded without deleting the existing three results.
 await post('revise',{feedback:'legacy reset'});await post('queue',{fullSet:false,targets:{front:'add',back:'add',detail:'add'}});await post('claim');for(const view of ['front','back','detail'])await upload('proposal',view);await post('complete',{qa:'legacy checked'});
 assert.equal((await post('revise',{fullSet:true})).status,200);assert.deepEqual(item.review.job.generationViews,['angle','product']);assert.equal(item.review.proposals.length,3);
});

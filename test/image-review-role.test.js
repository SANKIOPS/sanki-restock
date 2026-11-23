'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),express=require('express'),fs=require('fs'),os=require('os'),path=require('path');
const {apiAllowedForUser}=require('../auth');const {ROLES,userCanAccessPath,landingForUser}=require('../modules/auth-users');const {visibleFor}=require('../modules/module-registry');const {createImageReviewRouter}=require('../modules/image-review');
const reviewer={username:'review-test',roles:['image_reviewer'],role:'image_reviewer'};
test('Image Reviewer is assignable and sees only the review page',()=>{
 assert.ok(ROLES.some(r=>r.id==='image_reviewer'&&!r.assignmentHidden));assert.equal(landingForUser(reviewer),'/image-review.html');assert.deepEqual(visibleFor(reviewer).map(m=>m.key),['image-review']);
 assert.equal(userCanAccessPath(reviewer,'/image-review.html'),true);for(const p of ['/admin-users.html','/dashboard.html','/manage.html','/expenses.html','/inventory.html','/catalogue-images.html'])assert.equal(userCanAccessPath(reviewer,p),false,p);
});
test('review role can submit work but cannot generate-return, approve, publish, export or access other APIs',()=>{
 for(const [method,p] of [['GET','/api/image-review/catalogue'],['GET','/api/image-review/items/abc'],['GET','/api/image-review/files/photo.png'],['POST','/api/image-review/items/abc/queue'],['POST','/api/image-review/items/abc/photos'],['POST','/api/image-review/items/abc/revise'],['PATCH','/api/image-review/items/abc']])assert.equal(apiAllowedForUser(reviewer,p,method),true,p);
 for(const [method,p] of [['POST','/api/image-review/items/abc/publish'],['POST','/api/image-review/items/abc/approve'],['POST','/api/image-review/items/abc/claim'],['POST','/api/image-review/items/abc/complete'],['GET','/api/image-review/items/abc/packet'],['GET','/api/image-review/queue'],['GET','/api/image-review/export'],['GET','/api/admin/users'],['GET','/api/expenses'],['POST','/api/inventory/adjust'],['GET','/api/future']])assert.equal(apiAllowedForUser(reviewer,p,method),false,p);
 assert.equal(apiAllowedForUser({roles:['owner']},'/api/image-review/items/abc/publish','POST'),true);
});
test('module enforces reviewer restrictions even without the outer auth gate',async t=>{
 const root=fs.mkdtempSync(path.join(os.tmpdir(),'review-role-'));const app=express();app.use(express.json());app.use((req,res,next)=>{req.user=reviewer;next();});app.use(createImageReviewRouter({root,store:'test.myshopify.com',client:{request:async()=>({ok:true,headers:{get:()=>null},json:async()=>({products:[]})})}}));const server=app.listen(0,'127.0.0.1');await new Promise(r=>server.once('listening',r));t.after(()=>{server.close();fs.rmSync(root,{recursive:true,force:true});});const url='http://127.0.0.1:'+server.address().port+'/api/image-review';const list=await(await fetch(url+'/catalogue')).json();assert.equal(list.canPublish,false);
 for(const action of ['publish','approve','claim','complete'])assert.equal((await fetch(url+'/items/abc/'+action,{method:'POST',headers:{'Content-Type':'application/json'},body:'{}'})).status,403);
 const form=new FormData();form.append('kind','proposal');form.append('view','front');assert.equal((await fetch(url+'/items/abc/photos',{method:'POST',body:form})).status,403);
 assert.equal((await fetch(url+'/queue')).status,403);assert.equal((await fetch(url+'/items/abc/packet')).status,403);
});

const test=require('node:test'),assert=require('node:assert/strict');
const {prepare,accept}=require('../modules/procurement-codex-batch');
const group={key:'design:blue',photoUrl:'/api/procurement/photo/source.jpg',colour:'Blue'};
test('Codex manifest locks product/colour and has five views',()=>{
 const batch=prepare({id:'po-1'},group);assert.equal(batch.groupKey,group.key);assert.equal(batch.views.length,5);
 assert.equal(batch.views.find(v=>v.type==='model-back').status,'needs-reference');
});
test('returned results stay unapproved and cannot cross changed source mapping',()=>{
 const b=prepare({id:'po-1'},group),imgs=[{type:'front',url:'/api/procurement/photo/result.jpg'}];
 assert.equal(accept(b,group,'',imgs,()=>true)[0].approved,false);
 assert.throws(()=>accept(b,{...group,colour:'Red'},'',imgs,()=>true),/changed/);
 assert.throws(()=>accept(b,group,'',[...imgs,...imgs],()=>true),/duplicate/);
 assert.throws(()=>accept(b,group,'',imgs,()=>false),/Upload/);
});

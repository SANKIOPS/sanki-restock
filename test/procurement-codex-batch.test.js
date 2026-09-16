const test=require('node:test'),assert=require('node:assert/strict');
const {prepare,accept,acceptSeo}=require('../modules/procurement-codex-batch');
const group={key:'design:blue',photoUrl:'/api/procurement/photo/source.jpg',colour:'Blue'};
test('Codex manifest locks product/colour and has five views',()=>{
 const batch=prepare({id:'po-1'},group);assert.equal(batch.groupKey,group.key);assert.equal(batch.views.length,5);
 assert.equal(batch.views.find(v=>v.type==='model-back').status,'needs-reference');
 assert.deepEqual(batch.listingCopy.requiredFields,['displayName','title','handle','metaTitle','metaDescription','imageAlt','tags','bodyHtml']);
});
test('Codex listing copy is source-locked, complete and not approved automatically',()=>{
 const batch=prepare({id:'po-1'},group),seo={displayName:'Blue Top',title:'Blue Top',handle:'blue-top',metaTitle:'Blue Top | SANKI',metaDescription:'Blue top by SANKI.',imageAlt:'Blue top on hanger',tags:['Blue','Top'],bodyHtml:'<p>Blue top.</p>'};
 assert.deepEqual(acceptSeo(batch,group,'',seo),seo);
 assert.throws(()=>acceptSeo(batch,{...group,colour:'Red'},'',seo),/changed/);
 assert.throws(()=>acceptSeo(batch,group,'',{...seo,imageAlt:''}),/Complete/);
 assert.throws(()=>acceptSeo(batch,group,'',{...seo,bodyHtml:'<script>alert(1)</script>'}),/Unsafe/);
});
test('returned results stay unapproved and cannot cross changed source mapping',()=>{
 const b=prepare({id:'po-1'},group),imgs=[{type:'front',url:'/api/procurement/photo/result.jpg'}];
 assert.equal(accept(b,group,'',imgs,()=>true)[0].approved,false);
 assert.throws(()=>accept(b,{...group,colour:'Red'},'',imgs,()=>true),/changed/);
 assert.throws(()=>accept(b,group,'',[...imgs,...imgs],()=>true),/duplicate/);
 assert.throws(()=>accept(b,group,'',imgs,()=>false),/Upload/);
});

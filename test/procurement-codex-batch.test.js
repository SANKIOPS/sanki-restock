const test=require('node:test'),assert=require('node:assert/strict');
const {prepare,accept,acceptSeo}=require('../modules/procurement-codex-batch');
const group={audience:'Women',productType:'Shirt',key:'design:blue',photoUrl:'/api/procurement/photo/source.jpg',colour:'Blue'};

test('Unisex free-generation manifests request only female and male front model photos',()=>{
 const g={...group,audience:'Unisex',productType:'Shirt'},batch=prepare({id:'po-1'},g);
 assert.deepEqual(batch.views.map(v=>v.type),['front','female','male']);
 assert.match(batch.views.find(v=>v.type==='female').prompt,/adult .*woman/);
 assert.match(batch.views.find(v=>v.type==='male').prompt,/adult .*man/);
 assert.equal(accept(batch,g,'',[{type:'female',url:'/f.jpg'},{type:'male',url:'/m.jpg'}],()=>true).length,2);
 assert.throws(()=>accept(batch,g,'',[{type:'model-side-female',url:'/s.jpg'}],()=>true),/Invalid/);
 const withBack=prepare({id:'po-1',backRefs:{[g.key]:'/back.jpg'}},g);
 assert.deepEqual(withBack.views.map(v=>v.type),['front','back','female','male']);
});
test('Codex manifest locks product/colour and uses supported views without an invented back',()=>{
 const batch=prepare({id:'po-1'},group);assert.equal(batch.groupKey,group.key);assert.deepEqual(batch.views.map(view=>view.type),['front','model-front','model-side']);
 assert.equal(batch.views.find(v=>v.type==='model-side').status,'needs-reference');
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

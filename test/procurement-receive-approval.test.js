const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const source = fs.readFileSync(path.join(__dirname, '../modules/procurement.js'), 'utf8');
const html = fs.readFileSync(path.join(__dirname, '../public/procurement.html'), 'utf8');
const receiveRoute = source.slice(source.indexOf("router.post('/api/procurement/pos/:id/receive'"), source.indexOf('// ── Explicitly confirm'));
const sourceGroup = source.slice(source.indexOf('function studioSourceGroup('), source.indexOf('// A design-code correction'));
const readyFunction = html.slice(html.indexOf('    function studioReadyFor('), html.indexOf('    // Load the NEW-product groups'));
const typesFunction = html.slice(html.indexOf('    function paidTypesFor('), html.indexOf('    function missingPaidDrafts('));

async function recalculate(audience, secondAudience = audience) {
  const key = 'shirt|white', seo = {displayName:'Cotton Shirt', title:'White Cotton Shirt', metaTitle:'Cotton Shirt', metaDescription:'White cotton shirt.', imageAlt:'White shirt'};
  const po = {id:'PO-0006', status:'received', line:'casuals', season:'Summer', lines:[
    {key, audience, fit:'Regular Fit', qty:2, weightGrams:120, photoUrl:'/original.png'},
    {key, audience:secondAudience, fit:'Regular Fit', qty:2, weightGrams:120}
  ], seoDraft:[{key, seo, seoApproved:true}]};
  let handler, result;
  const context = {PurchaseProductProfile:require('../public/purchase-product-profile'),router:{post:(route, fn)=>{handler=fn;}}, loadStore:()=>({pos:{'PO-0006':po}}),
    saveStore:()=>{}, isLockedPo:()=>false, expandArticleWeights:(lines, weights)=>weights,
    num:Number, groupKey:line=>line.key, publicPo:p=>p, canManagePurchases:()=>true, stripPreviewForRole:p=>p,
    computePreview:async()=>({newProducts:[{key, productType:'Shirt', colour:'White', photoUrl:'/original.png', variants:[{sizeLabel:'M',qty:2,price:1299},{sizeLabel:'L',qty:2,price:1299}]}]}),
    studio:{images:{}, seo:{[key]:{seo,approved:true}},backRefs:{}}};
  vm.createContext(context);
  vm.runInContext(sourceGroup + '\n' + receiveRoute + '\n' + typesFunction + '\n' + readyFunction, context);
  const res = {json:value=>{result=value;return res;},status:()=>res};
  await handler({params:{id:po.id},body:{}}, res);
  assert.equal(result.success, true);
  const product = result.newProducts[0];
  context.studio.images[key] = Array.from(context.paidTypesFor(product), type=>({type,url:'/'+type+'.png',approved:true,qa:{status:'pass'}}));
  return {product, context};
}

test('received cost and selling-price recalculation preserves approved Women, Men and Unisex posting readiness', async()=>{
  for (const audience of ['Women','Men','Unisex']) {
    const {product, context} = await recalculate(audience);
    assert.equal(product.audience, audience);
    assert.equal(product.fit, 'Regular Fit');
    assert.equal(product.line, 'casuals');
    assert.equal(product.season, 'Summer');
    assert.deepEqual(Array.from(product.variants, v=>v.price), [1299,1299]);
    assert.equal(context.studioReadyFor(product), true, audience);
    context.studio.images[product.key][0].approved = false;
    assert.equal(context.studioReadyFor(product), false, 'missing approval remains blocked');
    context.studio.images[product.key][0].approved = true;
    context.studio.seo[product.key].approved = false;
    assert.equal(context.studioReadyFor(product), false, 'unapproved SEO remains blocked');
  }
});

test('recalculation keeps mixed or unset product audiences blocked', async()=>{
  for (const [audience, second] of [['Women','Men'],['',''],['Women','']]) {
    const {product, context} = await recalculate(audience,second);
    assert.equal(product.audience, '');
    assert.equal(context.studioReadyFor(product), false);
  }
});

'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const studio=require('../modules/catalogue-images');

function product(id,audience,count=3,name='image.png'){
  return {id,title:`Sample ${audience} product`,handle:`sample-${id}`,status:'active',product_type:'Shirt',
    tags:`SANKI Gender: ${audience}, SANKI Collection: Casuals`,options:[{name:'Color',values:['Blue']}],
    variants:[{sku:`SKU${id}`,inventory_quantity:1}],images:Array.from({length:count},(_,index)=>({id:id*100+index,src:`https://cdn.shopify.com/s/files/1/${index?`view-${index}.png`:name}`,position:index+1}))};
}

test('catalogue studio requires explicit audience and chooses a three-view clothing set',()=>{
  const women=studio.itemFromProduct(product(1,'Women'));
  const men=studio.itemFromProduct(product(2,'Men'));
  const unisex=studio.itemFromProduct(product(3,'Unisex'));
  assert.equal(women.audience,'Women');
  assert.deepEqual(women.requiredViews,['front','model-front','model-side']);
  assert.deepEqual(men.requiredViews,['front','model-front','model-side']);
  assert.deepEqual(unisex.requiredViews,['front','female','male']);
});

test('mixed pilot selects four source-ready products for each audience and prioritises incomplete/reference galleries',()=>{
  const products=[];
  for(const [offset,audience] of [[0,'Women'],[20,'Men'],[40,'Unisex']])for(let i=1;i<=6;i++)products.push(product(offset+i,audience,i===1?1:3,i===2?'WhatsApp_Image.jpg':'image.png'));
  const picked=studio.choosePilot(products,4);
  assert.equal(picked.length,12);
  for(const audience of ['Women','Men','Unisex']){
    const group=picked.filter(item=>item.audience===audience);
    assert.equal(group.length,4);
    assert.ok(group.some(item=>item.currentImages.length===1));
    assert.ok(group.some(item=>/WhatsApp/i.test(item.currentImages[0].name)));
  }
});

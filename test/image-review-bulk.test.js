'use strict';
const test=require('node:test'),assert=require('node:assert/strict');
const bulk=require('../public/image-review-bulk');
const item=id=>({id,status:'ready',review:{revision:4,job:{id:'job'+id,status:'ready'},proposals:['front','detail','angle','back','product'].map(view=>({view,jobId:'job'+id}))}});
test('bulk selection excludes incomplete, stale, processing and published sets and deduplicates',()=>{
 const a=item('a'),missing=item('missing'),stale=item('stale'),queued=item('queued'),published=item('published');missing.review.proposals.pop();stale.review.proposals[0].jobId='old';queued.review.job.status='processing';published.status='published';
 assert.deepEqual(bulk.plan([a,a,missing,stale,queued,published]).map(i=>i.id),['a']);
});
test('publishes sequentially, uses reviewed snapshots, reports failures and continues',async()=>{
 const rows=[item('a'),item('b'),item('c')],calls=[];let active=0;
 const results=await bulk.publish(rows,async i=>{assert.equal(active++,0);calls.push([i.id,i.review.revision]);await Promise.resolve();active--;if(i.id==='b')throw Error('Website changed');return {item:{review:{job:{status:'published'}}}};});
 assert.deepEqual(calls,[['a',4],['b',4],['c',4]]);assert.deepEqual(results.map(r=>r.ok),[true,false,true]);assert.equal(results[1].error,'Website changed');
});
test('stop finishes current product and does not send the next one; unconfirmed success fails',async()=>{
 let stop=false;const results=await bulk.publish([item('a'),item('b')],async()=>{stop=true;return {item:{review:{job:{status:'ready'}}}};},()=>stop);
 assert.equal(results.length,1);assert.equal(results[0].ok,false);
});

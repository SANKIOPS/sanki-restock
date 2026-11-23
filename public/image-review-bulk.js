'use strict';
(function(root){
  const views=['front','detail','angle','back','product'];
  function eligible(item){const job=item?.review?.job;return item?.status==='ready'&&job?.status==='ready'&&views.every(view=>(item.review.proposals||[]).some(p=>p.view===view&&p.jobId===job.id));}
  function plan(items){return [...new Map(items.filter(eligible).map(i=>[i.id,i])).values()];}
  async function publish(items,send,stop=()=>false,progress=()=>{}){
    const results=[];
    for(const item of items){
      if(stop())break;
      let result;
      try{const response=await send(item);if(response?.item?.review?.job?.status!=='published')throw Error('Publishing was not confirmed. Review this product before retrying.');result={item,ok:true};}
      catch(error){result={item,ok:false,error:error.message||'Publishing failed'};}
      results.push(result);progress(results.length,items.length,item,result);
    }
    return results;
  }
  const api={eligible,plan,publish};if(typeof module==='object'&&module.exports)module.exports=api;else root.ImageReviewBulk=api;
})(typeof window==='object'?window:this);

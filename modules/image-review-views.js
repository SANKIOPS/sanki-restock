'use strict';
const OUTPUT_VIEWS=['front','detail','angle','back','product'];
const outputViews=job=>job.outputViews||job.references.map(ref=>ref.view);
const generationViews=(job,proposals)=>job.generationViews||outputViews(job).filter(view=>!proposals.some(f=>f.jobId===job.id&&f.view===view));
module.exports={OUTPUT_VIEWS,outputViews,generationViews};

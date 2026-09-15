const crypto = require('crypto');
const VIEWS = ['front', 'model-front', 'model-side', 'model-back', 'detail'];
function fingerprint(group, back) {
  return crypto.createHash('sha256').update(JSON.stringify({group, back:back || ''})).digest('hex');
}
function prepare(po, group) {
  const back = (po.backRefs || {})[group.key] || '';
  return {id:crypto.randomUUID(), version:1, poId:po.id, groupKey:group.key,
    sourceFingerprint:fingerprint(group, back), createdAt:new Date().toISOString(),
    status:'prepared', product:group, backPhotoUrl:back,
    instructions:'Use built-in Codex image generation, not a paid API. Keep the exact garment colour, print placement, collar, buttons, pockets, sleeve and hem proportions. Use one consistent adult model, neutral studio background and soft lighting. No collage, watermark or added branding. Check each result against original photos. Never invent unseen garment details. Return individual files, not a composite.',
    views:VIEWS.map(type=>({type, status: type==='model-side' || (type==='model-back' && !back)?'needs-reference':'pending',
      prompt:({front:'Product-only front packshot, pure white background.', 'model-front':'Full-body model facing camera wearing the exact garment.', 'model-side':'Same model in side profile; requires a reliable side reference.', 'model-back':'Same model from rear; requires real rear garment reference.',detail:'Close-up of visible garment print and construction; do not invent texture.'})[type]}))};
}
function accept(batch, group, back, images, readPhoto) {
  if(batch.sourceFingerprint !== fingerprint(group, back)) throw new Error('Source product changed. Prepare a fresh batch before importing.');
  if(!Array.isArray(images) || !images.length) throw new Error('Images required.');
  const seen = new Set();
  return images.map(i=>{
    if(!VIEWS.includes(i.type) || seen.has(i.type)) throw new Error('Invalid or duplicate view.');
    seen.add(i.type);
    if(!readPhoto(i.url)) throw new Error('Upload the result to Purchases first; remote image URLs are not accepted.');
    return {type:i.type,label:i.type,url:i.url,approved:false,codexBatchId:batch.id};
  });
}
module.exports={VIEWS,prepare,accept,fingerprint};

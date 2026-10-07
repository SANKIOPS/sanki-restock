const crypto = require('crypto');
const VIEWS = ['front', 'model-front', 'model-side', 'model-back', 'detail'];
function fingerprint(group, back) {
  return crypto.createHash('sha256').update(JSON.stringify({group, back:back || ''})).digest('hex');
}
function prepare(po, group) {
  const back = (po.backRefs || {})[group.key] || '';
  const draft = (po.seoDraft || []).find(x => x.key === group.key);
  const pilot=require('./procurement-openai-pilot');
  const styling=pilot.normalizeStyling((po.imageStyling||{})[group.key],group);
  const types=pilot.pilotTypes(group,!!back);
  return {id:crypto.randomUUID(), version:2, poId:po.id, groupKey:group.key,
    sourceFingerprint:fingerprint(group, back), createdAt:new Date().toISOString(),
    status:'prepared', product:group, backPhotoUrl:back, styling,
    instructions:'Use built-in Codex image generation, not a paid API. Keep the exact garment colour, print placement, collar, buttons, pockets, sleeve and hem proportions. Use one consistent adult model, neutral studio background and soft lighting. No collage, watermark or added branding. Check each result against original photos. Never invent unseen garment details. Return individual files, not a composite.',
    listingCopy:{currentDraft:draft && draft.seo || null,
      instructions:'Inspect the actual source garment photo and verified product facts. Write accurate, natural customer-facing copy for SEO, GEO and answer engines. Do not infer unseen fabric, construction, gender, origin, or availability. Do not repeat the product type, include internal SKU/vendor codes, or make unsupported claims. Return one JSON object with batchId, groupKey, and seo. Keep the existing handle if suitable.',
      requiredFields:['displayName','title','handle','metaTitle','metaDescription','imageAlt','tags','bodyHtml'],
      resultExample:{batchId:'<this batch id>',groupKey:group.key,seo:{displayName:'',title:'',handle:'',metaTitle:'',metaDescription:'',imageAlt:'',tags:[],bodyHtml:''}}},
    views:types.map(type=>({type,status:type.startsWith('model-side')?'needs-reference':'pending',
      prompt:pilot.imagePrompt(group,type,styling,type.startsWith('model-side'))}))};
}
function acceptSeo(batch, group, back, seo) {
  if(batch.sourceFingerprint !== fingerprint(group, back)) throw new Error('Source product changed. Prepare a fresh batch before importing.');
  if(!seo || typeof seo !== 'object' || Array.isArray(seo)) throw new Error('Listing copy object required.');
  const fields=['displayName','title','handle','metaTitle','metaDescription','imageAlt','bodyHtml'];
  if(fields.some(k=>typeof seo[k]!=='string' || !seo[k].trim()) || !Array.isArray(seo.tags) || !seo.tags.length || seo.tags.some(t=>typeof t!=='string' || !t.trim())) throw new Error('Complete listing copy is required.');
  const out=Object.fromEntries(fields.map(k=>[k,seo[k].trim()]));
  out.tags=seo.tags.map(t=>t.trim());
  if(/<script|on\w+\s*=/i.test(out.bodyHtml)) throw new Error('Unsafe listing HTML.');
  return out;
}
function accept(batch, group, back, images, readPhoto) {
  if(batch.sourceFingerprint !== fingerprint(group, back)) throw new Error('Source product changed. Prepare a fresh batch before importing.');
  if(!Array.isArray(images) || !images.length) throw new Error('Images required.');
  const pilot=require('./procurement-openai-pilot');
  const types=pilot.pilotTypes(group,!!back);
  const seen = new Set();
  return images.map(i=>{
    if(!types.includes(i.type) || seen.has(i.type)) throw new Error('Invalid or duplicate view.');
    seen.add(i.type);
    if(!readPhoto(i.url)) throw new Error('Upload the result to Purchases first; remote image URLs are not accepted.');
    return {type:i.type,label:i.type,url:i.url,approved:false,codexBatchId:batch.id,
      sourceFingerprint:batch.sourceFingerprint,styling:pilot.MODEL_VIEWS.includes(i.type)?batch.styling:null,
      requestedStyling:pilot.MODEL_VIEWS.includes(i.type)?batch.styling:null};
  });
}
module.exports={VIEWS,prepare,accept,acceptSeo,fingerprint};

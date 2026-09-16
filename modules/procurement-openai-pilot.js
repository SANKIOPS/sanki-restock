// Metered, explicitly started PO pilot. No retries: a retry can be another billed call.
const IMAGE_TYPES = ['front', 'back', 'female', 'male', 'model-front', 'model-side', 'detail'];
const SEO_FIELDS = ['displayName', 'title', 'metaTitle', 'metaDescription', 'imageAlt', 'bodyHtml'];
const UPPER_TYPES = new Set(['shirt','t-shirt','sando','hoodie','jacket','top','sweatshirt','sweater']);
const LOWER_TYPES = new Set(['jeans','trouser','trousers','lower','lowers','shorts','jogger','jorts','denim joggers','cargo','skirt']);
const UPPER_PAIRS = ['Auto','Baggy trousers','Straight trousers','Jeans','Shorts'];
const LOWER_PAIRS = ['Auto','Plain white tee','Plain black tee','Oversized tee','Button-up shirt','Hoodie'];
const AESTHETICS = ['Auto','Minimal','Streetwear','Editorial'];

function garmentCategory(group) {
  const type=String(group.productType||'').toLowerCase();
  return UPPER_TYPES.has(type)?'upper':LOWER_TYPES.has(type)?'lower':'other';
}

function normalizeStyling(input,group) {
  const value=input && typeof input==='object' && !Array.isArray(input)?input:{};
  const category=garmentCategory(group);
  const pairs=category==='upper'?UPPER_PAIRS:category==='lower'?LOWER_PAIRS:['Auto'];
  const choice=(raw,allowed,fallback)=>allowed.includes(raw)?raw:fallback;
  return {
    pair:choice(value.pair,pairs,'Auto'),
    aesthetic:choice(value.aesthetic,AESTHETICS,'Auto'),
    tuck:choice(value.tuck,['Auto','Tucked in','Untucked'],'Auto'),
    chain:choice(value.chain,['None','Silver chain','Gold chain'],'None'),
    cap:value.cap===true, sunglasses:value.sunglasses===true,
    watch:value.watch===true, bag:value.bag===true
  };
}

function stylingPrompt(group,styling) {
  const style=normalizeStyling(styling,group),category=garmentCategory(group);
  const pair=style.pair==='Auto'
    ? category==='upper'?'simple neutral trousers that do not compete with the featured upper garment'
      :category==='lower'?'a simple neutral shirt or T-shirt that keeps the featured bottom garment visible'
      :'simple neutral supporting clothing'
    :style.pair.toLowerCase();
  const extras=[];
  if(style.chain!=='None')extras.push(style.chain.toLowerCase());
  if(style.cap)extras.push('a cap');
  if(style.sunglasses)extras.push('sunglasses');
  if(style.watch)extras.push('a watch');
  if(style.bag)extras.push('a simple bag');
  const aesthetic=style.aesthetic==='Auto'?'clean, restrained catalogue styling':style.aesthetic.toLowerCase()+' styling';
  return `Style the model with ${pair}, clean footwear and ${aesthetic}. ${style.tuck==='Auto'?'Keep the featured garment unobstructed.':`Wear the top ${style.tuck.toLowerCase()}.`} ${extras.length?`Add ${extras.join(', ')}. `:'Do not add jewellery, hats, sunglasses, watches or bags. '}Supporting garments and accessories must never obscure or change the featured item.`;
}

function pilotTypes(group,hasBackReference=false) {
  const audience = String(group.audience || '').toLowerCase();
  const finish=hasBackReference?'back':'detail';
  if (audience === 'women') return ['front', 'female', 'model-front', 'model-side',finish];
  if (audience === 'men') return ['front', 'male', 'model-front', 'model-side',finish];
  return ['front', 'female', 'male', 'model-front',finish];
}

function imagePrompt(group, type, styling) {
  const facts = `${group.colour} ${group.productType}${group.fit ? `, ${group.fit} fit` : ''}`;
  const common = `The reference shows the actual ${facts}. Preserve its exact colour, visible print, seams, neckline, sleeves, cut and length. Do not invent a logo, fabric composition, unseen back, pockets or details. One garment, no collage, text or watermark.`;
  if (type === 'front') return `Create a clean, photorealistic product-only front catalogue photo on a white studio background. ${common}`;
  if (type === 'back') return `Create a clean, photorealistic product-only BACK catalogue photo on a white studio background. The reference is a real photo of the back of this garment. Preserve only details actually visible in that back reference; do not copy front artwork onto the back or invent unseen details. ${common}`;
  if (type === 'detail') return `Create a photorealistic close-up detail photo of the garment's FRONT, showing only details clearly visible in the reference. No model or invented stitching, labels or fabric composition. ${common}`;
  if (type === 'model-front') return `Create a photorealistic full-body front-facing adult ${String(group.audience).toLowerCase()==='women'?'female':'male'} model wearing this garment. ${stylingPrompt(group,styling)} Keep the garment fully visible, face visible, neutral studio background. ${common}`;
  if (type === 'model-side') return `Create a photorealistic front-biased three-quarter model view showing the garment's fit and silhouette. Keep the garment's unseen back out of view. ${stylingPrompt(group,styling)} Do not invent details on unseen parts of the garment. Neutral studio background. ${common}`;
  return `Create a photorealistic full-body front-facing ${type === 'female' ? 'female' : 'male'} adult model shot on a neutral studio background, wearing this exact garment. ${stylingPrompt(group,styling)} Keep the face and whole garment visible. ${common}`;
}

function seoSchema() {
  return {type:'object',additionalProperties:false,required:[...SEO_FIELDS,'tags'],properties:{
    ...Object.fromEntries(SEO_FIELDS.map(k=>[k,{type:'string'}])),
    tags:{type:'array',items:{type:'string'}}
  }};
}

function responseText(body) {
  if (body.output_text) return body.output_text;
  return (body.output || []).flatMap(item=>item.content || []).filter(c=>c.type==='output_text').map(c=>c.text || '').join('');
}

async function readApiResponse(response) {
  const body = await response.json().catch(()=>({}));
  if (!response.ok) {
    const detail = String(body.error && body.error.message || `HTTP ${response.status}`);
    throw new Error(`OpenAI API: ${detail.slice(0,180)}`);
  }
  return body;
}

async function generateImage({key, group, source, type, styling, model='gpt-image-1.5', fetchImpl=global.fetch}) {
  if (!IMAGE_TYPES.includes(type)) throw new Error('Unsupported pilot image view.');
  const form = new FormData();
  form.append('model', model);
  const sourceExt = source.mime==='image/png'?'.png':source.mime==='image/webp'?'.webp':'.jpg';
  form.append('image', new Blob([source.buf],{type:source.mime}), 'source'+sourceExt);
  form.append('prompt', imagePrompt(group,type,styling));
  form.append('quality','medium');
  form.append('size','1024x1536');
  const response = await fetchImpl('https://api.openai.com/v1/images/edits', {
    method:'POST', headers:{Authorization:`Bearer ${key}`}, body:form, signal:AbortSignal.timeout(180000)
  });
  const body = await readApiResponse(response);
  const encoded = body.data && body.data[0] && body.data[0].b64_json;
  if (!encoded) throw new Error('OpenAI returned no image.');
  return {buffer:Buffer.from(encoded,'base64'),usage:body.usage || null,model};
}

async function generateSeo({key, group, source, model='gpt-4.1-mini', fetchImpl=global.fetch}) {
  const facts = {brand:'SANKI',productType:group.productType,colour:group.colour,
    audience:group.audience,fit:group.fit,sizes:group.sizeLabels};
  const prompt = `Inspect the source garment photo and these confirmed product facts: ${JSON.stringify(facts)}. Write accurate, natural storefront and SEO/AEO/GEO listing copy. Do not infer fabric, origin, availability, COD, unseen details or unverified gender/fit from the photo. Never use vendor codes or SKU in customer copy. Do not repeat the product type. Display name should be short; meta title <= 60 characters and meta description <= 155 characters. Tags should be 5-8 factual terms. bodyHtml may use only simple <p> tags.`;
  const response = await fetchImpl('https://api.openai.com/v1/responses',{
    method:'POST',headers:{Authorization:`Bearer ${key}`,'Content-Type':'application/json'},
    body:JSON.stringify({model,store:false,max_output_tokens:900,input:[{role:'user',content:[
      {type:'input_text',text:prompt},
      {type:'input_image',image_url:`data:${source.mime};base64,${source.buf.toString('base64')}`}
    ]}],text:{format:{type:'json_schema',name:'sanki_listing_copy',strict:true,schema:seoSchema()}}}),
    signal:AbortSignal.timeout(90000)
  });
  const body = await readApiResponse(response);
  const seo = JSON.parse(responseText(body));
  if (SEO_FIELDS.some(k=>typeof seo[k]!=='string' || !seo[k].trim()) || !Array.isArray(seo.tags) || !seo.tags.length) throw new Error('OpenAI returned incomplete SEO copy.');
  return {seo,usage:body.usage || null,model};
}

module.exports={IMAGE_TYPES,pilotTypes,garmentCategory,normalizeStyling,stylingPrompt,imagePrompt,generateImage,generateSeo,responseText};

// Metered, explicitly started PO generation. Every retry is separately confirmed.
const IMAGE_TYPES = ['front', 'back', 'female', 'male', 'model-front', 'model-side', 'model-side-female', 'model-side-male', 'detail'];
const SEO_FIELDS = ['displayName', 'title', 'metaTitle', 'metaDescription', 'imageAlt', 'bodyHtml'];
const UPPER_TYPES = new Set(['shirt','t-shirt','sando','hoodie','jacket','top','sweatshirt','sweater']);
const LOWER_TYPES = new Set(['jeans','trouser','trousers','lower','lowers','shorts','jogger','jorts','denim joggers','cargo','skirt']);
const UPPER_PAIRS = ['Auto','Baggy trousers','Straight trousers','Tailored trousers'];
const FUNKY_UPPER_PAIRS = [...UPPER_PAIRS,'Jeans','Shorts'];
const LOWER_PAIRS = ['Auto','Plain white tee','Plain black tee','Button-up shirt'];
const AESTHETICS = ['Auto','Limestone estate','Minimal','Streetwear','Editorial'];
const SHOES = ['Auto','Leather loafers','Minimal white sneakers','Ballet flats','Classic heels'];
const CAPS = ['None','Classic linen cap','Refined baker-boy cap'];
const BAGS = ['None','Gender-matched bag','Structured handbag','Minimal sling bag'];

function garmentCategory(group) {
  const type=String(group.productType||'').toLowerCase();
  return UPPER_TYPES.has(type)?'upper':LOWER_TYPES.has(type)?'lower':'other';
}

function normalizeStyling(input,group) {
  const value=input && typeof input==='object' && !Array.isArray(input)?input:{};
  const category=garmentCategory(group);
  const casuals=String(group.line||group.collection||'').toLowerCase().includes('casual');
  const pairs=category==='upper'?(casuals?UPPER_PAIRS:FUNKY_UPPER_PAIRS):category==='lower'?LOWER_PAIRS:['Auto'];
  const choice=(raw,allowed,fallback)=>allowed.includes(raw)?raw:fallback;
  return {
    pair:choice(value.pair,pairs,'Auto'),
    aesthetic:choice(value.aesthetic,AESTHETICS,'Auto'),
    tuck:choice(value.tuck,['Auto','Tucked in','Untucked'],'Auto'),
    chain:choice(value.chain,['None','Silver chain','Gold chain'],'None'),
    shoes:choice(value.shoes,SHOES,'Auto'),
    capStyle:choice(value.capStyle,CAPS,value.cap===true?'Classic linen cap':'None'),
    bagStyle:choice(value.bagStyle,BAGS,value.bag===true?(String(group.audience).toLowerCase()==='men'?'Minimal sling bag':'Structured handbag'):'None'),
    sunglasses:value.sunglasses===true,watch:value.watch===true
  };
}

function stylingPrompt(group,styling) {
  const style=normalizeStyling(styling,group),category=garmentCategory(group);
  const casuals=String(group.line||group.collection||'').toLowerCase().includes('casual');
  const pair=style.pair==='Auto'
    ? category==='upper'?(casuals?'tailored neutral trousers that do not compete with the featured upper garment':'complementary neutral bottoms that do not compete with the featured upper garment')
      :category==='lower'?'a simple neutral shirt or T-shirt that keeps the featured bottom garment visible'
      :'simple neutral supporting clothing'
    :style.pair.toLowerCase();
  const extras=[];
  if(style.chain!=='None')extras.push(style.chain.toLowerCase());
  if(style.capStyle!=='None')extras.push(style.capStyle.toLowerCase());
  if(style.sunglasses)extras.push('elegant understated sunglasses');
  if(style.watch)extras.push('a watch');
  if(style.bagStyle!=='None')extras.push(style.bagStyle.toLowerCase());
  const aesthetic=style.aesthetic==='Auto'?(casuals?'global old-money, quiet luxury':'clean, restrained catalogue'):style.aesthetic.toLowerCase();
  const shoes=style.shoes==='Auto'?'subtle classic shoes appropriate to the outfit':style.shoes.toLowerCase();
  return `Style the model with ${pair}, ${shoes} and ${aesthetic} styling. ${style.tuck==='Auto'?'Keep the featured garment unobstructed.':`Wear the top ${style.tuck.toLowerCase()}.`} ${extras.length?`Add only ${extras.join(', ')} when they do not hide the product. `:'No visible jewellery, hats, sunglasses, watches or bags. '}Supporting garments and accessories must never obscure or change the featured item.`;
}

function pilotTypes(group,hasBackReference=false) {
  const audience = String(group.audience || '').toLowerCase();
  const finish=hasBackReference?['back']:[];
  if (audience === 'women') return ['front',...finish,'model-front','model-side'];
  if (audience === 'men') return ['front',...finish,'model-front','model-side'];
  if (audience === 'unisex') return ['front',...finish,'female','model-side-female','male','model-side-male'];
  return ['front',...finish]; // Unspecified audience must be corrected, not guessed.
}

function castDescription(group,gender) {
  const key=String(group.designCode||group.key||group.designName||'SANKI');
  let hash=0;for(const char of key)hash=(hash*31+char.charCodeAt(0))>>>0;
  const women=['adult Indian woman with warm brown skin and dark hair in a neat low bun','adult Indian woman with deep brown skin and dark shoulder-length wavy hair'];
  const men=['adult Indian man with warm brown skin and neatly styled short dark hair','adult Indian man with deep brown skin and short textured dark hair'];
  return (gender==='female'?women:men)[hash%2];
}

function imagePrompt(group, type, styling) {
  const facts = `${group.colour} ${group.productType}${group.fit ? `, ${group.fit} fit` : ''}`;
  const common = `The reference shows the actual ${facts}. Preserve its exact colour, visible print, seams, neckline, sleeves, cut and length. Do not invent a logo, fabric composition, unseen back, pockets or details. One garment, no collage, text or watermark.`;
  if (type === 'front') return `Create a clean, photorealistic product-only front catalogue photo on a warm ivory studio background. ${String(group.audience).toLowerCase()==='women'&&garmentCategory(group)==='upper'?'Show the true fitted silhouette and bust shaping of the fully opaque garment on an invisible female-form mannequin, with no visible skin or mannequin parts. ':''}${common}`;
  if (type === 'back') return `Create a clean, photorealistic product-only BACK catalogue photo on a white studio background. The reference is a real photo of the back of this garment. Preserve only details actually visible in that back reference; do not copy front artwork onto the back or invent unseen details. ${common}`;
  if (type === 'detail') return `Create a photorealistic close-up detail photo of the garment's FRONT, showing only details clearly visible in the reference. No model or invented stitching, labels or fabric composition. ${common}`;
  const gender=type==='female'||type==='model-side-female'?'female':type==='male'||type==='model-side-male'?'male':String(group.audience).toLowerCase()==='women'?'female':'male';
  const cast=castDescription(group,gender);
  const setting=String(group.line||group.collection||'').toLowerCase().includes('casual')?'pale limestone colonnade of a refined heritage estate, natural daylight, understated global old-money mood':'restrained neutral editorial setting';
  const angle=type==='model-side'||type.startsWith('model-side-')?'front-biased three-quarter view, keeping the unseen back out of view':'front-facing full-body view';
  const resolvedStyle=normalizeStyling(styling,group);
  if(resolvedStyle.bagStyle==='Gender-matched bag')resolvedStyle.bagStyle=gender==='female'?'Structured handbag':'Minimal sling bag';
  return `Create a photorealistic ${angle} of the same ${cast} wearing this exact garment. ${stylingPrompt(group,resolvedStyle)} Keep the garment fully visible and face unobstructed, in the ${setting}. For the three-quarter shot, maintain the same model identity, outfit and location as the matching front shot. Do not invent unseen garment details. ${common}`;
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

module.exports={IMAGE_TYPES,pilotTypes,garmentCategory,normalizeStyling,stylingPrompt,imagePrompt,generateImage,generateSeo,responseText,castDescription};

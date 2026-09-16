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
const BAG_COLOURS = ['Auto','Beige','Tan','Ivory','Brown','Black'];
const COMPLEXIONS = ['Fair','Medium','Deep'];
const MODEL_ORIGINS = ['Indian','International'];
const UPPER_FITS = ['Auto','Fitted','Normal fit','Boxy / relaxed','Oversized','Slim fit'];
const LOWER_FITS = ['Auto','Shorts / half','Three-quarter (3/4)','Ankle length','Full length'];

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
    fit:choice(value.fit,category==='upper'?UPPER_FITS:category==='lower'?LOWER_FITS:['Auto'],'Auto'),
    pair:choice(value.pair,pairs,'Auto'),
    aesthetic:choice(value.aesthetic,AESTHETICS,'Auto'),
    tuck:choice(value.tuck,['Auto','Tucked in','Untucked'],'Auto'),
    chain:choice(value.chain,['None','Silver chain','Gold chain'],'None'),
    shoes:choice(value.shoes,SHOES,'Auto'),
    femaleComplexion:choice(value.femaleComplexion,COMPLEXIONS,'Medium'),
    maleComplexion:choice(value.maleComplexion,COMPLEXIONS,'Medium'),
    modelOrigin:choice(value.modelOrigin,MODEL_ORIGINS,'Indian'),
    capStyle:choice(value.capStyle,CAPS,value.cap===true?'Classic linen cap':'None'),
    bagStyle:choice(value.bagStyle,BAGS,value.bag===true?(String(group.audience).toLowerCase()==='men'?'Minimal sling bag':'Structured handbag'):'None'),
    bagColour:choice(value.bagColour,BAG_COLOURS,'Auto'),
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
  if(style.bagStyle!=='None'){
    const colour=style.bagColour==='Auto'
      ? (casuals?'beige or tan':'outfit-matching muted')
      :style.bagColour.toLowerCase();
    extras.push(`${colour} ${style.bagStyle.toLowerCase()}`);
  }
  const bagRule=style.bagStyle!=='None'&&style.bagColour==='Auto'&&casuals?'For a light ivory-and-beige outfit, the bag must be beige or tan, never black.':'';
  const aesthetic=style.aesthetic==='Auto'?(casuals?'global old-money, quiet luxury':'clean, restrained catalogue'):style.aesthetic.toLowerCase();
  const shoes=style.shoes==='Auto'?'subtle classic shoes appropriate to the outfit':style.shoes.toLowerCase();
  const selectedFit=style.fit==='Auto' && /^muscle\s*fit$/i.test(String(group.fit||'')) && String(group.audience||'').toLowerCase()==='women'?'Fitted':style.fit;
  const fitRule=category!=='upper'
    ? 'Keep the featured garment cut and length faithful to the original reference.'
    :selectedFit==='Fitted'||selectedFit==='Slim fit'
      ? 'The featured upper garment must be fitted at the natural shoulder with set-in sleeves and a close, tidy torso silhouette; no dropped shoulder seam, boxy cut or oversized drape.'
      :selectedFit==='Oversized'||selectedFit==='Boxy / relaxed'
        ? `Use the selected ${selectedFit.toLowerCase()} cut, while keeping its visible seams and proportions faithful to the real reference.`
        :selectedFit==='Normal fit'?'Use a regular, clean fit with shoulder seams at the natural shoulder; do not turn it into an oversized or drop-shoulder garment.':'Match the garment silhouette and shoulder seams visible in the reference; do not assume an oversized or drop-shoulder cut.';
  return `Style the model with ${pair}, ${shoes} and ${aesthetic} styling. ${fitRule} ${style.tuck==='Auto'?'Keep the featured garment unobstructed.':`Wear the top ${style.tuck.toLowerCase()}.`} ${extras.length?`Add only ${extras.join(', ')} when they do not hide the product. `:'No visible jewellery, hats, sunglasses, watches or bags. '}${bagRule} Supporting garments and accessories must never obscure or change the featured item.`;
}

function pilotTypes(group,hasBackReference=false) {
  const audience = String(group.audience || '').toLowerCase();
  const finish=hasBackReference?['back']:[];
  if (audience === 'women') return ['front',...finish,'model-front','model-side'];
  if (audience === 'men') return ['front',...finish,'model-front','model-side'];
  if (audience === 'unisex') return ['front',...finish,'female','model-side-female','male','model-side-male'];
  return ['front',...finish]; // Unspecified audience must be corrected, not guessed.
}

function castDescription(group,gender,styling) {
  const key=String(group.designCode||group.key||group.designName||'SANKI');
  let hash=0;for(const char of key)hash=(hash*31+char.charCodeAt(0))>>>0;
  const style=normalizeStyling(styling,group);
  const tone=gender==='female'?style.femaleComplexion:style.maleComplexion;
  const skin={Fair:'fair, light complexion (not medium or deep)',Medium:'medium brown complexion',Deep:'deep brown complexion'}[tone];
  const women=['dark hair in a neat low bun','dark shoulder-length wavy hair'];
  const men=['neatly styled short dark hair','short textured dark hair'];
  const origin=style.modelOrigin==='Indian'?'Indian':'international non-Indian';
  return `adult ${origin} ${gender==='female'?'woman':'man'} with ${skin} and ${(gender==='female'?women:men)[hash%2]}`;
}

function isWinter(group) {
  return /^winter$/i.test(String(group.season||'')) || /^(?:hoodie|sweatshirt|sweater|cardigan|pullover|jacket|coat)$/i.test(String(group.productType||''));
}

function retailFacts(group) {
  const women=String(group.audience||'').toLowerCase()==='women';
  const womenTop=women&&!isWinter(group);
  const rawType=String(group.productType||'').trim();
  const rawFit=String(group.fit||'').trim();
  return {
    productType:womenTop && /^t[ -]?shirt$/i.test(rawType)?'Top':rawType,
    fit:womenTop && /^muscle\s*fit$/i.test(rawFit)?'':rawFit
  };
}

function seoCopyNeedsReview(seo,group) {
  const copy=[seo.displayName,seo.title,seo.metaTitle,seo.metaDescription,seo.imageAlt,seo.bodyHtml,...(seo.tags||[])].join(' ');
  if(String(group.audience||'').toLowerCase()==='women'&&!isWinter(group) && (/\bmuscle\s*fit\b/i.test(copy)||/\b(?<!polo\s)t[ -]?shirts?\b/i.test(copy)))return true;
  if(/^(?:sanki\s+)?casuals?$/i.test(String(seo.displayName||'').trim()))return true;
  return false;
}

function imagePrompt(group, type, styling, hasContinuityReference=false) {
  const facts = `${group.colour} ${group.productType}${retailFacts(group).fit ? `, ${retailFacts(group).fit}` : ''}`;
  const common = `The reference shows the actual ${facts}. Preserve its exact colour, visible print, seams, neckline, sleeves, cut and length. Do not invent a logo, fabric composition, unseen back, pockets or details. One garment, no collage, text or watermark.`;
  if (type === 'front') return `Create a clean, photorealistic product-only front catalogue photo on a warm ivory studio background. ${String(group.audience).toLowerCase()==='women'&&garmentCategory(group)==='upper'?'Show the true fitted silhouette and bust shaping of the fully opaque garment on an invisible female-form mannequin, with no visible skin or mannequin parts. ':''}${common}`;
  if (type === 'back') return `Create a clean, photorealistic product-only BACK catalogue photo on a white studio background. The reference is a real photo of the back of this garment. Preserve only details actually visible in that back reference; do not copy front artwork onto the back or invent unseen details. ${common}`;
  if (type === 'detail') return `Create a photorealistic close-up detail photo of the garment's FRONT, showing only details clearly visible in the reference. No model or invented stitching, labels or fabric composition. ${common}`;
  const gender=type==='female'||type==='model-side-female'?'female':type==='male'||type==='model-side-male'?'male':String(group.audience).toLowerCase()==='women'?'female':'male';
  const cast=castDescription(group,gender,styling);
  const setting=String(group.line||group.collection||'').toLowerCase().includes('casual')?'pale limestone colonnade of a refined heritage estate, natural daylight, understated global old-money mood':'restrained neutral editorial setting';
  const isThreeQuarter=type==='model-side'||type.startsWith('model-side-');
  const angle=isThreeQuarter?'full-body three-quarter-angle':'front-facing full-body';
  const poseInstruction=isThreeQuarter?'Turn the model approximately 45 degrees toward the camera. Keep the head and shoes visible; this is a three-quarter ANGLE, not a three-quarter-length crop. Keep the unseen back out of view.':'Face the camera and show the full outfit from head to shoes.';
  const resolvedStyle=normalizeStyling(styling,group);
  if(resolvedStyle.bagStyle==='Gender-matched bag')resolvedStyle.bagStyle=gender==='female'?'Structured handbag':'Minimal sling bag';
  const continuity=isThreeQuarter?(hasContinuityReference?'The FIRST reference image shows the matching front model photograph: use that exact person, outfit, trouser colour, trouser cut, shoes, accessories and location as a visual continuity anchor. The SECOND reference is the original garment photo: preserve the featured garment exactly. Rotate the same model to a 45-degree pose; do not change the trousers or add another outfit. Neither reference image should appear as a separate panel in the output.':'Maintain the same model identity, trouser colour, outfit and location as the separate matching front photo. Do not include that front photo in this output.'):'One model only, in one pose; do not create a before-and-after layout.';
  return `Create exactly ONE photorealistic ${angle} photograph of ONE ${cast} wearing this exact garment. ${poseInstruction} The output is a single continuous full-frame scene with one camera view and one pose, not two photos. Never make a split image, side-by-side comparison, diptych, triptych, collage, contact sheet, inset, second panel, mirrored figure or duplicated person. ${stylingPrompt(group,resolvedStyle)} Keep the garment fully visible and face unobstructed, in the ${setting}. ${continuity} Do not invent unseen garment details. ${common}`;
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

async function generateImage({key, group, source, continuitySource=null, type, styling, model='gpt-image-1.5', fetchImpl=global.fetch}) {
  if (!IMAGE_TYPES.includes(type)) throw new Error('Unsupported pilot image view.');
  const form = new FormData();
  form.append('model', model);
  const sourceExt = source.mime==='image/png'?'.png':source.mime==='image/webp'?'.webp':'.jpg';
  if(continuitySource){
    const continuityExt=continuitySource.mime==='image/png'?'.png':continuitySource.mime==='image/webp'?'.webp':'.jpg';
    form.append('image[]',new Blob([continuitySource.buf],{type:continuitySource.mime}),'matching-front'+continuityExt);
    form.append('image[]',new Blob([source.buf],{type:source.mime}),'original-garment'+sourceExt);
  } else form.append('image', new Blob([source.buf],{type:source.mime}), 'source'+sourceExt);
  form.append('prompt', imagePrompt(group,type,styling,!!continuitySource));
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
  const retail=retailFacts(group);
  const facts = {brand:'SANKI',productType:retail.productType,colour:group.colour,
    audience:group.audience,fit:retail.fit,sizes:group.sizeLabels};
  const namingRule=String(group.audience||'').toLowerCase()==='women'&&!isWinter(group)
    ?'For women’s non-winter uppers, write "top", "knit top", "polo top", "crew-neck top" or another PHOTO-SUPPORTED style. Never say "muscle fit"; do not call an ordinary women’s top a generic T-shirt. "Polo T-shirt" is acceptable only when a polo collar is unmistakably visible.'
    :'For winter garments, preserve the bill’s confirmed product type (such as sweater, hoodie or jacket); do not relabel it as a top.';
  const prompt = `Inspect the actual garment photo FIRST and use these confirmed facts: ${JSON.stringify(facts)}. Write distinctive, accurate storefront and SEO/AEO/GEO listing copy. The internal vendor design name and category are not customer-facing descriptions. Describe only visible neckline, collar, trim, pattern and silhouette; distinguish each colourway. ${namingRule} If fit is omitted, do not invent one. Display name must describe a visible detail or style, never just "Casuals" or "SANKI". Alt text must literally describe the photographed garment, not make a generic streetwear claim. Do not infer fabric composition, origin, availability, COD or unseen details. Never use vendor codes or SKU in customer copy. Do not repeat the product type. Meta title <= 60 characters and meta description <= 155 characters. Tags should be 5-8 factual terms. bodyHtml may use only simple <p> tags.`;
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
  if(seoCopyNeedsReview(seo,group))throw new Error('AI copy used an unsuitable women’s category/fit or a generic display name; no SEO draft was saved. Retry after reviewing product facts.');
  return {seo,usage:body.usage || null,model};
}

module.exports={IMAGE_TYPES,pilotTypes,garmentCategory,normalizeStyling,stylingPrompt,imagePrompt,generateImage,generateSeo,responseText,castDescription,retailFacts,seoCopyNeedsReview,isWinter};

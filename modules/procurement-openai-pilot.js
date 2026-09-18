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
  const pairingRule=style.pair==='Straight trousers'?'The supporting trousers have a visibly straight leg from knee to hem; their colour may be ivory, cream, beige, tan or another quiet neutral. ':style.pair==='Baggy trousers'?'The supporting trousers have a visibly generous, baggy leg; do not turn them into straight or narrow trousers. ':'';
  const shoeRule=style.shoes==='Leather loafers'?'Show classic loafer-shaped shoes, preferably dark brown or black; shoe colour is not the shoe type. ':'';
  return `Style the model with ${pair}, ${shoes} and ${aesthetic} styling. ${pairingRule}${shoeRule}${fitRule} ${style.tuck==='Auto'?'Keep the featured garment unobstructed.':`Wear the top ${style.tuck.toLowerCase()}; make the waist/hem visible enough to verify that choice.`} ${extras.length?`Add only ${extras.join(', ')} when they do not hide the product. `:'No visible jewellery, hats, sunglasses, watches or bags. '}${bagRule} Supporting garments and accessories must never obscure or change the featured item.`;
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
  const common = `The reference shows the actual ${facts}. Treat the photograph as the garment authority if the purchase label conflicts with what is visibly shown. Preserve its exact colour, visible print, seams, neckline, sleeves, cut and length. The saved fit/style controls the model pose and garment silhouette only where the reference supports it; never invent a different shoulder construction. Do not invent a logo, fabric composition, unseen back, pockets or details. One garment, no collage, text or watermark.`;
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
  const artDirection=String(group.line||group.collection||'').toLowerCase().includes('casual')?'understated international old-money fashion editorial, natural daylight, refined stone architecture, quiet ivory and beige supporting palette, no loud props':'restrained editorial fashion photography that keeps the real product as the hero';
  return `Create exactly ONE photorealistic ${angle} photograph of ONE ${cast} wearing this exact garment. Art direction: ${artDirection}. ${poseInstruction} The output is a single continuous full-frame scene with one camera view and one pose, not two photos. Never make a split image, side-by-side comparison, diptych, triptych, collage, contact sheet, inset, second panel, mirrored figure or duplicated person. Follow every saved styling choice exactly: ${stylingPrompt(group,resolvedStyle)} Keep the garment fully visible and face unobstructed, in the ${setting}. ${continuity} Do not invent unseen garment details. ${common}`;
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

function repairGuidance(fields,group,styling,type) {
  const style=normalizeStyling(styling,group);
  const guidance={
    garmentMatch:'Keep the featured garment identical to the original photo in colour, neckline, seams, sleeves and silhouette.',
    singleFrame:'Output one model in one continuous photograph; no panels or split composition.',
    angleMatch:type.startsWith('model-side')?'Rotate the same model about 45 degrees and show head to shoes.':'Face the camera directly and show head to shoes.',
    fitMatch:`Match the real garment shoulder and cut shown in the source, respecting the selected ${style.fit} fit only where the source supports it.`,
    pairMatch:`Make the supporting bottom or top visibly ${style.pair.toLowerCase()}; distinguish baggy from straight by leg silhouette, not colour.`,
    shoeMatch:`Use the selected ${style.shoes.toLowerCase()} shape.`,
    tuckMatch:`Wear the top ${style.tuck.toLowerCase()}, with waist and hem visible.`,
    bagMatch:style.bagStyle==='None'?'No bag at all.':`Show only the selected ${style.bagColour} ${style.bagStyle.toLowerCase()}.`,
    shadesMatch:style.sunglasses?'Show selected understated sunglasses.':'No sunglasses.',
    capMatch:style.capStyle==='None'?'No cap.':`Show the selected ${style.capStyle.toLowerCase()}.`,
    chainMatch:style.chain==='None'?'No chain.':`Show the selected ${style.chain.toLowerCase()}.`,
    watchMatch:style.watch?'Show a subtle watch.':'No watch.',
    modelMatch:'Use exactly one model of the selected gender, with the garment unobstructed.',
    outfitContinuity:'Match the accepted front image: same person, trousers and trouser colour, shoes and accessories.'
  };
  return [...new Set(fields||[])].map(field=>guidance[field]).filter(Boolean).join(' ');
}

async function generateImage({key, group, source, continuitySource=null, type, styling, repairFields=[], model='gpt-image-1.5', fetchImpl=global.fetch}) {
  if (!IMAGE_TYPES.includes(type)) throw new Error('Unsupported pilot image view.');
  const form = new FormData();
  form.append('model', model);
  const sourceExt = source.mime==='image/png'?'.png':source.mime==='image/webp'?'.webp':'.jpg';
  if(continuitySource){
    const continuityExt=continuitySource.mime==='image/png'?'.png':continuitySource.mime==='image/webp'?'.webp':'.jpg';
    form.append('image[]',new Blob([continuitySource.buf],{type:continuitySource.mime}),'matching-front'+continuityExt);
    form.append('image[]',new Blob([source.buf],{type:source.mime}),'original-garment'+sourceExt);
  } else form.append('image', new Blob([source.buf],{type:source.mime}), 'source'+sourceExt);
  form.append('prompt', imagePrompt(group,type,styling,!!continuitySource)+(repairFields.length?' Correct these specific issues from the prior draft: '+repairGuidance(repairFields,group,styling,type):''));
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

async function preflightFit({key,group,source,styling,model='gpt-4.1-mini',fetchImpl=global.fetch}) {
  const style=normalizeStyling(styling,group);
  if(garmentCategory(group)!=='upper'||style.fit==='Auto')return {status:'not-required',reason:''};
  const schema={type:'object',additionalProperties:false,required:['status','reason'],properties:{status:{type:'string',enum:['compatible','conflict','uncertain']},reason:{type:'string'}}};
  const prompt=`Look ONLY at the original garment photograph. The user selected ${style.fit} for the model image. Is that choice visibly compatible with the actual shoulder seam and cut of this ${group.productType}? A hanger, fold or camera angle alone does not prove a garment is oversized. Treat a vendor label such as 'muscle fit' as unreliable; the photo is the authority. 'Fitted' and 'Slim fit' describe a close natural-shoulder silhouette, not a rigid measurement. Return conflict ONLY for a clear, obvious visual contradiction (for example, unmistakably dropped shoulder and broad boxy cut versus fitted). If not observable, return uncertain. Do not compare any generated image or supporting trousers.`;
  const response=await fetchImpl('https://api.openai.com/v1/responses',{method:'POST',headers:{Authorization:`Bearer ${key}`,'Content-Type':'application/json'},
    body:JSON.stringify({model,store:false,max_output_tokens:180,input:[{role:'user',content:[{type:'input_text',text:prompt},{type:'input_image',image_url:`data:${source.mime};base64,${source.buf.toString('base64')}`,detail:'high'}]}],text:{format:{type:'json_schema',name:'sanki_fit_preflight',strict:true,schema}}}),signal:AbortSignal.timeout(90000)});
  const body=await readApiResponse(response),result=JSON.parse(responseText(body));
  if(!['compatible','conflict','uncertain'].includes(result.status))throw new Error('Invalid fit preflight response.');
  return {status:result.status,reason:String(result.reason||'').slice(0,220),model,usage:body.usage||null};
}

// A separate vision check, not the generator's own claim of success. The
// response is deliberately small and structured so a failed/uncertain check
// blocks approval without another image-generation charge or silent retry.
function imageCheckSchema() {
  const fields=['garmentMatch','singleFrame','angleMatch','fitMatch','pairMatch','shoeMatch','tuckMatch','bagMatch','shadesMatch','capMatch','chainMatch','watchMatch','modelMatch','outfitContinuity'];
  const finding={type:'object',additionalProperties:false,required:['status','evidence'],properties:{status:{type:'string',enum:['pass','fail','uncertain']},evidence:{type:'string'}}};
  return {type:'object',additionalProperties:false,required:fields,properties:Object.fromEntries(fields.map(field=>[field,finding]))};
}

function evaluateImageCheck(check,type,styling={},group={}) {
  const modelView=['female','male','model-front','model-side','model-side-female','model-side-male'].includes(type);
  const side=type==='model-side'||type.startsWith('model-side-');
  const style=normalizeStyling(styling,group);
  const required=['garmentMatch','singleFrame'];
  if(modelView) required.push('angleMatch','fitMatch','pairMatch','modelMatch','bagMatch','shadesMatch','capMatch','chainMatch','watchMatch');
  if(modelView&&style.shoes!=='Auto')required.push('shoeMatch');
  if(modelView&&style.tuck!=='Auto'&&garmentCategory(group)==='upper')required.push('tuckMatch');
  if(side) required.push('outfitContinuity');
  // A requested, unobtrusive accessory can disappear behind hair, a collar or
  // the camera angle. Keep that finding visible for human approval, but do not
  // discard an otherwise faithful garment and outfit (or buy another attempt).
  // Unrequested visible accessories remain a blocking mismatch.
  const advisory=modelView?required.filter(field=>(field==='chainMatch'&&style.chain!=='None'||field==='watchMatch'&&style.watch||field==='shadesMatch'&&style.sunglasses||field==='capMatch'&&style.capStyle!=='None')&&['fail','uncertain'].includes(check?.[field]?.status)):[];
  const blocking=required.filter(field=>!advisory.includes(field));
  const missing=blocking.filter(field=>!check?.[field]||!['pass','fail','uncertain'].includes(check[field].status));
  const failed=blocking.filter(field=>check?.[field]?.status==='fail');
  const uncertain=blocking.filter(field=>check?.[field]?.status==='uncertain').concat(missing);
  const issues=failed.concat(uncertain).map(field=>`${field}: ${String(check?.[field]?.evidence||'Cannot verify from this image').slice(0,180)}`);
  const warnings=advisory.map(field=>`${field}: ${String(check?.[field]?.evidence||'Accessory not clearly visible').slice(0,180)}`);
  return {status:failed.length||uncertain.length?'needs-review':'pass',failed,uncertain,issues,warnings};
}
function shouldRetryImageCheck(check,attempt,maxAttempts) {
  return check?.status==='needs-review'&&Array.isArray(check.failed)&&check.failed.length>0&&!(check.uncertain||[]).length&&attempt<maxAttempts;
}

async function verifyImage({key,group,source,generated,continuitySource=null,type,styling,model='gpt-4.1-mini',fetchImpl=global.fetch}) {
  const style=normalizeStyling(styling,group),side=type==='model-side'||type.startsWith('model-side-');
  const isModel=['female','male','model-front','model-side','model-side-female','model-side-male'].includes(type);
  const checks={type,productColour:group.colour,productType:group.productType,originalFit:group.fit,
    chosenFit:style.fit,pair:style.pair,shoes:style.shoes,tuck:style.tuck,chain:style.chain,bag:style.bagStyle,bagColour:style.bagColour,
    sunglasses:style.sunglasses,cap:style.capStyle,watch:style.watch,modelOrigin:style.modelOrigin,modelGender:type==='female'||type==='model-side-female'?'female':type==='male'||type==='model-side-male'?'male':group.audience,
    femaleComplexion:style.femaleComplexion,maleComplexion:style.maleComplexion};
  const prompt=`Independently inspect three images in this order: original product photo, generated candidate, then optional matching model-front photo. Compare visible evidence to ${JSON.stringify(checks)}. For EACH named finding return status pass, fail, or uncertain plus one short evidence sentence. Do not list generic complaints or mix criteria. Product garment must retain visible colour, neckline, sleeves and construction; if the purchase title or fit conflicts with the original photograph, the photograph wins. For a women's top, an internal 'muscle fit' label and selected fitted/slim silhouette are not by themselves a contradiction; compare the actual shoulder seam and cut. For fitMatch, compare actual shoulder seams and garment width against the original and matching front; a tuck, changed pose, drape, lighting or camera angle alone does not prove a different fit. Fail only for a clearly changed garment cut; if the source does not establish the cut, return uncertain. singleFrame means one continuous photo/one person, not a diptych. Front angle is front-facing; three-quarter means body visibly rotated about 45 degrees rather than only a different crop. For pairMatch judge the supporting TROUSER CUT by leg silhouette: straight, baggy or tailored. Off-white, beige or other neutral trouser COLOUR is not evidence that straight trousers are wrong. If the trouser silhouette is hidden, return uncertain, never fail. For shoeMatch judge visible SHOE SHAPE: loafers are loafers whether black or brown. Do not infer leather material from pixels; black loafers do NOT fail 'leather loafers'. For tuckMatch use only a clearly visible waist or hem; if hidden, uncertain, not fail. Check bag style/colour only when a bag is requested; if None, any bag fails. Check shades, cap, chain and watch against selected presence/absence; do not mention a missing cap if None was selected. modelMatch checks visible model count and apparent requested gender; do not infer nationality from a face or penalize a complexion difference that lighting could explain. When a matching front is supplied, outfitContinuity requires same visible model, trouser cut AND colour, shoes and accessories. For product-only views set all model-only findings to pass. If evidence is ambiguous, choose uncertain over fail. Report a failure only when the actual visual evidence contradicts the specific choice; do not judge one field by another field's colour or material.`;
  const content=[{type:'input_text',text:prompt},
    {type:'input_image',image_url:`data:${source.mime};base64,${source.buf.toString('base64')}`,detail:'high'},
    {type:'input_image',image_url:`data:image/png;base64,${generated.toString('base64')}`,detail:'high'}];
  if(continuitySource)content.push({type:'input_image',image_url:`data:${continuitySource.mime};base64,${continuitySource.buf.toString('base64')}`,detail:'high'});
  const response=await fetchImpl('https://api.openai.com/v1/responses',{method:'POST',headers:{Authorization:`Bearer ${key}`,'Content-Type':'application/json'},
    body:JSON.stringify({model,store:false,max_output_tokens:1000,input:[{role:'user',content}],text:{format:{type:'json_schema',name:'sanki_image_check',strict:true,schema:imageCheckSchema()}}}),
    signal:AbortSignal.timeout(90000)});
  const body=await readApiResponse(response),check=JSON.parse(responseText(body));
  return {...evaluateImageCheck(check,type,style,group),model,usage:body.usage||null};
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

module.exports={IMAGE_TYPES,pilotTypes,garmentCategory,normalizeStyling,stylingPrompt,imagePrompt,generateImage,repairGuidance,preflightFit,verifyImage,evaluateImageCheck,shouldRetryImageCheck,generateSeo,responseText,castDescription,retailFacts,seoCopyNeedsReview,isWinter};

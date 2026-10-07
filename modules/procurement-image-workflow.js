// Provider failures end a job. Checking progress never repeats a paid request.
const VERSION = 3;
function failureOf(error) {
  const api=error?.api||{},code=String(api.code||''),message=String(error?.message||'');
  let kind='provider',action='Check the saved drafts and request details before explicitly trying again.';
  if(error?.name==='AbortError' || error?.name==='TimeoutError')kind='timeout';
  if(['moderation_blocked','safety_violation'].includes(code)||/request was rejected by the safety system|moderation[_ -]blocked|safety[_ -]violation/i.test(message)){
    kind='safety';action='Review the original photo and prompt. Contact provider support with the request ID if the rejection appears mistaken.';
  }else if(['insufficient_quota','billing_hard_limit_reached','billing_not_active'].includes(code)||/no credits remaining|exceeded your current quota|insufficient.quota|billing.hard.limit/i.test(message)){
    kind='quota';action='Check the API account billing and credits before generating again. A ChatGPT subscription does not fund these API calls.';
  }else if(api.status===401||code==='invalid_api_key'){
    kind='authentication';action='Ask an administrator to check the configured API key.';
  }else if(api.status===403){kind='access';action='Ask an administrator to check the API project and model access.';
  }else if(api.status===429){kind='rate-limit';action='Wait for the provider rate limit to clear before explicitly trying again.';
  }else if(api.type==='image_generation_user_error'||[400,404,413,422].includes(api.status)){
    kind='input';action='Review the source image, request and configured model before generating again.';
  }else if(api.status>=500){kind='provider';action='The provider is unavailable. Check progress and saved drafts before explicitly trying again.';}
  return {kind,action,...(api.requestId?{requestId:api.requestId}:{})};
}
function epochOf(store){return Number(store.settings?.imageGenerationEpoch)||0;}
module.exports={VERSION,failureOf,epochOf};

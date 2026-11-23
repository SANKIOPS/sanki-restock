'use strict';
function canReview(user){const roles=user?.roles||[user?.role];return roles.includes('owner')||roles.includes('image_reviewer');}
function canAccess(user,path,method='GET'){
 const roles=user?.roles||[user?.role];if(roles.includes('owner'))return true;if(!roles.includes('image_reviewer'))return false;
 if(method==='GET')return path==='/api/image-review/catalogue'||/^\/api\/image-review\/items\/[^/]+$/.test(path)||/^\/api\/image-review\/files\/[^/]+$/.test(path);
 if(method==='PATCH')return /^\/api\/image-review\/items\/[^/]+$/.test(path);
 if(method==='POST')return path==='/api/image-review/sync'||/^\/api\/image-review\/items\/[^/]+\/(photos|queue|revise)$/.test(path);
 return false;
}
module.exports={canReview,canAccess};

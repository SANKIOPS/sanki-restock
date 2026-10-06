(function () {
  'use strict';
  // Operational controls stay hidden until the signed-in user's access is known.
  window.sankiInventoryAccess = fetch('/api/auth/me').then(function (r) {
    if (!r.ok) throw Error('Access unavailable');
    return r.json();
  }).then(function (user) {
    if (!user.success) throw Error('Access unavailable');
    var roles = user.roles || [user.role], pages = user.allowedPages;
    var admin = roles.some(function (r) { return r === 'admin' || r === 'owner'; });
    var operations = admin || roles.some(function (r) { return r === 'inventory' || r === 'warehouse'; });
    return {
      care: operations && (pages === '*' || Array.isArray(pages) && pages.indexOf('/inventory-care.html') !== -1),
      movements: operations,
      costs: admin || roles.indexOf('inventory') !== -1,
      categorization: admin
    };
  }).catch(function () {
    return { care: false, movements: false, costs: false, categorization: false };
  });
  window.sankiInventoryAccess.then(function (access) {
    document.getElementById('careShortcut').hidden = !access.care;
    document.getElementById('moveShortcut').hidden = !access.movements;
    document.getElementById('categorizationPanel').hidden = !access.categorization;
  });
})();

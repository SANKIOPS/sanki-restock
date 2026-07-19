// ═══════════════════════════════════════════════════════════════
// Shared fixed sidebar — injected into the standalone pages
// (velocity / analytics / inventory-stats / showroom-replenishment)
// so the left menu is present on every page. index.html keeps its
// own inline sidebar; in-page tabs are deep-linked via #hash.
//
// Phase 0: the sidebar is now ROLE-AWARE. It asks /api/auth/me who the
// current user is, hides the pages their role can't open (the server
// gate blocks them anyway — this just avoids dead links), shows an
// Admin section for admins, and adds a footer with the username + a
// Logout button. It renders immediately with the full menu so the page
// never looks empty, then prunes once we know the role.
// ═══════════════════════════════════════════════════════════════
(function () {
  if (document.getElementById('sanki-shared-sidebar')) return;

  var NAV = [
    { label: 'Main', items: [
      { icon: '🏠', text: 'Dashboard',            href: '/index.html#dashboard' },
      { icon: '🔄', text: 'Restock Planner',      href: '/index.html#restock' },
      { icon: '⚡', text: 'Velocity Intelligence', href: '/velocity.html' },
      { icon: '🔥', text: 'Hot Sellers',          href: '/index.html#hot' },
      { icon: '🔍', text: 'Stock Search',         href: '/rack-locations.html' },
      { icon: '📊', text: 'Analytics',            href: '/analytics.html' },
      { icon: '📈', text: 'Inventory Statistics', href: '/inventory-stats.html' }
    ] },
    { label: 'Sales', items: [
      { icon: '🧾', text: 'Record a Sale', href: '/sales.html' }
    ] },
    { label: 'Store Ops', items: [
      { icon: '🛍️', text: 'Showroom Replenishment', href: '/showroom-replenishment.html' },
      { icon: '🗄️', text: 'Rack Locations',         href: '/rack-locations.html' }
    ] },
    { label: 'Orders', items: [
      { icon: '📋', text: 'Purchase Orders', href: '/index.html#po' },
      { icon: '🏭', text: 'Suppliers',       href: '/index.html#suppliers' },
      { icon: '⚙️', text: 'Settings',        href: '/index.html#settings' }
    ] }
  ];

  // Admin-only section, appended when /api/auth/me says the user is admin.
  var ADMIN_SECTION = { label: 'Admin', items: [
    { icon: '👥', text: 'Users & Roles', href: '/admin-users.html' }
  ] };

  var current = (location.pathname.split('/').pop() || 'index.html').toLowerCase();

  var css = ''
    + 'body.ss-has-sidebar{padding-left:210px}'
    + '#sanki-shared-sidebar{position:fixed;left:0;top:0;bottom:0;width:210px;background:#fff;'
    +   'border-right:1px solid rgba(0,0,0,0.1);display:flex;flex-direction:column;overflow-y:auto;z-index:800;'
    +   "font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;color:#1a1a1a}"
    + '#sanki-shared-sidebar .ss-logo{padding:18px 16px 14px;border-bottom:1px solid rgba(0,0,0,0.1)}'
    + '#sanki-shared-sidebar .ss-logo h2{font-size:16px;font-weight:700;margin:0}'
    + '#sanki-shared-sidebar .ss-logo p{font-size:11px;color:#666;margin:2px 0 0}'
    + '#sanki-shared-sidebar .ss-nav{flex:1 1 auto}'
    + '#sanki-shared-sidebar .ss-section{padding:8px 0}'
    + '#sanki-shared-sidebar .ss-label{font-size:10px;font-weight:600;color:#999;padding:6px 16px 2px;letter-spacing:.8px;text-transform:uppercase}'
    + '#sanki-shared-sidebar .ss-item{display:flex;align-items:center;gap:9px;padding:9px 16px;font-size:13px;cursor:pointer;'
    +   'color:#666;text-decoration:none;transition:all .12s;border-left:2px solid transparent}'
    + '#sanki-shared-sidebar .ss-item:hover{background:#f0f0f0;color:#1a1a1a}'
    + '#sanki-shared-sidebar .ss-item.active{background:#f0f4ff;color:#185FA5;font-weight:600;border-left-color:#185FA5}'
    + '#sanki-shared-sidebar .ss-icon{width:16px;text-align:center;font-size:15px}'
    + '#sanki-shared-sidebar .ss-foot{border-top:1px solid rgba(0,0,0,0.1);padding:12px 16px;font-size:12px}'
    + '#sanki-shared-sidebar .ss-user{font-weight:600;color:#1a1a1a;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}'
    + '#sanki-shared-sidebar .ss-role{color:#999;font-size:11px;text-transform:capitalize;margin:1px 0 8px}'
    + '#sanki-shared-sidebar .ss-logout{display:block;width:100%;box-sizing:border-box;text-align:center;padding:7px 8px;'
    +   'font-size:12px;font-weight:600;color:#b42318;background:#fff;border:1px solid #f0c2bd;border-radius:6px;cursor:pointer}'
    + '#sanki-shared-sidebar .ss-logout:hover{background:#fef2f1}'
    + '@media(max-width:980px){body.ss-has-sidebar{padding-left:52px}'
    +   '#sanki-shared-sidebar{width:52px}'
    +   '#sanki-shared-sidebar .ss-item span:not(.ss-icon),#sanki-shared-sidebar .ss-label,'
    +   '#sanki-shared-sidebar .ss-logo p,#sanki-shared-sidebar .ss-logo h2,'
    +   '#sanki-shared-sidebar .ss-foot .ss-user,#sanki-shared-sidebar .ss-foot .ss-role,'
    +   '#sanki-shared-sidebar .ss-logout span{display:none}}';

  var style = document.createElement('style');
  style.textContent = css;
  document.head.appendChild(style);

  function esc(s) {
    return String(s).replace(/[&<>"]/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c];
    });
  }

  // Build the whole aside from a NAV array + optional user footer.
  function render(nav, me) {
    var html = '<div class="ss-logo"><h2>\u26A1 Sanki</h2><p>Business OS</p></div>';
    html += '<div class="ss-nav">';
    nav.forEach(function (sec) {
      if (!sec.items.length) return;
      html += '<div class="ss-section"><div class="ss-label">' + esc(sec.label) + '</div>';
      sec.items.forEach(function (it) {
        var target = it.href.split('#')[0].split('/').pop().toLowerCase();
        var active = (it.href.indexOf('#') < 0) && target === current;
        html += '<a class="ss-item' + (active ? ' active' : '') + '" href="' + esc(it.href) + '">'
          + '<span class="ss-icon">' + it.icon + '</span><span>' + esc(it.text) + '</span></a>';
      });
      html += '</div>';
    });
    html += '</div>'; // .ss-nav

    if (me && me.username) {
      html += '<div class="ss-foot">'
        + '<div class="ss-user" title="' + esc(me.username) + '">' + esc(me.username) + '</div>'
        + '<div class="ss-role">' + esc(me.role || '') + '</div>'
        + '<button class="ss-logout" id="ss-logout-btn"><span>Log out</span></button>'
        + '</div>';
    }
    return html;
  }

  // Keep only the sections/items this role may open. Admins keep everything.
  function pruneForRole(me) {
    if (!me) return NAV;                 // unknown → leave full menu (gate still protects)
    if (me.isAdmin) return NAV.concat([ADMIN_SECTION]);
    var allow = me.allowedPages || [];
    return NAV.map(function (sec) {
      return { label: sec.label, items: sec.items.filter(function (it) {
        var page = it.href.split('#')[0];      // '/index.html' etc.
        return allow.indexOf(page) !== -1;
      }) };
    }).filter(function (sec) { return sec.items.length; });
  }

  var aside = document.createElement('div');
  aside.id = 'sanki-shared-sidebar';
  aside.innerHTML = render(NAV, null);   // optimistic full render first

  function wireLogout() {
    var btn = document.getElementById('ss-logout-btn');
    if (!btn) return;
    btn.addEventListener('click', function () {
      fetch('/api/auth/logout', { method: 'POST' })
        .catch(function () {})
        .then(function () { location.href = '/login.html'; });
    });
  }

  function mount() {
    if (document.getElementById('sanki-shared-sidebar')) return;
    document.body.appendChild(aside);
    document.body.classList.add('ss-has-sidebar');
  }
  if (document.body) mount();
  else document.addEventListener('DOMContentLoaded', mount);

  // Learn who we are, then prune the menu + show the user footer.
  fetch('/api/auth/me', { headers: { 'Accept': 'application/json' } })
    .then(function (r) { return r.ok ? r.json() : null; })
    .then(function (me) {
      if (!me || !me.success) return;
      aside.innerHTML = render(pruneForRole(me), me);
      wireLogout();
    })
    .catch(function () { /* keep optimistic full render */ });
})();

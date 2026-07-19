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

  // The menu is now driven by the module registry (GET /api/modules) so it
  // always matches the launcher dashboard — no more hard-coded, drifting
  // list. The only fixed item is the Dashboard home link. As modules are
  // switched 'live' in the registry they appear here automatically.
  var HOME_ITEM = { icon: '🏠', text: 'Dashboard', href: '/dashboard.html' };

  // Build the sidebar sections from the /api/modules payload.
  function navFromModules(data) {
    var mods = (data && data.modules) || [];
    var order = (data && data.sectionOrder) || [];
    var nav = [{ label: 'Main', items: [HOME_ITEM] }];
    var groups = {};
    mods.forEach(function (m) { (groups[m.section] = groups[m.section] || []).push(m); });
    Object.keys(groups).sort(function (a, b) {
      var ia = order.indexOf(a), ib = order.indexOf(b);
      return (ia < 0 ? 99 : ia) - (ib < 0 ? 99 : ib);
    }).forEach(function (sec) {
      var items = groups[sec].map(function (m) {
        return { icon: m.icon || '▪', text: m.title, href: m.href };
      });
      // Fold "Main" modules into the existing Main section under Dashboard.
      if (sec === 'Main') { nav[0].items = nav[0].items.concat(items); }
      else { nav.push({ label: sec, items: items }); }
    });
    return nav;
  }

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

  var aside = document.createElement('div');
  aside.id = 'sanki-shared-sidebar';
  // Optimistic first paint: just the Dashboard home link; real modules fill
  // in once /api/modules responds, so the menu never looks broken.
  aside.innerHTML = render([{ label: 'Main', items: [HOME_ITEM] }], null);

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

  // Learn who we are + which modules are live, then build the real menu.
  Promise.all([
    fetch('/api/auth/me',   { headers: { 'Accept': 'application/json' } }).then(function (r) { return r.ok ? r.json() : null; }).catch(function () { return null; }),
    fetch('/api/modules',   { headers: { 'Accept': 'application/json' } }).then(function (r) { return r.ok ? r.json() : null; }).catch(function () { return null; })
  ]).then(function (out) {
    var me = out[0], mods = out[1];
    if (!me || !me.success) return;      // not logged in — leave optimistic render
    aside.innerHTML = render(navFromModules(mods), me);
    wireLogout();
  });
})();

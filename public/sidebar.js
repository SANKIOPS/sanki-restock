// ═══════════════════════════════════════════════════════════════
// Shared fixed sidebar — injected into the standalone pages
// (velocity / analytics / inventory-stats / showroom-replenishment)
// so the left menu is present on every page. index.html keeps its
// own inline sidebar; in-page tabs are deep-linked via #hash.
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

  var current = (location.pathname.split('/').pop() || 'index.html').toLowerCase();

  var css = ''
    + 'body.ss-has-sidebar{padding-left:210px}'
    + '#sanki-shared-sidebar{position:fixed;left:0;top:0;bottom:0;width:210px;background:#fff;'
    +   'border-right:1px solid rgba(0,0,0,0.1);display:flex;flex-direction:column;overflow-y:auto;z-index:800;'
    +   "font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;color:#1a1a1a}"
    + '#sanki-shared-sidebar .ss-logo{padding:18px 16px 14px;border-bottom:1px solid rgba(0,0,0,0.1)}'
    + '#sanki-shared-sidebar .ss-logo h2{font-size:16px;font-weight:700;margin:0}'
    + '#sanki-shared-sidebar .ss-logo p{font-size:11px;color:#666;margin:2px 0 0}'
    + '#sanki-shared-sidebar .ss-section{padding:8px 0}'
    + '#sanki-shared-sidebar .ss-label{font-size:10px;font-weight:600;color:#999;padding:6px 16px 2px;letter-spacing:.8px;text-transform:uppercase}'
    + '#sanki-shared-sidebar .ss-item{display:flex;align-items:center;gap:9px;padding:9px 16px;font-size:13px;cursor:pointer;'
    +   'color:#666;text-decoration:none;transition:all .12s;border-left:2px solid transparent}'
    + '#sanki-shared-sidebar .ss-item:hover{background:#f0f0f0;color:#1a1a1a}'
    + '#sanki-shared-sidebar .ss-item.active{background:#f0f4ff;color:#185FA5;font-weight:600;border-left-color:#185FA5}'
    + '#sanki-shared-sidebar .ss-icon{width:16px;text-align:center;font-size:15px}'
    + '@media(max-width:980px){body.ss-has-sidebar{padding-left:52px}'
    +   '#sanki-shared-sidebar{width:52px}'
    +   '#sanki-shared-sidebar .ss-item span:not(.ss-icon),#sanki-shared-sidebar .ss-label,'
    +   '#sanki-shared-sidebar .ss-logo p,#sanki-shared-sidebar .ss-logo h2{display:none}}';

  var style = document.createElement('style');
  style.textContent = css;
  document.head.appendChild(style);

  function esc(s) {
    return String(s).replace(/[&<>"]/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c];
    });
  }

  var html = '<div class="ss-logo"><h2>\u26A1 Sanki</h2><p>Restocking Dashboard</p></div>';
  NAV.forEach(function (sec) {
    html += '<div class="ss-section"><div class="ss-label">' + esc(sec.label) + '</div>';
    sec.items.forEach(function (it) {
      // Highlight only the standalone page we're currently on (index hash links never match here).
      var target = it.href.split('#')[0].split('/').pop().toLowerCase();
      var active = (it.href.indexOf('#') < 0) && target === current;
      html += '<a class="ss-item' + (active ? ' active' : '') + '" href="' + esc(it.href) + '">'
        + '<span class="ss-icon">' + it.icon + '</span><span>' + esc(it.text) + '</span></a>';
    });
    html += '</div>';
  });

  var aside = document.createElement('div');
  aside.id = 'sanki-shared-sidebar';
  aside.innerHTML = html;

  function mount() {
    if (document.getElementById('sanki-shared-sidebar')) return;
    document.body.appendChild(aside);
    document.body.classList.add('ss-has-sidebar');
  }
  if (document.body) mount();
  else document.addEventListener('DOMContentLoaded', mount);
})();

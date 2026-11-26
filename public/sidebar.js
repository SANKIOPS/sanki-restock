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

  // Every standalone reporting page receives the same inclusive Till Date
  // control, including reports whose filters are rendered after navigation.
  if(!document.querySelector('script[data-sanki-till-date]')){
    var tillDateScript=document.createElement('script');
    tillDateScript.src='/till-date-filter.js';
    tillDateScript.defer=true;
    tillDateScript.dataset.sankiTillDate='1';
    document.head.appendChild(tillDateScript);
  }

  // The menu is now driven by the module registry (GET /api/modules) so it
  // always matches the launcher dashboard — no more hard-coded, drifting
  // list. The only fixed item is the Dashboard home link. As modules are
  // switched 'live' in the registry they appear here automatically.
  var HOME_ITEM = { icon: '🏠', text: 'Dashboard', href: '/dashboard.html' };

  // Build the sidebar sections from the /api/modules payload.
  function navFromModules(data,me) {
    var contentOnly=me&&(me.roles||[me.role]).length===1&&(me.roles||[me.role])[0]==='content_team';
    var mods = (data && data.modules) || [];
    var order = (data && data.sectionOrder) || [];
    if(contentOnly)return [{label:'Marketing',items:[{icon:'▤',text:'Content & Posting',href:'/content-production.html'}]}];
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
    + '#sanki-shared-sidebar .ss-find{width:100%;margin-top:10px;padding:7px 8px;border:1px solid #b8cfc4;border-radius:7px;background:#f8f5eb;color:#075f49;font:600 11px inherit;cursor:pointer;text-align:left}'
    + '#sanki-shared-sidebar .ss-find:hover{background:#e8f2eb}'
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
    + '#ss-quick-find{width:min(920px,94vw);max-height:88vh;padding:0;border:1px solid #b8cfc4;border-radius:16px;background:#fbf8ef;color:#173f34;box-shadow:0 22px 70px #102c2445;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}'
    + '#ss-quick-find::backdrop{background:#17372e70}'
    + '#ss-quick-find .qf-head{display:flex;justify-content:space-between;align-items:start;gap:16px;padding:20px 22px 12px}'
    + '#ss-quick-find h2{margin:0;font:600 25px Georgia,serif;color:#143f33}'
    + '#ss-quick-find .qf-help{margin:4px 0 0;color:#65766f;font-size:12px}'
    + '#ss-quick-find .qf-close{border:1px solid #c8d4ce;border-radius:8px;background:#fffdf8;padding:7px 11px;color:#204b3f;cursor:pointer}'
    + '#ss-quick-find .qf-controls{display:grid;grid-template-columns:minmax(230px,1fr) 115px 165px 130px 130px;gap:9px;padding:10px 22px 16px;border-bottom:1px solid #d9dfd7}'
    + '#ss-quick-find input,#ss-quick-find select{min-width:0;border:1px solid #bdcbbf;border-radius:8px;background:#fffefb;padding:10px;font:inherit;color:#173f34}'
    + '#ss-quick-find .qf-status{min-height:20px;margin:12px 22px;color:#65766f;font-size:12px}'
    + '#ss-quick-find .qf-status.bad{color:#a33624}'
    + '#ss-quick-find .qf-results{max-height:58vh;overflow:auto;padding:0 22px 22px}'
    + '#ss-quick-find .qf-summary{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:12px}'
    + '#ss-quick-find .qf-stat{padding:8px 10px;border:1px solid #d0dad1;border-radius:999px;background:#fffefb;font-size:12px}'
    + '#ss-quick-find .qf-group{margin:17px 0 7px;font-size:13px;text-transform:uppercase;letter-spacing:.5px;color:#4f6a60}'
    + '#ss-quick-find .qf-row{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:12px;padding:12px 13px;border:1px solid #d5ddd5;border-radius:10px;background:#fffefb;color:inherit;text-decoration:none;margin:7px 0}'
    + '#ss-quick-find .qf-row:hover{border-color:#5e9d84;background:#f0f6ef}'
    + '#ss-quick-find .qf-title{font-weight:700}#ss-quick-find .qf-meta{margin-top:3px;color:#667971;font-size:12px;overflow-wrap:anywhere}'
    + '#ss-quick-find .qf-amount{font-weight:750;color:#076b50;white-space:nowrap}'
    + '@media(max-width:980px){body.ss-has-sidebar{padding-left:52px}'
    +   '#sanki-shared-sidebar{width:52px}'
    +   '#sanki-shared-sidebar .ss-item span:not(.ss-icon),#sanki-shared-sidebar .ss-label,'
    +   '#sanki-shared-sidebar .ss-logo p,#sanki-shared-sidebar .ss-logo h2,'
    +   '#sanki-shared-sidebar .ss-find span,'
    +   '#sanki-shared-sidebar .ss-foot .ss-user,#sanki-shared-sidebar .ss-foot .ss-role,'
    +   '#sanki-shared-sidebar .ss-logout span{display:none}'
    +   '#sanki-shared-sidebar .ss-foot{padding:8px 6px}'
    +   '#sanki-shared-sidebar .ss-logout{display:flex;width:40px;height:40px;padding:0;align-items:center;justify-content:center;'
    +     'font-size:20px;border-radius:10px;background:#fff7f6}'
    +   '#sanki-shared-sidebar .ss-find{width:40px;height:40px;margin:0 0 0 -10px;padding:0;text-align:center;font-size:17px}'
    +   '#ss-quick-find .qf-controls{grid-template-columns:1fr 1fr}#ss-quick-find .qf-query{grid-column:1/-1}'
    +   '#sanki-shared-sidebar .ss-logout-icon{display:inline!important}}';

  var style = document.createElement('style');
  style.textContent = css;
  document.head.appendChild(style);

  function esc(s) {
    return String(s).replace(/[&<>"]/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c];
    });
  }

  function quickSearchAllowed(me) {
    var roles=(me&&me.roles)||(me&&me.role?[me.role]:[]),permissions=me&&me.permissions||{};
    return roles.some(function(role){return ['owner','admin','accounting','samast_accounting'].includes(role);})||permissions.personalAccounting===true;
  }

  // Build the whole aside from a NAV array + optional user footer.
  function render(nav, me) {
    var html = '<div class="ss-logo"><h2>\u26A1 Sanki</h2><p>Business OS</p>'+(quickSearchAllowed(me)?'<button class="ss-find" id="ss-find-money" type="button" title="Find an expense or transfer (Ctrl/Command + K)">🔎 <span>Find money</span></button>':'')+'</div>';
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
        + '<button class="ss-logout" id="ss-logout-btn" type="button" aria-label="Log out" title="Log out">'
        + '<b class="ss-logout-icon" aria-hidden="true">↪</b><span> Log out</span></button>'
        + '</div>';
    }
    return html;
  }

  var aside = document.createElement('div');
  aside.id = 'sanki-shared-sidebar';
  // Optimistic first paint: just the Dashboard home link; real modules fill
  // in once /api/modules responds, so the menu never looks broken.
  aside.innerHTML = render([], null);

  function wireLogout() {
    var btn = document.getElementById('ss-logout-btn');
    if (!btn) return;
    btn.addEventListener('click', function () {
      fetch('/api/auth/logout', { method: 'POST' })
        .catch(function () {})
        .then(function () { location.href = '/login.html'; });
    });
  }

  var quickFindDialog=null,quickFindTimer=0,quickFindGeneration=0;
  function money(value){return '₹'+Number(value||0).toLocaleString('en-IN',{minimumFractionDigits:2,maximumFractionDigits:2});}
  function quickFindLink(row,kind){
    if(kind==='expense')return '/expenses.html?tab=list&focus='+encodeURIComponent(row.id);
    return '/expenses.html?tab=cash&nature='+encodeURIComponent(row.fromNature)+'&account='+encodeURIComponent(row.fromAccount)+'&ledgerFrom='+encodeURIComponent(row.date)+'&ledgerTo='+encodeURIComponent(row.date);
  }
  function quickFindRow(row,kind,host){
    var link=document.createElement('a');link.className='qf-row';link.href=quickFindLink(row,kind);
    var body=document.createElement('div'),title=document.createElement('div'),meta=document.createElement('div'),amount=document.createElement('div');title.className='qf-title';meta.className='qf-meta';amount.className='qf-amount';
    title.textContent=kind==='transfer'?row.id+' · '+row.fromAccount+' → '+row.toAccount:row.id+' · '+(row.vendor||row.particulars||'Expense');
    meta.textContent=kind==='transfer'?[row.date,row.classification&&row.classification.replaceAll('_',' '),row.note].filter(Boolean).join(' · '):[row.date,row.nature,row.group&&'Group: '+row.group,'Category: '+row.category,row.particulars,row.account].filter(Boolean).join(' · ');
    amount.textContent=money(row.amount);body.append(title,meta);link.append(body,amount);host.appendChild(link);
  }
  function renderQuickFind(data){
    setQuickFindGroups(data.groups||[]);
    var results=quickFindDialog.querySelector('.qf-results');results.replaceChildren();
    var summary=document.createElement('div');summary.className='qf-summary';
    var stats=data.pair?[['Transferred '+data.pair.from+' → '+data.pair.to,money(data.summary.totalTransferred)],['Matching transfers',data.summary.transferCount]]:[['Transfers',data.summary.transferCount+' · '+money(data.summary.totalTransferred)],['Expenses',data.summary.expenseCount+' · '+money(data.summary.totalExpenses)]];
    stats.forEach(function(stat){var item=document.createElement('div');item.className='qf-stat';item.textContent=stat[0]+': '+stat[1];summary.appendChild(item);});results.appendChild(summary);
    function group(label,rows,kind){if(!rows.length)return;var heading=document.createElement('h3');heading.className='qf-group';heading.textContent=label;results.appendChild(heading);rows.forEach(function(row){quickFindRow(row,kind,results);});}
    group('Transfers',data.transfers||[],'transfer');group('Expenses',data.expenses||[],'expense');
    if(!(data.transfers||[]).length&&!(data.expenses||[]).length){var empty=document.createElement('p');empty.className='qf-help';empty.textContent='No accessible expense or transfer matches this search.';results.appendChild(empty);}
  }
  function setQuickFindGroups(groups){
    if(!quickFindDialog)return;var select=quickFindDialog.querySelector('.qf-expense-group'),selected=select.value;select.replaceChildren();var all=document.createElement('option');all.value='';all.textContent='All expense groups';select.appendChild(all);groups.forEach(function(name){var option=document.createElement('option');option.value=name;option.textContent=name;select.appendChild(option);});if(groups.includes(selected))select.value=selected;
  }
  function loadQuickFindGroups(){
    fetch('/api/expenses/quick-find?q=&kind=expense',{cache:'no-store',headers:{Accept:'application/json'}}).then(function(response){return response.ok?response.json():null;}).then(function(data){if(data&&data.success)setQuickFindGroups(data.groups||[]);}).catch(function(){});
  }
  function runQuickFind(){
    if(!quickFindDialog)return;var input=quickFindDialog.querySelector('.qf-query'),query=input.value.trim(),expenseGroup=quickFindDialog.querySelector('.qf-expense-group').value,status=quickFindDialog.querySelector('.qf-status'),results=quickFindDialog.querySelector('.qf-results');clearTimeout(quickFindTimer);
    if(query.length<2&&!expenseGroup){quickFindGeneration++;status.className='qf-status';status.textContent='Type an expense, transfer, vendor, details—or choose an expense group. For transfers, try 0993 to 3448.';results.replaceChildren();return;}
    if(query.length===1){quickFindGeneration++;status.className='qf-status bad';status.textContent='Enter at least two characters, or choose an expense group.';results.replaceChildren();return;}
    var generation=++quickFindGeneration,params=new URLSearchParams({q:query,kind:quickFindDialog.querySelector('.qf-kind').value,group:expenseGroup,from:quickFindDialog.querySelector('.qf-from').value,to:quickFindDialog.querySelector('.qf-to').value});status.className='qf-status';status.textContent='Searching accessible accounting records…';
    fetch('/api/expenses/quick-find?'+params.toString(),{cache:'no-store',headers:{Accept:'application/json'}}).then(function(response){return response.json().catch(function(){return{};}).then(function(data){if(!response.ok||!data.success)throw Error(data.error||'Search could not be completed.');return data;});}).then(function(data){if(generation!==quickFindGeneration)return;renderQuickFind(data);status.textContent=data.truncated?'Totals include every match; showing the first results. Add a date, account, vendor or reference to narrow it.':'Open any result to view its saved ledger entry.';}).catch(function(error){if(generation!==quickFindGeneration)return;results.replaceChildren();status.className='qf-status bad';status.textContent=error.message;});
  }
  function ensureQuickFind(){
    if(quickFindDialog)return quickFindDialog;var dialog=document.createElement('dialog');dialog.id='ss-quick-find';
    var head=document.createElement('div');head.className='qf-head';var intro=document.createElement('div'),title=document.createElement('h2'),help=document.createElement('p'),close=document.createElement('button');title.textContent='Find expense or transfer';help.className='qf-help';help.textContent='Type any related detail—account, vendor, amount, category, narration, proof or reference. Exact wording is not required. Example: 0993 to 3448.';close.className='qf-close';close.type='button';close.textContent='Close';intro.append(title,help);head.append(intro,close);
    var controls=document.createElement('div');controls.className='qf-controls';var query=document.createElement('input');query.type='search';query.className='qf-query';query.placeholder='Any account, expense, vendor, amount, narration, proof or reference';query.autocomplete='off';query.setAttribute('aria-label','Find an expense or transfer');var kind=document.createElement('select');kind.className='qf-kind';kind.setAttribute('aria-label','Search type');[['all','All'],['transfer','Transfers'],['expense','Expenses']].forEach(function(choice){var option=document.createElement('option');option.value=choice[0];option.textContent=choice[1];kind.appendChild(option);});var expenseGroup=document.createElement('select');expenseGroup.className='qf-expense-group';expenseGroup.setAttribute('aria-label','Expense group');var allGroups=document.createElement('option');allGroups.value='';allGroups.textContent='All expense groups';expenseGroup.appendChild(allGroups);var from=document.createElement('input');from.type='date';from.className='qf-from';from.setAttribute('aria-label','From date');var to=document.createElement('input');to.type='date';to.className='qf-to';to.setAttribute('aria-label','To date');controls.append(query,kind,expenseGroup,from,to);
    var status=document.createElement('p');status.className='qf-status';status.setAttribute('role','status');status.setAttribute('aria-live','polite');var results=document.createElement('div');results.className='qf-results';dialog.append(head,controls,status,results);document.body.appendChild(dialog);quickFindDialog=dialog;
    close.addEventListener('click',function(){dialog.close();});query.addEventListener('input',function(){clearTimeout(quickFindTimer);quickFindTimer=setTimeout(runQuickFind,260);});query.addEventListener('keydown',function(event){if(event.key==='Enter'){event.preventDefault();runQuickFind();}});kind.addEventListener('change',function(){expenseGroup.disabled=kind.value==='transfer';if(expenseGroup.disabled)expenseGroup.value='';runQuickFind();});[expenseGroup,from,to].forEach(function(field){field.addEventListener('change',runQuickFind);});dialog.addEventListener('close',function(){quickFindGeneration++;clearTimeout(quickFindTimer);});loadQuickFindGroups();return dialog;
  }
  function openQuickFind(){var dialog=ensureQuickFind();if(!dialog.open)dialog.showModal();setTimeout(function(){dialog.querySelector('.qf-query').focus();},0);runQuickFind();}
  function wireQuickFind(me){var button=document.getElementById('ss-find-money');if(button&&quickSearchAllowed(me))button.addEventListener('click',openQuickFind);}

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
    aside.innerHTML = render(navFromModules(mods,me), me);
    wireLogout();
    wireQuickFind(me);
    if (String(me.username || '').toLowerCase() === 'prashant' && !document.querySelector('script[data-daily-sop-reminders]')) {
      var sopScript = document.createElement('script'); sopScript.src = '/daily-sop-reminders.js?v=20261126'; sopScript.dataset.dailySopReminders = '1'; document.head.appendChild(sopScript);
    }
    if (((me.roles || [me.role]).includes('owner') || String(me.username||'').toLowerCase()==='prashant') && !document.querySelector('script[data-content-deletions]')) {
      var deletionScript=document.createElement('script');deletionScript.src='/content-deletion-notifications.js?v=20261117';deletionScript.dataset.contentDeletions='1';document.head.appendChild(deletionScript);
    }

    if ((me.roles || [me.role]).includes('owner') && !document.querySelector('script[data-rental-notifications]')) {
      var rentalScript = document.createElement('script');
      rentalScript.src = '/rental-notifications.js?v=20261001';
      rentalScript.dataset.rentalNotifications = '1';
      document.head.appendChild(rentalScript);
    }
  });
  document.addEventListener('keydown',function(event){if((event.metaKey||event.ctrlKey)&&String(event.key).toLowerCase()==='k'&&document.getElementById('ss-find-money')){event.preventDefault();openQuickFind();}});
})();

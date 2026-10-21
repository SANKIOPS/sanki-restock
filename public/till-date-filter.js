(function(){
  'use strict';
  if(window.__sankiTillDateFilter)return;
  window.__sankiTillDateFilter=true;
  var controllers=[];

  var style=document.createElement('style');
  style.textContent='.sanki-date-mode{display:inline-flex;align-self:flex-end;padding:3px;border:1px solid #d8e0e7;border-radius:10px;background:#f4f7f9;gap:2px;white-space:nowrap}.sanki-date-mode button{appearance:none;border:0;border-radius:7px;background:transparent;color:#64748b;font:inherit;font-size:12px;font-weight:700;line-height:1;padding:8px 11px;cursor:pointer}.sanki-date-mode button[aria-pressed="true"]{background:#185fa5;color:#fff;box-shadow:0 1px 3px rgba(15,23,42,.16)}.sanki-date-mode button:focus-visible{outline:2px solid #60a5fa;outline-offset:2px}.sanki-till-disabled{opacity:.48}.sanki-till-disabled input{background:#eef2f5!important;cursor:not-allowed}@media(max-width:640px){.sanki-date-mode{width:100%;box-sizing:border-box}.sanki-date-mode button{flex:1}}';
  document.head.appendChild(style);

  function day(){var d=new Date(Date.now()+5.5*3600000);return d.toISOString().slice(0,10);}
  function rollingRange(from,to){
    var end=day();
    if(to.max&&end>to.max)end=to.max;
    var start=new Date(end+'T00:00:00Z');start.setUTCDate(start.getUTCDate()-179);
    start=start.toISOString().slice(0,10);
    // Reports such as P&L cannot precede their accounting start date.
    if(from.min&&start<from.min)start=from.min;
    return {from:start,to:end};
  }
  function semantic(input,word){var id=String(input.id||'').toLowerCase(),name=String(input.name||'').toLowerCase(),title=String(input.title||'').toLowerCase(),wrap=input.closest('.fld,label'),label=wrap&&wrap.querySelector('label'),text=String(label&&label.textContent||wrap&&wrap.textContent||'').trim().toLowerCase();return input.dataset.dateFilter===word||id===word||id.endsWith('_'+word)||id.endsWith('-'+word)||id.endsWith(word)||name===word||title===word+' date'||text===word;}
  function pairedFrom(to){
    // Use the smallest common container, not the first From field in a main
    // element. Dashboard pages can contain several unrelated report filters.
    var expected=String(to.id||'').replace(/to$/i,'from').toLowerCase(),exact=Array.prototype.find.call(document.querySelectorAll('input[type="date"]'),function(input){return input.id&&input.id.toLowerCase()===expected;});
    if(exact&&exact!==to&&exact.type==='date')return exact;
    var root=to.parentElement;
    while(root&&root!==document.body){
      var dates=Array.prototype.slice.call(root.querySelectorAll('input[type="date"]')).filter(function(x){return x!==to&&semantic(x,'from');});
      if(dates.length===1)return dates[0];
      if(dates.length>1)return null;
      root=root.parentElement;
    }
    return null;
  }
  function refresh(root,control){
    var candidates=Array.prototype.slice.call(root.querySelectorAll('button,input[type="button"],input[type="submit"]'));
    var action=candidates.find(function(x){if(x.disabled||control.contains(x))return false;var key=(String(x.id||'')+' '+String(x.textContent||x.value||'')).toLowerCase();return /\b(apply|filter|search|show|load|view|go)\b/.test(key);});
    if(action)setTimeout(function(){action.click();},0);
  }
  function enhance(){
    controllers=controllers.filter(function(c){return c.from.isConnected&&c.to.isConnected;});
    Array.prototype.forEach.call(document.querySelectorAll('input[type="date"]'),function(to){
      if(to.dataset.tillDateEnhanced||to.readOnly||!semantic(to,'to'))return;
      var from=pairedFrom(to);if(!from||from.readOnly)return;
      to.dataset.tillDateEnhanced='1';
      var root=to.parentElement;
      while(root&&!root.contains(from))root=root.parentElement;
      if(!root)return;
      var fromWrap=from.closest('.fld,label')||from;
      var control=document.createElement('div');control.className='sanki-date-mode';control.setAttribute('role','group');control.setAttribute('aria-label','Date filter mode');
      var range=document.createElement('button');range.type='button';range.textContent='Date range';range.setAttribute('aria-pressed','true');
      var till=document.createElement('button');till.type='button';till.textContent='Till date';till.title='Show all records up to and including the selected date';till.setAttribute('aria-pressed','false');
      var recent=document.createElement('button');recent.type='button';recent.textContent='Last 180 days';recent.title='Approximately 6 months: 180 calendar days including today (IST), limited to the report’s allowed dates';recent.setAttribute('aria-pressed','false');
      control.appendChild(range);control.appendChild(till);control.appendChild(recent);
      fromWrap.insertAdjacentElement('beforebegin',control);

      var updating=false,previousFrom=from.value,previousTo=to.value;
      function setMode(mode,run){
        var isTill=mode==='till';
        updating=true;
        if(isTill){if(from.value)from.dataset.rangeValue=from.value;from.value='';if(!to.value)to.value=day();}
        else if(mode==='180'){var period=rollingRange(from,to);from.value=period.from;to.value=period.to;from.dataset.rangeValue=from.value;}
        else if(!from.value&&from.dataset.rangeValue)from.value=from.dataset.rangeValue;
        from.disabled=isTill;fromWrap.classList.toggle('sanki-till-disabled',isTill);
        range.setAttribute('aria-pressed',String(!isTill&&mode!=='180'));till.setAttribute('aria-pressed',String(isTill));recent.setAttribute('aria-pressed',String(mode==='180'));
        from.dispatchEvent(new Event('input',{bubbles:true}));to.dispatchEvent(new Event('input',{bubbles:true}));
        from.dispatchEvent(new Event('change',{bubbles:true}));to.dispatchEvent(new Event('change',{bubbles:true}));
        if(mode==='180'){
          var scope=root.closest('.card,main')||root;
          Array.prototype.forEach.call(scope.querySelectorAll('[data-range],[data-days],[data-pdays],[data-cfdays],[data-period]'),function(button){
            button.classList.remove('active');
            if(button.classList.contains('primary')){button.classList.remove('primary');button.classList.add('ghost');}
          });
        }
        updating=false;
        previousFrom=from.value;previousTo=to.value;
        if(run)refresh(root,control);
      }
      range.addEventListener('click',function(){setMode('range',true);});
      till.addEventListener('click',function(){setMode('till',true);});
      recent.addEventListener('click',function(){setMode('180',true);});
      function manualChange(){
        if(updating)return;
        recent.setAttribute('aria-pressed','false');range.setAttribute('aria-pressed',String(!from.disabled));
        previousFrom=from.value;previousTo=to.value;
      }
      from.addEventListener('input',manualChange);to.addEventListener('input',manualChange);
      from.addEventListener('change',manualChange);to.addEventListener('change',manualChange);
      // Native quick ranges often set values without dispatching change.
      // Keep Till date / 180-day state in sync without running another query.
      controllers.push({from:from,to:to,syncNative:function(){
        if(from.value&&(from.value!==previousFrom||to.value!==previousTo)){from.disabled=false;fromWrap.classList.remove('sanki-till-disabled');till.setAttribute('aria-pressed','false');manualChange();}
      }});
    });
  }
  document.addEventListener('click',function(event){
    if(!event.target.closest('[data-range],[data-days],[data-pdays],[data-cfdays],[data-period]'))return;
    setTimeout(function(){controllers.forEach(function(c){if(c.from.isConnected&&c.to.isConnected)c.syncNative();});},0);
  });
  if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',enhance);else enhance();
  new MutationObserver(enhance).observe(document.documentElement,{childList:true,subtree:true});
})();

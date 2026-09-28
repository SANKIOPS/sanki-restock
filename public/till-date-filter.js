(function(){
  'use strict';
  if(window.__sankiTillDateFilter)return;
  window.__sankiTillDateFilter=true;

  var style=document.createElement('style');
  style.textContent='.sanki-date-mode{display:inline-flex;align-self:flex-end;padding:3px;border:1px solid #d8e0e7;border-radius:10px;background:#f4f7f9;gap:2px;white-space:nowrap}.sanki-date-mode button{appearance:none;border:0;border-radius:7px;background:transparent;color:#64748b;font:inherit;font-size:12px;font-weight:700;line-height:1;padding:8px 11px;cursor:pointer}.sanki-date-mode button[aria-pressed="true"]{background:#185fa5;color:#fff;box-shadow:0 1px 3px rgba(15,23,42,.16)}.sanki-date-mode button:focus-visible{outline:2px solid #60a5fa;outline-offset:2px}.sanki-till-disabled{opacity:.48}.sanki-till-disabled input{background:#eef2f5!important;cursor:not-allowed}@media(max-width:640px){.sanki-date-mode{width:100%;box-sizing:border-box}.sanki-date-mode button{flex:1}}';
  document.head.appendChild(style);

  function day(){var d=new Date(Date.now()+5.5*3600000);return d.toISOString().slice(0,10);}
  function semantic(input,word){var id=String(input.id||'').toLowerCase(),name=String(input.name||'').toLowerCase(),title=String(input.title||'').toLowerCase(),wrap=input.closest('.fld,label'),label=wrap&&wrap.querySelector('label'),text=String(label&&label.textContent||wrap&&wrap.textContent||'').trim().toLowerCase();return id===word||id.endsWith('_'+word)||id.endsWith('-'+word)||id.endsWith(word)||name===word||title===word+' date'||text===word;}
  function pairedFrom(to){
    var root=to.closest('.filters,.toolbar,.report-filters,.date-range,form,.card,main')||to.parentElement&&to.parentElement.parentElement||document;
    var dates=Array.prototype.slice.call(root.querySelectorAll('input[type="date"]'));
    return dates.find(function(x){return x!==to&&semantic(x,'from');})||null;
  }
  function refresh(root,control){
    var candidates=Array.prototype.slice.call(root.querySelectorAll('button,input[type="button"],input[type="submit"]'));
    var action=candidates.find(function(x){if(control.contains(x))return false;var key=(String(x.id||'')+' '+String(x.textContent||x.value||'')).toLowerCase();return /\b(apply|filter|search|show|load|view|go)\b/.test(key);});
    if(action)setTimeout(function(){action.click();},0);
  }
  function enhance(){
    Array.prototype.forEach.call(document.querySelectorAll('input[type="date"]'),function(to){
      if(to.dataset.tillDateEnhanced||!semantic(to,'to'))return;
      var from=pairedFrom(to);if(!from)return;
      to.dataset.tillDateEnhanced='1';
      var root=to.closest('.filters,.toolbar,.report-filters,.date-range,form,.card,main')||document;
      var fromWrap=from.closest('.fld,label')||from.parentElement;
      var control=document.createElement('div');control.className='sanki-date-mode';control.setAttribute('role','group');control.setAttribute('aria-label','Date filter mode');
      var range=document.createElement('button');range.type='button';range.textContent='Date range';range.setAttribute('aria-pressed','true');
      var till=document.createElement('button');till.type='button';till.textContent='Till date';till.title='Show all records up to and including the selected date';till.setAttribute('aria-pressed','false');
      control.appendChild(range);control.appendChild(till);
      fromWrap.insertAdjacentElement('beforebegin',control);

      function setMode(mode,run){
        var isTill=mode==='till';
        if(isTill){if(from.value)from.dataset.rangeValue=from.value;from.value='';if(!to.value)to.value=day();}
        else if(!from.value&&from.dataset.rangeValue)from.value=from.dataset.rangeValue;
        from.disabled=isTill;fromWrap.classList.toggle('sanki-till-disabled',isTill);
        range.setAttribute('aria-pressed',String(!isTill));till.setAttribute('aria-pressed',String(isTill));
        from.dispatchEvent(new Event('change',{bubbles:true}));to.dispatchEvent(new Event('change',{bubbles:true}));
        if(run)refresh(root,control);
      }
      range.addEventListener('click',function(){setMode('range',true);});
      till.addEventListener('click',function(){setMode('till',true);});
    });
  }
  if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',enhance);else enhance();
  new MutationObserver(enhance).observe(document.documentElement,{childList:true,subtree:true});
})();

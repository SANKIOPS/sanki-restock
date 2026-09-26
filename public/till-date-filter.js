(function(){
  'use strict';
  if(window.__sankiTillDateFilter)return;
  window.__sankiTillDateFilter=true;
  function day(){var d=new Date(Date.now()+5.5*3600000);return d.toISOString().slice(0,10);}
  function semantic(input,word){var id=String(input.id||'').toLowerCase(),name=String(input.name||'').toLowerCase(),title=String(input.title||'').toLowerCase(),wrap=input.closest('.fld,label'),label=wrap&&wrap.querySelector('label'),text=String(label&&label.textContent||wrap&&wrap.textContent||'').trim().toLowerCase();return id===word||id.endsWith('_'+word)||id.endsWith('-'+word)||id.endsWith(word)||name===word||title===word+' date'||text===word;}
  function pairedFrom(to){
    var root=to.closest('.filters,.toolbar,.report-filters,.date-range,form,.card,main')||to.parentElement&&to.parentElement.parentElement||document;
    var dates=Array.prototype.slice.call(root.querySelectorAll('input[type="date"]'));
    return dates.find(function(x){return x!==to&&semantic(x,'from');})||null;
  }
  function refresh(root,button){
    var candidates=Array.prototype.slice.call(root.querySelectorAll('button,input[type="button"],input[type="submit"]'));
    var action=candidates.find(function(x){if(x===button)return false;var key=(String(x.id||'')+' '+String(x.textContent||x.value||'')).toLowerCase();return /\b(apply|filter|search|show|load|view|go)\b/.test(key);});
    if(action)setTimeout(function(){action.click();},0);
  }
  function enhance(){
    Array.prototype.forEach.call(document.querySelectorAll('input[type="date"]'),function(to){
      if(to.dataset.tillDateEnhanced||!semantic(to,'to'))return;
      var from=pairedFrom(to);if(!from)return;
      to.dataset.tillDateEnhanced='1';
      var button=document.createElement('button');button.type='button';button.className='sanki-till-date';button.textContent='Till Date';button.title='Show all records up to and including the selected To date';button.style.cssText='margin-left:6px;padding:7px 10px;border:1px solid #b8c3cc;border-radius:7px;background:#fff;color:#185fa5;font-weight:650;cursor:pointer;white-space:nowrap';
      button.addEventListener('click',function(){
        if(!to.value)to.value=day();from.value='';
        from.dispatchEvent(new Event('change',{bubbles:true}));to.dispatchEvent(new Event('change',{bubbles:true}));
        button.style.background='#185fa5';button.style.color='#fff';button.setAttribute('aria-pressed','true');
        refresh(to.closest('.filters,.toolbar,.report-filters,.date-range,form,.card,main')||document,button);
      });
      to.insertAdjacentElement('afterend',button);
      from.addEventListener('change',function(){if(from.value){button.style.background='#fff';button.style.color='#185fa5';button.setAttribute('aria-pressed','false');}});
    });
  }
  if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',enhance);else enhance();
  new MutationObserver(enhance).observe(document.documentElement,{childList:true,subtree:true});
})();

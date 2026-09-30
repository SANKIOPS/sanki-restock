(function(){
  'use strict';
  // Loaded only for the owner. No tenant messages or browser permission prompts.
  let busy=false;
  async function refresh(){
    if(busy||document.hidden)return;busy=true;
    try{
      const response=await fetch('/api/expenses/rentals/summary',{headers:{Accept:'application/json'}});
      if(!response.ok)return;
      const d=await response.json();if(!d.success)return;
      const link=document.querySelector('#sanki-shared-sidebar a[href="/rentals.html"]');
      if(link){let badge=link.querySelector('[data-rental-count]');if(!badge){badge=document.createElement('b');badge.dataset.rentalCount='1';badge.style.cssText='margin-left:auto;background:#b45309;color:white;border-radius:10px;padding:2px 6px;font-size:11px';link.appendChild(badge);}const count=d.followupCount+d.alertCount;badge.textContent=d.error?'!':count;badge.hidden=!count&&!d.error;link.title=d.error||d.followupCount+' rent follow-ups and '+d.alertCount+' lease / increase alerts';}
      if(location.pathname==='/dashboard.html'){
        let card=document.getElementById('rental-owner-digest');if(!card){card=document.createElement('a');card.id='rental-owner-digest';card.href='/rentals.html';card.className='tile';card.style.marginBottom='22px';document.querySelector('.head').after(card);}
        card.replaceChildren();const title=document.createElement('strong');title.textContent='Rent follow-ups · '+d.today;card.append(title);
        const detail=document.createElement('span');detail.textContent=d.error||d.followupCount+' tenant(s) to follow up · '+d.alertCount+' lease / increase alerts · '+d.summary.needsReview+' record(s) to review';card.append(detail);
        const totals=document.createElement('small');totals.textContent='Overdue: ₹'+d.summary.overdue.toLocaleString('en-IN')+' · This month outstanding: ₹'+d.summary.outstanding.toLocaleString('en-IN');card.append(totals);
      }
    }catch{}finally{busy=false;}
  }
  refresh();setInterval(refresh,60000);document.addEventListener('visibilitychange',refresh);window.addEventListener('rental-updated',refresh);
})();

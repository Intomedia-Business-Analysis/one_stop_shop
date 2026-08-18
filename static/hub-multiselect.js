/* ===========================================================================
 * HubMultiSelect — checkbox-dropdown med søgefelt, brugt i hub-filterbarer.
 *
 * Udtrukket fra marketing_deal_source.html, da benchmark-dashboardet skulle
 * bruge præcis samme widget. Adfærden er uændret: tom markering betyder "alle"
 * (triggeren viser allLabel), ét valg viser værdien, flere viser "N valgt".
 *
 * Markup: et tomt <div class="ms-wrap" id="…"></div> i filterbaren.
 * Stilene (.ms-wrap/.ms-trigger/.ms-panel/.ms-opt/…) ligger i den enkelte
 * sides <style>-blok — samme mønster som .dash-card og .v-table.
 *
 *   setupMultiSelect('ms-site', ['A','B'], 'Alle sites', onFilterChange);
 *   getSelected('ms-site');   // -> ['A']
 *   clearSelected('ms-site');
 *
 * onChange kaldes EFTER hvert klik i listen og efter Vælg alle / Ryd — men
 * ikke når panelet blot åbnes eller lukkes.
 * ======================================================================== */
'use strict';

// wrapId -> { options:[], selected:Set, allLabel, onChange }
const MS = {};

function msEscHtml(s){
  return String(s ?? '')
    .replace(/&/g, '&amp;').replace(/</g, '&lt;')
    .replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

function setupMultiSelect(wrapId, options, allLabel, onChange){
  const wrap = document.getElementById(wrapId);
  wrap.innerHTML = `
    <button type="button" class="ms-trigger" data-allabel="${msEscHtml(allLabel)}"></button>
    <div class="ms-panel">
      <input type="text" class="ms-search" placeholder="Søg…">
      <div class="ms-toolbar">
        <button type="button" data-act="all">Vælg alle</button>
        <button type="button" data-act="none">Ryd</button>
      </div>
      <div class="ms-list"></div>
    </div>`;
  MS[wrapId] = { options:[...options], selected:new Set(), allLabel, onChange };
  renderMS(wrapId);

  const trigger = wrap.querySelector('.ms-trigger');
  const panel   = wrap.querySelector('.ms-panel');
  const search  = wrap.querySelector('.ms-search');

  trigger.addEventListener('click', e => {
    e.stopPropagation();
    document.querySelectorAll('.ms-panel.open').forEach(p => { if(p !== panel) p.classList.remove('open'); });
    panel.classList.toggle('open');
    if(panel.classList.contains('open')){
      search.value = ''; filterMSList(wrapId, '');
      setTimeout(()=>search.focus(), 0);
    }
  });
  panel.addEventListener('click', e => e.stopPropagation());
  search.addEventListener('input', () => filterMSList(wrapId, search.value));
  // Vælg alle respekterer søgefeltet: er der filtreret, rammer den kun de synlige.
  wrap.querySelector('[data-act="all"]').addEventListener('click', () => {
    const filt = (search.value || '').toLowerCase();
    MS[wrapId].options.forEach(o => {
      if(!filt || o.toLowerCase().includes(filt)) MS[wrapId].selected.add(o);
    });
    renderMS(wrapId); MS[wrapId].onChange();
  });
  wrap.querySelector('[data-act="none"]').addEventListener('click', () => {
    MS[wrapId].selected.clear();
    renderMS(wrapId); MS[wrapId].onChange();
  });
}

function renderMS(wrapId){
  const state = MS[wrapId];
  const wrap = document.getElementById(wrapId);
  const trigger = wrap.querySelector('.ms-trigger');
  const list = wrap.querySelector('.ms-list');
  const sel = state.selected;
  if(sel.size === 0){
    trigger.textContent = state.allLabel;
    trigger.classList.remove('has-value');
  }else if(sel.size === 1){
    trigger.textContent = [...sel][0];
    trigger.classList.add('has-value');
  }else{
    trigger.textContent = `${sel.size} valgt`;
    trigger.classList.add('has-value');
  }

  if(!state.options.length){
    list.innerHTML = '<div class="ms-empty">Ingen muligheder</div>';
    return;
  }
  list.innerHTML = state.options.map(o => {
    const checked = sel.has(o);
    return `<label class="ms-opt${checked ? ' checked' : ''}" data-val="${msEscHtml(o)}">
      <input type="checkbox" ${checked ? 'checked' : ''}>
      <span>${msEscHtml(o)}</span>
    </label>`;
  }).join('');
  list.querySelectorAll('.ms-opt').forEach(el => {
    el.addEventListener('click', ev => {
      ev.preventDefault();
      const v = el.dataset.val;
      if(sel.has(v)) sel.delete(v); else sel.add(v);
      renderMS(wrapId); state.onChange();
    });
  });
}

function filterMSList(wrapId, q){
  q = (q || '').toLowerCase();
  const wrap = document.getElementById(wrapId);
  wrap.querySelectorAll('.ms-opt').forEach(el => {
    const v = el.dataset.val.toLowerCase();
    el.classList.toggle('hidden', !!q && !v.includes(q));
  });
}

function getSelected(wrapId){ return [...(MS[wrapId]?.selected || [])]; }
function clearSelected(wrapId){ MS[wrapId]?.selected.clear(); renderMS(wrapId); }

// Sæt markeringen programmatisk (fx når en gemt opsætning genskabes).
// Værdier der ikke findes i options ignoreres.
function setSelected(wrapId, values){
  const state = MS[wrapId];
  if(!state) return;
  state.selected.clear();
  (values || []).forEach(v => { if(state.options.includes(v)) state.selected.add(v); });
  renderMS(wrapId);
}

// Klik udenfor lukker alle paneler.
document.addEventListener('click', () => {
  document.querySelectorAll('.ms-panel.open').forEach(p => p.classList.remove('open'));
});

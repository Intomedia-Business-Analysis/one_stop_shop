/* Favorit-stjernen — deles af forsiden, /favorites, /recent og kategorisiderne.
 *
 * Klik på en .ri-star sender POST /api/favorites/<id>/toggle og opdaterer
 * knappen med serverens svar (så tilstanden altid afspejler DB). Fejler kaldet,
 * ruller knappen tilbage — så en stjerne aldrig lyver om at være gemt.
 */
(function () {
  function paint(btn, isFav) {
    btn.classList.toggle('on', isFav);
    btn.setAttribute('aria-pressed', isFav ? 'true' : 'false');
    btn.title = isFav ? 'Fjern fra favoritter' : 'Tilføj til favoritter';
  }

  document.addEventListener('click', async function (e) {
    const btn = e.target.closest('.ri-star');
    if (!btn) return;
    // Stjernen ligger inde i en rækkelink — undgå at klikket navigerer.
    e.preventDefault();
    e.stopPropagation();
    if (btn.dataset.busy) return;

    const wasFav = btn.classList.contains('on');
    btn.dataset.busy = '1';
    paint(btn, !wasFav);                     // optimistisk, rulles tilbage ved fejl
    try {
      const r = await fetch('/api/favorites/' + encodeURIComponent(btn.dataset.itemId) + '/toggle',
                            { method: 'POST', headers: { 'Accept': 'application/json' } });
      if (!r.ok) throw new Error(r.status);
      const d = await r.json();
      paint(btn, !!d.favorite);
      // På favoritsiden forsvinder rækken når stjernen slås fra — ellers står
      // en tom "favorit" tilbage, som først forsvinder ved næste sideload.
      if (!d.favorite && document.body.dataset.favoritesPage === '1') {
        const row = btn.closest('.ri');
        const list = row && row.parentElement;
        if (row) row.remove();
        if (list && !list.querySelector('.ri')) window.location.reload();
      }
    } catch (err) {
      paint(btn, wasFav);
    } finally {
      delete btn.dataset.busy;
    }
  });
})();

/* Fælles hjælpere til rotations-dashboards.
   Var før copy-pastet ind i hvert template — rettes noget her, slår det
   igennem på alle dashboards på én gang. */

/* HTML-escaping — al data der lægges i innerHTML skal igennem denne, så et
   organisationsnavn/deal-titel med HTML i (data kommer fra Pipedrive) ikke
   kan køre script i browseren. */
function escHtml(s) {
  return String(s).replace(/[&<>"']/g, c => (
    {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]
  ));
}

function short(n, len = 16) {
  return n && n.length > len ? n.substring(0, len - 1) + '…' : (n || '—');
}

/* Beløbsformattering. unit er fx 'kr.' eller 'NOK'.
   opts.zeroLabel: true → 0 vises som "0 kr."; false (default) → tom label. */
function makeMoneyFormatters(unit, opts = {}) {
  const fmt = v => v == null ? '—'
    : new Intl.NumberFormat('da-DK', {maximumFractionDigits: 0}).format(v) + ' ' + unit;
  const fmtAxis = v => {
    const a = Math.abs(v);
    if (a >= 1000000) return (v/1000000).toLocaleString('da-DK', {maximumFractionDigits: 0}) + 'mio. ' + unit;
    if (a >= 1000)    return (v/1000).toLocaleString('da-DK', {maximumFractionDigits: 0}) + 't. ' + unit;
    return v;
  };
  const fmtLabel = v => {
    if (v == null) return '';
    if (v === 0) return opts.zeroLabel ? '0 ' + unit : '';
    const a = Math.abs(v);
    if (a >= 1000000) return (v/1000000).toLocaleString('da-DK', {maximumFractionDigits: 3}) + 'mio. ' + unit;
    if (a >= 1000)    return (v/1000).toLocaleString('da-DK', {maximumFractionDigits: 0}) + 't. ' + unit;
    return fmt(v);
  };
  return { fmt, fmtAxis, fmtLabel };
}

/* Udfyld en tabel-tbody. cols: [{key, cls?, fmt?}]. Alle værdier escapes. */
function fillTable(id, rows, cols) {
  const tb = document.getElementById(id);
  if (!rows?.length) {
    tb.innerHTML = '<tr><td colspan="10" style="text-align:center;color:var(--ink-4);padding:6px;font-style:italic;font-size:10px">Ingen data</td></tr>';
    return;
  }
  tb.innerHTML = rows.map(r => '<tr>' + cols.map(c => {
    const v = r[c.key];
    const d = c.fmt ? c.fmt(v) : (v != null ? String(v) : '—');
    return `<td class="${c.cls || ''}" title="${escHtml(v != null ? String(v) : '')}">${escHtml(d)}</td>`;
  }).join('') + '</tr>').join('');
}

/* Fejlbanner: vises når data-endpointet fejler (500/netværk), så skærmen
   viser en forklaring i stedet for tomme charts. Dashboards genindlæser
   automatisk hvert 5. minut, så banneret forsvinder selv, når data er
   tilbage (hideDataError kaldes ved succes). */
function showDataError() {
  let el = document.getElementById('dataErrorBanner');
  if (!el) {
    el = document.createElement('div');
    el.id = 'dataErrorBanner';
    el.style.cssText =
      'position:fixed;top:16px;left:50%;transform:translateX(-50%);z-index:9999;' +
      'background:#b3261e;color:#fff;padding:10px 22px;border-radius:8px;' +
      'font:600 15px/1.4 system-ui,sans-serif;box-shadow:0 4px 14px rgba(0,0,0,.35)';
    el.textContent = 'Data utilgængelig — prøver igen om 5 min.';
    document.body.appendChild(el);
  }
  el.style.display = 'block';
}

function hideDataError() {
  const el = document.getElementById('dataErrorBanner');
  if (el) el.style.display = 'none';
}

/* Hent dashboard-data. Returnerer parsed JSON ved succes; ved HTTP-fejl eller
   netværksfejl vises fejlbanneret og der returneres null — kalderen skal blot
   gøre `if (!d) return;`. Ved succes skjules et evt. tidligere banner. */
async function fetchDashboardData(url) {
  try {
    const r = await fetch(url);
    if (!r.ok) throw new Error('HTTP ' + r.status);
    const d = await r.json();
    hideDataError();
    return d;
  } catch (e) {
    console.error('Datahentning fejlede:', url, e);
    showDataError();
    return null;
  }
}

/* Auto-skalér dashboardet til skærmen — design-lærred 1920×1080 (16:9).
   Gør visningen uafhængig af skærmens opløsning og Windows-skalering. */
function initStageScale(selector = '.page') {
  const stage = document.querySelector(selector);
  if (!stage) return;
  function fit() {
    const s = Math.min(window.innerWidth / 1920, window.innerHeight / 1080);
    const x = (window.innerWidth  - 1920 * s) / 2;
    const y = (window.innerHeight - 1080 * s) / 2;
    stage.style.transform = 'translate(' + x + 'px,' + y + 'px) scale(' + s + ')';
  }
  fit();
  window.addEventListener('resize', fit);
}

/* HH:MM-ur i headeren. */
function initClock(id = 'clock') {
  function tick() {
    const n = new Date();
    const e = document.getElementById(id);
    if (e) e.textContent = String(n.getHours()).padStart(2, '0') + ':' + String(n.getMinutes()).padStart(2, '0');
  }
  tick();
  setInterval(tick, 10000);
}

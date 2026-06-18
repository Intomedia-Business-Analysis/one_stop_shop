/* ===========================================================================
 * HubExport — genbrugelig client-side Excel-eksport for hub-widgets.
 *
 * Eksporterer de data en widget allerede har hentet til siden (respekterer
 * derfor automatisk aktive filtre). Bygger på xlsx-js-style (SheetJS-fork med
 * celle-styling) — samme bibliotek som barselsberegneren bruger. Biblioteket
 * lazy-loades først når der faktisk eksporteres, så det ikke koster noget på
 * dashboards hvor man ikke trykker eksport.
 *
 * Brug fra en widget:
 *
 *   HubExport.download({
 *     filename: 'afdelingsleder-maanedlig.xlsx',
 *     sheets: [{
 *       name:    'Månedlig netto',
 *       title:   'Månedlig netto — år for år',   // valgfri overskriftsrække
 *       columns: [
 *         { header: 'Måned',      key: 'maaned', width: 14, align: 'left'  },
 *         { header: 'Netto 2026', key: 'curr',   width: 16, align: 'right', numFmt: '#,##0' },
 *       ],
 *       rows: [ { maaned: 'Jan', curr: 12345 }, ... ],
 *     }],
 *   });
 *
 * Hver kolonne: { header, key, width?, align?, numFmt? }. En kolonne med numFmt
 * formateres som tal i Excel. `rows` kan være objekter (slås op via key) eller
 * allerede arrays i kolonnerækkefølge.
 * ======================================================================== */
(function (global) {
  'use strict';

  var LIB_URL = 'https://cdn.jsdelivr.net/npm/xlsx-js-style@1.2.0/dist/xlsx.bundle.js';

  // Temaets farver (matcher hub.css og dashboard-tabellerne).
  var HEADER_BG = '1C1C1A';   // --green (near-black)
  var TITLE_BG  = 'F0ECE6';   // --green-light
  var BORDER    = { style: 'thin', color: { rgb: 'E9E5DE' } };  // --border
  var ALL_BORDERS = { top: BORDER, bottom: BORDER, left: BORDER, right: BORDER };

  var _libPromise = null;

  // Lazy-load SheetJS én gang; returnér et promise der resolver når XLSX findes.
  function ensureLib() {
    if (global.XLSX) return Promise.resolve(global.XLSX);
    if (_libPromise) return _libPromise;
    _libPromise = new Promise(function (resolve, reject) {
      var s = document.createElement('script');
      s.src = LIB_URL;
      s.async = true;
      s.onload = function () {
        if (global.XLSX) resolve(global.XLSX);
        else reject(new Error('XLSX ikke tilgængelig efter indlæsning'));
      };
      s.onerror = function () { reject(new Error('Kunne ikke indlæse Excel-bibliotek')); };
      document.head.appendChild(s);
    });
    return _libPromise;
  }

  // Træk værdien for en kolonne ud af en datarække (objekt eller array).
  function cellValue(row, col, idx) {
    if (Array.isArray(row)) return row[idx];
    return row[col.key];
  }

  function buildSheet(XLSX, sheet) {
    var cols  = sheet.columns || [];
    var rows  = sheet.rows || [];
    var aoa   = [];
    var hasTitle = !!sheet.title;

    if (hasTitle) aoa.push([sheet.title]);
    aoa.push(cols.map(function (c) { return c.header; }));
    rows.forEach(function (r) {
      aoa.push(cols.map(function (c, i) {
        var v = cellValue(r, c, i);
        return (v === null || v === undefined) ? '' : v;
      }));
    });

    var ws  = XLSX.utils.aoa_to_sheet(aoa);
    var enc = XLSX.utils.encode_cell;
    var headerRow = hasTitle ? 1 : 0;
    var firstDataRow = headerRow + 1;

    // Kolonnebredder
    ws['!cols'] = cols.map(function (c) { return { wch: c.width || 16 }; });

    // Titelrække: flet hen over alle kolonner
    if (hasTitle) {
      ws['!merges'] = [{ s: { r: 0, c: 0 }, e: { r: 0, c: Math.max(cols.length - 1, 0) } }];
      var tcell = ws[enc({ r: 0, c: 0 })];
      if (tcell) tcell.s = {
        fill: { patternType: 'solid', fgColor: { rgb: TITLE_BG } },
        font: { bold: true, sz: 12, color: { rgb: '1A1A17' } },
        alignment: { horizontal: 'left', vertical: 'center' },
      };
    }

    // Kolonneoverskrifter
    cols.forEach(function (c, i) {
      var cell = ws[enc({ r: headerRow, c: i })];
      if (cell) cell.s = {
        fill: { patternType: 'solid', fgColor: { rgb: HEADER_BG } },
        font: { bold: true, sz: 10, color: { rgb: 'FFFFFF' } },
        alignment: {
          horizontal: c.align || 'left',
          vertical: 'center',
          wrapText: true,
        },
        border: ALL_BORDERS,
      };
    });

    // Dataceller
    for (var ri = 0; ri < rows.length; ri++) {
      cols.forEach(function (c, i) {
        var cell = ws[enc({ r: firstDataRow + ri, c: i })];
        if (!cell) return;
        cell.s = {
          font: { sz: 10, color: { rgb: '2B2B2B' } },
          alignment: { horizontal: c.align || 'left', vertical: 'center' },
          border: ALL_BORDERS,
        };
        if (c.numFmt && cell.t === 'n') cell.z = c.numFmt;
      });
    }

    // Lidt højere overskriftsrækker
    var rowH = [];
    if (hasTitle) rowH.push({ hpt: 22 });
    rowH.push({ hpt: 20 });
    ws['!rows'] = rowH;

    return ws;
  }

  // Excel-fanenavne: max 31 tegn, ingen af tegnene  : \ / ? * [ ]
  function safeSheetName(name, fallback) {
    var n = (name || fallback || 'Ark').replace(/[:\\/?*\[\]]/g, ' ').trim();
    return n.slice(0, 31) || fallback || 'Ark';
  }

  function download(opts) {
    opts = opts || {};
    var sheets = (opts.sheets || []).filter(function (s) { return s && (s.rows || []).length; });
    if (!sheets.length) { toast('Ingen data at eksportere'); return Promise.resolve(false); }

    return ensureLib().then(function (XLSX) {
      var wb = XLSX.utils.book_new();
      var used = {};
      sheets.forEach(function (sheet, idx) {
        var name = safeSheetName(sheet.name, 'Ark ' + (idx + 1));
        // Sikr unikke fanenavne
        var base = name, n = 2;
        while (used[name]) { name = (base.slice(0, 28) + ' ' + n).slice(0, 31); n++; }
        used[name] = true;
        XLSX.utils.book_append_sheet(wb, buildSheet(XLSX, sheet), name);
      });
      var filename = opts.filename || 'eksport.xlsx';
      if (!/\.xlsx$/i.test(filename)) filename += '.xlsx';
      XLSX.writeFile(wb, filename);
      toast('Excel hentet ✓');
      return true;
    }).catch(function (err) {
      console.error('[HubExport]', err);
      toast('Eksport mislykkedes — prøv igen');
      return false;
    });
  }

  // Letvægts-toast så enhver side får tilbagemelding uden egen toast-opsætning.
  function toast(msg) {
    var t = document.getElementById('hub-export-toast');
    if (!t) {
      t = document.createElement('div');
      t.id = 'hub-export-toast';
      t.style.cssText =
        'position:fixed;bottom:24px;left:50%;transform:translateX(-50%) translateY(8px);' +
        'background:#1C1C1A;color:#fff;font-family:inherit;font-size:13px;font-weight:600;' +
        'padding:10px 18px;border-radius:8px;box-shadow:0 6px 24px rgba(0,0,0,.22);' +
        'opacity:0;transition:opacity .2s,transform .2s;z-index:9999;pointer-events:none';
      document.body.appendChild(t);
    }
    t.textContent = msg;
    requestAnimationFrame(function () {
      t.style.opacity = '1';
      t.style.transform = 'translateX(-50%) translateY(0)';
    });
    clearTimeout(toast._t);
    toast._t = setTimeout(function () {
      t.style.opacity = '0';
      t.style.transform = 'translateX(-50%) translateY(8px)';
    }, 2200);
  }

  global.HubExport = { download: download, ensureLib: ensureLib, toast: toast };
})(window);

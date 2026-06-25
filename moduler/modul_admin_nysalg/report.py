"""Rapportgenerering: Excel (openpyxl) og PDF (reportlab).

Administrative nysalg kommer fra matchet (effective_is_admin, dvs. is_admin
justeret af direktørens override); administrative opsigelser hører til udtrækkets
`administrativ`-flag og indgår ikke her. Rapporten gemmes til report_dir og stien
returneres.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime

from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter

from moduler.modul_admin_nysalg.repo import effective_is_admin, is_admin_opsigelse

logger = logging.getLogger(__name__)

_DKK = "#,##0 \"kr.\""


def _num_fmt(cur: str | None) -> str:
    """Excel-talformat med valuta-suffiks ('kr.' for DKK, ellers fx 'NOK')."""
    return _DKK if (cur or "DKK") == "DKK" else f'#,##0 "{cur}"'


_HEAD_FILL = PatternFill("solid", fgColor="1C1C1A")  # hub-primær (nær-sort)
_HEAD_FONT = Font(bold=True, color="FFFFFF", size=10)
_TITLE_FONT = Font(bold=True, size=14)
_THIN = Side(style="thin", color="DDDDDD")
_BORDER = Border(left=_THIN, right=_THIN, top=_THIN, bottom=_THIN)


def report_dir() -> str:
    d = os.getenv("ADMIN_NYSALG_REPORT_DIR") or os.path.join("data", "admin_nysalg_reports")
    os.makedirs(d, exist_ok=True)
    return d


def _base_filename(run: dict) -> str:
    pf = (run.get("period_from") or "").strip()
    pt = (run.get("period_to") or "").strip()
    if pf or pt:
        period = f"{pf or 'start'}_{pt or 'slut'}"
    else:
        period = (run.get("period") or "alle").replace("/", "-")
    return f"monthly-performance-report-{run['run_id']}-{period}"


# ── Excel ────────────────────────────────────────────────────────────────────

def _style_header(ws, row, ncols):
    for c in range(1, ncols + 1):
        cell = ws.cell(row=row, column=c)
        cell.fill = _HEAD_FILL
        cell.font = _HEAD_FONT
        cell.alignment = Alignment(horizontal="left", vertical="center")
        cell.border = _BORDER


def _autosize(ws, widths):
    for i, w in enumerate(widths, start=1):
        ws.column_dimensions[get_column_letter(i)].width = w


def generate_excel(run: dict, matches: list[dict], summary: dict,
                   brand_rows: list[dict] | None = None, out_dir: str | None = None,
                   pd_deals: list[dict] | None = None,
                   org_names: dict | None = None,
                   months_breakdown: list[dict] | None = None) -> str:
    out_dir = out_dir or report_dir()
    org_names = org_names or {}
    wb = Workbook()

    # ── Ark 1: Summary ────────────────────────────────────────────────────────
    ws = wb.active
    ws.title = "Summary"
    ws["A1"] = "Monthly performance report"
    ws["A1"].font = _TITLE_FONT
    meta = [
        ("Periode", run.get("period") or "—"),
        ("Run-ID", run.get("run_id")),
        ("Kildefil", run.get("source_filename") or run.get("source_path") or "—"),
        ("Genereret", datetime.now().strftime("%Y-%m-%d %H:%M")),
        ("Godkendt af", run.get("approved_by") or "—"),
        ("Godkendt", run["approved_at"].strftime("%Y-%m-%d %H:%M")
            if run.get("approved_at") else "—"),
    ]
    r = 3
    for label, val in meta:
        ws.cell(row=r, column=1, value=label).font = Font(bold=True)
        ws.cell(row=r, column=2, value=val)
        r += 1

    totals = [
        ("Bruttonysalg (total)", summary["brutto"], False),
        ("  − heraf administrativt nysalg", -summary["adm_nysalg"], False),
        ("Opsigelser (total)", -summary["opsigelser"], False),
        ("  + heraf administrative opsigelser", summary["adm_opsigelser"], False),
        ("Netto tilvækst", summary["netto_tilvaekst"], True),
    ]
    for label, val, bold in totals:
        r += 1
        c1 = ws.cell(row=r, column=1, value=label)
        c2 = ws.cell(row=r, column=2, value=val)
        c2.number_format = _DKK
        if bold:
            c1.font = Font(bold=True)
            c2.font = Font(bold=True)

    # Pr. brand: totaler, administrative fradrag, netto tilvækst, budget, kommentar
    r += 2
    head_row = r
    bcols = ["Brand", "Bruttonysalg", "− heraf adm. nysalg", "Opsigelser",
             "− heraf adm. opsigelser", "Netto tilvækst", "Budget", "Kommentar"]
    for i, h in enumerate(bcols, start=1):
        ws.cell(row=head_row, column=i, value=h)
    _style_header(ws, head_row, len(bcols))
    r += 1
    for b in (brand_rows or []):
        cf = _num_fmt(b.get("currency"))   # salgstal i lokal valuta (NO/SE)
        ws.cell(row=r, column=1, value=b["brand"])
        ws.cell(row=r, column=2, value=b["brutto"]).number_format = cf
        ws.cell(row=r, column=3, value=b["adm_nysalg"]).number_format = cf
        ws.cell(row=r, column=4, value=b["opsigelser"]).number_format = cf
        ws.cell(row=r, column=5, value=b["adm_opsigelser"]).number_format = cf
        ws.cell(row=r, column=6, value=b["netto"]).number_format = cf
        ws.cell(row=r, column=7, value=b["budget"]).number_format = _DKK   # budget altid DKK
        ws.cell(row=r, column=8, value=b.get("comment") or "")
        r += 1
    _autosize(ws, [20, 16, 18, 16, 20, 16, 16, 45])

    # Samlede kommentar
    r += 1
    ws.cell(row=r, column=1, value="Samlede kommentar").font = Font(bold=True)
    ws.cell(row=r + 1, column=1, value=run.get("director_comment") or "—")

    # ── Ark 2: Administrative nysalg (det der trækkes fra) ─────────────────────
    ws2 = wb.create_sheet("Administrative nysalg")
    cols = ["Række", "Måned", "Site", "Kunde", "Org-ID", "Konto", "Movement",
            "Gross in", "Matchet deal", "Deal-værdi", "Pipeline", "Override", "Kommentar"]
    ws2.append(cols)
    _style_header(ws2, 1, len(cols))
    for m in matches:
        if not effective_is_admin(m):
            continue
        ws2.append([
            m.get("row_index"), m.get("month_end"), m.get("site"), m.get("matched_org_name"),
            m.get("pipedrive_id"), m.get("account_number"), m.get("movement"), m.get("gross_in"),
            m.get("matched_deal_id"), m.get("matched_value"), m.get("matched_pipeline"),
            m.get("override") or "", m.get("row_comment") or "",
        ])
    for row_cells in ws2.iter_rows(min_row=2, min_col=8, max_col=8):
        for c in row_cells:
            c.number_format = _DKK
    for row_cells in ws2.iter_rows(min_row=2, min_col=10, max_col=10):
        for c in row_cells:
            c.number_format = _DKK
    _autosize(ws2, [7, 11, 28, 28, 9, 14, 12, 14, 12, 14, 16, 10, 40])

    # ── Ark 3: Bevægelser pr. brand (Zuora) ───────────────────────────────────
    # Alle rækker med gross in eller gross out i perioden — kilden bag brand-
    # totalerne, så de kan holdes op imod PipeDrive-deals (Ark 4).
    ws4 = wb.create_sheet("Bevægelser pr. brand")
    mcols = ["Brand", "Site", "Kunde", "Org-ID", "Konto", "Måned", "Movement",
             "Valuta", "Gross in", "Gross out", "Netto", "Administrativ", "Matchet deal"]
    ws4.append(mcols)
    _style_header(ws4, 1, len(mcols))
    moves = sorted(
        [m for m in matches if (m.get("gross_in") or 0) or (m.get("gross_out") or 0)],
        key=lambda m: ((m.get("brand") or ""), (m.get("site") or "")))
    from moduler.modul_admin_nysalg.brands import brand_account
    for m in moves:
        gi = m.get("gross_in") or 0
        go = m.get("gross_out") or 0
        # Navn fra matchet deal, ellers slå op på org-id i brandets KONTO (org-id
        # er ikke unikke på tværs af konti) — også for ikke-admin rækker.
        acct = brand_account(m.get("brand") or "")
        pid = str(m.get("pipedrive_id") or "").strip()
        kunde = (m.get("matched_org_name")
                 or (org_names.get(acct, {}).get(pid, "") if acct else ""))
        # Administrativ = matchede en administrativ deal (nysalg ELLER opsigelse)
        # eller bærer Zuoras administrativ-flag.
        adm = "Ja" if (effective_is_admin(m) or is_admin_opsigelse(m)) else ""
        ws4.append([
            m.get("brand") or "", m.get("site"), kunde,
            m.get("pipedrive_id"), m.get("account_number"), m.get("month_end"),
            m.get("movement"), m.get("currency") or "", gi, go, gi - go,
            adm, m.get("matched_deal_id") or "",
        ])
    for col in (9, 10, 11):
        for cells in ws4.iter_rows(min_row=2, min_col=col, max_col=col):
            for c in cells:
                c.number_format = "#,##0"
    _autosize(ws4, [16, 26, 26, 9, 14, 11, 12, 8, 14, 14, 14, 11, 12])
    ws4.auto_filter.ref = ws4.dimensions   # filtrerbar Zuora-tabel

    # ── Ark 4: PipeDrive-deals (måned) ────────────────────────────────────────
    # Alle won-deals med service_activation_date i perioden — PipeDrive-kilden,
    # til afstemning mod Zuora-bevægelserne (Ark 3).
    ws5 = wb.create_sheet("PipeDrive-deals")
    pcols = ["Brand", "Site", "Kunde", "Org-ID", "Account", "Team", "Pipeline",
             "Status", "Administrativ", "Service-akt.dato", "Valuta", "Værdi"]
    ws5.append(pcols)
    _style_header(ws5, 1, len(pcols))
    for d in (pd_deals or []):
        ws5.append([
            d.get("brand") or "", d.get("site"), d.get("org_name"), d.get("org_id"),
            d.get("account"), d.get("team"), d.get("pipeline"), d.get("status"),
            "Ja" if d.get("administrativ") else "", d.get("service_activation_date"),
            d.get("currency") or "", d.get("value") or 0,
        ])
    for cells in ws5.iter_rows(min_row=2, min_col=12, max_col=12):
        for c in cells:
            c.number_format = "#,##0"
    _autosize(ws5, [16, 26, 26, 9, 18, 22, 16, 8, 12, 16, 8, 14])
    ws5.auto_filter.ref = ws5.dimensions   # filtrerbar PipeDrive-tabel

    # ── Ark: Performance pr. måned ────────────────────────────────────────────
    # Én række pr. (måned, brand) — så perioden kan brydes ned på de enkelte
    # måneder. Summen pr. brand over alle måneder matcher "Summary"-arket.
    if months_breakdown:
        ws6 = wb.create_sheet("Pr. måned")
        mcols2 = ["Måned", "Brand", "Bruttonysalg", "− adm. nysalg", "Opsigelser",
                  "− adm. opsigelser", "Netto tilvækst", "Budget"]
        ws6.append(mcols2)
        _style_header(ws6, 1, len(mcols2))
        for blk in months_breakdown:
            for b in blk.get("rows", []):
                cf = _num_fmt(b.get("currency"))   # salgstal i lokal valuta (NO/SE)
                ws6.append([blk["label"], b["brand"], b["brutto"], b["adm_nysalg"],
                            b["opsigelser"], b["adm_opsigelser"], b["netto"], b["budget"]])
                row = ws6.max_row
                for col in (3, 4, 5, 6, 7):
                    ws6.cell(row=row, column=col).number_format = cf
                ws6.cell(row=row, column=8).number_format = _DKK   # budget altid DKK
        ws6.auto_filter.ref = ws6.dimensions
        _autosize(ws6, [16, 20, 16, 16, 16, 18, 16, 16])

    path = os.path.join(out_dir, _base_filename(run) + ".xlsx")
    wb.save(path)
    return path


# ── PDF ──────────────────────────────────────────────────────────────────────

def generate_pdf(run: dict, matches: list[dict], summary: dict,
                 brand_rows: list[dict] | None = None, out_dir: str | None = None,
                 months_breakdown: list[dict] | None = None) -> str:
    out_dir = out_dir or report_dir()
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles import ParagraphStyle
        from reportlab.lib.units import mm
        from reportlab.platypus import (Paragraph, SimpleDocTemplate, Spacer, Table,
                                        TableStyle)
    except ImportError as e:
        raise RuntimeError("reportlab er ikke installeret — PDF kan ikke genereres.") from e

    # ── Palet ──────────────────────────────────────────────────────────────────
    # Hub-palet (static/hub.css): JP|Politiken nær-sort + varm beige.
    DARK    = colors.HexColor("#1C1C1A")   # --green (primær brand, nær-sort)
    INK     = colors.HexColor("#1A1A17")   # --ink
    MUTED   = colors.HexColor("#8B857A")   # --ink-3
    FAINT   = colors.HexColor("#BEB7AC")   # --ink-4 (banner-subtitle på mørk)
    LIGHT   = colors.HexColor("#F7F5F2")   # --bg (alternerende rækker)
    BORDER  = colors.HexColor("#E9E5DE")   # --border
    WIN     = colors.HexColor("#1F8A5B")   # --win (positiv netto)
    RED     = colors.HexColor("#B32E45")   # --danger (negativ)
    TAUPE   = colors.HexColor("#ACA199")   # --taupe (JP|Politiken signatur-bar)
    WHITE   = colors.white

    def kr(v):
        return f"{round(v or 0):,.0f} kr.".replace(",", ".")

    def money(v, cur="DKK"):
        n = f"{round(v or 0):,.0f}".replace(",", ".")
        return f"{n} kr." if (cur or "DKK") == "DKK" else f"{n} {cur}"

    # ── Stilarter ────────────────────────────────────────────────────────────
    s_title   = ParagraphStyle("t",  fontName="Helvetica-Bold", fontSize=21, textColor=WHITE, leading=23)
    s_sub     = ParagraphStyle("s",  fontName="Helvetica",      fontSize=9,  textColor=FAINT, leading=13)
    s_logo    = ParagraphStyle("lg", fontName="Helvetica-Bold", fontSize=12, textColor=WHITE, leading=13)
    s_logosub = ParagraphStyle("ls", fontName="Helvetica-Bold", fontSize=7,  textColor=FAINT, leading=9)
    s_kpi_lbl = ParagraphStyle("kl", fontName="Helvetica-Bold", fontSize=7,  textColor=MUTED, leading=10, spaceAfter=3)
    s_kpi_val = ParagraphStyle("kv", fontName="Helvetica-Bold", fontSize=15, textColor=INK,   leading=17)
    s_kpi_sub = ParagraphStyle("ks", fontName="Helvetica",      fontSize=6.5, textColor=MUTED, leading=9, spaceBefore=2)
    s_h       = ParagraphStyle("h",  fontName="Helvetica-Bold", fontSize=11, textColor=INK,   leading=14, spaceBefore=2, spaceAfter=6)
    s_body    = ParagraphStyle("b",  fontName="Helvetica",      fontSize=9,  textColor=INK,   leading=13)
    s_cell    = ParagraphStyle("c",  fontName="Helvetica",      fontSize=8,  textColor=INK,   leading=10)
    s_cellb   = ParagraphStyle("cb", fontName="Helvetica-Bold", fontSize=8,  textColor=INK,   leading=10)
    s_hd      = ParagraphStyle("hd", fontName="Helvetica-Bold", fontSize=7,  textColor=WHITE, leading=9)

    CW = 178 * mm  # indholdsbredde (A4 − margener)

    def _footer(canvas, doc_):
        canvas.saveState()
        canvas.setFont("Helvetica", 7)
        canvas.setFillColor(MUTED)
        canvas.drawString(16 * mm, 10 * mm,
                          f"Intomedia · Monthly performance report · genereret {datetime.now():%Y-%m-%d %H:%M}")
        canvas.drawRightString(194 * mm, 10 * mm, f"Side {doc_.page}")
        canvas.setStrokeColor(BORDER)
        canvas.setLineWidth(0.5)
        canvas.line(16 * mm, 13 * mm, 194 * mm, 13 * mm)
        canvas.restoreState()

    path = os.path.join(out_dir, _base_filename(run) + ".pdf")
    doc = SimpleDocTemplate(path, pagesize=A4, leftMargin=16 * mm, rightMargin=16 * mm,
                            topMargin=15 * mm, bottomMargin=18 * mm,
                            title="Monthly performance report")
    el = []

    # ── Header-banner med JP|Politiken-logo ─────────────────────────────────────
    meta = (f"Periode {run.get('period') or 'alle'}  ·  Run #{run.get('run_id')}  ·  "
            f"Godkendt af {run.get('approved_by') or '—'}")
    # Logoet er hubbens typografiske wordmark: taupe signatur-bar + JP|Politiken.
    logo_bar = Table([[""]], colWidths=[20 * mm], rowHeights=[1.8 * mm])
    logo_bar.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), TAUPE),
        ("LEFTPADDING", (0, 0), (-1, -1), 0), ("RIGHTPADDING", (0, 0), (-1, -1), 0),
        ("TOPPADDING", (0, 0), (-1, -1), 0), ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
    ]))
    banner = Table([[[
        logo_bar,
        Spacer(1, 4),
        Paragraph("JP|Politiken", s_logo),
        Paragraph("BUSINESS MEDIA", s_logosub),
        Spacer(1, 11),
        Paragraph("Monthly performance report", s_title),
        Paragraph(meta, s_sub),
    ]]], colWidths=[CW])
    banner.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, -1), DARK),
        ("LINEBELOW", (0, 0), (-1, -1), 3, TAUPE),
        ("LEFTPADDING", (0, 0), (-1, -1), 20), ("RIGHTPADDING", (0, 0), (-1, -1), 20),
        ("TOPPADDING", (0, 0), (-1, -1), 18), ("BOTTOMPADDING", (0, 0), (-1, -1), 18),
    ]))
    el.append(banner)
    el.append(Spacer(1, 9 * mm))

    # ── KPI-kort ─────────────────────────────────────────────────────────────
    def kpi_cell(label, value, sub, color):
        return [Paragraph(label, s_kpi_lbl),
                Paragraph(value, ParagraphStyle("kvx", parent=s_kpi_val, textColor=color)),
                Paragraph(sub, s_kpi_sub)]
    kpis = Table([[
        kpi_cell("BRUTTONYSALG (TOTAL)", kr(summary["brutto"]),
                 f"heraf adm.: {kr(summary['adm_nysalg'])}", INK),
        kpi_cell("OPSIGELSER (TOTAL)", kr(summary["opsigelser"]),
                 f"heraf adm.: {kr(summary['adm_opsigelser'])}", RED),
        kpi_cell("NETTO TILVÆKST", kr(summary["netto_tilvaekst"]),
                 "(brutto − adm.) − (opsig. − adm.)", WIN),
    ]], colWidths=[CW / 3.0] * 3)
    kpis.setStyle(TableStyle([
        ("BOX", (0, 0), (0, 0), 0.75, BORDER), ("BOX", (1, 0), (1, 0), 0.75, BORDER),
        ("BOX", (2, 0), (2, 0), 0.75, BORDER),
        ("LINEABOVE", (0, 0), (-1, 0), 2.5, DARK),
        ("BACKGROUND", (0, 0), (-1, -1), WHITE),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 12), ("RIGHTPADDING", (0, 0), (-1, -1), 12),
        ("TOPPADDING", (0, 0), (-1, -1), 12), ("BOTTOMPADDING", (0, 0), (-1, -1), 12),
    ]))
    el.append(kpis)
    el.append(Spacer(1, 9 * mm))

    # ── Performance pr. brand ──────────────────────────────────────────────────
    el.append(Paragraph("Performance pr. brand", s_h))
    head = ["Brand", "Bruttonysalg", "− adm. nysalg", "Opsigelser",
            "− adm. opsig.", "Netto tilvækst", "Budget"]
    brand_data = [[Paragraph(h, s_hd) for h in head]]
    netto_signs = []
    for b in (brand_rows or []):
        netto_signs.append(b["netto"] >= 0)
        c = b.get("currency") or "DKK"   # salgstal i lokal valuta (NO/SE), budget i DKK
        brand_data.append([
            Paragraph(b["brand"], s_cellb), money(b["brutto"], c),
            money(b["adm_nysalg"], c), money(b["opsigelser"], c), money(b["adm_opsigelser"], c),
            money(b["netto"], c), kr(b["budget"]),
        ])
    if len(brand_data) > 1:
        t = Table(brand_data, colWidths=[30 * mm, 26 * mm, 23 * mm, 23 * mm,
                                         23 * mm, 26 * mm, 27 * mm], repeatRows=1)
        style = [
            ("BACKGROUND", (0, 0), (-1, 0), DARK),
            ("TOPPADDING", (0, 0), (-1, 0), 7), ("BOTTOMPADDING", (0, 0), (-1, 0), 7),
            ("FONTNAME", (1, 1), (-1, -1), "Helvetica"),
            ("FONTSIZE", (1, 1), (-1, -1), 8),
            ("TEXTCOLOR", (1, 1), (-1, -1), INK),
            ("ALIGN", (1, 0), (-1, -1), "RIGHT"),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [WHITE, LIGHT]),
            ("LINEBELOW", (0, 1), (-1, -1), 0.4, BORDER),
            ("LEFTPADDING", (0, 0), (-1, -1), 7), ("RIGHTPADDING", (0, 0), (-1, -1), 7),
            ("TOPPADDING", (0, 1), (-1, -1), 6), ("BOTTOMPADDING", (0, 1), (-1, -1), 6),
            # muted adm.-kolonner
            ("TEXTCOLOR", (2, 1), (2, -1), MUTED), ("TEXTCOLOR", (4, 1), (4, -1), MUTED),
        ]
        for i, pos in enumerate(netto_signs, start=1):
            style.append(("TEXTCOLOR", (5, i), (5, i), WIN if pos else RED))
            style.append(("FONTNAME", (5, i), (5, i), "Helvetica-Bold"))
            style.append(("TEXTCOLOR", (6, i), (6, i), MUTED))
        t.setStyle(TableStyle(style))
        el.append(t)
        el.append(Spacer(1, 8 * mm))

    # ── Netto tilvækst pr. måned ───────────────────────────────────────────────
    # Bryder perioden ned på de enkelte måneder. Beløbene summeres på tværs af
    # brands (samme forenkling som topkortene); NO/SE-detaljer ses i Excel.
    if months_breakdown and len(months_breakdown) > 1:
        el.append(Paragraph("Netto tilvækst pr. måned", s_h))
        mhead = ["Måned", "Bruttonysalg", "Opsigelser", "Netto tilvækst"]
        mdata = [[Paragraph(h, s_hd) for h in mhead]]
        msigns = []
        for blk in months_breakdown:
            rows = blk.get("rows", [])
            brutto = sum((b["brutto"] - b["adm_nysalg"]) for b in rows)
            ops = sum((b["opsigelser"] - b["adm_opsigelser"]) for b in rows)
            netto = sum(b["netto"] for b in rows)
            msigns.append(netto >= 0)
            mdata.append([Paragraph(blk["label"], s_cellb), kr(brutto), kr(ops), kr(netto)])
        mt = Table(mdata, colWidths=[44 * mm, 45 * mm, 45 * mm, 44 * mm], repeatRows=1)
        mstyle = [
            ("BACKGROUND", (0, 0), (-1, 0), DARK),
            ("TOPPADDING", (0, 0), (-1, 0), 7), ("BOTTOMPADDING", (0, 0), (-1, 0), 7),
            ("FONTNAME", (1, 1), (-1, -1), "Helvetica"), ("FONTSIZE", (1, 1), (-1, -1), 8),
            ("TEXTCOLOR", (1, 1), (-1, -1), INK), ("ALIGN", (1, 0), (-1, -1), "RIGHT"),
            ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [WHITE, LIGHT]),
            ("LINEBELOW", (0, 1), (-1, -1), 0.4, BORDER),
            ("LEFTPADDING", (0, 0), (-1, -1), 7), ("RIGHTPADDING", (0, 0), (-1, -1), 7),
            ("TOPPADDING", (0, 1), (-1, -1), 6), ("BOTTOMPADDING", (0, 1), (-1, -1), 6),
        ]
        for i, pos in enumerate(msigns, start=1):
            mstyle.append(("TEXTCOLOR", (3, i), (3, i), WIN if pos else RED))
            mstyle.append(("FONTNAME", (3, i), (3, i), "Helvetica-Bold"))
        mt.setStyle(TableStyle(mstyle))
        el.append(mt)
        el.append(Spacer(1, 8 * mm))

    # ── Kommentarer pr. brand ──────────────────────────────────────────────────
    brand_comments = [b for b in (brand_rows or []) if (b.get("comment") or "").strip()]
    if brand_comments:
        el.append(Paragraph("Kommentarer pr. brand", s_h))
        for b in brand_comments:
            el.append(Paragraph(f"<b>{b['brand']}:</b> {b['comment'].replace(chr(10), '<br/>')}", s_body))
            el.append(Spacer(1, 2 * mm))
        el.append(Spacer(1, 6 * mm))

    # ── Samlede kommentar ──────────────────────────────────────────────────────
    if run.get("director_comment"):
        el.append(Paragraph("Samlede kommentar", s_h))
        box = Table([[Paragraph(run["director_comment"].replace("\n", "<br/>"), s_body)]],
                    colWidths=[CW])
        box.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, -1), LIGHT),
            ("LINEABOVE", (0, 0), (-1, -1), 2, DARK),
            ("LEFTPADDING", (0, 0), (-1, -1), 12), ("RIGHTPADDING", (0, 0), (-1, -1), 12),
            ("TOPPADDING", (0, 0), (-1, -1), 10), ("BOTTOMPADDING", (0, 0), (-1, -1), 10),
        ]))
        el.append(box)

    doc.build(el, onFirstPage=_footer, onLaterPages=_footer)
    return path


def generate_report(run: dict, matches: list[dict], summary: dict,
                    brand_rows: list[dict] | None = None, fmt: str = "xlsx",
                    out_dir: str | None = None, pd_deals: list[dict] | None = None,
                    org_names: dict | None = None,
                    months_breakdown: list[dict] | None = None) -> str:
    """fmt: 'xlsx' | 'pdf'. Returnerer stien til den genererede fil."""
    if fmt == "pdf":
        return generate_pdf(run, matches, summary, brand_rows, out_dir,
                            months_breakdown=months_breakdown)
    return generate_excel(run, matches, summary, brand_rows, out_dir, pd_deals, org_names,
                          months_breakdown=months_breakdown)

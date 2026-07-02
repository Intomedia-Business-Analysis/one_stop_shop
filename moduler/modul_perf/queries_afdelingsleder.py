from datetime import date, timedelta

from constants import CANCELLATION_PIPELINES
from db import get_conn
from moduler.modul_perf.queries import _ADM_EXCLUDE
import calendar

# Pladsholder for opsigelses-pipelines — gen-afledt lokalt (jf. queries.py linje 34).
_CANCEL_PH = "(" + ",".join(["%s"] * len(CANCELLATION_PIPELINES)) + ")"

# Belob i lokal valuta (NO/SE/DE i lokal, ellers DKK) — delt af modulets queries.
# EUR medregnes i lokal valuta, fordi Watch DE-budgettet (BudgetsIntoMedia) er
# indlaest i EUR — ellers maales DKK-salg mod EUR-budget (fx 784 % af budget).
_VAL = ("CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK','EUR') "
        "THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2))")

def db_brand_overblik(today: date, date_col: str = "won_time", ytd: bool = True,
                      years: list[int] | None = None):
    """Salg, opsigelser og netto pr. brand-gruppe (alle brands) for flere år.

    ÅTD: hvert år afgrænses 1/1 → samme dag/måned som i dag, så årene
    sammenlignes på lige fod. ytd=False giver hele kalenderår.
    Deal-afgrænsningen er identisk med YoY-værktøjet (db_yoy_data):
    status='won', ekskl. Web Sale og administrative deals, NO/SE i lokal
    valuta, opsigelser = CANCELLATION_PIPELINES.
    """
    _VALID_DATE_COLS = {"won_time", "service_activation_date"}
    if date_col not in _VALID_DATE_COLS:
        date_col = "won_time"
    d_col = f"[{date_col}]"
    years = years or [today.year - 2, today.year - 1, today.year]

    def _cut(y: int) -> date:
        if not ytd:
            return date(y + 1, 1, 1)
        try:
            return today.replace(year=y) + timedelta(days=1)
        except ValueError:  # 29/2 i skudår
            return date(y, today.month, 28) + timedelta(days=1)

    range_sql = " OR ".join([f"({d_col} >= %s AND {d_col} < %s)"] * len(years))
    range_params: list = []
    for y in years:
        range_params += [date(y, 1, 1).isoformat(), _cut(y).isoformat()]

    # Scope pr. kort: team-baseret gruppering. Hver deal hører til præcis ét
    # kort via dens [team], så watch_medier-accounten splittes automatisk i
    # Watch/Finans/Monitor/Banner, og jppol_advertising i Banner/Job — uden at
    # blande sites- og team-paradigmer. Watch Medier og Finans samler hver
    # DK+Int i ét kort. Undtagelse: Watch DE har team=NULL og matches derfor på
    # [account]='watch_de'. Rækkefølge: efter deal-volumen (juster efter behov).
    group_defs = [
        ("watch_medier",   "Watch Medier",   "[team] IN ('Team Watch DK','Team Watch Int')",   ()),
        ("monitor",        "Monitor",        "[team]='Team Monitor'",                          ()),
        ("finans",         "Finans",         "[team] IN ('Team FINANS DK','Team FINANS Int')", ()),
        ("watch_no",       "Watch NO",       "[team]='Team Watch NO'",                         ()),
        ("job",            "Job",            "[team]='Team Job'",                              ()),
        ("advertising_no", "Advertising NO", "[team]='Team Watch NO Advertising'",             ()),
        ("banner",         "Banner",         "[team]='Team Banner'",                           ()),
        ("watch_de",       "Watch DE",       "[account]='watch_de'",                           ()),
        ("marketwire",     "MarketWire",     "[team]='Team Marketwire'",                       ()),
        ("watch_se",       "Watch SE",       "[team]='Team Watch SE'",                         ()),
    ]

    conn = get_conn()
    cur  = conn.cursor(as_dict=True)
    groups = []
    for key, label, scope_sql, scope_params in group_defs:
        cur.execute(f"""
            SELECT
                YEAR({d_col}) AS aar,
                ISNULL(SUM(CASE WHEN [pipeline_name] NOT IN {_CANCEL_PH}
                    THEN {_VAL} ELSE 0 END), 0) AS won,
                ISNULL(ABS(SUM(CASE WHEN [pipeline_name] IN {_CANCEL_PH}
                    THEN {_VAL} ELSE 0 END)), 0) AS ops,
                COUNT(CASE WHEN [pipeline_name] NOT IN {_CANCEL_PH} THEN 1 END) AS won_count
            FROM [dbo].[PipedriveDeals]
            WHERE [status]='won' AND [pipeline_name]<>'Web Sale'
              AND ({range_sql})
              AND ({scope_sql})
              {_ADM_EXCLUDE}
            GROUP BY YEAR({d_col})
        """, tuple(CANCELLATION_PIPELINES) * 3 + tuple(range_params) + scope_params)
        by_year = {r["aar"]: r for r in cur.fetchall()}
        rows = []
        for y in years:
            r = by_year.get(y, {})
            won = float(r.get("won") or 0)
            ops = float(r.get("ops") or 0)
            rows.append({
                "aar":       y,
                "won":       round(won, 2),
                "ops":       round(ops, 2),
                "netto":     round(won - ops, 2),
                "won_count": r.get("won_count") or 0,
            })
        groups.append({"key": key, "label": label, "rows": rows})
    conn.close()

    return {
        "years":  years,
        "ytd":    ytd,
        "cutoff": today.isoformat(),
        "groups": groups,
    }

# ── Overblik + måneds-deepdive: fælles gruppering ────────────────────────────
# Deals grupperes team-baseret (som db_brand_overblik), MEN Web Sale-pipelinen
# skilles ud i sin egen række PR. LAND (via account) i stedet for at blive smidt
# væk. Web Sale-deals ejes af 'System Admin' (automatiseret websalg), så de
# kræver en lempet adm-eksklusion (_ADM_EXCL_WEB) — ellers overlever kun Watch
# DE's websalg (Christian Linde). Budgettet (BudgetsIntoMedia) mappes pr.
# (Brand, DealType, Salestype) til brand-grupperne:
#   • Web Sale-rækkerne har INTET eget budget — brandenes budgetter er allerede
#     inkl. websalg (websale-budgetrækker følger brandet, fx FINANS DK).
#   • Programmatisk salg (ProgrammaticSales — ligger uden for PipedriveDeals)
#     lægges til Banner-gruppen med andelen udstillet særskilt ('prog'), og det
#     programmatiske budget tæller derfor også med i Banner-budgettet.
#   • Abonnementsbudgetterne er NETTO-budgetter (samme tolkning som måneds-
#     rapporten: budget ↔ salg − opsigelser). Derfor måles pct af budget på
#     NETTO, ikke bruttosalg. Annonce-grupper har ingen opsigelser → netto=salg.
# Alt placeres på service_activation_date (SAD) — budgettet er sat pr.
# aktiveringsmåned, så salg mod budget skal ligge på samme dato-grundlag (deals
# uden SAD indgår ikke, som i media-performance/månedsrapporten).
# NO/SE/DE-tal er i lokal valuta (som resten af modulet) — budgetterne er indlæst
# i samme valuta, så pct pr. gruppe er meningsfuld; totalen blander valutaer.
OVERBLIK_GROUPS = [
    ("watch_medier",   "Watch Medier"),
    ("finans",         "Finans"),
    ("monitor",        "Monitor"),
    ("watch_no",       "Watch NO"),
    ("job",            "Job"),
    ("banner",         "Banner"),
    ("advertising_no", "Advertising NO"),
    ("watch_se",       "Watch SE"),
    ("watch_de",       "Watch DE"),
    ("marketwire",     "MarketWire"),
    ("web_dk",         "Web Sale DK"),
    ("web_no",         "Web Sale NO"),
    ("web_se",         "Web Sale SE"),
    ("web_de",         "Web Sale DE"),
    ("ovrige",         "Øvrige"),
]

# Web Sale-deals fordeles pr. land via account (watch_medier/monitor → DK).
_WEB_ACCOUNT_TO_GROUP = {"watch_no": "web_no", "watch_se": "web_se", "watch_de": "web_de"}

# Som modul_perf._ADM_EXCLUDE, men 'System Admin'-ejerskab tillades for Web
# Sale-pipelinen (automatiseret websalg har ingen sælger) — jf. modul_rotations
# _ADM_EXCLUDE_ALLOW_WEBSALE.
_ADM_EXCL_WEB = (
    "AND (COALESCE([administrativ],'') <> 'ja') "
    "AND UPPER(LTRIM([title])) NOT LIKE 'ADMINISTRATIV%' "
    "AND UPPER(LTRIM([title])) NOT LIKE 'ADM %' "
    "AND COALESCE([deal_type],'') <> 'Rapport' "
    "AND (COALESCE([owner_name],'') <> 'System Admin' OR [pipeline_name]='Web Sale')"
)

_TEAM_TO_GROUP = {
    "Team Watch DK":             "watch_medier",
    "Team Watch Int":            "watch_medier",
    "Team FINANS DK":            "finans",
    "Team FINANS Int":           "finans",
    "Team Monitor":              "monitor",
    "Team Watch NO":             "watch_no",
    "Team Job":                  "job",
    "Team Banner":               "banner",
    "Team Watch NO Advertising": "advertising_no",
    "Team Watch SE":             "watch_se",
    "Team Marketwire":           "marketwire",
}

# BudgetsIntoMedia.[Brand] (lowercased) → gruppe, for Subscription-budgetrækker.
_SUB_BUDGET_TO_GROUP = {
    "watch dk":       "watch_medier",
    "watch int":      "watch_medier",
    "finans dk":      "finans",
    "monitor":        "monitor",
    "watch no":       "watch_no",
    "watch se":       "watch_se",
    "finanswatch se": "watch_se",
    "watch de":       "watch_de",
    "finanzbusiness": "watch_de",
    "marketwire":     "marketwire",
}


def _deal_group(team: str | None, account: str | None, is_web: bool) -> str:
    """Gruppen en deal hører til: Web Sale pr. land før team; Watch DE har team=NULL."""
    if is_web:
        return _WEB_ACCOUNT_TO_GROUP.get((account or "").strip().lower(), "web_dk")
    g = _TEAM_TO_GROUP.get((team or "").strip())
    if g:
        return g
    if (account or "").strip().lower() == "watch_de":
        return "watch_de"
    return "ovrige"


def _budget_group(brand: str | None, dealtype: str | None, salestype: str | None) -> str | None:
    """Gruppen en budgetrække hører til (None = udelades).

    Brandenes budgetter er inkl. websalg: websale-budgetrækker (Salestype=
    'Websale') følger brandet (fx FINANS DK → finans) — Web Sale-rækkerne i
    dashboardet har intet eget budget. Programmatisk budget (Salestype=
    'Programmatic', DealType='Banner') følger banner-reglen og lander på
    Banner-gruppen — det programmatiske SALG lægges tilsvarende til Banner fra
    ProgrammaticSales."""
    b  = (brand or "").strip().lower()
    dt = (dealtype or "").strip().lower()
    st = (salestype or "").strip().lower()
    if dt in ("job", "banner"):
        return "advertising_no" if b == "watch no" else dt
    return _SUB_BUDGET_TO_GROUP.get(b, "ovrige")


_WEB_CASE = "CASE WHEN [pipeline_name]='Web Sale' THEN 1 ELSE 0 END"


def _budget_by_group_month(cur, year: int) -> dict[str, dict[int, float]]:
    """{gruppe: {måned: budget}} fra BudgetsIntoMedia (ekskl. programmatisk)."""
    cur.execute("""
        SELECT [Brand] AS b, [DealType] AS dt, COALESCE([Salestype],'') AS st,
               MONTH([BudgetDate]) AS mm, ISNULL(SUM([BudgetAmount]),0) AS v
        FROM [dbo].[BudgetsIntoMedia]
        WHERE YEAR([BudgetDate]) = %s
        GROUP BY [Brand], [DealType], COALESCE([Salestype],''), MONTH([BudgetDate])
    """, (year,))
    out: dict[str, dict[int, float]] = {}
    for r in cur.fetchall():
        g = _budget_group(r["b"], r["dt"], r["st"])
        if g is None or not r["mm"]:
            continue
        out.setdefault(g, {})
        out[g][int(r["mm"])] = out[g].get(int(r["mm"]), 0.0) + float(r["v"] or 0)
    return out


def db_afdelingsleder_overblik(today: date):
    """Overblik-fanen: afdelings-total øverst + samme tal pr. brand-gruppe.

    Alt på ÉN konsistent basis: service_activation_date (SAD), status='won',
    ekskl. administrative deals. Web Sale er sin egen gruppe (ikke ekskluderet),
    og programmatisk salg (ProgrammaticSales) lægges til Banner med andelen i
    'prog'. Netto = salg − opsigelser (CANCELLATION_PIPELINES); pct = netto mod
    dag-proreret ÅTD-budget (jf. gruppe-/budget-mappingen ovenfor). Total = Σ
    grupper, så toppen ALTID stemmer med rækkerne nedenunder.
    """
    year = today.year
    m    = today.month
    frac = today.day / calendar.monthrange(year, m)[1]   # andel af indevaerende maaned
    jan1 = date(year, 1, 1)
    cut_excl = today + timedelta(days=1)                 # til og med i dag
    dcol = "[service_activation_date]"    # SAD: budgettet er sat pr. aktiveringsmåned

    conn = get_conn()
    cur  = conn.cursor(as_dict=True)

    # ÅTD pr. (team, account, web) — mappes til grupper i Python.
    cur.execute(f"""
        SELECT [team] AS t, [account] AS a, {_WEB_CASE} AS web,
               ISNULL(SUM(CASE WHEN [pipeline_name] NOT IN {_CANCEL_PH}
                   THEN {_VAL} ELSE 0 END), 0) AS won,
               ISNULL(ABS(SUM(CASE WHEN [pipeline_name] IN {_CANCEL_PH}
                   THEN {_VAL} ELSE 0 END)), 0) AS ops,
               COUNT(CASE WHEN [pipeline_name] NOT IN {_CANCEL_PH} THEN 1 END) AS n
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='won' AND {dcol} >= %s AND {dcol} < %s
          {_ADM_EXCL_WEB}
        GROUP BY [team], [account], {_WEB_CASE}
    """, tuple(CANCELLATION_PIPELINES) * 3 + (jan1, cut_excl))
    agg: dict[str, dict] = {}
    for r in cur.fetchall():
        g = _deal_group(r["t"], r["a"], bool(r["web"]))
        d = agg.setdefault(g, {"salg": 0.0, "ops": 0.0, "deals": 0})
        d["salg"]  += float(r["won"] or 0)
        d["ops"]   += float(r["ops"] or 0)
        d["deals"] += int(r["n"] or 0)

    # Månedlige serier (hele året) til grafen — samme scope som ÅTD.
    cur.execute(f"""
        SELECT MONTH({dcol}) AS mm,
               ISNULL(SUM(CASE WHEN [pipeline_name] NOT IN {_CANCEL_PH}
                   THEN {_VAL} ELSE 0 END), 0) AS won,
               ISNULL(ABS(SUM(CASE WHEN [pipeline_name] IN {_CANCEL_PH}
                   THEN {_VAL} ELSE 0 END)), 0) AS ops
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='won' AND {dcol} >= %s AND {dcol} < %s
          {_ADM_EXCL_WEB}
        GROUP BY MONTH({dcol})
    """, tuple(CANCELLATION_PIPELINES) * 2 + (jan1, date(year + 1, 1, 1)))
    by_m = {int(r["mm"]): (float(r["won"] or 0), float(r["ops"] or 0)) for r in cur.fetchall()}

    # Programmatisk salg (ProgrammaticSales — uden for PipedriveDeals) → Banner.
    cur.execute("""
        SELECT MONTH([Date]) AS mm, ISNULL(SUM([Amount]),0) AS v
        FROM [dbo].[ProgrammaticSales]
        WHERE [Date] >= %s AND [Date] < %s
        GROUP BY MONTH([Date])
    """, (jan1, date(year + 1, 1, 1)))
    prog_m = {int(r["mm"]): float(r["v"] or 0) for r in cur.fetchall()}
    cur.execute("""
        SELECT ISNULL(SUM([Amount]),0) AS v FROM [dbo].[ProgrammaticSales]
        WHERE [Date] >= %s AND [Date] < %s
    """, (jan1, cut_excl))
    prog_atd = float((cur.fetchone() or {}).get("v") or 0)
    if prog_atd or any(prog_m.values()):
        d = agg.setdefault("banner", {"salg": 0.0, "ops": 0.0, "deals": 0})
        d["salg"] += prog_atd
        d["prog"] = prog_atd

    budgets = _budget_by_group_month(cur, year)
    conn.close()

    def _budget_atd(g: str) -> float:
        bm = budgets.get(g, {})
        return sum(v for mm, v in bm.items() if mm < m) + bm.get(m, 0.0) * frac

    def _pct_status(netto: float, bud: float):
        pct = round(netto / bud * 100, 1) if bud > 0 else None
        status = ("rod" if pct < 90 else "gul" if pct < 100 else "gron") if pct is not None else "ukendt"
        return pct, status

    groups = []
    for key, label in OVERBLIK_GROUPS:
        d = agg.get(key)
        bm = budgets.get(key, {})
        if d is None and not bm:
            continue                       # fx 'Øvrige' uden data
        salg = round((d or {}).get("salg", 0.0), 2)
        ops  = round((d or {}).get("ops", 0.0), 2)
        netto = round(salg - ops, 2)
        bud_atd = _budget_atd(key)
        fuldaar = sum(bm.values())
        pct, status = _pct_status(netto, bud_atd)
        groups.append({
            "key": key, "label": label,
            "salg": salg, "ops": ops, "netto": netto,
            "prog": round((d or {}).get("prog", 0.0), 2),   # heraf programmatisk
            "deals": (d or {}).get("deals", 0),
            "budget_atd": round(bud_atd),
            "fuldaar_budget": round(fuldaar),
            "pct": pct, "status": status,
        })

    # Total = Σ grupper (blandede valutaer: NO/SE i lokal) — matcher rækkerne 1:1.
    t_salg  = round(sum(g["salg"] for g in groups), 2)
    t_ops   = round(sum(g["ops"] for g in groups), 2)
    t_netto = round(t_salg - t_ops, 2)
    t_bud   = sum(_budget_atd(g["key"]) for g in groups)
    t_fuld  = sum(g["fuldaar_budget"] for g in groups)
    t_pct, t_status = _pct_status(t_netto, t_bud)
    prognose = round(t_pct / 100 * t_fuld) if t_pct is not None else None

    return {
        "year":   year,
        "cutoff": today.isoformat(),
        "total": {
            "salg": round(t_salg), "ops": round(t_ops), "netto": round(t_netto),
            "budget_atd": round(t_bud), "fuldaar_budget": round(t_fuld),
            "pct": t_pct, "status": t_status, "prognose": prognose,
        },
        "groups": groups,
        "monthly_salg":   [round(by_m.get(i, (0, 0))[0] + prog_m.get(i, 0)) for i in range(1, 13)],
        "monthly_netto":  [round(by_m.get(i, (0, 0))[0] - by_m.get(i, (0, 0))[1] + prog_m.get(i, 0))
                           for i in range(1, 13)],
        "monthly_budget": [round(sum(budgets.get(g["key"], {}).get(i, 0.0) for g in groups))
                           for i in range(1, 13)],
    }


def db_maaned_deepdive(year: int, month: int):
    """Måneds-deepdive: sælgernes performance pr. brand-gruppe i én måned.

    Samme gruppering, deal-scope og budget-mapping som db_afdelingsleder_overblik
    (service_activation_date i måneden, ekskl. administrative; Web Sale egen
    gruppe; programmatisk salg som egen række under Banner). Pr. gruppe: totaler
    + månedens budget + sælgerrækker (salg/opsigelser/netto/deals), sorteret
    efter netto. Grupper uden aktivitet i måneden udelades.
    """
    first = date(year, month, 1)
    nxt   = date(year + 1, 1, 1) if month == 12 else date(year, month + 1, 1)
    dcol  = "[service_activation_date]"   # SAD: samme dato-grundlag som budgettet

    conn = get_conn()
    cur  = conn.cursor(as_dict=True)
    cur.execute(f"""
        SELECT COALESCE([owner_name],'(uden ejer)') AS o, [team] AS t, [account] AS a,
               {_WEB_CASE} AS web,
               ISNULL(SUM(CASE WHEN [pipeline_name] NOT IN {_CANCEL_PH}
                   THEN {_VAL} ELSE 0 END), 0) AS won,
               ISNULL(ABS(SUM(CASE WHEN [pipeline_name] IN {_CANCEL_PH}
                   THEN {_VAL} ELSE 0 END)), 0) AS ops,
               COUNT(CASE WHEN [pipeline_name] NOT IN {_CANCEL_PH} THEN 1 END) AS n
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='won' AND {dcol} >= %s AND {dcol} < %s
          {_ADM_EXCL_WEB}
        GROUP BY COALESCE([owner_name],'(uden ejer)'), [team], [account], {_WEB_CASE}
    """, tuple(CANCELLATION_PIPELINES) * 3 + (first, nxt))

    per_group: dict[str, dict[str, dict]] = {}
    for r in cur.fetchall():
        g = _deal_group(r["t"], r["a"], bool(r["web"]))
        s = per_group.setdefault(g, {}).setdefault(r["o"], {"salg": 0.0, "ops": 0.0, "deals": 0})
        s["salg"]  += float(r["won"] or 0)
        s["ops"]   += float(r["ops"] or 0)
        s["deals"] += int(r["n"] or 0)

    # Månedens programmatiske salg → egen "sælger"-række under Banner.
    cur.execute("""
        SELECT ISNULL(SUM([Amount]),0) AS v FROM [dbo].[ProgrammaticSales]
        WHERE [Date] >= %s AND [Date] < %s
    """, (first, nxt))
    prog_md = round(float((cur.fetchone() or {}).get("v") or 0), 2)

    budgets = _budget_by_group_month(cur, year)
    conn.close()

    groups = []
    for key, label in OVERBLIK_GROUPS:
        sellers_raw = per_group.get(key)
        bud_md = budgets.get(key, {}).get(month, 0.0)
        sellers = [{"navn": navn,
                    "salg": round(s["salg"], 2), "ops": round(s["ops"], 2),
                    "netto": round(s["salg"] - s["ops"], 2), "deals": s["deals"]}
                   for navn, s in (sellers_raw or {}).items()]
        if key == "banner" and prog_md:
            sellers.append({"navn": "Programmatisk salg (FINANS DK)",
                            "salg": prog_md, "ops": 0.0, "netto": prog_md,
                            "deals": None})
        if not sellers and not bud_md:
            continue
        sellers.sort(key=lambda s: s["netto"], reverse=True)
        salg = round(sum(s["salg"] for s in sellers), 2)
        ops  = round(sum(s["ops"] for s in sellers), 2)
        netto = round(salg - ops, 2)
        pct = round(netto / bud_md * 100, 1) if bud_md > 0 else None
        groups.append({
            "key": key, "label": label,
            "salg": salg, "ops": ops, "netto": netto,
            "deals": sum(s["deals"] or 0 for s in sellers),   # programmatisk række: None
            "budget": round(bud_md), "pct": pct,
            "sellers": sellers,
        })

    t_salg = round(sum(g["salg"] for g in groups), 2)
    t_ops  = round(sum(g["ops"] for g in groups), 2)
    t_bud  = round(sum(g["budget"] for g in groups))
    t_netto = round(t_salg - t_ops, 2)
    return {
        "year": year, "month": month,
        "total": {"salg": round(t_salg), "ops": round(t_ops), "netto": round(t_netto),
                  "budget": t_bud,
                  "pct": round(t_netto / t_bud * 100, 1) if t_bud > 0 else None},
        "groups": groups,
    }


def db_afdelingsleder_churn(today: date):
    """Blok ③: churn-rate (B') + top-opsigelser for hele afdelingen.

    Churn-rate = opsigelser ÅTD / NUVAERENDE portefoelje (PipeDrive_ACV, nyeste
    raekke pr. kunde x site). Naevneren er den nuvaerende base, fordi ACV-historik
    ikke raekker tilbage foer 1/1 — "base ved aarets start" er ikke muligt.
    """
    year     = today.year
    cut_excl = today + timedelta(days=1)
    dcol     = "COALESCE([service_activation_date],[won_time])"

    conn = get_conn()
    cur  = conn.cursor(as_dict=True)

    def _scalar(sql, params=()):
        cur.execute(sql, params)
        row = cur.fetchone() or {}
        return float(list(row.values())[0] or 0)

    ops_atd = _scalar(f"""
        SELECT ISNULL(ABS(SUM({_VAL})),0) AS v
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='won' AND [pipeline_name] IN {_CANCEL_PH}
          AND {dcol} >= %s AND {dcol} < %s
          {_ADM_EXCLUDE}
    """, tuple(CANCELLATION_PIPELINES) + (date(year, 1, 1), cut_excl))

    portefolje = _scalar("""
        WITH latest AS (
            SELECT acv_value_dkk, ROW_NUMBER() OVER
                (PARTITION BY org_id, site ORDER BY updated_at DESC) AS rn
            FROM [dbo].[PipeDrive_ACV]
        )
        SELECT ISNULL(SUM(acv_value_dkk),0) AS v FROM latest WHERE rn = 1
    """)

    cur.execute(f"""
        SELECT TOP 10 [org_name] AS kunde, ABS(SUM({_VAL})) AS belob
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='won' AND [pipeline_name] IN {_CANCEL_PH}
          AND {dcol} >= %s AND {dcol} < %s
          {_ADM_EXCLUDE}
        GROUP BY [org_name]
        ORDER BY belob DESC
    """, tuple(CANCELLATION_PIPELINES) + (date(year, 1, 1), cut_excl))
    top = [{"kunde": r["kunde"] or "(uden navn)", "belob": round(float(r["belob"] or 0))}
           for r in cur.fetchall()]

    # Afhaengighedsrisiko: de 10 stoerste navngivne kunder (ekskl. Web Sale) mod fuld
    # portefolje. Headline-maalet er top 5 (top_kunder[:5]); tabellen viser top 10.
    cur.execute("""
        WITH latest AS (
            SELECT org_id, org_name, site, acv_value_dkk,
                ROW_NUMBER() OVER (PARTITION BY org_id, site ORDER BY updated_at DESC) AS rn
            FROM [dbo].[PipeDrive_ACV]
        )
        SELECT TOP 10 MAX(org_name) AS kunde, SUM(acv_value_dkk) AS val
        FROM latest WHERE rn = 1 AND org_name NOT LIKE 'Web Sale%'
        GROUP BY org_id
        ORDER BY val DESC
    """)
    top_kunder = [{"kunde": r["kunde"] or "(uden navn)",
                   "belob": round(float(r["val"] or 0)),
                   "andel": round(float(r["val"] or 0) / portefolje * 100, 1) if portefolje else None}
                  for r in cur.fetchall()]
    top5_andel  = round(sum(k["belob"] for k in top_kunder[:5]) / portefolje * 100, 1) if portefolje else None
    top10_andel = round(sum(k["belob"] for k in top_kunder)      / portefolje * 100, 1) if portefolje else None

    conn.close()

    churn_rate = round(ops_atd / portefolje * 100, 1) if portefolje else None

    return {
        "ops_atd":    round(ops_atd),
        "portefolje": round(portefolje),
        "churn_rate": churn_rate,
        "top":        top,
        "top_kunder":  top_kunder,
        "top5_andel":  top5_andel,
        "top10_andel": top10_andel,
        "cutoff":      today.isoformat(),
    }


def db_afdelingsleder_vaekst(today: date):
    """Blok ④: vaekst fra nye vs. eksisterende kunder.

    Ny kunde = org_id hvis foerste won-deal (ekskl. Web Sale/opsigelser/admin)
    ligger i indevaerende aar. Splittet daekker hele faktisk-salget AATD.
    """
    year     = today.year
    cut_excl = today + timedelta(days=1)
    jan1     = date(year, 1, 1)
    jan1_str = f"'{year}0101'"          # sikkert YYYYMMDD-literal til CASE/filter
    dcol     = "COALESCE([service_activation_date],[won_time])"

    conn = get_conn()
    cur  = conn.cursor(as_dict=True)

    # 1. Split AATD-salg paa ny vs. eksisterende kunde
    cur.execute(f"""
        WITH first_deal AS (
            SELECT org_id, MIN({dcol}) AS first_dato
            FROM [dbo].[PipedriveDeals]
            WHERE [status]='won' AND [pipeline_name]<>'Web Sale'
              AND [pipeline_name] NOT IN {_CANCEL_PH} {_ADM_EXCLUDE}
            GROUP BY org_id
        )
        SELECT
            CASE WHEN fd.first_dato >= {jan1_str} THEN 'ny' ELSE 'eks' END AS type,
            ISNULL(SUM({_VAL}),0) AS salg,
            COUNT(DISTINCT d.org_id) AS kunder
        FROM [dbo].[PipedriveDeals] d
        JOIN first_deal fd ON fd.org_id = d.org_id
        WHERE d.[status]='won' AND d.[pipeline_name]<>'Web Sale'
          AND d.[pipeline_name] NOT IN {_CANCEL_PH}
          AND {dcol} >= %s AND {dcol} < %s {_ADM_EXCLUDE}
        GROUP BY CASE WHEN fd.first_dato >= {jan1_str} THEN 'ny' ELSE 'eks' END
    """, tuple(CANCELLATION_PIPELINES) + tuple(CANCELLATION_PIPELINES) + (jan1, cut_excl))

    rows     = {r["type"]: r for r in cur.fetchall()}
    ny_salg  = float((rows.get("ny")  or {}).get("salg") or 0)
    ny_k     = (rows.get("ny")  or {}).get("kunder") or 0
    eks_salg = float((rows.get("eks") or {}).get("salg") or 0)
    eks_k    = (rows.get("eks") or {}).get("kunder") or 0
    tot      = ny_salg + eks_salg

    # 2. Top 5 nye logoer i aar
    cur.execute(f"""
        WITH first_deal AS (
            SELECT org_id, MIN({dcol}) AS first_dato
            FROM [dbo].[PipedriveDeals]
            WHERE [status]='won' AND [pipeline_name]<>'Web Sale'
              AND [pipeline_name] NOT IN {_CANCEL_PH} {_ADM_EXCLUDE}
            GROUP BY org_id
        )
        SELECT TOP 5 MAX(d.[org_name]) AS kunde, ISNULL(SUM({_VAL}),0) AS salg
        FROM [dbo].[PipedriveDeals] d
        JOIN first_deal fd ON fd.org_id = d.org_id
        WHERE d.[status]='won' AND d.[pipeline_name]<>'Web Sale'
          AND d.[pipeline_name] NOT IN {_CANCEL_PH}
          AND {dcol} >= %s AND {dcol} < %s {_ADM_EXCLUDE}
          AND fd.first_dato >= {jan1_str}
        GROUP BY d.org_id
        ORDER BY salg DESC
    """, tuple(CANCELLATION_PIPELINES) + tuple(CANCELLATION_PIPELINES) + (jan1, cut_excl))
    top_nye = [{"kunde": r["kunde"] or "(uden navn)", "belob": round(float(r["salg"] or 0))}
               for r in cur.fetchall()]

    # 3. Maanedlig kundeudvikling: salg fra ny vs. eksisterende pr. maaned (hele aaret)
    cur.execute(f"""
        WITH first_deal AS (
            SELECT org_id, MIN({dcol}) AS first_dato
            FROM [dbo].[PipedriveDeals]
            WHERE [status]='won' AND [pipeline_name]<>'Web Sale'
              AND [pipeline_name] NOT IN {_CANCEL_PH} {_ADM_EXCLUDE}
            GROUP BY org_id
        )
        SELECT MONTH({dcol}) AS m,
            ISNULL(SUM(CASE WHEN fd.first_dato >= {jan1_str} THEN {_VAL} ELSE 0 END),0) AS ny,
            ISNULL(SUM(CASE WHEN fd.first_dato <  {jan1_str} THEN {_VAL} ELSE 0 END),0) AS eks
        FROM [dbo].[PipedriveDeals] d
        JOIN first_deal fd ON fd.org_id = d.org_id
        WHERE d.[status]='won' AND d.[pipeline_name]<>'Web Sale'
          AND d.[pipeline_name] NOT IN {_CANCEL_PH}
          AND {dcol} >= %s AND {dcol} < %s {_ADM_EXCLUDE}
        GROUP BY MONTH({dcol})
    """, tuple(CANCELLATION_PIPELINES) + tuple(CANCELLATION_PIPELINES) + (jan1, date(year + 1, 1, 1)))
    by_m = {int(r["m"]): (float(r["ny"] or 0), float(r["eks"] or 0)) for r in cur.fetchall()}

    conn.close()

    return {
        "ny_salg":     round(ny_salg),
        "ny_kunder":   ny_k,
        "eks_salg":    round(eks_salg),
        "eks_kunder":  eks_k,
        "ny_andel":    round(ny_salg / tot * 100, 1) if tot else None,
        "eks_andel":   round(eks_salg / tot * 100, 1) if tot else None,
        "top_nye":     top_nye,
        "monthly_ny":  [round(by_m.get(i, (0, 0))[0]) for i in range(1, 13)],
        "monthly_eks": [round(by_m.get(i, (0, 0))[1]) for i in range(1, 13)],
        "cutoff":      today.isoformat(),
    }
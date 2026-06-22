from datetime import date, timedelta

from constants import CANCELLATION_PIPELINES
from db import get_conn
from moduler.modul_perf.queries import _ADM_EXCLUDE
import calendar

# Pladsholder for opsigelses-pipelines — gen-afledt lokalt (jf. queries.py linje 34).
_CANCEL_PH = "(" + ",".join(["%s"] * len(CANCELLATION_PIPELINES)) + ")"

# Belob i lokal valuta (NO/SE i lokal, ellers DKK) — delt af modulets queries.
_VAL = ("CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') "
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

def db_afdelingsleder_hero(today: date):
    """Blok ①: budget-vs-faktisk ÅTD + run-rate-prognose for hele afdelingen.

    Faktisk = salg ÅTD (won, ekskl. opsigelser/Web Sale/admin, NO/SE i lokal
    valuta) direkte fra PipedriveDeals. Budget = dag-proreret ÅTD fra
    BudgetsIntoMedia. Prognose = faktisk-pct × fuldaarsbudget.
    """
    year = today.year
    m    = today.month
    frac = today.day / calendar.monthrange(year, m)[1]   # andel af indevaerende maaned
    cut_excl = today + timedelta(days=1)                 # til og med i dag

    conn = get_conn()
    cur  = conn.cursor(as_dict=True)

    def _scalar(sql, params=()):
        cur.execute(sql, params)
        row = cur.fetchone() or {}
        return float(list(row.values())[0] or 0)

    dcol = "COALESCE([service_activation_date],[won_time])"

    faktisk_atd = _scalar(f"""
        SELECT ISNULL(SUM({_VAL}),0) AS v
        FROM [dbo].[PipedriveDeals]
        WHERE [status]='won' AND [pipeline_name]<>'Web Sale'
          AND [pipeline_name] NOT IN {_CANCEL_PH}
          AND {dcol} >= %s AND {dcol} < %s
          {_ADM_EXCLUDE}
    """, tuple(CANCELLATION_PIPELINES) + (date(year, 1, 1), cut_excl))

    bud_hele = _scalar("""
        SELECT ISNULL(SUM([BudgetAmount]),0) AS v FROM [dbo].[BudgetsIntoMedia]
        WHERE YEAR([BudgetDate]) = %s AND MONTH([BudgetDate]) < %s
    """, (year, m))

    bud_md = _scalar("""
        SELECT ISNULL(SUM([BudgetAmount]),0) AS v FROM [dbo].[BudgetsIntoMedia]
        WHERE YEAR([BudgetDate]) = %s AND MONTH([BudgetDate]) = %s
    """, (year, m))

    fuldaar_budget = _scalar("""
        SELECT ISNULL(SUM([BudgetAmount]),0) AS v FROM [dbo].[BudgetsIntoMedia]
        WHERE YEAR([BudgetDate]) = %s
    """, (year,))

    conn.close()

    budget_atd = bud_hele + bud_md * frac
    pct        = round(faktisk_atd / budget_atd * 100, 1) if budget_atd else None
    prognose   = round(pct / 100 * fuldaar_budget) if pct is not None else None
    status     = ("rod" if pct < 90 else "gul" if pct < 100 else "gron") if pct is not None else "ukendt"

    return {
        "faktisk_atd":    round(faktisk_atd),
        "budget_atd":     round(budget_atd),
        "fuldaar_budget": round(fuldaar_budget),
        "pct":            pct,
        "prognose":       prognose,
        "status":         status,
        "cutoff":         today.isoformat(),
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

    # Afhaengighedsrisiko: top 5 navngivne kunder (ekskl. Web Sale) mod fuld portefolje.
    cur.execute("""
        WITH latest AS (
            SELECT org_id, org_name, site, acv_value_dkk,
                ROW_NUMBER() OVER (PARTITION BY org_id, site ORDER BY updated_at DESC) AS rn
            FROM [dbo].[PipeDrive_ACV]
        )
        SELECT TOP 5 MAX(org_name) AS kunde, SUM(acv_value_dkk) AS val
        FROM latest WHERE rn = 1 AND org_name <> 'Web Sale'
        GROUP BY org_id
        ORDER BY val DESC
    """)
    top_kunder = [{"kunde": r["kunde"] or "(uden navn)",
                   "belob": round(float(r["val"] or 0)),
                   "andel": round(float(r["val"] or 0) / portefolje * 100, 1) if portefolje else None}
                  for r in cur.fetchall()]
    top5_andel = round(sum(k["belob"] for k in top_kunder) / portefolje * 100, 1) if portefolje else None

    conn.close()

    churn_rate = round(ops_atd / portefolje * 100, 1) if portefolje else None

    return {
        "ops_atd":    round(ops_atd),
        "portefolje": round(portefolje),
        "churn_rate": churn_rate,
        "top":        top,
        "top_kunder": top_kunder,
        "top5_andel": top5_andel,
        "cutoff":     today.isoformat(),
    }
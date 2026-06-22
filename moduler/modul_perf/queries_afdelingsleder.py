from datetime import date, timedelta

from constants import CANCELLATION_PIPELINES
from db import get_conn
from moduler.modul_perf.queries import _ADM_EXCLUDE

# Pladsholder for opsigelses-pipelines — gen-afledt lokalt (jf. queries.py linje 34).
_CANCEL_PH = "(" + ",".join(["%s"] * len(CANCELLATION_PIPELINES)) + ")"

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

    _VAL = ("CAST(COALESCE(CASE WHEN [currency] IN ('NOK','SEK') "
            "THEN [value] ELSE [value_dkk] END,[value]) AS DECIMAL(18,2))")
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
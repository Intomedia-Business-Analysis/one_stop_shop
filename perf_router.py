import traceback
import os
from datetime import date, timedelta

from dotenv import load_dotenv
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
import pymssql

from auth import ROLE_LABELS, get_current_user, has_access

load_dotenv()

router = APIRouter(prefix="/tools/performance", tags=["Performance"])
templates = Jinja2Templates(directory="templates")
templates.env.globals["ROLE_LABELS"] = ROLE_LABELS

SUBSCRIPTION_BRANDS = [
    "EnergiWatch NO", "MobilityWatch DK", "CleantechWatch DK", "TechWatch NO",
    "AdvokatWatch NO", "Kforum DK", "Seniormonitor", "All Monitor Sites",
    "FinansWatch SE", "Watch Medier DK", "Byrummonitor", "ShippingWatch DK",
    "Idrætsmonitor", "Justitsmonitor", "MatvareWatch NO", "Naturmonitor",
    "Socialmonitor", "FinansWatch DK", "Uddannelsesmonitor", "MedWatch NO",
    "Klimamonitor", "EjendomsWatch DK", "FINANS DK", "DetailWatch DK",
    "FinansWatch NO", "AdvokatWatch DK", "ITWatch DK", "KForum",
    "All Watch Sites DK", "EnergiWatch DK", "Medier24 NO", "AgriWatch DK",
    "Skolemonitor", "EiendomsWatch NO", "Kulturmonitor", "Sundhedsmonitor",
    "MarketWire", "Kom24 NO", "AMWatch DK", "KapitalWatch DK",
    "Policy DK", "HandelsWatch NO", "MedWatch DK", "FødevareWatch DK",
    "Fødevare Watch DK",   # DB-variant med mellemrum
    "All Watch Sites NO", "MediaWatch DK", "Turistmonitor", "PolicyWatch DK",
    "Monitormedier",       # Tilføjet fra DB-tjek
]
BRANDS_PLACEHOLDER = "(" + ",".join(["%s"] * len(SUBSCRIPTION_BRANDS)) + ")"

# Brand group filter — maps a filter key to a subset of SUBSCRIPTION_BRANDS
BRAND_GROUPS: dict[str, list[str]] = {
    "watch_dk": [
        "FinansWatch DK", "Watch Medier DK", "ShippingWatch DK", "EjendomsWatch DK",
        "AdvokatWatch DK", "ITWatch DK", "EnergiWatch DK", "AgriWatch DK",
        "AMWatch DK", "KapitalWatch DK", "MedWatch DK", "FødevareWatch DK",
        "Fødevare Watch DK",  # DB-variant med mellemrum
        "MediaWatch DK", "DetailWatch DK", "KForum", "Kforum DK", "All Watch Sites DK",
        "PolicyWatch DK", "Policy DK", "MobilityWatch DK", "CleantechWatch DK",
    ],
    "finans": ["FINANS DK"],
    "watch_no": [
        "EnergiWatch NO", "TechWatch NO", "AdvokatWatch NO", "MatvareWatch NO",
        "MedWatch NO", "FinansWatch NO", "EiendomsWatch NO", "Kom24 NO",
        "HandelsWatch NO", "Medier24 NO", "All Watch Sites NO",
    ],
    "watch_se": ["FinansWatch SE"],
    "monitor": [
        "Seniormonitor", "Byrummonitor", "Idrætsmonitor", "Justitsmonitor",
        "Naturmonitor", "Socialmonitor", "Uddannelsesmonitor", "Klimamonitor",
        "Kulturmonitor", "Sundhedsmonitor", "Skolemonitor", "Turistmonitor",
        "All Monitor Sites", "Monitormedier",
    ],
    "marketwire": ["MarketWire"],
}
BRAND_GROUP_LABELS = {
    "watch_dk":   "Watch DK",
    "finans":     "FINANS DK",
    "watch_no":   "Watch NO",
    "watch_se":   "Watch SE",
    "monitor":    "Monitor",
    "marketwire": "MarketWire",
}

# Whitelisted grouping columns for the /breakdown endpoint
GROUPBY_COLUMNS = {
    "sales_type": "[sales_type]",
    "source":     "[source_name]",
    "basis":      "[deal_basis]",
}


def get_conn():
    return pymssql.connect(
        server=os.getenv("DB_SERVER"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME", "INTOMEDIA"),
        tds_version="7.0",
        login_timeout=5,
        timeout=5,
    )


def _resolve_brand_list(brand_groups_param: str | None) -> list | None:
    """Konvertér kommasepareret brand_groups param til en kombineret site-liste.
    Returnerer None (= alle brands) hvis ingen grupper er valgt."""
    if not brand_groups_param:
        return None
    keys = [k.strip() for k in brand_groups_param.split(",") if k.strip() in BRAND_GROUPS]
    if not keys:
        return None
    combined: list = []
    seen: set = set()
    for k in keys:
        for site in BRAND_GROUPS[k]:
            if site not in seen:
                combined.append(site)
                seen.add(site)
    return combined or None


def _date_expr(date_field):
    if date_field == "service_activation_date":
        return "COALESCE([service_activation_date], [won_time])"
    return "[won_time]"


def _shift_year_back(from_str: str, to_str: str):
    """Returnér (from, to) forskudt 1 år tilbage — til YoY-sammenligning."""
    f = date.fromisoformat(from_str)
    t = date.fromisoformat(to_str)
    try:
        ly_f = f.replace(year=f.year - 1)
    except ValueError:  # 29. feb i skudår -> 28. feb
        ly_f = f.replace(year=f.year - 1, day=28)
    try:
        ly_t = t.replace(year=t.year - 1)
    except ValueError:
        ly_t = t.replace(year=t.year - 1, day=28)
    return ly_f.isoformat(), ly_t.isoformat()


# Pipelines der tæller som opsigelser / cancellations (dansk + engelsk)
CANCELLATION_PIPELINES = ["Cancellation", "Cancellations", "Opsigelser"]
_CANCEL_PH = "(" + ",".join(["%s"] * len(CANCELLATION_PIPELINES)) + ")"

# Omsætning-pipelines: Company trial, New Bizz, Customer
# (Web Sale håndteres separat via include_web_sale-flaget)
REVENUE_PIPELINES = ["Company trial", "Company Trial", "New Bizz", "Customer"]

# "Subscription" og "Abonnement" er samme deal type — normalisér til ét filter-valg
DEAL_TYPE_ALIASES: dict[str, list[str]] = {
    "Abonnement":  ["Abonnement", "Subscription"],
    "Subscription": ["Abonnement", "Subscription"],
}
# Canonical display name for the merged group
DEAL_TYPE_CANONICAL = {
    "Abonnement":  "Abonnement",
    "Subscription": "Abonnement",
}


def _build_where(date_field, date_from, date_to, include_web_sale, deal_type, sales_type,
                 owner_filter, cancellations_only=False, exclude_cancellations=False,
                 source=None, basis=None, brand_list=None):
    """Returnér (WHERE-streng, params-liste). date_to er eksklusiv (dagen efter).
    brand_list: liste af sites at filtrere på — None = alle SUBSCRIPTION_BRANDS.
    Cancellations identificeres via pipeline_name (Cancellation/Cancellations).
    Omsætning (ekskl. cancellations) = pipeline_name NOT IN cancellation pipelines."""
    date_expr = _date_expr(date_field)
    brands = brand_list if brand_list else SUBSCRIPTION_BRANDS
    brands_ph = "(" + ",".join(["%s"] * len(brands)) + ")"
    clauses = [
        "[status] = 'won'",
        f"{date_expr} >= %s",
        f"{date_expr} < %s",
        f"[sites] IN {brands_ph}",
    ]
    params = [date_from, date_to] + list(brands)

    if not include_web_sale:
        clauses.append("[pipeline_name] <> 'Web Sale'")

    if cancellations_only:
        # Kun Cancellation pipeline
        clauses.append(f"[pipeline_name] IN {_CANCEL_PH}")
        params.extend(CANCELLATION_PIPELINES)
    elif exclude_cancellations:
        # Ekskluder Cancellation pipeline (omsætning: Company trial + New Bizz + Customer)
        clauses.append(f"[pipeline_name] NOT IN {_CANCEL_PH}")
        params.extend(CANCELLATION_PIPELINES)

    if owner_filter:
        clauses.append("[owner_name] = %s")
        params.append(owner_filter)

    if deal_type:
        # Håndtér Subscription/Abonnement som synonymer
        aliases = DEAL_TYPE_ALIASES.get(deal_type, [deal_type])
        alias_ph = "(" + ",".join(["%s"] * len(aliases)) + ")"
        clauses.append(f"[deal_type] IN {alias_ph}")
        params.extend(aliases)

    if sales_type:
        clauses.append("[sales_type] = %s")
        params.append(sales_type)

    if source:
        clauses.append("[source_name] = %s")
        params.append(source)

    if basis:
        clauses.append("[deal_basis] = %s")
        params.append(basis)

    return "WHERE " + " AND ".join(clauses), params


# ---------------------------------------------------------------------------
# Page
# ---------------------------------------------------------------------------

@router.get("/", response_class=HTMLResponse)
async def perf_dashboard(request: Request, user=Depends(get_current_user)):
    today = date.today()
    is_manager = has_access(user, "sales_manager")
    return templates.TemplateResponse("perf_dashboard.html", {
        "request":       request,
        "user":          user,
        "years":         list(range(today.year - 3, today.year + 1)),
        "current_year":  today.year,
        "current_month": today.month,
        "current_day":   today.day,
        "is_manager":    is_manager,
    })


@router.get("/overview", response_class=HTMLResponse)
async def perf_overview_page(request: Request, user=Depends(get_current_user)):
    if not has_access(user, "sales_manager"):
        raise HTTPException(403, "Kun Sales Managers og derover har adgang")
    today = date.today()
    return templates.TemplateResponse("perf_overview.html", {
        "request":       request,
        "user":          user,
        "current_year":  today.year,
        "current_month": today.month,
        "current_day":   today.day,
        "is_manager":    True,
    })


# ---------------------------------------------------------------------------
# Filters API
# ---------------------------------------------------------------------------

@router.get("/filters")
async def perf_filters(user=Depends(get_current_user)):
    results = {"deal_types": [], "sales_types": [], "sources": [], "bases": [], "brands": SUBSCRIPTION_BRANDS,
               "brand_groups": [{"value": k, "label": v} for k, v in BRAND_GROUP_LABELS.items()]}
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        for col, key in [
            ("[deal_type]",   "deal_types"),
            ("[sales_type]",  "sales_types"),
            ("[source_name]", "sources"),
            ("[deal_basis]",  "bases"),
        ]:
            try:
                cur.execute(f"""
                    SELECT DISTINCT {col} AS val
                    FROM [dbo].[PipedriveDeals]
                    WHERE {col} IS NOT NULL AND {col} <> ''
                      AND [status] = 'won'
                      AND [sites] IN {BRANDS_PLACEHOLDER}
                    ORDER BY val
                """, tuple(SUBSCRIPTION_BRANDS))
                raw = [r["val"] for r in cur.fetchall()]
                if key == "deal_types":
                    # Fold 'Subscription' into 'Abonnement' so they appear as one entry
                    seen: set = set()
                    deduped = []
                    for v in raw:
                        canonical = DEAL_TYPE_CANONICAL.get(v, v)
                        if canonical not in seen:
                            seen.add(canonical)
                            deduped.append(canonical)
                    results[key] = sorted(deduped)
                else:
                    results[key] = raw
            except Exception:
                pass
        conn.close()
    except Exception as e:
        traceback.print_exc()
    return JSONResponse(results)


# ---------------------------------------------------------------------------
# Data API
# ---------------------------------------------------------------------------

@router.get("/data")
async def perf_data(
    date_from: str,
    date_to: str,
    date_field: str = "won_time",
    include_web_sale: bool = False,
    deal_type: str = None,
    sales_type: str = None,
    brand_groups: str = None,
    user=Depends(get_current_user),
):
    if date_field not in ("won_time", "service_activation_date"):
        raise HTTPException(400, "date_field skal være 'won_time' eller 'service_activation_date'")
    try:
        date.fromisoformat(date_from)
        date.fromisoformat(date_to)
    except ValueError:
        raise HTTPException(400, "date_from og date_to skal være YYYY-MM-DD")

    is_manager = has_access(user, "sales_manager")
    owner_filter = None if is_manager else user["name"]
    if not is_manager:
        include_web_sale = False

    brand_list = _resolve_brand_list(brand_groups)

    ly_from, ly_to = _shift_year_back(date_from, date_to)

    # Budget covers all calendar months touched by the selected period
    _df = date.fromisoformat(date_from)
    _dt = date.fromisoformat(date_to) - timedelta(days=1)   # inclusive end
    budget_from     = date(_df.year, _df.month, 1)
    budget_to_excl  = date(_dt.year + (_dt.month // 12), _dt.month % 12 + 1, 1)
    # For HubForecasts (monthly granularity stored as year+month int), keep year for fallback
    budget_year = _df.year

    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)

        # Q1: Won (ikke cancellations), valgt periode
        where, params = _build_where(
            date_field, date_from, date_to, include_web_sale, deal_type, sales_type,
            owner_filter, exclude_cancellations=True, brand_list=brand_list,
        )
        cur.execute(f"""
            SELECT COALESCE([sites], 'Ukendt') AS brand,
                   COALESCE([owner_name], 'Ukendt') AS owner_name,
                   SUM(CAST(COALESCE([value_dkk], [value]) AS DECIMAL(18,2))) AS won_amount,
                   COUNT(*) AS won_count
            FROM [dbo].[PipedriveDeals]
            {where}
            GROUP BY [sites], [owner_name]
        """, tuple(params))
        won_rows = cur.fetchall()

        # Q2: Cancellations, valgt periode
        where_c, params_c = _build_where(
            date_field, date_from, date_to, include_web_sale, deal_type, sales_type,
            owner_filter, cancellations_only=True, brand_list=brand_list,
        )
        try:
            cur.execute(f"""
                SELECT COALESCE([sites], 'Ukendt') AS brand,
                       COALESCE([owner_name], 'Ukendt') AS owner_name,
                       ABS(SUM(CAST(COALESCE([value_dkk], [value]) AS DECIMAL(18,2)))) AS cancel_amount,
                       COUNT(*) AS cancel_count
                FROM [dbo].[PipedriveDeals]
                {where_c}
                GROUP BY [sites], [owner_name]
            """, tuple(params_c))
            cancel_rows = cur.fetchall()
        except Exception:
            cancel_rows = []

        # Q3: Won (ikke cancellations), samme periode sidste år
        where_ly, params_ly = _build_where(
            date_field, ly_from, ly_to, include_web_sale, deal_type, sales_type,
            owner_filter, exclude_cancellations=True, brand_list=brand_list,
        )
        cur.execute(f"""
            SELECT COALESCE([sites], 'Ukendt') AS brand,
                   COALESCE([owner_name], 'Ukendt') AS owner_name,
                   SUM(CAST(COALESCE([value_dkk], [value]) AS DECIMAL(18,2))) AS won_amount
            FROM [dbo].[PipedriveDeals]
            {where_ly}
            GROUP BY [sites], [owner_name]
        """, tuple(params_ly))
        last_year_rows = cur.fetchall()

        # Q3b: Opsigelser, samme periode sidste år (til tilvækst-YoY)
        where_lyc, params_lyc = _build_where(
            date_field, ly_from, ly_to, include_web_sale, deal_type, sales_type,
            owner_filter, cancellations_only=True, brand_list=brand_list,
        )
        try:
            cur.execute(f"""
                SELECT COALESCE([sites], 'Ukendt') AS brand,
                       COALESCE([owner_name], 'Ukendt') AS owner_name,
                       ABS(SUM(CAST(COALESCE([value_dkk], [value]) AS DECIMAL(18,2)))) AS cancel_amount
                FROM [dbo].[PipedriveDeals]
                {where_lyc}
                GROUP BY [sites], [owner_name]
            """, tuple(params_lyc))
            last_year_cancel_rows = cur.fetchall()
        except Exception:
            last_year_cancel_rows = []

        # Q4: Brand budget — dækker alle måneder inden for den valgte periode
        try:
            cur.execute("""
                SELECT [Site] AS dimension_key,
                       SUM([BudgetAmount]) AS budget
                FROM [dbo].[BudgetsIntoMedia]
                WHERE [BudgetDate] >= %s AND [BudgetDate] < %s
                GROUP BY [Site]
            """, (budget_from.isoformat(), budget_to_excl.isoformat()))
            brand_budget_rows = cur.fetchall()
        except Exception:
            brand_budget_rows = []

        # Q5: Saelger budget — dækker alle måneder inden for den valgte periode
        # Hvis en brand-gruppe er valgt, filtreres på SalesPersonBudget.Brand
        # så sælgerens budget kun vises for den relevante brand (ikke tværgående total).
        saelger_budget_rows = []
        if is_manager:
            try:
                brand_keys = (
                    [k.strip() for k in brand_groups.split(",") if k.strip() in BRAND_GROUPS]
                    if brand_groups else []
                )
                if brand_keys:
                    bk_ph = "(" + ",".join(["%s"] * len(brand_keys)) + ")"
                    cur.execute(f"""
                        SELECT [Owner] AS dimension_key,
                               SUM([BudgetAmount]) AS budget
                        FROM [dbo].[SalesPersonBudget]
                        WHERE [BudgetDate] >= %s AND [BudgetDate] < %s
                          AND [Brand] IN {bk_ph}
                        GROUP BY [Owner]
                    """, (budget_from.isoformat(), budget_to_excl.isoformat(), *brand_keys))
                else:
                    cur.execute("""
                        SELECT [Owner] AS dimension_key,
                               SUM([BudgetAmount]) AS budget
                        FROM [dbo].[SalesPersonBudget]
                        WHERE [BudgetDate] >= %s AND [BudgetDate] < %s
                        GROUP BY [Owner]
                    """, (budget_from.isoformat(), budget_to_excl.isoformat()))
                saelger_budget_rows = cur.fetchall()
            except Exception:
                saelger_budget_rows = []

        # Q6: Brand forecast fra HubForecasts (level='medie')
        try:
            cur.execute("""
                SELECT [dimension_key],
                       SUM([forecast_amount]) AS forecast_total
                FROM [dbo].[HubForecasts]
                WHERE [forecast_year] = %s AND [level] = 'medie'
                GROUP BY [dimension_key]
            """, (budget_year,))
            brand_forecast_rows = cur.fetchall()
        except Exception:
            brand_forecast_rows = []

        # Q7: Saelger forecast fra HubForecasts (level='saelger')
        saelger_forecast_rows = []
        if is_manager:
            try:
                cur.execute("""
                    SELECT [dimension_key],
                           SUM([forecast_amount]) AS forecast_total
                    FROM [dbo].[HubForecasts]
                    WHERE [forecast_year] = %s AND [level] = 'saelger'
                    GROUP BY [dimension_key]
                """, (budget_year,))
                saelger_forecast_rows = cur.fetchall()
            except Exception:
                saelger_forecast_rows = []

        conn.close()

        # Merge i Python
        won_map = {}
        for r in won_rows:
            k = (r["brand"], r["owner_name"])
            won_map[k] = {"won_amount": float(r["won_amount"] or 0), "won_count": int(r["won_count"] or 0)}

        cancel_map = {}
        for r in cancel_rows:
            k = (r["brand"], r["owner_name"])
            cancel_map[k] = {"cancel_amount": float(r["cancel_amount"] or 0), "cancel_count": int(r["cancel_count"] or 0)}

        # last_year_net = last year won - last year cancel (tilvækst samme periode i fjor)
        ly_won_map = {}
        for r in last_year_rows:
            k = (r["brand"], r["owner_name"])
            ly_won_map[k] = float(r["won_amount"] or 0)

        ly_cancel_map = {}
        for r in last_year_cancel_rows:
            k = (r["brand"], r["owner_name"])
            ly_cancel_map[k] = float(r["cancel_amount"] or 0)

        brand_budget_map   = {r["dimension_key"]: float(r["budget"] or 0) for r in brand_budget_rows}
        saelger_budget_map = {r["dimension_key"]: float(r["budget"] or 0) for r in saelger_budget_rows}
        brand_fc_map       = {r["dimension_key"]: float(r["forecast_total"] or 0) for r in brand_forecast_rows}
        saelger_fc_map     = {r["dimension_key"]: float(r["forecast_total"] or 0) for r in saelger_forecast_rows}

        all_keys = sorted(set(
            list(won_map.keys()) + list(cancel_map.keys()) +
            list(ly_won_map.keys()) + list(ly_cancel_map.keys())
        ))

        brand_data = {}
        saelger_data = {}

        for (brand, owner) in all_keys:
            w       = won_map.get((brand, owner), {"won_amount": 0, "won_count": 0})
            c       = cancel_map.get((brand, owner), {"cancel_amount": 0, "cancel_count": 0})
            ly_net  = ly_won_map.get((brand, owner), 0.0) - ly_cancel_map.get((brand, owner), 0.0)
            net     = w["won_amount"] - c["cancel_amount"]

            if brand not in brand_data:
                brand_data[brand] = {
                    "brand": brand, "won_amount": 0, "won_count": 0,
                    "cancel_amount": 0, "cancel_count": 0,
                    "net_amount": 0, "last_year_net": 0,
                    "budget": brand_budget_map.get(brand, 0.0),
                    "forecast": brand_fc_map.get(brand, 0.0),
                }
            bd = brand_data[brand]
            bd["won_amount"]    += w["won_amount"]
            bd["won_count"]     += w["won_count"]
            bd["cancel_amount"] += c["cancel_amount"]
            bd["cancel_count"]  += c["cancel_count"]
            bd["net_amount"]    += net
            bd["last_year_net"] += ly_net

            if is_manager:
                if owner not in saelger_data:
                    saelger_data[owner] = {
                        "owner_name": owner, "won_amount": 0, "won_count": 0,
                        "cancel_amount": 0, "cancel_count": 0,
                        "net_amount": 0, "last_year_net": 0,
                        "budget": saelger_budget_map.get(owner, 0.0),
                        "forecast": saelger_fc_map.get(owner, 0.0),
                    }
                sd = saelger_data[owner]
                sd["won_amount"]    += w["won_amount"]
                sd["won_count"]     += w["won_count"]
                sd["cancel_amount"] += c["cancel_amount"]
                sd["cancel_count"]  += c["cancel_count"]
                sd["net_amount"]    += net
                sd["last_year_net"] += ly_net

        def finalize_row(row):
            ly_net = row["last_year_net"]
            net    = row["net_amount"]
            bud    = row["budget"]
            fc     = row["forecast"]
            # YoY sammenligner tilvækst (netto) mod tilvækst samme periode i fjor
            row["yoy_pct"]        = round((net - ly_net) / abs(ly_net) * 100, 1) if ly_net else None
            row["vs_budget"]      = round(net - bud, 2) if bud else None
            row["vs_budget_pct"]  = round((net - bud) / bud * 100, 1) if bud else None
            row["vs_forecast"]    = round(net - fc, 2) if fc else None
            row["vs_forecast_pct"]= round((net - fc) / fc * 100, 1) if fc else None
            for k in ("won_amount", "cancel_amount", "net_amount", "last_year_net", "budget", "forecast"):
                row[k] = round(row[k], 2)
            return row

        by_brand   = [finalize_row(v) for v in sorted(brand_data.values(), key=lambda x: -x["won_amount"])]
        by_saelger = [
            finalize_row(v) for v in sorted(saelger_data.values(), key=lambda x: -x["won_amount"])
            if v["won_amount"] > 0 or v["cancel_amount"] > 0
        ] if is_manager else []

        totals = {
            "won_amount":    round(sum(r["won_amount"]    for r in by_brand), 2),
            "won_count":     sum(r["won_count"]     for r in by_brand),
            "cancel_amount": round(sum(r["cancel_amount"] for r in by_brand), 2),
            "cancel_count":  sum(r["cancel_count"]  for r in by_brand),
            "net_amount":    round(sum(r["net_amount"]    for r in by_brand), 2),
            "last_year_net": round(sum(r["last_year_net"] for r in by_brand), 2),
            "budget":        round(sum(r["budget"]        for r in by_brand), 2),
            "forecast":      round(sum(r["forecast"]      for r in by_brand), 2),
        }
        ly_t  = totals["last_year_net"]
        net_t = totals["net_amount"]
        bud_t = totals["budget"]
        fc_t  = totals["forecast"]
        totals["yoy_pct"]         = round((net_t - ly_t) / abs(ly_t) * 100, 1) if ly_t else None
        totals["vs_budget"]       = round(net_t - bud_t, 2) if bud_t else None
        totals["vs_budget_pct"]   = round((net_t - bud_t) / bud_t * 100, 1) if bud_t else None
        totals["vs_forecast"]     = round(net_t - fc_t, 2) if fc_t else None
        totals["vs_forecast_pct"] = round((net_t - fc_t) / fc_t * 100, 1) if fc_t else None

        return JSONResponse({
            "by_brand":   by_brand,
            "by_saelger": by_saelger,
            "totals":     totals,
            "is_manager": is_manager,
            "date_from":  date_from,
            "date_to":    date_to,
        })

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))


# ---------------------------------------------------------------------------
# Breakdown by dimension (sales_type / source / basis)
# ---------------------------------------------------------------------------

@router.get("/breakdown")
async def perf_breakdown(
    group_by: str,
    date_from: str,
    date_to: str,
    date_field: str = "won_time",
    include_web_sale: bool = False,
    deal_type: str = None,
    sales_type: str = None,
    brand_groups: str = None,
    user=Depends(get_current_user),
):
    if group_by not in GROUPBY_COLUMNS:
        raise HTTPException(400, f"group_by skal være én af: {', '.join(GROUPBY_COLUMNS)}")
    if date_field not in ("won_time", "service_activation_date"):
        raise HTTPException(400, "Ugyldigt date_field")

    is_manager = has_access(user, "sales_manager")
    owner_filter = None if is_manager else user["name"]
    if not is_manager:
        include_web_sale = False

    brand_list = _resolve_brand_list(brand_groups)
    ly_from, ly_to = _shift_year_back(date_from, date_to)
    col = GROUPBY_COLUMNS[group_by]

    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)

        # Won (ekskl. cancellations), valgt periode
        where, params = _build_where(
            date_field, date_from, date_to, include_web_sale, deal_type, sales_type, owner_filter,
            exclude_cancellations=True, brand_list=brand_list,
        )
        cur.execute(f"""
            SELECT COALESCE({col}, '(Ukendt)') AS dim,
                   SUM(CAST(COALESCE([value_dkk], [value]) AS DECIMAL(18,2))) AS won_amount,
                   COUNT(*) AS won_count
            FROM [dbo].[PipedriveDeals]
            {where}
            GROUP BY {col}
        """, tuple(params))
        won_rows = cur.fetchall()

        # Cancellations, valgt periode
        where_c, params_c = _build_where(
            date_field, date_from, date_to, include_web_sale, deal_type, sales_type, owner_filter,
            cancellations_only=True, brand_list=brand_list,
        )
        try:
            cur.execute(f"""
                SELECT COALESCE({col}, '(Ukendt)') AS dim,
                       ABS(SUM(CAST(COALESCE([value_dkk], [value]) AS DECIMAL(18,2)))) AS cancel_amount,
                       COUNT(*) AS cancel_count
                FROM [dbo].[PipedriveDeals]
                {where_c}
                GROUP BY {col}
            """, tuple(params_c))
            cancel_rows = cur.fetchall()
        except Exception:
            cancel_rows = []

        # Won, samme periode sidste år (YoY)
        where_ly, params_ly = _build_where(
            date_field, ly_from, ly_to, include_web_sale, deal_type, sales_type, owner_filter,
            exclude_cancellations=True, brand_list=brand_list,
        )
        cur.execute(f"""
            SELECT COALESCE({col}, '(Ukendt)') AS dim,
                   SUM(CAST(COALESCE([value_dkk], [value]) AS DECIMAL(18,2))) AS won_amount
            FROM [dbo].[PipedriveDeals]
            {where_ly}
            GROUP BY {col}
        """, tuple(params_ly))
        last_year_rows = cur.fetchall()

        # Opsigelser, samme periode sidste år (til tilvækst-YoY)
        where_lyc, params_lyc = _build_where(
            date_field, ly_from, ly_to, include_web_sale, deal_type, sales_type, owner_filter,
            cancellations_only=True, brand_list=brand_list,
        )
        try:
            cur.execute(f"""
                SELECT COALESCE({col}, '(Ukendt)') AS dim,
                       ABS(SUM(CAST(COALESCE([value_dkk], [value]) AS DECIMAL(18,2)))) AS cancel_amount
                FROM [dbo].[PipedriveDeals]
                {where_lyc}
                GROUP BY {col}
            """, tuple(params_lyc))
            last_year_cancel_rows = cur.fetchall()
        except Exception:
            last_year_cancel_rows = []

        conn.close()

        won_map       = {r["dim"]: {"won_amount": float(r["won_amount"] or 0), "won_count": int(r["won_count"] or 0)} for r in won_rows}
        cancel_map    = {r["dim"]: {"cancel_amount": float(r["cancel_amount"] or 0), "cancel_count": int(r["cancel_count"] or 0)} for r in cancel_rows}
        ly_won_map    = {r["dim"]: float(r["won_amount"] or 0) for r in last_year_rows}
        ly_cancel_map = {r["dim"]: float(r["cancel_amount"] or 0) for r in last_year_cancel_rows}

        all_dims = sorted(set(list(won_map.keys()) + list(cancel_map.keys()) + list(ly_won_map.keys()) + list(ly_cancel_map.keys())))
        rows = []
        for dim in all_dims:
            w      = won_map.get(dim,    {"won_amount": 0, "won_count": 0})
            c      = cancel_map.get(dim, {"cancel_amount": 0, "cancel_count": 0})
            ly_net = ly_won_map.get(dim, 0.0) - ly_cancel_map.get(dim, 0.0)
            net    = w["won_amount"] - c["cancel_amount"]
            yoy    = round((net - ly_net) / abs(ly_net) * 100, 1) if ly_net else None
            rows.append({
                "dim":            dim,
                "won_amount":     round(w["won_amount"], 2),
                "won_count":      w["won_count"],
                "cancel_amount":  round(c["cancel_amount"], 2),
                "cancel_count":   c["cancel_count"],
                "net_amount":     round(net, 2),
                "last_year_net":  round(ly_net, 2),
                "yoy_pct":        yoy,
            })

        rows.sort(key=lambda r: -r["won_amount"])
        return JSONResponse({"rows": rows, "group_by": group_by})

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))


# ---------------------------------------------------------------------------
# Deal drill-down API
# ---------------------------------------------------------------------------

@router.get("/deals")
async def perf_deals(
    date_from: str,
    date_to: str,
    date_field: str = "won_time",
    include_web_sale: bool = False,
    deal_type: str = None,
    sales_type: str = None,
    brand: str = None,
    owner_name: str = None,
    source: str = None,
    basis: str = None,
    cancellations_only: bool = False,
    exclude_cancellations: bool = False,
    brand_groups: str = None,
    user=Depends(get_current_user),
):
    if date_field not in ("won_time", "service_activation_date"):
        raise HTTPException(400, "Ugyldigt date_field")

    is_manager = has_access(user, "sales_manager")

    # Salesperson tvinges til egne deals
    if not is_manager:
        owner_name = user["name"]
        include_web_sale = False

    deal_brand_list = _resolve_brand_list(brand_groups)
    date_expr = _date_expr(date_field)
    where, params = _build_where(
        date_field, date_from, date_to, include_web_sale, deal_type, sales_type,
        owner_name, cancellations_only=cancellations_only,
        exclude_cancellations=exclude_cancellations,
        source=source, basis=basis, brand_list=deal_brand_list,
    )

    if brand:
        where += " AND [sites] = %s"
        params.append(brand)

    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute(f"""
            SELECT TOP 500
                [title],
                COALESCE([owner_name], 'Ukendt') AS owner_name,
                COALESCE([sites], 'Ukendt') AS brand,
                CAST(COALESCE([value_dkk], [value]) AS DECIMAL(18,2)) AS value,
                CONVERT(NVARCHAR(10), {date_expr}, 23) AS deal_date,
                [deal_type],
                [sales_type],
                [pipeline_name],
                CASE WHEN [pipeline_name] IN ('Cancellation','Cancellations','Opsigelser')
                     THEN 1 ELSE 0 END AS is_cancellation
            FROM [dbo].[PipedriveDeals]
            {where}
            ORDER BY {date_expr} DESC
        """, tuple(params))
        rows = cur.fetchall()
        conn.close()

        deals = []
        total = 0.0
        for r in rows:
            v = float(r["value"] or 0)
            total += v
            deals.append({
                "title":          r["title"] or "(Uden titel)",
                "owner_name":     r["owner_name"],
                "brand":          r["brand"],
                "value":          v,
                "date":           r["deal_date"],
                "deal_type":      r["deal_type"],
                "sales_type":     r["sales_type"],
                "pipeline_name":  r["pipeline_name"],
                "is_cancellation": bool(r["is_cancellation"]),
            })

        return JSONResponse({"deals": deals, "total": round(total, 2), "count": len(deals)})

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))


# ---------------------------------------------------------------------------
# Overview dashboard data (Power BI-style)
# ---------------------------------------------------------------------------

MONTH_NAMES_DA = ["Januar","Februar","Marts","April","Maj","Juni",
                  "Juli","August","September","Oktober","November","December"]

def _budget_range(period_from: date, period_to_excl: date):
    """Udvid en periode til komplette måneder til budgetopslag."""
    incl = period_to_excl - timedelta(days=1)
    start = date(period_from.year, period_from.month, 1)
    if incl.month == 12:
        end = date(incl.year + 1, 1, 1)
    else:
        end = date(incl.year, incl.month + 1, 1)
    return start, end


@router.get("/overview-data")
async def perf_overview_data(user=Depends(get_current_user)):
    if not has_access(user, "sales_manager"):
        raise HTTPException(403, "Ingen adgang")

    today = date.today()

    # ── Periodeberegning ──────────────────────────────────────────────────────
    day_from   = today
    day_to     = today + timedelta(days=1)

    wd         = today.weekday()   # 0=Man
    week_from  = today - timedelta(days=wd)
    week_to    = week_from + timedelta(days=7)

    m          = today.month
    month_from = date(today.year, m, 1)
    month_to   = date(today.year + (m // 12), m % 12 + 1, 1)

    year_from  = date(today.year, 1, 1)
    year_to    = date(today.year + 1, 1, 1)

    q          = (m - 1) // 3
    q_start    = q * 3 + 1
    qtr_from   = date(today.year, q_start, 1)
    q_end      = q_start + 3
    qtr_to     = date(today.year + (q_end > 12), (q_end - 1) % 12 + 1, 1)

    brands_ph  = "(" + ",".join(["%s"] * len(SUBSCRIPTION_BRANDS)) + ")"

    try:
        conn = get_conn()
        cur  = conn.cursor(as_dict=True)

        def fetch_rev(pfrom, pto):
            # Omsætning (Won) — baseret på won_time, ingen opsigelser
            cur.execute(f"""
                SELECT ISNULL(SUM(CASE WHEN [cancellation] IS NULL OR [cancellation]=''
                    THEN CAST(COALESCE([value_dkk], [value]) AS DECIMAL(18,2)) ELSE 0 END),0) AS won
                FROM [dbo].[PipedriveDeals]
                WHERE [status]='won' AND [pipeline_name]<>'Web Sale'
                  AND [won_time]>=%s AND [won_time]<%s
                  AND [sites] IN {brands_ph}
            """, (pfrom.isoformat(), pto.isoformat()) + tuple(SUBSCRIPTION_BRANDS))
            won = float((cur.fetchone() or {}).get("won", 0) or 0)

            # Tilvækst (Net) — baseret på service_activation_date (fallback: won_time)
            cur.execute(f"""
                SELECT
                    ISNULL(SUM(CASE WHEN [cancellation] IS NULL OR [cancellation]=''
                        THEN CAST([value] AS DECIMAL(18,2)) ELSE 0 END),0) AS won_sad,
                    ISNULL(SUM(CASE WHEN [cancellation] IS NOT NULL AND [cancellation]<>''
                        THEN CAST([value] AS DECIMAL(18,2)) ELSE 0 END),0) AS cancel
                FROM [dbo].[PipedriveDeals]
                WHERE [status]='won' AND [pipeline_name]<>'Web Sale'
                  AND COALESCE([service_activation_date],[won_time])>=%s
                  AND COALESCE([service_activation_date],[won_time])<%s
                  AND [sites] IN {brands_ph}
            """, (pfrom.isoformat(), pto.isoformat()) + tuple(SUBSCRIPTION_BRANDS))
            r2     = cur.fetchone() or {}
            cancel = float(r2.get("cancel", 0) or 0)
            net    = float(r2.get("won_sad", 0) or 0) - cancel
            return {"won": round(won, 2), "cancel": round(cancel, 2), "net": round(net, 2)}

        rev_dag    = fetch_rev(day_from,  day_to)
        rev_uge    = fetch_rev(week_from, week_to)
        rev_maaned = fetch_rev(month_from,month_to)
        rev_aar    = fetch_rev(year_from, year_to)

        # ── Team-fordeling ────────────────────────────────────────────────────
        def fetch_team_perf(pfrom, pto):
            bud_from, bud_to = _budget_range(pfrom, pto)
            # Brug Hub's TeamMemberships → Teams til team-attribution.
            # For sælgere i flere teams: match site-brand til team-brand via OUTER APPLY.
            # Tilvækst baseret på service_activation_date (fallback: won_time).
            # PersonTeams inkluderer ALLE historiske memberships (ikke filtreret på GETDATE()),
            # så deals matches mod det hold sælgeren var i PÅ DEALENS DATO.
            cur.execute(f"""
                WITH PersonTeams AS (
                    SELECT u.name AS owner_name,
                           t.name AS team_name,
                           t.brand AS team_brand,
                           tm.start_date AS mem_start,
                           tm.end_date   AS mem_end
                    FROM HubUsers u
                    JOIN TeamMemberships tm ON tm.user_id = u.id
                    JOIN Teams t ON t.id = tm.team_id
                ),
                SiteTagged AS (
                    SELECT
                        pd.owner_name, pd.value, pd.cancellation,
                        COALESCE(pd.service_activation_date, pd.won_time) AS deal_date,
                        CASE
                            WHEN pd.sites LIKE '%onitor%'    THEN 'monitor'
                            WHEN pd.sites = 'FINANS DK'      THEN 'finans'
                            -- Watch SE
                            WHEN pd.sites LIKE '%Watch%' AND pd.sites LIKE '% SE' THEN 'watch_se'
                            -- Watch NO
                            WHEN pd.sites LIKE '%Watch%' AND pd.sites LIKE '% NO' THEN 'watch_no'
                            -- Watch DE
                            WHEN pd.sites LIKE '%Watch%' AND pd.sites LIKE '% DE' THEN 'watch_de'
                            -- Watch DK (alle andre Watch-sites)
                            WHEN pd.sites LIKE '%Watch%'     THEN 'watch_dk'
                            -- FINANS-brands ekskl. FINANS DK
                            WHEN pd.sites LIKE '%FINANS%'
                              OR pd.sites LIKE '%Finans%'    THEN 'finans_int'
                            WHEN pd.sites LIKE '%arketWire%' THEN 'marketwire'
                            ELSE NULL
                        END AS site_brand
                    FROM [dbo].[PipedriveDeals] pd
                    WHERE pd.status='won' AND pd.pipeline_name<>'Web Sale'
                      AND COALESCE(pd.service_activation_date, pd.won_time) >= %s
                      AND COALESCE(pd.service_activation_date, pd.won_time) < %s
                      AND pd.sites IN {brands_ph}
                ),
                DealWithTeam AS (
                    SELECT
                        st.value, st.cancellation,
                        COALESCE(
                            pt_match.team_name,   -- 1. Præcis brand-match (f.eks. watch_dk) på dealens dato
                            pt_catch.team_name,   -- 2. Tværgående hold uden brand på dealens dato
                            pt_any.team_name      -- 3. Fallback: kategori-match, så alfabetisk
                        ) AS team
                    FROM SiteTagged st
                    OUTER APPLY (
                        -- Præcis match: sælgerens hold-brand = site-brand, aktivt på dealens dato
                        SELECT TOP 1 pt.team_name
                        FROM PersonTeams pt
                        WHERE pt.owner_name = st.owner_name
                          AND pt.team_brand = st.site_brand
                          AND (pt.mem_start IS NULL OR pt.mem_start <= st.deal_date)
                          AND (pt.mem_end   IS NULL OR pt.mem_end   >= st.deal_date)
                    ) pt_match
                    OUTER APPLY (
                        -- Catch-all: hold uden brand (tværgående hold), aktivt på dealens dato
                        SELECT TOP 1 pt2.team_name
                        FROM PersonTeams pt2
                        WHERE pt2.owner_name = st.owner_name
                          AND (pt2.team_brand IS NULL OR pt2.team_brand = '')
                          AND (pt2.mem_start IS NULL OR pt2.mem_start <= st.deal_date)
                          AND (pt2.mem_end   IS NULL OR pt2.mem_end   >= st.deal_date)
                    ) pt_catch
                    OUTER APPLY (
                        -- Fallback: hold aktivt på dealens dato — foretrækker kategori-match
                        -- (Watch-hold for Watch-sites, Finans-hold for Finans-sites) frem for alfabetisk
                        SELECT TOP 1 pt3.team_name
                        FROM PersonTeams pt3
                        WHERE pt3.owner_name = st.owner_name
                          AND (pt3.mem_start IS NULL OR pt3.mem_start <= st.deal_date)
                          AND (pt3.mem_end   IS NULL OR pt3.mem_end   >= st.deal_date)
                        ORDER BY
                            CASE
                                WHEN st.site_brand LIKE 'watch%'   AND pt3.team_name LIKE '%Watch%'  THEN 0
                                WHEN st.site_brand LIKE 'finans%'  AND pt3.team_name LIKE '%Finans%' THEN 0
                                WHEN st.site_brand = 'monitor'     AND pt3.team_name LIKE '%Monitor%' THEN 0
                                WHEN st.site_brand = 'marketwire'  AND pt3.team_name LIKE '%Market%'  THEN 0
                                ELSE 1
                            END,
                            pt3.team_name
                    ) pt_any
                )
                SELECT
                    ISNULL(team, 'Ukendt') AS team,
                    ISNULL(SUM(CASE WHEN cancellation IS NULL OR cancellation=''
                        THEN CAST(value AS DECIMAL(18,2)) ELSE 0 END),0) AS won,
                    ISNULL(SUM(CASE WHEN cancellation IS NOT NULL AND cancellation<>''
                        THEN CAST(value AS DECIMAL(18,2)) ELSE 0 END),0) AS cancel
                FROM DealWithTeam
                GROUP BY ISNULL(team, 'Ukendt')
            """, (pfrom.isoformat(), pto.isoformat()) + tuple(SUBSCRIPTION_BRANDS))
            deal_rows = {r["team"]: r for r in cur.fetchall()}

            cur.execute("""
                SELECT Team, SUM(BudgetAmount) AS budget
                FROM [dbo].[SalesPersonBudget]
                WHERE BudgetDate >= %s AND BudgetDate < %s
                GROUP BY Team
            """, (bud_from.isoformat(), bud_to.isoformat()))
            bud_map = {r["Team"]: float(r["budget"] or 0) for r in cur.fetchall()}

            teams = sorted(set(list(deal_rows.keys()) + list(bud_map.keys())))
            result = []
            for t in teams:
                r   = deal_rows.get(t, {"won": 0, "cancel": 0})
                won = float(r["won"] or 0)
                can = float(r["cancel"] or 0)
                net = round(won - can, 2)
                bud = bud_map.get(t, 0.0)
                result.append({"team": t, "won": round(won,2), "cancel": round(can,2),
                                "net": net, "budget": round(bud,2)})
            return result

        team_quarter = fetch_team_perf(qtr_from,   qtr_to)
        team_month   = fetch_team_perf(month_from,  month_to)

        # ── Per-sælger denne måned ────────────────────────────────────────────
        cur.execute(f"""
            SELECT owner_name,
                COUNT(CASE WHEN cancellation IS NULL OR cancellation='' THEN 1 END)   AS won_count,
                ISNULL(SUM(CASE WHEN cancellation IS NULL OR cancellation=''
                    THEN CAST(value AS DECIMAL(18,2)) ELSE 0 END), 0)                 AS won_amount,
                COUNT(CASE WHEN cancellation IS NOT NULL AND cancellation<>'' THEN 1 END) AS cancel_count
            FROM [dbo].[PipedriveDeals]
            WHERE status='won' AND pipeline_name<>'Web Sale'
              AND won_time>=%s AND won_time<%s
              AND sites IN {brands_ph}
            GROUP BY owner_name
        """, (month_from.isoformat(), month_to.isoformat()) + tuple(SUBSCRIPTION_BRANDS))
        saelger_month = sorted(cur.fetchall(), key=lambda r: -(r["won_count"] or 0))

        saelger_won     = [{"name": r["owner_name"], "count": r["won_count"] or 0,
                             "revenue": round(float(r["won_amount"] or 0), 2)}
                            for r in saelger_month]

        # ── Deals oprettet denne måned (valgfri kolonne add_time) ─────────────
        try:
            cur.execute(f"""
                SELECT owner_name, COUNT(*) AS cnt
                FROM [dbo].[PipedriveDeals]
                WHERE add_time>=%s AND add_time<%s
                  AND pipeline_name<>'Web Sale'
                  AND sites IN {brands_ph}
                GROUP BY owner_name
                ORDER BY cnt DESC
            """, (month_from.isoformat(), month_to.isoformat()) + tuple(SUBSCRIPTION_BRANDS))
            saelger_created = [{"name": r["owner_name"], "count": r["cnt"]} for r in cur.fetchall()]
        except Exception:
            saelger_created = []

        # ── Seneste vundne deals (uden opsigelser) ────────────────────────────
        try:
            cur.execute(f"""
                SELECT TOP 25
                    pd.owner_name,
                    ISNULL(pd.org_name,'') AS org_name,
                    ISNULL(ot.team_name,'') AS team,
                    ISNULL(pd.sites,'')    AS sites,
                    CAST(pd.value AS DECIMAL(18,2)) AS deal_value,
                    CONVERT(NVARCHAR(19), pd.won_time, 120) AS won_dt
                FROM [dbo].[PipedriveDeals] pd
                OUTER APPLY (
                    -- Vælg det hold sælgeren var i PÅ won_time-datoen.
                    -- Foretrækker kategori-match (Watch-hold for Watch-sites) frem for alfabetisk.
                    SELECT TOP 1 t.name AS team_name
                    FROM HubUsers u
                    JOIN TeamMemberships tm ON tm.user_id = u.id
                        AND (tm.start_date IS NULL OR tm.start_date <= pd.won_time)
                        AND (tm.end_date   IS NULL OR tm.end_date   >= pd.won_time)
                    JOIN Teams t ON t.id = tm.team_id
                    WHERE u.name = pd.owner_name
                    ORDER BY
                        CASE
                            WHEN pd.sites LIKE '%Watch%'   AND t.name LIKE '%Watch%'  THEN 0
                            WHEN pd.sites LIKE '%FINANS%'  AND t.name LIKE '%Finans%' THEN 0
                            WHEN pd.sites LIKE '%Finans%'  AND t.name LIKE '%Finans%' THEN 0
                            WHEN pd.sites LIKE '%onitor%'  AND t.name LIKE '%Monitor%' THEN 0
                            ELSE 1
                        END,
                        t.name
                ) ot
                WHERE pd.status='won' AND pd.pipeline_name<>'Web Sale'
                  AND (pd.cancellation IS NULL OR pd.cancellation='')
                  AND pd.sites IN {brands_ph}
                ORDER BY pd.won_time DESC
            """, tuple(SUBSCRIPTION_BRANDS))
            recent_deals = [{"owner": r["owner_name"] or "",
                              "org":   r["org_name"] or "",
                              "team":  r["team"] or "",
                              "site":  r["sites"] or "",
                              "value": round(float(r["deal_value"] or 0), 2),
                              "won":   r["won_dt"] or ""}
                            for r in cur.fetchall()]
        except Exception:
            traceback.print_exc()
            recent_deals = []

        conn.close()

        return JSONResponse({
            "today":    today.isoformat(),
            "periods":  {
                "qtr_label":   f"Q{q+1} {today.year}",
                "month_label": f"{MONTH_NAMES_DA[m-1]} {today.year}",
            },
            "revenue":       {"dag": rev_dag, "uge": rev_uge,
                              "maaned": rev_maaned, "aar": rev_aar},
            "team_quarter":  team_quarter,
            "team_month":    team_month,
            "saelger_won":   saelger_won,
            "saelger_created": saelger_created,
            "recent_deals":  recent_deals,
        })

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))

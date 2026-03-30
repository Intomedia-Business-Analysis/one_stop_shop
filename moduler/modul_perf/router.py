import traceback
from datetime import date, timedelta

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from auth import ROLE_LABELS, get_current_user, has_access
from moduler.modul_perf.queries import (
    SUBSCRIPTION_BRANDS, BRAND_GROUPS, BRAND_GROUP_LABELS, GROUPBY_COLUMNS,
    CANCELLATION_PIPELINES, DEAL_TYPE_ALIASES, DEAL_TYPE_CANONICAL, MONTH_NAMES_DA,
    resolve_brand_list, date_expr, shift_year_back, budget_range, build_where,
    db_get_filters, db_perf_data, db_breakdown, db_deals, db_overview_data,
    db_manager_data,
)

router = APIRouter(prefix="/tools/performance", tags=["Performance"])
templates = Jinja2Templates(directory="templates")
templates.env.globals["ROLE_LABELS"] = ROLE_LABELS


@router.get("/", response_class=HTMLResponse)
async def perf_dashboard(request: Request, user=Depends(get_current_user)):
    today = date.today()
    return templates.TemplateResponse("perf_dashboard.html", {
        "request":       request,
        "user":          user,
        "years":         list(range(today.year - 3, today.year + 1)),
        "current_year":  today.year,
        "current_month": today.month,
        "current_day":   today.day,
        "is_manager":    has_access(user, "sales_manager"),
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


@router.get("/filters")
async def perf_filters(user=Depends(get_current_user)):
    return JSONResponse(db_get_filters())


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

    is_manager   = has_access(user, "sales_manager")
    owner_filter = None if is_manager else user["name"]
    if not is_manager:
        include_web_sale = False

    brand_list = resolve_brand_list(brand_groups)

    _df = date.fromisoformat(date_from)
    _dt = date.fromisoformat(date_to) - timedelta(days=1)
    budget_from    = date(_df.year, _df.month, 1)
    budget_to_excl = date(_dt.year + (_dt.month // 12), _dt.month % 12 + 1, 1)
    budget_year    = _df.year

    try:
        (won_rows, cancel_rows, last_year_rows, last_year_cancel_rows,
         brand_budget_rows, saelger_budget_rows,
         brand_forecast_rows, saelger_forecast_rows) = db_perf_data(
            date_from, date_to, date_field, include_web_sale,
            deal_type, sales_type, brand_list, is_manager,
            owner_filter, brand_groups, budget_from, budget_to_excl, budget_year
        )

        won_map        = {}
        cancel_map     = {}
        ly_won_map     = {}
        ly_cancel_map  = {}

        for r in won_rows:
            k = (r["brand"], r["owner_name"])
            won_map[k] = {"won_amount": float(r["won_amount"] or 0), "won_count": int(r["won_count"] or 0)}
        for r in cancel_rows:
            k = (r["brand"], r["owner_name"])
            cancel_map[k] = {"cancel_amount": float(r["cancel_amount"] or 0), "cancel_count": int(r["cancel_count"] or 0)}
        for r in last_year_rows:
            k = (r["brand"], r["owner_name"])
            ly_won_map[k] = float(r["won_amount"] or 0)
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

        brand_data   = {}
        saelger_data = {}

        for (brand, owner) in all_keys:
            w      = won_map.get((brand, owner),    {"won_amount": 0, "won_count": 0})
            c      = cancel_map.get((brand, owner), {"cancel_amount": 0, "cancel_count": 0})
            ly_net = ly_won_map.get((brand, owner), 0.0) - ly_cancel_map.get((brand, owner), 0.0)
            net    = w["won_amount"] - c["cancel_amount"]

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
            row["yoy_pct"]         = round((net - ly_net) / abs(ly_net) * 100, 1) if ly_net else None
            row["vs_budget"]       = round(net - bud, 2) if bud else None
            row["vs_budget_pct"]   = round((net - bud) / bud * 100, 1) if bud else None
            row["vs_forecast"]     = round(net - fc, 2) if fc else None
            row["vs_forecast_pct"] = round((net - fc) / fc * 100, 1) if fc else None
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
        totals["yoy_pct"]         = round((net_t - ly_t)  / abs(ly_t)  * 100, 1) if ly_t  else None
        totals["vs_budget"]       = round(net_t - bud_t, 2) if bud_t else None
        totals["vs_budget_pct"]   = round((net_t - bud_t) / bud_t * 100, 1) if bud_t else None
        totals["vs_forecast"]     = round(net_t - fc_t, 2) if fc_t else None
        totals["vs_forecast_pct"] = round((net_t - fc_t)  / fc_t  * 100, 1) if fc_t  else None

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

    is_manager   = has_access(user, "sales_manager")
    owner_filter = None if is_manager else user["name"]
    if not is_manager:
        include_web_sale = False

    brand_list = resolve_brand_list(brand_groups)

    try:
        won_rows, cancel_rows, last_year_rows, last_year_cancel_rows = db_breakdown(
            group_by, date_from, date_to, date_field, include_web_sale,
            deal_type, sales_type, owner_filter, brand_list
        )

        won_map       = {r["dim"]: {"won_amount": float(r["won_amount"] or 0), "won_count": int(r["won_count"] or 0)} for r in won_rows}
        cancel_map    = {r["dim"]: {"cancel_amount": float(r["cancel_amount"] or 0), "cancel_count": int(r["cancel_count"] or 0)} for r in cancel_rows}
        ly_won_map    = {r["dim"]: float(r["won_amount"] or 0) for r in last_year_rows}
        ly_cancel_map = {r["dim"]: float(r["cancel_amount"] or 0) for r in last_year_cancel_rows}

        all_dims = sorted(set(
            list(won_map.keys()) + list(cancel_map.keys()) +
            list(ly_won_map.keys()) + list(ly_cancel_map.keys())
        ))
        rows = []
        for dim in all_dims:
            w      = won_map.get(dim,    {"won_amount": 0, "won_count": 0})
            c      = cancel_map.get(dim, {"cancel_amount": 0, "cancel_count": 0})
            ly_net = ly_won_map.get(dim, 0.0) - ly_cancel_map.get(dim, 0.0)
            net    = w["won_amount"] - c["cancel_amount"]
            rows.append({
                "dim":           dim,
                "won_amount":    round(w["won_amount"], 2),
                "won_count":     w["won_count"],
                "cancel_amount": round(c["cancel_amount"], 2),
                "cancel_count":  c["cancel_count"],
                "net_amount":    round(net, 2),
                "last_year_net": round(ly_net, 2),
                "yoy_pct":       round((net - ly_net) / abs(ly_net) * 100, 1) if ly_net else None,
            })
        rows.sort(key=lambda r: -r["won_amount"])
        return JSONResponse({"rows": rows, "group_by": group_by})

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))


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
    if not is_manager:
        owner_name       = user["name"]
        include_web_sale = False

    brand_list = resolve_brand_list(brand_groups)
    where, params = build_where(
        date_field, date_from, date_to, include_web_sale, deal_type, sales_type,
        owner_name, cancellations_only=cancellations_only,
        exclude_cancellations=exclude_cancellations,
        source=source, basis=basis, brand_list=brand_list,
    )
    if brand:
        where += " AND [sites] = %s"
        params.append(brand)

    try:
        rows = db_deals(where, params, date_field)
        deals = []
        total = 0.0
        for r in rows:
            v = float(r["value"] or 0)
            total += v
            deals.append({
                "title":           r["title"] or "(Uden titel)",
                "owner_name":      r["owner_name"],
                "brand":           r["brand"],
                "value":           v,
                "date":            r["deal_date"],
                "deal_type":       r["deal_type"],
                "sales_type":      r["sales_type"],
                "pipeline_name":   r["pipeline_name"],
                "is_cancellation": bool(r["is_cancellation"]),
            })
        return JSONResponse({"deals": deals, "total": round(total, 2), "count": len(deals)})

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))


@router.get("/overview-data")
async def perf_overview_data(user=Depends(get_current_user)):
    if not has_access(user, "sales_manager"):
        raise HTTPException(403, "Ingen adgang")
    try:
        return JSONResponse(db_overview_data(date.today()))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))


#----------------------------------------------------------------------------------------------------------------------
#                                                   DET NYE DASHBOARD
#----------------------------------------------------------------------------------------------------------------------
@router.get("/manager", response_class=HTMLResponse)
async def perf_manager_page(request: Request, user=Depends(get_current_user)):
    if not has_access(user, "sales_manager"):
        raise HTTPException(403, "Kun Sales Managers og derover har adgang")
    today = date.today()
    return templates.TemplateResponse("perf_manager.html", {
        "request":       request,
        "user":          user,
        "current_year":  today.year,
        "current_month": today.month,
        "current_day":   today.day,
    })

@router.get("/manager-data")
async def perf_manager_data(
    team: str | None = None,
    user=Depends(get_current_user)
):
    if not has_access(user, "sales_manager"):
        raise HTTPException(403, "Ingen adgang")
    try:
        return JSONResponse(db_manager_data(date.today(), team=team))
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))
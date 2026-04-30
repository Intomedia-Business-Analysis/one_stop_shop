import traceback

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse

from auth import ROLE_RANK, get_current_user
from moduler.modul_barsel.queries import (
    get_settings, upsert_settings,
    get_cases, create_case, update_case, delete_case,
)

router = APIRouter(prefix="/tools/barsel", tags=["Barsel"])


def _can_see_all(user: dict) -> bool:
    """
    Brugere med admin-rolle kan se alle sager.
    Alle andre ser kun de sager, de selv har oprettet.
    """
    return user["role"] == "management"

# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------

@router.get("/api/settings")
async def api_get_settings(user=Depends(get_current_user)):
    return JSONResponse(get_settings())


@router.post("/api/settings")
async def api_save_settings(request: Request, user=Depends(get_current_user)):
    try:
        data = await request.json()
        upsert_settings(data, user["id"])
        return JSONResponse({"status": "ok"})
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

# ---------------------------------------------------------------------------
# Cases
# ---------------------------------------------------------------------------

@router.get("/api/cases")
async def api_get_cases(user=Depends(get_current_user)):
    see_all = _can_see_all(user)
    cases = get_cases(user["id"], see_all)
    return JSONResponse({"cases": cases, "seeAll": see_all})


@router.post("/api/cases")
async def api_create_case(request: Request, user=Depends(get_current_user)):
    try:
        data = await request.json()
        new_id = create_case(data, user["id"])
        return JSONResponse({"id": new_id})
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/api/cases/{case_id}")
async def api_update_case(case_id: int, request: Request, user=Depends(get_current_user)):
    try:
        data = await request.json()
        see_all = _can_see_all(user)
        update_case(case_id, data, user["id"], see_all)
        return JSONResponse({"status": "ok"})
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/api/cases/{case_id}")
async def api_delete_case(case_id: int, user=Depends(get_current_user)):
    try:
        see_all = _can_see_all(user)
        delete_case(case_id, user["id"], see_all)
        return JSONResponse({"status": "ok"})
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

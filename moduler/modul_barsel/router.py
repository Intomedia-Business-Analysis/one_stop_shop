import logging

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse

from auth import get_current_user
from moduler.modul_barsel.queries import (
    get_settings, upsert_settings,
    get_cases, get_case, create_case, update_case, delete_case,
    user_can_access_case, user_can_approve_case, set_approval_status,
    list_hub_users,
)
from moduler.modul_barsel.mail import send_approval_notification

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/tools/barsel", tags=["Barsel"])


def _can_see_all(user: dict) -> bool:
    """Admin og management ser alle sager. Andre ser kun deres egne /
    deres medarbejderes."""
    return user["role"] in ("admin", "management")


def _is_admin(user: dict) -> bool:
    return user["role"] == "admin"


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------

@router.get("/api/settings")
async def api_get_settings(user=Depends(get_current_user)):
    data = get_settings()
    data["canEdit"] = _is_admin(user)
    return JSONResponse(data)


@router.post("/api/settings")
async def api_save_settings(request: Request, user=Depends(get_current_user)):
    if not _is_admin(user):
        raise HTTPException(status_code=403, detail="Kun admin kan ændre indstillinger")
    try:
        data = await request.json()
        upsert_settings(data, user["id"])
        return JSONResponse({"status": "ok"})
    except Exception:
        logger.exception("api_save_settings fejlede")
        raise HTTPException(status_code=500, detail="Data kunne ikke hentes")

# ---------------------------------------------------------------------------
# Cases
# ---------------------------------------------------------------------------

@router.get("/api/cases")
async def api_get_cases(user=Depends(get_current_user)):
    see_all = _can_see_all(user)
    cases = get_cases(user["id"], see_all)
    # Markér hvilke sager den aktuelle bruger kan godkende (frontend bruger
    # dette til at vise/skjule godkend-/afvis-knapperne).
    for c in cases:
        c["canApprove"] = see_all or (
            c.get("hubUserManagerId") is not None
            and c["hubUserManagerId"] == user["id"]
        )
    return JSONResponse({
        "cases":     cases,
        "seeAll":    see_all,
        "isAdmin":   _is_admin(user),
        "userId":    user["id"],
        "userRole":  user["role"],
    })


@router.get("/api/hub-users")
async def api_list_hub_users(user=Depends(get_current_user)):
    """Liste over aktive HubUsers til medarbejder-dropdown.

    Begrænset til management/admin — almindelige brugere skal ikke kunne
    enumere alle ansatte gennem dette endpoint.
    """
    if not _can_see_all(user):
        return JSONResponse({"users": []})
    return JSONResponse({"users": list_hub_users()})


@router.post("/api/cases")
async def api_create_case(request: Request, user=Depends(get_current_user)):
    try:
        data = await request.json()
        new_id = create_case(data, user["id"])
        return JSONResponse({"id": new_id})
    except Exception:
        logger.exception("api_create_case fejlede")
        raise HTTPException(status_code=500, detail="Data kunne ikke hentes")


@router.put("/api/cases/{case_id}")
async def api_update_case(case_id: int, request: Request, user=Depends(get_current_user)):
    if not user_can_access_case(user, case_id):
        raise HTTPException(status_code=403, detail="Ingen adgang til denne sag")
    try:
        data = await request.json()
        update_case(case_id, data, user)
        return JSONResponse({"status": "ok"})
    except Exception:
        logger.exception("api_update_case fejlede")
        raise HTTPException(status_code=500, detail="Data kunne ikke hentes")


@router.delete("/api/cases/{case_id}")
async def api_delete_case(case_id: int, user=Depends(get_current_user)):
    if not user_can_access_case(user, case_id):
        raise HTTPException(status_code=403, detail="Ingen adgang til denne sag")
    try:
        delete_case(case_id)
        return JSONResponse({"status": "ok"})
    except Exception:
        logger.exception("api_delete_case fejlede")
        raise HTTPException(status_code=500, detail="Data kunne ikke hentes")


# ---------------------------------------------------------------------------
# Godkendelses-flow
# ---------------------------------------------------------------------------

@router.post("/api/cases/{case_id}/submit")
async def api_submit_case(case_id: int, user=Depends(get_current_user)):
    """Medarbejder/HR indsender plan til godkendelse."""
    if not user_can_access_case(user, case_id):
        raise HTTPException(status_code=403, detail="Ingen adgang til denne sag")
    set_approval_status(case_id, "pending", None)
    return JSONResponse({"status": "ok"})


@router.post("/api/cases/{case_id}/approve")
async def api_approve_case(case_id: int, user=Depends(get_current_user)):
    """Nærmeste leder (eller admin/management) godkender."""
    if not user_can_approve_case(user, case_id):
        raise HTTPException(status_code=403, detail="Du kan ikke godkende denne sag")
    set_approval_status(case_id, "approved", user["id"])
    # Send notifikation til distributionslisten. send_approval_notification
    # fejler aldrig hårdt — godkendelsen står ved magt selv hvis mailen
    # ikke kan afsendes (fejlen logges i stedet).
    case = get_case(case_id)
    settings = get_settings()
    mail_sent = send_approval_notification(case or {}, settings.get("notifyEmails") or "")
    return JSONResponse({"status": "ok", "mailSent": mail_sent})


@router.post("/api/cases/{case_id}/reject")
async def api_reject_case(case_id: int, user=Depends(get_current_user)):
    if not user_can_approve_case(user, case_id):
        raise HTTPException(status_code=403, detail="Du kan ikke afvise denne sag")
    set_approval_status(case_id, "rejected", None)
    return JSONResponse({"status": "ok"})


@router.post("/api/cases/{case_id}/reopen")
async def api_reopen_case(case_id: int, user=Depends(get_current_user)):
    """Trækker en indsendt/godkendt plan tilbage til kladde."""
    if not user_can_access_case(user, case_id):
        raise HTTPException(status_code=403, detail="Ingen adgang til denne sag")
    set_approval_status(case_id, "draft", None)
    return JSONResponse({"status": "ok"})


# ---------------------------------------------------------------------------
# Eksporter enkelt sag (returnerer rå data — frontend bygger CSV'en)
# ---------------------------------------------------------------------------

@router.get("/api/cases/{case_id}")
async def api_get_case(case_id: int, user=Depends(get_current_user)):
    if not user_can_access_case(user, case_id):
        raise HTTPException(status_code=403, detail="Ingen adgang til denne sag")
    case = get_case(case_id)
    if not case:
        raise HTTPException(status_code=404, detail="Sag ikke fundet")
    return JSONResponse(case)

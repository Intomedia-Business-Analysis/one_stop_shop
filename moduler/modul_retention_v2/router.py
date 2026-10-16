"""Retention v2: Arbejdsliste (én side, HTMX).

Backend regner og gemmer, frontend viser (fase 2, logikplacering):
- beregning.py regner X og Y ud fra CSV'erne
- denne fil henter seneste farve og org-navn fra SQL, flette dem på listerne,
  og gemmer Christines registreringer i dbo.RetentionOutcomesV2 og Pipedrive
- skabelonerne viser kun, HTMX henter færdig HTML

v1 (modul_retention) er urørt og importeres ikke herfra.
"""

import datetime as dt
import html
import logging
import os

import pandas as pd
from fastapi import APIRouter, Depends, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from auth import get_current_user, resolve_resource_access
from db import get_conn
from env import load_env
from nav_utils import register_nav_globals
from os_trust import session

from . import beregning

load_env()
logger = logging.getLogger(__name__)

templates = Jinja2Templates(directory="templates")
register_nav_globals(templates)
templates.env.filters["dk"] = lambda n: f"{int(n):,}".replace(",", ".")

router = APIRouter()

# Samme id i nav_utils.CATEGORIES og her: resolve_resource_access slår op på
# præcis dette id. Sales Operations og admin, ikke marketing og management
# (besluttet 2026-08-10 for retention).
RES_ID = "retention-v2"
MIN_ROLLE = "sales_operations"
EKSKLUDEREDE_ROLLER = ["marketing", "management"]

BRANDS = ["Watch", "Finans"]
FARVER = ["rød", "gul", "grøn"]
FARVE_KLASSE = {"rød": "rod", "gul": "gul", "grøn": "gron"}
# Opfølgning beregnes ud fra oprettet og gemmes ikke. Grøn skal ikke følges op.
OPFOELGNING_DAGE = {"rød": 30, "gul": 60}

# Standard i filteret (Kenneth 15-10): de 10 % mest faldende, og nystartede
# uden visninger.
X_STANDARD = 10
Y_STANDARD = 0

# Finans og Watch ligger i samme Pipedrive, så ét token dækker begge.
PIPEDRIVE_TOKEN = "PD_TOKEN_WATCH_DK_FINANS"
PIPEDRIVE_URL = "https://api.pipedrive.com/api/v2/activities"


def _kraev_adgang(user: dict) -> None:
    if resolve_resource_access(user, RES_ID, MIN_ROLLE, exclude_roles=EKSKLUDEREDE_ROLLER) == "none":
        raise HTTPException(403, "Ingen adgang til Arbejdslisten")


def _dev() -> bool:
    # DEV_MODE springer kun login over, ikke SQL. Lokalt er der ingen database,
    # og et klik på Gem må aldrig ramme det rigtige Pipedrive.
    return os.getenv("DEV_MODE") == "1"


# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

def _seneste_farver() -> dict:
    """{(brand, pipedrive_id): (farve, dato)}: kundens seneste registrering."""
    if _dev():
        return {}
    conn = get_conn()
    try:
        cur = conn.cursor(as_dict=True)
        cur.execute("""
            SELECT brand, pipedrive_id, farve, oprettet FROM (
                SELECT brand, pipedrive_id, farve, oprettet,
                       ROW_NUMBER() OVER (PARTITION BY brand, pipedrive_id
                                          ORDER BY oprettet DESC, id DESC) AS rn
                FROM dbo.RetentionOutcomesV2
            ) t WHERE rn = 1""")
        return {(r["brand"], str(r["pipedrive_id"])): (r["farve"], pd.Timestamp(r["oprettet"]).date())
                for r in cur.fetchall()}
    finally:
        conn.close()


def _org_navne() -> dict:
    """{pipedrive_id: org_name}. Mangler navnet, viser siden ID'et."""
    if _dev():
        return {}
    conn = get_conn()
    try:
        cur = conn.cursor(as_dict=True)
        cur.execute("SELECT DISTINCT org_id, org_name FROM dbo.PipedriveDeals WHERE org_id IS NOT NULL AND org_name IS NOT NULL")
        raekker = cur.fetchall()
    finally:
        conn.close()
    navne = {str(r["org_id"]).strip(): r["org_name"].strip() for r in raekker}
    if len(navne) != len(raekker):
        # To navne på samme organisation: ét af dem vinder i stilhed.
        logger.warning("PipedriveDeals har flere navne pr. org_id (%d rækker, %d org_id)", len(raekker), len(navne))
    return navne


def _gem(brand: str, pipedrive_id: int, farve: str, kommentar: str, aktivitet_id) -> None:
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""INSERT INTO dbo.RetentionOutcomesV2 (brand, pipedrive_id, farve, kommentar, pipedrive_activity_id)
                       VALUES (%s, %s, %s, %s, %s)""",
                    (brand, pipedrive_id, farve, kommentar or None, aktivitet_id))
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Pipedrive
# ---------------------------------------------------------------------------

def _pipedrive_aktivitet(pipedrive_id: int, farve: str, kommentar: str):
    """(aktivitets-id, None) ved succes, ellers (None, tekst der siger hvorfor).

    Kaster aldrig: en fejl her må ikke forhindre, at registreringen gemmes.
    """
    token = os.getenv(PIPEDRIVE_TOKEN)
    if not token:
        return None, f"{PIPEDRIVE_TOKEN} mangler i .env, så intet er sendt til Pipedrive."
    # Ét afsnit pr. linje. Pipedrives tidslinje viser ikke <br> mellem linjerne
    # (set i testen 16-10: "Farve: gulTEST..." løb sammen).
    note = f"<p><b>Farve:</b> {farve}</p>"
    if kommentar:
        note += "<p>" + html.escape(kommentar).replace("\n", "<br>") + "</p>"
    note += "<p>Registreret i One Stop Shop (retention).</p>"
    payload = {"subject": f"Retention: {farve}", "type": "call", "done": True,
               "due_date": dt.date.today().isoformat(), "org_id": pipedrive_id, "note": note}
    try:
        r = session().post(PIPEDRIVE_URL, headers={"x-api-token": token}, json=payload, timeout=20)
        krop = r.json()
        if r.status_code >= 400 or not krop.get("success"):
            raise RuntimeError(f"Pipedrive {r.status_code}: {krop.get('error') or krop}")
        return int(krop["data"]["id"]), None
    except Exception:
        logger.exception("Pipedrive-aktivitet fejlede")
        return None, "Pipedrive-aktiviteten kunne ikke oprettes. Registreringen er gemt i hubben."


# ---------------------------------------------------------------------------
# Visning
# ---------------------------------------------------------------------------

def _kunde(r, farver: dict, navne: dict, idag: dt.date) -> dict:
    farve, dato = farver.get((r.brand, r.pipedrive_id), (None, None))
    opfoelgning = dato + dt.timedelta(days=OPFOELGNING_DAGE[farve]) if farve in OPFOELGNING_DAGE else None
    # Klaret: grøn, eller opfølgningen er ikke nået endnu. Grånes, men flyttes ikke.
    klaret = farve == "grøn" or (opfoelgning is not None and opfoelgning > idag)
    return {"navn": navne.get(r.pipedrive_id, r.pipedrive_id), "brand": r.brand, "pipedrive_id": r.pipedrive_id,
            "farve": farve, "farve_klasse": FARVE_KLASSE.get(farve),
            "opfoelgning": opfoelgning.strftime("%d-%m-%Y") if opfoelgning else "", "klaret": klaret}


def _valgt_brand(brand: str, tom_tilladt: bool = False) -> str:
    # Tom betyder "Begge" i filteret. En registrering skal altid have et brand.
    if not (brand in BRANDS or (tom_tilladt and brand == "")):
        raise HTTPException(400, "Ukendt brand")
    return brand


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/retention_v2", response_class=HTMLResponse)
def arbejdsliste(request: Request, user=Depends(get_current_user)):
    _kraev_adgang(user)
    return templates.TemplateResponse(request, "retention_v2.html", {
        "user": user, "x_standard": X_STANDARD, "y_standard": Y_STANDARD})


@router.get("/retention_v2/liste", response_class=HTMLResponse)
def liste(request: Request, brand: str = "", x: float = Query(X_STANDARD, ge=0, le=100),
          y: float = Query(Y_STANDARD, ge=0), user=Depends(get_current_user)):
    _kraev_adgang(user)
    brand = _valgt_brand(brand, tom_tilladt=True)
    x_df, y_df, udtraek = beregning.data()
    # Percentilen regnes inden for brandet, så brandfilteret kommer bagefter.
    x_df, y_df = beregning.x_liste(x_df, x), beregning.y_liste(y_df, y)
    if brand:
        x_df, y_df = x_df[x_df["brand"] == brand], y_df[y_df["brand"] == brand]

    farver, navne, idag = _seneste_farver(), _org_navne(), dt.date.today()
    x_raekker = [{**_kunde(r, farver, navne, idag), "foer": r.aaret_foer, "aaret": r.aaret,
                  "aendring": f"{r.aendring * 100:.0f} %"} for r in x_df.itertuples()]
    y_raekker = [{**_kunde(r, farver, navne, idag), "start": r.start.strftime("%d-%m-%Y"),
                  "snit": f"{r.pr_maaned:.1f}".replace(".", ",")} for r in y_df.itertuples()]
    return templates.TemplateResponse(request, "_retention_v2_liste.html", {
        "x_raekker": x_raekker, "y_raekker": y_raekker, "x_pct": f"{x:g}", "y_graense": f"{y:g}", "udtraek": udtraek})


@router.get("/retention_v2/form", response_class=HTMLResponse)
def form(request: Request, brand: str, pipedrive_id: int, user=Depends(get_current_user)):
    _kraev_adgang(user)
    brand = _valgt_brand(brand)
    navn = _org_navne().get(str(pipedrive_id), str(pipedrive_id))
    return templates.TemplateResponse(request, "_retention_v2_form.html", {
        "brand": brand, "pipedrive_id": pipedrive_id, "navn": navn, "farver": FARVER, "farve_klasse": FARVE_KLASSE})


@router.post("/retention_v2/registrer", response_class=HTMLResponse)
def registrer(request: Request, brand: str = Form(), pipedrive_id: int = Form(), farve: str = Form(),
              kommentar: str = Form(""), user=Depends(get_current_user)):
    _kraev_adgang(user)
    brand = _valgt_brand(brand)
    if farve not in FARVER:
        raise HTTPException(400, "Ukendt farve")
    kommentar = kommentar.strip()[:2000]
    navn = _org_navne().get(str(pipedrive_id), str(pipedrive_id))
    kvittering = {"navn": navn, "farve": farve, "farve_klasse": FARVE_KLASSE[farve],
                  "opfoelgning": "", "pipedrive": None, "fejl": None}

    if _dev():
        kvittering["fejl"] = "DEV_MODE: intet er gemt og intet er sendt til Pipedrive."
        return templates.TemplateResponse(request, "_retention_v2_form.html", {"kvittering": kvittering})

    # Pipedrive først, så aktivitets-id'et kan stå i den ene INSERT (tabellen
    # får kun INSERT, aldrig UPDATE). Fejler Pipedrive, gemmes rækken alligevel.
    aktivitet_id, pd_fejl = _pipedrive_aktivitet(pipedrive_id, farve, kommentar)
    try:
        _gem(brand, pipedrive_id, farve, kommentar, aktivitet_id)
    except Exception:
        logger.exception("INSERT i RetentionOutcomesV2 fejlede")
        kvittering["fejl"] = "Registreringen kunne IKKE gemmes i hubben. Prøv igen, og sig til, hvis det gentager sig."
        if aktivitet_id:
            kvittering["fejl"] += " Aktiviteten er allerede oprettet i Pipedrive."
        return templates.TemplateResponse(request, "_retention_v2_form.html", {"kvittering": kvittering})

    if farve in OPFOELGNING_DAGE:
        kvittering["opfoelgning"] = (dt.date.today() + dt.timedelta(days=OPFOELGNING_DAGE[farve])).strftime("%d-%m-%Y")
    kvittering["pipedrive"] = "Aktivitet oprettet i Pipedrive." if aktivitet_id else pd_fejl
    svar = templates.TemplateResponse(request, "_retention_v2_form.html", {"kvittering": kvittering})
    svar.headers["HX-Trigger"] = "liste-opdater"  # listen genindlæses, og kunden grånes
    return svar

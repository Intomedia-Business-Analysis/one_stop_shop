"""Indlæsning + validering af Zuora ARR-bevægelsesudtræk (.xlsx/.csv).

Læser fra en sti ELLER fra uploadede bytes. Validerer at påkrævede kolonner
findes, at month_end kan parses til en dato, og at net_diff/gross_in er
numeriske. Tolererer ekstra kolonner. Afviser ellers med en klar fejlbesked
(ExtractError) som routeren kan vise direkte til brugeren.
"""
from __future__ import annotations

import io
import logging
import os
from typing import Optional

import pandas as pd

from moduler.modul_admin_nysalg.matcher import _as_date
from moduler.modul_admin_nysalg.models import ExtractRow

logger = logging.getLogger(__name__)

# Påkrævede kolonner (skal alle findes i udtrækket).
REQUIRED_COLUMNS = [
    "month_end", "account_number", "pipedrive_id", "site", "brands",
    "account_type", "currency", "arr_local", "arr_dkk", "prev_arr",
    "net_diff", "gross_in", "gross_out", "movement", "administrativ",
]
_NUMERIC_COLUMNS = ["arr_local", "arr_dkk", "prev_arr", "net_diff", "gross_in", "gross_out"]


class ExtractError(ValueError):
    """Validerings-/indlæsningsfejl med brugervendt besked."""


def _to_float(v) -> float:
    if v is None:
        return 0.0
    if isinstance(v, (int, float)):
        return 0.0 if pd.isna(v) else float(v)
    s = str(v).strip()
    if not s or s.lower() in ("nan", "none", "-"):
        return 0.0
    # Tolerér tusind-/decimalseparatorer fra CSV-eksport (dansk og engelsk).
    s = s.replace(" ", "").replace(" ", "")
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")   # 1.234,56 -> 1234.56
    elif "," in s:
        s = s.replace(",", ".")                      # 1234,56 -> 1234.56
    try:
        return float(s)
    except ValueError as e:
        raise ExtractError(f"Kan ikke tolke tal: {v!r}") from e


def _read_dataframe(path: Optional[str], file_bytes: Optional[bytes],
                    filename: Optional[str]) -> pd.DataFrame:
    name = (filename or path or "").lower()
    is_csv = name.endswith(".csv")
    is_xlsx = name.endswith(".xlsx") or name.endswith(".xlsm")
    if not (is_csv or is_xlsx):
        raise ExtractError("Filtypen understøttes ikke — brug .xlsx eller .csv.")
    try:
        if file_bytes is not None:
            buf = io.BytesIO(file_bytes)
            return pd.read_csv(buf) if is_csv else pd.read_excel(buf, engine="openpyxl")
        if not path:
            raise ExtractError("Ingen sti eller fil angivet.")
        if not os.path.exists(path):
            raise ExtractError(f"Filen blev ikke fundet: {path}")
        return pd.read_csv(path) if is_csv else pd.read_excel(path, engine="openpyxl")
    except ExtractError:
        raise
    except Exception as e:
        logger.exception("Kunne ikke læse udtræk (%s)", name)
        raise ExtractError(f"Kunne ikke læse filen: {e}") from e


def load_extract(path: Optional[str] = None, file_bytes: Optional[bytes] = None,
                 filename: Optional[str] = None) -> list[ExtractRow]:
    """Læs og validér udtrækket → liste af ExtractRow (month_end normaliseret).

    Rejser ExtractError ved manglende kolonner, ulæselig dato eller ikke-numeriske
    beløb.
    """
    df = _read_dataframe(path, file_bytes, filename)
    df.columns = [str(c).strip() for c in df.columns]

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ExtractError("Udtrækket mangler påkrævede kolonner: " + ", ".join(missing))

    rows: list[ExtractRow] = []
    for i, rec in enumerate(df.to_dict("records"), start=1):
        # month_end skal kunne parses til en dato.
        try:
            month_end = _as_date(rec.get("month_end"))
        except Exception as e:
            raise ExtractError(f"Række {i}: kan ikke tolke month_end "
                               f"({rec.get('month_end')!r}) — forventer YYYY-MM-DD.") from e

        # net_diff / gross_in skal være numeriske (resten coerces blødt).
        try:
            net_diff = _to_float(rec.get("net_diff"))
            gross_in = _to_float(rec.get("gross_in"))
        except ExtractError as e:
            raise ExtractError(f"Række {i}: {e}") from e

        pd_id = rec.get("pipedrive_id")
        pd_id = "" if pd_id is None or (isinstance(pd_id, float) and pd.isna(pd_id)) else str(pd_id).strip()
        # '2874.0' (Excel-float) -> '2874'
        if pd_id.endswith(".0") and pd_id[:-2].isdigit():
            pd_id = pd_id[:-2]

        def _txt(key):
            v = rec.get(key)
            return "" if v is None or (isinstance(v, float) and pd.isna(v)) else str(v).strip()

        def _adm(v):
            s = str(v).strip().lower()
            return 1 if s in ("1", "1.0", "ja", "true", "yes", "x") else 0

        rows.append(ExtractRow(
            month_end=month_end,
            account_number=_txt("account_number"),
            pipedrive_id=pd_id,
            site=_txt("site"),
            brands=_txt("brands"),
            account_type=_txt("account_type"),
            currency=_txt("currency") or "DKK",
            arr_local=_to_float(rec.get("arr_local")),
            arr_dkk=_to_float(rec.get("arr_dkk")),
            prev_arr=_to_float(rec.get("prev_arr")),
            net_diff=net_diff,
            gross_in=gross_in,
            gross_out=_to_float(rec.get("gross_out")),
            movement=_txt("movement"),
            administrativ=_adm(rec.get("administrativ")),
            row_index=i,
        ))

    if not rows:
        raise ExtractError("Udtrækket indeholder ingen rækker.")
    return rows


def available_periods(rows: list[ExtractRow]) -> list[str]:
    """Distinkte perioder ('YYYY-MM') i udtrækket, nyeste først."""
    return sorted({r.month_end[:7] for r in rows if r.month_end}, reverse=True)


def filter_period(rows: list[ExtractRow], period: Optional[str]) -> list[ExtractRow]:
    """Afgræns til én periode ('YYYY-MM'). Tom/None => alle rækker."""
    if not period:
        return list(rows)
    return [r for r in rows if r.month_end[:7] == period]

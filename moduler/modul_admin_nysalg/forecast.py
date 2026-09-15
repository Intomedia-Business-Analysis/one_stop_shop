"""Hardcodet forecast pr. brand pr. måned (Business Media).

Forecastet er ledelsens tal og findes IKKE i nogen database — det er aftalt én
gang og tastet ind her. Det vises som kolonnen "Forecast" ved siden af Budget i
MÅNEDSTABELLEN. Det indgår bevidst IKKE i ÅTD-tabellen: forecastet dækker kun
jul–dec, så en ÅTD-periode der starter i januar ville sammenligne et helt års
realiserede tal med et halvt års forecast og give et misvisende billede.

Beløbene står i brandets egen valuta (jf. brands.BRAND_CURRENCY) — Watch NO er
altså NOK, resten DKK.

Skal forecastet opdateres (nyt år, ny reforecast): tilføj et nyt år-dict til
FORECAST og lad det gamle stå. Nøglerne SKAL være brand-labels fra
brands.DISPLAY_ORDER, ellers rammer tallet ingen række i rapporten — _validate()
nedenfor fejler ved import, hvis et label er stavet forkert.

NB: ledelsens regneark opgiver Abo Watch DK til 2.470.000 for jul–dec, mens
månedstallene herunder summer til 2.500.000. Månedstallene er brugt som de står
(de er dem der rapporteres pr. måned) — afvigelsen på 30.000 er i kildearket.
"""
from __future__ import annotations

# {brand-label: {'YYYY-MM': beløb i brandets egen valuta}}
FORECAST: dict[str, dict[str, float]] = {
    # Abonnement Watch DK (DKK)
    "Watch DK": {
        "2026-07":  40_000, "2026-08": 480_000, "2026-09": 610_000,
        "2026-10": 490_000, "2026-11": 520_000, "2026-12": 360_000,
    },
    # Annonce Watch DK — job (DKK)
    "Job": {
        "2026-07":   200_000, "2026-08": 700_000, "2026-09": 1_000_000,
        "2026-10":   800_000, "2026-11": 800_000, "2026-12":   600_000,
    },
    # Annonce Watch DK — banner (DKK)
    "Banner": {
        "2026-07": 350_000, "2026-08": 530_000, "2026-09": 530_000,
        "2026-10": 530_000, "2026-11": 530_000, "2026-12": 530_000,
    },
    # Abonnement Finans (DKK) — forecastet starter først i september
    "Finans": {
        "2026-09": 152_000, "2026-10": 43_000,
        "2026-11": 136_000, "2026-12": 66_000,
    },
    # Abonnement Watch NO (NOK)
    "Watch NO": {
        "2026-07": 244_000, "2026-08": 543_000, "2026-09": 600_000,
        "2026-10": 550_000, "2026-11": 600_000, "2026-12": 400_000,
    },
}

# Forecastet er kun aftalt for Business Media-rapporten. Monitor-rapportens
# rækker er enkelt-sites og har ingen forecast.
FORECAST_SCOPES = ("business_media",)


def _validate() -> None:
    """Fang stavefejl i brand-labels ved import i stedet for i en tom kolonne."""
    from moduler.modul_admin_nysalg.brands import DISPLAY_ORDER
    unknown = [b for b in FORECAST if b not in DISPLAY_ORDER]
    if unknown:
        raise ValueError(
            f"forecast.FORECAST har ukendte brand-labels: {unknown}. "
            f"Brug labels fra brands.DISPLAY_ORDER: {DISPLAY_ORDER}")


_validate()


def has_forecast(scope: str = "business_media") -> bool:
    """True hvis der overhovedet findes forecast for rapport-scopet."""
    return (scope or "business_media") in FORECAST_SCOPES


def month_forecast(ym: str, scope: str = "business_media") -> dict[str, float]:
    """{brand-label: forecast} for én måned ('YYYY-MM'). {} uden forecast."""
    if not has_forecast(scope) or not ym:
        return {}
    return {brand: float(months[ym])
            for brand, months in FORECAST.items() if ym in months}


def range_forecast(months: list[str], scope: str = "business_media") -> dict[str, float]:
    """{brand-label: Σ forecast} over en liste af måneder ('YYYY-MM').

    Et run kan spænde over flere måneder (fx et efterslæb der køres samlet), og
    så er månedsforecastet summen af de måneder runnet dækker. Måneder uden
    forecast bidrager med 0 — de udelades ikke, så en delvist dækket periode
    ikke ser ud til at have et fuldt forecast.
    """
    if not has_forecast(scope):
        return {}
    out: dict[str, float] = {}
    for brand, by_ym in FORECAST.items():
        total = sum(float(by_ym.get(ym, 0.0)) for ym in months or [])
        if total:
            out[brand] = total
    return out


def attach(rows: list[dict], months: list[str],
           scope: str = "business_media") -> list[dict]:
    """Sæt 'forecast' på brand-rækker for de(n) måned(er) runnet dækker.

    Rækker uden forecast får forecast=None (vises som '—'), så en tom celle ikke
    forveksles med et forecast på 0. Muterer ikke input-rækkerne.
    """
    fc = range_forecast(months, scope)
    return [dict(r, forecast=fc.get(r.get("brand"))) for r in rows or []]

"""Kernelogik: matchning af administrative nysalg mod administrative deals.

VALIDERET REGEL (se overlevering §4): matchning sker på en sammensat nøgle MED
fortegn. For samme (site, org_id, måned) findes ofte TO administrative deals med
samme nøgle men modsat fortegn (en positiv tilgang og en negativ afgang). Uden
fortegn i nøglen lander beløbet forkert. Værktøjet matcher KUN nysalgssiden
(net_diff > 0); opsigelsessiden tages fra udtrækkets `administrativ`-flag.

Modulet er bevidst fri for IO og framework, så det kan unit-testes isoleret.
"""
from __future__ import annotations

import datetime as _dt
from typing import Iterable, Optional

from moduler.modul_admin_nysalg.models import AdminDeal, ExtractRow


# ── Normalisering ────────────────────────────────────────────────────────────

def _as_date(value) -> str:
    """Returnér 'YYYY-MM-DD' uanset om input er date/datetime/Timestamp/tekst.

    Datoformatet SKAL være identisk tekst på begge sider af matchet, ellers nul
    match. Tekst tages som de første 10 tegn hvis det allerede ligner en ISO-dato
    ('2026-05-31', '2026-05-31 00:00:00', '2026-05-31T00:00:00'); ellers forsøges
    et par almindelige formater.
    """
    if value is None:
        raise ValueError("dato mangler (None)")
    # date/datetime (og pandas.Timestamp, der arver fra datetime)
    if isinstance(value, (_dt.date, _dt.datetime)):
        return value.strftime("%Y-%m-%d")
    s = str(value).strip()
    if not s:
        raise ValueError("dato mangler (tom streng)")
    # ISO-lignende: '2026-05-31', '2026-05-31 00:00:00', '2026-05-31T...'
    head = s[:10]
    try:
        _dt.date.fromisoformat(head)
        return head
    except ValueError:
        pass
    for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%m/%d/%Y", "%Y/%m/%d"):
        try:
            return _dt.datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    raise ValueError(f"kan ikke tolke dato: {value!r}")


def last_day_of_month(value) -> str:
    """EOMONTH: returnér sidste dag i måneden for input-datoen som 'YYYY-MM-DD'.

    Bruges af PipeDrive-adapteren til at lægge service_activation_date på samme
    måneds-grid som Zuoras month_end. Matcheren selv kalder den ikke.
    """
    iso = _as_date(value)
    y, m, _ = (int(p) for p in iso.split("-"))
    if m == 12:
        nxt = _dt.date(y + 1, 1, 1)
    else:
        nxt = _dt.date(y, m + 1, 1)
    return (nxt - _dt.timedelta(days=1)).strftime("%Y-%m-%d")


def normalize_site(site: str, site_map: Optional[dict] = None) -> str:
    """Site-navne matcher 1:1 i de fleste tilfælde. site_map (config/DB) håndterer
    de få afvigelser mellem Zuora og PipeDrive uden kodeændring.
    """
    s = (site or "").strip()
    if site_map:
        return site_map.get(s, site_map.get(s.lower(), s))
    return s


def make_key(site, org_id, month_end, site_map: Optional[dict] = None) -> str:
    """Sammensat nøgle: site|org_id|YYYY-MM-DD.

    Begge sider køres gennem PRÆCIS samme normalisering (site-map, str+strip på
    id, ensartet datoformat), så Zuora- og PipeDrive-siden er sammenlignelige.
    """
    return f"{normalize_site(site, site_map)}|{str(org_id).strip()}|{_as_date(month_end)}"


def sign_of(x) -> Optional[str]:
    """Fortegn som tekst. 0 => None (hverken til- eller afgang => intet match)."""
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    if v > 0:
        return "pos"
    if v < 0:
        return "neg"
    return None


# ── Indeksering og matchning ─────────────────────────────────────────────────

def build_index(deals: Iterable[AdminDeal], site_map: Optional[dict] = None):
    """Byg opslagsindeks (nøgle, fortegn) → deal.

    Dubletter (samme nøgle + samme fortegn): default tag den FØRSTE (som XOPSLAG),
    men markér nøglen i `dups`, så de berørte rækker kan løftes til "Kræver
    vurdering" frem for at blive stiltiende valgt.

    Returnerer (idx, dups).
    """
    idx: dict[tuple[str, str], AdminDeal] = {}
    dups: set[tuple[str, str]] = set()
    for d in deals:
        s = sign_of(d.value)
        if s is None:
            continue
        k = (make_key(d.site, d.org_id, d.month_end, site_map), s)
        if k in idx:
            dups.add(k)          # samme nøgle+fortegn => ambiguous
        else:
            idx[k] = d
    return idx, dups


def match_rows(rows: Iterable[ExtractRow], idx: dict, dups: set,
               site_map: Optional[dict] = None) -> None:
    """Sæt match-resultatet på hver række (muterer rækkerne).

    KUN nysalgssiden matches:
      net_diff > 0  → slå op mod (nøgle, 'pos') → administrativt nysalg ved match.
      net_diff < 0  → ingen matchning (opsigelser styres af `administrativ`-flaget).
      net_diff = 0  → ingen behandling.

    Blank/intet match = almindeligt nysalg (match forbliver None).
    """
    for r in rows:
        s = sign_of(r.net_diff)
        # Vi matcher kun nysalgssiden (positive bevægelser).
        if s != "pos":
            r.match = None
            r.match_sign = None
            r.ambiguous = False
            continue
        k = (make_key(r.site, r.pipedrive_id, r.month_end, site_map), s)
        r.match = idx.get(k)
        r.match_sign = s if r.match is not None else None
        r.ambiguous = (k in dups) and (r.match is not None)

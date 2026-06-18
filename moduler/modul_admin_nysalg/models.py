"""Dataklasser for admin-nysalg-matchning.

Holdes fri for IO/framework så matcher.py kan testes isoleret med fixtures.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class AdminDeal:
    """En administrativ PipeDrive-deal (kilde: synket PipedriveDeals-tabel).

    month_end er ALTID sidste dag i måneden som tekst 'YYYY-MM-DD' — adapteren
    har allerede kørt service_activation_date gennem EOMONTH, så matcheren kun
    sammenligner identisk-formatteret tekst.
    value er fortegnsbærende (positiv = nysalg/tilgang, negativ = opsigelse).
    """
    deal_id: str
    org_id: str
    site: str
    month_end: str          # 'YYYY-MM-DD' (sidste dag i måneden)
    value: float
    pipeline: str = ""
    status: str = ""
    org_name: str = ""


@dataclass
class ExtractRow:
    """En bevægelsesrække fra Zuora-udtrækket (ARR-bevægelser).

    De rå felter spejler udtrækket; match-felterne udfyldes af matcher.match_rows.
    """
    # ── rå felter fra udtrækket ──────────────────────────────────────────────
    month_end: str          # 'YYYY-MM-DD' (sidste dag i bevægelsesmåneden)
    account_number: str
    pipedrive_id: str
    site: str
    brands: str
    account_type: str
    currency: str
    arr_local: float
    arr_dkk: float
    prev_arr: float
    net_diff: float         # fortegnsbærende — styrer nysalg vs. opsigelse
    gross_in: float
    gross_out: float
    movement: str
    administrativ: int      # 0/1 — allerede sat i udtrækket (opsigelsessiden)
    row_index: int = 0      # rækkenummer i kildefilen (til reference/visning)
    brand: str = ""         # brand-gruppe (udfyldes af brands.classify ved run)

    # ── match-resultat (udfyldes af matcheren) ───────────────────────────────
    match: Optional[AdminDeal] = None
    match_sign: Optional[str] = None     # 'pos' | 'neg' | None
    ambiguous: bool = False              # samme nøgle+fortegn fandtes flere gange

    def is_admin_nysalg(self) -> bool:
        """Administrativt nysalg = positiv bevægelse der matchede en positiv deal."""
        return self.match is not None and self.match_sign == "pos"

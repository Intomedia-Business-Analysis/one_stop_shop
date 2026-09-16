"""Kontrollér ÅTD, baseline, forecast og PipeDrive-topsalg for ét run.

    python check_ytd.py <run_id>

Hvorfor: ÅTD-tabellen er en sum af tal fra to steder — den gemte baseline for
årets tidligere måneder og runnets eget (reviewede) resultat for rapportmåneden.
Ser man kun på den færdige tabel, kan man ikke se, om sammenlægningen er rigtig.
Scriptet regner efter brand for brand og siger hvor det ikke stemmer.

Scriptet SKRIVER IKKE. Det kalder præcis de samme funktioner som review-siden og
rapporten, men gemmer hverken baseline eller rapportfiler — så det kan køres så
mange gange man vil, også på et godkendt run, uden at ændre noget.

Exit-kode 0 = alt stemmer, 1 = mindst én afvigelse.
"""
import sys

from env import force_utf8_output, load_env

load_env()

from moduler.modul_admin_nysalg import forecast, repo  # noqa: E402
from moduler.modul_admin_nysalg import router as R  # noqa: E402

OK, FEJL, INFO = "  ✓ ", "  ✗ ", "    "
_afvigelser: list[str] = []


def afvig(besked: str) -> None:
    print(FEJL + besked)
    _afvigelser.append(besked)


def overskrift(titel: str) -> None:
    print(f"\n── {titel} " + "─" * max(0, 62 - len(titel)))


def kr(v, cur="DKK") -> str:
    return f"{round(v or 0):>14,.0f} {cur}".replace(",", ".")


def _sale_churn(b: dict) -> tuple[float, float]:
    """(Actual Sale, Actual Churn) for en brand-række — ekskl. administrative."""
    return (round((b.get("brutto") or 0) - (b.get("adm_nysalg") or 0), 2),
            round((b.get("opsigelser") or 0) - (b.get("adm_opsigelser") or 0), 2))


def main(run_id: int) -> int:
    force_utf8_output()
    print("=" * 68)
    print(f"  ÅTD-kontrol — run #{run_id}   (læse-only, intet gemmes)")
    print("=" * 68)

    run = repo.get_run(run_id)
    if not run:
        print(FEJL + f"Run #{run_id} findes ikke.")
        return 1

    scope = repo.run_scope(run)
    date_from, date_to = repo.run_date_range(run)
    run_ms = repo.run_months(run)
    ytd_from, ytd_to = repo.run_ytd_range(run)

    overskrift("1. Runnet")
    print(INFO + f"Periode      : {date_from} → {date_to}  ({', '.join(run_ms) or '—'})")
    print(INFO + f"Rapport-type : {scope}")
    print(INFO + f"Status       : {run.get('status')}")
    print(INFO + f"ÅTD slået til: {'ja' if run.get('ytd_enabled') else 'NEJ'}")
    if run.get("ytd_enabled"):
        print(INFO + f"ÅTD-periode  : {ytd_from} → {ytd_to}")
    else:
        print(INFO + "Runnet er oprettet uden ÅTD — opret et nyt run med "
                     "«Vis også ÅTD-tabel» for at teste ÅTD-tabellen.")
    if len(run_ms) > 1:
        print(INFO + f"NB: runnet dækker {len(run_ms)} måneder. Bevægelserne du "
                     "gennemgår er derfor ikke kun én måneds.")

    # ── Månedens brand-rækker (samme kald som review-siden) ──────────────────
    brand_comments = repo.get_brand_comments(run_id)
    budgets = repo.brand_budgets(date_from, date_to) if scope != "monitor" else {}
    matches = R._visible_matches(run)
    hidden = repo.get_hidden_brands(run_id)
    brand_rows = R._scope_brand_rows(scope, matches, date_from, date_to,
                                     brand_comments, budgets)
    brand_rows = [b for b in brand_rows if b["brand"] not in hidden]
    brand_rows = R._attach_forecast(run, brand_rows)

    overskrift("2. Månedstabellen + forecast")
    fc = forecast.range_forecast(run_ms, scope)
    print(INFO + f"{'Brand':<18}{'Actual Sale':>18}{'Actual Churn':>18}"
                 f"{'Netto':>18}{'Forecast':>18}")
    for b in brand_rows:
        sale, churn = _sale_churn(b)
        cur = b.get("currency") or "DKK"
        f = b.get("forecast")
        print(INFO + f"{b['brand']:<18}{kr(sale, cur)}{kr(churn, cur)}"
                     f"{kr(sale - churn, cur)}"
                     f"{kr(f, cur) if f is not None else '             —    '}")
    if fc:
        print(OK + f"Forecast fundet for {len(fc)} brands i "
                   f"{', '.join(run_ms)}: {', '.join(sorted(fc))}")
    else:
        print(INFO + "Intet forecast for perioden — kolonnen skjules i rapporten. "
                     "(Forecast findes kun for Business Media, juli–december 2026.)")
    for b in brand_rows:
        if b.get("forecast") is not None and b["brand"] not in fc:
            afvig(f"{b['brand']}: forecast sat på rækken, men ikke i forecast.py")

    if not run.get("ytd_enabled"):
        print("\n" + "=" * 68)
        print("  Runnet har ikke ÅTD slået til — resten af kontrollen springes over.")
        return 1 if _afvigelser else 0

    # ── Baseline for ÅTD-perioden ────────────────────────────────────────────
    ytd_ms = repo.months_in_range(ytd_from, ytd_to)
    stored = repo.get_baseline(scope, ytd_ms[0], ytd_ms[-1]) if ytd_ms else {}

    overskrift("3. Baseline pr. måned")
    for ym in ytd_ms:
        if ym in run_ms:
            print(INFO + f"{ym}  ← dette run (baseline ignoreres for denne måned)")
            continue
        rows = stored.get(ym) or {}
        if not rows:
            print(FEJL + f"{ym}  INGEN baseline — måneden tæller som 0 i ÅTD")
            continue
        kilder = {r["source"] for r in rows.values()}
        netto = sum(r["netto"] for r in rows.values() if r["currency"] == "DKK")
        print(OK + f"{ym}  {len(rows):>2} rækker  ({', '.join(sorted(kilder))})"
                   f"  netto DKK {netto:>12,.0f}".replace(",", "."))

    # ── ÅTD-tabellen + efterregning ──────────────────────────────────────────
    block = R._ytd_block(run, brand_rows, brand_comments, hidden)
    overskrift("4. ÅTD-tabellen")
    print(INFO + f"Periode: {block['label']}")
    if block["missing"]:
        print(INFO + f"Måneder uden baseline: {', '.join(block['missing'])} "
                     "(tæller som 0 — advarslen skal også stå i reviewet)")
    print(INFO + f"{'Brand':<18}{'Sale ÅTD':>18}{'Churn ÅTD':>18}"
                 f"{'Netto ÅTD':>18}{'Budget ÅTD':>18}")
    for b in block["rows"]:
        sale, churn = _sale_churn(b)
        cur = b.get("currency") or "DKK"
        print(INFO + f"{b['brand']:<18}{kr(sale, cur)}{kr(churn, cur)}"
                     f"{kr(sale - churn, cur)}{kr(b.get('budget'), cur)}")

    # Uafhængig genudregning: baseline for de tidligere måneder + denne måneds
    # tal. Stemmer den ikke med ÅTD-tabellen, er sammenlægningen gal.
    overskrift("5. Efterregning — baseline + rapportmåned = ÅTD?")
    forventet: dict[str, list[float]] = {}
    for ym in ytd_ms:
        if ym in run_ms:
            continue
        for r in (stored.get(ym) or {}).values():
            f = forventet.setdefault(r["brand"], [0.0, 0.0])
            f[0] += r["sale"]
            f[1] += r["churn"]
    for b in brand_rows:
        sale, churn = _sale_churn(b)
        f = forventet.setdefault(b["brand"], [0.0, 0.0])
        f[0] += sale
        f[1] += churn

    faktisk = {b["brand"]: _sale_churn(b) for b in block["rows"]}
    for brand in sorted(set(forventet) | set(faktisk)):
        v_sale, v_churn = forventet.get(brand, (0.0, 0.0))
        h = faktisk.get(brand)
        if h is None:
            afvig(f"{brand}: mangler helt i ÅTD-tabellen "
                  f"(forventet sale {v_sale:,.0f})".replace(",", "."))
            continue
        if abs(round(v_sale, 2) - h[0]) > 0.5 or abs(round(v_churn, 2) - h[1]) > 0.5:
            # Tallene formateres hver for sig — et .replace() på hele sætningen
            # ville også ramme kommaerne i selve teksten.
            def _t(v):
                return f"{v:,.0f}".replace(",", ".")
            afvig(f"{brand}: ÅTD viser sale {_t(h[0])} / churn {_t(h[1])}, "
                  f"men baseline+måned giver {_t(v_sale)} / {_t(v_churn)}")
    if not _afvigelser:
        print(OK + f"Alle {len(faktisk)} ÅTD-rækker stemmer med "
                   "baseline + rapportmåneden")

    # ── PipeDrive-topsalg mod brand-rækkerne ─────────────────────────────────
    overskrift("6. Største salg pr. PipeDrive-brand")
    grupper = repo.pipedrive_top_deals(date_from, date_to, scope)
    pr_brand = {b["brand"]: b for b in brand_rows}
    for g in grupper:
        sum_deals = round(sum(d["value"] for d in g["deals"]), 2)
        b = pr_brand.get(g["brand"])
        raekke = b["netto"] if b else None
        mark = ""
        if raekke is not None and abs(sum_deals - raekke) > 0.5:
            # Banner afviger bevidst: det programmatiske salg har ingen deals.
            mark = ("  ← forskel = programmatisk salg (forventet)"
                    if g["brand"] == "Banner" and g.get("note")
                    else "  ← FORSKEL, undersøg")
            if not g.get("note"):
                afvig(f"{g['brand']}: Σ deals {sum_deals:,.0f} ≠ rækkens netto "
                      f"{raekke:,.0f}".replace(",", "."))
        top = g["deals"][0] if g["deals"] else None
        print(INFO + f"{g['brand']:<16}{len(g['deals']):>3} deals  "
                     f"Σ {kr(sum_deals, g['currency'])}  "
                     f"vs. række {kr(raekke, g['currency']) if raekke is not None else '—'}{mark}")
        if top:
            print(INFO + f"  største: {top['customer']} — "
                         f"{kr(top['value'], g['currency'])} ({top['date']})")
    if not grupper:
        print(INFO + "Ingen PipeDrive-deals i perioden.")

    print("\n" + "=" * 68)
    if _afvigelser:
        print(f"  {len(_afvigelser)} afvigelse(r):")
        for a in _afvigelser:
            print(f"    ✗ {a}")
        return 1
    print("  Alt stemmer. Intet blev gemt.")
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 2 or not sys.argv[1].isdigit():
        print(__doc__)
        sys.exit(2)
    sys.exit(main(int(sys.argv[1])))

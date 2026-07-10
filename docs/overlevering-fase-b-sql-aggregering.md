# Overlevering: Månedsrapport Fase B — flyt aggregeringen til SQL

*Skrevet 2026-07-10. Kontekst: modul_admin_nysalg (Monthly performance report).
Målgruppe: den (person eller AI-session) der skal udføre Fase B senere.*

## §1 Mål

Gør månedsrapportens ventetider **uafhængige af periodelængden** ved at flytte
total-/brand-/måneds-beregningerne fra Python (der henter alle bevægelsesrækker
over netværket) ind i SQL Server som `GROUP BY`-queries. Konkret skal:

1. **Klik-gem i reviewet** (ret gross in/out, adm-andel, medtag/udeluk, override)
   gå fra ~3-4 s til <1 s på lange runs. Hvert klik kalder i dag
   `_visible_matches(run)` → `repo.get_matches` (3,2 s for 17k rækker på run 32)
   blot for at genberegne topkort + per-brand-netto.
2. **Måneds-opdelingen** (`/months-fragment`, ~20 s for 54 måneder) ned til få
   sekunder — den laver stadig ~25 PipeDrive-/budget-queries **pr. måned**.

## §2 Status — hvad er allerede gjort (Fase A + A2, 2026-07-09/10)

Baseline før: review af run 32 (2022-01–2026-06, 54 mdr., 363k gemte rækker)
blev aldrig færdig (~19 min. målt komponentvis). Nu: **5,5 s** (run 31, én måned:
1,6 s). Gennemført:

- `get_matches` filtrerer nul-rækker i SQL (363k → 17k; kun ~4,6 % af rækkerne
  er reelle bevægelser eller har admin/override-markering).
- Bevægelseslisten renderes IKKE længere i Jinja (det kostede ~125 s) — rækkerne
  sendes som JSON pr. brand-gruppe og bygges klient-side i bidder af 200
  ("Vis flere"). Events er delegerede (`admin_nysalg_review.html`).
- Måneds-opdelingen hentes asynkront (`/months-fragment` → partial
  `_admin_nysalg_months.html`).
- Reviewets tre tunge opslag (matches, org-navne, PipeDrive-rækker) hentes
  parallelt; `pipedrive_brand_rows(parallel=True)` kører hver spec i egen
  tråd/forbindelse.
- `brand_budgets_by_month` henter alle måneders budgetter i ét `GROUP BY`-kald;
  månederne i `brand_rows_by_month` beregnes parallelt (6 tråde).
- Alle DB-tunge endpoints er sync `def` (threadpool) — `async def` med blokerende
  pymssql frøs hele hubben.
- `insert_matches` batcher 200 rækker pr. INSERT.

## §3 Opgaven i delmål

### B1. SQL-aggregat for topkort + per-brand (klik-gem-stien) — vigtigst

Skriv én `GROUP BY brand`-query (+ en uden GROUP BY til topkortene) over
`admin_nysalg_match`, der udtrykker `effective_*`-logikken (se §4) som
CASE-udtryk, og brug den i:

- `repo.summarize(matches)` → ny `repo.summarize_sql(run_id, scope)`
- `router._brand_summary_map` (per-brand netto til live-opdatering)
- Klik-endpoints i `router.py`: `set_override`, `row_include`, `row_value`,
  `row_adm` — de behøver herefter IKKE `_visible_matches`/`get_matches`.

Scope-filteret (se §5): business_media = `brand <> 'Monitor'` (+ NULL-brand
klassificeres via site — i praksis er `brand` altid sat af `insert_matches`);
monitor = `brand = 'Monitor'` grupperet på `site` i stedet for `brand`.

### B2. Paritetstest SQL ↔ Python

Python-helperne (`effective_gross_in/out`, `effective_adm_in/out`,
`summarize`, `summarize_by_brand`) BEHOLDES som dokumenteret reference.
Tilføj en test der genererer et bredt sæt fixture-rækker (alle kombinationer af
overrides/flags/tolerance-grænser), indsætter dem i en test-tabel ELLER
oversætter CASE-udtrykkene til SQLite i testen, og asserter at SQL-aggregatet
== Python-beregningen række for række og aggregeret. Uden denne test VIL de to
implementeringer drive fra hinanden.

### B3. PipeDrive-rækker pr. måned i ét kald

`brand_rows_by_month` kalder stadig `pipedrive_brand_rows` pr. måned
(~25 queries × 54). Udvid spec-motoren i `repo.pipedrive_brand_rows` med en
by-month-variant: samme WHERE, men `GROUP BY CONVERT(varchar(7),
[service_activation_date], 126)` → {ym: række}. Husk også
`_dk_advertising_brand_rows` (Watch/FINANS-split + ProgrammaticSales) og
budget-opslagene (`_budget_for_where` kan også GROUP BY måned — jf.
`brand_budgets_by_month` og `monitor_site_budgets_by_month` som mønstre).

### B4. (Valgfrit) Server-paginering af bevægelses-JSON

JSON-payloaden for run 32 er ~7 MB. Acceptabelt nu, men ved endnu større runs:
lever kun de første N rækker pr. gruppe + et endpoint til resten.

## §4 Beregningsreglerne der skal udtrykkes i SQL (VALIDERET — afvig ikke)

Kilde: `repo.py` (`effective_*`-funktionerne). Pr. række i `admin_nysalg_match`:

```
effective_gross_in  = 0 hvis total_excluded=1
                      ellers gross_in_override hvis ikke NULL
                      ellers COALESCE(gross_in, 0)

effective_gross_out = 0 hvis total_excluded=1
                      ellers gross_out_override hvis ikke NULL
                      ellers gross_out hvis ikke NULL/0
                      ellers ABS(net_diff) hvis net_diff < 0, ellers 0
                      (= _row_opsigelse-fallback)

effective_is_admin  = 0 hvis override='exclude'
                      1 hvis override='include'
                      ellers is_admin

is_admin_opsigelse  = administrativ=1 ELLER match_sign='neg'

auto_adm_share(gross, deal) =
    gross hvis gross<=0-håndtering: 0 hvis gross<=0
    gross hvis deal er NULL
    ABS(deal) hvis ABS(deal) < gross * (1 - 0.01)   ← ADM_PARTIAL_TOLERANCE = 1 %
    ellers gross

effective_adm_in    = 0 hvis total_excluded=1
                      ellers adm_in_override hvis ikke NULL
                      ellers 0 hvis ikke effective_is_admin
                      ellers auto_adm_share(effective_gross_in,
                             matched_value hvis match_sign='pos' ellers NULL)

effective_adm_out   = 0 hvis total_excluded=1
                      ellers adm_out_override hvis ikke NULL
                      ellers 0 hvis ikke is_admin_opsigelse
                      ellers auto_adm_share(effective_gross_out,
                             matched_value hvis match_sign='neg' ellers NULL)

Topkort:  brutto=Σeff_gross_in, adm_nysalg=Σeff_adm_in, opsigelser=Σeff_gross_out,
          adm_opsigelser=Σeff_adm_out,
          netto = (brutto − adm_nysalg) − (opsigelser − adm_opsigelser)
          n_admin = antal rækker hvor effective_is_admin ELLER eff_adm_in > 0
```

Delvis-adm-reglen (deal-værdi < gross → kun deal-værdien er administrativ) er
CFO-valideret 2026-07-09: automatisk, 1 % tolerance, symmetrisk på churn,
manuel `adm_in/out_override` vinder altid. Testene i
`tests/test_admin_nysalg_partial_adm.py` ER facit — SQL-versionen skal bestå
en spejling af dem (B2).

## §5 Rapport-scopes (vigtig kontekst fra 2026-07-10)

Et run har `report_scope` ('business_media' | 'monitor'; NULL = business_media):

- **business_media**: alt undtagen Monitor (EXCLUDED_BRANDS i `brands.py`).
- **monitor**: kun Monitor, **pr. enkelt site** — `repo.monitor_relabel` sætter
  `brand = site`, så hele brand-pipelinen kører pr. site. I SQL: gruppér på
  `site` for monitor-scope. Budget-join via `repo.monitor_norm` (æøå-foldning,
  '.dk'/' DK' strippes). Bemærk: Zuora-udtrækkene indeholder pr. juli 2026 endnu
  INGEN Monitor-sites (kommer i fremtidige udtræk).

## §6 Faldgruber

- **`.venv\Scripts\python.exe`** — `python` er ikke på PATH.
- **Tests kører UDEN database** (`pytest tests -q`); preview (`hub-dev`, port
  8123, DEV_MODE=1) kører mod den **RIGTIGE** database — verificér aldrig
  muterende endpoints i preview mod rigtige runs.
- **CLS' rigtige hub kører på port 8000** (`python app.py`): Jinja-templates
  genindlæses fra disk pr. request, men Python-kode kræver **genstart af
  processen** — ellers serveres nye templates mod gamle endpoints (gav 404 på
  `/months-fragment` 2026-07-10).
- **DB'en er on-prem (192.168.1.251) over langsomt/flaky net** — antal
  netværksrundture betyder mere end query-kompleksitet. Query-timeout er 15 s
  (db.py); forbindelses-poolen er på 10.
- **`.git` ligger på OneDrive** — verificér `git show --stat` efter hver commit
  (fantom-staged filer er set før).
- Excel-/PDF-generering (`report.py`) bruger de SAMME Python-helpers på
  rækkeniveau (Ark "Administrative new sales" og "Movements per brand" viser
  per-række-værdier) — de skal fortsat have rækkerne; kun AGGREGATERNE flyttes
  til SQL.
- `summarize_by_brand` har `ensure_labels`/`seed_defaults`-parametre
  (Monitor-rapporten) og `extra_rows` (PipeDrive-annonce-rækker) — SQL-versionen
  erstatter kun Zuora-delen; annonce-rækkerne lægges ovenpå som nu.

## §7 Målepunkter (baseline pr. 2026-07-10, run 32 = 54 mdr.)

| Operation | Nu | Mål efter Fase B |
|---|---|---|
| Review-åbning | 5,5 s | ~2-3 s |
| Klik-gem (row-adm/row-value/…) | ~3-4 s | <1 s |
| /months-fragment | ~20 s | <5 s |
| Rapportgenerering (Excel+PDF) | ~1-2 min. | <30 s |

## §8 Nøglefiler

- `moduler/modul_admin_nysalg/repo.py` — al beregning + SQL. Se `effective_*`,
  `summarize`, `summarize_by_brand`, `brand_rows_by_month`,
  `pipedrive_brand_rows` (spec-motor med `specs`/`dk_split`/`parallel`),
  `monitor_*`-funktionerne, `brand_budgets_by_month`.
- `moduler/modul_admin_nysalg/router.py` — endpoints; `_visible_matches(run)`
  (scope-filter + monitor-relabel), `_scope_extra_rows`/`_scope_brand_rows`,
  klik-endpoints, `/months-fragment`.
- `moduler/modul_admin_nysalg/brands.py` — brand-/scope-konstanter,
  `MONITOR_PIPEDRIVE_ROWS`, `brand_geo`/`brand_account` (monitor-fallback).
- `templates/admin_nysalg_review.html` + `templates/_admin_nysalg_months.html`.
- `tests/test_admin_nysalg_partial_adm.py`, `tests/test_admin_nysalg_monitor.py`,
  `tests/test_admin_nysalg_matcher.py` — 84 tests i alt, alle grønne.

## §9 Branch-status pr. 2026-07-10

Branch `MonthlyReport-partial-administrativ` indeholder fire ucommittede,
logisk adskilte ændringer (bør committes hver for sig):
1. Delvis administrativ (auto deal-værdi, 1 % tolerance) + tests
2. Performance Fase A (rækkefiltrering, batch-budget, sync endpoints, batch-insert)
3. Monitor/Business Media-rapport-scopes + tests
4. Rendering-optimering (JSON-bevægelser, async måneder, parallel forhentning)

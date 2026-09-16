"""Værktøj til matchning af administrative nysalg mod administrative PipeDrive-deals.

Se overlevering: bruttonysalget i ARR-rapporteringen indeholder administrative
nysalg (flytninger, rekontering, intern omflytning) som skal trækkes ud før
tallet rapporteres. Modulet matcher udtrækkets nysalgsrækker mod administrative
PipeDrive-deals (på sammensat nøgle MED fortegn) og lader direktøren reviewe,
kommentere og godkende, før en rapport genereres.

Rapporten kan køres med en ÅTD-tabel (flag på runnet): bevægelserne der reviewes
er stadig kun rapportmånedens, mens årets tidligere måneder kommer fra en gemt
baseline (admin_nysalg_baseline) — så et review kun skal laves én gang pr. måned.
Når rapporten genereres, gemmes månedens tal selv som baseline til næste gang.

Lagdeling (IO holdes ude af matcher.py så kernen er unit-testbar):
  matcher.py          – ren matchnings-logik, ingen IO
  models.py           – dataklasser (AdminDeal, ExtractRow)
  extract_loader.py   – læs+validér .xlsx/.csv (sti eller upload)
  forecast.py         – hardcodet forecast pr. brand pr. måned (kolonne i månedstabellen)
  pipedrive_source.py – PipeDriveAdminSource adapter (synket DB-tabel som default)
  repo.py             – SQL Server CRUD (run + matches + ÅTD-baseline) + init_admin_nysalg_db
  report.py           – Excel/PDF-generering
  router.py           – FastAPI routes + Jinja2-templates
"""

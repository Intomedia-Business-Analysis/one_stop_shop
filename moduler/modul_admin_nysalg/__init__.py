"""Værktøj til matchning af administrative nysalg mod administrative PipeDrive-deals.

Se overlevering: bruttonysalget i ARR-rapporteringen indeholder administrative
nysalg (flytninger, rekontering, intern omflytning) som skal trækkes ud før
tallet rapporteres. Modulet matcher udtrækkets nysalgsrækker mod administrative
PipeDrive-deals (på sammensat nøgle MED fortegn) og lader direktøren reviewe,
kommentere og godkende, før en rapport genereres.

Lagdeling (IO holdes ude af matcher.py så kernen er unit-testbar):
  matcher.py          – ren matchnings-logik, ingen IO
  models.py           – dataklasser (AdminDeal, ExtractRow)
  extract_loader.py   – læs+validér .xlsx/.csv (sti eller upload)
  pipedrive_source.py – PipeDriveAdminSource adapter (synket DB-tabel som default)
  repo.py             – SQL Server CRUD (run + matches) + init_admin_nysalg_db
  report.py           – Excel/PDF-generering
  router.py           – FastAPI routes + Jinja2-templates
"""

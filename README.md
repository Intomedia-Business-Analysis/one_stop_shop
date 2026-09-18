# Intomedia Hub

Internt website til salg, marketing og ledelse i Intomedia: 14 moduler —
dashboards og værktøjer — bygget som én FastAPI-app oven på databasen
`INTOMEDIA` i SQL Server. Alt data læses og skrives direkte i den database;
hubben har ingen egen datamodel ud over sine bruger- og
personaliseringstabeller.

Dokumentationen følger [dokumentationsstandarden](docs/DOKUMENTATIONSSTANDARD.md),
som gælder for alle vores Python-scripts og -applikationer.

---

## 1. Adgang

| | |
|---|---|
| **Server** | `172.29.11.31` |
| **Adresse** | http://172.29.11.31:8000 |
| **Login** | Brugernavn + adgangskode, oprettet af en admin under `/admin/users` |
| **Første bruger** | `create_admin.py` (se §3) |

Porten kommer fra `HUB_PORT` i serverens `.env` (standard 8000), og adressen er
`https://` i stedet for `http://`, hvis `HUB_SSL_KEYFILE` og `HUB_SSL_CERTFILE`
er sat. Tjek serverens `.env`, hvis linket ikke svarer.

Hubben kan derudover eksponeres udefra gennem Azure AD Application Proxy
(`https://jpbmdatawarehouse-jppol.msappproxy.net`). Det kræver to linjer i
`.env` — se §7.

**Roller.** Adgang er rangbaseret: en bruger ser alt, der kræver hans rang eller
lavere. Rækkefølgen er

`screen` (0) → `salesperson` (1) → `sales_manager` (2) → `sales_operations` (3) →
`marketing` (4) → `management` (5) → `admin` (6)

Oven på rangen findes tre finere greb, som admin styrer i UI'et:

* **Ressource-overrides** pr. bruger eller pr. rolle — åbner eller lukker et
  enkelt menupunkt uden at ændre rangen.
* **Holdkrav** (`required_team`) — fx Banner & Job-modulerne, som kun holdets
  medlemmer ser.
* **Datahold** (`HubUserTeamAccess`) — begrænser hvilke teams' *tal* en bruger må
  se, uafhængigt af hvilke sider han kan åbne.

Rollen `screen` er kontorskærmenes: rang 0, ingen adgang til hubben, kun til
rotationsdashboardene via en indbygget override.

---

## 2. Funktioner

| Modul | Sti | Hvad det gør | Min. rolle |
|---|---|---|---|
| **Sælger Dashboard** | `/tools/performance/saelger` | Egen performance: salg, konvertering, pipeline, år-til-år | salesperson |
| **Sælger Portefølje** | `/tools/saelger-portfolio/` | Egne kunder med abonnementer, ARR og historik | salesperson |
| **Manager Dashboard** | `/tools/performance/manager` | Teamets tal, drill-down til den enkelte sælger | sales_manager |
| **Afdelingsleder Dashboard** | `/tools/performance/afdelingsleder` | Brand- og månedsoverblik, churn og vækst på tværs | sales_operations |
| **Medie Benchmark** | `/tools/benchmark/medier` | Sammenligner medier/brands på salgstal over tid | management |
| **Budget** | `/tools/budget/` | Upload og redigering af medie- og sælgerbudgetter (Excel ind, tabel ud) | sales_manager |
| **Forecast** | `/tools/forecast/` | Sælgeren indtaster sit forecast; manager reviewer og godkender | salesperson |
| **Portfolio Alignment** | `/tools/portfolio-alignment/` | Afstemmer ACV i Pipedrive mod Zuora og kan oprette afstemnings-deals via Pipedrive-API'et | sales_operations |
| **Banner & Job Dashboard** | `/tools/banner-job/` | Kunde- og sælgertal for Banner- og Job-pipelines (DK/NO) | salesperson + hold |
| **Klippekort Overblik** | `/tools/klippekort/` | Registrerer forbrugte klip på job-deals og skriver tilbage til Pipedrive | salesperson + hold |
| **Deal Source (Marketing)** | `/tools/marketing/deal-source` | Lead-kilder og konvertering pr. konto | marketing |
| **Retention** | `/retention/prioritering`, `/retention/overview`, `/retention/risk_overview` | Dagens opkaldsliste, porteføljeudvikling og churn-risiko pr. abonnement, med registrering af samtaleudfald | sales_operations |
| **Monthly Performance Report** | `/tools/admin-nysalg/` | Matcher administrative nysalg mod Zuora-udtræk, review + direktørgodkendelse, Excel/PDF-rapport. Valgfri ÅTD-tabel og forecast-kolonne | management |
| **ÅTD-baseline** | `/tools/admin-nysalg/baseline` | Tidligere måneders færdigreviewede Actual Sale/Churn, som ÅTD-tabellen bygger på | management |
| **Barselsplanlægger** | `/tool/barselsberegner` | Barselsberegning og godkendelsesflow med mailnotifikation | salesperson |
| **Rotation** | `/tools/rotation/` | Fuldskærms-dashboards til kontorskærme: Sales, Department, Banner, Job, Media, NO Advertising — med autoplay og navngivne skærmopsætninger | salesperson / screen |
| **Datastatus** | `/tools/maintenance/` | Hvornår hver datakilde sidst blev loadet ind — tabeller målt mod serverens scheduled tasks, filbaserede eksporter mod deres forventede kadence | sales_operations |
| **Administration** | `/admin/users`, `/admin/roles`, `/admin/teams`, `/admin/usage` | Brugere, roller, hold, adgangs-overrides og forbrugsstatistik | admin |

Fælles på tværs af alle sider: favoritter (`/favorites`), senest besøgt
(`/recent`), søgning (`/api/search`), egne indstillinger (`/settings`) og en
sidebar, der kun viser det, brugeren må se.

---

## 3. Kør det lokalt

**Forudsætninger:** Python 3.10+ (serveren kører 3.13) og netværksadgang til
SQL Server. Alle kommandoer køres fra projektmappen.

```bash
python -m venv .venv
```

```bash
.venv\Scripts\python.exe -m pip install -r requirements.txt -r requirements-dev.txt
```

Er maskinen uden internetadgang, installeres fra den lokale wheelhouse:

```bash
.venv\Scripts\python.exe -m pip install --no-index --find-links=..\wheelhouse\one_stop_shop -r requirements.txt
```

Opret konfigurationen:

```bash
copy .env.example .env
```

Udfyld mindst `DB_SERVER`, `DB_NAME`, `DB_USER`, `DB_PASSWORD` og `SECRET_KEY`.
Nøglen genereres med:

```bash
python -c "import secrets; print(secrets.token_hex(32))"
```

Kontrollér hele opsætningen, før du starter noget:

```bash
.venv\Scripts\python.exe preflight.py
```

Den går database, tabeller, mapper og udgående forbindelser igennem og siger,
hvad der mangler. Exit 0 = klar. Er `HubUsers` tom, så opret den første bruger:

```bash
.venv\Scripts\python.exe create_admin.py
```

Start appen:

```bash
.venv\Scripts\python.exe run_server.py
```

Den lytter på `HUB_HOST`:`HUB_PORT` — som standard http://localhost:8000.

### Uden database

Sætter du `DEV_MODE=1` i `.env`, springes login over (du er automatisk
"Dev User" med admin-rolle), og `/docs`, `/redoc` og `/openapi.json` slås til.
Dashboards viser stadig "Data utilgængelig" uden en database, men UI, navigation
og adgangslogik kan udvikles og gennemses uden forbindelse.

`DEV_MODE` **skal** være `0` eller usat på serveren.

---

## 4. Konfiguration

Alle indstillinger er miljøvariabler. De læses af `env.load_env()`, som finder
`.env` via en **absolut** sti ved siden af koden — ikke via arbejdsmappen.
`HUB_ENV` kan pege på en fil et andet sted, fx
`%PROGRAMDATA%\IntomediaHub\.env`. Rigtige miljøvariabler vinder over filen, så
en enkelt værdi kan overstyres uden at redigere den.

**Den fulde liste med forklaringer står i [`.env.example`](.env.example)** — den
er kilden, og den er kommenteret variabel for variabel. Her er kun de
beslutninger, man skal tage stilling til:

| Variabel | Betydning |
|---|---|
| `SECRET_KEY` | Påkrævet. Signerer session-cookies. Appen nægter at starte uden. Skiftes den, ryger alle sessioner |
| `DB_TDS_VERSION` | 7.4. Skal være 7.4 mod servere, der kræver kryptering — 7.0 kan ikke TLS |
| `DB_DATE_AS_STRING` | 1 = DATE-kolonner leveres som `'YYYY-MM-DD'`-strenge. Se §9 — lad den stå på 1 |
| `TRUST_PROXY` | 1 **kun** bag en reverse proxy, der selv sætter `X-Forwarded-For` |
| `HUB_TRUSTED_ORIGINS` | Eksterne adresser, der må sende formularer. Påkrævet bag App Proxy, ellers afvises alle logins med 403 |
| `HUB_SSL_KEYFILE` / `HUB_SSL_CERTFILE` | Sat = uvicorn terminerer TLS selv. Begge eller ingen |
| `ZUORA_SNAPSHOT_DIR` / `USAGE_SNAPSHOT_DIR` | Filmapper med snapshots. Standardstierne peger ind i en OneDrive-profil og findes ikke på serveren. Flere stier adskilles med `;` |
| `PD_TOKEN_*` | Pipedrive-tokens pr. konto. Mangler et, fejler kun det modul, der bruger det |
| `SMTP_*` | Barselsplanlæggerens godkendelsesmails. Er `SMTP_USER`/`SMTP_PASSWORD` tomme, springes afsendelsen over uden fejl |
| `DEV_MODE` | 1 = login bypasses og API-dokumentationen åbnes. Aldrig på serveren |

---

## 5. Tests

```bash
.venv\Scripts\python.exe -m pytest tests/ -q
```

218 tests. **Ingen database krævet** — `tests/conftest.py` peger `DB_SERVER` på
et hostnavn, der ikke findes, så alle connects fejler hurtigt, og appen falder
tilbage til sine defaults. Login og adgang testes gennem FastAPIs
`dependency_overrides`, så der skal ikke findes rigtige brugere nogen steder.

Kør en enkelt fil eller et enkelt emne:

```bash
.venv\Scripts\python.exe -m pytest tests/test_smoke.py -q
```

```bash
.venv\Scripts\python.exe -m pytest tests/ -k benchmark -q
```

Hvad dækkes:

| Fil | Dækker |
|---|---|
| `test_smoke.py` | At alle sider svarer, og at adgangskontrollen holder for hver rolle |
| `test_benchmark.py` | Benchmark-modulets beregninger og filtre |
| `test_db_datokompat.py` | Datobroen: hvad der konverteres, hvad der ikke gør, alle fetch-veje, og at `DB_DATE_AS_STRING=0` slår den fra |
| `test_admin_nysalg_*.py` | Matchning mod Zuora-udtrækket, delvist administrative deals, og at Python-logikken giver samme resultat som SQL'en |
| `test_admin_nysalg_ytd.py` | Forecast-tallene, baseline-konverteringen og at ÅTD lægger baseline + rapportmåned rigtigt sammen |

### Kontrol mod rigtige data

`pytest` kører uden database og kan derfor ikke se, om ÅTD-tabellen stemmer med
den baseline, der faktisk ligger i basen. Til det:

```bash
.venv\Scripts\python.exe check_ytd.py <run_id>
```

Scriptet **skriver ikke** — det kalder de samme funktioner som review-siden og
rapporten, men gemmer hverken baseline eller rapportfiler, så det kan køres på et
godkendt run uden at ændre noget. Det regner ÅTD efter brand for brand
(baseline + rapportmåned skal give ÅTD-rækken), viser hvilke måneder der mangler
baseline, og holder PipeDrive-topsalgene op mod brand-rækkerne. Exit-kode 0 =
alt stemmer, 1 = mindst én afvigelse.
| `test_spejlkopier.py` | At alle moduler frasorterer den samme deal-dubletdefekt |
| `test_valuta.py` | Valutaomregning |
| `test_nav_recent.py` | Favoritter og senest besøgt, inkl. at et menupunkt man har mistet adgang til, falder ud |

Testene kører automatisk på pull requests og på push til `main`
(`.github/workflows/tests.yml`, Ubuntu + Python 3.13).

**En ny test hører til, når** du retter en fejl, der ikke blev fanget, eller
tilføjer et adgangskrav. Adgangstests er billige: tilføj rollen i
`test_smoke.py`, så kan den næste ikke komme til at åbne modulet ved et uheld.

---

## 6. Sådan hænger koden sammen

### Filkort

```
app.py               FastAPI-appen: middleware, login, forside, søgning, favoritter.
                     Samler alle moduler med include_router().
auth.py              Brugere, roller, hold og adgangslogik. Opretter hub-tabellerne.
db.py                Al databaseadgang. Connection-pool + datokompatibilitet.
env.py               Indlæser .env fra en absolut sti. Kaldes af alt.
nav_utils.py         CATEGORIES — registret over alle menupunkter og deres adgangskrav.
constants.py         Brands, sites og pipelines. Én kilde til sandheden på tværs af moduler.
log_setup.py         logs/hub.log (drift) og logs/audit.log (hvem gjorde hvad).
personalization.py   Favoritter og senest besøgt.
usage_tracking.py    Sidevisninger til HubUsageLog, via en baggrundstråd.
os_trust.py          Udgående HTTPS gennem virksomhedens TLS-inspektion.
preflight.py         Tjekker opsætningen før start. Kør den ved enhver tvivl.
run_server.py        Opstart i drift. Erstatter `uvicorn app:app --reload`.
create_admin.py      Opretter den første bruger.
cert_match.py        Hvilken nøglefil hører til hvilket certifikat.

moduler/modul_<navn>/
    router.py        HTTP-endpoints, adgangstjek, rendering
    queries.py       SQL — al databaselæsning for modulet
    (øvrige)         modulspecifik logik, fx retention/risiko.py, klippekort/pipedrive_api.py

moduler/modul_maintenance/
    dataloads.py     Kørselsloggen. ORIGINALEN af en fil der ligger identisk i
                     pipedrive_sync, ACV_updater_pipedrive, currencycollector og
                     ProgrammaticFinansSales. Rettes den her, skal kopierne
                     opdateres — tests/test_maintenance.py fejler ellers.
    queries.py       Katalog over datakilder med deres planlagte køretider, plus
                     vurderingen af om en kørsel er i hus.

templates/           Jinja2-skabeloner. _sidebar.html er fælles for alle sider.
static/              CSS og JavaScript. Ingen build — filerne serveres som de er.
tests/               pytest. Kører uden database.
screen_configs.json  Navngivne skærmopsætninger til rotationen (skrives af UI'et).
```

### Et request fra ende til anden

1. **Session** — `SessionMiddleware` afkoder den signerede cookie.
2. **CSRF** — skrivende requests (POST/PUT/PATCH/DELETE) skal have en `Origin`,
   der matcher vores egen host eller står i `HUB_TRUSTED_ORIGINS`. Ellers 403.
3. **Usage** — sidevisningen lægges i en kø; en baggrundstråd skriver den til
   `HubUsageLog`, så databasen ikke sinker svaret.
4. **Routeren** kalder `Depends(get_current_user)`. Er man ikke logget ind,
   kastes `RequiresLoginException`, og man sendes til `/login?next=…` —
   destinationen bevares hele vejen rundt.
5. **Adgang** — `resolve_resource_access(user, item_id, min_role, …)` afgør
   `none` / `read` / `write`. Rækkefølgen er: bruger-override → rolle-override →
   rang → brand → holdkrav → udelukkede roller.
6. **Data** — `queries.py` henter en forbindelse fra poolen med `get_conn()`,
   kører sin SQL og lukker (dvs. leverer forbindelsen tilbage til poolen).
7. **Svar** — HTML fra Jinja, eller JSON til dashboardets fetch-kald. Fejler
   noget undervejs, fanger en global handler det: HTML-sider får en fejlside,
   data-endpoints får `{"error": …}`, så dashboardet kan vise "Data
   utilgængelig" i stedet for at gå i stå. Tracebacken ligger i `hub.log`.

### Sådan tilføjer du et modul

1. Opret `moduler/modul_<navn>/` med `__init__.py`, `router.py` og `queries.py`.
   Kopiér et lille eksisterende modul — `modul_marketing` er en god skabelon.
2. `router = APIRouter(prefix="/tools/<navn>", tags=["<Navn>"])`.
3. Registrér den i `app.py` med `app.include_router(...)`.
4. Tilføj menupunktet i `CATEGORIES` i `nav_utils.py`.
5. **`min_role` i `nav_utils.py` skal matche modulets eget krav i `router.py`.**
   Sættes den lavere i navigationen, får brugeren et menupunkt, der svarer 403.
   Det er den hyppigste fejl her.
6. Skabelonen skal arve sidebaren: kald `register_nav_globals(templates)` på din
   `Jinja2Templates`-instans, som de andre moduler gør.
7. Tilføj sti + forventet status pr. rolle i `tests/test_smoke.py`.

---

## 7. Drift

Appen startes med `run_server.py` — aldrig `uvicorn --reload` på serveren.
Genindlæseren genstarter appen, hver gang logrotationen skriver i `logs/`.

### Som Windows-tjeneste

Med [NSSM](https://nssm.cc/):

```bash
nssm install IntomediaHub "C:\Users\admin_cs\Documents\JPPOLBM Tools\one_stop_shop\.venv\Scripts\python.exe" run_server.py
```

Sæt derefter i NSSM **Startup directory** til projektmappen og **Log on** til den
konto, der har adgang til SQL Server og snapshot-mapperne. Uden korrekt
arbejdsmappe finder `Jinja2Templates("templates")` og `StaticFiles("static")`
ikke deres filer.

Alternativt en Task Scheduler-opgave med trigger *At startup*, samme program og
argument, og "Start in" sat til projektmappen (uden anførselstegn — Task
Scheduler fejler på citerede "Start in"-stier). Slå samtidig *Stop the task if it
runs longer than* fra på Settings-fanen; den står som standard på 3 dage og
lukker ellers sitet uden varsel.

### Logs

| Fil | Indhold |
|---|---|
| `logs/hub.log` | Drift: fejl, advarsler, tracebacks. Her slår man op, når et dashboard er tomt |
| `logs/audit.log` | Hvem gjorde hvad: logins, bruger-/rolle-/holdændringer, budgetændringer |

Begge roteres automatisk. `HUB_LOG_DIR` kan flytte mappen, hvis projektmappen er
skrivebeskyttet for tjenestekontoen.

### HTTPS

Certifikat og nøgle sættes i `.env`, ikke som uvicorn-flag på kommandolinjen —
så gælder de både for NSSM, Task Scheduler og en manuel start:

```
HUB_SSL_KEYFILE=C:\cert\jpbm.key
HUB_SSL_CERTFILE=C:\cert\jpbm.crt
```

`run_server.py` tjekker begge filer, før uvicorn startes, og skriver en konkret
fejl, hvis en fil mangler, ikke kan læses af tjenestekontoen, eller er DER/PFX i
stedet for PEM. Uden det tjek dør processen tavst, og Task Scheduler viser kun
`0x1`.

Indeholder `.crt` kun serverens eget certifikat, mangler klienterne
mellemcertifikaterne. Læg hele kæden i én PEM-fil (eget certifikat først,
derefter intermediates), eller sæt `HUB_SSL_CA_CERTS` til en separat kædefil.
Tjek resultatet udefra:

```bash
openssl s_client -connect servernavn:8000 -servername servernavn -showcerts
```

Er du i tvivl om, hvilken nøgle der hører til hvilket certifikat:

```bash
.venv\Scripts\python.exe cert_match.py C:\cert
```

### Bag en reverse proxy

App Proxy oversætter `Host` til den interne adresse, mens browserens `Origin`
forbliver den eksterne. CSRF-tjekket sammenligner netop de to, så uden en
hvidliste afvises alle logins med 403. Sæt i `.env`:

```
HUB_TRUSTED_ORIGINS=https://jpbmdatawarehouse-jppol.msappproxy.net
TRUST_PROXY=1
```

Listen er bevidst eksplicit. At udlede den af `X-Forwarded-Host` ville lade
enhver, der kan nå porten, sætte sin egen værdi — og så beskytter tjekket ikke
længere mod noget. `TRUST_PROXY=1` sørger for, at klient-IP i audit-loggen og
login-rate-limiteren er brugerens og ikke connector-serverens.

### Hvad serveren skal kunne nå

| Retning | Vært | Bruges af |
|---|---|---|
| SQL | `DB_SERVER` (1433) | alt |
| HTTPS | `api.pipedrive.com` | Portfolio Alignment, Klippekort |
| SMTP | `smtp.office365.com:587` | Barselsplanlæggerens godkendelsesmails |

`preflight.py` prøver at åbne forbindelserne, så en manglende firewall-åbning
opdages, før en bruger rammer den.

---

## 8. Fejlfinding

**Start altid med `preflight.py`.** Den finder de fleste af nedenstående på ti
sekunder.

| Symptom | Årsag | Handling |
|---|---|---|
| Alle dashboards viser "Data utilgængelig" | Database uden svar, eller `.env` blev ikke fundet | `preflight.py`. Tjek `hub.log` |
| Appen starter ikke: `SECRET_KEY mangler` | Nøglen er ikke sat | `python -c "import secrets; print(secrets.token_hex(32))"` → `.env` |
| Login virker, men ingen data nogen steder | Forkert database eller manglende læserettigheder | Tjek at `DB_NAME` er `INTOMEDIA`, og at kontoen har adgang |
| Ét modul er tomt, resten virker | Modulets tabel eller snapshot-mappe mangler | `preflight.py` viser hvilken |
| Alle logins afvises med 403 | CSRF-tjekket: `Origin` matcher ikke `Host` | Sæt `HUB_TRUSTED_ORIGINS` (§7) |
| Login-rate-limiteren blokerer alle | `TRUST_PROXY=1` uden en rigtig proxy foran — alle ser ud som samme IP | Fjern `TRUST_PROXY` |
| Blokeret efter for mange forsøg | 5 fejlede logins fra samme IP inden for 15 min. | Vent, eller genstart appen (tælleren er in-memory) |
| Templates/static findes ikke | Tjenesten kører med forkert arbejdsmappe | Sæt "Startup directory"/"Start in" til projektmappen |
| Én bestemt side giver 500 med `TemplateNotFound` efter en udrulning | En ny .html-fil kom ikke med — `git commit -am` tager kun ændrede, **sporede** filer med | `git status` på udviklingsmaskinen. `git add` de untrackede filer og udrul igen. `preflight.py` (§4) melder dem ved navn |
| Task Scheduler viser `0x1`, intet i loggen | Næsten altid certifikatfilerne | Kør `run_server.py` manuelt — den skriver den konkrete fejl |
| Porten lytter, men forbindelsen dør i håndtrykket | `truststore` er blevet injiceret globalt igen | Se §9 |
| Sitet lukkede af sig selv efter et par dage | Task Schedulers "Stop the task if it runs longer than" | Slå den fra |
| Datoer eller sammenligninger opfører sig sært | Datotypen har ændret sig | Sæt `DB_DATE_AS_STRING=1` (§9) |
| Et menupunkt giver 403 | `min_role` i `nav_utils.py` er lavere end i modulets router | Ret dem, så de matcher |
| To dashboards viser forskellige tal for samme sælger | Spejlkopier eller forskellige adm.-filtre | Se `constants.mirror_exclude_sql()` og `test_spejlkopier.py` |

---

## 9. Det man skal vide

Nogle ting i denne kode ser forkerte ud, indtil man kender grunden. De er alle
bevidste, og de går i stykker, hvis nogen "rydder op".

### Datoer kommer som strenge — med vilje

Koden havde `tds_version="7.0"` hårdkodet fire steder. **TDS 7.0 kan ikke TLS**,
og serveren kræver kryptering, så versionen *skulle* op på 7.4.

Det har en konsekvens, som ikke er til at se: TDS 7.0 kendte ikke `date` og
`datetime2` (de kom i 7.3), så SQL Server sendte dem som **strenge**
(`'2026-08-14'`). Fra 7.4 kommer de som rigtige `date`-objekter. Modulerne er
skrevet til strengene — `modul_retention` sammenligner ligefrem datoer som tekst
— og en forkert type viser sig ikke som en fejl, nogen ser. Den viser sig som
"Data utilgængelig" i et dashboard.

Derfor er transporten og datatyperne skilt fra hinanden. `db.py` pakker cursors
ind, så DATE-kolonner leveres som strenge præcis som før:

| `DB_DATE_AS_STRING` | Betydning |
|---|---|
| `1` (standard) | DATE-kolonner kommer som `'YYYY-MM-DD'` — som TDS 7.0 |
| `0` | Rigtige `date`-objekter |

**Broen er midlertidig.** Sådan kommer du af med den: sæt
`DB_DATE_AS_STRING=0`, gå modulerne igennem ét for ét, og ret de steder, der
regner på datoer som tekst. De fleste queries konverterer allerede selv i SQL'en
(`CONVERT(NVARCHAR(10), ...)` optræder ~60 steder) og er upåvirkede uanset
indstillingen. Kendte steder, der skal ses på først:

* `moduler/modul_retention/queries.py` — sammenligner datoer som tekst; se noten
  ved `db_opsigelser`.
* `moduler/modul_retention/outcomes.py` — normaliserer selv og tager **begge**
  former. Den er allerede klar.

Én ting kan broen ikke: `DATETIME2`-kolonner kom også som strenge under 7.0, men
de er umulige at skelne fra almindelige `DATETIME` (begge ankommer som
`datetime`), så dem leveres som `datetime` uanset indstilling. Det kendte sted,
der læser sådanne felter (`outcomes.py`), håndterer begge.

### Udgående HTTPS må ikke patche `ssl` globalt

Pipedrive-modulerne kaldte tidligere `truststore.inject_into_ssl()` for at komme
gennem Zscalers TLS-inspektion. Den udskifter `ssl.SSLContext` i hele processen —
også den, uvicorn bygger for at *terminere* TLS — og så forsøger serveren at
verificere klientens certifikat. Browsere sender ingen, og hver forbindelse
afbrydes med `Peer sent no certificates to verify`: porten lytter, TCP
accepteres, og håndtrykket dør uden en linje i loggen.

Brug `os_trust.session()` til udgående kald. Den scoper trust-storen til de
requests, der har brug for den.

### `.env` findes via en absolut sti, ikke via arbejdsmappen

`load_dotenv()` uden argumenter leder fra den **aktuelle arbejdsmappe** og
opefter. En tjeneste starter typisk med en anden arbejdsmappe, og så blev `.env`
ikke fundet — appen kørte videre med tomme værdier og viste tomme dashboards
uden en eneste fejl. Derfor kalder alt `env.load_env()`, som binder stien til
filens egen placering. Kald aldrig `load_dotenv()` direkte i et nyt modul.

Samme mønster: `pymssql.connect()` findes kun i `db.py`, og `_required()`
navngiver den manglende variabel. Uden det kom en manglende `DB_SERVER` ud som
en kryptisk login-fejl.

### Adgangskravet står to steder og skal matche

`min_role` findes både på menupunktet i `nav_utils.py` og i modulets egen router.
De er ikke koblet sammen. Sættes navigationens krav lavere end routerens, får
brugeren et menupunkt, der svarer 403. Kommentarerne i `nav_utils.py` markerer de
steder, hvor det allerede er gået galt en gang.

### Datastatus måler mod en plan, der står i koden — ikke i Task Scheduler

`/tools/maintenance/` svarer på "kan jeg stole på tallet, jeg kigger på". Det
kræver to oplysninger, der ligger hvert sit sted:

| Hvad | Hvor |
|---|---|
| Hvornår data **skulle** have været der | `KILDER` i `moduler/modul_maintenance/queries.py` |
| Hvornår de **faktisk** kom | `dbo.HubDataLoads`, én række pr. kørsel |

Planen er skrevet af én gang fra Task Scheduler på serveren. Appen kan ikke
læse Windows' opgavebibliotek, så **flytter nogen en opgave i Task Scheduler,
skal `KILDER` rettes i samme ombæring** — ellers måler siden mod en forventning,
der ikke længere gælder, og den fejl ser ud som et grønt felt.

Det er valgt frem for slet ikke at have en forventning. Uden en planlagt
køretid kan siden kun vise en alder, og en alder alene siger ikke, om 14 timer
er for meget: for den frekvente Pipedrive-sync er det katastrofalt, for
retention-snapshottet er det normalt.

**Kørselsloggen er ikke pynt.** To af tabellerne — `valutakurser` og
`ProgrammaticSales` — har ingen tidsstempelkolonne overhovedet, og `PipeDrive_ACV`
skriver kun ændrings-rækker, så dens `updated_at` står stille på en dag uden
ændringer. For dem alle tre kan "nyeste dato i tabellen" ikke skelne de tre
tilstande, der betyder vidt forskellige ting:

    scriptet kørte og hentede nye rækker        (alt er som det skal være)
    scriptet kørte, men der var intet nyt       (helt normalt i en weekend)
    scriptet kørte slet ikke, eller det fejlede (nogen skal kigge på det)

Falder en række tilbage på tabellens eget tidsstempel, siger dashboardet det
med mærket **fallback**. Den skelnen går tabt dér, og siden skjuler det ikke.

Kilderne står i **tre tabeller**, fordi de tre slags ikke kan sammenlignes: en
scheduled task måles mod et klokkeslæt, en fil mod en kadence, og en manuel
upload mod ingenting. I én fælles tabel ville kolonnerne "Planlagt" og "Næste"
stå tomme på over halvdelen af rækkerne — og et tomt felt ligner en fejl.
Gruppen **udledes** af kilden (`gruppe_for()`), så katalog og gruppering ikke
kan drive fra hinanden.

### Programmatic-salg måles mod i går, og et nul er en fejl

To ting gør netop den kilde anderledes, og begge er tavse fejl, hvis de glemmes:

**Kørslen henter altid dagen før.** `save_rows()` sorterer dags dato og
fremtidige datoer fra, fordi de tal ikke er komplette endnu. Nyeste `[Date]` =
i går er derfor det RIGTIGE billede efter morgenens kørsel. Uden
`data_forsinkelse_dage: 1` i `KILDER` ville rækken melde forsinket hver eneste
dag — og en side, der råber hver dag uden grund, er en side, folk holder op med
at kigge på.

**Beløbet på nyeste dato må aldrig være 0.** Scrapingen kan nå igennem uden en
eneste fejl og alligevel aflevere en tom rapport: Relevant Digital har ikke
lukket dagen endnu, tabelvisningen skiftede, sessionen udløb. Så skrives et nul,
der ser ud som en dag helt uden omsætning — usynligt i alt, der summerer.
Derfor står nulkontrollen to steder, og de siger det samme:

| Hvor | Hvad |
|---|---|
| `nulkontrol_sql` i `KILDER` | Dashboardet spørger databasen direkte og står rødt |
| `save_rows()` i `ProgrammaticFinansSales` | Kørslen markerer sig selv som fejlet i loggen |

Scriptets exitkode er uændret (`log.fail()` kaster ikke), så Task Scheduler ser
det samme som før. Rettelsen er at trigge en ny kørsel.

### `dataloads.py` er den samme fil fem steder

Kørselsloggen skrives af fire scripts, der hver har deres eget repo og deres
egen `db.py`. Filen importerer kun `get_conn` (og valgfrit `_P`), som alle fem
projekter har, så den kan lægges uændret over:

```
one_stop_shop/moduler/modul_maintenance/dataloads.py   ← originalen
pipedrive_sync/dataloads.py
ACV_updater_pipedrive/dataloads.py
currencycollector/dataloads.py
ProgrammaticFinansSales/dataloads.py
```

Rettes originalen, skal kopierne opdateres. `tests/test_maintenance.py` fejler,
hvis de driver fra hinanden — men kun hvis sidemapperne er tjekket ud ved siden
af dette repo; ellers springes tjekket over.

### Månedsrapportens ÅTD bygger på en gemt baseline — ikke på rådata

ÅTD-tabellen i `/tools/admin-nysalg/` lægger **ikke** hele årets Zuora-bevægelser
sammen. Den lægger gemte månedstal sammen. Grunden er reviewet: direktøren retter
gross in/out, sætter administrative andele og udelader rækker, og de rettelser
findes kun i det run hvor de blev lavet. Skulle ÅTD regnes forfra hver måned,
skulle hele årets bevægelser rettes igennem igen hver gang.

Derfor gemmer rapportgenereringen månedens Actual Sale og Actual Churn pr. brand
i `admin_nysalg_baseline` (`source='run'`), og næste måneds ÅTD-tabel summerer
dem. Tal man selv har tastet på `/tools/admin-nysalg/baseline` står som
`source='manual'` og **overskrives aldrig** af en rapportkørsel — ellers ville en
bevidst korrektion forsvinde, næste gang måneden blev kørt om.

To følger, som ikke er til at se i koden:

* **Måneder uden baseline tæller som 0.** De listes som en advarsel i reviewet og
  i rapporten i stedet for at fejle — en rapport skal kunne laves, selvom januar
  mangler. Første gang værktøjet bruges, skal årets tidligere måneder tastes ind
  (eller hentes med «Hent fra database» og gemmes).
* **Baseline overlever, at et run slettes.** Rækkerne peger på `run_id`, men
  ryddes ikke med — det rapporterede tal står ved magt, også når kildekørslen er
  væk. Skal de af, slettes måneden på baseline-siden.

Budgettet går derimod **uden om** baseline: det hentes live fra
`BudgetsIntoMedia` for hele ÅTD-perioden. Det er et fast tal pr. måned, kræver
intet review, og skal følge med, hvis budgettet rettes bagudrettet.

### Forecast-kolonnen er hardcodet — og står kun på månedstabellen

Tallene i `moduler/modul_admin_nysalg/forecast.py` er ledelsens forecast og
findes ingen steder i databasen. De er tastet ind én gang, pr. brand pr. måned.
Skal de opdateres, er det den fil, der rettes; `_validate()` fejler ved import,
hvis et brand-label er stavet forkert, så en tastefejl ikke bare giver en tom
kolonne.

Forecastet står bevidst **ikke** i ÅTD-tabellen. Det er kun aftalt for juli–
december, så en ÅTD-periode fra januar ville sætte et helt års realiserede tal op
mod et halvt års forecast. Kolonnen hører til den enkelte måned og står derfor
kun i månedstabellen og i «Performance pr. måned».

NB: ledelsens regneark opgiver Abo Watch DK til 2.470.000 for juli–december,
mens månedstallene i filen summer til 2.500.000. Månedstallene er brugt som de
står — afvigelsen på 30.000 er i kildearket.

### Øvrigt værd at vide

* **Ingen build.** CSS og JavaScript i `static/` serveres, som de er. Ingen npm,
  ingen bundler. Hard refresh efter ændringer.
* **Ingen migrationer.** Tabellerne oprettes idempotent ved opstart
  (`init_db()` og modulernes egne `init_*_db()`). En ny kolonne tilføjes ved at
  skrive et `IF NOT EXISTS`-statement samme sted.
* **`HUB_WORKERS` bør blive på 1.** Hver proces har sin egen connection-pool og
  sine egne baggrundstråde — der deles ingen tilstand. Sessioner ligger i
  signerede cookies og overlever både genstart og flere workers, så det *virker*;
  hæv kun, hvis CPU'en faktisk er flaskehalsen.
* **`/docs` er slået fra i produktion** med vilje. `openapi.json` er et komplet
  kort over alle endpoints og datastrukturer. Kun `DEV_MODE=1` åbner den.
* **Den gamle udgave** under `intomedia\Operations - Dokumenter\Business
  Analysis\PythonScripts\one_stop_shop` er urørt. **Kør ikke begge mod samme
  database samtidig** — begge opretter hub-tabeller ved opstart og skriver usage
  og besøg.

---

## 10. Ejerskab

| | |
|---|---|
| **Vedligeholdes af** | Business Analysis, Intomedia |
| **Database** | `INTOMEDIA` i SQL Server (`DB_SERVER`) |
| **Kilder** | Zuora (abonnementer/ARR), Pipedrive (deals), filbaserede snapshots. Deres friskhed kan ses på `/tools/maintenance/` |
| **Tests i CI** | GitHub Actions, `.github/workflows/tests.yml` |
| **Status** | I drift på `172.29.11.31`. Migreringen fra den gamle netværksmappe-udgave er gennemført; datobroen (§9) er stadig aktiv og bør afvikles modul for modul |

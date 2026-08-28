# Intomedia Hub — server-udgave

FastAPI-app med 16 moduler oven på `INTOMEDIA` i SQL Server. Denne udgave er
tilpasset til at køre på serveren.

> Den gamle udgave under
> `intomedia\Operations - Dokumenter\Business Analysis\PythonScripts\one_stop_shop`
> er urørt og kører videre, indtil skiftet er gennemført. Kør ikke begge mod
> samme database samtidig — begge opretter hub-tabeller ved opstart og skriver
> usage/besøg.

## Kom i gang

```bash
python -m venv .venv
.venv\Scripts\python.exe -m pip install --no-index --find-links=..\wheelhouse\one_stop_shop -r requirements.txt
copy .env.example .env
```

Udfyld `.env` (mindst `DB_SERVER`, `DB_USER`, `DB_PASSWORD`, `SECRET_KEY` og de
to snapshot-mapper), og kontrollér så hele opsætningen:

```bash
.venv\Scripts\python.exe preflight.py
```

Den går database, tabeller, mapper og udgående forbindelser igennem og siger
hvad der mangler. Exit 0 = klar. Er `HubUsers` tom, opret den første bruger:

```bash
.venv\Scripts\python.exe create_admin.py
```

Start appen:

```bash
.venv\Scripts\python.exe run_server.py
```

## Det vigtigste ved denne migrering: datoer

Koden havde `tds_version="7.0"` hårdkodet fire steder. **TDS 7.0 kan ikke TLS**,
og den nye server kræver kryptering — så versionen *skal* op på 7.4.

Det har en konsekvens som ikke er til at se: TDS 7.0 kendte ikke `date` og
`datetime2` (de kom i 7.3), så SQL Server sendte dem som **strenge**
(`'2026-08-14'`). Fra 7.4 kommer de som rigtige `date`-objekter. Modulerne er
skrevet til strengene — `modul_retention` sammenligner ligefrem datoer som
tekst — og en forkert type viser sig ikke som en fejl nogen ser. Den viser sig
som "Data utilgængelig" i et dashboard.

Derfor er transporten og datatyperne skilt fra hinanden. `db.py` pakker cursors
ind, så DATE-kolonner leveres som strenge præcis som før:

| `DB_DATE_AS_STRING` | Betydning |
|---|---|
| `1` (standard) | DATE-kolonner kommer som `'YYYY-MM-DD'` — som TDS 7.0. **Brug denne ved flytningen.** |
| `0` | Rigtige `date`-objekter. |

Broen er midlertidig. Sådan kommer du af med den, når hubben ellers kører
stabilt på serveren: sæt `DB_DATE_AS_STRING=0`, gå modulerne igennem ét for ét,
og ret de steder der regner på datoer som tekst. De fleste queries konverterer
allerede selv i SQL'en (`CONVERT(NVARCHAR(10), ...)` optræder ~60 steder) og er
upåvirkede uanset indstillingen. Kendte steder der skal ses på først:

- `moduler/modul_retention/queries.py` — modulet sammenligner datoer som tekst;
  se noten ved `db_opsigelser`.
- `moduler/modul_retention/outcomes.py` — normaliserer selv og tager **begge**
  former. Den er allerede klar.

Én ting kan broen ikke: `DATETIME2`-kolonner kom også som strenge under 7.0, men
de er umulige at skelne fra almindelige `DATETIME` (begge ankommer som
`datetime`), så dem leveres som `datetime` uanset indstillingen. Det kendte sted
der læser sådanne felter (`outcomes.py`) håndterer begge.

## Øvrige ændringer

| Før | Nu | Hvorfor |
|---|---|---|
| `load_dotenv()` i 19 filer | `env.load_env()` — absolut sti, `HUB_ENV` kan flytte filen | `load_dotenv()` leder fra den **aktuelle arbejdsmappe**. En tjeneste starter typisk med en anden arbejdsmappe, og så blev `.env` ikke fundet — appen kørte videre med tomme værdier |
| `pymssql.connect()` fire steder | Kun i `db.py`; `new_connection()` giver modulerne deres egne timeouts | Ét sted at rette server, login og TDS-version |
| `os.getenv("DB_SERVER")` uden tjek | `_required()` navngiver den manglende variabel | Manglede den, kom fejlen som en kryptisk login-fejl → "Data utilgængelig" |
| `uvicorn app:app --reload` | `run_server.py` | `--reload` genstarter appen når logrotationen skriver i `logs/`. Host/port/workers kommer fra miljøet |
| `logs/` altid ved koden | `HUB_LOG_DIR` kan flytte den | Projektmappen kan være skrivebeskyttet for tjenestekontoen |
| Intet opstartstjek | `preflight.py` | Appen fejler ikke ved opstart når noget mangler — den viser bare tomme dashboards |
| To navne for proxy-tillid | Kun `TRUST_PROXY` | Uvicorns `proxy_headers` og appens rate-limiter kan ikke længere være uenige |

`TRUST_PROXY=1` **kun** bag en reverse proxy der selv sætter `X-Forwarded-For`.
Uden proxy kan headeren spoofes, og så er login-rate-limiteren virkningsløs.

## Hvad serveren skal kunne nå

| Retning | Vært | Bruges af |
|---|---|---|
| SQL | `DB_SERVER` (1433) | alt |
| HTTPS | `api.pipedrive.com` | Portfolio Alignment, Klippekort |
| SMTP | `smtp.office365.com:587` | Barselsplanlæggerens godkendelsesmails |

`preflight.py` prøver at åbne forbindelserne, så en manglende firewall-åbning
opdages før en bruger rammer den. **SMTP-porten er sandsynligvis ikke åben** —
det er den, der skal bestilles ud over de allerede åbnede.

Snapshot-mapperne er filbaserede: `ZUORA_SNAPSHOT_DIR` og `USAGE_SNAPSHOT_DIR`
skal peges på et sted serveren faktisk kan læse — standardstierne ligger i en
brugerprofils OneDrive-mappe og findes ikke der. Begge må indeholde flere stier
adskilt med `;`, så samme `.env` kan bruges lokalt og på serveren.

## Som Windows-tjeneste

Med [NSSM](https://nssm.cc/):

```bash
nssm install IntomediaHub "C:\Users\admin_cs\Documents\JPPOLBM Tools\one_stop_shop\.venv\Scripts\python.exe" run_server.py
```

Sæt derefter i NSSM: **Startup directory** til projektmappen, og
**Log on** til den konto der har adgang til SQL Server og snapshot-mapperne.
Uden korrekt arbejdsmappe finder `Jinja2Templates("templates")` og
`StaticFiles("static")` ikke deres filer.

Alternativt en Task Scheduler-opgave med trigger *At startup*, samme program og
argument, og "Start in" sat til projektmappen.

## Tests

```bash
.venv\Scripts\python.exe -m pytest tests/ -q
```

218 tests, ingen database krævet — `tests/conftest.py` peger `DB_SERVER` på et
navn der ikke findes, så appen falder tilbage til sine defaults.
`tests/test_db_datokompat.py` dækker datobroen: hvad der konverteres, hvad der
ikke gør, alle fetch-veje, og at `DB_DATE_AS_STRING=0` slår den fra.

## Fejlfinding

| Problem | Løsning |
|---|---|
| Alle dashboards viser "Data utilgængelig" | `preflight.py` — næsten altid database eller `.env` der ikke blev fundet |
| `SECRET_KEY mangler` | Generér: `python -c "import secrets; print(secrets.token_hex(32))"` |
| Login virker, men intet data | Tjek at `DB_NAME` peger på `INTOMEDIA`, og at kontoen har læseadgang |
| Ét modul er tomt, resten virker | Modulets tabel eller snapshot-mappe mangler — `preflight.py` viser hvilken |
| Datoer eller sammenligninger opfører sig sært | Sæt `DB_DATE_AS_STRING=1` (se ovenfor) |
| Templates/static findes ikke | Tjenesten kører med forkert arbejdsmappe |
| Login-rate-limiter blokerer alle | `TRUST_PROXY=1` uden en rigtig proxy foran — alle ser ud som samme IP |

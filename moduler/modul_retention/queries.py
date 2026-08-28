from db import get_conn
import logging

logger = logging.getLogger(__name__)

# dbo.PipeDrive_ACV's `brand` → Pipedrive's `account`.
#
# Der findes tre vokabularer for samme forretningsenhed: Pipedrive's `account`
# ('watch_medier'), Zuora's `brand` ('Watch', 'Finans' — se SCOPE_BY_ZUORA_BRAND
# i modul_portfolio_alignment) og ACV-tabellens `brand` ('Watch DK', 'Finans').
# Kun de to første var i brug før retention-modulet, så denne tredje ligger her
# frem for i alignment-modulets ACCOUNT_SCOPES.
#
# Mappingen er verificeret empirisk 2026-08-04 ved org_name-match mellem
# dbo.retention og PipeDrive_ACV: parrene herunder matcher i 99,1-99,6% af
# tilfældene, mens ALLE kryds-par matcher i 0% (fx account='monitor' mod
# brand='Watch DK', 987 par, 0 navne-match). Det skal tages alvorligt, fordi
# org_id kun er unikt inden for én account — en forkert brand-oversættelse
# kobler to fremmede virksomheder sammen (org_id 3995 er både 'Sorø Akademis
# Skole' i Monitor og 'Ret og Råd Sekretariatet A/S' i Watch DK).
#
# watch_de og marketwire står bevidst IKKE på listen: PipeDrive_ACV har ingen
# rækker for dem (brand-kolonnen indeholder kun de fem værdier herunder), så de
# kunder kan ikke få et ARR-tal og tælles alene firmabredt.
ACV_BRAND_TO_ACCOUNT: dict[str, str] = {
    "Watch DK": "watch_medier",
    "Finans":   "watch_medier",
    "Monitor":  "monitor",
    "Watch NO": "watch_no",
    "Watch SE": "watch_se",
}

# ACV's site-vokabular mod dbo.retentions. ACV gemmer sitet UDEN landekode
# ('FinansWatch') og landet i brand ('Watch DK'), mens dbo.retention gemmer det
# samlet ('FinansWatch DK'). Reglen er et suffiks pr. brand plus syv navngivne
# undtagelser.
#
# Maalt 2026-08-18 mod juli 2026, FOER den danske afgraensning 25-08 (de
# 15.203 var derfor UFILTRERET, alle lande med): alle 42 (brand, site)-par i
# ACV rammer et site der findes i dbo.retention, og 15.039 af 15.203
# abonnementer kan dermed faa deres EGET beloeb i stedet for kundens ARR delt
# med antal sites. Selve funktionen er stadig ufiltreret med vilje (se dens
# docstring), saa daekningsprocenten (98,9%) staar til troende, men totalen
# 15.203 maa ikke sammenlignes med db_abonnementer's danske 13.044.
ACV_SITE_SUFFIKS: dict[str, str] = {
    "Watch DK": " DK",
    "Watch NO": " NO",
    "Watch SE": " SE",
    "Monitor":  "monitor",
}

# Undtagelserne er NAVNGIVNE og ikke regelbaserede med vilje. De tre
# monitor-navne mangler et fuge-s ('Sundhed' -> 'Sundhedsmonitor'), som ingen
# regel kan udlede uden at gaette, og en gaettet fuge-s-regel ville ramme forkert
# paa det naeste nye site. De tre oevrige er helt egne navne.
ACV_SITE_UNDTAGELSER: dict[tuple[str, str], str] = {
    ("Watch DK", "WatchMedier"):   "Watch Medier DK",
    ("Watch NO", "Shifter"):       "Shifter",
    ("Monitor",  "Monitormedier"): "Monitormedier",
    ("Monitor",  "Idræt"):         "Idrætsmonitor",
    ("Monitor",  "Uddannelse"):    "Uddannelsesmonitor",
    ("Monitor",  "Sundhed"):       "Sundhedsmonitor",
    ("Finans",   "Finans"):        "FINANS DK",
}


def acv_site_til_retention(brand: str | None, site: str | None) -> str | None:
    """ACV's (brand, site) oversat til dbo.retention.sites. None ved ukendt brand.

    Kommer der et nyt brand i ACV, giver funktionen None frem for at gaette.
    Abonnementet falder saa tilbage til den gamle ligedeling, og fejlen bliver
    et beloeb der er lidt for groft i stedet for et beloeb paa det FORKERTE site.

    KENDT FAELDE, verificeret 2026-08-18: parret ('Watch DK', 'Finans') findes
    1.626 gange i PipeDrive_ACV, men NUL af dem er den nyeste raekke for sit
    (org_id, site). Kalderen filtrerer paa rk = 1, saa parret rammer aldrig
    funktionen i drift. Sker der en redigering, der loefter en af dem til nyeste,
    mapper reglen den til 'Finans DK', som ikke findes i dbo.retention, og
    beloebet falder tavst tilbage til ligedeling. Det er sikkert, men det er
    tavst: rammer daekningen under 98,9%, er det her man skal se foerst.
    """
    if brand is None or site is None:
        return None
    egen = ACV_SITE_UNDTAGELSER.get((brand, site))
    if egen:
        return egen
    suffiks = ACV_SITE_SUFFIKS.get(brand)
    return site + suffiks if suffiks else None


# Aktivt team-medlemskab — samme datovindue som get_led_teams i
# modul_saelger_portfolio, så en risikoliste og en sælger-dropdown ikke kan
# blive uenige om hvem der sidder i hvilket team.
_AKTIVT_MEDLEMSKAB = """
    tm.start_date <= CONVERT(varchar(10), GETDATE(), 23)
    AND (tm.end_date IS NULL OR tm.end_date >= CONVERT(varchar(10), GETDATE(), 23))
"""

# Kun B2B: hver Pipedrive-account har én organisation ved navn 'Web Sale'
# (watch_medier har også 'Web Sale Euro'), som samler alle
# B2C-selvbetjenings-abonnementer i én kunstig kunde. Mønsteret er kodebasens
# eget (PD_WEB_SALE_NAME_LIKE i modul_portfolio_alignment).
#
# Verificeret 2026-08-04: der er NUL Web Sale-rækker i HELE dbo.retention, og
# ingen rækker med NULL org_name. Filteret ændrer altså ingen tal — heller ikke
# historiske — men står som spærre, fordi PipeDrive_ACV DERIMOD har dem med
# 6,07 mio. kr. Begynder en Web Sale-deal at ramme en af retention-viewets
# pipelines, ville de kroner ellers dukke op som én kunde i toppen af listen.
_KUN_B2B = " AND ISNULL(r.org_name, '') NOT LIKE 'Web Sale%' "

# Kun danske accounts. Retention-teamet er dansk (aftalt 2026-08-20, udvidet til
# HELE modulet 2026-08-25).
#
# EXCLUDE og ikke include, med vilje. En include-liste ville tavst droppe en NY
# dansk account, og en tabt dansk kunde kan ingen se. En ny udenlandsk account
# dukker derimod op som stoej, og stoej kan man se og rette.
#
# AFGRAENSNINGEN ER REN GEOGRAFI. Alle 20 watch_medier-sites er danske (inkl.
# FINANS DK), og der findes ikke ét .com-site i HELE dbo.retention. De
# internationale udgaver bor kun i Snowplow, hvor zones.BRAND_FAMILIE folder
# .com ned paa .dk, saa et account-filter kan ikke ramme den internationale
# sektion.
#
# LISTE 1 RAMMES IKKE, og det er med vilje. Den bygger paa RetentionOutcomes og
# db_org_navne, som ingen af dem gaar gennem dette filter. Et lovet opkald er en
# forpligtelse uanset land. Se prioritering.py.
#
# HISTORIKKEN FILTRERES OGSAA (besluttet 2026-08-25). Overblikkets graf falder
# derfor ca. 14 % i hele sin laengde. Konsistens vejer tungere end den absolutte
# total: en graf paa 15.213 over en liste paa 13.040 faar nogen til at spoerge
# hver maaned, hvor de 2.173 blev af.
#
# Maalt 2026-08-25: 15.213 -> 13.040 abonnementer, 218,5 -> 189,5 mio. kr.
# watch_no 2.050, watch_se 119, watch_de 4. monitor og marketwire er DANSKE og
# staar bevidst ikke paa listen.
#
# FOELGEVIRKNING RETTET 2026-08-25: alle "Maalt <dato>"-tal i modulet der
# refererede en FOER-filter population (db_acv_beloeb_pr_site x2, TRE UDFALD-
# blokken i abonnementer_med_ejer, db_opsigelser, og risiko.py's opsigelses-
# maaling) er enten genmaalt paa dagens 13.040 danske abonnementer, eller
# maerket UFILTRERET med en advarsel om at de ikke maa sammenlignes med den
# danske total. De linjenumre en tidligere session listede her (130-140 osv.)
# havde allerede flyttet sig og pegede paa forkert indhold - soeg i stedet paa
# "Maalt 20" for at finde daterede tal, hvis en fremtidig aendring skal
# efterses igen.
UDENLANDSKE_ACCOUNTS = ("watch_no", "watch_se", "watch_de")

# Literaler i SQL'en og ikke %s, praecis som _KUN_B2B: fragmentet splejses ind
# med f-string i tre queries med positionelle params, og en parameter her skulle
# placeres det rigtige sted i hver af de tre tupler. Bygget af konstanten, saa de
# to ikke kan drive fra hinanden.
_KUN_DANSKE = (" AND r.account NOT IN ("
               + ", ".join(f"'{a}'" for a in UDENLANDSKE_ACCOUNTS) + ") ")

# Opsigelser bor i TRE pipelines. 'Opsigelser' er marketwires egen, og
# dbo.retention-viewet kender kun de to foerste, saa marketwires opsigelser har
# aldrig lukket et abonnement: 9 staar aktive med et ophoer op til tre aar
# tilbage, det aeldste fra 2023-03-04.
OPSIGELSE_PIPELINES = ("Cancellation", "Cancellations", "Opsigelser")

# Livstegn: en deal der viser at aftalen fortsaetter. Fornyelse og Upsale SKAL
# vaere med. Refinitiv Limited opsagde i februar 2023 og har fornyet tre gange
# siden, senest i denne maaned - uden de to pipelines ville de se opsagte ud og
# blive skrabet af listerne som en tabt kunde.
LIVSTEGN_PIPELINES = ("Customer", "Newbizz", "Company Trial",
                      "Fornyelse", "Upsale")


def _acv_owner_cte() -> tuple[str, tuple]:
    """CTE-kæde der giver ÉN ejer pr. (account, org_id) ud fra dbo.PipeDrive_ACV.

    Både trendlinjen og risikolisten bruger denne, så de ikke kan blive uenige om
    hvem der ejer en kunde. Returnerer (sql, params) hvor sql er de CTE'er der
    skal stå først i en WITH, og params skal ligge FØRST i kalderens params.

    **Hvorfor ACV og ikke dbo.retention_owner** (besluttet 2026-08-04): de to er
    uenige om 47% af rækkerne, fordi ACV's owner_name er org-ejeren — hvem der
    ejer kunderelationen NU — mens retention_owner er ejeren på hvert sites
    seneste start-deal. Det afgørende tal er dækning: ACV tilskriver 13.916 af
    15.123 rækker (92,0%) mod retention_owners 12.623 (83,5%). Oveni gør
    org-grainen at risikolisten kan summere ARR uden at tælle samme kunde i tre
    sælgeres lister (2.402 af 2.403 fler-site-kunder har præcis én ACV-ejer,
    mod 1.358 af 2.389 med 2+ retention_owner-ejere).

    ALLE TAL I DETTE AFSNIT er målt 2026-08-04, FOER den danske afgraensning
    25-08, og er ikke genmålt: sammenligningen kræver en kørsel af
    dbo.retention_owner. ACV-siden alene er genmålt 2026-08-26 til 12.178 af
    13.046 rækker (93,3%), se db_monthly_active_counts.

    Bemærk at ingen af kilderne er historisk korrekte: begge er nutids-snapshots,
    så en sælger ansat i 2024 får en trendlinje tilbage til 2016 med kunder han
    har arvet. Linjen er "min bogs historie", ikke "det jeg har solgt".

    Kendt konsekvens: watch_de og marketwire har ingen ACV-rækker, så deres
    kunder står uden ejer og tælles alene firmabredt. Det gør dbo.retention_owner
    ubrugt af appen — viewet bliver liggende i databasen, men dets Watch
    DE-team-mapping er dermed uden effekt.
    """
    brand_rows = ",".join(["(%s,%s)"] * len(ACV_BRAND_TO_ACCOUNT))
    params: tuple = ()
    for brand, account in ACV_BRAND_TO_ACCOUNT.items():
        params += (brand, account)

    sql = f"""brand_map AS (
                SELECT * FROM (VALUES {brand_rows}) AS b(acv_brand, account)
            ),
            acv_ranked AS (
                -- RANK, ikke ROW_NUMBER: PipeDrive_ACV har ingen unik kolonne til
                -- at bryde tie på updated_at, og 67 (org_id, site)-grupper HAR en
                -- tie. ROW_NUMBER dropper dem tavst — det er den kendte bug der
                -- taber 67 kunder fra sælger-porteføljerne.
                SELECT org_id, brand, site, owner_name, acv_value_dkk,
                       RANK() OVER (
                           PARTITION BY org_id, site ORDER BY updated_at DESC
                       ) AS rk
                FROM dbo.PipeDrive_ACV
            ),
            acv_site AS (
                -- MAX() afgør tie'en deterministisk i stedet for at tabe rækken.
                -- System Admin nulles FØR aggregeringen, samme regel som
                -- dbo.retention_owner: den er ikke en rigtig ejer, og fordi MAX()
                -- ignorerer NULL, kan en System Admin-række på ét site ikke
                -- skygge for en rigtig ejer på et andet. Har kunden KUN System
                -- Admin, ender den som uden ejer og tælles alene firmabredt.
                SELECT org_id, brand, site,
                       MAX(NULLIF(NULLIF(owner_name, 'System Admin'), '')) AS owner_name,
                       MAX(acv_value_dkk) AS acv_value_dkk
                FROM acv_ranked
                WHERE rk = 1
                GROUP BY org_id, brand, site
            ),
            acv_kunde AS (
                SELECT bm.account, a.org_id,
                       MAX(a.owner_name)    AS owner_name,
                       SUM(a.acv_value_dkk) AS arr_dkk,
                       COUNT(*)             AS acv_sites
                FROM acv_site a
                JOIN brand_map bm ON bm.acv_brand = a.brand
                GROUP BY bm.account, a.org_id
            )"""
    return sql, params


def _team_exists_clause(teams: list) -> tuple[str, tuple]:
    """EXISTS-betingelse for at kundens ejer sidder i et af de givne teams.

    EXISTS og ikke et join: 'Victoria Eikevold' har to aktive rækker i HubUsers
    (id 64 og 78), så et join på navn dublerer hendes kunder — hvilket ville
    tælle deres ARR to gange og gøre rækkeantallet forkert.

    Team udledes af HubUsers + TeamMemberships, ikke af PipedriveDeals.team: det
    er den kilde rolle-modellen og admin-UI'et vedligeholder. 99,1% af kronerne
    rammer en ejer der findes i HubUsers.
    """
    ph = ",".join(["%s"] * len(teams))
    sql = f"""
            AND EXISTS (
                SELECT 1
                FROM dbo.HubUsers u
                JOIN dbo.TeamMemberships tm ON tm.user_id = u.id
                JOIN dbo.Teams t ON t.id = tm.team_id
                WHERE u.name = k.owner_name
                  AND t.name IN ({ph})
                  AND {_AKTIVT_MEDLEMSKAB}
            )"""
    return sql, tuple(teams)


def db_monthly_active_counts(owner_name: str | None = None,
                             teams: list | None = None) -> list:
    """Antal aktive org+site-kombinationer pr. måned — retention-trendlinjen.

    Grainen er bevidst uændret: én række pr. (account, org_id, sites) pr. måned,
    så hele historikken er sammenlignelig med de tal der er valideret tidligere.
    `active_count` er 13.046 for indeværende måned, genmålt 2026-08-26. Var
    15.123 målt 2026-08-04, FOER den danske afgraensning 25-08.

    Ejeren kommer fra ACV (se _acv_owner_cte), ikke længere fra
    dbo.retention_owner. Det ændrer IKKE `active_count`, kun `attributed_count`.
    Genmålt 2026-08-26: 12.178 af 13.046 rækker har en ejer, altså 93,3%
    dækning, og gruppen '(Ikke tilskrevet)' i grafen er 868 abonnementer.
    Sammenligningen med dbo.retention_owner (12.623 til 13.916 af 15.123, 83,5%
    til 92,0%, uenighed om 47% af rækkerne) er målt 2026-08-04, FOER den danske
    afgraensning 25-08, og er IKKE genmålt: den gamle kilde skal køres igen for
    at give et tal på dagens grundlag. Skiftet blev valgt for at risikolisten og
    denne graf kan bruge samme ejer-definition.

    Ejer-filteret ligger i WHERE og ikke i ON: i ON ville rækker der tilhører
    ANDRE sælgere blive bevaret med NULL (LEFT JOIN-semantik), så en sælger ville
    se firmaets samlede tal og tro det var hans egen bog. I WHERE bliver joinet
    reelt et inner join, hvilket er det ønskede for sælger/team-visninger, mens
    den ufiltrerede CEO-visning beholder LEFT-adfærden.

    Returnerer pr. måned: `active_count` (abonnementer), `attributed_count`
    (abonnementer med en ACV-ejer), `customer_count` (distinkte (account,
    org_id)), `churned_count` og `churn_pct`. Porteføljen kræver de to sidste
    kolonner, og kunde-linjen findes for at gøre forskellen på 13.046 og 9.784
    synlig i stedet for forvirrende (genmålt 2026-08-26).

    CHURN-DEFINITIONEN er GAP-baseret, ikke "sidste måned abonnementet fandtes".
    Et abonnement kan forsvinde og komme tilbage: maj 2026 havde 1.776
    genstartede mod 7-44 i en normal måned (genmålt 2026-08-26). En MAX-baseret definition ville kun
    se det sidste farvel og undertælle al historisk churn, så `LEAD` finder i
    stedet HVER gang der opstår et hul. Den sidste måned i dataen udelades —
    ellers ville alle nulevende abonnementer se ud som om de churnede.

    TÆRSKLEN ER TO MÅNEDER, jf. Definitioner, besluttet 2026-08-17. Et hul
    på PRÆCIS én
    måned er en pause og tælles ikke. Genmålt 2026-08-26: 154 af 8.016 hændelser
    var én-måneds huller, altså 1,9%, og de klumpede i april 2026 (30) frem for
    at ligge jævnt. Det er synkronisering og fakturering, ikke kundeadfærd.
    Totalen er 7.862 efter ændringen, og kun april 2026 flyttede sig synligt,
    fra 1,91% til 1,61%.

    DE TO DATEADD-TAL I `churn`-CTE'EN ER FORSKELLIGE MED VILJE. Det er den eneste
    fælde i funktionen, og den ligner en tastefejl. `month, 2` i WHERE er
    TÆRSKLEN, altså hvor stort hullet skal være. `month, 1` i GROUP BY er
    REGISTRERINGSMÅNEDEN, altså at churn tilskrives den måned kunden forsvandt og
    ikke den måned vi kan bekræfte det. Gøres de to ens, forsvinder enten
    pause-reglen eller registreringsmåneden, og INGEN TEST FANGER DET:
    churn-tallet indgår hverken i roegtest_outcomes eller roegtest_prioritering,
    og læses kun af retention_overview.html.

    FØLGE AF TÆRSKLEN: `h.naeste IS NULL` rammer også rækker i måneden før den
    nyeste, og de kan endnu ikke skelnes fra pauser. Nyeste måneds churn er derfor
    et MAKSIMUM, der revideres NEDAD ved næste eksport. Porteføljen kræver at søjlen
    markeres foreløbig, ellers læses artefaktet som en churn-stigning.

    Genmålt 2026-08-26: churn ligger på 0,39-2,37% gennem hele historikken, vist
    for 89 måneder fra 2019-04 og frem. Juni 2026 er undtagelsen med 2,37%,
    måneden efter at porteføljen sprang fra 9.932 til 13.358 abonnementer. Grafen får derfor en
    lodret klippe i maj 2026, som SKAL forklares på siden.
    """
    cte, params = _acv_owner_cte()

    clause = ''
    if owner_name:
        clause += ' AND k.owner_name = %s'
        params += (owner_name,)
    if teams:
        # Et TOMT team-filter springes over og ville give firmatotalen — derfor
        # ligger tomhedstjekket i routerens _resolve_filters, som er det eneste
        # sted der ved HVORFOR listen er tom.
        team_sql, team_params = _team_exists_clause(teams)
        clause += team_sql
        params += team_params

    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute(
                       f"""WITH {cte},
            base AS (
                SELECT r.FirstDayOfMonth,
                       r.account,
                       r.org_id,
                       -- marketwire har sites = NULL. En fast streng holder de 35
                       -- rækker samlet i PARTITION BY nedenfor, i stedet for at
                       -- lade NULL-semantik afgøre om de hører sammen.
                       ISNULL(r.sites, '(intet site)') AS sites,
                       k.owner_name
                FROM dbo.retention r
                LEFT JOIN acv_kunde k
                       ON k.account = r.account AND k.org_id = r.org_id
                WHERE r.FirstDayOfMonth <= EOMONTH(GETDATE())
                    {_KUN_B2B}
                    {_KUN_DANSKE}
                    {clause}
            ),
            sidste AS (
                SELECT MAX(FirstDayOfMonth) AS max_maaned FROM base
            ),
            huller AS (
                SELECT FirstDayOfMonth,
                       LEAD(FirstDayOfMonth) OVER (
                           PARTITION BY account, org_id, sites
                           ORDER BY FirstDayOfMonth) AS naeste
                FROM base
            ),
            churn AS (
                SELECT DATEADD(month, 1, h.FirstDayOfMonth) AS maaned,
                       COUNT(*) AS churned_count
                FROM huller h
                CROSS JOIN sidste s
                WHERE h.FirstDayOfMonth < s.max_maaned
                  AND (h.naeste IS NULL
                       OR h.naeste > DATEADD(month, 2, h.FirstDayOfMonth))
                GROUP BY DATEADD(month, 1, h.FirstDayOfMonth)
            ),
            aktive AS (
                SELECT FirstDayOfMonth,
                       COUNT(*)                                     AS active_count,
                       COUNT(owner_name)                            AS attributed_count,
                       COUNT(DISTINCT CONCAT(account, '|', org_id)) AS customer_count
                FROM base
                GROUP BY FirstDayOfMonth
            )
            SELECT a.FirstDayOfMonth,
                   a.active_count,
                   a.attributed_count,
                   a.customer_count,
                   ISNULL(c.churned_count, 0) AS churned_count,
                   -- Under 1.000 aktive abonnementer i M-1 er raten støj og
                   -- ikke måling: i 2016-2018 er der 1-5 churn om måneden på et
                   -- grundlag under 600, så linjen hopper mellem 0 og 7% uden at
                   -- afspejle andet end afrunding. Grænsen rammer først april
                   -- 2019 (1.161 aktive i marts). NULL giver et hul i grafen,
                   -- hvilket er et ærligt "det kan ikke måles endnu" frem for en
                   -- takket kurve der ligner information.
                   CAST(CASE
                          WHEN LAG(a.active_count)
                               OVER (ORDER BY a.FirstDayOfMonth) >= 1000
                          THEN 100.0 * ISNULL(c.churned_count, 0)
                               / LAG(a.active_count)
                                 OVER (ORDER BY a.FirstDayOfMonth)
                        END AS decimal(5,2)) AS churn_pct
            FROM aktive a
            LEFT JOIN churn c ON c.maaned = a.FirstDayOfMonth
            ORDER BY a.FirstDayOfMonth;""",
            params,
        )
        result = cur.fetchall()
        conn.close()
        return result
    except Exception:
        logger.exception("db_monthly_active_counts fejlede")
        return []


def _maaned_foer(iso_dato: str) -> str:
    """Måneden FØR `iso_dato` ('YYYY-MM-DD', altid en FirstDayOfMonth-dato).

    Samme heltalsregning som zones.forskyd_maaned, bare på en fuld ISO-dato i
    stedet for 'YYYY-MM' — de to konventioner lever side om side i modulet, og
    denne funktion hører til her, hvor kalderen allerede har fulde datoer fra
    db_monthly_active_counts.
    """
    aar, md = int(iso_dato[:4]), int(iso_dato[5:7])
    i = aar * 12 + (md - 1) - 1
    return f"{i // 12:04d}-{i % 12 + 1:02d}-01"


def db_monthly_churn_pr_site(maaneder: list[str], owner_name: str | None = None,
                             teams: list | None = None) -> list:
    """Aktive og churnede abonnementer pr. (måned, account, site) — Porteføljens
    "Måned mod måned pr. site"-panel.

    Samme gap-baserede churn-definition som db_monthly_active_counts, én
    dimension mere. Se den funktions docstring for selve reglerne (to
    måneders tærskel, DE TO FORSKELLIGE DATEADD-TAL, sidste måned som
    maksimum) — de er IKKE gentaget her, og de er IKKE ændret.

    `maaneder` er de(n) måned(er) brugeren har valgt i UI'et ('YYYY-MM-01').
    Funktionen henter selv hver måneds FORGÆNGER (_maaned_foer), fordi
    account-raten (regnet i Python, se queries-kaldestedet i router.py) skal
    bruge aktive i M-1 som nævner — churn-raten pr. site vises bevidst IKKE,
    se churn-rate-kan-ikke-maales-pr-site: kun 2 af 35 danske sites har
    grundlag over 1.000 aktive.

    KRITISK: `base`-CTE'en er IKKE afgrænset til `maaneder`. Skæres serien af
    før huller-CTE'en kører sin LEAD, ser hvert abonmenent, der er aktivt i
    den tidligste valgte måned, ud som om det lige er startet der — og et
    abonnement der churner LIGE UDEN FOR det valgte vindue ville se ud som om
    det stadig var aktivt. Afgrænsningen til `maaneder` sker udelukkende i
    den afsluttende SELECT.

    Returnerer én række pr. (FirstDayOfMonth, account, sites):
        FirstDayOfMonth, account, sites, active_count, churned_count
    Ingen customer_count (en kunde med syv sites ville tælles syv gange) og
    ingen churn_pct (se ovenfor).
    """
    if not maaneder:
        return []

    cte, params = _acv_owner_cte()

    clause = ''
    if owner_name:
        clause += ' AND k.owner_name = %s'
        params += (owner_name,)
    if teams:
        team_sql, team_params = _team_exists_clause(teams)
        clause += team_sql
        params += team_params

    # Både de valgte måneder og deres forgængere skal med i resultatet — se
    # docstringen. sorted(set(...)) fjerner dubletter, når to valgte måneder
    # ligger lige efter hinanden (forgængeren for den ene ER den anden).
    alle_maaneder = sorted(set(maaneder) | {_maaned_foer(m) for m in maaneder})
    maaned_ph = ",".join(["%s"] * len(alle_maaneder))

    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute(
                       f"""WITH {cte},
            base AS (
                SELECT r.FirstDayOfMonth,
                       r.account,
                       r.org_id,
                       -- Samme sentinel som db_monthly_active_counts, af
                       -- samme grund: marketwires 35 rækker skal holdes
                       -- samlet i PARTITION BY, ikke spredes af NULL-semantik.
                       ISNULL(r.sites, '(intet site)') AS sites,
                       k.owner_name
                FROM dbo.retention r
                LEFT JOIN acv_kunde k
                       ON k.account = r.account AND k.org_id = r.org_id
                WHERE r.FirstDayOfMonth <= EOMONTH(GETDATE())
                    {_KUN_B2B}
                    {_KUN_DANSKE}
                    {clause}
            ),
            sidste AS (
                SELECT MAX(FirstDayOfMonth) AS max_maaned FROM base
            ),
            huller AS (
                SELECT FirstDayOfMonth,
                       account,
                       sites,
                       LEAD(FirstDayOfMonth) OVER (
                           PARTITION BY account, org_id, sites
                           ORDER BY FirstDayOfMonth) AS naeste
                FROM base
            ),
            churn AS (
                -- SAMME TO DATEADD-TAL, forskellige med vilje: se
                -- db_monthly_active_counts. `month, 2` er tærsklen (hvor
                -- stort hullet skal være), `month, 1` er
                -- registreringsmåneden.
                SELECT DATEADD(month, 1, h.FirstDayOfMonth) AS maaned,
                       h.account,
                       h.sites,
                       COUNT(*) AS churned_count
                FROM huller h
                CROSS JOIN sidste s
                WHERE h.FirstDayOfMonth < s.max_maaned
                  AND (h.naeste IS NULL
                       OR h.naeste > DATEADD(month, 2, h.FirstDayOfMonth))
                GROUP BY DATEADD(month, 1, h.FirstDayOfMonth), h.account, h.sites
            ),
            aktive AS (
                SELECT FirstDayOfMonth,
                       account,
                       sites,
                       COUNT(*) AS active_count
                FROM base
                GROUP BY FirstDayOfMonth, account, sites
            )
            SELECT a.FirstDayOfMonth,
                   a.account,
                   a.sites,
                   a.active_count,
                   ISNULL(c.churned_count, 0) AS churned_count
            FROM aktive a
            LEFT JOIN churn c ON c.maaned = a.FirstDayOfMonth
                             AND c.account = a.account
                             AND c.sites = a.sites
            WHERE a.FirstDayOfMonth IN ({maaned_ph})
            ORDER BY a.FirstDayOfMonth, a.account, a.sites;""",
            params + tuple(alle_maaneder),
        )
        result = cur.fetchall()
        conn.close()
        return result
    except Exception:
        logger.exception("db_monthly_churn_pr_site fejlede")
        return []


# Samme grænse som db_monthly_active_counts' churn_pct: under 1.000 aktive i
# M-1 er raten støj, ikke måling. Genbruges her og IKKE i SQL'en, se
# account_churn_rate's docstring for hvorfor.
MIN_AKTIVE_FOR_RATE = 1000


def account_churn_rate(rows: list, maaneder: list[str]) -> list:
    """Ruller db_monthly_churn_pr_site's site-rækker op til en rate pr.
    account — churn-rate-kan-ikke-maales-pr-site (målt 2026-08-27): kun 2 af
    35 danske sites har grundlag over MIN_AKTIVE_FOR_RATE, mens account-
    niveauet holder (fx AdvokatWatch DK svinger med faktor 7,5 på syv
    måneder, hele porteføljen kun 0,39-2,37% over 126 måneder).

    Ren funktion over rækkerne, IKKE en ny forespørgsel: grænsen skal stå ét
    sted, ikke gentages i endnu en SQL-aggregering, hvor den kunne komme til
    at afvige fra db_monthly_active_counts' egen.

    For hver valgt måned M: nævneren er summen af active_count over ALLE
    sites for kontoen i M-1 (måneden før), tælleren er summen af
    churned_count over alle sites for kontoen i M. Rækkerne for M-1 findes i
    `rows`, fordi db_monthly_churn_pr_site selv henter hver måneds forgænger
    — se den funktions docstring.

    Returnerer én række pr. (account, maaned):
        account, maaned, active_foer, churned, churn_pct (None under grænsen)
    """
    aktive: dict[tuple, int] = {}
    churnet: dict[tuple, int] = {}
    for r in rows:
        noegle = (r["account"], r["FirstDayOfMonth"])
        aktive[noegle] = aktive.get(noegle, 0) + r["active_count"]
        churnet[noegle] = churnet.get(noegle, 0) + r["churned_count"]

    resultat = []
    for maaned in maaneder:
        forrige = _maaned_foer(maaned)
        accounts = sorted({acc for (acc, m) in aktive if m in (maaned, forrige)})
        for account in accounts:
            active_foer = aktive.get((account, forrige), 0)
            churned_nu = churnet.get((account, maaned), 0)
            churn_pct = (round(100.0 * churned_nu / active_foer, 2)
                         if active_foer >= MIN_AKTIVE_FOR_RATE else None)
            resultat.append({
                "account": account,
                "maaned": maaned,
                "active_foer": active_foer,
                "churned": churned_nu,
                "churn_pct": churn_pct,
            })
    return resultat


def db_customers_at_risk_base(owner_name: str | None = None,
                              teams: list | None = None) -> list:
    """Aktive kunder i indeværende måned med ARR og ejer — grundlaget for
    risikolisten. Usage-signalet lægges på i Python (se usage.py).

    Grain er `(account, org_id)` = én kunde inden for én forretningsenhed. Ikke
    org_id alene (samme org_id er to fremmede firmaer i to accounts), og ikke
    per site: ACV's site-vokabular kan ikke brolægges til retentions. Verificeret
    med `normalize_site` fra alignment-modulet — kun 20 fælles kanoniske nøgler af
    45 i retention og 37 i ACV, fordi alle Monitor-sites ('Byrum' mod
    'Byrummonitor') og alle NO-sites ('EiendomsWatch' mod 'EiendomsWatch NO')
    divergerer, og ACV's bare navne er tvetydige ('techwatch.dk' i ACV mod
    'techwatch.no' i retention). Nøglen ville kun kunne udledes af `brand`
    SAMMEN med `site`.

    Ejeren kommer fra ACV, ikke fra `dbo.retention_owner` — se `_acv_owner_cte`
    for begrundelsen. Trendlinjen bruger nu samme kilde, så de to sider ikke
    længere kan være uenige om hvem der ejer en kunde.

    Kunder uden ACV-række får `arr_dkk = NULL` og ingen ejer — de forsvinder
    derfor ud af enhver sælger-filtreret visning og tælles kun firmabredt, præcis
    som `(Ikke tilskrevet)` i overview-grafen. Det gælder alle watch_de- og
    marketwire-kunder, fordi ACV ikke har de brands.
    """
    if not ACV_BRAND_TO_ACCOUNT:
        logger.error("ACV_BRAND_TO_ACCOUNT er tom — ingen ARR kan kobles")
        return []

    cte, params = _acv_owner_cte()

    clause = ''
    if owner_name:
        clause += ' AND k.owner_name = %s'
        params += (owner_name,)
    if teams:
        team_sql, team_params = _team_exists_clause(teams)
        clause += team_sql
        params += team_params

    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute(
            f"""WITH {cte},
            aktive AS (
                -- Kun indeværende måned. dbo.retention projicerer frem til
                -- 2030-12, så uden dette filter ville alle fremtidige måneder med.
                SELECT r.account, r.org_id,
                       MAX(r.org_name) AS org_name,
                       COUNT(*)        AS sites,
                       -- Site-navnene skal med, så UI'et kan skelne en tavs kunde
                       -- fra en kunde der ikke KAN trackes: FINANS DK sætter
                       -- aldrig access_account_number i Snowplow.
                       STRING_AGG(ISNULL(r.sites, ''), ', ')
                           WITHIN GROUP (ORDER BY r.sites) AS sites_list
                FROM dbo.retention r
                WHERE r.FirstDayOfMonth = DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1)
                  {_KUN_B2B}
                  {_KUN_DANSKE}
                GROUP BY r.account, r.org_id
            )
            SELECT a.account,
                   a.org_id,
                   a.org_name,
                   a.sites,
                   a.sites_list,
                   k.owner_name,
                   k.arr_dkk,
                   k.acv_sites,
                   -- Korreleret subquery, IKKE et join mod HubUsers: 'Victoria
                   -- Eikevold' har to aktive brugerrækker (id 64 og 78), og et
                   -- join på navn dublerede derfor hendes 14 kunder — hvilket
                   -- ville have talt deres ARR med to gange. DISTINCT inde i
                   -- subqueryen, fordi STRING_AGG ikke selv kan tage DISTINCT.
                   (SELECT STRING_AGG(x.name, ', ') WITHIN GROUP (ORDER BY x.name)
                      FROM (
                        SELECT DISTINCT t.name
                        FROM dbo.HubUsers u
                        JOIN dbo.TeamMemberships tm ON tm.user_id = u.id
                        JOIN dbo.Teams t ON t.id = tm.team_id
                        WHERE u.name = k.owner_name
                          AND {_AKTIVT_MEDLEMSKAB}
                      ) x) AS teams
            FROM aktive a
            LEFT JOIN acv_kunde k
                   ON k.account = a.account AND k.org_id = a.org_id
            WHERE 1 = 1
                {clause}
            ORDER BY k.arr_dkk DESC;""",
            params,
        )
        result = cur.fetchall()
        conn.close()
        return result
    except Exception:
        logger.exception("db_customers_at_risk_base fejlede")
        return []


def db_abonnementer(maaned: str) -> list:
    """Abonnementerne i én måned, med abonnementets første måned som alder.

    Grain er `(account, org_id, sites)` = Zonemodellens måleenhed. `maaned` er
    'YYYY-MM' og skal være den sidste HELE måned — samme reference som
    zones.bestem_zone regner i.

    `foerste_maaned` er MIN over HELE historikken (viewet går tilbage til
    2016-03), ikke over usage-eksportens 13 måneder. Målt 2026-08-07: med
    13-måneders-vinduet bliver 436 abonnementer fejlagtigt "ny", fordi de har
    et hul i historikken — 147 af dem har aldrig læst noget. Vinduet ville
    altså give dem vægt 0,00 og fjerne dem fra specialistens liste.

    Alternativet var start på seneste SAMMENHÆNGENDE kæde. Det er forkastet:
    mellem april og maj 2026 blev 1.776 eksisterende abonnementer genskabt
    (mod 7-44 i en normal måned, genmålt 2026-08-26), og kædestart ville give
    hele den bunke vægt 0,00 på grundlag af en dataartefakt.

    Rækkeantallet ligger under viewets, fordi 2026-07 har to ægte dubletrækker
    som GROUP BY her folder sammen (15.203 mod 15.205, målt 2026-08-07, FOER
    den danske afgraensning 25-08; mekanismen er upåvirket). Tallet
    er ikke fast: viewet er live, og et redigeret Pipedrive-deal kan tilføje en
    række i en måned der ellers er afsluttet — 15.204 samme dag kl. 11.
    """
    # Én reference til dbo.retention, ikke to. Viewet har 1.484.578 rækker, og
    # queryen MÅ IKKE joines mod _acv_owner_cte i samme statement: den variant
    # timede ud efter 300 s, mens hver del alene tager under ét sekund. Ejeren
    # hentes derfor separat i db_acv_ejere og joines i Python (2,1 s + 0,3 s).
    sql = f"""
        WITH historik AS (
            SELECT r.account, r.org_id, r.sites, r.org_name, r.FirstDayOfMonth,
                   -- PARTITION BY uden ISNULL: window-partitionering samler
                   -- NULL i én gruppe, og marketwires 35 rækker har sites NULL.
                   MIN(r.FirstDayOfMonth) OVER (
                       PARTITION BY r.account, r.org_id, r.sites
                   ) AS foerste_maaned
            FROM dbo.retention r
            WHERE r.FirstDayOfMonth <= %s
              {_KUN_B2B}
              {_KUN_DANSKE}
        )
        SELECT account, org_id, sites,
               MAX(org_name) AS org_name,
               -- varchar(7) og ikke en dato: hele modulet sammenligner måneder
               -- som tekst ('YYYY-MM'), så en date her ville kræve konvertering
               -- i hvert kaldested — og zones.py regner udelukkende på strenge.
               CONVERT(varchar(7), MIN(foerste_maaned), 23) AS foerste_maaned
        FROM historik
        WHERE FirstDayOfMonth = %s
        GROUP BY account, org_id, sites
    """

    foerste_dag = f"{maaned}-01"
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute(sql, (foerste_dag, foerste_dag))
        result = cur.fetchall()
        conn.close()
        return result
    except Exception:
        logger.exception("db_abonnementer(%s) fejlede", maaned)
        return []


def db_acv_ejere(owner_name: str | None = None,
                 teams: list | None = None) -> dict:
    """{(account, org_id): {owner_name, arr_dkk, acv_sites}} — ejer og ARR.

    Skilt ud som sin egen query frem for et join i db_abonnementer: se
    performance-noten dér. Resultatet er 15.174 kunder på 0,3 s, og 99,2% af
    juli-abonnementerne finder et match.

    Grainen er KUNDEN `(account, org_id)`, ikke abonnementet — ACV's
    site-vokabular kan ikke brolægges til retentions (kun 20 af 45 kanoniske
    nøgler matcher, se db_customers_at_risk_base). `arr_dkk` er derfor kundens
    samlede ARR, og den må IKKE summeres pr. abonnement uden at deles først.
    """
    # Lokal import: usage.py trækker pandas ind ved import, og DB-laget skal
    # kunne importeres uden. customer_key str()'er org_id, som kommer tilbage
    # som int både herfra og fra db_abonnementer — nøglerne SKAL gå gennem
    # samme funktion, ellers matcher 1 aldrig '1'.
    from .usage import customer_key

    cte, params = _acv_owner_cte()

    clause = ''
    if owner_name:
        clause += ' AND k.owner_name = %s'
        params += (owner_name,)
    if teams:
        # Tomt team-filter springes over her og fanges i routerens
        # _resolve_filters — samme arbejdsdeling som modulets to andre queries.
        team_sql, team_params = _team_exists_clause(teams)
        clause += team_sql
        params += team_params

    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute(
            f"""WITH {cte}
            SELECT k.account, k.org_id, k.owner_name, k.arr_dkk, k.acv_sites,
                   -- Korreleret subquery, IKKE et join mod HubUsers: 'Victoria
                   -- Eikevold' har to aktive brugerrækker (id 64 og 78), og et
                   -- join på navn ville dublere hendes kunder og dermed tælle
                   -- deres ARR to gange. DISTINCT inde i subqueryen, fordi
                   -- STRING_AGG ikke selv kan tage DISTINCT. Samme mønster som
                   -- db_customers_at_risk_base.
                   (SELECT STRING_AGG(x.name, ', ') WITHIN GROUP (ORDER BY x.name)
                      FROM (
                        SELECT DISTINCT t.name
                        FROM dbo.HubUsers u
                        JOIN dbo.TeamMemberships tm ON tm.user_id = u.id
                        JOIN dbo.Teams t ON t.id = tm.team_id
                        WHERE u.name = k.owner_name
                          AND {_AKTIVT_MEDLEMSKAB}
                      ) x) AS teams
            FROM acv_kunde k
            WHERE 1 = 1
                {clause};""",
            params,
        )
        rows = cur.fetchall()
        conn.close()
        return {customer_key(r["account"], r["org_id"]): r for r in rows}
    except Exception:
        logger.exception("db_acv_ejere fejlede")
        return {}


def db_org_navne() -> dict:
    """{(account, org_id): org_name} — kundenavn uden hensyn til aktivitet.

    Findes fordi dbo.RetentionOutcomes ikke har en org_name-kolonne, og fordi
    navnet ikke kan tages fra risikolaget: det viser kun abonnementer der er
    AKTIVE i måneden, mens en kunde man har lovet at ringe tilbage til ofte er
    en, hvis abonnement er ophørt. Målt 2026-08-11, FOER den danske
    afgraensning 25-08: viewet har 15.269 kunder, risikolaget 11.621 for juli,
    altså 3.648 kunder uden navn. Størrelsesordenen bærer pointen, og tallene
    er ikke genmålt.

    MAX(org_name) og ikke ROW_NUMBER på måneden: målt 2026-08-11 har 0 af
    15.269 kunder mere end ét org_name, så navnet er funktionelt afhængigt af
    (account, org_id). Holder den antagelse op, er det HER det skal rettes — en
    aggregering skjuler et navneskift i stedet for at vælge imellem.

    Regler og Guardrails, regel 6: FirstDayOfMonth-filteret er påkrævet, viewet
    projicerer til 2030-12. Uden det læses fremtidige rækker.

    ÉN pass over viewet. En CTE-kæde der scannede dbo.retention tre gange
    timede forbindelsen ud — aggreger i SQL, sammenlign i Python."""

    # Lokal import, samme grund som i db_acv_ejere: usage.py trækker pandas ind
    # ved import, og DB-laget skal kunne importeres uden. Nøglerne SKAL gå
    # gennem samme funktion som resten af pakken, ellers rammer '1' aldrig 1.
    from .usage import customer_key

    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute(
            """SELECT account, org_id, MAX(org_name) AS org_name
               FROM dbo.retention
               WHERE FirstDayOfMonth <= EOMONTH(GETDATE())
               GROUP BY account, org_id;"""
        )
        rows = cur.fetchall()
        conn.close()
        # Tomme navne filtreres IKKE væk, de tælles. Kalderen skal kunne skelne
        # "kunden findes ikke" fra "kunden har intet navn", og logningen er den
        # eneste måde at opdage, hvis hullet vokser.
        uden_navn = sum(1 for r in rows if not r["org_name"])
        if uden_navn:
            logger.warning("db_org_navne: %s af %s kunder uden org_name",
                           uden_navn, len(rows))
        return {customer_key(r["account"], r["org_id"]): r["org_name"]
                for r in rows}
    except Exception:
        logger.exception("db_org_navne fejlede")
        return {}


def db_acv_beloeb_pr_site() -> dict:
    """{(account, org_id, sites): beloeb} — ACV's kroner pr. ABONNEMENT.

    Modstykket til db_acv_ejere, som er paa KUNDE-niveau. Denne har grainen
    (account, org_id, site) og gjorde det muligt at holde op med at dele kundens
    ARR ligeligt ud paa dens sites.

    UFILTRERET MED VILJE: beloebet pr. site afhaenger ikke af hvem der ser paa
    det, og afgraensningen sker i forvejen paa kundeniveau i
    abonnementer_med_ejer. En WHERE her ville kunne komme i utakt med den.

    Noeglen gaar gennem customer_key, praecis som db_acv_ejere og
    db_abonnementer. org_id kommer tilbage som int, og de tre opslag SKAL bruge
    samme funktion, ellers matcher 1 aldrig '1'.

    Maalt 2026-08-18, FOER den danske afgraensning 25-08: 15.039 af 15.203
    juli-abonnementer (UFILTRERET, alle lande med) finder et beloeb her,
    altsaa 98,9%, og de daekker alle 218.238.867 ACV-kroner. De 164 der ikke
    goer, er Kom24 NO, Medier24 NO, marketwire og FinanzBusiness, hvis brands
    ACV slet ikke har, plus fem kunder der mangler en raekke paa et site de har.
    Funktionen selv er stadig ufiltreret (se ovenfor), saa daekningsprocenten
    staar til troende, men 15.203 er IKKE det samme tal som db_abonnementer's
    danske 13.044 (maalt 2026-08-25).
    """
    from .usage import customer_key

    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute("""
            WITH acv_ranked AS (
                -- RANK og ikke ROW_NUMBER, samme grund som i _acv_owner_cte: 67
                -- (org_id, site)-grupper har en tie paa updated_at, og
                -- ROW_NUMBER dropper dem tavst.
                SELECT org_id, brand, site, acv_value_dkk,
                       RANK() OVER (
                           PARTITION BY org_id, site ORDER BY updated_at DESC
                       ) AS rk
                FROM dbo.PipeDrive_ACV
            )
            SELECT org_id, brand, site, MAX(acv_value_dkk) AS arr
            FROM acv_ranked
            WHERE rk = 1
            GROUP BY org_id, brand, site;
        """)
        raekker = cur.fetchall()
        conn.close()
    except Exception:
        logger.exception("db_acv_beloeb_pr_site fejlede")
        return {}

    ud: dict = {}
    for r in raekker:
        account = ACV_BRAND_TO_ACCOUNT.get(r["brand"])
        site = acv_site_til_retention(r["brand"], r["site"])
        # NUL ER UKENDT, IKKE NUL. Verificeret 2026-08-18: 7.766 af 20.642
        # ACV-raekker har vaerdien 0, og de har ALLE mindst én deal bag sig (DNB
        # Bank har 79 paa FinansWatch). Vaerdien er ogsaa 0 i lokal valuta, saa
        # det er ikke en kursfejl: beloebet MANGLER.
        #
        # Springes de ikke over, faar 568 abonnementer score 0 i stedet for at
        # staa som uopgjorte, og de forsvinder tavst fra prioriteringslisten,
        # fordi score = ARR x vaegt. Samme regel som risiko.py's kommentar om
        # score: 0 betyder "ingen risiko", None betyder "vi ved det ikke".
        if account is None or site is None or not r["arr"]:
            continue
        kunde = customer_key(account, r["org_id"])
        ud[(kunde[0], kunde[1], site)] = float(r["arr"])
    return ud


def db_opsigelser() -> dict:
    """{(account, org_id, sites): 'YYYY-MM-DD'} - datoen for en GAELDENDE opsigelse.

    Et abonnement er opsagt, naar der findes en vundet opsigelse dateret EFTER
    det seneste livstegn paa aftalen. Retningen ER reglen: ligger opsigelsen
    FOER livstegnet, er kunden kommet tilbage efter at have sagt op.

    HVORFOR IKKE BARE "har en opsigelse": maalt 2026-08-19, FOER den danske
    afgraensning 25-08 (tallet skal genmaales paa dagens 13.044 abonnementer,
    men princippet er upaavirket): 4.430 af dengang 15.189 aktive abonnementer
    (39 mio. kr.) havde en opsigelse et sted i historikken. 107 af dem havde
    opsigelse og ny aftale paa SAMME dato (genmaalt 2026-08-25: 83 par i dag).
    Det er genforhandlinger: abonnementet loeber uaendret videre, men
    opsigelsen bliver staaende i historikken. 3.858 var kunder der var holdt
    op og kommet tilbage aar senere. Uden datosammenligningen ville en
    fjerdedel af portefoeljen forsvinde tavst.

    DATOEN ER service_activation_date, ikke won_time. won_time er hvornaar
    opsigelsen blev REGISTRERET, sad er hvornaar abonnementet OPHOERER. Maalt
    paa 12.755 vundne opsigelser ligger sad EFTER won_time i 69% af dem, typisk
    32 til 200 dage, altsaa varslet: Oersteds Klimamonitor blev opsagt
    2026-08-10 med ophoer 2027-08-20. Reserven COALESCE(..., won_time,
    add_time) daekker 262 raekker med standardvaerdien 2019-01-01 plus 5 helt
    uden dato. Det er samme kolonne og samme reserveregel som
    dbo.retention-viewet selv bruger, saa modulet og portefoeljetallet ikke kan
    blive uenige om hvornaar et abonnement holdt op.

    AABNE DEALS TAELLER SOM LIVSTEGN, men kun vundne som opsigelse. En aaben
    fornyelse siger at aftalen loeber videre; en aaben opsigelse er ikke en
    opsigelse endnu.

    UFILTRERET MED VILJE, praecis som db_acv_beloeb_pr_site: opslaget kender
    ingen maaned og indeholder derfor ogsaa abonnementer der ophoerte for aar
    siden og laenge er ude af dbo.retention, samt udenlandske accounts, som
    IKKE er filtreret fra her (filteret ligger paa db_abonnementer, ikke her).
    Maalt 2026-08-25 PAA DET DANSKE GRUNDLAG (13.044 abonnementer, efter
    afgraensningen samme dag): 5.801 opslag i alt, og 277 af dem rammer et af
    maanedens abonnementer, fordelt paa 9 forfaldne (alle marketwire), 72
    ophoert og 205 i opsigelse.

    Noeglen gaar gennem customer_key som de tre andre opslag, og `sites`
    beholdes RAA: marketwire har NULL, som bliver None i Python og matcher
    db_abonnementer's egen noegle.
    """
    from .usage import customer_key

    ph_liv = ",".join(["%s"] * len(LIVSTEGN_PIPELINES))
    ph_ops = ",".join(["%s"] * len(OPSIGELSE_PIPELINES))

    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute(f"""
            WITH deals AS (
                SELECT account, org_id, sites, pipeline_name, status,
                       COALESCE(
                           CASE WHEN service_activation_date = '2019-01-01'
                                THEN NULL
                                ELSE CAST(service_activation_date AS datetime)
                           END,
                           won_time, add_time) AS dato
                FROM dbo.PipedriveDeals
                WHERE status IN ('won', 'open')
                  AND org_id IS NOT NULL
            ),
            livstegn AS (
                SELECT account, org_id, sites, MAX(dato) AS livsdato
                FROM deals
                WHERE pipeline_name IN ({ph_liv})
                GROUP BY account, org_id, sites
            ),
            opsigelse AS (
                SELECT account, org_id, sites, MAX(dato) AS opsigelsesdato
                FROM deals
                WHERE status = 'won'
                  AND pipeline_name IN ({ph_ops})
                GROUP BY account, org_id, sites
            )
            SELECT o.account, o.org_id, o.sites,
                   CONVERT(char(10), o.opsigelsesdato, 23) AS opsigelsesdato
            FROM opsigelse o
            JOIN livstegn l
              ON l.account = o.account AND l.org_id = o.org_id
             -- ISNULL paa BEGGE sider: marketwires sites er NULL, og NULL = NULL
             -- er falsk i SQL. Uden den ville de 9 forfaldne aldrig kobles.
             AND ISNULL(l.sites, '') = ISNULL(o.sites, '')
            WHERE o.opsigelsesdato > l.livsdato;
        """, LIVSTEGN_PIPELINES + OPSIGELSE_PIPELINES)
        raekker = cur.fetchall()
        conn.close()
    except Exception:
        logger.exception("db_opsigelser fejlede")
        return {}

    ud: dict = {}
    for r in raekker:
        kunde = customer_key(r["account"], r["org_id"])
        # CONVERT i SQL'en gav en streng med vilje. TDS 7.0 leverer date og
        # datetime2 som str alligevel, og resten af modulet sammenligner datoer
        # som tekst, saa 'YYYY-MM-DD' kan bruges direkte.
        ud[(kunde[0], kunde[1], r["sites"])] = r["opsigelsesdato"]
    return ud


def abonnementer_med_ejer(maaned: str,
                          owner_name: str | None = None,
                          teams: list | None = None) -> list:
    """Månedens abonnementer med ejer, ARR og alder — input til zonerne.

    Joinet ligger i Python og ikke i SQL af rene performance-grunde; se noten i
    db_abonnementer.

    Filter-semantikken efterligner bevidst SQL-versionens: UDEN ejer/team-filter
    beholdes abonnementer uden ACV-række (LEFT JOIN), så CEO-visningen viser
    firmaets fulde portefølje. MED filter droppes de (INNER), fordi en sælger
    ellers ville se kunder han ikke ejer. Det rammer alle watch_de- og
    marketwire-kunder, som ACV slet ikke har brands for."""

    from .usage import customer_key

    abo = db_abonnementer(maaned)
    ejere = db_acv_ejere(owner_name, teams)
    filtreret = bool(owner_name or teams)

    resultat = []
    for r in abo:
        kunde = customer_key(r["account"], r["org_id"])
        ejer = ejere.get(kunde) or {}
        if not ejer and filtreret:
            continue
        resultat.append({
            "kunde":          kunde,
            "account":        kunde[0],
            "org_id":         kunde[1],
            "org_name":       r["org_name"],
            "sites":          r["sites"],
            "foerste_maaned": r["foerste_maaned"],
            "owner_name":     ejer.get("owner_name"),
            "teams":          ejer.get("teams"),
            # Navngivet kunde_arr_dkk og ikke arr_dkk, så den ikke kan forveksles
            # med abonnementets andel: summeres denne kolonne over en kundes tre
            # abonnementer, tælles ARR'en tre gange.
            "kunde_arr_dkk":  ejer.get("arr_dkk"),
            "acv_sites":      ejer.get("acv_sites"),
        })
    # Antallet taelles EFTER filtreringen, saa ligedelingen (naar den bruges)
    # summerer til kundens ARR inden for den visning brugeren faktisk ser.
    #
    # `har_eget` SKAL beregnes i den foerste loekke: reglen nedenfor er pr. KUNDE
    # og ikke pr. site, og den kan derfor ikke afgoeres mens man gaar raekkerne
    # igennem foerste gang.
    beloeb = db_acv_beloeb_pr_site()

    antal: dict = {}
    har_eget: dict = {}
    for r in resultat:
        antal[r["kunde"]] = antal.get(r["kunde"], 0) + 1
        if (r["kunde"][0], r["kunde"][1], r["sites"]) in beloeb:
            har_eget[r["kunde"]] = True

    # TRE UDFALD, og det sidste er hele grunden til at reglen er pr. kunde.
    # Maalt 2026-08-18 mod juli 2026, FOER den danske afgraensning 25-08 (alle
    # lande med, samme forbehold som i db_acv_beloeb_pr_site's docstring):
    #
    #   1. Sitet har sin egen ACV-raekke              -> rigtigt beloeb.  15.039
    #   2. Kunden har INGEN beloeb paa noget af sine  -> ligedeling.           2
    #   3. Kunden har beloeb paa ANDRE sites, ikke her -> None.               34
    #
    # NUMMER 3 MAA IKKE LIGEDELES. Et ligedelt tal ved siden af rigtige beloeb
    # paa samme kundeside modsiger dem: kunden ville se fem beloeb, hvor det
    # femte er regnet paa en anden maade end de fire. None siger i stedet "vi ved
    # det ikke", og maskineriet findes allerede: fold_risici taeller
    # `abonnementer_med_arr` for sig, og prioriteringslisten viser tagget
    # "scoren daekker 3 af 4", som fra nu af begynder at virke.
    #
    # Nummer 2 beholder ligedelingen, saa de kunder ikke stilles DAARLIGERE end
    # i dag. Det er to abonnementer, og de har ingen rigtige beloeb at modsige.
    for r in resultat:
        r["sites_i_alt"] = antal[r["kunde"]]
        eget = beloeb.get((r["kunde"][0], r["kunde"][1], r["sites"]))
        if eget is not None:
            r["arr_pr_abonnement"] = eget
            r["arr_kilde"] = "site"
        # `> 0` og ikke `is not None`: en total paa nul er summen af ukendte
        # beloeb og ikke en maaling, jf. nul-reglen i db_acv_beloeb_pr_site. 2.311
        # af 11.620 kunder har en samlet ACV paa nul, og at dele det ud ville
        # give dem et beloeb der ser maalt ud og er nul.
        elif not har_eget.get(r["kunde"]) and (r["kunde_arr_dkk"] or 0) > 0:
            r["arr_pr_abonnement"] = r["kunde_arr_dkk"] / r["sites_i_alt"]
            r["arr_kilde"] = "lige_deling"
        else:
            r["arr_pr_abonnement"] = None
            r["arr_kilde"] = None
    return resultat

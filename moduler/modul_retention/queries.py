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
    så `active_count` fortsat er 15.123 for indeværende måned og hele historikken
    er sammenlignelig med de tal der er valideret tidligere.

    Ejeren kommer fra ACV (se _acv_owner_cte), ikke længere fra
    dbo.retention_owner. Det ændrer IKKE `active_count`, kun `attributed_count`:
    målt 2026-08-04 flytter den fra 12.623 til 13.916 af 15.123 rækker, altså fra
    83,5% til 92,0% dækning. Gruppen '(Ikke tilskrevet)' i grafen falder dermed
    fra ~2.500 til ~1.200. Skiftet blev valgt for at risikolisten og denne graf
    kan bruge samme ejer-definition — de var før uenige om 47% af rækkerne.

    Ejer-filteret ligger i WHERE og ikke i ON: i ON ville rækker der tilhører
    ANDRE sælgere blive bevaret med NULL (LEFT JOIN-semantik), så en sælger ville
    se firmaets samlede tal og tro det var hans egen bog. I WHERE bliver joinet
    reelt et inner join, hvilket er det ønskede for sælger/team-visninger, mens
    den ufiltrerede CEO-visning beholder LEFT-adfærden.
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
           f"""WITH {cte}
            SELECT r.FirstDayOfMonth,
                   COUNT(*)             AS active_count,
                   COUNT(k.owner_name)  AS attributed_count
            FROM dbo.retention r
            LEFT JOIN acv_kunde k
                   ON k.account = r.account AND k.org_id = r.org_id
            WHERE r.FirstDayOfMonth <= EOMONTH(GETDATE())
                {_KUN_B2B}
                {clause}
            GROUP BY r.FirstDayOfMonth
            ORDER BY r.FirstDayOfMonth;""",
            params,
        )
        result = cur.fetchall()
        conn.close()
        return result
    except Exception:
        logger.exception("db_monthly_active_counts fejlede")
        return []


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
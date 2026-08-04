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


def db_monthly_active_counts(owner_name: str | None = None,
                             teams: list | None = None) -> list:
    """Antal aktive org+site-kombinationer pr. måned — retention-trendlinjen."""
    clause = ''
    params: tuple = ()

    if owner_name:
        clause += ' AND o.owner_name = %s'
        params += (owner_name,)
    # Kendt begrænsning (verificeret 2026-08-03): Watch DE's 9 kunde×site-
    # kombinationer ejes alle af Christian Linde, som ikke findes i HubUsers.
    # dbo.retention_owner mapper dem til 'Team Watch DE', men det team findes
    # ikke i Teams — så ingen afdelingsleder kan få dem med i et team-filter.
    # De tælles derfor kun i den ufiltrerede (firmabrede) visning.
    if teams:
        ph = ','.join(['%s'] * len(teams))
        clause += f' AND o.team IN ({ph})'
        params += tuple(teams)

    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute(
           f"""SELECT FirstDayOfMonth,
             COUNT(*) AS active_count,
             COUNT(o.owner_name) AS attributed_count
            FROM dbo.retention r
            LEFT JOIN dbo.retention_owner o
            ON o.account = r.account
            AND o.org_id = r.org_id
            AND ISNULL(o.sites, '') = ISNULL(r.sites, '')
            WHERE FirstDayOfMonth <= EOMONTH(GETDATE())
                {clause}
            GROUP BY FirstDayOfMonth
            ORDER BY FirstDayOfMonth;""",
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

    **Ejeren kommer fra ACV, ikke fra `dbo.retention_owner`.** De to er uenige om
    47% af rækkerne, fordi de svarer på hver sit spørgsmål: ACV's `owner_name` er
    org-ejeren (2.402 af 2.403 fler-site-kunder har præcis én), mens
    `retention_owner` er ejeren på det enkelte sites seneste start-deal. Til en
    risikoliste er org-ejeren den rigtige: det er hende der skal ringe, og ARR kan
    summeres uden at samme kunde tælles i tre sælgeres lister. Overview-grafen på
    samme side bruger fortsat `retention_owner` — forskellen SKAL fremgå af UI'et.

    Team udledes af `HubUsers` + `TeamMemberships` (som `get_led_teams`), ikke af
    `PipedriveDeals.team`: det er den kilde rolle-modellen og admin-UI'et allerede
    vedligeholder. Team-filteret er et `EXISTS`, ikke et join, så en ejer med to
    team-medlemskaber ikke dublerer kundens ARR.

    Kunder uden ACV-række får `arr_dkk = NULL` og ingen ejer — de forsvinder
    derfor ud af enhver sælger-filtreret visning og tælles kun firmabredt, præcis
    som `(Ikke tilskrevet)` i overview-grafen. Det gælder alle watch_de- og
    marketwire-kunder, fordi ACV ikke har de brands.
    """
    if not ACV_BRAND_TO_ACCOUNT:
        logger.error("ACV_BRAND_TO_ACCOUNT er tom — ingen ARR kan kobles")
        return []

    # Brand→account-mappingen sendes ind som en VALUES-tabel, så den kun findes
    # ét sted i koden. Værdierne går gennem params, aldrig interpoleret.
    brand_rows = ",".join(["(%s,%s)"] * len(ACV_BRAND_TO_ACCOUNT))
    params: tuple = ()
    for brand, account in ACV_BRAND_TO_ACCOUNT.items():
        params += (brand, account)

    clause = ''
    if owner_name:
        clause += ' AND k.owner_name = %s'
        params += (owner_name,)
    if teams:
        ph = ','.join(['%s'] * len(teams))
        clause += f"""
            AND EXISTS (
                SELECT 1
                FROM dbo.HubUsers u
                JOIN dbo.TeamMemberships tm ON tm.user_id = u.id
                JOIN dbo.Teams t ON t.id = tm.team_id
                WHERE u.name = k.owner_name
                  AND t.name IN ({ph})
                  AND {_AKTIVT_MEDLEMSKAB}
            )"""
        params += tuple(teams)

    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute(
            f"""WITH brand_map AS (
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
            ),
            aktive AS (
                -- Kun indeværende måned. dbo.retention projicerer frem til
                -- 2030-12, så uden dette filter ville alle fremtidige måneder med.
                SELECT account, org_id,
                       MAX(org_name) AS org_name,
                       COUNT(*)      AS sites,
                       -- Site-navnene skal med, så UI'et kan skelne en tavs kunde
                       -- fra en kunde der ikke KAN trackes: FINANS DK sætter
                       -- aldrig access_account_number i Snowplow.
                       STRING_AGG(ISNULL(sites, ''), ', ')
                           WITHIN GROUP (ORDER BY sites) AS sites_list
                FROM dbo.retention
                WHERE FirstDayOfMonth = DATEFROMPARTS(YEAR(GETDATE()), MONTH(GETDATE()), 1)
                  -- KUN B2B. Hver Pipedrive-account har én organisation ved navn
                  -- 'Web Sale' (watch_medier har også 'Web Sale Euro'), som samler
                  -- alle B2C-selvbetjenings-abonnementer i én kunstig kunde. Den
                  -- hører ikke i en churn-liste: ingen sælger ejer den, og ét
                  -- samlet ARR-tal for tusindvis af privatabonnenter kan ingen
                  -- handle på. Mønsteret er kodebasens eget
                  -- (PD_WEB_SALE_NAME_LIKE i modul_portfolio_alignment).
                  --
                  -- Verificeret 2026-08-04: der er i forvejen NUL Web
                  -- Sale-organisationer i dbo.retention, så filteret fjerner intet
                  -- i dag. Det står som en spærre, fordi PipeDrive_ACV DERIMOD har
                  -- dem (6,07 mio. kr. fordelt på Watch DK, Watch NO, Monitor og
                  -- Watch SE) — begynder en Web Sale-deal at ramme en af
                  -- retention-viewets pipelines, ville de kroner ellers dukke op
                  -- som én kunde i toppen af risikolisten.
                  AND ISNULL(org_name, '') NOT LIKE 'Web Sale%'
                GROUP BY account, org_id
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
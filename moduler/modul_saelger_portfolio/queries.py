import os
import pymssql
from dotenv import load_dotenv

load_dotenv()

def get_conn():
    return pymssql.connect(
        server=os.getenv("DB_SERVER"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME", "INTOMEDIA"),
        tds_version="7.0",
        login_timeout=5,
        timeout=5,
    )

def get_led_teams(user_id: int) -> list:
    """Teamnavne hvor brugeren har et AKTIVT medlemskab med rollen 'leader'.

    Bruges til sælger-dropdownen: en sales manager må kun vælge sælgere fra
    de teams, han/hun er leder for (fx må Watch DK-lederen ikke se Monitor).
    """
    conn = get_conn()
    cur = conn.cursor(as_dict=True)
    cur.execute("""
        SELECT DISTINCT t.name
        FROM TeamMemberships tm
        JOIN Teams t ON t.id = tm.team_id
        WHERE tm.user_id = %s
          AND tm.role = 'leader'
          AND tm.start_date <= CONVERT(varchar(10), GETDATE(), 23)
          AND (tm.end_date IS NULL OR tm.end_date >= CONVERT(varchar(10), GETDATE(), 23))
    """, (user_id,))
    rows = [r["name"] for r in cur.fetchall()]
    conn.close()
    return rows


def get_team_member_owners(team_names: list) -> list:
    """Aktive hub-brugere med aktivt medlemskab af mindst ét af de angivne
    teams — sorteret efter navn. Tom liste ind = tom liste ud (fail-closed).
    """
    if not team_names:
        return []
    placeholders = ",".join(["%s"] * len(team_names))
    conn = get_conn()
    cur = conn.cursor(as_dict=True)
    cur.execute(f"""
        SELECT DISTINCT u.name
        FROM HubUsers u
        JOIN TeamMemberships tm ON tm.user_id = u.id
        JOIN Teams t ON t.id = tm.team_id
        WHERE u.is_active = 1
          AND t.name IN ({placeholders})
          AND tm.start_date <= CONVERT(varchar(10), GETDATE(), 23)
          AND (tm.end_date IS NULL OR tm.end_date >= CONVERT(varchar(10), GETDATE(), 23))
        ORDER BY u.name
    """, tuple(team_names))
    rows = [r["name"] for r in cur.fetchall()]
    conn.close()
    return rows


def get_available_owners(teams: list | None = None) -> list:
    """Aktive hub-brugere der kan vælges som sælger.

    Med `teams` begrænses listen til brugere, der har deals i mindst ét af de
    angivne teams (samme afgrænsning som HubUserTeamAccess i perf-modulet).
    """
    conn = get_conn()
    cur = conn.cursor(as_dict=True)
    if teams:
        placeholders = ",".join(["%s"] * len(teams))
        cur.execute(f"""
            SELECT u.[name]
            FROM [dbo].[HubUsers] u
            WHERE u.[is_active] = 1
              AND EXISTS (
                  SELECT 1 FROM [dbo].[PipedriveDeals] d
                  WHERE d.owner_name = u.[name] AND d.team IN ({placeholders})
              )
            ORDER BY u.[name]
        """, tuple(teams))
    else:
        cur.execute("""
            SELECT [name]
            FROM [dbo].[HubUsers]
            WHERE [is_active] = 1
            ORDER BY [name]
        """)
    rows = [r["name"] for r in cur.fetchall()]
    conn.close()
    return rows

# Admin-kun specialvalg i sælger-dropdownen: kunder helt uden ejer i
# Pipedrive. Matcher owner_name IS NULL i stedet for et navn.
UNASSIGNED_OWNER = "(Uden ejer)"

# Abonnements-deal_types: DK bruger 'Abonnement', NO/SE bruger 'Subscription'.
# (SQL Server-collationen er case-insensitiv, så lowercase matcher fint.)
SUBSCRIPTION_DEAL_TYPES = ("abonnement", "subscription")


def _owner_clause(alias: str, owner_name: str) -> tuple[str, tuple]:
    """SQL-betingelse for ejerskab — håndterer specialvalget (Uden ejer)."""
    if owner_name == UNASSIGNED_OWNER:
        return f"{alias}.owner_name IS NULL", ()
    return f"{alias}.owner_name = %s", (owner_name,)


def get_kundeliste(owner_name: str) -> list:
    """Sælgerens kundeportefølje: ACV pr. kunde, kun NYESTE række pr. (kunde, site).

    PipeDrive_ACV tilføjer en ny række hver gang et site på en kunde opdateres,
    så samme (org_id, site) kan have flere rækker med forskellig updated_at.
    Et globalt updated_at = MAX-filter ville kun beholde den allernyeste række i
    HELE tabellen (og dermed kun vise den sælger, der sidst blev opdateret).

    Vi tager derfor den seneste række pr. (org_id, site) og summer kundens sites
    til én porteføljeværdi pr. org. Ejer-filteret lægges PÅ den seneste række, så
    en kunde der er flyttet til en anden sælger korrekt forsvinder fra bogen.
    """
    clause, params = _owner_clause("l", owner_name)
    conn = get_conn()
    cur = conn.cursor(as_dict=True)
    cur.execute(f"""
        WITH latest AS (
            SELECT
                org_id, org_name, owner_name, site,
                acv_value, acv_value_dkk, currency, last_activation,
                ROW_NUMBER() OVER (
                    PARTITION BY org_id, site ORDER BY updated_at DESC
                ) AS rn
            FROM [dbo].[PipeDrive_ACV]
        )
        SELECT
            l.org_name,
            l.org_id,
            l.owner_name,
            SUM(l.acv_value)     AS value,
            SUM(l.acv_value_dkk) AS value_dkk,
            MAX(l.currency)      AS currency,
            CONVERT(varchar(10), MAX(l.last_activation), 23) AS last_activation
        FROM latest l
        WHERE l.rn = 1
          AND {clause}
        GROUP BY l.org_id, l.org_name, l.owner_name
        ORDER BY value_dkk DESC
    """, params)
    rows = cur.fetchall()
    conn.close()
    for r in rows:
        r["value"]     = float(r["value"] or 0)
        r["value_dkk"] = float(r["value_dkk"] or 0)
        r["currency"]  = r["currency"] or "DKK"
    return rows


def get_org_owners(org_id: str) -> list:
    """Kundens nuværende ejere i PipeDrive_ACV (None = uden ejer).
    Bruges til adgangskontrol på kunde-siden: brugeren skal kunne se mindst
    én af kundens ejere via sælger-dropdownens regler.
    """
    conn = get_conn()
    cur = conn.cursor(as_dict=True)
    cur.execute("""
        SELECT DISTINCT owner_name
        FROM [dbo].[PipeDrive_ACV]
        WHERE org_id = %s
    """, (org_id,))
    rows = [r["owner_name"] for r in cur.fetchall()]
    conn.close()
    return rows


def get_owner_accounts(owner_name: str) -> list:
    """Sælgerens brand/account(s) — udledt af deres egne abonnements-deals.

    En Monitor-sælger (fx Caroline) får ['monitor'], en Watch DK-sælger
    ['watch_dk'] osv. Bruges til at afgrænse kundesidens abonnementer til
    sælgerens eget brand, så en Monitor-sælger ikke ser kundens Watch-deals.
    Tomt navn / (Uden ejer) → tom liste (kalderen dropper så account-filteret).
    """
    if not owner_name or owner_name == UNASSIGNED_OWNER:
        return []
    dt_ph = ",".join(["%s"] * len(SUBSCRIPTION_DEAL_TYPES))
    conn = get_conn()
    cur = conn.cursor(as_dict=True)
    cur.execute(f"""
        SELECT DISTINCT account
        FROM [dbo].[PipedriveDeals]
        WHERE owner_name = %s
          AND deal_type IN ({dt_ph})
          AND account IS NOT NULL
          AND account <> ''
    """, (owner_name,) + SUBSCRIPTION_DEAL_TYPES)
    rows = [r["account"] for r in cur.fetchall()]
    conn.close()
    return rows


def get_customer_portfolio(org_id: str, owner: str = "") -> dict:
    """Kundens aktive abonnementer for sælgerens brand.

    Pr. deal: titel, deal-id, sælger, aktiveringsdato og porteføljeværdi
    (SUM af value_dkk). Afgrænses til deal_type='abonnement' og — hvis
    sælgerens brand kan udledes — til sælgerens account(s) (get_owner_accounts).
    Kan sælgerens brand ikke udledes (fx (Uden ejer)), droppes account-filteret,
    og alle kundens abonnementer vises.
    """
    accounts = get_owner_accounts(owner)
    dt_ph = ",".join(["%s"] * len(SUBSCRIPTION_DEAL_TYPES))

    conn = get_conn()
    cur = conn.cursor(as_dict=True)

    # Kundenavn som fallback, hvis kunden ingen abonnements-deals har at vise.
    cur.execute("""
        SELECT TOP 1 org_name FROM [dbo].[PipeDrive_ACV]
        WHERE org_id = %s ORDER BY acv_value_dkk DESC
    """, (org_id,))
    name_row = cur.fetchone()

    account_clause = ""
    params: tuple = (org_id,) + SUBSCRIPTION_DEAL_TYPES
    if accounts:
        ph = ",".join(["%s"] * len(accounts))
        account_clause = f"AND d.account IN ({ph})"
        params = params + tuple(accounts)

    # value = lokal valuta (NOK/SEK/EUR/USD/DKK), value_dkk = omregnet til DKK
    # (bruges til sortering/totaler på tværs af valutaer). currency styrer hvad
    # frontend viser, så NO/SE-kunder vises i deres egen valuta.
    cur.execute(f"""
        SELECT
            d.title,
            d.pd_deal_id,
            d.org_id,
            d.org_name,
            d.owner_name,
            CONVERT(varchar(10), d.service_activation_date, 23) AS service_activation_date,
            SUM(d.value)     AS value,
            SUM(d.value_dkk) AS value_dkk,
            MAX(d.currency)  AS currency
        FROM [dbo].[PipedriveDeals] d
        WHERE d.org_id = %s
          AND d.deal_type IN ({dt_ph})
          {account_clause}
        GROUP BY d.title, d.org_name, d.org_id, d.pd_deal_id,
                 d.owner_name, d.service_activation_date
        ORDER BY value_dkk DESC
    """, params)

    deals = []
    org_name = None
    for r in cur.fetchall():
        org_name = org_name or r.get("org_name")
        deals.append({
            "title":           r["title"] or "(Uden titel)",
            "pd_deal_id":      r["pd_deal_id"],
            "owner_name":      r["owner_name"] or "—",
            "activation_date": r["service_activation_date"] or "—",
            "value":           float(r["value"] or 0),
            "value_dkk":       float(r["value_dkk"] or 0),
            "currency":        r["currency"] or "DKK",
        })
    conn.close()

    return {
        "org_id":      str(org_id),
        "org_name":    org_name or (name_row or {}).get("org_name") or str(org_id),
        "owner":       owner,
        "accounts":    accounts,
        "deals":       deals,
    }

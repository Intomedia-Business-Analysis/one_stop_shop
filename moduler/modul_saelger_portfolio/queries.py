import os
import pymssql
from dotenv import load_dotenv

from constants import CANCELLATION_PIPELINES

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


def _owner_clause(alias: str, owner_name: str) -> tuple[str, tuple]:
    """SQL-betingelse for ejerskab — håndterer specialvalget (Uden ejer)."""
    if owner_name == UNASSIGNED_OWNER:
        return f"{alias}.owner_name IS NULL", ()
    return f"{alias}.owner_name = %s", (owner_name,)


def get_kundeliste(owner_name: str) -> list:
    a_clause, a_params = _owner_clause("a", owner_name)
    a2_clause, a2_params = _owner_clause("acv", owner_name)
    d_clause, d_params = _owner_clause("d", owner_name)
    conn = get_conn()
    cursor = conn.cursor(as_dict=True)
    # first_deal_owner (hvem vandt kundens ALLERFØRSTE deal, på tværs af
    # ejere — bruges af "Skaffet af sælger"-visningen) beregnes som windowed
    # JOIN frem for korreleret subquery: pr.-række-opslag mod PipedriveDeals
    # tog flere sekunder pr. sælger.
    cursor.execute(f"""
        SELECT
            a.org_name,
            a.org_id,
            a.site,
            a.acv_value_dkk,
            a.brand,
            a.first_activation,
            a.last_activation,
            (
                SELECT TOP 1 d.team
                FROM [dbo].[PipedriveDeals] d
                WHERE d.org_id = a.org_id
                  AND {d_clause}
                  AND d.team IS NOT NULL
                  AND d.team <> ''
                ORDER BY d.won_time DESC
            ) AS team,
            fd.owner_name AS first_deal_owner,
            CONVERT(varchar(10), (
                -- Seneste RIGTIGE salgskontakt: samme eksklusioner som
                -- resten af siden, så adm-omposteringer og webshop-salg
                -- ikke får kunden til at se friskere ud, end den er.
                SELECT MAX(d.won_time)
                FROM [dbo].[PipedriveDeals] d
                WHERE d.org_id = a.org_id
                  AND {d_clause}
                  AND d.status = 'won'
                  AND d.pipeline_name <> 'Web Sale'
                  AND (COALESCE(d.administrativ,'') <> 'ja')
                  AND UPPER(LTRIM(d.title)) NOT LIKE 'ADMINISTRATIV%%'
                  AND UPPER(LTRIM(d.title)) NOT LIKE 'ADM %%'
                  AND COALESCE(d.deal_type,'') <> 'Rapport'
            ), 23) AS last_deal_date
        FROM [dbo].[PipeDrive_ACV] a
        LEFT JOIN (
            SELECT d2.org_id, d2.owner_name,
                   ROW_NUMBER() OVER (PARTITION BY d2.org_id ORDER BY d2.won_time ASC) AS rn
            FROM [dbo].[PipedriveDeals] d2
            WHERE d2.status = 'won'
              AND d2.won_time IS NOT NULL
              AND (COALESCE(d2.administrativ,'') <> 'ja')
              AND UPPER(LTRIM(d2.title)) NOT LIKE 'ADMINISTRATIV%%'
              AND UPPER(LTRIM(d2.title)) NOT LIKE 'ADM %%'
              AND COALESCE(d2.deal_type,'') <> 'Rapport'
              AND d2.org_id IN (
                  SELECT acv.org_id FROM [dbo].[PipeDrive_ACV] acv WHERE {a2_clause}
              )
        ) fd ON fd.org_id = a.org_id AND fd.rn = 1
        WHERE {a_clause}
        ORDER BY a.acv_value_dkk DESC
    """, d_params + d_params + a2_params + a_params)
    rows = cursor.fetchall()
    conn.close()
    for r in rows:
        if r.get("acv_value_dkk") is not None:
            r["acv_value_dkk"] = float(r["acv_value_dkk"])
    return rows


def get_org_owners(org_id: str) -> list:
    """Kundens nuværende ejere i PipeDrive_ACV (None = uden ejer).
    Bruges til adgangskontrol på kunde-historik-siden: brugeren skal kunne
    se mindst én af kundens ejere via sælger-dropdownens regler.
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


def get_customer_history(org_id: str, owner: str = "") -> dict:
    """Kundens fulde deal-historik men kun for den valgte sælger + stamdata fra ACV.

    Vundne deals ekskl. administrativ/rapport-støj. Web Sale og opsigelser
    MEDTAGES — de er en del af kundens historie og er synlige via
    pipeline-kolonnen. NO/SE i lokal valuta som resten af siden.
    by_year deler salg og opsigelser på aktiveringsår (vækstkort-princip).
    """
    cancel_ph = ",".join(["%s"] * len(CANCELLATION_PIPELINES))
    value_expr = ("CAST(COALESCE(CASE WHEN d.currency IN ('NOK','SEK') "
                  "THEN d.value ELSE d.value_dkk END, d.value) AS DECIMAL(18,2))")
    act = "COALESCE(d.service_activation_date, d.won_time)"
    d_clause, d_params = _owner_clause("d", owner)

    conn = get_conn()
    cur = conn.cursor(as_dict=True)

    # Stamdata fra ACV (navn, samlet ACV, første aktivering, sites)
    cur.execute("""
        SELECT TOP 1 org_name FROM [dbo].[PipeDrive_ACV]
        WHERE org_id = %s ORDER BY acv_value_dkk DESC
    """, (org_id,))
    name_row = cur.fetchone()
    cur.execute("""
        SELECT ISNULL(SUM(acv_value_dkk), 0) AS acv,
                ISNULL(SUM(CASE WHEN owner_name = %s THEN acv_value_dkk ELSE 0 END), 0) AS acv_own,
               CONVERT(varchar(10), MIN(first_activation), 23) AS first_activation,
               COUNT(DISTINCT site) AS sites
        FROM [dbo].[PipeDrive_ACV] WHERE org_id = %s
    """, (owner,org_id))
    meta = cur.fetchone() or {}

    # Deal-historikken
    cur.execute(f"""
        SELECT
            d.title,
            COALESCE(d.pipeline_name, '') AS pipeline,
            COALESCE(d.sites, '')         AS sites,
            COALESCE(d.owner_name, '—')   AS owner_name,
            CONVERT(varchar(10), d.won_time, 23)                AS vundet,
            CONVERT(varchar(10), d.service_activation_date, 23) AS aktiveret,
            CAST({value_expr} AS FLOAT)   AS value,
            YEAR({act})                   AS aar,
            CASE WHEN d.pipeline_name IN ({cancel_ph}) THEN 1 ELSE 0 END AS er_opsigelse
        FROM [dbo].[PipedriveDeals] d
        WHERE d.org_id = %s
        AND {d_clause}
          AND d.status = 'won'
          AND (COALESCE(d.administrativ,'') <> 'ja')
          AND UPPER(LTRIM(d.title)) NOT LIKE 'ADMINISTRATIV%%'
          AND UPPER(LTRIM(d.title)) NOT LIKE 'ADM %%'
          AND COALESCE(d.deal_type,'') <> 'Rapport'
        ORDER BY d.won_time DESC
    """, tuple(CANCELLATION_PIPELINES) + (org_id,) + d_params)
    deals = []
    for r in cur.fetchall():
        deals.append({
            "title":        r["title"] or "(Uden titel)",
            "pipeline":     r["pipeline"],
            "sites":        r["sites"] or "—",
            "owner_name":   r["owner_name"],
            "vundet":       r["vundet"] or "—",
            "aktiveret":    r["aktiveret"] or "—",
            "value":        float(r["value"] or 0),
            "aar":          int(r["aar"]) if r["aar"] else None,
            "er_opsigelse": bool(r["er_opsigelse"]),
        })

    # Pr. år: salg vs. opsigelser (aktiveringsår, som vækstkortet)
    by_year: dict[int, dict] = {}
    for d in deals:
        if d["aar"] is None:
            continue
        y = by_year.setdefault(d["aar"], {"aar": d["aar"], "salg": 0.0, "ops": 0.0, "antal": 0})
        if d["er_opsigelse"]:
            y["ops"] += abs(d["value"])
        else:
            y["salg"] += d["value"]
            y["antal"] += 1

    conn.close()
    return {
        "org_name":         (name_row or {}).get("org_name") or str(org_id),
        "acv":              float(meta.get("acv") or 0),
        "acv_own":          float(meta.get("acv_own") or 0),
        "first_activation": meta.get("first_activation"),
        "site_count":       int(meta.get("sites") or 0),
        "by_year":          sorted(by_year.values(), key=lambda y: y["aar"]),
        "deals":            deals,
    }


def get_growth_timeline(owner_name: str) -> list:
    """Porteføljevækst for én sælger, fordelt på år og måned.

    'Vækst i eksisterende portefølje' = sælgerens vundne deals på kunder, der
    allerede EKSISTEREDE før starten af det år, dealen aktiveres
    (first_activation før 1/1 i aktiveringsåret — årskohorte-princip). Deals
    på nye kunder tæller altså IKKE med — det er mersalg/upsell på den
    eksisterende kundebase.

    Eksistens-tjekket er bevidst på KUNDE-niveau (org_id), ikke "i sælgerens
    nuværende bog": ellers ville sælgerens historik ændre sig, hver gang en
    kunde omfordeles i Pipedrive (gamle deals forsvandt fra grafen, når
    kunden fik ny ejer). Sælgerens egne deals er stadig afgrænsningen —
    historikken er bare stabil under bog-omrokeringer.

    Selve deal-afgrænsningen følger Tilvækst-logikken fra manager-dashboardet
    (modul_perf.db_manager_data):
      - dateres efter service_activation_date (fallback: won_time)
      - Web Sale og administrative deals ekskluderes
      - annoncesalg (Banner/Job-pipelines og advertising-accounts)
        ekskluderes — dashboardet er til abonnementssælgere
      - NO/SE-deals regnes i lokal valuta, øvrige i DKK
      - opsigelser (CANCELLATION_PIPELINES) holdes adskilt fra salget,
        så frontend kan vise won, opsigelser og netto hver for sig

    Returnerer rækker {yr, mth, deals, won_dkk, ops_dkk}.
    """
    cancel_ph = ",".join(["%s"] * len(CANCELLATION_PIPELINES))
    value_expr = ("CAST(COALESCE(CASE WHEN d.currency IN ('NOK','SEK') "
                  "THEN d.value ELSE d.value_dkk END, d.value) AS DECIMAL(18,2))")
    act_date = "COALESCE(d.service_activation_date, d.won_time)"

    d_clause, d_params = _owner_clause("d", owner_name)

    conn = get_conn()
    cur = conn.cursor(as_dict=True)
    cur.execute(f"""
        SELECT
            YEAR({act_date})  AS yr,
            MONTH({act_date}) AS mth,
            SUM(CASE WHEN d.pipeline_name NOT IN ({cancel_ph}) THEN 1 ELSE 0 END) AS deals,
            SUM(CASE WHEN d.pipeline_name NOT IN ({cancel_ph})
                THEN {value_expr} ELSE 0 END) AS won_dkk,
            SUM(CASE WHEN d.pipeline_name IN ({cancel_ph})
                THEN ABS({value_expr}) ELSE 0 END) AS ops_dkk
        FROM [dbo].[PipedriveDeals] d
        WHERE {d_clause}
          AND d.status = 'won'
          AND d.pipeline_name <> 'Web Sale'
          AND (COALESCE(d.administrativ,'') <> 'ja')
          AND UPPER(LTRIM(d.title)) NOT LIKE 'ADMINISTRATIV%%'
          AND UPPER(LTRIM(d.title)) NOT LIKE 'ADM %%'
          AND COALESCE(d.deal_type,'') <> 'Rapport'
          -- Annoncesalg (Banner/Job) holdes ude: dette dashboard er til
          -- abonnementssælgere — Banner & Job har deres eget dashboard.
          AND COALESCE(d.account,'') NOT IN ('jppol_advertising','watch_no_advertising')
          AND UPPER(COALESCE(d.pipeline_name,'')) NOT IN ('BANNER','JOB')
          AND {act_date} IS NOT NULL
          -- EXISTS i stedet for JOIN: PipeDrive_ACV har én række pr. site,
          -- så en JOIN ville tælle samme deal én gang pr. kundens sites.
          -- Kunde-niveau (ingen ejer-match): historikken må ikke ændre sig,
          -- når kunder omfordeles mellem sælgere i Pipedrive.
          AND EXISTS (
              SELECT 1 FROM [dbo].[PipeDrive_ACV] a
              WHERE a.org_id = d.org_id
                AND a.first_activation < DATEFROMPARTS(YEAR({act_date}), 1, 1)
          )
        GROUP BY YEAR({act_date}), MONTH({act_date})
        ORDER BY yr, mth
    """, tuple(CANCELLATION_PIPELINES) * 3 + d_params)
    rows = cur.fetchall()
    conn.close()
    for r in rows:
        r["won_dkk"] = float(r["won_dkk"] or 0)
        r["ops_dkk"] = float(r["ops_dkk"] or 0)
    return rows

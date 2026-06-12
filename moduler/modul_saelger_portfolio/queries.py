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

def get_kundeliste(owner_name: str) -> list:
    conn = get_conn()
    cursor = conn.cursor(as_dict=True)
    cursor.execute("""
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
                  AND d.owner_name = %s
                  AND d.team IS NOT NULL
                  AND d.team <> ''
                ORDER BY d.won_time DESC
            ) AS team,
            CONVERT(varchar(10), (
                SELECT MAX(d.won_time)
                FROM [dbo].[PipedriveDeals] d
                WHERE d.org_id = a.org_id
                  AND d.owner_name = %s
                  AND d.status = 'won'
            ), 23) AS last_deal_date
        FROM [dbo].[PipeDrive_ACV] a
        WHERE a.owner_name = %s
        ORDER BY a.acv_value_dkk DESC
    """, (owner_name, owner_name, owner_name))
    rows = cursor.fetchall()
    conn.close()
    for r in rows:
        if r.get("acv_value_dkk") is not None:
            r["acv_value_dkk"] = float(r["acv_value_dkk"])
    return rows


def get_growth_timeline(owner_name: str) -> list:
    """Porteføljevækst for én sælger, fordelt på år og måned.

    'Vækst i eksisterende portefølje' = vundne deals på kunder, der allerede
    var i sælgerens portefølje før starten af det år, dealen aktiveres
    (first_activation før 1/1 i aktiveringsåret). Deals på nye kunder tæller
    altså IKKE med — det er mersalg/upsell på den eksisterende kundebase.

    Selve deal-afgrænsningen følger Tilvækst-logikken fra manager-dashboardet
    (modul_perf.db_manager_data):
      - dateres efter service_activation_date (fallback: won_time)
      - Web Sale og administrative deals ekskluderes
      - NO/SE-deals regnes i lokal valuta, øvrige i DKK
      - opsigelser (CANCELLATION_PIPELINES) holdes adskilt fra salget,
        så frontend kan vise won, opsigelser og netto hver for sig

    Returnerer rækker {yr, mth, deals, won_dkk, ops_dkk}.
    """
    cancel_ph = ",".join(["%s"] * len(CANCELLATION_PIPELINES))
    value_expr = ("CAST(COALESCE(CASE WHEN d.currency IN ('NOK','SEK') "
                  "THEN d.value ELSE d.value_dkk END, d.value) AS DECIMAL(18,2))")
    act_date = "COALESCE(d.service_activation_date, d.won_time)"

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
        WHERE d.owner_name = %s
          AND d.status = 'won'
          AND d.pipeline_name <> 'Web Sale'
          AND (COALESCE(d.administrativ,'') <> 'ja')
          AND UPPER(LTRIM(d.title)) NOT LIKE 'ADMINISTRATIV%%'
          AND UPPER(LTRIM(d.title)) NOT LIKE 'ADM %%'
          AND COALESCE(d.deal_type,'') <> 'Rapport'
          AND {act_date} IS NOT NULL
          -- EXISTS i stedet for JOIN: PipeDrive_ACV har én række pr. site,
          -- så en JOIN ville tælle samme deal én gang pr. kundens sites.
          AND EXISTS (
              SELECT 1 FROM [dbo].[PipeDrive_ACV] a
              WHERE a.org_id = d.org_id
                AND a.owner_name = d.owner_name
                AND a.first_activation < DATEFROMPARTS(YEAR({act_date}), 1, 1)
          )
        GROUP BY YEAR({act_date}), MONTH({act_date})
        ORDER BY yr, mth
    """, tuple(CANCELLATION_PIPELINES) * 3 + (owner_name,))
    rows = cur.fetchall()
    conn.close()
    for r in rows:
        r["won_dkk"] = float(r["won_dkk"] or 0)
        r["ops_dkk"] = float(r["ops_dkk"] or 0)
    return rows

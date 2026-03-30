import os
import traceback
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

def db_get_all_users():
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute(
            "SELECT id, username, name, initials, role, brand, is_active, created_at "
            "FROM HubUsers ORDER BY name"
        )
        users = cur.fetchall()
        conn.close()
        return users
    except Exception:
        print(traceback.format_exc())
        return []

def db_create_user(username, password_hash, name, initials, role, brand):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO HubUsers (username, password_hash, name, initials, role, brand, is_active) "
        "VALUES (%s, %s, %s, %s, %s, %s, 1)",
        (username, password_hash, name, initials, role, brand),
    )
    conn.commit()
    conn.close()

def db_get_user_by_id(user_id):
    conn = get_conn()
    cur = conn.cursor(as_dict=True)
    cur.execute("SELECT * FROM HubUsers WHERE id = %s", (user_id,))
    user = cur.fetchone()
    conn.close()
    return user

def db_update_user(user_id, name, initials, role, brand, is_active, password_hash=None):
    conn = get_conn()
    cur = conn.cursor()
    if password_hash:
        cur.execute(
            "UPDATE HubUsers SET name=%s, initials=%s, role=%s, brand=%s, "
            "is_active=%s, password_hash=%s WHERE id=%s",
            (name, initials, role, brand, is_active, password_hash, user_id),
        )
    else:
        cur.execute(
            "UPDATE HubUsers SET name=%s, initials=%s, role=%s, brand=%s, "
            "is_active=%s WHERE id=%s",
            (name, initials, role, brand, is_active, user_id),
        )
    conn.commit()
    conn.close()

def db_get_user_memberships(user_id):
    conn = get_conn()
    cur = conn.cursor(as_dict=True)
    cur.execute(
        """
        SELECT tm.id, tm.team_id, tm.role, tm.start_date, tm.end_date, tm.notes,
               t.name AS team_name, t.brand AS team_brand
        FROM   TeamMemberships tm
        JOIN   Teams t ON t.id = tm.team_id
        WHERE  tm.user_id = %s
        ORDER  BY tm.start_date DESC
        """,
        (user_id,),
    )
    memberships = cur.fetchall()
    cur.execute("SELECT id, name, brand FROM Teams ORDER BY name")
    all_teams = cur.fetchall()
    conn.close()
    return memberships, all_teams

def db_add_membership(user_id, team_id, role, start_date, end_date, notes):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO TeamMemberships (user_id, team_id, role, start_date, end_date, notes) "
        "VALUES (%s, %s, %s, %s, %s, %s)",
        (user_id, team_id, role, start_date, end_date, notes),
    )
    conn.commit()
    conn.close()

def db_remove_membership(membership_id, user_id=None, team_id=None):
    conn = get_conn()
    cur = conn.cursor()
    if user_id:
        cur.execute("DELETE FROM TeamMemberships WHERE id=%s AND user_id=%s", (membership_id, user_id))
    else:
        cur.execute("DELETE FROM TeamMemberships WHERE id=%s AND team_id=%s", (membership_id, team_id))
    conn.commit()
    conn.close()

def db_save_resource_access(user_id, all_resource_ids, form_data):
    conn = get_conn()
    cur = conn.cursor()
    for rid in all_resource_ids:
        val = form_data.get(f"access_{rid}", "default")
        cur.execute(
            "DELETE FROM UserResourceAccess WHERE user_id=%s AND resource_id=%s",
            (user_id, rid),
        )
        if val != "default":
            cur.execute(
                "INSERT INTO UserResourceAccess (user_id, resource_id, access) VALUES (%s, %s, %s)",
                (user_id, rid, val),
            )
    conn.commit()
    conn.close()

def db_get_all_teams():
    conn = get_conn()
    cur = conn.cursor(as_dict=True)
    cur.execute(
        """
        SELECT t.id, t.name, t.brand, t.description, t.created_at,
               COUNT(DISTINCT tm.id) AS member_count
        FROM   Teams t
        LEFT JOIN TeamMemberships tm ON tm.team_id = t.id
        GROUP  BY t.id, t.name, t.brand, t.description, t.created_at
        ORDER  BY t.name
        """
    )
    teams = cur.fetchall()
    conn.close()
    return teams

def db_create_team(name, brand, description):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO Teams (name, brand, description) VALUES (%s, %s, %s)",
        (name, brand, description),
    )
    conn.commit()
    conn.close()

def db_get_team_by_id(team_id):
    conn = get_conn()
    cur = conn.cursor(as_dict=True)
    cur.execute("SELECT * FROM Teams WHERE id = %s", (team_id,))
    team = cur.fetchone()
    cur.execute(
        """
        SELECT tm.id, tm.user_id, tm.role, tm.start_date, tm.end_date, tm.notes,
               u.name AS user_name, u.initials, u.username
        FROM   TeamMemberships tm
        JOIN   HubUsers u ON u.id = tm.user_id
        WHERE  tm.team_id = %s
        ORDER  BY tm.start_date DESC
        """,
        (team_id,),
    )
    memberships = cur.fetchall()
    cur.execute("SELECT id, name, initials FROM HubUsers WHERE is_active = 1 ORDER BY name")
    all_users = cur.fetchall()
    conn.close()
    return team, memberships, all_users

def db_update_team(team_id, name, brand, description):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "UPDATE Teams SET name=%s, brand=%s, description=%s WHERE id=%s",
        (name, brand, description, team_id),
    )
    conn.commit()
    conn.close()

def db_update_membership(membership_id, team_id, role, start_date, end_date, notes):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "UPDATE TeamMemberships SET role=%s, start_date=%s, end_date=%s, notes=%s "
        "WHERE id=%s AND team_id=%s",
        (role, start_date, end_date, notes, membership_id, team_id),
    )
    conn.commit()
    conn.close()
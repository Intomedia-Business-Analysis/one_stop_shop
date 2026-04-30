#!/usr/bin/env python3
"""
Opret HubUsers-tabellen og den første admin-bruger.
Kør dette én gang inden du starter appen:

    python create_admin.py
"""
import os
import getpass

import pymssql
from dotenv import load_dotenv

load_dotenv()

from auth import hash_password

CREATE_TABLE_SQL = """
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='HubUsers' AND xtype='U')
CREATE TABLE HubUsers (
    id            INT IDENTITY(1,1) PRIMARY KEY,
    username      NVARCHAR(50)  UNIQUE NOT NULL,
    password_hash NVARCHAR(255) NOT NULL,
    name          NVARCHAR(100) NOT NULL,
    initials      NVARCHAR(10)  NOT NULL,
    role          NVARCHAR(30)  NOT NULL DEFAULT 'salesperson',
    brand         NVARCHAR(50)  NULL,
    is_active     BIT           NOT NULL DEFAULT 1,
    created_at    DATETIME      DEFAULT GETDATE()
);
"""


def main():
    print("=" * 50)
    print("  Intomedia Hub — Opsætning af brugertabel")
    print("=" * 50)
    print()

    conn = pymssql.connect(
        server=os.getenv("DB_SERVER"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME", "INTOMEDIA"),
        tds_version="7.0",
    )
    cur = conn.cursor()

    print("Opretter HubUsers-tabel (hvis den ikke findes)...")
    cur.execute(CREATE_TABLE_SQL)
    conn.commit()
    print("Tabel klar.\n")

    print("Udfyld oplysninger til den første admin-bruger:")
    username = input("  Brugernavn:  ").strip()
    name     = input("  Fulde navn:  ").strip()
    initials = input("  Initialer:   ").strip().upper()
    password = getpass.getpass("  Adgangskode: ")

    if not all([username, name, initials, password]):
        print("\nAlle felter er påkrævet. Prøv igen.")
        conn.close()
        return

    cur.execute(
        "SELECT COUNT(*) FROM HubUsers WHERE username = %s",
        (username,),
    )
    if cur.fetchone()[0] > 0:
        print(f"\nBrugernavn '{username}' er allerede i brug.")
        conn.close()
        return

    cur.execute(
        "INSERT INTO HubUsers (username, password_hash, name, initials, role, brand, is_active) "
        "VALUES (%s, %s, %s, %s, 'admin', NULL, 1)",
        (username, hash_password(password), name, initials),
    )
    conn.commit()
    conn.close()

    print(f"\nAdmin-bruger '{username}' oprettet.")
    print("Start appen og log ind på /login")


if __name__ == "__main__":
    main()

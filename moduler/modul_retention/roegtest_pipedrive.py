"""Røgtest af Pipedrive-koblingen mod den RIGTIGE konto.

    .venv/Scripts/python.exe moduler/modul_retention/roegtest_pipedrive.py

SKRIVER ALDRIG. Kun preview_opkalds_aktivitet kaldes, og den POSTer ikke —
den bygger payloaden og slår (kun i tilstanden `org_ejer`) organisationens ejer
op med et GET. Vil man se en rigtig aktivitet opstå, skal et menneske gøre det
bevidst gennem UI'et.

Hedder ikke `test_*.py` med vilje, samme grund som roegtest_outcomes.py: den
kræver netværk og et rigtigt API-token, og pytest-suiten skal kunne køre uden
begge dele. De rene funktioner (payload, emne, note) er dækket af
tests/test_retention_pipedrive.py, som CI kører.

Hvad testen beviser: at kontoen svarer, at org_id'et findes derovre, at
tilskrivningen følger AKTIVITET_EJER, og at payloaden kommer hele vejen
igennem. Hvad den IKKE beviser: at POST /activities accepterer den — det kan
kun ét rigtigt kald vise, og det ER vist i drift 2026-09-02 (aktivitet 91519,
siden slettet igen).
"""
import datetime as dt
import json
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
os.chdir(REPO_ROOT)

from moduler.modul_retention import pipedrive  # noqa: E402
from moduler.modul_retention.outcomes import (INGEN_KONTAKT,  # noqa: E402
                                              KONTAKT_OPNAAET)
from moduler.modul_retention.queries import db_abonnementer  # noqa: E402

_fejl = []


def tjek(navn, betingelse, detalje=""):
    print(("  OK   " if betingelse else "  FEJL ") + navn
          + ("  " + detalje if detalje else ""))
    if not betingelse:
        _fejl.append(navn)


def main() -> int:
    print("--- 1. En rigtig kunde fra dbo.retention ---")
    # Hentes fra basen frem for at stå hardkodet: et org_id skrevet i hånden
    # kan blive slettet i Pipedrive, og så fejler testen på noget andet end
    # det, den måler.
    abo = db_abonnementer("2026-08")
    tjek("db_abonnementer gav rækker", bool(abo), "%d" % len(abo))
    if not abo:
        return 1

    # Første kunde med et rigtigt site, så noten har noget at vise.
    kunde = next((r for r in abo if r.get("sites")), abo[0])
    account, org_id, site = kunde["account"], kunde["org_id"], kunde.get("sites")
    print("       account=%s org_id=%s site=%s org_name=%s"
          % (account, org_id, site, kunde.get("org_name")))
    tjek("kunden ligger på en aktiv account", account == "watch_medier", account)

    samtale = {
        "account":      account,
        "org_id":       org_id,
        "contacted_at": dt.datetime.now().replace(second=0, microsecond=0),
        "channel":      "telefon",
        "summary":      "Røgtest: intet er sendt, og intet er gemt.",
        "created_by":   "roegtest@intomedia.dk",
    }
    udfald = [{
        "site":           site,
        "contact_result": KONTAKT_OPNAAET,
        "outcome":        "fornyet",
        "arr_before_dkk": 12500.0,
        "arr_after_dkk":  None,
        "renewal_date":   dt.date(2027, 1, 1),
        "note":           "Vil gerne fortsætte <uændret> & på samme aftale.",
    }]

    print()
    print("--- 2. Har vi overhovedet et token? ---")
    token = pipedrive._token(account)
    tjek("token fundet i .env for %r" % account, bool(token))
    if not token:
        print("       (uden token kan resten ikke måles)")
        return 1

    print()
    print("--- 3. preview_opkalds_aktivitet (slår ejer op, POSTer IKKE) ---")
    res = pipedrive.preview_opkalds_aktivitet(samtale, udfald)
    tjek("previewet lykkedes", res.get("ok") is True, str(res.get("besked")))
    tjek("intet blev sendt", res.get("sendt") is False)
    tjek("der er en payload", "payload" in res)
    if "payload" not in res:
        return 1

    p = res["payload"]
    print(json.dumps(p, ensure_ascii=False, indent=2)[:1400])

    print()
    print("--- 4. Payloadens felter ---")
    tjek("type=call", p.get("type") == "call", str(p.get("type")))
    tjek("done=True", p.get("done") is True)
    tjek("org_id er int og matcher kunden",
         p.get("org_id") == int(org_id), str(p.get("org_id")))
    tjek("due_time har formen HH:MM",
         len(p.get("due_time", "")) == 5 and p["due_time"][2] == ":",
         str(p.get("due_time")))
    tjek("emnet nævner udfaldet", "Fornyet" in p.get("subject", ""),
         p.get("subject", ""))
    tjek("noten har escapet specialistens tekst",
         "&lt;uændret&gt;" in p.get("note", "") and "&amp;" in p.get("note", ""))
    tjek("noten nævner hvem der ringede",
         "roegtest@intomedia.dk" in p.get("note", ""))

    print()
    print("--- 5. Ejeren: hvem lander aktiviteten hos? ---")
    # Testen følger tilstanden i AKTIVITET_EJER frem for at forudsætte den ene.
    # Skiftes konstanten, skal røgtesten ikke skulle rettes bagefter.
    ejer = res.get("ejer_id")
    print("       AKTIVITET_EJER = %r" % pipedrive.AKTIVITET_EJER)
    if pipedrive.AKTIVITET_EJER == "org_ejer":
        tjek("organisationen har en ejer i Pipedrive", bool(ejer), str(ejer))
        tjek("owner_id er sat i payloaden", p.get("owner_id") == ejer)
    else:
        tjek("ingen ejer slaas op", ejer is None, str(ejer))
        tjek("owner_id udeladt, saa Pipedrive selv saetter tokenets bruger",
             "owner_id" not in p)

    print()
    print("--- 6. Uden kontakt sendes der intet ---")
    tom = pipedrive.preview_opkalds_aktivitet(
        samtale, [{**udfald[0], "contact_result": INGEN_KONTAKT, "outcome": None}])
    tjek("aarsag=ingen_kontakt", tom.get("aarsag") == "ingen_kontakt",
         str(tom.get("besked")))
    tjek("ok er stadig True (det er ikke en fejl)", tom.get("ok") is True)

    print()
    if _fejl:
        print("FEJLEDE: " + ", ".join(_fejl))
        return 1
    print("ALT GRØNT (og intet er skrevet, hverken i basen eller i Pipedrive)")
    return 0


if __name__ == "__main__":
    sys.exit(main())

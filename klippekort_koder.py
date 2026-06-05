# -*- coding: utf-8 -*-
"""Oversæt historiske mediekoder (fra Excel-importen) til pæne sitenavne og split
kombinerede koder ud pr. site.

Trin:
  1) python klippekort_koder.py export  -> skriver klippekort_koder.csv til gennemsyn
       (udfyld/ret kolonnen 'site' for hver kode; tom = lad koden stå urørt)
  2) python klippekort_koder.py apply           -> dry-run (viser hvad der ændres)
     python klippekort_koder.py apply --commit  -> udfør (kun importerede rækker)
"""
import csv
import os
import re
import sys
import uuid

from moduler.modul_klippekort import queries as q

CSV = os.path.join(os.path.dirname(os.path.abspath(__file__)), "klippekort_koder.csv")
IMPORT_TAG = "excel-import"

# Bedste bud pr. kode (lowercase-nøgle). Tom streng = usikker, udfyld selv.
BEST = {
    # Watch DK
    "fw": "FinansWatch DK", "wm": "Watch Medier DK",
    "mew": "MediaWatch DK", "mediawatch": "MediaWatch DK",
    "adw": "AdvokatWatch DK", "advokatwatch": "AdvokatWatch DK",
    "ew": "EnergiWatch DK", "energy": "EnergiWatch DK",
    "itw": "ITWatch DK", "itwatch": "ITWatch DK", "iw": "ITWatch DK", "it": "ITWatch DK",
    "sw": "ShippingWatch DK", "shipping": "ShippingWatch DK",
    "mw": "MedWatch DK", "medw": "MedWatch DK",
    "ejw": "EjendomsWatch DK", "ejd": "EjendomsWatch DK", "ejdw": "EjendomsWatch DK",
    "ejdw ": "EjendomsWatch DK", "ejendomswatch": "EjendomsWatch DK", "ejdw.": "EjendomsWatch DK",
    "ctw": "CleantechWatch DK", "cw": "CleantechWatch DK", "clw": "CleantechWatch DK",
    "cleantech": "CleantechWatch DK",
    "kaw": "KapitalWatch DK", "kapitalwatch": "KapitalWatch DK", "kw": "KapitalWatch DK",
    "mow": "MobilityWatch DK", "mobility": "MobilityWatch DK",
    "aw": "AgriWatch DK", "agw": "AgriWatch DK", "agri": "AgriWatch DK", "agriwatch": "AgriWatch DK",
    "fødw": "FødevareWatch DK", "fødeware": "FødevareWatch DK", "føw": "FødevareWatch DK",
    "pol": "PolicyWatch DK", "pow": "PolicyWatch DK", "policy": "PolicyWatch DK",
    "polw": "PolicyWatch DK", "pw": "PolicyWatch DK",
    "kforum": "Kforum DK", "k-forum": "Kforum DK", "kf": "Kforum DK", "kfurum": "Kforum DK",
    "finans.dk": "FINANS DK", "finans": "FINANS DK", "finansdk": "FINANS DK",
    "ejdw": "EjendomsWatch DK",
    # Monitor
    "kulturmonitor": "Kulturmonitor", "byrum": "Byrummonitor",
    "klima": "Klimamonitor", "klim": "Klimamonitor", "klimw": "Klimamonitor",
    "klimamoniotor": "Klimamonitor",
    "natur": "Naturmonitor", "natm": "Naturmonitor", "nm": "Naturmonitor",
    "social": "Socialmonitor", "socialmonitor": "Socialmonitor",
    "sundhed": "Sundhedsmonitor", "sundheds": "Sundhedsmonitor",
    "skole": "Skolemonitor", "uddannelse": "Uddannelsesmonitor",
    # "Magasin/karriere"-varianter — antaget = samme site
    "fwm": "FinansWatch DK", "fwk": "FinansWatch DK",
    "itwm": "ITWatch DK", "itwk": "ITWatch DK",
    "ejwm": "EjendomsWatch DK", "wmk": "Watch Medier DK",
    "wa": "Watch Anbefaler",
    # USIKRE — udfyld selv (tom):
    "hh": "", "jusm": "", "ma": "", "wj": "", "dew": "",
    "km": "", "itm": "", "sm": "", "fk": "", "kapitalwatch ": "",
    "banner": "", "profilbanner": "",
}


def tokens(site: str):
    parts = re.split(r"[+/,]| og | & ", site or "")
    return [p.strip() for p in parts if p.strip()]


def load_rows():
    conn = q.get_conn(); cur = conn.cursor(as_dict=True)
    cur.execute("SELECT site, COUNT(*) AS n FROM KlippekortForbrug WHERE created_by=%s GROUP BY site",
                (IMPORT_TAG,))
    rows = cur.fetchall(); conn.close()
    return rows


def export():
    rows = load_rows()
    counts = {}
    for r in rows:
        for t in tokens(r["site"]):
            counts[t] = counts.get(t, 0) + r["n"]
    out = []
    for t in sorted(counts, key=lambda x: -counts[x]):
        guess = BEST.get(t.lower().strip(), "")
        out.append({"kode": t, "antal": counts[t], "site": guess,
                    "sikker": "ja" if guess else "NEJ-UDFYLD"})
    with open(CSV, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=["kode", "antal", "site", "sikker"], delimiter=";")
        w.writeheader(); w.writerows(out)
    n_ok = sum(1 for r in out if r["site"])
    print(f"Skrev {len(out)} koder til {CSV}")
    print(f"  udfyldt (bedste bud): {n_ok}, mangler udfyldning: {len(out)-n_ok}")
    mangler = [r["kode"] for r in out if not r["site"]]
    if mangler:
        print(f"  UDFYLD 'site' for: {', '.join(mangler)}")


def _read_map():
    if not os.path.exists(CSV):
        print(f"Mangler {CSV} — kør 'export' først."); sys.exit(1)
    m = {}
    with open(CSV, encoding="utf-8-sig") as f:
        for r in csv.DictReader(f, delimiter=";"):
            site = (r.get("site") or "").strip()
            if site:
                m[r["kode"].strip().lower()] = site
    return m


def apply(commit=False):
    code_map = _read_map()
    conn = q.get_conn(); cur = conn.cursor(as_dict=True)
    cur.execute("""SELECT id, job_id, pd_deal_id, org_id, org_name, site, stilling,
                          CONVERT(NVARCHAR(10),tidspunkt,23) AS tidspunkt,
                          CONVERT(NVARCHAR(10),slutdato,23) AS slutdato,
                          effektgaranti, klip, pris_pr_klip, clip_card_size, created_by
                   FROM KlippekortForbrug WHERE created_by=%s""", (IMPORT_TAG,))
    rows = cur.fetchall()
    cur2 = conn.cursor()
    n_changed = n_split = n_unmapped = 0
    for r in rows:
        toks = tokens(r["site"])
        mapped = []
        for t in toks:
            clean = code_map.get(t.lower())
            if clean:
                if clean not in mapped:
                    mapped.append(clean)
            else:
                n_unmapped += 1
                if t not in mapped:
                    mapped.append(t)  # ukendt kode bevares som-er
        # tom/uparsebar site — lad rækken stå helt urørt (mister ikke klip)
        if not mapped:
            continue
        # ingen ændring hvis præcis samme enkelt-site
        if len(mapped) == 1 and mapped[0] == r["site"]:
            continue
        n_changed += 1
        if len(mapped) > 1:
            n_split += 1
        if commit:
            total_klip = float(r["klip"] or 0)
            per = round(total_klip / len(mapped), 4) if mapped else total_klip
            jid = r["job_id"] or uuid.uuid4().hex
            cur2.execute("DELETE FROM KlippekortForbrug WHERE id=%s", (r["id"],))
            for s in mapped:
                cur2.execute("""
                    INSERT INTO KlippekortForbrug
                        (job_id, pd_deal_id, org_id, org_name, site, stilling, tidspunkt,
                         slutdato, effektgaranti, klip, pris_pr_klip, clip_card_size, created_by)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (jid, r["pd_deal_id"], r["org_id"], r["org_name"], s, r["stilling"],
                      r["tidspunkt"], r["slutdato"], r["effektgaranti"], per,
                      r["pris_pr_klip"], r["clip_card_size"], IMPORT_TAG))
    if commit:
        conn.commit()
    conn.close()
    mode = "COMMIT" if commit else "DRY-RUN"
    print(f"[{mode}] rækker der ændres: {n_changed} (heraf split i flere sites: {n_split}); "
          f"ukendte kode-forekomster bevaret: {n_unmapped}")
    if not commit:
        print("Kør med '--commit' for at udføre.")


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "export"
    if mode == "export":
        export()
    elif mode == "apply":
        apply(commit="--commit" in sys.argv)
    else:
        print("Brug: python klippekort_koder.py [export|apply] [--commit]")

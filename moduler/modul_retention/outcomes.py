"""Skrivesiden: registrerede samtaler og udfald (PRD §5.1, §5.2, §6).

Læsesiden (risiko.py) siger hvem der BØR ringes til. Den her siger hvad der
FAKTISK skete. Uden den kan §9's forudsigelsesrate aldrig beregnes, og så
forbliver alle syv zonevægte skøn.

TO TABELLER, ÉN TRANSAKTION: en samtale uden udfald er en tom række, og et
udfald uden samtale er umuligt (fremmednøglen er NOT NULL). Derfor skriver
registrer_samtale() begge dele under ét — går udfaldet galt, forsvinder
samtalen med det.

INTET OPDATERES. PRD §10 pkt. 5: der indsættes altid en ny række. Ringer man
igen om samme abonnement, er det et nyt udfald, ikke en rettelse af det gamle.
Det er derfor "seneste udfald" er et opslag og ikke bare en kolonne.
"""
import datetime as dt
import logging

from db import get_conn

logger = logging.getLogger(__name__)

# Samme sentinel som churn-beregningen i queries.py. dbo.retention.sites er
# NULL for marketwires 35 rækker, og en nøgle med NULL i kan aldrig slås op
# igen — NULL = NULL er ukendt, ikke sandt.
INTET_SITE = "(intet site)"

# Udfald der holder sagen åben. PRD §6.2: de kræver followup_date, hvilket
# databasen håndhæver i CK_RetOut_followup_paa_aabne.
AABNE_UDFALD = ("forskudt", "tilbud_sendt")

# Hvor `arr_before_dkk` kom fra. ARR pr. abonnement er kundens ARR divideret
# med antal sites (queries.py: "lige deling er et VALG, ikke en måling"), fordi
# ACV's og retentions site-vokabularer ikke kan brolægges endnu. Registreres et
# udfald på det tal, arver §9's "kroner reddet" divisionen — og et gæt kan
# ikke skelnes fra et målt beløb i en decimal-kolonne bagefter.
#
# Specialisten har den rigtige pris foran sig under opkaldet. Formularen
# forudfylder med delingen (`lige_deling`) og skifter til `bekraeftet`, så snart
# feltet redigeres. Databasen håndhæver værdierne i CK_RetOut_arr_kilde.
ARR_KILDE_DELING = "lige_deling"
ARR_KILDE_BEKRAEFTET = "bekraeftet"

# db.py forbinder med tds_version="7.0", og TDS 7.0 kender ikke `date` og
# `datetime2` — de kom i 7.3. SQL Server sender dem derfor som STRENGE
# ('2026-08-14' og '2026-08-14 15:04:05.1234567'), mens det gamle `datetime`
# kommer tilbage som et rigtigt Python-objekt. Uden normalisering her sprang
# db_opfoelgninger på `str <= date`, og §7.3 kan ikke markere en overskredet
# opfølgning uden at kunne regne på datoen. Rettes ved kanten, én gang, i
# stedet for i hver enkelt kalder.
_DATO_FELTER = ("renewal_date", "expiry_date", "followup_date")
_TIDSPUNKT_FELTER = ("contacted_at", "created_at")


def _som_dato(vaerdi):
    """'2026-08-14' → date(2026, 8, 14). None og date'er slipper uændret igennem."""
    if isinstance(vaerdi, str):
        return dt.date.fromisoformat(vaerdi[:10])
    if isinstance(vaerdi, dt.datetime):
        return vaerdi.date()
    return vaerdi


def _som_tidspunkt(vaerdi):
    """datetime2(7) → datetime. Afskæres til 6 cifres brøkdel.

    fromisoformat kan ikke tage syv cifre. Afkortningen er bevidst og ikke
    afrunding: mikrosekunder på et opkaldstidspunkt er alligevel støj, og en
    afrunding kunne skubbe tidspunktet et sekund frem.
    """
    if isinstance(vaerdi, str):
        return dt.datetime.fromisoformat(vaerdi[:26])
    return vaerdi


def _normaliser(raekke: dict) -> dict:
    for felt in _DATO_FELTER:
        if felt in raekke:
            raekke[felt] = _som_dato(raekke[felt])
    for felt in _TIDSPUNKT_FELTER:
        if felt in raekke:
            raekke[felt] = _som_tidspunkt(raekke[felt])
    return raekke


def registrer_samtale(samtale: dict, udfald: list) -> int | None:
    """Skriv én samtale og de udfald den gav. Returnerer conversation_id.

    `samtale` skal have account, org_id, contacted_at, channel, created_by og
    valgfrit summary. Hvert element i `udfald` skal have site og
    contact_result, og derudover de felter PRD §5.1 tillader.

    Returnerer None hvis noget gik galt — og rulles der tilbage, er INTET
    skrevet. En delvist registreret samtale ville være værre end ingen:
    specialisten ville tro udfaldet var gemt.
    """
    conn = None
    try:
        conn = get_conn()
        cur = conn.cursor()

        # OUTPUT INSERTED frem for SCOPE_IDENTITY(): id'et kommer tilbage fra
        # selve indsættelsen, så der ikke er et vindue mellem skriv og opslag,
        # og så er der ikke tvivl om hvilket scope tælleren blev læst i.
        cur.execute(
            """INSERT INTO dbo.RetentionConversations
                   (account, org_id, contacted_at, channel, summary, created_by)
               OUTPUT INSERTED.conversation_id
               VALUES (%s, %s, %s, %s, %s, %s)""",
            (samtale["account"], samtale["org_id"], samtale["contacted_at"],
             samtale["channel"], samtale.get("summary"), samtale["created_by"]),
        )
        conversation_id = cur.fetchone()[0]

        for u in udfald:
            # arr_after_dkk beregnes HER og gemmes som tal. PRD §6.3: kursen
            # fryses, ellers ændrer historiske "kroner reddet" sig hver gang
            # valutaen bevæger sig. Derfor ikke en computed column.
            lokal, kurs = u.get("arr_after_local"), u.get("fx_rate")
            arr_after_dkk = None
            if lokal is not None and kurs is not None:
                arr_after_dkk = round(float(lokal) * float(kurs), 2)

            cur.execute(
                """INSERT INTO dbo.RetentionOutcomes
                       (account, org_id, site, conversation_id,
                        contact_result, outcome,
                        arr_before_dkk, arr_before_kilde,
                        arr_after_local, arr_after_currency,
                        fx_rate, arr_after_dkk,
                        renewal_date, expiry_date, followup_date,
                        note, created_by)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                           %s, %s, %s, %s, %s, %s)""",
                (samtale["account"], samtale["org_id"],
                 u.get("site") or INTET_SITE, conversation_id,
                 u["contact_result"], u.get("outcome"),
                 u.get("arr_before_dkk"), u.get("arr_before_kilde"),
                 lokal, u.get("arr_after_currency"),
                 kurs, arr_after_dkk,
                 u.get("renewal_date"), u.get("expiry_date"),
                 u.get("followup_date"), u.get("note"), samtale["created_by"]),
            )

        conn.commit()
        return conversation_id
    except Exception:
        # Rul eksplicit tilbage. Uden det ville en fejl efter den første
        # indsættelse efterlade en samtale uden udfald, og pymssql lukker ikke
        # nødvendigvis forbindelsen med det samme.
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                logger.exception("rollback fejlede efter registrer_samtale")
        logger.exception("registrer_samtale fejlede")
        return None
    finally:
        if conn is not None:
            conn.close()


def db_seneste_udfald() -> dict:
    """Seneste udfald pr. abonnement: {(account, org_id, site): række}.

    Det er opslaget §7.3 hviler på — et abonnement ryddes af listen af sit
    seneste udfald, ikke af data (PRD §6.4). Hentes ufiltreret og filtreres i
    Python, fordi prioriteringslisten alligevel har alle abonnementer i hånden.

    Dato- og tidsfelter kommer ud som rigtige `date`/`datetime` — se
    `_normaliser`. Kalderen skal ikke parse noget.

    ROW_NUMBER er korrekt HER, i modsætning til `PipeDrive_ACV`-opslagene hvor
    RANK er det rigtige: der kan to rækker have samme `updated_at` og dermed
    være lige gyldige, mens `outcome_id` er unik og altid bryder uafgjort.
    """
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute(
            """WITH rangeret AS (
                   SELECT o.*,
                          ROW_NUMBER() OVER (
                              PARTITION BY o.account, o.org_id, o.site
                              ORDER BY o.created_at DESC, o.outcome_id DESC
                          ) AS rn
                   FROM dbo.RetentionOutcomes o
               )
               SELECT * FROM rangeret WHERE rn = 1;"""
        )
        rows = cur.fetchall()
        conn.close()
        return {(r["account"], r["org_id"], r["site"]): _normaliser(r) for r in rows}
    except Exception:
        logger.exception("db_seneste_udfald fejlede")
        return {}


def db_historik(account: str, org_id: int) -> list:
    """Alle samtaler for én kunde, nyeste først, hver med sine udfald.

    PRD §7.4: "Tidligere udfald og samtaler, nyeste først". Grupperet på
    SAMTALEN og ikke på udfaldet, fordi ét opkald kan have dækket fem
    abonnementer — fem løsrevne rækker ville læses som fem opkald.

    Nøglen er kunden `(account, org_id)` og ikke abonnementet: siden viser hele
    kundens historik, også udfald på sites hun ikke længere har. Et opsagt
    abonnement er netop det, man har brug for at kende før man ringer.

    Returnerer en liste af samtaler med `udfald` som liste. Tom liste hvis der
    intet er — og tom liste ved FEJL, hvilket er en bevidst svaghed: siden må
    ikke gå ned, fordi historikken ikke kan hentes, men en tom historik ser ud
    som "vi har aldrig talt med dem". Derfor logges fejlen.
    """
    try:
        conn = get_conn()
        cur = conn.cursor(as_dict=True)
        cur.execute(
            """SELECT c.conversation_id, c.contacted_at, c.channel, c.summary,
                      c.created_by, c.created_at,
                      o.outcome_id, o.site, o.contact_result, o.outcome,
                      o.arr_before_dkk, o.arr_before_kilde,
                      o.arr_after_dkk, o.arr_after_local,
                      o.arr_after_currency, o.fx_rate,
                      o.renewal_date, o.expiry_date, o.followup_date, o.note
               FROM dbo.RetentionConversations c
               LEFT JOIN dbo.RetentionOutcomes o
                      ON o.conversation_id = c.conversation_id
               WHERE c.account = %s AND c.org_id = %s
               ORDER BY c.contacted_at DESC, c.conversation_id DESC,
                        o.outcome_id ASC;""",
            (account, org_id),
        )
        rows = cur.fetchall()
        conn.close()
    except Exception:
        logger.exception("db_historik fejlede (account=%s, org_id=%s)", account, org_id)
        return []

    # LEFT JOIN: en samtale uden udfald kan ikke opstå gennem
    # registrer_samtale(), men kan gennem en manuel indsættelse. Den skal vises
    # som en samtale uden udfald, ikke skjules.
    samtaler: dict = {}
    for r in rows:
        cid = r["conversation_id"]
        s = samtaler.get(cid)
        if s is None:
            s = {"conversation_id": cid,
                 "contacted_at": _som_tidspunkt(r["contacted_at"]),
                 "channel": r["channel"], "summary": r["summary"],
                 "created_by": r["created_by"],
                 "created_at": _som_tidspunkt(r["created_at"]),
                 "udfald": []}
            samtaler[cid] = s
        if r["outcome_id"] is not None:
            s["udfald"].append(_normaliser({
                k: r[k] for k in
                ("outcome_id", "site", "contact_result", "outcome",
                 "arr_before_dkk", "arr_before_kilde",
                 "arr_after_dkk", "arr_after_local",
                 "arr_after_currency", "fx_rate",
                 "renewal_date", "expiry_date", "followup_date", "note")
            }))
    # dict bevarer indsættelsesrækkefølgen, og queryen er allerede sorteret
    # nyeste først — derfor ingen ny sortering her.
    return list(samtaler.values())


def db_opfoelgninger(til_og_med) -> list:
    """Åbne sager med opfølgning senest `til_og_med` (en date).

    PRD §4: listens længde er 10 kunder MINUS dagens opfølgninger, så det her
    tal styrer hvor mange nye navne specialisten får. `<=` og ikke `=`, fordi
    en opfølgning der blev overset i går ikke må forsvinde i morgen.

    Kun det SENESTE udfald pr. abonnement tæller. Et abonnement der først blev
    'tilbud_sendt' og siden 'fornyet' har stadig den gamle followup_date
    liggende på den gamle række, og den skal ikke kalde nogen til handling.
    """
    seneste = db_seneste_udfald()
    return [r for r in seneste.values()
            if r["outcome"] in AABNE_UDFALD
            and r["followup_date"] is not None
            and r["followup_date"] <= til_og_med]

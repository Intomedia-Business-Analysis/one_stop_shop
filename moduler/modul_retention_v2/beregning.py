"""Retention v2: hvem Christine skal ringe til, regnet ud fra CSV-udtrækkene.

Porteret fra BA_scripts/retention_fase1 (kobling.py, forbrug.py, percentil.py),
hvor reglerne er forklaret og afstemt. Tallene her skal give de samme som dér.

X, "på vej ned": kundens artikelvisninger i de seneste 6 hele måneder mod de
samme 6 måneder året før, for 1-medieaftaler med mindst 30 visninger året før.
Y, "ikke kommet i gang": nystartede kunder, målt på gennemsnitlige
artikelvisninger pr. måned i de første 2 hele måneder efter startmåneden.
En kunde er brand + Pipedrive-ID.

Filerne (20_konti, 21_abonnementer, 22_forbrug, 23_proever) ligger i en mappe
på appserveren. Hvert udtræk er en ny fil med datostempel (22_forbrug_13102026.csv),
aldrig overskrivning, og den nyeste pr. præfiks bruges. Vinduet ruller med
datostemplet på forbrugsfilen: seneste hele måned er måneden før.
"""

import os
from functools import lru_cache
from pathlib import Path

import pandas as pd

BRANDS = ["Watch", "Finans"]
# Under 30 visninger året før fylder små tal (4 til 0 = -100 %) listen.
MINIMUM = 30
# Zuora skriver teksten "none" og ikke NULL.
MANGLER_PIPEDRIVE = {"none", ""}


def _datamappe() -> Path:
    # Samme princip som USAGE_SNAPSHOT_DIR i v1: standardmappen på
    # Operations-drevet, og RETENTION_V2_DATA_DIR i .env overstyrer.
    standard = Path.home() / "intomedia" / "Operations - Dokumenter" / "Business Analysis" / "Retention_v2"
    return Path(os.getenv("RETENTION_V2_DATA_DIR") or standard)


def nyeste(praefiks: str) -> Path:
    filer = list(_datamappe().glob(f"{praefiks}_*.csv"))
    if not filer:
        raise FileNotFoundError(f"Ingen {praefiks}_*.csv i {_datamappe()}")
    # Datostemplet står sidst i navnet som DDMMYYYY.
    return max(filer, key=lambda f: pd.to_datetime(f.stem[-8:], format="%d%m%Y"))


def _laes(sti: Path) -> pd.DataFrame:
    # Som tekst, med tomme celler som "" og aldrig NaN.
    return pd.read_csv(sti, dtype=str, keep_default_na=False)


def data():
    """(x, y, udtræksdato) regnet på de nyeste filer.

    Cachet på filnavnene, så CSV'erne kun læses igen, når der kommer et nyt udtræk.
    """
    return _beregn(*(nyeste(p) for p in ("20_konti", "21_abonnementer", "22_forbrug", "23_proever")))


@lru_cache(maxsize=1)
def _beregn(konti_fil, abonnementer_fil, forbrug_fil, proever_fil):
    konti, abonnementer, proever = _laes(konti_fil), _laes(abonnementer_fil), _laes(proever_fil)
    # Unikke nøgler: ellers dobles kunder i stilhed.
    assert konti["account_number"].is_unique, "account_number er ikke unik i 20_konti"
    assert abonnementer["dim_subscription_id"].is_unique, "dim_subscription_id er ikke unik i 21_abonnementer"
    assert proever["dim_subscription_id"].is_unique, "dim_subscription_id er ikke unik i 23_proever"

    udtraek = pd.to_datetime(forbrug_fil.stem[-8:], format="%d%m%Y")
    # Seneste hele måned. Udtrækkets egen måned er ikke slut og bruges aldrig.
    m = udtraek.to_period("M") - 1

    # --- 1-medieaftaler: ét K-nummer, ét aktivt abonnement, og det er betalt ---
    aktive_abb = abonnementer[abonnementer["subscription_status"] == "Active"]
    aktive_konti = konti[konti["account_number"].isin(aktive_abb["account_number"])
                         & ~konti["pipedrive_id"].isin(MANGLER_PIPEDRIVE)]
    k_numre = aktive_konti.groupby(["brand", "pipedrive_id"])["account_number"].transform("nunique")
    et_k_nummer = aktive_konti[k_numre == 1]
    antal_aktive = aktive_abb.groupby("account_number")["dim_subscription_id"].nunique()
    et_abb = et_k_nummer[et_k_nummer["account_number"].map(antal_aktive) == 1]
    # Kun kontiene i et_abb, så hver konto står én gang og map er sikker.
    aktivt_id = aktive_abb[aktive_abb["account_number"].isin(et_abb["account_number"])] \
        .set_index("account_number")["dim_subscription_id"]
    aktivt_id = et_abb["account_number"].map(aktivt_id)
    # Betalt = har betalt og ingen prøve. Prøver, ukendte og abonnementer, der
    # mangler i 23, sorteres fra.
    betalt_ids = set(proever.loc[(proever["har_betalt"] == "1") & (proever["har_proeve"] != "1"), "dim_subscription_id"])
    proeve_ids = set(proever.loc[(proever["har_proeve"] == "1") & (proever["har_betalt"] != "1"), "dim_subscription_id"])
    medie = et_abb[aktivt_id.isin(betalt_ids)]
    assert not medie.duplicated(["brand", "pipedrive_id"]).any(), "en kunde står to gange i 1-medieaftalerne"

    # --- Forbrug: kun Watch og Finans, kun rækker koblet til en kunde ---
    forbrug = _laes(forbrug_fil)
    forbrug = forbrug[forbrug["brand"].isin(BRANDS) & (forbrug["pipedrive_id"] != "")].copy()
    forbrug["artikelvisninger"] = pd.to_numeric(forbrug["artikelvisninger"])

    # --- X ---
    nu = [str(m - i) for i in range(5, -1, -1)]
    aaret_foer = [str(m - 12 - i) for i in range(5, -1, -1)]

    def sum_for(maaneder):
        return forbrug[forbrug["maaned"].isin(maaneder)].groupby(["brand", "pipedrive_id"])["artikelvisninger"].sum()

    x = pd.DataFrame({"aaret_foer": sum_for(aaret_foer), "aaret": sum_for(nu)}).fillna(0).astype(int).reset_index()
    x = x.merge(medie[["brand", "pipedrive_id"]], on=["brand", "pipedrive_id"])
    x = x[x["aaret_foer"] >= MINIMUM].copy()
    x["aendring"] = x["aaret"] / x["aaret_foer"] - 1

    # --- Y: nystartede, startet de seneste 12 måneder ---
    aktive_start = pd.to_datetime(aktive_abb["subscription_startdate"], errors="coerce")
    start = medie["account_number"].map(aktive_start.groupby(aktive_abb["account_number"]).min())
    i_vinduet = start.dt.to_period("M").between(m - 11, m)
    nye = medie[i_vinduet].assign(start=start[i_vinduet])
    # Tidligere abonnementer hos samme kunde, også på inaktive K-numre.
    alle = abonnementer.merge(konti[["account_number", "brand", "pipedrive_id"]], on="account_number")
    alle = alle.assign(start_tidl=pd.to_datetime(alle["subscription_startdate"], errors="coerce"),
                       slut_tidl=pd.to_datetime(alle["subscription_enddate"], errors="coerce"))
    par = nye[["brand", "pipedrive_id", "start"]].merge(
        alle[["brand", "pipedrive_id", "start_tidl", "slut_tidl", "dim_subscription_id"]],
        on=["brand", "pipedrive_id"])
    tidligere = par[par["start_tidl"] < par["start"]]
    # Det seneste tidligere abonnement er det, kunden kom fra. Uden slutdato
    # først, så den aldrig vælges som den seneste.
    seneste = (tidligere.sort_values(["slut_tidl", "start_tidl", "dim_subscription_id"], na_position="first")
               .drop_duplicates(["brand", "pipedrive_id"], keep="last"))
    # Skift fra et betalt abonnement tæller ikke som nystartet. Ny kunde eller
    # kunde fra en prøve gør.
    kom_fra_betalt = seneste.loc[~seneste["dim_subscription_id"].isin(proeve_ids), ["brand", "pipedrive_id"]]
    y = nye.merge(kom_fra_betalt, on=["brand", "pipedrive_id"], how="left", indicator=True)
    y = y[y["_merge"] == "left_only"].copy()

    # Forbrug de første 2 hele måneder efter startmåneden. Kun kunder, der har
    # haft begge måneder, kan måles. En måned uden række i 22 har forbrug 0.
    y["startmaaned"] = y["start"].dt.to_period("M")
    y = y[y["startmaaned"] + 2 <= m].copy()
    pr_maaned = forbrug.groupby(["brand", "pipedrive_id", "maaned"])["artikelvisninger"].sum()
    summer = 0
    for nr in (1, 2):
        noegle = pd.MultiIndex.from_arrays([y["brand"], y["pipedrive_id"], (y["startmaaned"] + nr).astype(str)])
        summer = summer + pr_maaned.reindex(noegle).fillna(0).to_numpy()
    y["pr_maaned"] = summer / 2

    return (x[["brand", "pipedrive_id", "aaret_foer", "aaret", "aendring"]],
            y[["brand", "pipedrive_id", "start", "pr_maaned"]],
            udtraek.strftime("%d-%m-%Y"))


def x_liste(x: pd.DataFrame, percentil: float) -> pd.DataFrame:
    """De `percentil` % mest faldende kunder i hvert brand, største fald øverst.

    Percentilen rangerer inden for brandet. Kunder, der er gået i nul (-100 %),
    står øverst, og blandt dem kommer den, der mistede flest visninger, først.
    """
    graense = x.groupby("brand")["aendring"].transform(lambda s: s.quantile(percentil / 100))
    return x[x["aendring"] <= graense].sort_values(["aendring", "aaret_foer"], ascending=[True, False])


def y_liste(y: pd.DataFrame, graense: float) -> pd.DataFrame:
    """Nystartede med højst `graense` visninger pr. måned, ældste start først."""
    return y[y["pr_maaned"] <= graense].sort_values("start")

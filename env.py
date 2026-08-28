"""
Indlæsning af .env — ét sted, så app.py, db.py og alle moduler er enige om hvor
hemmelighederne kommer fra.

Samme mønster som pipedrive_sync/env.py, blot med sin egen sti-variabel:

  1. HUB_ENV     — eksplicit sti til en .env-fil (anbefalet i produktion,
                      f.eks. %PROGRAMDATA%/IntomediaHub/.env)
  2. .env ved siden  — den historiske placering i projektmappen
     af scriptet

Det afgørende for en tjeneste: den gamle kode kaldte load_dotenv() uden
argumenter, og den leder efter .env i den AKTUELLE ARBEJDSMAPPE og opefter.
Startes appen af Task Scheduler eller NSSM med en anden arbejdsmappe, blev
.env derfor ikke fundet, og appen faldt tilbage til tomme værdier. Stien her
er absolut og bundet til filens placering, så det ikke kan ske.

Findes ingen af dem, er det ikke en fejl: værdierne kan lige så godt komme
fra rigtige miljøvariabler (f.eks. sat på den konto Task Scheduler kører som).
Kun hvis en påkrævet variabel så mangler, fejler koden — og det sker
højlydt i db.check_connection().

Bemærk: værdier der allerede står i miljøet vinder over .env-filen
(load_dotenv overskriver ikke som standard), så en Task Scheduler-opgave kan
overstyre enkelte variabler uden at redigere filen.
"""
import os
from pathlib import Path

from dotenv import load_dotenv

_PROJECT_ENV = Path(__file__).resolve().parent / ".env"

_loaded = False


def load_env() -> Path | None:
    """
    Indlæs .env én gang pr. proces. Returnerer stien der blev brugt, eller
    None hvis ingen fil blev fundet (så kommer værdierne fra miljøet).
    """
    global _loaded
    if _loaded:
        return _env_path()
    _loaded = True

    path = _env_path()
    if path is not None:
        load_dotenv(path)
    return path


def _env_path() -> Path | None:
    override = os.getenv("HUB_ENV")
    if override:
        p = Path(os.path.expandvars(override)).expanduser()
        if p.is_dir():
            p = p / ".env"
        if p.is_file():
            return p
        # En eksplicit sti der ikke findes er næsten altid en tastefejl —
        # sig det, i stedet for stille at falde tilbage til projektmappen.
        raise FileNotFoundError(
            f"HUB_ENV peger på '{p}', som ikke findes. "
            f"Ret stien, eller fjern HUB_ENV for at bruge {_PROJECT_ENV}."
        )
    return _PROJECT_ENV if _PROJECT_ENV.is_file() else None


def force_utf8_output() -> None:
    """
    Gør sys.stdout/sys.stderr i stand til at skrive ✓ / ⚠ / æøå uden at rejse
    UnicodeEncodeError. Samme mekanisme som pipedrive_sync: et rigtigt
    Windows-konsolvindue kan UTF-8, men bliver output omdirigeret
    (`python main.py > log.txt`, eller Task Scheduler) falder Python tilbage
    til cp1252 — og så ville netop advarslerne, dem man skal se når noget er
    galt, fælde kørslen i stedet for at blive vist.

    Kaldes fra scripternes main(), ikke ved import, så et importerende script
    selv bestemmer over sine streams.
    """
    import sys

    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except (AttributeError, ValueError, OSError):
            pass      # ingen tekst-stream (pythonw: None) — der er intet at rette

# Dokumentationsstandard for Python-scripts og -applikationer

Formålet er, at man kan åbne et vilkårligt af vores projekter og finde de samme
oplysninger de samme steder. En ny kollega — eller en vikar en fredag eftermiddag —
skal kunne svare på "hvad er det, hvor kører det, hvordan starter jeg det, og hvad
går galt" uden at læse kode.

Hvert projekt har én `README.md` i roden med sektionerne herunder, **i denne
rækkefølge**. Er en sektion ikke relevant (et lille script har ingen roller eller
drift), udelades den — men de sektioner der er med, hedder det samme og står
samme sted som i alle andre projekter.

## Sektionerne

| # | Sektion | Skal svare på | Udelad hvis |
|---|---|---|---|
| 0 | **Titel + 3-linjers resumé** | Hvad er det, hvem er det til, hvad står det oven på | aldrig |
| 1 | **Adgang** | Hvor kører det (URL/sti), hvem har adgang, hvordan får man den | scriptet kører kun manuelt |
| 2 | **Funktioner** | Hvad kan det — én tabelrække pr. funktion/modul med adgangskrav | aldrig |
| 3 | **Kør det lokalt** | Fra tomt checkout til kørende app, kopiér-og-indsæt | aldrig |
| 4 | **Konfiguration** | Hver miljøvariabel: hvad den gør, hvad der sker hvis den mangler | ingen konfiguration |
| 5 | **Tests** | Kommandoen, hvad der dækkes, hvad der kræver database | ingen tests (så skriv det) |
| 6 | **Sådan hænger koden sammen** | Filkort, forespørgslens vej gennem systemet, hvordan man tilføjer noget nyt | aldrig |
| 7 | **Drift** | Hvordan det startes/genstartes på serveren, logs, tjenestekonto | kører kun lokalt |
| 8 | **Fejlfinding** | Symptom → årsag → handling, i tabelform | aldrig |
| 9 | **Det man skal vide** | Fælder der ikke er til at se i koden, og *hvorfor* de er som de er | aldrig |
| 10 | **Ejerskab** | Hvem vedligeholder, hvor ligger kilderne, hvad er status | aldrig |

## Regler der gør dokumentationen brugbar

**Skriv symptomet, ikke årsagen, i venstre kolonne.** Ingen slår op under
"manglende miljøvariabel". Man slår op under "alle dashboards er tomme".

**Enhver kommando skal kunne kopieres direkte.** Fuld sti til fortolkeren
(`.venv\Scripts\python.exe`, ikke `python`), ingen pladsholdere man skal gætte.

**Forklar hvorfor, når en løsning ser mærkelig ud.** Et hack uden begrundelse
bliver "ryddet op" af den næste, og fejlen kommer igen. Står grunden der,
bliver den stående.

**Én kilde til sandheden.** Miljøvariabler dokumenteres i `.env.example` med
kommentarer; README henviser og gentager ikke listen. Ellers driver de fra
hinanden.

**Angiv hvad der er midlertidigt.** En bro eller et kompatibilitetsflag skal
have en note om, hvordan man kommer af med det — ellers står det for evigt.

**Skriv sektion 9 først.** Det er den, der faktisk sparer nogen for en dag.

## Ny fil, kort version

```
# <Projektnavn>
<Hvad, til hvem, oven på hvad — 3 linjer.>

## 1. Adgang
## 2. Funktioner
## 3. Kør det lokalt
## 4. Konfiguration
## 5. Tests
## 6. Sådan hænger koden sammen
## 7. Drift
## 8. Fejlfinding
## 9. Det man skal vide
## 10. Ejerskab
```

`one_stop_shop/README.md` er referenceudgaven — kopiér strukturen derfra.

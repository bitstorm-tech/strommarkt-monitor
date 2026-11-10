//! Import des MaStR-Gesamtdatenexports (ZIP mit XML-Dateien) nach DuckDB.
use crate::Res;
use encoding_rs_io::DecodeReaderBytes;
use duckdb::{appender_params_from_iter, Appender, Config, Connection};
use serde::{de::{IgnoredAny, MapAccess, Visitor}, Deserializer};
use std::{collections::HashMap, fs::File, io::{BufReader, Read}, sync::LazyLock};

/// Spalten der Tabelle `einheiten` nach Typ: alle Felder, die in den Einheiten-Dateien vorkommen.
/// Die Namen sind die Tag-Namen im MaStR-XML. Felder, die hier fehlen, werden beim Import verworfen.
const TEXT: &[&str] = &[
    "Adresszusatz", "AltAnlagenbetreiberMastrNummer", "AnlagenbetreiberMastrNummer", "EegAnlagentyp",
    "EegMaStRNummer", "EinheitMastrNummer", "Einsatzverantwortlicher", "FlurFlurstuecknummern", "Gemarkung",
    "Gemeinde", "Gemeindeschluessel", "GemeinsamRegistrierteSolareinheitMastrNummer", "GenMastrNummer",
    "Hausnummer", "Kraftwerksnummer", "KwkMaStRNummer", "Landkreis", "LokationMaStRNummer",
    "MastrNummernKombibetrieb", "NameDesSolarparks", "NameKraftwerk", "NameKraftwerksblock",
    "NameStromerzeugungseinheit", "NameWindpark", "Ort", "Postleitzahl", "SpeMastrNummer", "Strasse",
    "Typenbezeichnung", "Weic", "WeicDisplayName",
];
const DATUM: &[&str] = &[
    "DatumBaubeginn", "DatumBeginnVoruebergehendeStilllegung", "DatumDesBetreiberwechsels",
    "DatumEndgueltigeStilllegung", "DatumKapazitaetsreserve", "DatumLetzteAktualisierung", "DatumNetzreserve",
    "DatumRegistrierungDesBetreiberwechsels", "DatumWiederaufnahmeBetrieb", "GeplantesInbetriebnahmedatum",
    "Inbetriebnahmedatum", "InbetriebnahmedatumAmAktuellenStandort", "NetzbetreiberpruefungDatum",
    "Registrierungsdatum", "SicherheitsbereitschaftAbDatum",
];
/// Zahlen und Ja/Nein-Felder (0/1).
const ZAHL: &[&str] = &[
    "AnlageIstImKombibetrieb", "AnschlussAnHoechstOderHochSpannung", "AnzahlModule",
    "AuflageAbschaltungLeistungsbegrenzung", "AuflagenAbschaltungEiswurf",
    "AuflagenAbschaltungSchallimmissionsschutzNachts", "AuflagenAbschaltungSchallimmissionsschutzTagsueber",
    "AuflagenAbschaltungSchattenwurf", "AuflagenAbschaltungSonstige", "AuflagenAbschaltungTierschutz",
    "AusschliesslicheVerwendungImKombibetrieb", "BestandteilGrenzkraftwerk", "Breitengrad", "Bruttoleistung",
    "Buergerenergie", "EinheitlicheAusrichtungUndNeigungswinkel", "FernsteuerbarkeitDv", "FernsteuerbarkeitNb",
    "GroesseDerInAnspruchGenommenenFlaeche", "GroesseDerInAnspruchGenommenenFlaecheInHektar",
    "HausnummerNichtGefunden", "Hausnummer_nv", "KapazitaetsreserveZugeordnet", "Kraftwerksnummer_nv",
    "Kuestenentfernung", "Laengengrad", "LichteHoehe", "MinderungStromerzeugung", "Nabenhoehe",
    "Nachtkennzeichnung", "Nettonennleistung", "NettonennleistungDeutschland", "NetzreserveZugeordnet",
    "NichtVorhandenInMigriertenEinheiten", "Notstromaggregat", "PumpbetriebKontinuierlichRegelbar",
    "PumpbetriebLeistungsaufnahme", "Rotorblattenteisungssystem", "Rotordurchmesser", "SpeicherAmGleichenOrt",
    "SteigerungNettonennleistungKombibetrieb", "StrasseNichtGefunden", "Wassertiefe", "Weic_nv",
    "ZugeordnenteWirkleistungWechselrichter", "ZugeordneteWirkleistungWechselrichter",
];
/// Im XML als Katalog-Id kodiert (teils mehrere, durch Leerzeichen getrennt); wird beim Import in Klartext
/// übersetzt. Welche Felder das sind, steht nicht im Export: ein neues Katalog-Feld muss hier ergänzt werden.
const KATALOG: &[&str] = &[
    "AcDcKoppelung", "ArtDerSolaranlage", "ArtDerWasserkraftanlage", "ArtDesZuflusses", "Batterietechnologie",
    "Biomasseart", "Bundesland", "EinheitBetriebsstatus", "EinheitSystemstatus", "Einsatzort",
    "Einspeisungsart", "Energietraeger", "GebietNachDemFlaechenentwicklungsplanNordsee",
    "GebietNachDemFlaechenentwicklungsplanOstsee", "Hauptausrichtung", "HauptausrichtungNeigungswinkel",
    "Hauptbrennstoff", "Hersteller", "Land", "Leistungsbegrenzung", "Nebenausrichtung",
    "NebenausrichtungNeigungswinkel", "NetzbetreiberpruefungStatus", "Nutzungsbereich",
    "Pumpspeichertechnologie", "Seelage", "SolaranlagenKategorie", "Technologie",
    "UeberwiegendeNutzungsartDerFlaecheVorErrichtung", "VorherigerNutzungsartenbereichDerFlaeche",
    "VorlaeufigeRegelzonenDerEinheit", "WeitereBrennstoffe", "WeitererHauptbrennstoff", "WindAnLandOderAufSee",
    "ZusaetzlicheMerkmaleDerFlaecheUndDerAktuellenFlaechennutzung",
];
/// Spalten mit Auswahl-Filter.
const AUSWAHL: &[&str] = &["Energietraeger", "EinheitBetriebsstatus", "Bundesland"];

/// Eine Gruppe von Dateien im Export und die Felder, die daraus in die Rohtabelle übernommen werden (nach Typ).
pub struct Quelle {
    pub tabelle: &'static str,
    dateien: &'static [&'static str],
    text: &'static [&'static str],
    datum: &'static [&'static str],
    zahl: &'static [&'static str],
    katalog: &'static [&'static str],
}

impl Quelle {
    /// Alle Spalten; die Katalog-Spalten stehen am Ende (darauf verlässt sich `insert`).
    fn spalten(&self) -> Vec<&'static str> {
        [self.text, self.datum, self.zahl, self.katalog].concat()
    }
}

/// Alle Stromerzeugungseinheiten (ohne Verbraucher- und Gas-Einheiten).
pub const EINHEITEN: Quelle = Quelle {
    tabelle: "einheiten_roh",
    dateien: &[
        "EinheitenSolar", "EinheitenWind", "EinheitenBiomasse", "EinheitenWasser", "EinheitenVerbrennung",
        "EinheitenKernkraft", "EinheitenGeothermieGrubengasDruckentspannung", "EinheitenStromSpeicher",
    ],
    text: TEXT,
    datum: DATUM,
    zahl: ZAHL,
    katalog: KATALOG,
};
pub const MARKTAKTEURE: Quelle = Quelle {
    tabelle: "marktakteure_roh",
    dateien: &["Marktakteure"],
    text: &[
        "MastrNummer", "Firmenname", "MarktakteurVorname", "MarktakteurNachname", "Strasse", "Hausnummer",
        "Adresszusatz", "Postleitzahl", "Ort", "Region", "Nuts2", "Email", "Telefon", "Fax", "Webseite",
        "WebportalDesNetzbetreibers", "AcerCode", "Umsatzsteueridentifikationsnummer",
        "BundesnetzagenturBetriebsnummer", "Registernummer", "RegistergerichtAusland", "RegisternummerAusland",
        "SonstigeRechtsform", "Netz", "StrasseAnZustelladresse", "HausnummerAnZustelladresse",
        "AdresszusatzAnZustelladresse", "PostleitzahlAnZustelladresse", "OrtAnZustelladresse",
    ],
    datum: &["RegistrierungsdatumMarktakteur", "DatumLetzeAktualisierung", "Taetigkeitsbeginn", "Taetigkeitsende"],
    zahl: &[
        "Kmu", "Direktvermarktungsunternehmen", "Stromgrosshaendler", "BelieferungVonLetztverbrauchernStrom",
        "BelieferungHaushaltskundenStrom", "Gasgrosshaendler", "BelieferungVonLetztverbrauchernGas",
        "BelieferungHaushaltskundenGas",
    ],
    katalog: &[
        "Marktfunktion", "Personenart", "Rechtsform", "Land", "Bundesland", "LandAnZustelladresse", "Registergericht",
        "RegisternummerPraefix", "HauptwirtdschaftszweigAbschnitt", "HauptwirtdschaftszweigAbteilung",
        "HauptwirtdschaftszweigGruppe",
    ],
};
pub const NETZANSCHLUSSPUNKTE: Quelle = Quelle {
    tabelle: "netzanschlusspunkte_roh",
    dateien: &["Netzanschlusspunkte"],
    text: &[
        "NetzanschlusspunktMastrNummer", "NetzanschlusspunktBezeichnung", "LokationMaStRNummer",
        "NetzbetreiberMaStRNummer", "NetzMaStRNummer", "Messlokation",
    ],
    datum: &["LetzteAenderung"],
    zahl: &["Nettoengpassleistung", "Netzanschlusskapazitaet", "MaximaleEinspeiseleistung", "MaximaleAusspeiseleistung", "NochInPlanung"],
    katalog: &["Spannungsebene", "RegelzoneNetzanschlusspunkt", "BilanzierungsgebietNetzanschlusspunktId", "Gasqualitaet"],
};
pub const LOKATIONEN: Quelle = Quelle {
    tabelle: "lokationen",
    dateien: &["Lokationen"],
    text: &["MastrNummer", "NameDerTechnischenLokation", "VerknuepfteEinheitenMaStRNummern"],
    datum: &["DatumLetzteAktualisierung"],
    zahl: &[],
    katalog: &["Lokationtyp"],
};
/// Nur für zusätzliche Spalten der Einheiten und die Marktrollen der Marktakteure.
const VERKNUEPFT: &[Quelle] = &[
    Quelle {
        tabelle: "anlagen_eeg",
        dateien: &[
            "AnlagenEegSolar", "AnlagenEegWind", "AnlagenEegBiomasse", "AnlagenEegWasser",
            "AnlagenEegGeothermieGrubengasDruckentspannung", "AnlagenEegSpeicher",
        ],
        text: &["EegMaStRNummer", "AnlagenschluesselEeg", "Zuschlagsnummer"],
        datum: &["EegInbetriebnahmedatum"],
        zahl: &["InstallierteLeistung"],
        katalog: &[],
    },
    Quelle { tabelle: "anlagen_kwk", dateien: &["AnlagenKwk"], text: &["KwkMastrNummer"], datum: &[], zahl: &["ElektrischeKwkLeistung", "ThermischeNutzleistung"], katalog: &[] },
    Quelle { tabelle: "anlagen_speicher", dateien: &["AnlagenStromSpeicher"], text: &["MaStRNummer"], datum: &[], zahl: &["NutzbareSpeicherkapazitaet"], katalog: &[] },
    Quelle { tabelle: "marktrollen", dateien: &["MarktakteureUndRollen"], text: &["MarktakteurMastrNummer", "Marktrolle"], datum: &[], zahl: &[], katalog: &[] },
];
fn quellen() -> impl Iterator<Item = &'static Quelle> {
    [&EINHEITEN, &MARKTAKTEURE, &NETZANSCHLUSSPUNKTE, &LOKATIONEN].into_iter().chain(VERKNUEPFT)
}

/// Felder, deren Ids nicht auf Katalogwerte.xml verweisen, sondern auf eine eigene Datei: (Feld, Datei, Wert-Feld).
const EIGENE_KATALOGE: &[(&str, &str, &str)] = &[
    ("Marktfunktion", "Marktfunktionen.xml", "Wert"),
    ("Lokationtyp", "Lokationstypen.xml", "Wert"),
    ("BilanzierungsgebietNetzanschlusspunktId", "Bilanzierungsgebiete.xml", "Yeic"),
];
/// Id → Klartext je Feld; "" steht für Katalogwerte.xml.
pub type Kataloge = HashMap<&'static str, HashMap<String, String>>;

/// Spalten der Einheiten über die der Einheiten-Dateien hinaus, beim Import aus den verknüpften Daten berechnet
/// (siehe `finish`).
const VERKNUEPFTE_SPALTEN: &[&str] = &[
    "AnlagenbetreiberName", "NetzbetreiberMastrNummer", "NetzbetreiberName", "Spannungsebene",
    "NutzbareSpeicherkapazitaet", "AnlagenschluesselEeg", "EegInbetriebnahmedatum", "InstallierteLeistung",
    "Zuschlagsnummer", "ElektrischeKwkLeistung", "ThermischeNutzleistung",
];

/// Eine Tabelle im Frontend (ein Tab) und ihre gleichnamige Tabelle in der Datenbank.
pub struct Ansicht {
    pub name: &'static str,
    pub spalten: Vec<&'static str>,
    /// Spalten, die das Suchfeld durchsucht.
    pub suche: &'static [&'static str],
    /// Spalten mit Auswahl-Filter.
    pub auswahl: &'static [&'static str],
}

pub static ANSICHTEN: LazyLock<[Ansicht; 3]> = LazyLock::new(|| {
    [
        Ansicht {
            name: "einheiten",
            spalten: [&EINHEITEN.spalten()[..], VERKNUEPFTE_SPALTEN].concat(),
            suche: &["NameStromerzeugungseinheit", "EinheitMastrNummer"],
            auswahl: AUSWAHL,
        },
        Ansicht {
            name: "netzanschlusspunkte",
            spalten: vec![
                "NetzanschlusspunktMastrNummer", "NetzanschlusspunktBezeichnung", "LokationMaStRNummer",
                "NameDerTechnischenLokation", "Lokationtyp", "VerknuepfteEinheitenMaStRNummern",
                "NetzbetreiberMaStRNummer", "NetzbetreiberName", "NetzMaStRNummer", "Spannungsebene",
                "RegelzoneNetzanschlusspunkt", "BilanzierungsgebietNetzanschlusspunktId", "Messlokation",
                "Nettoengpassleistung", "Netzanschlusskapazitaet", "MaximaleEinspeiseleistung",
                "MaximaleAusspeiseleistung", "Gasqualitaet", "NochInPlanung", "LetzteAenderung",
                "DatumLetzteAktualisierung",
            ],
            suche: &["NetzanschlusspunktMastrNummer", "NetzanschlusspunktBezeichnung", "LokationMaStRNummer", "NameDerTechnischenLokation"],
            auswahl: &["Lokationtyp", "Spannungsebene"],
        },
        Ansicht {
            name: "marktakteure",
            spalten: [MARKTAKTEURE.spalten(), vec!["Marktrollen"]].concat(),
            suche: &["Firmenname", "MastrNummer"],
            auswahl: &["Marktfunktion", "Personenart", "Bundesland"],
        },
    ]
});

pub fn ansicht(name: &str) -> Option<&'static Ansicht> {
    ANSICHTEN.iter().find(|a| a.name == name)
}

type Row = HashMap<String, String>;

/// Reicht die Datensätze einzeln weiter: eine Einheiten-Datei hat 100.000 Datensätze mit bis zu
/// ~75 Feldern, komplett im Speicher wären das mehrere hundert MB.
struct Rows<F>(F);

impl<'de, F: FnMut(Row) -> Res<()>> Visitor<'de> for Rows<F> {
    type Value = ();
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("MaStR-Datei")
    }
    fn visit_map<A: MapAccess<'de>>(mut self, mut map: A) -> Result<(), A::Error> {
        while map.next_key::<IgnoredAny>()?.is_some() {
            (self.0)(map.next_value()?).map_err(serde::de::Error::custom)?;
        }
        Ok(())
    }
}

/// Eine MaStR-XML-Datei: flache Liste von Datensätzen, UTF-16 mit BOM (quick-xml erwartet UTF-8).
pub fn parse(xml: impl Read, each: impl FnMut(Row) -> Res<()>) -> Res<()> {
    let mut de = quick_xml::de::Deserializer::from_reader(BufReader::new(DecodeReaderBytes::new(xml)));
    Ok(de.deserialize_map(Rows(each))?)
}

/// Typ einer Spalte (Datenbank und Anzeige im Frontend); gleichnamige Felder haben in allen Dateien denselben Typ.
pub fn typ(column: &str) -> &'static str {
    if quellen().any(|q| q.zahl.contains(&column)) {
        "num"
    } else if quellen().any(|q| q.datum.contains(&column)) {
        "date"
    } else {
        "text"
    }
}

/// Legt die Rohtabellen an, in die `insert` schreibt.
pub fn create(db: &Connection) -> Res<()> {
    for q in quellen() {
        let cols: Vec<String> = q.spalten().iter().map(|c| format!("{c} {}", if typ(c) == "num" { "DOUBLE" } else { "TEXT" })).collect();
        db.execute_batch(&format!("CREATE TABLE {} ({})", q.tabelle, cols.join(", ")))?;
    }
    Ok(())
}

pub fn insert(app: &mut Appender, q: &Quelle, kataloge: &Kataloge, row: &Row) -> Res<()> {
    let klartext = |c: &str, ids: &String| {
        let katalog = kataloge.get(c).or(kataloge.get(""));
        ids.split(' ').map(|id| katalog.and_then(|k| k.get(id)).map_or(id, |w| w)).collect::<Vec<_>>().join(", ")
    };
    // Werte als Text: DuckDB wandelt sie beim Anhängen in den Spaltentyp (z. B. "9.900" → 9.9).
    app.append_row(appender_params_from_iter(
        q.text.iter().chain(q.datum).chain(q.zahl).map(|c| row.get(*c).cloned())
            .chain(q.katalog.iter().map(|c| row.get(*c).map(|ids| klartext(c, ids)))),
    ))?;
    Ok(())
}

/// Baut aus den Rohtabellen die Tabellen der Ansichten im Katalog `ziel`, verknüpft über die MaStR-Nummern.
/// Bei 150 MB schafft DuckDB nur eine Verknüpfung je Abfrage und keine, an der alle ~130 Einheiten-Spalten hängen:
/// daher schmale Zwischentabellen Schritt für Schritt, an die Einheiten zum Schluss über die Zeilenposition angefügt.
pub fn finish(db: &Connection, ziel: &str) -> Res<()> {
    let spalten = |name| ansicht(name).unwrap().spalten.join(",");
    let schritte = [
        "CREATE TABLE nap_nb AS SELECT n.*, m.Firmenname AS NetzbetreiberName
            FROM netzanschlusspunkte_roh n LEFT JOIN marktakteure_roh m ON m.MastrNummer = n.NetzbetreiberMaStRNummer".to_string(),
        // Netzbetreiber einer Einheit: über die Netzanschlusspunkte ihrer Lokation, bei mehreren mit Komma.
        "CREATE TABLE lok_nb AS SELECT LokationMaStRNummer, string_agg(DISTINCT NetzbetreiberMaStRNummer, ', ') AS NetzbetreiberMastrNummer,
            string_agg(DISTINCT NetzbetreiberName, ', ') AS NetzbetreiberName, string_agg(DISTINCT Spannungsebene, ', ') AS Spannungsebene
            FROM nap_nb GROUP BY 1".to_string(),
        "CREATE TABLE zusatz AS SELECT e.rowid AS zeile, e.EinheitMastrNummer, e.LokationMaStRNummer, e.SpeMastrNummer, e.EegMaStRNummer,
            e.KwkMaStRNummer, a.Firmenname AS AnlagenbetreiberName
            FROM einheiten_roh e LEFT JOIN marktakteure_roh a ON a.MastrNummer = e.AnlagenbetreiberMastrNummer".to_string(),
        "CREATE OR REPLACE TABLE zusatz AS SELECT z.*, nb.* EXCLUDE (LokationMaStRNummer) FROM zusatz z LEFT JOIN lok_nb nb USING (LokationMaStRNummer)".to_string(),
        "CREATE OR REPLACE TABLE zusatz AS SELECT z.*, s.NutzbareSpeicherkapazitaet FROM zusatz z LEFT JOIN anlagen_speicher s ON s.MaStRNummer = z.SpeMastrNummer".to_string(),
        "CREATE OR REPLACE TABLE zusatz AS SELECT z.*, g.* EXCLUDE (EegMaStRNummer) FROM zusatz z LEFT JOIN anlagen_eeg g USING (EegMaStRNummer)".to_string(),
        "CREATE OR REPLACE TABLE zusatz AS SELECT z.*, k.* EXCLUDE (KwkMastrNummer) FROM zusatz z LEFT JOIN anlagen_kwk k ON k.KwkMastrNummer = z.KwkMaStRNummer".to_string(),
        "CREATE OR REPLACE TABLE zusatz AS SELECT * FROM zusatz ORDER BY zeile".to_string(),
        format!("CREATE TABLE {ziel}.einheiten AS SELECT {} FROM (SELECT e.*, z.* EXCLUDE (zeile, EinheitMastrNummer, LokationMaStRNummer,
            SpeMastrNummer, EegMaStRNummer, KwkMaStRNummer) FROM einheiten_roh e POSITIONAL JOIN zusatz z)", spalten("einheiten")),
        // Alle Lokationen und alle Netzanschlusspunkte, auch die ohne Gegenstück.
        format!("CREATE TABLE {ziel}.netzanschlusspunkte AS SELECT {} FROM (
            SELECT n.* EXCLUDE (LokationMaStRNummer), coalesce(l.MastrNummer, n.LokationMaStRNummer) AS LokationMaStRNummer, l.* EXCLUDE (MastrNummer)
            FROM lokationen l FULL JOIN nap_nb n ON n.LokationMaStRNummer = l.MastrNummer)", spalten("netzanschlusspunkte")),
        format!("CREATE TABLE {ziel}.marktakteure AS SELECT {} FROM marktakteure_roh m LEFT JOIN (
            SELECT MarktakteurMastrNummer, string_agg(DISTINCT Marktrolle, ', ') AS Marktrollen FROM marktrollen GROUP BY 1
            ) r ON r.MarktakteurMastrNummer = m.MastrNummer", spalten("marktakteure")),
    ];
    for (i, sql) in schritte.iter().enumerate() {
        if sql.contains("POSITIONAL JOIN") {
            // Zeile für Zeile nebeneinander: gibt es eine Nummer doppelt, wären die Daten ab da verrutscht.
            let verrutscht: i64 = db.query_row(
                "SELECT count(*) FROM einheiten_roh e POSITIONAL JOIN zusatz z WHERE e.EinheitMastrNummer IS DISTINCT FROM z.EinheitMastrNummer", [], |r| r.get(0))?;
            if verrutscht > 0 {
                return Err(format!("{verrutscht} Einheiten passen nicht zu ihren verknüpften Daten").into());
            }
        }
        db.execute_batch(sql)?;
        eprintln!("Verknüpfen: Schritt {}/{} fertig", i + 1, schritte.len());
    }
    // Die Werte für die Auswahl-Filter (einmal hier statt bei jedem Seitenaufruf). Indizes braucht DuckDB nicht:
    // es speichert spaltenweise und durchsucht eine Spalte mit 10 Mio. Werten in Millisekunden.
    let werte: Vec<String> = ANSICHTEN
        .iter()
        .flat_map(|a| a.auswahl.iter().map(move |c| format!("SELECT DISTINCT '{n}' AS ansicht, '{c}' AS spalte, {c} AS wert FROM {ziel}.{n} WHERE {c} IS NOT NULL", n = a.name)))
        .collect();
    Ok(db.execute_batch(&format!("CREATE TABLE {ziel}.filterwerte AS {}", werte.join(" UNION ALL ")))?)
}

pub fn import(zip_path: &str, db_path: &str) -> Res<()> {
    let mut zip = zip::ZipArchive::new(File::open(zip_path)?)?;
    let mut kataloge = Kataloge::new();
    for &(feld, datei, wert) in [("", "Katalogwerte.xml", "Wert")].iter().chain(EIGENE_KATALOGE) {
        let katalog = kataloge.entry(feld).or_default();
        parse(zip.by_name(datei)?, |mut r| {
            katalog.extend(r.remove("Id").zip(r.remove(wert)));
            Ok(())
        })?;
    }

    // Rohdaten in eine Hilfsdatenbank, die fertigen Tabellen in eine temporäre Datei: so enthält die Datenbank
    // keinen Platz gelöschter Rohtabellen, und ein abgebrochener Import lässt die bisherige DB unberührt.
    let (roh, tmp) = (format!("{db_path}.roh"), format!("{db_path}.tmp"));
    for f in [&roh, &tmp] {
        let _ = std::fs::remove_file(f);
        let _ = std::fs::remove_file(format!("{f}.wal"));
    }
    // Der Server hat 1 GB, ohne Grenze belegt der Import ~1,5 GB.
    // Was nicht in den Speicher passt, lagert DuckDB aus; ohne Reihenfolge-Garantie braucht es dabei weniger Speicher.
    let config = Config::default().max_memory("150MB")?.threads(1)?.with("preserve_insertion_order", "false")?.with("temp_directory", format!("{roh}.tmp"))?;
    let db = Connection::open_with_flags(&roh, config)?;
    create(&db)?;
    for q in quellen() {
        let names: Vec<String> = zip
            .file_names()
            .map(|n| n.map(String::from))
            .filter(|n| n.as_ref().is_ok_and(|n| q.dateien.iter().any(|f| n.strip_prefix(f).is_some_and(|rest| rest.starts_with(['_', '.'])))))
            .collect::<Result<_, _>>()?;
        let mut app = db.appender(q.tabelle)?;
        let mut total = 0;
        for name in &names {
            let mut n = 0;
            parse(zip.by_name(name)?, |row| {
                n += 1;
                insert(&mut app, q, &kataloge, &row)?;
                // Der Appender puffert sonst ~200.000 Zeilen außerhalb der Speichergrenze.
                if n % 10_000 == 0 {
                    app.flush()?;
                }
                Ok(())
            })?;
            total += n;
            eprintln!("{name}: {n} Datensätze");
        }
        eprintln!("{}: {total} Datensätze aus {} Dateien", q.tabelle, names.len());
    }
    db.execute_batch(&format!("ATTACH '{tmp}' AS fertig"))?;
    finish(&db, "fertig")?;
    drop(db);
    std::fs::rename(&tmp, db_path)?;
    let _ = std::fs::remove_file(&roh);
    Ok(())
}

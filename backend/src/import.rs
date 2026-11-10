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
/// Alle Spalten; die Katalog-Spalten stehen am Ende (darauf verlässt sich `insert`).
pub static COLUMNS: LazyLock<Vec<&str>> = LazyLock::new(|| [TEXT, DATUM, ZAHL, KATALOG].concat());
/// Spalten mit Auswahl-Filter.
pub const AUSWAHL: &[&str] = &["Energietraeger", "EinheitBetriebsstatus", "Bundesland"];
/// Alle Stromerzeugungseinheiten (ohne Verbraucher- und Gas-Einheiten).
const FILES: &[&str] = &[
    "EinheitenSolar", "EinheitenWind", "EinheitenBiomasse", "EinheitenWasser", "EinheitenVerbrennung",
    "EinheitenKernkraft", "EinheitenGeothermieGrubengasDruckentspannung", "EinheitenStromSpeicher",
];

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

/// Typ einer Spalte für die Anzeige im Frontend.
pub fn typ(column: &str) -> &'static str {
    if ZAHL.contains(&column) {
        "num"
    } else if DATUM.contains(&column) {
        "date"
    } else {
        "text"
    }
}

pub fn create(db: &Connection) -> Res<()> {
    let cols: Vec<String> = COLUMNS.iter().map(|c| format!("{c} {}", if ZAHL.contains(c) { "DOUBLE" } else { "TEXT" })).collect();
    Ok(db.execute_batch(&format!("CREATE TABLE einheiten ({})", cols.join(", ")))?)
}

pub fn insert(app: &mut Appender, katalog: &HashMap<String, String>, row: &Row) -> Res<()> {
    let (plain, kat) = COLUMNS.split_at(COLUMNS.len() - KATALOG.len());
    let klartext = |ids: &String| ids.split(' ').map(|id| katalog.get(id).map_or(id, |w| w)).collect::<Vec<_>>().join(", ");
    // Werte als Text: DuckDB wandelt sie beim Anhängen in den Spaltentyp (z. B. "9.900" → 9.9).
    app.append_row(appender_params_from_iter(
        plain.iter().map(|c| row.get(*c).cloned()).chain(kat.iter().map(|c| row.get(*c).map(klartext))),
    ))?;
    Ok(())
}

/// Die Werte für die Auswahl-Filter (einmal hier statt bei jedem Seitenaufruf). Indizes braucht DuckDB nicht:
/// es speichert spaltenweise und durchsucht eine Spalte mit 10 Mio. Werten in Millisekunden.
pub fn finish(db: &Connection) -> Res<()> {
    let werte: Vec<String> = AUSWAHL.iter().map(|c| format!("SELECT DISTINCT '{c}' AS spalte, {c} AS wert FROM einheiten WHERE {c} IS NOT NULL")).collect();
    Ok(db.execute_batch(&format!("CREATE TABLE filterwerte AS {}", werte.join(" UNION ALL ")))?)
}

pub fn import(zip_path: &str, db_path: &str) -> Res<()> {
    let mut zip = zip::ZipArchive::new(File::open(zip_path)?)?;
    let mut katalog = HashMap::new();
    parse(zip.by_name("Katalogwerte.xml")?, |mut r| {
        katalog.extend(r.remove("Id").zip(r.remove("Wert")));
        Ok(())
    })?;
    let names: Vec<String> = zip
        .file_names()
        .map(|n| n.map(String::from))
        .filter(|n| n.as_ref().is_ok_and(|n| FILES.iter().any(|f| n.strip_prefix(f).is_some_and(|rest| rest.starts_with(['_', '.'])))))
        .collect::<Result<_, _>>()?;

    // In eine temporäre Datei schreiben: ein abgebrochener Import lässt die bisherige DB unberührt.
    let tmp = format!("{db_path}.tmp");
    let _ = std::fs::remove_file(&tmp);
    let _ = std::fs::remove_file(format!("{tmp}.wal"));
    // Der Server hat 1 GB, ohne Grenze belegt der Import ~1,5 GB.
    let db = Connection::open_with_flags(&tmp, Config::default().max_memory("150MB")?.threads(1)?)?;
    create(&db)?;
    let mut app = db.appender("einheiten")?;
    let mut total = 0;
    for (i, name) in names.iter().enumerate() {
        let mut n = 0;
        parse(zip.by_name(name)?, |row| {
            n += 1;
            insert(&mut app, &katalog, &row)?;
            // Der Appender puffert sonst ~200.000 Zeilen außerhalb der Speichergrenze.
            if n % 10_000 == 0 {
                app.flush()?;
            }
            Ok(())
        })?;
        total += n;
        eprintln!("[{}/{}] {name}: {n} Einheiten", i + 1, names.len());
    }
    drop(app);
    eprintln!("{total} Einheiten importiert, erstelle Filterwerte …");
    finish(&db)?;
    drop(db);
    std::fs::rename(&tmp, db_path)?;
    Ok(())
}

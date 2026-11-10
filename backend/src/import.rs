//! Import des MaStR-Gesamtdatenexports (ZIP mit XML-Dateien) nach SQLite.
use crate::Res;
use encoding_rs_io::DecodeReaderBytes;
use rusqlite::Connection;
use serde::Deserialize;
use std::{collections::HashMap, fs::File, io::{BufReader, Read}};

/// Spalten der Tabelle `einheiten`; die Namen sind die Tag-Namen im MaStR-XML.
pub const COLUMNS: &[&str] = &[
    "EinheitMastrNummer", "NameStromerzeugungseinheit", "Energietraeger", "EinheitBetriebsstatus",
    "Bruttoleistung", "Nettonennleistung", "Inbetriebnahmedatum", "Registrierungsdatum",
    "Bundesland", "Postleitzahl", "Ort", "Landkreis",
];
const REAL: &[&str] = &["Bruttoleistung", "Nettonennleistung"];
/// Im XML als Katalog-Id kodiert; wird beim Import in Klartext übersetzt. Zugleich die Auswahl-Filter.
const KATALOG: &[&str] = &["Energietraeger", "EinheitBetriebsstatus", "Bundesland"];
const INDEXED: &[&str] = &["Energietraeger", "Bundesland", "Bruttoleistung", "Inbetriebnahmedatum", "Postleitzahl"];
/// Alle Stromerzeugungseinheiten (ohne Verbraucher- und Gas-Einheiten).
const FILES: &[&str] = &[
    "EinheitenSolar", "EinheitenWind", "EinheitenBiomasse", "EinheitenWasser", "EinheitenVerbrennung",
    "EinheitenKernkraft", "EinheitenGeothermieGrubengasDruckentspannung", "EinheitenStromSpeicher",
];

type Row = HashMap<String, String>;

#[derive(Deserialize)]
struct Datei {
    #[serde(rename = "$value", default)]
    rows: Vec<Row>,
}

/// Eine MaStR-XML-Datei: flache Liste von Datensätzen, UTF-16 mit BOM (quick-xml erwartet UTF-8).
pub fn parse(xml: impl Read) -> Res<Vec<Row>> {
    Ok(quick_xml::de::from_reader::<_, Datei>(BufReader::new(DecodeReaderBytes::new(xml)))?.rows)
}

pub fn create(db: &Connection) -> Res<()> {
    let cols: Vec<String> = COLUMNS.iter().map(|c| format!("{c} {}", if REAL.contains(c) { "REAL" } else { "TEXT" })).collect();
    Ok(db.execute_batch(&format!("CREATE TABLE einheiten ({})", cols.join(", ")))?)
}

pub fn insert(db: &Connection, katalog: &HashMap<String, String>, rows: &[Row]) -> Res<()> {
    let sql = format!("INSERT INTO einheiten VALUES ({})", vec!["?"; COLUMNS.len()].join(","));
    let mut stmt = db.prepare_cached(&sql)?;
    for row in rows {
        stmt.execute(rusqlite::params_from_iter(COLUMNS.iter().map(|c| {
            let v = row.get(*c)?;
            Some(if KATALOG.contains(c) { katalog.get(v).unwrap_or(v) } else { v })
        })))?;
    }
    Ok(())
}

/// Indizes und die Werte für die Auswahl-Filter (einmal hier statt bei jedem Seitenaufruf).
pub fn finish(db: &Connection) -> Res<()> {
    for c in INDEXED {
        db.execute_batch(&format!("CREATE INDEX idx_{c} ON einheiten ({c})"))?;
    }
    let werte: Vec<String> = KATALOG.iter().map(|c| format!("SELECT DISTINCT '{c}' AS spalte, {c} AS wert FROM einheiten WHERE {c} IS NOT NULL")).collect();
    Ok(db.execute_batch(&format!("CREATE TABLE filterwerte AS {}; ANALYZE;", werte.join(" UNION ALL ")))?)
}

pub fn import(zip_path: &str, db_path: &str) -> Res<()> {
    let mut zip = zip::ZipArchive::new(File::open(zip_path)?)?;
    let katalog: HashMap<String, String> = parse(zip.by_name("Katalogwerte.xml")?)?
        .into_iter()
        .filter_map(|mut r| Some((r.remove("Id")?, r.remove("Wert")?)))
        .collect();
    let names: Vec<String> = zip
        .file_names()
        .map(|n| n.map(String::from))
        .filter(|n| n.as_ref().is_ok_and(|n| FILES.iter().any(|f| n.strip_prefix(f).is_some_and(|rest| rest.starts_with(['_', '.'])))))
        .collect::<Result<_, _>>()?;

    // In eine temporäre Datei schreiben: ein abgebrochener Import lässt die bisherige DB unberührt.
    let tmp = format!("{db_path}.tmp");
    let _ = std::fs::remove_file(&tmp);
    let mut db = Connection::open(&tmp)?;
    db.execute_batch("PRAGMA journal_mode = OFF; PRAGMA synchronous = OFF;")?;
    create(&db)?;
    let tx = db.transaction()?;
    let mut total = 0;
    for (i, name) in names.iter().enumerate() {
        let rows = parse(zip.by_name(name)?)?;
        insert(&tx, &katalog, &rows)?;
        total += rows.len();
        eprintln!("[{}/{}] {name}: {} Einheiten", i + 1, names.len(), rows.len());
    }
    tx.commit()?;
    eprintln!("{total} Einheiten importiert, erstelle Indizes …");
    finish(&db)?;
    drop(db);
    std::fs::rename(&tmp, db_path)?;
    Ok(())
}

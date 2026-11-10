mod import;

use axum::{extract::{Query, State}, http::StatusCode, routing::get, Json, Router};
use import::COLUMNS;
use duckdb::{params_from_iter, types::{Value, ValueRef}, AccessMode, Config, Connection};
use serde::Deserialize;
use serde_json::{json, Map, Value as Json_};
use tokio::sync::Semaphore;
use std::{collections::HashMap, sync::{Arc, Mutex}, time::SystemTime};

pub type Res<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Deserialize, Default)]
struct Filter {
    q: Option<String>,
    sort: Option<String>,
    desc: Option<bool>,
    page: Option<u32>,
    page_size: Option<u32>,
}

/// Spaltenfilter: `Spalte.von=…` / `Spalte.bis=…` (Bereich), `Spalte=…` (Auswahl: gleich, sonst: beginnt mit).
type Spaltenfilter = HashMap<String, String>;

fn where_clause(f: &Filter, spalten: &Spaltenfilter) -> (String, Vec<Value>) {
    let (mut conds, mut params) = (Vec::new(), Vec::<Value>::new());
    if let Some(q) = &f.q {
        conds.push("ifnull(NameStromerzeugungseinheit, '') || ' ' || EinheitMastrNummer ILIKE ?".to_string());
        params.push(format!("%{q}%").into());
    }
    for (key, v) in spalten.iter().filter(|(_, v)| !v.is_empty()) {
        let (name, op) = match key.rsplit_once('.') {
            Some((c, "von")) => (c, ">= ?"),
            Some((c, "bis")) => (c, "<= ?"),
            _ if import::AUSWAHL.contains(&key.as_str()) => (key.as_str(), "= ?"),
            _ => (key.as_str(), "ILIKE ? || '%'"),
        };
        // Der Spaltenname landet im SQL-Text, deshalb nur bekannte Spalten; sonstige Parameter (sort, page …) fallen hier raus.
        if !COLUMNS.contains(&name) {
            continue;
        }
        conds.push(format!("{name} {op}"));
        // Als Text gebunden: DuckDB wandelt ihn beim Vergleich mit einer Zahlenspalte selbst in eine Zahl.
        params.push(v.clone().into());
    }
    let sql = if conds.is_empty() { String::new() } else { format!("WHERE {}", conds.join(" AND ")) };
    (sql, params)
}

fn to_json(v: ValueRef) -> Json_ {
    match v {
        ValueRef::BigInt(i) => i.into(),
        ValueRef::Double(f) => f.into(),
        ValueRef::Text(t) => String::from_utf8_lossy(t).into(),
        _ => Json_::Null,
    }
}

fn einheiten(db: &Connection, f: &Filter, spalten: &Spaltenfilter) -> Res<Json_> {
    let (wher, params) = where_clause(f, spalten);
    let total: i64 = db.query_row(&format!("SELECT count(*) FROM einheiten {wher}"), params_from_iter(&params), |r| r.get(0))?;
    // `sort` landet im SQL-Text, deshalb nur bekannte Spaltennamen zulassen.
    let sort = f.sort.as_deref().filter(|s| COLUMNS.contains(s)).unwrap_or("rowid");
    let dir = if f.desc == Some(true) { "DESC" } else { "ASC" };
    let size = f.page_size.unwrap_or(50).min(10_000) as u64; // wie MAX_PAGE_SIZE im Frontend
    // Erst nur die rowids der Seite bestimmen, dann deren Spalten holen: sonst sortiert DuckDB bei hohen
    // Seiten alle 127 Spalten aller Treffer (42 s statt 0,1 s für Seite 100.000).
    let order = format!("ORDER BY {sort} {dir}, rowid {dir}");
    let sql = format!(
        "SELECT {} FROM einheiten WHERE rowid IN (SELECT rowid FROM einheiten {wher} {order} LIMIT {size} OFFSET {}) {order}",
        COLUMNS.join(","),
        f.page.unwrap_or(0) as u64 * size
    );
    let rows = db
        .prepare(&sql)?
        .query_map(params_from_iter(&params), |r| {
            COLUMNS.iter().enumerate().map(|(i, c)| Ok((c.to_string(), to_json(r.get_ref(i)?)))).collect::<duckdb::Result<Map<_, _>>>()
        })?
        .collect::<duckdb::Result<Vec<_>>>()?;
    Ok(json!({ "total": total, "rows": rows }))
}

/// Mögliche Werte der Auswahl-Filter: { "Bundesland": ["Bayern", …], … }
fn filterwerte(db: &Connection) -> Res<Json_> {
    let mut out = Map::new();
    let mut stmt = db.prepare("SELECT spalte, wert FROM filterwerte ORDER BY spalte, wert")?;
    for row in stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, to_json(r.get_ref(1)?))))? {
        let (spalte, wert) = row?;
        out.entry(spalte).or_insert_with(|| json!([])).as_array_mut().unwrap().push(wert);
    }
    Ok(out.into())
}

/// Alle Spalten mit Anzeigetyp: [{ "key": "Bruttoleistung", "type": "num" }, …]
fn spalten() -> Json_ {
    COLUMNS.iter().map(|c| json!({ "key": c, "type": import::typ(c) })).collect()
}

/// Eine DuckDB-Instanz für alle Anfragen: jede Instanz hat eigenen Puffer und eigene Threads, eine pro Anfrage
/// belegte bei 8 parallelen Anfragen ~5 GB (der Server hat 1 GB).
struct Db {
    path: String,
    conn: Mutex<Option<(SystemTime, Connection)>>,
    /// Höchstens 2 Abfragen gleichzeitig, sonst reicht auch die Speichergrenze nicht; weitere warten.
    slots: Arc<Semaphore>,
}

impl Db {
    /// Eine Verbindung der gemeinsamen Instanz; nach einem Neuimport (Datei ersetzt) wird neu geöffnet.
    fn connect(&self) -> Res<Connection> {
        let mtime = std::fs::metadata(&self.path)?.modified()?;
        let mut conn = self.conn.lock().unwrap();
        if conn.as_ref().is_none_or(|(t, _)| *t != mtime) {
            // 200 MB: eine schwere Abfrage belegt so ~300 MB, 8 parallele ~400–500 MB (bei 400 MB bis 730 MB).
            // Was nicht in den Speicher passt (große Sortierungen), lagert DuckDB in temp_directory aus.
            let config = Config::default()
                .access_mode(AccessMode::ReadOnly)?
                .max_memory("200MB")?
                .threads(2)?
                .with("temp_directory", std::env::temp_dir().join("strommarkt-duckdb").to_string_lossy())?;
            *conn = Some((mtime, Connection::open_with_flags(&self.path, config)?));
        }
        Ok(conn.as_ref().unwrap().1.try_clone()?)
    }
}

/// DuckDB blockiert, daher läuft jede Anfrage auf einem Blocking-Thread.
async fn with_db(db: Arc<Db>, f: impl FnOnce(&Connection) -> Res<Json_> + Send + 'static) -> Result<Json<Json_>, (StatusCode, String)> {
    // Die Erlaubnis wandert in den Thread: bricht der Client ab, läuft die Abfrage trotzdem zu Ende und zählt weiter mit.
    let slot = db.slots.clone().acquire_owned().await.expect("Semaphore wird nie geschlossen");
    tokio::task::spawn_blocking(move || {
        let _slot = slot;
        f(&db.connect()?)
    })
        .await
        .map_err(|e| e.to_string())
        .and_then(|r| r.map_err(|e| e.to_string()))
        .map(Json)
        .map_err(|e| {
            eprintln!("Fehler: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, "Datenbankfehler".into())
        })
}

#[tokio::main]
async fn main() -> Res<()> {
    let db = std::env::var("MASTR_DB").unwrap_or("mastr.db".into());
    let args: Vec<String> = std::env::args().collect();
    if args.get(1).map(String::as_str) == Some("import") {
        return import::import(args.get(2).ok_or("Aufruf: backend import <Gesamtdatenexport.zip>")?, &db);
    }
    let app = Router::new()
        .route("/api/einheiten", get(|State(db), Query(f): Query<Filter>, Query(s): Query<Spaltenfilter>| with_db(db, move |c| einheiten(c, &f, &s))))
        .route("/api/filter", get(|State(db)| with_db(db, filterwerte)))
        .route("/api/spalten", get(|| async { Json(spalten()) }))
        // Im Container liefert das Backend auch das gebaute Frontend aus
        .fallback_service(tower_http::services::ServeDir::new(std::env::var("STATIC_DIR").unwrap_or("../frontend/dist".into())))
        .with_state(Arc::new(Db { path: db, conn: Mutex::new(None), slots: Arc::new(Semaphore::new(2)) }));
    let listener = tokio::net::TcpListener::bind(std::env::var("ADDR").unwrap_or("127.0.0.1:3000".into())).await?;
    eprintln!("Backend läuft auf http://{}", listener.local_addr()?);
    Ok(axum::serve(listener, app).await?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn import_und_filter() {
        // Wie im Export: UTF-16 mit BOM, Katalog-Ids, Entities.
        let xml = "<?xml version='1.0' encoding='UTF-16'?><EinheitenSolar>\
            <EinheitSolar><EinheitMastrNummer>SEE1</EinheitMastrNummer><NameStromerzeugungseinheit>Müller &amp; Söhne</NameStromerzeugungseinheit><Bundesland>1403</Bundesland><Bruttoleistung>9.900</Bruttoleistung><WeitereBrennstoffe>1403 7</WeitereBrennstoffe><Unbekannt>x</Unbekannt></EinheitSolar>\
            <EinheitSolar><EinheitMastrNummer>SEE2</EinheitMastrNummer><Bundesland>1400</Bundesland><Bruttoleistung>100.000</Bruttoleistung></EinheitSolar>\
            </EinheitenSolar>";
        let utf16: Vec<u8> = format!("\u{feff}{xml}").encode_utf16().flat_map(u16::to_le_bytes).collect();
        let katalog = [("1403".to_string(), "Bayern".to_string())].into();
        let db = Connection::open_in_memory().unwrap();
        import::create(&db).unwrap();
        let mut app = db.appender("einheiten").unwrap();
        import::parse(&utf16[..], |row| import::insert(&mut app, &katalog, &row)).unwrap();
        drop(app);
        import::finish(&db).unwrap();

        let none = Spaltenfilter::new();
        let all = einheiten(&db, &Filter { sort: Some("Bruttoleistung; DROP TABLE einheiten".into()), ..Default::default() }, &none).unwrap();
        assert_eq!(all["total"], 2);
        assert_eq!(all["rows"][0]["NameStromerzeugungseinheit"], "Müller & Söhne");
        assert_eq!(all["rows"][0]["Bruttoleistung"], 9.9);
        assert_eq!(all["rows"][0]["WeitereBrennstoffe"], "Bayern, 7");
        assert_eq!(all["rows"][1]["WeitereBrennstoffe"], Json_::Null);
        assert!(spalten().as_array().unwrap().contains(&json!({ "key": "Bruttoleistung", "type": "num" })));

        let f = Filter { q: Some("müller".into()), ..Default::default() };
        let s = |pairs: &[(&str, &str)]| pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect::<Spaltenfilter>();
        let total = |f: &Filter, s: Spaltenfilter| einheiten(&db, f, &s).unwrap()["total"].clone();
        assert_eq!(total(&f, s(&[("Bundesland", "Bayern"), ("Bruttoleistung.bis", "10")])), 1);
        // Zahlen numerisch vergleichen (als Text wäre "9.9" > "10"), Auswahl exakt, Text als Präfix, Unbekanntes ignorieren.
        assert_eq!(total(&Filter::default(), s(&[("Bruttoleistung.von", "10")])), 1);
        assert_eq!(total(&Filter::default(), s(&[("Bundesland", "Bay")])), 0);
        assert_eq!(total(&Filter::default(), s(&[("NameStromerzeugungseinheit", "mül")])), 1);
        assert_eq!(total(&Filter::default(), s(&[("x; DROP TABLE einheiten", "1"), ("page", "0"), ("Ort", "")])), 2);
        let desc = einheiten(&db, &Filter { sort: Some("Bruttoleistung".into()), desc: Some(true), ..Default::default() }, &none).unwrap();
        assert_eq!(desc["rows"][0]["EinheitMastrNummer"], "SEE2");
        assert_eq!(filterwerte(&db).unwrap()["Bundesland"], json!(["1400", "Bayern"]));
    }
}

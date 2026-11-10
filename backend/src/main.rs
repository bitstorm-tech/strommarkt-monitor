mod import;

use axum::{extract::{Query, State}, http::StatusCode, routing::get, Json, Router};
use import::COLUMNS;
use rusqlite::{params_from_iter, types::{Value, ValueRef}, Connection, OpenFlags};
use serde::Deserialize;
use serde_json::{json, Map, Value as Json_};
use std::{collections::HashMap, sync::Arc};

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
        conds.push("ifnull(NameStromerzeugungseinheit, '') || ' ' || EinheitMastrNummer LIKE ?".to_string());
        params.push(format!("%{q}%").into());
    }
    for (key, v) in spalten.iter().filter(|(_, v)| !v.is_empty()) {
        let (name, op) = match key.rsplit_once('.') {
            Some((c, "von")) => (c, ">= ?"),
            Some((c, "bis")) => (c, "<= ?"),
            _ if import::AUSWAHL.contains(&key.as_str()) => (key.as_str(), "= ?"),
            // GLOB statt LIKE: nur GLOB kann für die Präfixsuche den Index nutzen.
            _ if key == "Postleitzahl" => (key.as_str(), "GLOB ? || '*'"),
            _ => (key.as_str(), "LIKE ? || '%'"),
        };
        // Der Spaltenname landet im SQL-Text, deshalb nur bekannte Spalten; sonstige Parameter (sort, page …) fallen hier raus.
        if !COLUMNS.contains(&name) {
            continue;
        }
        conds.push(format!("{name} {op}"));
        // Als Text gebunden: SQLite wandelt ihn beim Vergleich mit einer REAL-Spalte selbst in eine Zahl.
        params.push(v.clone().into());
    }
    let sql = if conds.is_empty() { String::new() } else { format!("WHERE {}", conds.join(" AND ")) };
    (sql, params)
}

fn to_json(v: ValueRef) -> Json_ {
    match v {
        ValueRef::Integer(i) => i.into(),
        ValueRef::Real(f) => f.into(),
        ValueRef::Text(t) => String::from_utf8_lossy(t).into(),
        _ => Json_::Null,
    }
}

fn einheiten(db: &Connection, f: &Filter, spalten: &Spaltenfilter) -> Res<Json_> {
    let (wher, params) = where_clause(f, spalten);
    // shortcut: count(*) und LIKE '%…%' scannen Tabelle bzw. Index (1–3 s bei 10 Mio. Zeilen mit 127 Spalten); bei Bedarf FTS5 / Count-Cache.
    let total: i64 = db.query_row(&format!("SELECT count(*) FROM einheiten {wher}"), params_from_iter(&params), |r| r.get(0))?;
    // `sort` landet im SQL-Text, deshalb nur bekannte Spaltennamen zulassen.
    let sort = f.sort.as_deref().filter(|s| COLUMNS.contains(s)).unwrap_or("rowid");
    let dir = if f.desc == Some(true) { "DESC" } else { "ASC" };
    let size = f.page_size.unwrap_or(50).min(500) as u64;
    // shortcut: OFFSET-Paginierung wird auf sehr hohen Seiten langsam; dann auf Keyset-Paginierung umstellen.
    let sql = format!(
        "SELECT {} FROM einheiten {wher} ORDER BY {sort} {dir}, rowid {dir} LIMIT {size} OFFSET {}",
        COLUMNS.join(","),
        f.page.unwrap_or(0) as u64 * size
    );
    let rows = db
        .prepare(&sql)?
        .query_map(params_from_iter(&params), |r| {
            COLUMNS.iter().enumerate().map(|(i, c)| Ok((c.to_string(), to_json(r.get_ref(i)?)))).collect::<rusqlite::Result<Map<_, _>>>()
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
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

/// SQLite blockiert, daher pro Anfrage eine eigene Nur-Lese-Verbindung auf einem Blocking-Thread.
async fn with_db(path: Arc<str>, f: impl FnOnce(&Connection) -> Res<Json_> + Send + 'static) -> Result<Json<Json_>, (StatusCode, String)> {
    tokio::task::spawn_blocking(move || f(&Connection::open_with_flags(&*path, OpenFlags::SQLITE_OPEN_READ_ONLY)?))
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
        .with_state(Arc::<str>::from(db));
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
        import::parse(&utf16[..], |row| import::insert(&db, &katalog, &row)).unwrap();
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

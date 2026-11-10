mod import;

use axum::{extract::{Path, Query, State}, http::StatusCode, response::{IntoResponse, Response}, routing::{get, post}, Json, Router};
use import::Ansicht;
use duckdb::{params_from_iter, types::{Value, ValueRef}, AccessMode, Config, Connection};
use serde::Deserialize;
use serde_json::{json, Map, Value as Json_};
use tokio::sync::Semaphore;
use std::{collections::HashMap, sync::{mpsc, Arc, Mutex}, time::{Duration, SystemTime}};

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

fn where_clause(a: &Ansicht, f: &Filter, spalten: &Spaltenfilter) -> (String, Vec<Value>) {
    let (mut conds, mut params) = (Vec::new(), Vec::<Value>::new());
    if let Some(q) = &f.q {
        conds.push(format!("concat_ws(' ', {}) ILIKE ?", a.suche.join(", ")));
        params.push(format!("%{q}%").into());
    }
    for (key, v) in spalten.iter().filter(|(_, v)| !v.is_empty()) {
        let (name, op) = match key.rsplit_once('.') {
            Some((c, "von")) => (c, ">= ?"),
            Some((c, "bis")) => (c, "<= ?"),
            _ if a.auswahl.contains(&key.as_str()) => (key.as_str(), "= ?"),
            _ => (key.as_str(), "ILIKE ? || '%'"),
        };
        // Der Spaltenname landet im SQL-Text, deshalb nur bekannte Spalten; sonstige Parameter (sort, page …) fallen hier raus.
        if !a.spalten.contains(&name) {
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

fn zeilen(db: &Connection, a: &Ansicht, f: &Filter, spalten: &Spaltenfilter) -> Res<Json_> {
    let (wher, params) = where_clause(a, f, spalten);
    let t = a.name;
    let total: i64 = db.query_row(&format!("SELECT count(*) FROM {t} {wher}"), params_from_iter(&params), |r| r.get(0))?;
    // `sort` landet im SQL-Text, deshalb nur bekannte Spaltennamen zulassen.
    let sort = f.sort.as_deref().filter(|s| a.spalten.contains(s)).unwrap_or("rowid");
    let dir = if f.desc == Some(true) { "DESC" } else { "ASC" };
    let size = f.page_size.unwrap_or(50).min(1000) as u64; // größte Seitengröße im Frontend
    // Erst nur die rowids der Seite bestimmen, dann deren Spalten holen: sonst sortiert DuckDB bei hohen
    // Seiten alle ~130 Spalten aller Treffer (42 s statt 0,1 s für Seite 100.000).
    let order = format!("ORDER BY {sort} {dir}, rowid {dir}");
    let sql = format!(
        "SELECT {} FROM {t} WHERE rowid IN (SELECT rowid FROM {t} {wher} {order} LIMIT {size} OFFSET {}) {order}",
        a.spalten.join(","),
        f.page.unwrap_or(0) as u64 * size
    );
    let rows = db
        .prepare(&sql)?
        .query_map(params_from_iter(&params), |r| {
            a.spalten.iter().enumerate().map(|(i, c)| Ok((c.to_string(), to_json(r.get_ref(i)?)))).collect::<duckdb::Result<Map<_, _>>>()
        })?
        .collect::<duckdb::Result<Vec<_>>>()?;
    Ok(json!({ "total": total, "rows": rows }))
}

/// Mögliche Werte der Auswahl-Filter: { "Bundesland": ["Bayern", …], … }
fn filterwerte(db: &Connection, a: &Ansicht) -> Res<Json_> {
    let mut out = Map::new();
    let mut stmt = db.prepare("SELECT spalte, wert FROM filterwerte WHERE ansicht = ? ORDER BY spalte, wert")?;
    for row in stmt.query_map([a.name], |r| Ok((r.get::<_, String>(0)?, to_json(r.get_ref(1)?))))? {
        let (spalte, wert) = row?;
        out.entry(spalte).or_insert_with(|| json!([])).as_array_mut().unwrap().push(wert);
    }
    Ok(out.into())
}

/// Alle Spalten mit Anzeigetyp: [{ "key": "Bruttoleistung", "type": "num" }, …]
fn spalten(a: &Ansicht) -> Json_ {
    a.spalten.iter().map(|c| json!({ "key": c, "type": import::typ(c) })).collect()
}

/// Größte Antwort an einen Agenten: mehr sprengt dessen Kontext.
const MAX_BYTES: usize = 50_000;
const SQL_TIMEOUT: Duration = Duration::from_secs(30);

/// Freie Abfrage für Agenten: { "columns": […], "rows": [[…]], "truncated": false }, alle Werte als Text.
/// Die Instanz ist read-only, ohne Dateizugriff und mit gesperrter Konfiguration (`Db::connect`);
/// hier kommen die Grenzen für Laufzeit und Größe dazu.
fn sql(db: &Connection, query: &str, timeout: Duration) -> Res<Json_> {
    let query = query.trim().trim_end_matches(';');
    // Sonst führt prepare() vorangestellte Anweisungen aus, etwa ATTACH ':memory:', das den gemeinsamen Speicher füllt.
    // shortcut: lehnt auch ein ';' in einem Textwert ab; bei Bedarf die Anweisungen mit duckdb_extract_statements zählen.
    if query.contains(';') {
        return Err("Nur eine Anweisung ohne ';' erlaubt".into());
    }
    let abbruch = db.interrupt_handle();
    // Endet die Funktion, fällt `_fertig` weg und der Wächter beendet sich ohne Abbruch.
    let (_fertig, fertig) = mpsc::channel::<()>();
    std::thread::spawn(move || {
        if fertig.recv_timeout(timeout) == Err(mpsc::RecvTimeoutError::Timeout) {
            abbruch.interrupt();
        }
    });
    // Als Unterabfrage sind nur lesende Anweisungen möglich, und DuckDB wandelt jeden Typ selbst in Text.
    // Das LIMIT begrenzt, was DuckDB als Ergebnis im Speicher hält: jede Zeile zählt unten mindestens
    // 5 Bytes, MAX_BYTES greift also immer zuerst.
    let mut stmt = db.prepare(&format!("SELECT COLUMNS(*)::VARCHAR FROM (\n{query}\n) LIMIT {}", MAX_BYTES / 4))?;
    let mut rows = stmt.query([])?;
    let columns = rows.as_ref().expect("Abfrage läuft").column_names();
    let (mut out, mut bytes, mut truncated) = (Vec::new(), 0, false);
    while let Some(r) = rows.next()? {
        let row = (0..columns.len()).map(|i| r.get_ref(i).map(to_json)).collect::<duckdb::Result<Vec<_>>>()?;
        bytes += serde_json::to_string(&row)?.len() + 1;
        if bytes > MAX_BYTES {
            truncated = true;
            break;
        }
        out.push(row);
    }
    Ok(json!({ "columns": columns, "rows": out, "truncated": truncated }))
}

const TOOL_BESCHREIBUNG: &str = "Fragt das Marktstammdatenregister (MaStR) der Bundesnetzagentur per SQL ab: \
genau eine lesende Abfrage im DuckDB-Dialekt, auch DESCRIBE <tabelle> und SUMMARIZE <tabelle>.\n\
Tabellen, jeweils mehrere Millionen Zeilen:\n\
- einheiten: Stromerzeugungseinheiten und Speicher, Schlüssel EinheitMastrNummer. Unter anderem Energietraeger, \
Bruttoleistung und Nettonennleistung (kW), Bundesland, Landkreis, Gemeinde, Postleitzahl, Inbetriebnahmedatum, \
EinheitBetriebsstatus, AnlagenbetreiberMastrNummer, NetzbetreiberMastrNummer, NetzbetreiberName, LokationMaStRNummer.\n\
- netzanschlusspunkte: Schlüssel NetzanschlusspunktMastrNummer, dazu LokationMaStRNummer, NetzbetreiberMaStRNummer, Spannungsebene.\n\
- marktakteure: Schlüssel MastrNummer, dazu Firmenname, Marktfunktion, Marktrollen.\n\
Verknüpfungen: einheiten.LokationMaStRNummer = netzanschlusspunkte.LokationMaStRNummer; \
einheiten.AnlagenbetreiberMastrNummer, einheiten.NetzbetreiberMastrNummer (vereinzelt mehrere, mit ', ' getrennt) \
und netzanschlusspunkte.NetzbetreiberMaStRNummer = marktakteure.MastrNummer.\n\
Datumsspalten sind Text (JJJJ-MM-TT, zum Rechnen ::DATE), Ja/Nein-Spalten sind 1/0. \
Vor der ersten Abfrage die Spalten mit DESCRIBE ansehen und Auswahlwerte mit SELECT DISTINCT prüfen.\n\
Grenzen: Abbruch nach 30 s (Fehler 'Interrupted'), höchstens etwa 50 KB Ergebnis (danach truncated: true). \
Deshalb aggregieren, nur benötigte Spalten wählen und LIMIT setzen. Alle Werte kommen als Text.";

/// MCP-Server (Streamable HTTP, zustandslos) mit dem einen Tool `sql`. Jede Antwort ist ein einzelnes JSON-Objekt, kein SSE.
async fn mcp(State(db): State<Arc<Db>>, Json(msg): Json<Json_>) -> Response {
    // Benachrichtigungen (ohne id) erwarten keine Antwort.
    let Some(id) = msg.get("id") else { return StatusCode::ACCEPTED.into_response() };
    let result = match msg["method"].as_str().unwrap_or_default() {
        "initialize" => json!({
            // shortcut: bestätigt jede Protokollversion des Clients, für ein einzelnes Tool unterscheiden sie sich bisher nicht;
            // feste Versionen aushandeln, sobald eine neue mehr verlangt.
            "protocolVersion": msg["params"]["protocolVersion"],
            "capabilities": { "tools": {} },
            "serverInfo": { "name": "strommarkt-monitor", "version": env!("CARGO_PKG_VERSION") },
        }),
        "ping" => json!({}),
        "tools/list" => json!({ "tools": [{
            "name": "sql",
            "description": TOOL_BESCHREIBUNG,
            "inputSchema": { "type": "object", "properties": { "query": { "type": "string", "description": "Eine SELECT-Abfrage (DuckDB-SQL)" } }, "required": ["query"] },
        }] }),
        "tools/call" => {
            let query = msg["params"]["arguments"]["query"].as_str().unwrap_or_default().to_string();
            let eine = db.sql_slot.clone().acquire_owned().await.expect("Semaphore wird nie geschlossen");
            let r = run(db, move |c| {
                let _eine = eine;
                sql(c, &query, SQL_TIMEOUT)
            })
            .await;
            // Der Agent bekommt die Fehlermeldung von DuckDB und kann seine Abfrage damit korrigieren.
            json!({ "isError": r.is_err(), "content": [{ "type": "text", "text": r.map_or_else(|e| e, |v| v.to_string()) }] })
        }
        _ => return Json(json!({ "jsonrpc": "2.0", "id": id, "error": { "code": -32601, "message": "Unbekannte Methode" } })).into_response(),
    };
    Json(json!({ "jsonrpc": "2.0", "id": id, "result": result })).into_response()
}

/// Eine DuckDB-Instanz für alle Anfragen: jede Instanz hat eigenen Puffer und eigene Threads, eine pro Anfrage
/// belegte bei 8 parallelen Anfragen ~5 GB (der Server hat 1 GB).
struct Db {
    path: String,
    conn: Mutex<Option<(SystemTime, Connection)>>,
    /// Höchstens 2 Abfragen gleichzeitig, sonst reicht auch die Speichergrenze nicht; weitere warten.
    slots: Arc<Semaphore>,
    /// Höchstens eine Agenten-Abfrage gleichzeitig; sie braucht zusätzlich einen der `slots`, belegt also nie beide.
    sql_slot: Arc<Semaphore>,
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
                .with("temp_directory", std::env::temp_dir().join("strommarkt-duckdb").to_string_lossy())?
                // Für freies SQL: kein Zugriff auf Dateien, ATTACH oder Extensions, und kein SET, das die Grenzen oben aufhebt.
                // Erst nach temp_directory, das sich danach nicht mehr setzen lässt.
                .enable_external_access(false)?
                .with("lock_configuration", "true")?;
            *conn = Some((mtime, Connection::open_with_flags(&self.path, config)?));
        }
        Ok(conn.as_ref().unwrap().1.try_clone()?)
    }
}

fn finde(name: &str) -> Result<&'static Ansicht, (StatusCode, String)> {
    import::ansicht(name).ok_or((StatusCode::NOT_FOUND, "Unbekannte Tabelle".into()))
}

/// DuckDB blockiert, daher läuft jede Anfrage auf einem Blocking-Thread.
async fn run(db: Arc<Db>, f: impl FnOnce(&Connection) -> Res<Json_> + Send + 'static) -> Result<Json_, String> {
    // Die Erlaubnis wandert in den Thread: bricht der Client ab, läuft die Abfrage trotzdem zu Ende und zählt weiter mit.
    let slot = db.slots.clone().acquire_owned().await.expect("Semaphore wird nie geschlossen");
    tokio::task::spawn_blocking(move || {
        let _slot = slot;
        f(&db.connect()?)
    })
        .await
        .map_err(|e| e.to_string())
        .and_then(|r| r.map_err(|e| e.to_string()))
}

async fn with_db(db: Arc<Db>, f: impl FnOnce(&Connection) -> Res<Json_> + Send + 'static) -> Result<Json<Json_>, (StatusCode, String)> {
    run(db, f).await.map(Json).map_err(|e| {
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
    let mut app = Router::new()
        .route("/api/{ansicht}", get(|State(db), Path(a): Path<String>, Query(f): Query<Filter>, Query(s): Query<Spaltenfilter>| async move {
            let a = finde(&a)?;
            with_db(db, move |c| zeilen(c, a, &f, &s)).await
        }))
        .route("/api/{ansicht}/filter", get(|State(db), Path(a): Path<String>| async move {
            let a = finde(&a)?;
            with_db(db, move |c| filterwerte(c, a)).await
        }))
        .route("/api/{ansicht}/spalten", get(|Path(a): Path<String>| async move { finde(&a).map(|a| Json(spalten(a))) }));
    // Ohne MCP_TOKEN gibt es den Endpunkt nicht. Der Token steht im Pfad statt in einem Header,
    // weil nicht jeder Chat-Client eigene Header mitschicken kann.
    if let Some(token) = std::env::var("MCP_TOKEN").ok().filter(|t| !t.is_empty()) {
        app = app.route(&format!("/mcp/{token}"), post(mcp));
    }
    let app = app
        // Im Container liefert das Backend auch das gebaute Frontend aus
        .fallback_service(tower_http::services::ServeDir::new(std::env::var("STATIC_DIR").unwrap_or("../frontend/dist".into())))
        .with_state(Arc::new(Db { path: db, conn: Mutex::new(None), slots: Arc::new(Semaphore::new(2)), sql_slot: Arc::new(Semaphore::new(1)) }));
    let listener = tokio::net::TcpListener::bind(std::env::var("ADDR").unwrap_or("127.0.0.1:3000".into())).await?;
    eprintln!("Backend läuft auf http://{}", listener.local_addr()?);
    Ok(axum::serve(listener, app).await?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_fuer_agenten() {
        let path = std::env::temp_dir().join(format!("strommarkt-test-{}.db", std::process::id()));
        let _ = std::fs::remove_file(&path);
        Connection::open(&path).unwrap().execute_batch("CREATE TABLE t AS SELECT range AS n, 'x' || range AS s FROM range(3)").unwrap();
        let db = Db { path: path.to_string_lossy().into(), conn: Mutex::new(None), slots: Arc::new(Semaphore::new(2)), sql_slot: Arc::new(Semaphore::new(1)) };
        let db = db.connect().unwrap();
        let ok = |q: &str| sql(&db, q, SQL_TIMEOUT).unwrap();

        // Sortierung bleibt, jeder Typ kommt als Text, ';' und Kommentar am Ende stören nicht.
        assert_eq!(
            ok("SELECT s, n / 2 AS halb, DATE '2024-01-02' AS d, [n] AS l, NULL AS leer FROM t ORDER BY n DESC LIMIT 2;\n"),
            json!({ "columns": ["s", "halb", "d", "l", "leer"], "rows": [["x2", "1.0", "2024-01-02", "[2]", null], ["x1", "0.5", "2024-01-02", "[1]", null]], "truncated": false })
        );
        assert_eq!(ok("SELECT sum(n) FROM t -- Summe")["rows"], json!([["3"]]));
        assert_eq!(ok("DESCRIBE t")["rows"][0][0], "n");
        assert_eq!(ok("SUMMARIZE t")["rows"][1][0], "s");
        assert_eq!(ok("SHOW TABLES")["rows"], json!([["t"]]));
        let gross = ok("SELECT repeat('x', 1000) FROM range(100)");
        assert_eq!(gross["truncated"], true);
        assert_eq!(gross["rows"].as_array().unwrap().len(), 49);

        // Kein Dateizugriff, keine zweite Anweisung, nichts Schreibendes, Konfiguration gesperrt.
        for q in ["SELECT * FROM read_csv('/etc/passwd')", "SELECT * FROM glob('/etc/*')", "SELECT 1) ; ATTACH ':memory:' AS a; SELECT (1", "DELETE FROM t", "SET max_memory = '10GB'"] {
            assert!(sql(&db, q, SQL_TIMEOUT).is_err(), "{q}");
        }
        for q in ["SET max_memory = '10GB'", "COPY t TO '/tmp/strommarkt-test.csv'", "ATTACH '/tmp/strommarkt-test2.db' AS a"] {
            assert!(db.execute_batch(q).is_err(), "{q}");
        }

        let start = std::time::Instant::now();
        assert!(sql(&db, "SELECT sum(a.range * b.range) FROM range(1000000) a, range(1000000) b", Duration::from_millis(200)).is_err());
        assert!(start.elapsed() < Duration::from_secs(5));
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn import_und_filter() {
        let db = Connection::open_in_memory().unwrap();
        import::create(&db).unwrap();
        let kataloge: import::Kataloge = [
            ("", [("1403".to_string(), "Bayern".to_string()), ("1".to_string(), "falsch".to_string())].into()),
            ("Marktfunktion", [("1".to_string(), "Stromnetzbetreiber".to_string())].into()),
        ]
        .into();
        // Wie im Export: UTF-16 mit BOM, Katalog-Ids, Entities.
        let lade = |q: &import::Quelle, xml: &str| {
            let utf16: Vec<u8> = format!("\u{feff}<?xml version='1.0' encoding='UTF-16'?><Liste>{xml}</Liste>").encode_utf16().flat_map(u16::to_le_bytes).collect();
            let mut app = db.appender(q.tabelle).unwrap();
            import::parse(&utf16[..], |row| import::insert(&mut app, q, &kataloge, &row)).unwrap();
        };
        lade(&import::EINHEITEN, "<EinheitSolar><EinheitMastrNummer>SEE1</EinheitMastrNummer><NameStromerzeugungseinheit>Müller &amp; Söhne</NameStromerzeugungseinheit><Bundesland>1403</Bundesland><Bruttoleistung>9.900</Bruttoleistung><WeitereBrennstoffe>1403 7</WeitereBrennstoffe><LokationMaStRNummer>SEL1</LokationMaStRNummer><Unbekannt>x</Unbekannt></EinheitSolar>\
            <EinheitSolar><EinheitMastrNummer>SEE2</EinheitMastrNummer><Bundesland>1400</Bundesland><Bruttoleistung>100.000</Bruttoleistung></EinheitSolar>");
        lade(&import::MARKTAKTEURE, "<M><MastrNummer>SNB1</MastrNummer><Firmenname>Netz GmbH</Firmenname><Marktfunktion>1</Marktfunktion></M>");
        lade(&import::LOKATIONEN, "<L><MastrNummer>SEL1</MastrNummer><VerknuepfteEinheitenMaStRNummern>SEE1</VerknuepfteEinheitenMaStRNummern></L>");
        // Zwei Netzanschlusspunkte derselben Lokation: die Einheit bleibt eine Zeile.
        lade(&import::NETZANSCHLUSSPUNKTE, "<N><NetzanschlusspunktMastrNummer>SAN1</NetzanschlusspunktMastrNummer><LokationMaStRNummer>SEL1</LokationMaStRNummer><NetzbetreiberMaStRNummer>SNB1</NetzbetreiberMaStRNummer></N>\
            <N><NetzanschlusspunktMastrNummer>SAN2</NetzanschlusspunktMastrNummer><LokationMaStRNummer>SEL1</LokationMaStRNummer><NetzbetreiberMaStRNummer>SNB1</NetzbetreiberMaStRNummer></N>");
        import::finish(&db, "memory").unwrap();

        let a = |name| import::ansicht(name).unwrap();
        let none = Spaltenfilter::new();
        let all = zeilen(&db, a("einheiten"), &Filter { sort: Some("Bruttoleistung; DROP TABLE einheiten".into()), ..Default::default() }, &none).unwrap();
        assert_eq!(all["total"], 2);
        assert_eq!(all["rows"][0]["NameStromerzeugungseinheit"], "Müller & Söhne");
        assert_eq!(all["rows"][0]["Bruttoleistung"], 9.9);
        assert_eq!(all["rows"][0]["WeitereBrennstoffe"], "Bayern, 7");
        assert_eq!(all["rows"][1]["WeitereBrennstoffe"], Json_::Null);
        assert!(spalten(a("einheiten")).as_array().unwrap().contains(&json!({ "key": "Bruttoleistung", "type": "num" })));

        let f = Filter { q: Some("müller".into()), ..Default::default() };
        let s = |pairs: &[(&str, &str)]| pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect::<Spaltenfilter>();
        let total = |name, f: &Filter, s: Spaltenfilter| zeilen(&db, a(name), f, &s).unwrap()["total"].clone();
        assert_eq!(total("einheiten", &f, s(&[("Bundesland", "Bayern"), ("Bruttoleistung.bis", "10")])), 1);
        // Zahlen numerisch vergleichen (als Text wäre "9.9" > "10"), Auswahl exakt, Text als Präfix, Unbekanntes ignorieren.
        assert_eq!(total("einheiten", &Filter::default(), s(&[("Bruttoleistung.von", "10")])), 1);
        assert_eq!(total("einheiten", &Filter::default(), s(&[("Bundesland", "Bay")])), 0);
        assert_eq!(total("einheiten", &Filter::default(), s(&[("NameStromerzeugungseinheit", "mül")])), 1);
        assert_eq!(total("einheiten", &Filter::default(), s(&[("x; DROP TABLE einheiten", "1"), ("page", "0"), ("Ort", "")])), 2);
        // Spalten einer anderen Ansicht gelten nicht als Filter.
        assert_eq!(total("einheiten", &Filter::default(), s(&[("Marktfunktion", "x")])), 2);
        let desc = zeilen(&db, a("einheiten"), &Filter { sort: Some("Bruttoleistung".into()), desc: Some(true), ..Default::default() }, &none).unwrap();
        assert_eq!(desc["rows"][0]["EinheitMastrNummer"], "SEE2");
        assert_eq!(filterwerte(&db, a("einheiten")).unwrap()["Bundesland"], json!(["1400", "Bayern"]));

        let verknuepft = zeilen(&db, a("einheiten"), &Filter { q: Some("SEE1".into()), ..Default::default() }, &none).unwrap();
        assert_eq!(verknuepft["total"], 1);
        assert_eq!(verknuepft["rows"][0]["NetzbetreiberName"], "Netz GmbH");
        assert_eq!(verknuepft["rows"][0]["NetzbetreiberMastrNummer"], "SNB1");
        assert_eq!(total("einheiten", &Filter::default(), s(&[("NetzbetreiberName", "netz")])), 1);

        let nap = zeilen(&db, a("netzanschlusspunkte"), &Filter { sort: Some("NetzanschlusspunktMastrNummer".into()), ..Default::default() }, &none).unwrap();
        assert_eq!(nap["total"], 2);
        assert_eq!(nap["rows"][1]["NetzanschlusspunktMastrNummer"], "SAN2");
        assert_eq!(nap["rows"][1]["VerknuepfteEinheitenMaStRNummern"], "SEE1");
        assert_eq!(nap["rows"][1]["NetzbetreiberName"], "Netz GmbH");

        let akteure = zeilen(&db, a("marktakteure"), &Filter { q: Some("netz".into()), ..Default::default() }, &none).unwrap();
        assert_eq!(akteure["rows"][0]["Marktfunktion"], "Stromnetzbetreiber");
        assert_eq!(filterwerte(&db, a("marktakteure")).unwrap()["Marktfunktion"], json!(["Stromnetzbetreiber"]));
    }
}

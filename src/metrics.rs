use chrono::{MIN_DATETIME, MAX_DATETIME, DateTime, Utc, TimeZone};
use data::{Location, Visit};
use json::JsonValue;
use postgres_types::ToSql;
use postgres::Client;
use postgres::tls::NoTls;
use std::convert::{TryInto, TryFrom};
use std::fmt::Display;
use std::io::{self, Read};

mod data;

const CONN_STR: &str = "postgres://Garrett@localhost/Garrett";
const SQLITE_DB_FILE: &str = "/root/unkdir/doc_root/loclog/_data.sqlite";

// data types:
// - json str
// - rust obj
// - sql obj
// ---
// conversion paths:
// - sql obj -> json str:
//      - scenario: serving api read request
//      - method: query map to json::object macro
// - rust obj <-> sql obj:
//      - scenario: reading from db in rust, maybe to do calculation in rust
//      - method: ToSql and FromSql
// - json str -> rust obj:
//      - scenario: reading data from upload request
//      - method: json parse and manual populate
// ---
// maybe: treat rust obj as source of truth and all conversions go thru rust obj


// TODO: implement map_err alterative to translate this:
// <fallible_expr>.map_err(|e| format!("Error preparing statement: {}", e))?;
//  to
// <fallible_expr>.to_str_err("preparing statement")?;

pub(crate) fn locations_start_end_helper(start: DateTime<Utc>, end: DateTime<Utc>) -> Result<Vec<Location>, String> {
    let mut client = get_db_client()?;
    let query = format!("
        select date, latitude, longitude, altitude, horizontal_accuracy, vertical_accuracy, course, speed, floor
        from get_locations()
        where date >= '{start}'
            and date <= '{end}'",
        start = start.to_rfc3339(),
        end = end.to_rfc3339());
    client.query(query.as_str(), &[])
        .map_err(|e| format!("Error executing query '{}': {}", query, e))?
        .iter()
        .map(|row| row.try_into())
        .collect::<Result<Vec<Location>, String>>()
}

pub(crate) fn locations_start_end(start: &str, end: &str) -> Result<(i32, Vec<u8>, &'static str), (i32, String)> {
    let start = DateTime::parse_from_rfc3339(start)
        .map_err(|e| (500, format!("Error parsing '{}' as start date: {}", start, e)))?
        .with_timezone(&Utc);
    let end = DateTime::parse_from_rfc3339(end)
        .map_err(|e| (500, format!("Error parsing '{}' as end date: {}", end, e)))?
        .with_timezone(&Utc);
    let locations = locations_start_end_helper(start, end)
        .map_err(|e| (500, format!("Error getting visit data: {}", e)))?;
    let json_str = json::stringify(locations).into_bytes();
    Ok((200, json_str, "application/json; charset=utf-8"))
}

pub(crate) fn locations() -> Result<(i32, Vec<u8>, &'static str), (i32, String)> {
    let locations = locations_start_end_helper(MIN_DATETIME, MAX_DATETIME)
        .map_err(|e| (500, format!("Error getting location data: {}", e)))?;
    let json_str = json::stringify(locations).into_bytes();
    Ok((200, json_str, "application/json; charset=utf-8"))
}

pub(crate) fn visits() -> Result<(i32, Vec<u8>, &'static str), (i32, String)> {
    let visits = visits_start_end_helper(MIN_DATETIME, MAX_DATETIME)
        .map_err(|e| (500, format!("Error getting visit data: {}", e)))?;
    let json_str = json::stringify(visits).into_bytes();
    Ok((200, json_str, "application/json; charset=utf-8"))
}

fn visits_start_end_helper(start: DateTime<Utc>, end: DateTime<Utc>) -> Result<Vec<Visit>, String> {
    let mut client = get_db_client()?;
    let query = format!("
        select arrival, departure, latitude, longitude, horizontal_accuracy
        from get_visits()
        where departure >= '{start}'
            and arrival <= '{end}'",
        start = start.to_rfc3339(),
        end = end.to_rfc3339());
    client.query(query.as_str(), &[])
        .map_err(|e| format!("Error executing query '{}': {}", query, e))?
        .iter()
        .map(|row| row.try_into())
        .collect::<Result<Vec<Visit>, String>>()
}

pub(crate) fn visits_start_end(start: &str, end: &str) -> Result<(i32, Vec<u8>, &'static str), (i32, String)> {
    let start = DateTime::parse_from_rfc3339(start)
        .map_err(|e| (500, format!("Error parsing '{}' as start date: {}", start, e)))?
        .with_timezone(&Utc);
    let end = DateTime::parse_from_rfc3339(end)
        .map_err(|e| (500, format!("Error parsing '{}' as end date: {}", end, e)))?
        .with_timezone(&Utc);
    let visits = visits_start_end_helper(start, end)
        .map_err(|e| (500, format!("Error getting visit data: {}", e)))?;
    let json_str = json::stringify(visits).into_bytes();
    Ok((200, json_str, "application/json; charset=utf-8"))
}

pub(crate) fn upload_visits() -> Result<(i32, Vec<u8>, &'static str), (i32, String)> {
    let mut visits_json = String::new();
    io::stdin().read_to_string(&mut visits_json)
        .map_err(|e| (500, format!("Error reading visit data from body: {}", e)))?;
    try_block! {
        let json_obj: JsonValue = json::parse(&visits_json)
            .map_err(|e| format!("Error parsing visit data as json: {}", e))?;
        let json_array: Vec<JsonValue> = if let JsonValue::Array(a) = json_obj { a } else {
            return Err("visit json is not json array".to_string());
        };
        let visits: Vec<Visit> = json_array.into_iter()
            .map(|x| x.try_into())
            .collect::<Result<Vec<Visit>, String>>()
            .map_err(|e| format!("Error parsing json obj to visit obj: {}", e))?;
        let param_placeholders = get_param_placeholders(visits.len(), 5);
        let query = format!(
            "insert into visits (arrival,departure,latitude,longitude,horizontal_accuracy)\n\
            values\n{}",
            param_placeholders);
        let params = visits.iter()
            .map(|v| -> Vec<&(dyn ToSql + Sync)> {
                vec![&v.arrival, &v.departure, &v.latitude, &v.longitude, &v.horizontal_accuracy]
            })
            .flatten()
            .collect::<Vec<&(dyn ToSql + Sync)>>();

        let mut client = get_db_client()?;
        client.execute(query.as_str(), &params[..])
            .map_err(|e| format!("Error executing query '{}': {}", query, e))?;

        Ok(())
    }.map_err(|e| (500, format!("Error uploading visit data: {}", e)))?;
    Ok((200, Vec::new(), "text/plain"))
}

pub(crate) fn upload_locations() -> Result<(i32, Vec<u8>, &'static str), (i32, String)> {
    let mut locations_json = String::new();
    io::stdin().read_to_string(&mut locations_json)
        .map_err(|e| (500, format!("Error reading location data from body: {}", e)))?;
    try_block! {
        let json_obj: JsonValue = json::parse(&locations_json)
            .map_err(|e| format!("Error parsing location data as json: {}", e))?;
        let json_array: Vec<JsonValue> = if let JsonValue::Array(a) = json_obj { a } else {
            return Err("location json is not json array".to_string());
        };
        let locations: Vec<Location> = json_array.into_iter()
            .map(|x| x.try_into())
            .collect::<Result<Vec<Location>, String>>()
            .map_err(|e| format!("Error parsing json obj to location obj: {}", e))?;
        let param_placeholders = get_param_placeholders(locations.len(), 9);
        let query = format!(
            "insert into locations (date,latitude,longitude,altitude,horizontal_accuracy,vertical_accuracy,course,speed,floor)\n\
            values\n{}",
            param_placeholders);
        let params = locations.iter()
            .map(|l| -> Vec<&(dyn ToSql + Sync)> {
                vec![&l.date, &l.latitude, &l.longitude, &l.altitude, &l.horizontal_accuracy,
                    &l.vertical_accuracy, &l.course, &l.speed, &l.floor]
            })
            .flatten()
            .collect::<Vec<&(dyn ToSql + Sync)>>();

        let mut client = get_db_client()?;
        client.execute(query.as_str(), &params[..])
            .map_err(|e| format!("Error executing query '{}': {}", query, e))?;

        Ok(())
    }.map_err(|e| (500, format!("Error uploading location data: {}", e)))?;
    Ok((200, Vec::new(), "text/plain"))
}

// create a bunch of lines like:
// ($1, $2, ..., $9),
// ($10, $11, ..., $18)
fn get_param_placeholders(rows: usize, cols: usize) -> String {
    (0..rows)
        .map(|i| {
            let comma_sep = ((i*cols+1)..=(i*cols+cols))
                .map(|p| format!("${}", p))
                .collect::<Vec<String>>()
                .join(",");
            format!("({})", comma_sep)
        })
        .collect::<Vec<String>>()
        .join(",\n")
}

fn get_db_client() -> Result<Client, String> {
    Client::connect(CONN_STR, NoTls)
        .map_err(|e| format!("Error setting up connection with connection string '{}': {}", CONN_STR, e))
}

fn parse_json_to_vec<E: Display, T: TryFrom<JsonValue, Error = E>>(json: &str) -> Result<Vec<T>, String> {
    let json_obj: JsonValue = json::parse(json)
        .map_err(|e| format!("Error parsing str as json: {}", e))?;
    let json_array: Vec<JsonValue> = if let JsonValue::Array(a) = json_obj { a } else {
        let enum_tag = match json_obj {
            JsonValue::Null => "Null",
            JsonValue::Short(_) => "Short",
            JsonValue::String(_) => "String",
            JsonValue::Number(_) => "Number",
            JsonValue::Boolean(_) => "Boolean",
            JsonValue::Object(_) => "Object",
            JsonValue::Array(_) => "Array",
        };
        return Err(format!("expected json array but found '{}'", enum_tag));
    };
    json_array.into_iter()
        .map(|json_value| json_value.try_into())
        .collect::<Result<Vec<T>, _>>()
        .map_err(|e| format!("Error converting JsonValue to '{}'", std::any::type_name::<T>()))
}

mod sqlite {
    use super::*;
    use verify_sqlite_schema::verify_table_schema;
    use rusqlite::{ToSql, params, named_params, Connection, Result};

    // id                  INTEGER PRIMARY KEY
    // date_unix_ms        INTEGER
    // latitude            REAL
    // longitude           REAL
    // altitude            REAL
    // horizontal_accuracy REAL
    // vertical_accuracy   REAL
    // course              REAL
    // speed               REAL
    // floor               INTEGER
    const LOCATION_SCHEMA: &[(&str, &str)] = &[
        ("id", "INTEGER"),
        ("date_unix_ms", "INTEGER"),
        ("latitude", "REAL"),
        ("longitude", "REAL"),
        ("altitude", "REAL"),
        ("horizontal_accuracy", "REAL"),
        ("vertical_accuracy", "REAL"),
        ("course", "REAL"),
        ("speed", "REAL"),
        ("floor", "INTEGER"),
    ];

    // id                       INTEGER PRIMARY KEY
    // arrival_time_unix_ms     INTEGER
    // departure_time_unix_ms   INTEGER
    // latitude                 REAL
    // longitude                REAL
    // horizontal_accuracy      REAL
    const VISIT_SCHEMA: &[(&str, &str)] = &[
        ("id", "INTEGER"),
        ("arrival_time_unix_ms", "INTEGER"),
        ("departure_time_unix_ms", "INTEGER"),
        ("latitude", "REAL"),
        ("longitude", "REAL"),
        ("horizontal_accuracy", "REAL"),
    ];

    fn get_sqlite_conn() -> Result<Connection, String> {
        let conn = Connection::open(SQLITE_DB_FILE)
            .map_err(|e| format!("Error opening db at path '{}': {}", SQLITE_DB_FILE, e))?;
        verify_table_schema(&conn, "location", LOCATION_SCHEMA)
            .map_err(|e| format!("Error verifying location table schema: {}", e))?;
        verify_table_schema(&conn, "visit", VISIT_SCHEMA)
            .map_err(|e| format!("Error verifying visit table schema: {}", e))?;
        Ok(conn)
    }

    pub(crate) fn upload_locations() -> Result<(i32, Vec<u8>, &'static str), (i32, String)> {
        try_block! {
            let mut locations_json = String::new();
            io::stdin().read_to_string(&mut locations_json)
                .map_err(|e| format!("Error reading location data from body: {}", e))?;
            let locations: Vec<Location> = parse_json_to_vec(&locations_json)
                .map_err(|e| format!("Error parsing json to location vec: {}", e))?;
            let mut conn = get_sqlite_conn()
                .map_err(|e| format!("Error getting sqlite connection: {}", e))?;
            let tx = conn.transaction().map_err(|e| format!("Error starting new transaction: {}", e))?;
            let mut insert_stmt = tx.prepare(
                "INSERT INTO location (
                    date_unix_ms, latitude, longitude, altitude, horizontal_accuracy,
                    vertical_accuracy, course, speed, floor)
                VALUES (
                    :date_unix_ms, :latitude, :longitude, :altitude, :horizontal_accuracy,
                    :vertical_accuracy, :course, :speed, :floor)")
                .map_err(|e| format!("Error preparing insert statement: {}", e))?;
            for location in locations {
                insert_stmt.execute(
                    named_params! {
                        ":date_unix_ms": location.date.timestamp_millis(),
                        ":latitude": location.latitude,
                        ":longitude": location.longitude,
                        ":altitude": location.altitude,
                        ":horizontal_accuracy,": location.horizontal_accuracy,
                        ":vertical_accuracy": location.vertical_accuracy,
                        ":course": location.course,
                        ":speed": location.speed,
                        ":floor": location.floor,
                    }).map_err(|e| format!("Error executing insert statement: {}", e))?;
            }
            std::mem::drop(insert_stmt);
            tx.commit().map_err(|e| format!("Error committing transaction: {}", e))?;
            Ok(())
        }.map_err(|e: String| (500, format!("Error uploading location data: {}", e)))?;
        Ok((200, Vec::new(), "text/plain"))
    }
    
    pub(crate) fn upload_visits() -> Result<(i32, Vec<u8>, &'static str), (i32, String)> {
        try_block! {
            let mut visits_json = String::new();
            io::stdin().read_to_string(&mut visits_json)
                .map_err(|e| format!("Error reading visit data from body: {}", e))?;
            let visits: Vec<Visit> = parse_json_to_vec(&visits_json)
                .map_err(|e| format!("Error parsing json to visit vec: {}", e))?;

            let mut conn = get_sqlite_conn()
                .map_err(|e| format!("Error getting sqlite connection: {}", e))?;
            let tx = conn.transaction().map_err(|e| format!("Error starting new transaction: {}", e))?;
            let mut insert_stmt = tx.prepare(
                "INSERT INTO visit (
                    arrival_time_unix_ms, departure_time_unix_ms, latitude, longitude, horizontal_accuracy)
                VALUES (
                    :arrival_time_unix_ms, :departure_time_unix_ms, :latitude, :longitude, :horizontal_accuracy)")
                .map_err(|e| format!("Error preparing insert statement: {}", e))?;

            // row -> obj
            //  - for each field name, try to get col of that name
            //      arrival_time_unix_ms   => || Utc.timestamp_millis(arrival_time_unix_ms)
            //      departure_time_unix_ms => || Utc.timestamp_millis(departure_time_unix_ms)
            // obj -> row
            //  - for each field name, have col in insert statement
            //    (but how to know what column name is?)
            //      visit.arrival.map(|d| d.timestamp_millis())     => arrival_time_unix_ms
            //      visit.departure.map(|d| d.timestamp_millis())   => departure_time_unix_ms
            
            for visit in visits {
                insert_stmt.execute(
                    named_params! {
                        ":arrival_time_unix_ms": visit.arrival.map(|a| a.timestamp_millis()),
                        ":departure_time_unix_ms": visit.departure.map(|d| d.timestamp_millis()),
                        ":latitude": visit.latitude,
                        ":longitude": visit.longitude,
                        ":horizontal_accuracy": visit.horizontal_accuracy,
                    }).map_err(|e| format!("Error executing insert statement: {}", e))?;
            }
            std::mem::drop(insert_stmt);
            tx.commit().map_err(|e| format!("Error committing transaction: {}", e))?;
            Ok(())
        }.map_err(|e: String| (500, format!("Error uploading visit data: {}", e)))?;
        Ok((200, Vec::new(), "text/plain"))
    }

    pub(crate) fn locations_start_end_helper(start: DateTime<Utc>, end: DateTime<Utc>) -> Result<Vec<Location>, String> {
        unimplemented!();

        // let mut conn = get_sqlite_conn()
        //     .map_err(|e| format!("Error getting sqlite connection: {}", e))?;
        // // TODO: implement get_locations view in sqlite db
        // let mut stmt = conn.prepare(
        //         "select date_unix_ms, latitude, longitude, altitude, horizontal_accuracy, vertical_accuracy, course, speed, floor
        //         from get_locations()
        //         where date_unix_ms >= :start
        //             and date_unix_ms <= :end")
        //     .map_err(|e| format!("Error preparing statement: {}", e))?;
        // stmt.query_map(
        //         named_params! { ":start": start.timestamp_millis(), ":end": end.timestamp_millis() },
        //         |row| Ok(row.try_into()))
        //     .map_err(|e| format!("Error executing insert statement: {}", e))?
        //     .collect::<Vec<Result<Result<Location, String>, rusqlite::Error>>>();
    }
}

///////////////////
// Mostly unused //
///////////////////
#[allow(dead_code)]
pub mod computer_activity {
    use super::get_db_client;
    
    #[derive(Debug)]
    pub(crate) struct TimeInMetric {
        // Abbreviated day of the week, i.e. Mon, Tue, etc.
        day_of_week: String,

        // average minutes after midnight that I start interacting with my work computer in the morning
        avg_minutes: f64,
    }

    fn time_in_helper() -> Result<Vec<TimeInMetric>, String> {
        let mut client = get_db_client()?;

        let query = "select * from time_in()";

        let results: Vec<TimeInMetric> = client.query(query, &[])
            .map_err(|e| format!("Error executing query '{}': {}", query, e))?
            .iter()
            .map(|row| TimeInMetric {
                day_of_week: row.get(0),
                avg_minutes: row.get(1),
            })
            .collect();
        Ok(results)
    }

    pub(crate) fn time_in() -> Result<(i32, Vec<u8>, &'static str), (i32, String)> {
        // TODO: fix or remove this fn
        return Err((500, "too lazy to implement json serialization".to_string()));
        // let times = time_in_helper()
        //     .map_err(|e| (500, format!("Error getting time in data: {}", e)))?;
        // let json_str = Ok::<_, u8>(String::new())// serde_json::to_string(&times)
        //     .map_err(|e| (500, format!("Error serializing to json: {}", e)))?
        //     .into_bytes();
        // Ok((200, json_str, "application/json; charset=utf-8"))
    }

    #[derive(Debug)]
    pub(crate) struct ProgramUsageMetric2 {
        hour_of_day: i32,
        program: String,
        window_title: String,
        count: i32,
    }

    fn top_limit_helper() -> Result<Vec<ProgramUsageMetric2>, String> {
        let mut client = get_db_client()?;

        let query = "select * from top_foo()";

        let results: Vec<ProgramUsageMetric2> = client.query(query, &[])
            .map_err(|e| format!("Error executing query '{}': {}", query, e))?
            .iter()
            .map(|row| ProgramUsageMetric2 {
                hour_of_day: row.get(0),
                program: row.get(1),
                window_title: row.get(2),
                count: row.get(3),
            })
            .collect();
        Ok(results)
    }

    pub(crate) fn top_limit() -> Result<(i32, Vec<u8>, &'static str), (i32, String)> {
        // TODO: fix or remove this fn
        return Err((500, "too lazy to implement json serialization".to_string()));
        // let usage = top_limit_helper()
        //     .map_err(|e| (500, format!("Error getting program usage: {}", e)))?;
        // let json_str = Ok::<_, u8>(String::new()) // serde_json::to_string(&usage)
        //     .map_err(|e| (500, format!("Error serializing to json: {}", e)))?
        //     .into_bytes();
        // Ok((200, json_str, "application/json; charset=utf-8"))
    }


    #[derive(Debug)]
    pub(crate) struct ProgramUsageMetric {
        hour_of_day: f64,
        program: String,
        window_title: String,
        count: i64,
    }

    #[allow(dead_code)]
    fn program_usage_by_hour_helper() -> Result<Vec<ProgramUsageMetric>, String> {
        let mut client = get_db_client()?;

        let query = "select * from program_usage_by_hour()";

        let results: Vec<ProgramUsageMetric> = client.query(query, &[])
            .map_err(|e| format!("Error executing query '{}': {}", query, e))?
            .iter()
            .map(|row| ProgramUsageMetric {
                hour_of_day: row.get(0),
                program: row.get(1),
                window_title: row.get(2),
                count: row.get(3),
            })
            .collect();
        Ok(results)
    }

    pub(crate) fn program_usage_by_hour() -> Result<(i32, Vec<u8>, &'static str), (i32, String)> {
        // TODO: fix or remove this fn
        return Err((500, "too lazy to implement json serialization".to_string()));
        // let usage = program_usage_by_hour_helper()
        //     .map_err(|e| (500, format!("Error getting program usage: {}", e)))?;
        // let json_str = Ok::<_, u8>(String::new()) // serde_json::to_string(&usage)
        //     .map_err(|e| (500, format!("Error serializing to json: {}", e)))?
        //     .into_bytes();
        // Ok((200, json_str, "application/json; charset=utf-8"))
    }
}

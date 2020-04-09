use std::io::{self, Read};

use serde::{Serialize, Deserialize};

use chrono::{DateTime, Utc};
use chrono::offset::FixedOffset;
use postgres::{Connection, TlsMode};
use postgres::types::ToSql;


const CONN_STR: &str = "postgres://Garrett@localhost/Garrett";

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Visit {
    arrival: Option<DateTime<FixedOffset>>,
    departure: Option<DateTime<FixedOffset>>,
    latitude: f64,
    longitude: f64,
    horizontal_accuracy: f64
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Location {
    date: DateTime<FixedOffset>,
    latitude: f64,
    longitude: f64,
    altitude: f64,
    horizontal_accuracy: f64,
    vertical_accuracy: f64,
    course: Option<f64>,
    speed: Option<f64>,
    floor: Option<i32>,
}

pub(crate) fn locations_start_end_helper(start: DateTime<Utc>, end: DateTime<Utc>) -> Result<Vec<Location>, String> {
    let conn = Connection::connect(CONN_STR, TlsMode::None)
        .map_err(|e| format!("Error setting up connection with connection string '{}': {}", CONN_STR, e))?;

    let query = format!("
        select date, latitude, longitude, altitude, horizontal_accuracy, vertical_accuracy, course, speed, floor
        from get_locations()
        where date >= '{start}'
            and date <= '{end}'",
        start = start.to_rfc3339(),
        end = end.to_rfc3339());

    let results: Vec<Location> = conn.query(&query, &[])
        .map_err(|e| format!("Error executing query '{}': {}", query, e))?
        .iter()
        .map(|row| Location {
            date: row.get::<usize, DateTime<FixedOffset>>(0),
            latitude: row.get(1),
            longitude: row.get(2),
            altitude: row.get(3),
            horizontal_accuracy: row.get(4),
            vertical_accuracy: row.get(5),
            course: {
                let c: f64 = row.get(6);
                if c < 0.0 {
                    None
                } else {
                    Some(c)
                }
            },
            speed: {
                let s: f64 = row.get(7);
                if s < 0.0 {
                    None
                } else {
                    Some(s)
                }
            },
            floor: row.get(8),
        })
        .collect();
    Ok(results)
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
    let json_str = serde_json::to_string(&locations)
        .map_err(|e| (500, format!("Error serializing to json: {}", e)))?
        .into_bytes();
    Ok((200, json_str, "application/json; charset=utf-8"))
}

fn locations_helper() -> Result<Vec<Location>, String> {
    let conn = Connection::connect(CONN_STR, TlsMode::None)
        .map_err(|e| format!("Error setting up connection with connection string '{}': {}", CONN_STR, e))?;

    let query = "select * from get_locations()";

    let results: Vec<Location> = conn.query(query, &[])
        .map_err(|e| format!("Error executing query '{}': {}", query, e))?
        .iter()
        .map(|row| Location {
            date: row.get::<usize, DateTime<FixedOffset>>(0),
            latitude: row.get(1),
            longitude: row.get(2),
            altitude: row.get(3),
            horizontal_accuracy: row.get(4),
            vertical_accuracy: row.get(5),
            course: {
                let c: f64 = row.get(6);
                if c < 0.0 {
                    None
                } else {
                    Some(c)
                }
            },
            speed: {
                let s: f64 = row.get(7);
                if s < 0.0 {
                    None
                } else {
                    Some(s)
                }
            },
            floor: row.get(8),
        })
        .collect();
    Ok(results)
}

pub(crate) fn locations() -> Result<(i32, Vec<u8>, &'static str), (i32, String)> {
    let locations = locations_helper()
        .map_err(|e| (500, format!("Error getting location data: {}", e)))?;
    let json_str = serde_json::to_string(&locations)
        .map_err(|e| (500, format!("Error serializing to json: {}", e)))?
        .into_bytes();
    Ok((200, json_str, "application/json; charset=utf-8"))
}

fn visits_helper() -> Result<Vec<Visit>, String> {
    let conn = Connection::connect(CONN_STR, TlsMode::None)
        .map_err(|e| format!("Error setting up connection with connection string '{}': {}", CONN_STR, e))?;

    let query = "select * from get_visits()";

    let results: Vec<Visit> = conn.query(query, &[])
        .map_err(|e| format!("Error executing query '{}': {}", query, e))?
        .iter()
        .map(|row| Visit {
            arrival: row.get::<usize, Option<DateTime<FixedOffset>>>(0),
            departure: row.get::<usize, Option<DateTime<FixedOffset>>>(1),
            latitude: row.get(2),
            longitude: row.get(3),
            horizontal_accuracy: row.get(4),
        })
        .collect();
    Ok(results)
}

pub(crate) fn visits() -> Result<(i32, Vec<u8>, &'static str), (i32, String)> {
    let visits = visits_helper()
        .map_err(|e| (500, format!("Error getting visit data: {}", e)))?;
    let json_str = serde_json::to_string(&visits)
        .map_err(|e| (500, format!("Error serializing to json: {}", e)))?
        .into_bytes();
    Ok((200, json_str, "application/json; charset=utf-8"))
}

fn visits_start_end_helper(start: DateTime<Utc>, end: DateTime<Utc>) -> Result<Vec<Visit>, String> {
    let conn = Connection::connect(CONN_STR, TlsMode::None)
        .map_err(|e| format!("Error setting up connection with connection string '{}': {}", CONN_STR, e))?;

    let query = format!("
        select arrival, departure, latitude, longitude, horizontal_accuracy
        from get_visits()
        where departure >= '{start}'
            and arrival <= '{end}'",
        start = start.to_rfc3339(),
        end = end.to_rfc3339());

    let results: Vec<Visit> = conn.query(&query, &[])
        .map_err(|e| format!("Error executing query '{}': {}", query, e))?
        .iter()
        .map(|row| Visit {
            arrival: row.get::<usize, Option<DateTime<FixedOffset>>>(0),
            departure: row.get::<usize, Option<DateTime<FixedOffset>>>(1),
            latitude: row.get(2),
            longitude: row.get(3),
            horizontal_accuracy: row.get(4),
        })
        .collect();
    Ok(results)
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
    let json_str = serde_json::to_string(&visits)
        .map_err(|e| (500, format!("Error serializing to json: {}", e)))?
        .into_bytes();
    Ok((200, json_str, "application/json; charset=utf-8"))
}


#[derive(Debug, Serialize)]
pub(crate) struct TimeInMetric {
    // Abbreviated day of the week, i.e. Mon, Tue, etc.
    day_of_week: String,

    // average minutes after midnight that I start interacting with my work computer in the morning
    avg_minutes: f64,
}

fn time_in_helper() -> Result<Vec<TimeInMetric>, String> {
    let conn = Connection::connect(CONN_STR, TlsMode::None)
        .map_err(|e| format!("Error setting up connection with connection string '{}': {}", CONN_STR, e))?;

    let query = "select * from time_in()";

    let results: Vec<TimeInMetric> = conn.query(query, &[])
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
    let times = time_in_helper()
        .map_err(|e| (500, format!("Error getting time in data: {}", e)))?;
    let json_str = serde_json::to_string(&times)
        .map_err(|e| (500, format!("Error serializing to json: {}", e)))?
        .into_bytes();
    Ok((200, json_str, "application/json; charset=utf-8"))
}


#[derive(Debug, Serialize)]
pub(crate) struct ProgramUsageMetric2 {
    hour_of_day: i32,
    program: String,
    window_title: String,
    count: i32,
}

fn top_limit_helper() -> Result<Vec<ProgramUsageMetric2>, String> {
    let conn = Connection::connect(CONN_STR, TlsMode::None)
        .map_err(|e| format!("Error setting up connection with connection string '{}': {}", CONN_STR, e))?;

    let query = "select * from top_foo()";

    let results: Vec<ProgramUsageMetric2> = conn.query(query, &[])
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
    let usage = top_limit_helper()
        .map_err(|e| (500, format!("Error getting program usage: {}", e)))?;
    let json_str = serde_json::to_string(&usage)
        .map_err(|e| (500, format!("Error serializing to json: {}", e)))?
        .into_bytes();
    Ok((200, json_str, "application/json; charset=utf-8"))
}


#[derive(Debug, Serialize)]
pub(crate) struct ProgramUsageMetric {
    hour_of_day: f64,
    program: String,
    window_title: String,
    count: i64,
}

fn program_usage_by_hour_helper() -> Result<Vec<ProgramUsageMetric>, String> {
    let conn = Connection::connect(CONN_STR, TlsMode::None)
        .map_err(|e| format!("Error setting up connection with connection string '{}': {}", CONN_STR, e))?;

    let query = "select * from program_usage_by_hour()";

    let results: Vec<ProgramUsageMetric> = conn.query(query, &[])
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
    let usage = program_usage_by_hour_helper()
        .map_err(|e| (500, format!("Error getting program usage: {}", e)))?;
    let json_str = serde_json::to_string(&usage)
        .map_err(|e| (500, format!("Error serializing to json: {}", e)))?
        .into_bytes();
    Ok((200, json_str, "application/json; charset=utf-8"))
}

fn upload_visits_helper(visits_json: String) -> Result<(), String> {
    let visits: Vec<Visit> = serde_json::from_str(&visits_json)
        .map_err(|e| format!("Error parsing visit data as json: {}", e))?;
    // create a bunch of lines like:
    // ($1, $2, ..., $5)
    // ($6, $7, ..., $10)
    let param_placeholders = (0..visits.len())
        .map(|i| {
            let comma_sep = ((i*5+1)..=(i*5+5))
                .map(|p| format!("${}", p))
                .collect::<Vec<String>>()
                .join(",");
            format!("({})", comma_sep)
        })
        .collect::<Vec<String>>()
        .join(",\n");
    let query = format!(
        "insert into visits (arrival,departure,latitude,longitude,horizontal_accuracy)\n\
        values\n{}",
        param_placeholders);
    let params = visits.iter()
        .map(|v| vec![
            &v.arrival as &dyn ToSql,
            &v.departure as &dyn ToSql,
            &v.latitude as &dyn ToSql,
            &v.longitude as &dyn ToSql,
            &v.horizontal_accuracy as &dyn ToSql])
        .flatten()
        .collect::<Vec<&dyn ToSql>>();

    let conn = Connection::connect(CONN_STR, TlsMode::None)
        .map_err(|e| format!("Error setting up connection with connection string '{}': {}", CONN_STR, e))?;
    conn.execute(&query, &params)
        .map_err(|e| format!("Error executing query '{}': {}", query, e))?;

    Ok(())
}

pub(crate) fn upload_visits() -> Result<(i32, Vec<u8>, &'static str), (i32, String)> {
    let mut visits_json = String::new();
    io::stdin().read_to_string(&mut visits_json)
        .map_err(|e| (500, format!("Error reading visit data from body: {}", e)))?;
    match upload_visits_helper(visits_json) {
        Ok(_) => Ok((200, Vec::new(), "text/plain")),
        Err(e) => Err((500, format!("Error uploading visit data: {}", e)))
    }
}

fn upload_locations_helper(locations_json: String) -> Result<(), String> {
    let locations: Vec<Location> = serde_json::from_str(&locations_json)
        .map_err(|e| format!("Error parsing location data as json: {}", e))?;
    // create a bunch of lines like:
    // ($1, $2, ..., $9)
    // ($10, $11, ..., $18)
    let param_placeholders = (0..locations.len())
        .map(|i| {
            let comma_sep = ((i*9+1)..=(i*9+9))
                .map(|p| format!("${}", p))
                .collect::<Vec<String>>()
                .join(",");
            format!("({})", comma_sep)
        })
        .collect::<Vec<String>>()
        .join(",\n");
    let query = format!(
        "insert into locations (date,latitude,longitude,altitude,horizontal_accuracy,vertical_accuracy,course,speed,floor)\n\
        values\n{}",
        param_placeholders);
    let params = locations.iter()
        .map(|l| vec![
            &l.date as &dyn ToSql,
            &l.latitude as &dyn ToSql,
            &l.longitude as &dyn ToSql,
            &l.altitude as &dyn ToSql,
            &l.horizontal_accuracy as &dyn ToSql,
            &l.vertical_accuracy as &dyn ToSql,
            &l.course as &dyn ToSql,
            &l.speed as &dyn ToSql,
            &l.floor as &dyn ToSql])
        .flatten()
        .collect::<Vec<&dyn ToSql>>();

    let conn = Connection::connect(CONN_STR, TlsMode::None)
        .map_err(|e| format!("Error setting up connection with connection string '{}': {}", CONN_STR, e))?;
    conn.execute(&query, &params)
        .map_err(|e| format!("Error executing query '{}': {}", query, e))?;

    Ok(())
}

pub(crate) fn upload_locations() -> Result<(i32, Vec<u8>, &'static str), (i32, String)> {
    let mut locations_json = String::new();
    io::stdin().read_to_string(&mut locations_json)
        .map_err(|e| (500, format!("Error reading location data from body: {}", e)))?;
    match upload_locations_helper(locations_json) {
        Ok(_) => Ok((200, Vec::new(), "text/plain")),
        Err(e) => Err((500, format!("Error uploading location data: {}", e)))
    }
}

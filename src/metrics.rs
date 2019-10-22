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

pub(crate) fn locations_start_end(start: DateTime<Utc>, end: DateTime<Utc>) -> Result<Vec<Location>, String> {
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

pub(crate) fn locations() -> Result<Vec<Location>, String> {
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

pub(crate) fn visits() -> Result<Vec<Visit>, String> {
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

pub(crate) fn visits_start_end(start: DateTime<Utc>, end: DateTime<Utc>) -> Result<Vec<Visit>, String> {
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

#[derive(Debug, Serialize)]
pub(crate) struct TimeInMetric {
    // Abbreviated day of the week, i.e. Mon, Tue, etc.
    day_of_week: String,

    // average minutes after midnight that I start interacting with my work computer in the morning
    avg_minutes: f64,
}

pub(crate) fn time_in() -> Result<Vec<TimeInMetric>, String> {
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

#[derive(Debug, Serialize)]
pub(crate) struct ProgramUsageMetric2 {
    hour_of_day: i32,
    program: String,
    window_title: String,
    count: i32,
}

pub(crate) fn top_foo() -> Result<Vec<ProgramUsageMetric2>, String> {
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

#[derive(Debug, Serialize)]
pub(crate) struct ProgramUsageMetric {
    hour_of_day: f64,
    program: String,
    window_title: String,
    count: i64,
}

pub(crate) fn program_usage_by_hour() -> Result<Vec<ProgramUsageMetric>, String> {
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


pub(crate) fn upload_visits(visits_json: String) -> Result<(), String> {
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

pub(crate) fn upload_locations(locations_json: String) -> Result<(), String> {
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


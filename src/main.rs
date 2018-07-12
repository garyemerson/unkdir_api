extern crate postgres;
extern crate chrono;
extern crate serde;

#[macro_use]
extern crate serde_json;

#[macro_use]
extern crate serde_derive;

use serde_json::to_string_pretty;
// use std::time::SystemTime;
use std::env;
use postgres::{Connection, TlsMode};
use chrono::{DateTime, Utc};

const CONN_STR: &str = "postgres://Garrett@garspace.com/Garrett";

fn main() {
    let (status, json_str): (i32, String) = match env::var("PATH_INFO") {
        Ok(path) => {
            handle_request(path).unwrap_or_else(|e| e)
        },
        Err(_) => {
            (404, error("Must specify resource"))
        },
    };
    // body.push_str("\n");
    // for (key, value) in env::vars() {
    //     body.push_str(&format!("{}: {}\n", key, value));
    // }

    // let body = format!("{:?}\n\n{}", SystemTime::now(), json_str);
    let body = format!("{}\n", json_str);

    //"Status: 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}\r\n",
    let headers = [
        &format!("Status: {}", status),
        "Content-type: application/json; charset=utf-8",
        &format!("Content-Length: {}", body.len()),
        "Connection: close",
        "Access-Control-Allow-Origin: *",
    ].join("\r\n");

    print!(
        "{headers}\r\n\r\n{body}",
        headers = headers,
        body = body);
}

fn handle_request(path: String) -> Result<(i32, String), (i32, String)> {
    match path.as_ref() {
        "/top" => {
            let usage = program_usage_by_hour()
                .map_err(|e| (500, error(&format!("Error getting program usage: {}", e))))?;
            let json_str = serde_json::to_string(&usage)
                .map_err(|e| (500, format!("Error serializing to json: {}", e)))?;
            Ok((200, json_str))
        },

        "/toplimit" => {
            let usage = top_foo()
                .map_err(|e| (500, error(&format!("Error getting program usage: {}", e))))?;
            let json_str = serde_json::to_string(&usage)
                .map_err(|e| (500, format!("Error serializing to json: {}", e)))?;
            Ok((200, json_str))
        },

        "/timein" => {
            let times = time_in()
                .map_err(|e| (500, error(&format!("Error getting time in data: {}", e))))?;
            let json_str = serde_json::to_string(&times)
                .map_err(|e| (500, format!("Error serializing to json: {}", e)))?;
            Ok((200, json_str))
        },

        "/visits" => {
            let visits = visits()
                .map_err(|e| (500, error(&format!("Error getting visit data: {}", e))))?;
            let json_str = serde_json::to_string(&visits)
                .map_err(|e| (500, format!("Error serializing to json: {}", e)))?;
            Ok((200, json_str))
        }

        "/locations" => {
            let locations = locations()
                .map_err(|e| (500, error(&format!("Error getting location data: {}", e))))?;
            let json_str = serde_json::to_string(&locations)
                .map_err(|e| (500, format!("Error serializing to json: {}", e)))?;
            Ok((200, json_str))
        }

        req_path => {
            Ok((404, error(&format!("Unknown resource {}", req_path))))
        },
    }
}

fn error(msg: &str) -> String {
    to_string_pretty(&json!({"message": msg})).unwrap()
}

#[derive(Debug, Serialize)]
struct Location {
    date: String,
    latitude: f64,
    longitude: f64,
    altitude: f64,
    horizontal_accuracy: f64,
    vertical_accuracy: f64,
    course: Option<f64>,
    speed: Option<f64>,
    floor: Option<i32>,
}

fn locations() -> Result<Vec<Location>, String> {
    let conn = Connection::connect(CONN_STR, TlsMode::None)
        .map_err(|e| format!("Error setting up connection with connection string '{}': {}", CONN_STR, e))?;

    let query = "select * from get_locations()";

    let results: Vec<Location> = conn.query(query, &[])
        .map_err(|e| format!("Error executing query '{}': {}", query, e))?
        .iter()
        .map(|row| Location {
            date: row.get::<usize, Option<DateTime<Utc>>>(0).map_or("null".to_string(), |d| d.to_string()),
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

#[derive(Debug, Serialize)]
struct Visit {
    arrival: String,
    departure: String,
    latitude: f64,
    longitude: f64,
    horizontal_accuracy: f64
}

fn visits() -> Result<Vec<Visit>, String> {
    let conn = Connection::connect(CONN_STR, TlsMode::None)
        .map_err(|e| format!("Error setting up connection with connection string '{}': {}", CONN_STR, e))?;

    let query = "select * from get_visits()";

    let results: Vec<Visit> = conn.query(query, &[])
        .map_err(|e| format!("Error executing query '{}': {}", query, e))?
        .iter()
        .map(|row| Visit {
            arrival: row.get::<usize, Option<DateTime<Utc>>>(0).map_or("null".to_string(), |d| d.to_string()),
            departure: row.get::<usize, Option<DateTime<Utc>>>(1).map_or("null".to_string(), |d| d.to_string()),
            latitude: row.get(2),
            longitude: row.get(3),
            horizontal_accuracy: row.get(4),
        })
        .collect();
    Ok(results)
}

#[derive(Debug, Serialize)]
struct TimeInMetric {
    // Abbreviated day of the week, i.e. Mon, Tue, etc.
    day_of_week: String,

    // average minutes after midnight that I start interacting with my work computer in the morning
    avg_minutes: f64,
}

fn time_in() -> Result<Vec<TimeInMetric>, String> {
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
struct ProgramUsageMetric2 {
    hour_of_day: i32,
    program: String,
    window_title: String,
    count: i32,
}

fn top_foo() -> Result<Vec<ProgramUsageMetric2>, String> {
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
struct ProgramUsageMetric {
    hour_of_day: f64,
    program: String,
    window_title: String,
    count: i64,
}

fn program_usage_by_hour() -> Result<Vec<ProgramUsageMetric>, String> {
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

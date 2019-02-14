extern crate postgres;
extern crate chrono;
extern crate serde;
extern crate image;

#[macro_use]
extern crate serde_json;

#[macro_use]
extern crate serde_derive;

use chrono::{DateTime, Utc, Local, SecondsFormat};
use postgres::{Connection, TlsMode};
use serde_json::{to_string_pretty, Value};
use image::imageops::{resize, overlay /*, brighten*/};
use image::{ImageBuffer, Luma, DynamicImage, FilterType, load_from_memory};
//use image::imageops::colorops::contrast;

use std::time::SystemTime;
use std::{str, env};
use std::process::Command;
use std::fs::{self, OpenOptions};
use std::io::{self, Read, Write};
use std::path::Path;


const CONN_STR: &str = "postgres://Garrett@localhost/Garrett";
const MEME_FILE: &str = "/root/unkdir/meme_board/meme.png";
const RAW_MEME_FILE: &str = "/root/unkdir/meme_board/meme_raw.png";
const MEME_ID_FILE: &str = "/root/unkdir/meme_board/meme_id";
const BATTERY_FILE_PATH: &str = "/root/unkdir/meme_board/battery_percent";
const ARCHIVE_DIR: &str = "/root/unkdir/meme_board/archive";

#[derive(Debug, Serialize, Deserialize)]
struct Visit {
    arrival: Option<String>,
    departure: Option<String>,
    latitude: f64,
    longitude: f64,
    horizontal_accuracy: f64
}

#[derive(Debug, Serialize, Deserialize)]
struct Location {
    date: Option<String>,
    latitude: f64,
    longitude: f64,
    altitude: f64,
    horizontal_accuracy: f64,
    vertical_accuracy: f64,
    course: Option<f64>,
    speed: Option<f64>,
    floor: Option<i32>,
}

fn main() {
    let (status, body, content_type): (i32, Vec<u8>, &str) = handle_request()
        .unwrap_or_else(|e| (e.0, json_msg(&e.1).into_bytes(), "application/json; charset=utf-8"));

    let headers = [
        &format!("Status: {}", status),
        &format!("Content-type: {}", content_type),
        &format!("Content-Length: {}", body.len()),
        "Connection: close",
        // "Access-Control-Allow-Headers: Content-Type",
        // "Access-Control-Allow-Origin: *",
    ].join("\r\n");

    print!("{}\r\n\r\n", headers);
    let stdout = io::stdout();
    stdout
        .lock()
        .write(&body)
        .expect("write body to stdout");
}

fn get_request_info() -> Result<(String, String), (i32, String)> {
    let path = env::var("PATH_INFO")
        .or(Err((400, "Must specify resource".to_string())))?;
    let method = env::var("REQUEST_METHOD")
        .or(Err((400, "Must specify method".to_string())))?;
    Ok((path, method))
}

fn handle_request() -> Result<(i32, Vec<u8>, &'static str), (i32, String)> {
    let (path, method) = get_request_info()?;
    let resource: Vec<&str> = path.split('/')
        .skip(1)
        .collect();
    match method.as_ref() {
        "GET" => {
            match &resource[..] {
                ["top"] => {
                    let usage = program_usage_by_hour()
                        .map_err(|e| (500, format!("Error getting program usage: {}", e)))?;
                    let json_str = serde_json::to_string(&usage)
                        .map_err(|e| (500, format!("Error serializing to json: {}", e)))?
                        .into_bytes();
                    Ok((200, json_str, "application/json; charset=utf-8"))
                },

                ["vars"] => {
                    let mut body = format!("{:?}\n\n", SystemTime::now());
                    for (key, value) in env::vars() {
                        body.push_str(&format!("{}: {}\n", key, value));
                    }
                    Ok((200, body.into_bytes(), "text/plain"))
                }

                ["toplimit"] => {
                    let usage = top_foo()
                        .map_err(|e| (500, format!("Error getting program usage: {}", e)))?;
                    let json_str = serde_json::to_string(&usage)
                        .map_err(|e| (500, format!("Error serializing to json: {}", e)))?
                        .into_bytes();
                    Ok((200, json_str, "application/json; charset=utf-8"))
                },

                ["timein"] => {
                    let times = time_in()
                        .map_err(|e| (500, format!("Error getting time in data: {}", e)))?;
                    let json_str = serde_json::to_string(&times)
                        .map_err(|e| (500, format!("Error serializing to json: {}", e)))?
                        .into_bytes();
                    Ok((200, json_str, "application/json; charset=utf-8"))
                },

                ["visits", start, end] => {
                    let start = DateTime::parse_from_rfc3339(start)
                        .map_err(|e| (500, format!("Error parsing '{}' as start date: {}", start, e)))?
                        .with_timezone(&Utc);
                    let end = DateTime::parse_from_rfc3339(end)
                        .map_err(|e| (500, format!("Error parsing '{}' as end date: {}", end, e)))?
                        .with_timezone(&Utc);

                    let visits = visits_start_end(start, end)
                        .map_err(|e| (500, format!("Error getting visit data: {}", e)))?;
                    let json_str = serde_json::to_string(&visits)
                        .map_err(|e| (500, format!("Error serializing to json: {}", e)))?
                        .into_bytes();
                    Ok((200, json_str, "application/json; charset=utf-8"))
                },

                ["visits"] => {
                    let visits = visits()
                        .map_err(|e| (500, format!("Error getting visit data: {}", e)))?;
                    let json_str = serde_json::to_string(&visits)
                        .map_err(|e| (500, format!("Error serializing to json: {}", e)))?
                        .into_bytes();
                    Ok((200, json_str, "application/json; charset=utf-8"))
                },

                ["locations", start, end] => {
                    let start = DateTime::parse_from_rfc3339(start)
                        .map_err(|e| (500, format!("Error parsing '{}' as start date: {}", start, e)))?
                        .with_timezone(&Utc);
                    let end = DateTime::parse_from_rfc3339(end)
                        .map_err(|e| (500, format!("Error parsing '{}' as end date: {}", end, e)))?
                        .with_timezone(&Utc);

                    let locations = locations_start_end(start, end)
                        .map_err(|e| (500, format!("Error getting visit data: {}", e)))?;
                    let json_str = serde_json::to_string(&locations)
                        .map_err(|e| (500, format!("Error serializing to json: {}", e)))?
                        .into_bytes();
                    Ok((200, json_str, "application/json; charset=utf-8"))
                },

                ["locations"] => {
                    let locations = locations()
                        .map_err(|e| (500, format!("Error getting location data: {}", e)))?;
                    let json_str = serde_json::to_string(&locations)
                        .map_err(|e| (500, format!("Error serializing to json: {}", e)))?
                        .into_bytes();
                    Ok((200, json_str, "application/json; charset=utf-8"))
                },

                ["meme"] => {
                    let img_bytes = fs::read(MEME_FILE)
                        .map_err(|e| (500, format!("Error reading meme file {}: {}", MEME_FILE, e)))?;
                    Ok((200, img_bytes, "image/png"))
                },

                ["battery_history"] => {
                    let json = battery_history()
                        .map_err(|e| (500, format!("Error getting battery history: {}", e)))?;
                    Ok((200, json.into_bytes(), "application/json; charset=utf-8"))
                },

                ["foo"] => {
                    Ok((200, "bar".to_string().into_bytes(), "text/plain"))
                },

                _ => {
                    Err((400, format!("Unknown GET resource {}", path)))
                },
            }
        },

        "POST" => {
            match &resource[..] {
                ["meme_status"] => {
                    let meme_id = meme_status()
                        .map_err(|e| (500, format!("Error getting meme status: {}", e)))?;
                    Ok((200, meme_id.into_bytes(), "text/plain"))
                },

                ["update_meme_url"] => {
                    update_meme_from_url()
                        .map_err(|e| (500, format!("Error updating meme from url: {}", e)))?;
                    Ok((200, Vec::new(), "text/plain")) 
                },

                ["update_meme"] => {
                    update_meme()
                        .map_err(|e| (500, format!("Error updating meme: {}", e)))?;
                    Ok((200, Vec::new(), "text/plain"))
                },

                ["upload_visits"] => {
                    let mut visits_json = String::new();
                    io::stdin().read_to_string(&mut visits_json)
                        .map_err(|e| (500, format!("Error reading visit data from body: {}", e)))?;
                    match upload_visits(visits_json) {
                        Ok(_) => Ok((200, Vec::new(), "text/plain")),
                        Err(e) => Err((500, format!("Error uploading visit data: {}", e)))
                    }
                },

                ["upload_locations"] => {
                    let mut locations_json = String::new();
                    io::stdin().read_to_string(&mut locations_json)
                        .map_err(|e| (500, format!("Error reading location data from body: {}", e)))?;
                    match upload_locations(locations_json) {
                        Ok(_) => Ok((200, Vec::new(), "text/plain")),
                        Err(e) => Err((500, format!("Error uploading location data: {}", e)))
                    }
                },

                _ => {
                    Err((400, format!("Unknown POST resource {}", path)))
                },
            }
        },

        method => {
            Err((400, format!("Unsupported method {}", method)))
        }
    }

}

fn battery_history() -> Result<String, String> {
    let stats_raw = fs::read_to_string(BATTERY_FILE_PATH)
        .map_err(|e| format!("Error reading battery file {}: {}", BATTERY_FILE_PATH, e))?;
    let stats: Vec<Value> = stats_raw.split('\n')
        .map(|l| l.split("||"))
        .filter_map(|mut split_line| {
            if split_line.clone().filter(|e| e.len() != 0).count() != 2 {
                return None;
            }
            let date = split_line.next().expect("first elem for date");
            let percent = split_line.next()
                .expect("second elem for percent")
                .parse::<i32>();
            match percent {
                Ok(percent) => Some(json!({"date": date, "percent": percent})),
                Err(_) => None
            }
        })
        .collect();

    serde_json::to_string(&stats)
        .map_err(|e| format!("Error converting to json string: {}", e))
}

fn meme_status() -> Result<String, String> {
    let mut battery_percent = String::new();
    match io::stdin().read_to_string(&mut battery_percent) {
        Ok(_) => {
            save_battery_percentage(battery_percent)
                .unwrap_or_else(|e| log_error(&format!("Error saving battery percentage: {}", e)));
        },
        Err(e) => {
            log_error(&format!("Error reading battery percent from body from stdin: {}", e));
        }
    }

    let meme_id = fs::read_to_string(MEME_ID_FILE)
        .map_err(|e| format!("Error reading meme_id file {}: {}", MEME_ID_FILE, e))?;

    Ok(meme_id)
}

fn update_meme() -> Result<(), String> {
    let mut img_bytes = Vec::new();
    io::stdin().read_to_end(&mut img_bytes)
        .map_err(|e| format!("Error reading img bytes from stdin: {}", e))?;
    update_meme_from_bytes(img_bytes)?;

    Ok(())
}

fn update_meme_from_url() -> Result<(), String> {
    let mut url_bytes = Vec::new();
    io::stdin().read_to_end(&mut url_bytes)
        .map_err(|e| format!("Error reading url bytes from stdin: {}", e))?;
    let url = str::from_utf8(&url_bytes)
        .map_err(|e| format!("Error parsing POST data as utf8 string: {}", e))?;

    let img_bytes = Command::new("curl")
        .arg("--location")
        .arg(url)
        .output()
        .map_err(|e| format!("Error excuting curl on url '{}': {}", url, e))?
        .stdout;

    update_meme_from_bytes(img_bytes)
        .map_err(|e| format!("Error updating meme from url bytes: {}", e))?;

    Ok(())
}

fn update_meme_from_bytes(img_bytes: Vec<u8>) -> Result<(), String> {
    let img = load_from_memory(&img_bytes)
        .map_err(|e| format!("Error loading img from buffer with length {}: {}", img_bytes.len(), e))?;
    img.save(RAW_MEME_FILE)
        .map_err(|e| format!("Error saving raw img to file {}: {}", RAW_MEME_FILE, e))?;
    archive_meme(&img)
        .unwrap_or_else(|e| log_error(&format!("Error archiving meme: {}", e)));
    let formatted_img = format_img_for_kindle(img);
    formatted_img.save(MEME_FILE)
        .map_err(|e| format!("Error saving img to file {}: {}", MEME_FILE, e))?;

    let meme_id_raw = fs::read_to_string(MEME_ID_FILE)
        .map_err(|e| format!("Error reading meme_id file {}: {}", MEME_ID_FILE, e))?;
    let meme_id = meme_id_raw
        .trim()
        .parse::<i32>()
        .map_err(|e| format!("Error parsing '{}' as i32: {}", meme_id_raw, e))?;
    fs::write(MEME_ID_FILE, (meme_id + 1).to_string().into_bytes())
        .map_err(|e| format!("Error updating and saving meme id to file {}: {}", MEME_ID_FILE, e))?;

    Ok(())
}

fn archive_meme(img: &DynamicImage) -> Result<(), String> {
    let timestamp  = Utc::now()
        .to_rfc3339_opts(SecondsFormat::Secs, true)
        .replace([':', '-'].as_ref(), "");
    let filepath = if let Ok(ip) = env::var("REMOTE_ADDR") {
        format!("{dir}/{time}-{ip}.png", dir = ARCHIVE_DIR, time = timestamp, ip = ip)
    } else {
        format!( "{dir}/{time}.png", dir = ARCHIVE_DIR, time = timestamp)
    };
    if Path::new(&filepath).exists() {
        return Err("filename already exists".to_string());
    }
    img.save(&filepath)
        .map_err(|e| format!("Error saving raw img to archive file {}: {}", filepath, e))
}

fn upload_visits(visits_json: String) -> Result<(), String> {
    let visits: Vec<Visit> = serde_json::from_str(&visits_json)
        .map_err(|e| format!("Error parsing visit data as json: {}", e))?;
    let query = format!(
        "insert into visits (arrival,departure,latitude,longitude,horizontal_accuracy)\n\
        values\n{}",
        visits.into_iter()
            .map(|v| format!(
                "({},{},{},{},{})",
                if let Some(a) = v.arrival { format!("'{}'", a) } else { "NULL".to_string() },
                if let Some(d) = v.departure { format!("'{}'", d) } else { "NULL".to_string() },
                v.latitude,
                v.longitude,
                v.horizontal_accuracy))
            .collect::<Vec<String>>()
            .join(",\n"));

    let conn = Connection::connect(CONN_STR, TlsMode::None)
        .map_err(|e| format!("Error setting up connection with connection string '{}': {}", CONN_STR, e))?;
    conn.execute(&query, &[])
        .map_err(|e| format!("Error executing query '{}': {}", query, e))?;

    Ok(())
}

fn upload_locations(locations_json: String) -> Result<(), String> {
    let locations: Vec<Location> = serde_json::from_str(&locations_json)
        .map_err(|e| format!("Error parsing location data as json: {}", e))?;
    let query = format!(
        "insert into locations (date,latitude,longitude,altitude,horizontal_accuracy,vertical_accuracy,course,speed,floor)\n\
        values\n{}",
        locations.into_iter()
            .map(|l| format!(
                "({},{},{},{},{},{},{},{},{})",
                if let Some(d) = l.date { format!("'{}'", d) } else { "NULL".to_string() },
                l.latitude,
                l.longitude,
                l.altitude,
                l.horizontal_accuracy,
                l.vertical_accuracy,
                l.course.map(|c| c.to_string()).unwrap_or("NULL".to_string()),
                l.speed.map(|s| s.to_string()).unwrap_or("NULL".to_string()),
                l.floor.map(|f| f.to_string()).unwrap_or("NULL".to_string())))
            .collect::<Vec<String>>()
            .join(",\n"));

    let conn = Connection::connect(CONN_STR, TlsMode::None)
        .map_err(|e| format!("Error setting up connection with connection string '{}': {}", CONN_STR, e))?;
    conn.execute(&query, &[])
        .map_err(|e| format!("Error executing query '{}': {}", query, e))?;

    Ok(())
}

fn format_img_for_kindle(dyn_img: DynamicImage) -> ImageBuffer<Luma<u8>, Vec<u8>> {
    let width = 768;
    let height = 1024;
    let /*mut*/ img = dyn_img.to_luma();
    // img = brighten(&img, 50);
    // img = contrast(&img, 30.0);
    let mut final_img: ImageBuffer<Luma<u8>, Vec<u8>> = img.clone();
    if img.width() != width || img.height() != height {
        let img_ratio = img.width() as f32 / img.height() as f32;
        let scr_ratio = width as f32 / height as f32;
        if img_ratio > scr_ratio {
            // img wider than screen
            let ratio: f32 = width as f32 / img.width() as f32;
            let new_width = width;
            let new_height = (img.height() as f32 * ratio) as u32;

            // log(&format!("resizing to {}w x {}h to fit to screen size {}w x {}h", new_width, new_height, width, height));
            let resized_img = resize(&img, new_width, new_height, FilterType::CatmullRom);

            // overlaying onto black background and centering vertically
            let vertical_padding = ((height - new_height) as f32 / 2.0) as u32;
            final_img = ImageBuffer::from_pixel(width, height, Luma([0]));
            overlay(&mut final_img, &resized_img, 0, vertical_padding);
        } else {
            // img taller than screen
            let ratio: f32 = height as f32 / img.height() as f32;
            let new_width = (img.width() as f32 * ratio) as u32;
            let new_height = height;

            // log(&format!("resizing to {}w x {}h to fit to screen size {}w x {}h", new_width, new_height, width, height));
            let resized_img = resize(&img, new_width, new_height, FilterType::CatmullRom);

            // overlaying onto black background and centering horizontally
            let horizontal_padding = ((width - new_width) as f32 / 2.0) as u32;
            final_img = ImageBuffer::from_pixel(width, height, Luma([0]));
            overlay(&mut final_img, &resized_img, horizontal_padding, 0);
        };
    }

    final_img
}

fn save_battery_percentage(battery_percent: String) -> Result<(), String> {
    let mut battery_file = OpenOptions::new()
        .append(true)
        .open(BATTERY_FILE_PATH)
        .map_err(|e| format!("Error opening file {} : {}", BATTERY_FILE_PATH, e))?;
    let bytes = format!("{}||{}\n", Local::now(), battery_percent).into_bytes();
    battery_file.write_all(&bytes)
        .map_err(|e| format!("Error writing to file {}: {}", BATTERY_FILE_PATH, e))?;

    Ok(())
}

fn log_error(msg: &str) {
    eprintln!(
        "[{}] [cgi: metrics_api] {}",
        Local::now(),
        msg);
}

fn json_msg(msg: &str) -> String {
    to_string_pretty(&json!({"message": msg})).unwrap()
}

fn locations_start_end(start: DateTime<Utc>, end: DateTime<Utc>) -> Result<Vec<Location>, String> {
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
            date: row.get::<usize, Option<DateTime<Utc>>>(0).map(|d| d.to_string()),
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

fn locations() -> Result<Vec<Location>, String> {
    let conn = Connection::connect(CONN_STR, TlsMode::None)
        .map_err(|e| format!("Error setting up connection with connection string '{}': {}", CONN_STR, e))?;

    let query = "select * from get_locations()";

    let results: Vec<Location> = conn.query(query, &[])
        .map_err(|e| format!("Error executing query '{}': {}", query, e))?
        .iter()
        .map(|row| Location {
            date: row.get::<usize, Option<DateTime<Utc>>>(0).map(|d| d.to_string()),
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

fn visits() -> Result<Vec<Visit>, String> {
    let conn = Connection::connect(CONN_STR, TlsMode::None)
        .map_err(|e| format!("Error setting up connection with connection string '{}': {}", CONN_STR, e))?;

    let query = "select * from get_visits()";

    let results: Vec<Visit> = conn.query(query, &[])
        .map_err(|e| format!("Error executing query '{}': {}", query, e))?
        .iter()
        .map(|row| Visit {
            arrival: row.get::<usize, Option<DateTime<Utc>>>(0).map(|d| d.to_string()),
            departure: row.get::<usize, Option<DateTime<Utc>>>(1).map(|d| d.to_string()),
            latitude: row.get(2),
            longitude: row.get(3),
            horizontal_accuracy: row.get(4),
        })
        .collect();
    Ok(results)
}

fn visits_start_end(start: DateTime<Utc>, end: DateTime<Utc>) -> Result<Vec<Visit>, String> {
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
            arrival: row.get::<usize, Option<DateTime<Utc>>>(0).map(|d| d.to_string()),
            departure: row.get::<usize, Option<DateTime<Utc>>>(1).map(|d| d.to_string()),
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

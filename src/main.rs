extern crate postgres;
extern crate chrono;
extern crate serde;
extern crate image;

#[macro_use]
extern crate serde_json;

#[macro_use]
extern crate serde_derive;

use chrono::{DateTime, Local, Utc, SecondsFormat};
use chrono::offset::FixedOffset;
use postgres::{Connection, TlsMode};
use postgres::types::ToSql;
use serde_json::{to_string_pretty, Value};
use image::imageops::{resize, overlay /*, brighten*/};
use image::{Pixel, GenericImageView, ImageBuffer, Luma, DynamicImage, FilterType, load_from_memory};
use image::ImageOutputFormat;
use image::png::PNGEncoder;
//use image::imageops::colorops::contrast;

use std::time::SystemTime;
use std::{str, env};
use std::process::{Command, Stdio};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::Path;
use std::str::Split;


const CONN_STR: &str = "postgres://Garrett@localhost/Garrett";
const KINDLE_MEME_FILE: &str = "/root/unkdir/meme_board/meme.png";
const RAW_MEME_FILE: &str = "/root/unkdir/meme_board/meme_raw.png";
const WEB_COMPRESSED_MEME_FILE: &str = "/root/unkdir/meme_board/meme_compressed.png";
const MEME_ID_FILE: &str = "/root/unkdir/meme_board/meme_id";
const BATTERY_FILE_PATH: &str = "/root/unkdir/meme_board/battery_percent";
const ARCHIVE_DIR: &str = "/root/unkdir/meme_board/archive";
const NOTES_FILE: &str = "/root/unkdir/doc_root/notes/contents";

#[derive(Debug, Serialize, Deserialize)]
struct Visit {
    arrival: Option<DateTime<FixedOffset>>,
    departure: Option<DateTime<FixedOffset>>,
    latitude: f64,
    longitude: f64,
    horizontal_accuracy: f64
}

#[derive(Debug, Serialize, Deserialize)]
struct Location {
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
                    let img_bytes = fs::read(KINDLE_MEME_FILE)
                        .map_err(|e| (500, format!("Error reading meme file {}: {}", KINDLE_MEME_FILE, e)))?;
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
                    let response_bytes = meme_status()
                        .map_err(|e| (500, format!("Error getting meme status: {}", e)))?;
                    Ok((200, response_bytes, "application/octet-stream"))
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

                ["update_notes"] => {
                    update_notes()
                        .map_err(|e| (500, format!("Error updating notes: {}", e)))?;
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

fn update_notes() -> Result<(), String> {
    let mut notes_bytes = Vec::new();
    io::stdin().read_to_end(&mut notes_bytes)
        .map_err(|e| format!("Error reading notes bytes from stdin: {}", e))?;
    let notes = str::from_utf8(&notes_bytes)
        .map_err(|e| format!("Error parsing POST data as utf8 string: {}", e))?;
    fs::write(NOTES_FILE, notes.to_string().into_bytes())
        .map_err(|e| format!("Error writing notes to file {}: {}", NOTES_FILE, e))
}

fn battery_history() -> Result<String, String> {
    let stats_raw = fs::read_to_string(BATTERY_FILE_PATH)
        .map_err(|e| format!("Error reading battery file {}: {}", BATTERY_FILE_PATH, e))?;
    //return Ok(stats_raw);
    let stats: Vec</*String*/Value> = stats_raw.split('\n')
        .rev()
        .take(10_000)
        .map(|l: &str| l.split("||"))
        .filter_map(|mut split_line: Split<'_, &str>| {
            let date = split_line.next()?;
            let percent = split_line.next()?.parse::<i32>().ok()?;
            //Some(format!("{{\"date\": {}, \"percent\": {}}}", date, percent))
            Some(json!({"date": date, "percent": percent}))
        })
        .collect();

    //Ok(format!("[{}]", stats.join(",")))
    serde_json::to_string(&stats)
        .map_err(|e| format!("Error converting to json string: {}", e))
}

fn meme_status() -> Result<Vec<u8>, String> {
    let mut battery_percent_and_meme_id = String::new();
    io::stdin().read_to_string(&mut battery_percent_and_meme_id)
        .map_err(|e| format!("Error reading battery percent from body from stdin: {}", e))?;
    let mut parts = battery_percent_and_meme_id.split(' ');
    let battery_percent = parts.next().ok_or("Expected chunk for battery_percent but got nothing")?;
    let kindle_meme_id_str = parts.next()
        .ok_or("Expected chunk for kindle_meme_id but got nothing")?;
    let kindle_meme_id = if kindle_meme_id_str.len() == 0 {
        None
    } else {
        let id = kindle_meme_id_str
            .parse::<i32>()
            .map_err(|e| format!("Error parseing '{}' as i32 for kindle meme id: {}", kindle_meme_id_str, e))?;
        Some(id)
    };

    save_battery_percentage(battery_percent.to_string())
        .unwrap_or_else(|e| log_error(&format!("Error saving battery percentage: {}", e)));

    let server_meme_id = fs::read_to_string(MEME_ID_FILE)
        .map_err(|e| format!("Error reading meme id file {}: {}", MEME_ID_FILE, e))?
        .trim()
        .parse::<i32>()
        .map_err(|e| format!("Error parseing server meme id to i32: {}", e))?;

    let mut response_bytes: Vec<u8> = Vec::new();
    response_bytes.append(&mut format!("{}\n", server_meme_id).as_bytes().to_vec());
    if kindle_meme_id.is_none() || kindle_meme_id.expect("kindle_meme_id") != server_meme_id {
        let mut img_bytes = fs::read(KINDLE_MEME_FILE)
            .map_err(|e| format!("Error reading meme file {}: {}", KINDLE_MEME_FILE, e))?;
        response_bytes.append(&mut img_bytes);
    }

    Ok(response_bytes)
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

    let formatted_img = format_img_for_kindle(&img);
    let mut png_bytes = Vec::new();
    PNGEncoder::new(&mut png_bytes)
        .encode(&formatted_img, formatted_img.width(), formatted_img.height(), <Luma<u8> as Pixel>::COLOR_TYPE)
        .map_err(|e| format!("Error encoding formatted_img to png: {}", e))?;
    let mut child = Command::new("convert")
        .arg("-auto-gamma")
        .arg("-auto-level")
        .arg("-normalize")
        .arg("png:-")
        .arg("png:-")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .map_err(|e| format!("Error starting pngquant cmd: {}", e))?;
    child
        .stdin
        .as_mut()
        .ok_or("Unable to get stdin for child convert process".to_string())?
        .write_all(&png_bytes)
        .map_err(|e| format!("Error writing img bytes to convert process stdin: {}", e))?;
    let output = child.wait_with_output()
        .map_err(|e| format!("Error reading stdout of convert: {}", e))?
        .stdout;
    File::create(KINDLE_MEME_FILE)
        .map_err(|e| format!("Error creating file kindle meme file: {}", e))?
        .write_all(&output)
        .map_err(|e| format!("Error writing bytes to kindle meme file: {}", e))?;

    compress_meme(&img)
        .map_err(|e| format!("Error compressing img: {}", e))?;
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

fn compress_meme(img: &DynamicImage) -> Result<(), String> {
    let mut resized_img: DynamicImage = img.clone();
    if resized_img.width() > 600 {
        let width = 400;
        let height = ((400.0 / resized_img.width() as f32) * (resized_img.height() as f32)) as u32;
        resized_img = resized_img.resize(width, height, FilterType::CatmullRom);
    }
    let mut img_bytes = Vec::new();
    resized_img.write_to(&mut img_bytes, ImageOutputFormat::PNG)
        .map_err(|e| format!("Error writing resized bytes to buffer: {}", e))?;
    let mut child = Command::new("pngquant")
        .arg("-")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .map_err(|e| format!("Error starting pngquant cmd: {}", e))?;
    {
        let stdin = child.stdin.as_mut()
            .ok_or("Unable to get stdin for child pngquant process".to_string())?;
        stdin.write_all(&img_bytes)
            .map_err(|e| format!("Error writing img bytes to pngquant process stdin: {}", e))?;
    }
    let output = child.wait_with_output()
        .map_err(|e| format!("Error reading stdout of pngquant: {}", e))?
        .stdout;
    File::create(WEB_COMPRESSED_MEME_FILE)
        .map_err(|e| format!("Error creating file compressed meme file: {}", e))?
        .write_all(&output)
        .map_err(|e| format!("Error writing compressed bytes to file: {}", e))
}

fn archive_meme(img: &DynamicImage) -> Result<(), String> {
    let timestamp  = Utc::now()
        .to_rfc3339_opts(SecondsFormat::Secs, true)
        .replace([':', '-'].as_ref(), "");
    let filepath = if let Ok(ip) = env::var("REMOTE_ADDR") {
        format!("{dir}/{time}-{ip}.png", dir = ARCHIVE_DIR, time = timestamp, ip = ip)
    } else {
        format!("{dir}/{time}.png", dir = ARCHIVE_DIR, time = timestamp)
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

fn upload_locations(locations_json: String) -> Result<(), String> {
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

fn format_img_for_kindle(dyn_img: &DynamicImage) -> ImageBuffer<Luma<u8>, Vec<u8>> {
    let width = 768;
    let height = 1024;
    let /*mut*/ img = dyn_img.to_luma();
    // img = brighten(&img, 50);
    // img = contrast(&img, 30.0);
    let mut final_img: ImageBuffer<Luma<u8>, Vec<u8>> = img.clone();
    if img.width() != width || img.height() != height {
        let img_ratio = img.width() as f32 / img.height() as f32;
        let screen_ratio = width as f32 / height as f32;
        if img_ratio > screen_ratio {
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
        Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
        msg);
}

fn json_msg(msg: &str) -> String {
    let mut s = to_string_pretty(&json!({"message": msg})).unwrap();
    s.retain(|c| c != '\n');
    s
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

fn locations() -> Result<Vec<Location>, String> {
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

fn visits() -> Result<Vec<Visit>, String> {
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

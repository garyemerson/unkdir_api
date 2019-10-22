use serde_json::{to_string_pretty, json};

use chrono::{DateTime, Utc, SecondsFormat};

use std::time::SystemTime;
use std::{str, env};
use std::fs;
use std::io::{self, Read, Write};

use metrics::program_usage_by_hour;
use metrics::top_foo;
use metrics::time_in;
use metrics::visits_start_end;
use metrics::visits;
use metrics::locations_start_end;                                                                                                              
use metrics::locations;                                                                                                                        
use metrics::upload_visits;                                                                                                                    
use metrics::upload_locations;

use meme::battery_history;                                                                                                                     
use meme::meme_status;
use meme::update_meme_from_url;
use meme::update_meme;
// use meme::KINDLE_MEME_FILE;

mod meme;
mod metrics;


const NOTES_FILE: &str = "/root/unkdir/doc_root/notes/contents";

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

                // ["meme"] => {
                //     let img_bytes = fs::read(KINDLE_MEME_FILE)
                //         .map_err(|e| (500, format!("Error reading meme file {}: {}", KINDLE_MEME_FILE, e)))?;
                //     Ok((200, img_bytes, "image/png"))
                // },

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

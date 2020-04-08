use serde_json::{to_string_pretty, json};

use chrono::{DateTime, Utc, SecondsFormat};

use std::time::{SystemTime, Duration};
use std::{str, env};
use std::{fs, thread};
use std::io::{self, Read, Write};
use std::process::Command;

use metrics::{program_usage_by_hour, top_foo, time_in, visits_start_end, visits,
    locations_start_end, locations, upload_visits, upload_locations};
use meme::{battery_history, meme_status, update_meme_from_url, update_meme};

macro_rules! log_error {
    ($($tts:tt)*) => {
        eprintln!(
            "[{}] [cgi: unkdir_api] {}",
            Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
            format!($($tts)*));
    }
}

mod meme;
mod metrics;

const NOTES_DIR: &str = "/root/unkdir/doc_root/notes";
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

                ["sleep", num_secs_str] => {
                    let num_secs = num_secs_str.parse::<u64>()
                        .map_err(|e| (500, format!("Error parsing '{}' as u64: {}", num_secs_str, e)))?;
                    thread::sleep(Duration::from_secs(num_secs));
                    Ok((200, format!("{:?}", SystemTime::now()).into_bytes(), "text/plain"))
                }

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
        .map_err(|e| format!("Error writing notes to file {}: {}", NOTES_FILE, e))?;
    env::set_current_dir(NOTES_DIR)
        .map_err(|e| format!("Error changing directory to {}: {}", NOTES_DIR, e))?;
    env::set_var("GIT_DIR", "history");
    env::set_var("GIT_WORK_TREE", ".");
    let status_output = Command::new("git")
        .arg("status")
        .arg("--porcelain")
        .arg(NOTES_FILE)
        .output()
        .map_err(|e| format!("Error excuting git status on file {}: {}", NOTES_FILE, e))?;
    log_error!("status stdout: '{}'", str::from_utf8(&status_output.stdout).unwrap());
    log_error!("status stderr: '{}'", str::from_utf8(&status_output.stderr).unwrap());
    if status_output.stdout.as_slice().starts_with(b" M") {
        let commit_output = Command::new("git")
            .arg("commit")
            .arg("--allow-empty-message")
            .arg("-am")
            .arg("")
            .output()
            .map_err(|e| format!("Error excuting git commit: {}", e))?;
        log_error!("commit stdout: '{}'", str::from_utf8(&commit_output.stdout).unwrap());
        log_error!("commit stderr: '{}'", str::from_utf8(&commit_output.stderr).unwrap());
    }

    Ok(())
}

fn json_msg(msg: &str) -> String {
    let mut s = to_string_pretty(&json!({"message": msg})).unwrap();
    s.retain(|c| c != '\n');
    s
}

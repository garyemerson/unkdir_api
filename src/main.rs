use chrono::{Utc, SecondsFormat};
use meme::{battery_history, meme_status, update_meme_from_url, update_meme};
use metrics::{program_usage_by_hour, top_limit, time_in, visits_start_end, visits,
    locations_start_end, locations, upload_visits, upload_locations};
use std::{fs, thread};
use std::{str, env};
use std::io::{self, Read, Write};
use std::ops::Index;
use std::process::Command;
use std::time::{SystemTime, Duration};
use file_lock::{FileLock, FileOptions};
use rand::Rng;
use rand::distributions::Alphanumeric;


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
const NOTES_LOCK_FILE: &str = "/root/unkdir/doc_root/notes/lock";

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
    io::stdout()
        .lock()
        .write_all(&body)
        .expect("write body to stdout");
}

fn get_request_info() -> Result<(String, String), (i32, String)> {
    let path = env::var("PATH_INFO")
        .or(Err((400, "Must specify resource".to_string())))?;
    let method = env::var("REQUEST_METHOD")
        .or(Err((400, "Must specify method".to_string())))?;
    Ok((path, method))
}

// TODO:
// - result with requestsuccess and requesterror strtuctures
// - form implementation so functions can use ? operator but not need to explicitly return tuple will 500 status
//  - alternatively: could make better helper functions structure
// ---
// returns: Result<(status, body, content/mime type), (status, error msg)>
fn handle_request() -> Result<(i32, Vec<u8>, &'static str), (i32, String)> {
    let (path, method) = get_request_info()?;
    let resource: Vec<&str> = path.split('/')
        .skip(1)
        .collect();
    match method.as_ref() {
        "GET" => {
            match &resource[..] {
                ["top"] => { program_usage_by_hour() },
                ["toplimit"] => { top_limit() },
                ["timein"] => { time_in() },
                ["visits", start, end] => { visits_start_end(start, end) },
                ["visits"] => { visits() },
                ["locations", start, end] => { locations_start_end(start, end) },
                ["locations"] => { locations() },
                ["battery_history"] => { battery_history() },

                ["sleep", num_secs_str] => {
                    let num_secs = num_secs_str.parse::<u64>()
                        .map_err(|e| (500, format!("Error parsing '{}' as u64: {}", num_secs_str, e)))?;
                    thread::sleep(Duration::from_secs(num_secs));
                    Ok((200, format!("{:?}", SystemTime::now()).into_bytes(), "text/plain"))
                }

                ["vars"] => {
                    let mut body = format!("{:?}\n\n", SystemTime::now());
                    for (key, value) in env::vars() {
                        let json_str = json::stringify(json::JsonValue::String(value));
                        body.push_str(&format!("{}: {}\n", key, json_str.trim_matches('"')));
                    }
                    Ok((200, body.into_bytes(), "text/plain"))
                }

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
                ["meme_status"] => { meme_status() },
                ["update_meme_url"] => { update_meme_from_url() },
                ["update_meme"] => { update_meme() },
                ["update_notes"] => { update_notes() },
                ["update_notes_json"] => { update_notes_json() },
                ["upload_visits"] => { upload_visits() },
                ["upload_locations"] => { upload_locations() },

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

fn update_notes_helper() -> Result<(), String> {
    let filelock = FileLock::lock(&NOTES_LOCK_FILE, /*is_blocking*/ true, FileOptions::new().write(true))
        .map_err(|e| format!("Error locking notes lock file: {}", e))?;
    let mut notes_bytes = Vec::new();
    io::stdin().read_to_end(&mut notes_bytes)
        .map_err(|e| format!("Error reading notes bytes from stdin: {}", e))?;
    log_error!("notes update with {} bytes", notes_bytes.len());
    let notes = str::from_utf8(&notes_bytes)
        .map_err(|e| format!("Error parsing POST data as utf8 string: {}", e))?;

    let mut rng = rand::thread_rng();
    let rand_suffix: String = (0..5).map(|_| rng.sample(Alphanumeric) as char).collect();
    let tmp_file = format!("{}.{}", NOTES_FILE, rand_suffix);

    fs::write(&tmp_file, notes.to_string().into_bytes())
        .map_err(|e| format!("Error writing notes to tmp file {}: {}", &tmp_file, e))?;
    fs::rename(&tmp_file, NOTES_FILE)
        .map_err(|e| format!("Error renaming tmp file {} to notes file {}: {}", &tmp_file, NOTES_FILE, e))?;
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
    log_error!("status stdout: '{}'", str::from_utf8(&status_output.stdout).unwrap_or("<error convert stdout to utf8>"));
    log_error!("status stderr: '{}'", str::from_utf8(&status_output.stderr).unwrap_or("<error convert stderr to utf8>"));
    if status_output.stdout.as_slice().starts_with(b" M") {
        let commit_output = Command::new("git")
            .arg("commit")
            .arg("--allow-empty-message")
            .arg("-am")
            .arg("")
            .output()
            .map_err(|e| format!("Error excuting git commit: {}", e))?;
        log_error!("commit stdout: '{}'", str::from_utf8(&commit_output.stdout).unwrap_or("<error convert stdout to utf8>"));
        log_error!("commit stderr: '{}'", str::from_utf8(&commit_output.stderr).unwrap_or("<error convert stderr to utf8>"));
    }

    Ok(())
}

fn update_notes() -> Result<(i32, Vec<u8>, &'static str), (i32, String)> {
    update_notes_helper()
        .map_err(|e| (500, format!("Error updating notes: {}", e)))?;
    Ok((200, Vec::new(), "text/plain"))
}

fn update_notes_json_helper() -> Result<(), String> {
    let filelock = FileLock::lock(&NOTES_LOCK_FILE, /*is_blocking*/ true, FileOptions::new().write(true))
        .map_err(|e| format!("Error locking notes lock file: {}", e))?;
    let mut notes_bytes = Vec::new();
    io::stdin().read_to_end(&mut notes_bytes)
        .map_err(|e| format!("Error reading notes bytes from stdin: {}", e))?;
    log_error!("notes update with {} bytes", notes_bytes.len());
    let notes_json = str::from_utf8(&notes_bytes)
        .map_err(|e| format!("Error parsing POST data as utf8 string: {}", e))?;
    let notes: String = json::parse(notes_json)
        .map_err(|e| format!("Error parsing json: {}", e))?
        .index("notes")
        .as_str()
        .ok_or_else(|| format!("notes key not found in json object"))?
        .to_string();

    let mut rng = rand::thread_rng();
    let rand_suffix: String = (0..5).map(|_| rng.sample(Alphanumeric) as char).collect();
    let tmp_file = format!("{}.{}", NOTES_FILE, rand_suffix);
    fs::write(&tmp_file, notes.into_bytes())
        .map_err(|e| format!("Error writing notes to tmp file {}: {}", &tmp_file, e))?;
    fs::rename(&tmp_file, NOTES_FILE)
        .map_err(|e| format!("Error renaming tmp file {} to notes file {}: {}", &tmp_file, NOTES_FILE, e))?;

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
    log_error!("status stdout: '{}'", str::from_utf8(&status_output.stdout).unwrap_or("<error convert stdout to utf8>"));
    log_error!("status stderr: '{}'", str::from_utf8(&status_output.stderr).unwrap_or("<error convert stderr to utf8>"));
    if status_output.stdout.as_slice().starts_with(b" M") {
        let commit_output = Command::new("git")
            .arg("commit")
            .arg("--allow-empty-message")
            .arg("-am")
            .arg("")
            .output()
            .map_err(|e| format!("Error excuting git commit: {}", e))?;
        log_error!("commit stdout: '{}'", str::from_utf8(&commit_output.stdout).unwrap_or("<error convert stdout to utf8>"));
        log_error!("commit stderr: '{}'", str::from_utf8(&commit_output.stderr).unwrap_or("<error convert stderr to utf8>"));
    }

    Ok(())
}

fn update_notes_json() -> Result<(i32, Vec<u8>, &'static str), (i32, String)> {
    update_notes_helper()
        .map_err(|e| (500, format!("Error updating notes via json: {}", e)))?;
    Ok((200, Vec::new(), "text/plain"))
}

fn json_msg(msg: &str) -> String {
    let mut s = json::stringify(json::object!{message: msg});
    s.retain(|c| c != '\n');
    s
}

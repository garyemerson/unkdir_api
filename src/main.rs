use chrono::{Utc, SecondsFormat};
use file_lock::{FileLock, FileOptions};
use meme::{battery_history, meme_status, update_meme_from_url, update_meme};
use metrics::computer_activity::{program_usage_by_hour, top_limit, time_in};
use rand::distributions::Alphanumeric;
use rand::Rng;
use std::{fs, thread, str, env};
use std::io::{self, Read, Write};
use std::ops::Index;
use std::process::Command;
use std::time::{SystemTime, Duration};


macro_rules! log_error {
    ($($tts:tt)*) => {
        eprintln!(
            "[{}] [cgi: unkdir_api] {}",
            Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
            format!($($tts)*))
    }
}

macro_rules! try_block {
    { $($token:tt)* } => {{
        (|| {
            $($token)*
        })()
    }}
}

mod meme;
mod metrics;

const NOTES_DIR: &str = "/root/unkdir/doc_root/notes";
const NOTES_FILE: &str = "/root/unkdir/doc_root/notes/contents";
const NOTES_LOCK_FILE: &str = "/root/unkdir/doc_root/notes/lock";

fn main() {
    let (http_code, body, content_type): (i32, Vec<u8>, &str) = match handle_request() {
        Ok(x) => x,
        Err((http_code, err_msg)) => {
            log_error!(
                "Request '{}:{}' failed with http code {}: {}",
                env::var("REQUEST_METHOD").unwrap_or("?".to_string()),
                env::var("PATH_INFO").unwrap_or("?".to_string()),
                http_code,
                err_msg);
            (http_code, json::object!{message: err_msg}.to_string().into_bytes(), "application/json; charset=utf-8")
        }
    };

    let headers = [
        &format!("Status: {}", http_code) as &str,
        &format!("Content-type: {}", content_type) as &str,
        &format!("Content-Length: {}", body.len()) as &str,
        // "Connection: close",
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
                ["update_notes_incremental"] => { update_notes_incremental() },

                // ["test"] => {
                //     log_error!("in test api");
                //     try_block! {
                //         let mut input_bytes = Vec::new();
                //         io::stdin().read_to_end(&mut input_bytes)
                //             .map_err(|e| format!("Error reading notes bytes from stdin: {}", e))?;
                //         log_error!("{} input bytes", input_bytes.len());
                //         let notes_json = str::from_utf8(&input_bytes)
                //             .map_err(|e| format!("Error parsing body as utf8 string: {}", e))?;
                //         let notes: String = json::parse(notes_json)
                //             .map_err(|e| format!("Error parsing json: {}", e))?
                //             .index("notes")
                //             .as_str()
                //             .ok_or_else(|| format!("notes key not found in json object"))?
                //             .to_string();
                //         log_error!("notes: {}", notes);
                //         log_error!("notes naive len: {}", notes.len());
                //         log_error!("notes chars len: {}", notes.chars().count());
                //         Ok(())
                //     }.map_err(|e: String| (500, format!("Error in test api: {}", e)))?;
                //     Ok((200, Vec::new(), "text/plain"))
                // }

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

fn update_notes() -> Result<(i32, Vec<u8>, &'static str), (i32, String)> {
    try_block! {
        let _filelock = FileLock::lock(&NOTES_LOCK_FILE, /*is_blocking*/ true, FileOptions::new().write(true))
            .map_err(|e| format!("Error locking notes lock file: {}", e))?;
        let mut notes_bytes = Vec::new();
        io::stdin().read_to_end(&mut notes_bytes)
            .map_err(|e| format!("Error reading notes bytes from stdin: {}", e))?;
        log_error!("notes update with {} bytes", notes_bytes.len());
        let notes = str::from_utf8(&notes_bytes)
            .map_err(|e| format!("Error parsing POST data as utf8 string: {}", e))?;

        update_notes_file_and_git_history(notes)
    }.map_err(|e: String| (500, format!("Error updating notes: {}", e)))?;
    Ok((200, Vec::new(), "text/plain"))
}

fn update_notes_json() -> Result<(i32, Vec<u8>, &'static str), (i32, String)> {
    try_block! {
        let _filelock = FileLock::lock(&NOTES_LOCK_FILE, /*is_blocking*/ true, FileOptions::new().write(true))
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
        update_notes_file_and_git_history(&notes)
            .map_err(|e| format!("Error updating notes file or git history: {}", e))
    }.map_err(|e: String| (500, format!("Error updating notes via json: {}", e)))?;
    Ok((200, Vec::new(), "text/plain"))
}

fn update_notes_incremental() -> Result<(i32, Vec<u8>, &'static str), (i32, String)> {
    try_block! {
        let _filelock = FileLock::lock(&NOTES_LOCK_FILE, /*is_blocking*/ true, FileOptions::new().write(true))
            .map_err(|e| format!("Error locking notes lock file: {}", e))?;
        let mut request_body = Vec::new();
        io::stdin().read_to_end(&mut request_body)
            .map_err(|e| format!("Error reading notes bytes from stdin: {}", e))?;
        log_error!("update_notes_incremental: size of body: {}", request_body.len());
        let json_raw = str::from_utf8(&request_body)
            .map_err(|e| format!("Error parsing POST data as utf8 string: {}", e))?;
        let json: json::JsonValue = json::parse(json_raw)
            .map_err(|e| format!("Error parsing json: {}", e))?;

        // extract json fields: reusable_prefix_len, reusable_suffix_len, new_content, hash
        let prefix_len: usize = json["reusable_prefix_len"].as_usize().ok_or_else(|| format!("reusable_prefix_len missing"))?;
        let suffix_len: usize = json["reusable_suffix_len"].as_usize().ok_or_else(|| format!("reusable_suffix_len missing"))?;
        let new_content: &str = json["new_content"].as_str().ok_or_else(|| format!("new_content missing"))?;
        let expected_hash: u32 = json["hash"].as_u32().ok_or_else(|| format!("hash missing"))?;
        // construct new notes
        let curr_notes: String = String::from_utf8(
                std::fs::read(NOTES_FILE)
                    .map_err(|e| format!("Error reading notes file: {}", e))?)
            .map_err(|e| format!("Error parsing notes files as utf8: {}", e))?;
        log_error!("    curr_notes char count: {}", curr_notes.chars().count());
        let prefix: String = curr_notes.chars().take(prefix_len).collect();
        let char_count = curr_notes.chars().count();
        let suffix: String = curr_notes.chars().skip(char_count - suffix_len).collect();
        log_error!("    new_content({}): '{}'", new_content.len(), new_content);
        log_error!("    reusing prefix of {} chars and suffix of {} chars", prefix.chars().count(), suffix.chars().count());
        let new_notes = format!("{}{}{}", prefix, new_content, suffix);
        //log_error!("new_notes: '{}'", new_notes);
        log_error!("    new_notes char count: {}", new_notes.chars().count());
        // hash new notes
        let actual_hash: u32 = hash_str(&new_notes);
        log_error!("    str with len: {} has hash: {}", new_notes.len(), actual_hash);
        // compare expect and actual hash
        if actual_hash != expected_hash {
            return Err(format!("Actual hash '{}' does not match expected hash '{}'", actual_hash, expected_hash));
        }

        // let prefix_len: usize = json["reusable_prefix_len"].as_usize().ok_or_else(|| format!("Missing reusable_prefix_len"))?;
        // let suffix_len: usize = json["reusable_suffix_len"].as_usize().ok_or_else(|| format!("Missing reusable_suffix_len"))?;
        // let new_content_base64: &str = json["new_content_base64"].as_str().ok_or_else(|| format!("Missing new_content_base64"))?;
        // let new_content: Vec<u8> = base64::decode(new_content_base64).map_err(|e| format!("Error decoding base64: {}", e))?;
        // let expected_hash: u32 = json["hash"].as_u32().ok_or_else(|| format!("hash missing"))?;
        // let curr_notes: Vec<u8> = std::fs::read(NOTES_FILE)
        //     .map_err(|e| format!("Error reading notes file: {}", e))?;
        // let new_notes_bytes: Vec<u8> = curr_notes.iter().take(prefix_len)
        //     .chain(new_content.iter())
        //     .chain(curr_notes.iter().skip(curr_notes.len() - suffix_len))
        //     .copied()
        //     .collect();
        // let new_notes: String = String::from_utf8(new_notes_bytes).map_err(|e| format!("Error converting new_notes_bytes to utf: {}", e))?;
        // // TODO: compare hashes
        
        update_notes_file_and_git_history(&new_notes)
            .map_err(|e| format!("Error updating notes file or git history: {}", e))
    }.map_err(|e: String| (500, format!("Error updating notes incrementally: {}", e)))?;
    Ok((200, Vec::new(), "text/plain"))
}

fn hash_str(s: &str) -> u32 {
    let bytes = s.as_bytes();
    let mut hash = std::num::Wrapping(0u32);
    let mut idx = 0usize;
    for _ in 0..((bytes.len() as f32 / 4f32).ceil() as usize) {
      let a = *bytes.get(idx + 0).unwrap_or(&0) as u32;
      let b = *bytes.get(idx + 1).unwrap_or(&0) as u32;
      let c = *bytes.get(idx + 2).unwrap_or(&0) as u32;
      let d = *bytes.get(idx + 3).unwrap_or(&0) as u32;
      let x = (a << 24) | (b << 16) | (c << 8) | d;
      hash = (hash << 5) - hash + std::num::Wrapping(x);
      idx += 4;
    }
    hash.0
}

// fn hash_vec(bytes: &Vec<u8>) -> u32 {
//     let mut hash = std::num::Wrapping(0u32);
//     let mut idx = 0usize;
//     for _ in 0..((bytes.len() as f32 / 4f32).ceil() as usize) {
//       let a = *bytes.get(idx + 0).unwrap_or(&0) as u32;
//       let b = *bytes.get(idx + 1).unwrap_or(&0) as u32;
//       let c = *bytes.get(idx + 2).unwrap_or(&0) as u32;
//       let d = *bytes.get(idx + 3).unwrap_or(&0) as u32;
//       let x = (a << 24) | (b << 16) | (c << 8) | d;
//       hash = (hash << 5) - hash + std::num::Wrapping(x);
//       idx += 4;
//     }
//     hash.0
// }

fn update_notes_file_and_git_history(notes: &str) -> Result<(), String> {
    // update file contents
    let mut rng = rand::thread_rng();
    let rand_suffix: String = (0..5).map(|_| rng.sample(Alphanumeric) as char).collect();
    let tmp_file = format!("{}.{}", NOTES_FILE, rand_suffix);
    fs::write(&tmp_file, notes.as_bytes())
        .map_err(|e| format!("Error writing notes to tmp file {}: {}", &tmp_file, e))?;
    fs::rename(&tmp_file, NOTES_FILE)
        .map_err(|e| format!("Error renaming tmp file {} to notes file {}: {}", &tmp_file, NOTES_FILE, e))?;

    // update git history
    env::set_current_dir(NOTES_DIR)
        .map_err(|e| format!("Error changing directory to {}: {}", NOTES_DIR, e))?;
    env::set_var("GIT_DIR", "history");
    env::set_var("GIT_WORK_TREE", ".");
    let status_output = Command::new("git")
        .args(["status", "--porcelain", NOTES_FILE])
        .output()
        .map_err(|e| format!("Error excuting git status on file {}: {}", NOTES_FILE, e))?;
    if !status_output.status.success() {
        return Err(
            format!(
                "git status failed, status: '{}', stdout: '{}', stderr: '{}'",
                status_output.status,
                str::from_utf8(&status_output.stdout).unwrap_or("<stdout to utf8 error>"),
                str::from_utf8(&status_output.stderr).unwrap_or("<stderr to utf8 error>")));
    }
    if !status_output.stdout.as_slice().starts_with(b" M") {
        // nothing to do, early return
        return Ok(())
    }
    let commit_output = Command::new("git")
        .args(["commit", "--allow-empty-message", "-am", ""])
        .output()
        .map_err(|e| format!("Error excuting git commit: {}", e))?;
    if !commit_output.status.success() {
        return Err(
            format!(
                "git commit failed, status: '{}', stdout: '{}', stderr: '{}'",
                status_output.status,
                str::from_utf8(&commit_output.stdout).unwrap_or("<stdout to utf8 error>"),
                str::from_utf8(&commit_output.stderr).unwrap_or("<stderr to utf8 error>")));
    }
    Ok(())
}

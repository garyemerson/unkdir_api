use chrono::{Utc, SecondsFormat};
use metrics::computer_activity::{program_usage_by_hour, top_limit, time_in};
use std::{thread, str, env};
use std::io::{self, Write};
use std::time::{SystemTime, Duration};


macro_rules! log_error {
    ($($tts:tt)*) => {
        eprintln!(
            "[{}] [cgi: unkdir_api] {}",
            Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true),
            format!($($tts)*))
    }
}

mod metrics;

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

        method => {
            Err((400, format!("Unsupported method {}", method)))
        }
    }

}

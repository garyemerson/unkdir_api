use chrono::{DateTime, Utc, SecondsFormat};
use json::JsonValue;
use std::{str, env, matches};
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::process::{Command, Stdio};
use std::str::Split;


const KINDLE_MEME_FILE: &str = "/root/unkdir/meme_board/meme.png";
const RAW_MEME_FILE: &str = "/root/unkdir/meme_board/meme_raw.png";
const WEB_COMPRESSED_MEME_FILE: &str = "/root/unkdir/meme_board/meme_compressed.png";
const MEME_ID_FILE: &str = "/root/unkdir/meme_board/meme_id";
const BATTERY_FILE_PATH: &str = "/root/unkdir/meme_board/battery_percent";
const ARCHIVE_DIR: &str = "/root/unkdir/meme_board/archive";

pub(crate) fn battery_history() -> Result<(i32, Vec<u8>, &'static str), (i32, String)> {
    let json = try_block! {
        let earliest_time_filter: DateTime<Utc> = try_block! {
            let qstr = env::var("QUERY_STRING").ok()?;
            let mut parts = qstr.split("=");
            let key = parts.next()?;
            if key != "limit" {
                return None;
            }
            let duration_back =
                chrono::Duration::from_std(humantime::parse_duration(parts.next()?).ok()?).ok()?;
            Some(Utc::now() - duration_back)
        }.unwrap_or(chrono::MIN_DATETIME);
        let stats: Vec<JsonValue> = fs::read_to_string(BATTERY_FILE_PATH)
            .map_err(|e| format!("Error reading battery file {}: {}", BATTERY_FILE_PATH, e))?
            .split('\n')
            .rev()
            .map(|l: &str| l.split("||"))
            .filter_map(|mut split_line: Split<'_, &str>| {
                let date = split_line.next()?.to_string();
                if matches!(DateTime::parse_from_rfc3339(&date), Ok(dt) if dt < earliest_time_filter) {
                    return None;
                }
                let percent = split_line.next()?.parse::<i64>().ok()?;
                Some(json::object!{"date": date, "percent": percent})
            })
            .collect();
        Ok(json::stringify(stats))
    }.map_err(|e: String| (500, format!("Error getting battery history: {}", e)))?;
    Ok((200, json.into_bytes(), "application/json; charset=utf-8"))
}

pub(crate) fn meme_status() -> Result<(i32, Vec<u8>, &'static str), (i32, String)> {
    let response_bytes = try_block! {
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
            .unwrap_or_else(|e| log_error!("Error saving battery percentage: {}", e));

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
    }.map_err(|e: String| (500, format!("Error getting meme status: {}", e)))?;
    Ok((200, response_bytes, "application/octet-stream"))
}

pub(crate) fn update_meme() -> Result<(i32, Vec<u8>, &'static str), (i32, String)> {
    try_block! {
        let mut img_bytes = Vec::new();
        io::stdin().read_to_end(&mut img_bytes)
            .map_err(|e| format!("Error reading img bytes from stdin: {}", e))?;
        update_meme_from_bytes(img_bytes)?;
        Ok(())
    }.map_err(|e: String| (500, format!("Error updating meme: {}", e)))?;
    Ok((200, Vec::new(), "text/plain"))
}

pub(crate) fn update_meme_from_url() -> Result<(i32, Vec<u8>, &'static str), (i32, String)> {
    try_block! {
        let mut url_bytes = Vec::new();
        io::stdin().read_to_end(&mut url_bytes)
            .map_err(|e| format!("Error reading url bytes from stdin: {}", e))?;
        let url = str::from_utf8(&url_bytes)
            .map_err(|e| format!("Error parsing POST data as utf8 string: {}", e))?;
        let img_bytes = piped_cmd(&Vec::new(), ["curl", "--location", "--silent", "--fail", "--show-error", url])
            .map_err(|e| format!("Error downloading meme image from url '{}': {}", url, e))?;
        update_meme_from_bytes(img_bytes)
            .map_err(|e| format!("Error updating meme from url bytes: {}", e))
    }.map_err(|e| (500, format!("Error updating meme from url: {}", e)))?;
    Ok((200, Vec::new(), "text/plain"))
}

pub(crate) fn update_meme_from_bytes(img_bytes: Vec<u8>) -> Result<(), String> {
    let processed_img_bytes = load_from_memory(&img_bytes)
        .map_err(|e| format!("Error loading img from buffer with length {}: {}", img_bytes.len(), e))?;
    File::create(RAW_MEME_FILE)
        .map_err(|e| format!("Error creating raw img to file {}: {}", RAW_MEME_FILE, e))?
        .write_all(&processed_img_bytes)
        .map_err(|e| format!("Error writing bytes to raw img file: {}", e))?;

    archive_meme(&processed_img_bytes)
        .unwrap_or_else(|e| log_error!("Error archiving meme: {}", e));
    create_kindle_format_img(&processed_img_bytes)
        .map_err(|e| format!("Error formatting for kindle: {}", e))?;
    compress_meme(&processed_img_bytes)
        .map_err(|e| format!("Error compressing img: {}", e))?;

    let meme_id_raw = fs::read_to_string(MEME_ID_FILE)
        .map_err(|e| format!("Error reading meme_id file {}: {}", MEME_ID_FILE, e))?;
    let meme_id = meme_id_raw.trim().parse::<i32>()
        .map_err(|e| format!("Error parsing '{}' as i32: {}", meme_id_raw, e))?;
    fs::write(MEME_ID_FILE, (meme_id + 1).to_string().into_bytes())
        .map_err(|e| format!("Error updating and saving meme id to file {}: {}", MEME_ID_FILE, e))?;

    Ok(())
}

fn load_from_memory(img_bytes: &Vec<u8>) -> Result<Vec<u8>, String> {
    piped_cmd(img_bytes, ["convert", "-auto-orient", "-", "png:-"])
}

fn compress_meme(img_bytes: &Vec<u8>) -> Result<(), String> {
    // convert -resize 400 - png:-
    let resized_img_bytes = piped_cmd(img_bytes, ["convert", "-resize", "400", "-", "png:-"])
        .map_err(|e| format!("Error resizing image: {}", e))?;
    File::create(WEB_COMPRESSED_MEME_FILE)
        .map_err(|e| format!("Error creating file compressed meme file: {}", e))?
        .write_all(&resized_img_bytes)
        .map_err(|e| format!("Error writing compressed bytes to file: {}", e))
}

fn archive_meme(img_bytes: &Vec<u8>) -> Result<(), String> {
    let timestamp  = Utc::now()
        .to_rfc3339_opts(SecondsFormat::Secs, true)
        .replace([':', '-'].as_ref(), "");
    let filepath = if let Ok(ip) = env::var("REMOTE_ADDR") {
        format!("{dir}/{time}-{ip}.png", dir = ARCHIVE_DIR, time = timestamp, ip = ip)
    } else {
        format!("{dir}/{time}.png", dir = ARCHIVE_DIR, time = timestamp)
    };
    File::create(&filepath)
        .map_err(|e| format!("Error creating archive file {}: {}", filepath, e))?
        .write_all(&img_bytes)
        .map_err(|e| format!("Error writing bytes to img file {}: {}", filepath, e))
}

fn create_kindle_format_img(img_bytes: &Vec<u8>) -> Result<(), String> {
    // convert -resize 768x1024 -extent 768x1024 -gravity center -background black -grayscale Rec709Luma -strip -auto-gamma -auto-level -normalize - png:-
    let width = 768;
    let height = 1024;
    let wh = format!("{}x{}", width, height);
    let cmd_and_args = [
        "convert",
        "-resize", &wh,
        "-extent", &wh,
        "-gravity", "center",
        "-background", "black",
        "-grayscale", "Rec709Luma",
        "-strip",
        "-colors", "15",
        // "-auto-gamma",
        // "-auto-level",
        // "-normalize",
        "-",
        "png:-"];
    let fmt_img_bytes = piped_cmd(img_bytes, cmd_and_args)
        .map_err(|e| format!("Error converting img to kindle format: {}", e))?;
    File::create(KINDLE_MEME_FILE)
        .map_err(|e| format!("Error creating kindle meme file: {}", e))?
        .write_all(&fmt_img_bytes)
        .map_err(|e| format!("Error writing bytes to kindle meme file: {}", e))
}

fn piped_cmd<I, S>(data_in: &Vec<u8>, cmd_and_args: I) -> Result<Vec<u8>, String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let mut cmd_and_args_iter = cmd_and_args.into_iter();
    let cmd = cmd_and_args_iter.next().ok_or_else(|| format!("missing command"))?;
    let mut child = Command::new(&cmd)
        .args(cmd_and_args_iter)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .map_err(|e| format!("Error starting cmd '{:?}': {}", cmd.as_ref(), e))?;
    child
        .stdin
        .as_mut()
        .ok_or("Unable to get stdin for child process".to_string())?
        .write_all(data_in)
        .map_err(|e| format!("Error writing stdin bytes to child process: {}", e))?;
    let output = child.wait_with_output()
        .map_err(|e| format!("Error reading stdout of cmd '{:?}': {}", cmd.as_ref(), e))?;
    if !output.status.success() {
        return Err(
            format!(
                "cmd '{:?}' failed, exit status: '{:?}', stderr: '{}'",
                cmd.as_ref(),
                output.status.code(),
                str::from_utf8(&output.stderr).unwrap_or("<stderr to utf8 error>")))
    }
    Ok(output.stdout)
}

fn save_battery_percentage(battery_percent: String) -> Result<(), String> {
    let mut battery_file = OpenOptions::new()
        .append(true)
        .open(BATTERY_FILE_PATH)
        .map_err(|e| format!("Error opening file {} : {}", BATTERY_FILE_PATH, e))?;
    let time = chrono::offset::Local::now().to_rfc3339_opts(SecondsFormat::Millis, true);
    let bytes = format!("{}||{}\n", time, battery_percent).into_bytes();
    battery_file.write_all(&bytes)
        .map_err(|e| format!("Error writing to file {}: {}", BATTERY_FILE_PATH, e))?;

    Ok(())
}

use chrono::{Local, Utc, SecondsFormat};
//use image::imageops::colorops::contrast;
use image::imageops::{resize, overlay /*, brighten*/};
use image::ImageOutputFormat;
use image::png::PNGEncoder;
use image::{Pixel, GenericImageView, ImageBuffer, Luma, DynamicImage, FilterType, load_from_memory};
use serde_json::{Value, json};

use std::{str, env};
use std::process::{Command, Stdio};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::Path;
use std::str::Split;

use crate::log_error;

const KINDLE_MEME_FILE: &str = "/root/unkdir/meme_board/meme.png";
const RAW_MEME_FILE: &str = "/root/unkdir/meme_board/meme_raw.png";
const WEB_COMPRESSED_MEME_FILE: &str = "/root/unkdir/meme_board/meme_compressed.png";
const MEME_ID_FILE: &str = "/root/unkdir/meme_board/meme_id";
const BATTERY_FILE_PATH: &str = "/root/unkdir/meme_board/battery_percent";
const ARCHIVE_DIR: &str = "/root/unkdir/meme_board/archive";

pub(crate) fn battery_history() -> Result<String, String> {
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

pub(crate) fn meme_status() -> Result<Vec<u8>, String> {
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

pub(crate) fn update_meme() -> Result<(), String> {
    let mut img_bytes = Vec::new();
    io::stdin().read_to_end(&mut img_bytes)
        .map_err(|e| format!("Error reading img bytes from stdin: {}", e))?;
    update_meme_from_bytes(img_bytes)?;

    Ok(())
}

pub(crate) fn update_meme_from_url() -> Result<(), String> {
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

pub(crate) fn update_meme_from_bytes(img_bytes: Vec<u8>) -> Result<(), String> {
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
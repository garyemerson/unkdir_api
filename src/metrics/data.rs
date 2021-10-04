use chrono::{DateTime, Utc, SecondsFormat};
use json::JsonValue;
use json::object;
use json::object::Object;
use postgres::row::Row;
use std::convert::TryFrom;


#[derive(Debug)]
pub(crate) struct Visit {
    pub(crate) arrival: Option<DateTime<Utc>>,
    pub(crate) departure: Option<DateTime<Utc>>,
    pub(crate) latitude: f64,
    pub(crate) longitude: f64,
    pub(crate) horizontal_accuracy: f64
}

impl From<&Visit> for JsonValue {
    fn from(visit: &Visit) -> Self {
        object! {
            arrival: visit.arrival.map(|dt| dt.to_rfc3339_opts(SecondsFormat::Millis, true)),
            departure: visit.departure.map(|dt| dt.to_rfc3339_opts(SecondsFormat::Millis, true)),
            latitude: visit.latitude,
            longitude: visit.longitude,
            horizontal_accuracy: visit.horizontal_accuracy,
        }
    }
}

impl TryFrom<&JsonValue> for Visit {
    type Error = String;

    fn try_from(json_value: &JsonValue) -> Result<Self, Self::Error> {
        let json_obj: &Object = match json_value {
            JsonValue::Object(obj) => obj,
            v => return Err(format!("Found non-object type: '{:?}'", v)),
        };
        let arrival: Option<DateTime<Utc>> = if json_obj["arrival"].is_null() { None } else {
            let dt = extract_datetime(&json_obj, "arrival")
                .map_err(|e| format!("Error getting arrival: {}", e))?;
            Some(dt)
        };
        let departure: Option<DateTime<Utc>> = if json_obj["departure"].is_null() { None } else {
            let dt = extract_datetime(&json_obj, "departure")
                .map_err(|e| format!("Error getting departure: {}", e))?;
            Some(dt)
        };
        let latitude: f64 = extract_f64(&json_obj, "latitude")
            .map_err(|e| format!("Error getting latitude: {}", e))?;
        let longitude: f64 = extract_f64(&json_obj, "longitude")
            .map_err(|e| format!("Error getting longitude: {}", e))?;
        let horizontal_accuracy: f64 = extract_f64(&json_obj, "horizontal_accuracy")
            .map_err(|e| format!("Error getting horizontal_accuracy: {}", e))?;
        Ok(Visit { arrival, departure, latitude, longitude, horizontal_accuracy })
    }
}

impl TryFrom<&Row> for Visit {
    type Error = String;

    fn try_from(row: &Row) -> Result<Self, Self::Error> {
        let arrival: Option<DateTime<Utc>> = row.try_get("arrival")
            .map_err(|e| format!("Error getting arrival: {}", e))?;
        let departure: Option<DateTime<Utc>> = row.try_get("departure")
            .map_err(|e| format!("Error getting departure: {}", e))?;
        let latitude: f64 = row.try_get("latitude")
            .map_err(|e| format!("Error getting latitude: {}", e))?;
        let longitude: f64 = row.try_get("longitude")
            .map_err(|e| format!("Error getting longitude: {}", e))?;
        let horizontal_accuracy: f64 = row.try_get("horizontal_accuracy")
            .map_err(|e| format!("Error getting horizontal_accuracy: {}", e))?;
        Ok(Visit { arrival, departure, latitude, longitude, horizontal_accuracy })
    }
}

#[derive(Debug)]
pub(crate) struct Location {
    pub(crate) date: DateTime<Utc>,
    pub(crate) latitude: f64,
    pub(crate) longitude: f64,
    pub(crate) altitude: f64,
    pub(crate) horizontal_accuracy: f64,
    pub(crate) vertical_accuracy: f64,
    pub(crate) course: Option<f64>,
    pub(crate) speed: Option<f64>,
    pub(crate) floor: Option<i32>,
}

impl From<&Location> for JsonValue {
    fn from(location: &Location) -> Self {
        object! {
            date: location.date.to_rfc3339_opts(SecondsFormat::Millis, true),
            latitude: location.latitude,
            longitude: location.longitude,
            altitude: location.altitude,
            horizontal_accuracy: location.horizontal_accuracy,
            vertical_accuracy: location.vertical_accuracy,
            course: location.course,
            speed: location.speed,
            floor: location.floor,
        }
    }
}

impl TryFrom<&JsonValue> for Location {
    type Error = String;

    fn try_from(value: &JsonValue) -> Result<Self, Self::Error> {
        let json_obj: &Object = match value {
            JsonValue::Object(obj) => obj,
            v => return Err(format!("Found non-object type: '{:?}'", v)),
        };
        let date: DateTime<Utc> = extract_datetime(&json_obj, "date")
                .map_err(|e| format!("Error getting date: {}", e))?;
        let latitude: f64 = extract_f64(&json_obj, "latitude")
            .map_err(|e| format!("Error getting latitude: {}", e))?;
        let longitude: f64 = extract_f64(&json_obj, "longitude")
            .map_err(|e| format!("Error getting longitude: {}", e))?;
        let altitude: f64 = extract_f64(&json_obj, "altitude")
            .map_err(|e| format!("Error getting altitude: {}", e))?;
        let horizontal_accuracy: f64 = extract_f64(&json_obj, "horizontal_accuracy")
            .map_err(|e| format!("Error getting horizontal_accuracy: {}", e))?;
        let vertical_accuracy: f64 = extract_f64(&json_obj, "vertical_accuracy")
            .map_err(|e| format!("Error getting vertical_accuracy: {}", e))?;
        let course: Option<f64> = if json_obj["course"].is_null() { None } else {
            let c: f64 = extract_f64(&json_obj, "course")
                .map_err(|e| format!("Error getting course: {}", e))?;
            Some(c)
        };
        let speed: Option<f64> = if json_obj["speed"].is_null() { None } else {
            let c: f64 = extract_f64(&json_obj, "speed")
                .map_err(|e| format!("Error getting speed: {}", e))?;
            Some(c)
        };
        let floor: Option<i32> = match &json_obj["floor"] {
            v @ (JsonValue::Number(_) | JsonValue::Null) => v.as_i32(),
            v => return Err(format!("Expected floor to be number but found: '{:?}'", v)),
        };
        Ok(Location { date, latitude, longitude, altitude, horizontal_accuracy, vertical_accuracy,
            course, speed, floor })
    }
}

//use std::convert::TryInto;
//impl<T: TryInto<JsonValue>> TryFrom<Option<T>> for JsonValue {
//    type Error = String;
//    fn try_from(val: Option<T>) -> Result<Self, Self::Error> {
//        match val {
//            Some(val) => val.try_into().map_err(|e| String::new())?,
//            None => JsonValue::Null,
//        }
//    }
//}

impl TryFrom<&Row> for Location {
    type Error = String;

    fn try_from(row: &Row) -> Result<Self, Self::Error> {
        let date: DateTime<Utc> = row.try_get("date")
            .map_err(|e| format!("Error getting date: {}", e))?;
        let latitude: f64 = row.try_get("latitude")
            .map_err(|e| format!("Error getting latitude: {}", e))?;
        let longitude: f64 = row.try_get("longitude")
            .map_err(|e| format!("Error getting longitude: {}", e))?;
        let altitude: f64 = row.try_get("altitude")
            .map_err(|e| format!("Error getting altitude: {}", e))?;
        let horizontal_accuracy: f64 = row.try_get("horizontal_accuracy")
            .map_err(|e| format!("Error getting horizontal_accuracy: {}", e))?;
        let vertical_accuracy: f64 = row.try_get("vertical_accuracy")
            .map_err(|e| format!("Error getting vertical_accuracy: {}", e))?;
        let course: Option<f64> = row.try_get("course")
            .map_err(|e| format!("Error getting course: {}", e))?;
        let speed: Option<f64> = row.try_get("speed")
            .map_err(|e| format!("Error getting speed: {}", e))?;
        let floor: Option<i32> = row.try_get("floor")
            .map_err(|e| format!("Error getting floor: {}", e))?;
        Ok(Location { date, latitude, longitude, altitude, horizontal_accuracy, vertical_accuracy,
            course, speed, floor })
    }
}

fn extract_f64(obj: &Object, key: &str) -> Result<f64, String> {
    match &obj[key] {
        JsonValue::Number(n) => Ok((*n).into()),
        v => Err(format!("Expected {} to be number but found: '{:?}'", key, v)),
    }
}

fn extract_datetime(obj: &Object, key: &str) -> Result<DateTime<Utc>, String> {
    let dt_str: &str = match &obj[key] {
        JsonValue::String(s) => s.as_str(),
        JsonValue::Short(s) => s.as_str(),
        v => return Err(format!("Expected {} to be string but found: '{:?}'", key, v)),
    };
    let dt = DateTime::parse_from_rfc3339(dt_str)
        .map_err(|e| format!("Error parsing datetime '{}': {}", dt_str, e))?
        .with_timezone(&Utc);
    Ok(dt)
}

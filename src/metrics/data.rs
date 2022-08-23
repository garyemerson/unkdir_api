use chrono::{DateTime, Utc, SecondsFormat, TimeZone};
use json::JsonValue;
use json::object;
use json::object::Object;
use postgres::row::Row;
use std::convert::TryFrom;
use std::fmt::Display;
    

#[derive(Debug)]
pub(crate) struct Visit {
    pub(crate) arrival: Option<DateTime<Utc>>,
    pub(crate) departure: Option<DateTime<Utc>>,
    pub(crate) latitude: f64,
    pub(crate) longitude: f64,
    pub(crate) horizontal_accuracy: f64
}

// visit -> json
impl From<Visit> for JsonValue {
    fn from(visit: Visit) -> Self {
        object! {
            arrival: visit.arrival.map(|dt| dt.to_rfc3339_opts(SecondsFormat::Millis, true)),
            departure: visit.departure.map(|dt| dt.to_rfc3339_opts(SecondsFormat::Millis, true)),
            latitude: visit.latitude,
            longitude: visit.longitude,
            horizontal_accuracy: visit.horizontal_accuracy,
        }
    }
}

// json -> visit
impl TryFrom<JsonValue> for Visit {
    type Error = String;

    fn try_from(json_value: JsonValue) -> Result<Self, Self::Error> {
        let json_obj: Object = if let JsonValue::Object(obj) = json_value { obj } else {
            return Err(format!("Found non-object type: '{:?}'", json_value));
        };
        let arrival: Option<DateTime<Utc>> = if json_obj["arrival"].is_null() { None } else {
            Some(get_key("arrival", |k| extract_datetime(&json_obj, k))?)
        };
        let departure: Option<DateTime<Utc>> = if json_obj["departure"].is_null() { None } else {
            Some(get_key("departure", |k| extract_datetime(&json_obj, k))?)
        };
        let latitude: f64 = get_key("latitude", |idx| extract_f64(&json_obj, idx))?;
        let longitude: f64 = get_key("longitude", |idx| extract_f64(&json_obj, idx))?;
        let horizontal_accuracy: f64 = get_key("horizontal_accuracy", |idx| extract_f64(&json_obj, idx))?;
        Ok(Visit { arrival, departure, latitude, longitude, horizontal_accuracy })
    }
}

// postgres -> visit
impl TryFrom<&Row> for Visit {
    type Error = String;

    fn try_from(row: &Row) -> Result<Self, Self::Error> {
        let arrival: Option<DateTime<Utc>> = get_key("arrival", |idx| row.try_get(idx))?;
        let departure: Option<DateTime<Utc>> = get_key("departure", |idx| row.try_get(idx))?;
        let latitude: f64 = get_key("latitude", |idx| row.try_get(idx))?;
        let longitude: f64 = get_key("longitude", |idx| row.try_get(idx))?;
        let horizontal_accuracy: f64 = get_key("horizontal_accuracy", |idx| row.try_get(idx))?;
        Ok(Visit { arrival, departure, latitude, longitude, horizontal_accuracy })
    }
}

// sqlite -> visit
impl TryFrom<rusqlite::Row<'_>> for Visit {
    type Error = String;

    fn try_from(row: rusqlite::Row) -> Result<Self, Self::Error> {
        let arrival: Option<DateTime<Utc>> = get_key("arrival_time_unix_ms", |idx| row.get::<_, Option<i64>>(*idx))?
            .map(|ms| Utc.timestamp_millis(ms));
        let departure: Option<DateTime<Utc>> = get_key("departure_time_unix_ms", |idx| row.get::<_, Option<i64>>(*idx))?
            .map(|ms| Utc.timestamp_millis(ms));
        let latitude: f64 = get_key("latitude", |idx| row.get(*idx))?;
        let longitude: f64 = get_key("longitude", |idx| row.get(*idx))?;
        let horizontal_accuracy: f64 = get_key("horizontal_accuracy", |idx| row.get(*idx))?;
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

// location -> json
impl From<Location> for JsonValue {
    fn from(location: Location) -> Self {
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

// json -> location
impl TryFrom<JsonValue> for Location {
    type Error = String;

    fn try_from(value: JsonValue) -> Result<Self, Self::Error> {
        let json_obj: Object = if let JsonValue::Object(obj) = value { obj } else {
            return Err(format!("Found non-object type: '{:?}'", value));
        };
        let date: DateTime<Utc> = get_key("date", |k| extract_datetime(&json_obj, k))?;
        let latitude: f64 = get_key("latitude", |k| extract_f64(&json_obj, k))?;
        let longitude: f64 = get_key("longitude", |k| extract_f64(&json_obj, k))?;
        let altitude: f64 = get_key("altitude", |k| extract_f64(&json_obj, k))?;
        let horizontal_accuracy: f64 = get_key("horizontal_accuracy", |k| extract_f64(&json_obj, k))?;
        let vertical_accuracy: f64 = get_key("vertical_accuracy", |k| extract_f64(&json_obj, k))?;
        let course: Option<f64> = if json_obj["course"].is_null() { None } else {
            Some(get_key("course", |k| extract_f64(&json_obj, k))?)
        };
        let speed: Option<f64> = if json_obj["speed"].is_null() { None } else {
            Some(get_key("speed", |k| extract_f64(&json_obj, k))?)
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

// postgres -> location
impl TryFrom<&Row> for Location {
    type Error = String;

    fn try_from(row: &Row) -> Result<Self, Self::Error> {
        let date: DateTime<Utc> = get_key("date", |idx| row.try_get(idx))?;
        let latitude: f64 = get_key("latitude", |idx| row.try_get(idx))?;
        let longitude: f64 = get_key("longitude", |idx| row.try_get(idx))?;
        let altitude: f64 = get_key("altitude", |idx| row.try_get(idx))?;
        let horizontal_accuracy: f64 = get_key("horizontal_accuracy", |idx| row.try_get(idx))?;
        let vertical_accuracy: f64 = get_key("vertical_accuracy", |idx| row.try_get(idx))?;
        let course: Option<f64> = get_key("course", |idx| row.try_get(idx))?;
        let speed: Option<f64> = get_key("speed", |idx| row.try_get(idx))?;
        let floor: Option<i32> = get_key("floor", |idx| row.try_get(idx))?;
            
        Ok(Location { date, latitude, longitude, altitude, horizontal_accuracy, vertical_accuracy,
            course, speed, floor })
    }
}

// sqlite -> location
impl TryFrom<rusqlite::Row<'_>> for Location {
    type Error = String;

    fn try_from(row: rusqlite::Row) -> Result<Self, Self::Error> {
        let date: DateTime<Utc> = Utc.timestamp_millis(get_key("date_unix_ms", |idx| row.get(*idx))?);
        let latitude: f64 = get_key("latitude", |idx| row.get(*idx))?;
        let longitude: f64 = get_key("longitude", |idx| row.get(*idx))?;
        let altitude: f64 = get_key("altitude", |idx| row.get(*idx))?;
        let horizontal_accuracy: f64 = get_key("horizontal_accuracy", |idx| row.get(*idx))?;
        let vertical_accuracy: f64 = get_key("vertical_accuracy", |idx| row.get(*idx))?;
        let course: Option<f64> = get_key("course", |idx| row.get(*idx))?;
        let speed: Option<f64> = get_key("speed", |idx| row.get(*idx))?;
        let floor: Option<i32> = get_key("floor", |idx| row.get(*idx))?;
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

fn get_key<I: Display, T, E: Display>(idx: I, provider: impl Fn(&I) -> Result<T, E>) -> Result<T, String> {
    provider(&idx).map_err(|e| format!("Error getting '{}': {}", idx, e))
}

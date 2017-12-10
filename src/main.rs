extern crate postgres;
extern crate chrono;
extern crate serde;
extern crate serde_json;

#[macro_use]
extern crate serde_derive;

use std::time::SystemTime;
use std::env;
use postgres::{Connection, TlsMode};

fn main() {
    let mut body = format!("{:?}\n\n", SystemTime::now());
    match env::var("PATH_INFO") {
        Ok(val) => {
            match val.as_ref() {
                "/top" => body.push_str("top programs\n"),
                res => body.push_str(&format!("unknown resource: {}\n", res))
            }
        },
        Err(_) => body.push_str("invalid request\n"),
    }
    body.push_str("\n");
    for (key, value) in env::vars() {
        body.push_str(&format!("{}: {}\n", key, value));
    }

    match program_usage_by_hour() {
        Ok(progs) => {
            match serde_json::to_string(&progs) {
                Ok(json) => {
                    body.push_str(&json);
                },
                Err(e) => {
                    body.push_str(&format!("Error serializing to json: {}", e));
                }
            }

        },
        Err(e) => {
            body.push_str(&format!("Error getting program usage: {}", e));
        }
    }

    //"Status: 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}\r\n",
    let headers = [
        "Content-type: application/json; charset=utf-8",
        &format!("Content-Length: {}", body.len()),
        "Connection: close",
    ].join("\r\n");

    print!(
        "{headers}\r\n\r\n{body}",
        headers = headers,
        body = body);
}

#[derive(Debug, Serialize)]
struct ProgramUsageMetric {
    hour_of_day: f64,
    program: String,
    window_title: String,
    count: i64,
}

fn program_usage_by_hour() -> Result<Vec<ProgramUsageMetric>, String> {
    let conn_str = "postgres://Garrett@garspace.com/Garrett";
    let conn = Connection::connect(conn_str, TlsMode::None)
        .map_err(|e| format!("Error setting up connection with connection string '{}': {}", conn_str, e))?;

    let query =
"with tmp as (
    select
        hour_of_day,
        program,
        max(window_title) as window_title, -- TODO: grab most recent window title
        count(*) as count
    from (
        select *, date_part('hour', timestamp) as hour_of_day
        from test_metrics2
        where idle_time_ms < 120 * 1000 and lower(program) not like '%lockapp%'
    ) as t
    group by hour_of_day, program
)
select *
from tmp tmp_a
where (
    select count(*)
    from tmp tmp_b
    where tmp_a.hour_of_day = tmp_b.hour_of_day
        and tmp_a.count < tmp_b.count
) <= 2
order by hour_of_day desc, count desc";

    let results = conn.query(query, &[])
        .map_err(|e| format!("Error executing query '{}': {}", query, e))?
        .iter()
        .map(|row| ProgramUsageMetric {
            hour_of_day: row.get(0),
            program: row.get(1),
            window_title: row.get(2),
            count: row.get(3),
        })
        .collect::<Vec<ProgramUsageMetric>>();
    Ok(results)
}


extern crate postgres;
extern crate chrono;
extern crate serde;

#[macro_use]
extern crate serde_json;

#[macro_use]
extern crate serde_derive;

use serde_json::to_string_pretty;
use std::time::SystemTime;
use std::env;
use postgres::{Connection, TlsMode};

fn main() {
    let (status, json_str): (i32, String) = match env::var("PATH_INFO") {
        Ok(path) => {
            match path.as_ref() {
                "/top" => {
                    let (status, json_str) = match program_usage_by_hour() {
                        Ok(progs) => {
                            match serde_json::to_string(&progs) {
                                Ok(json_str) => { (200, json_str) },
                                Err(e) => { (500, to_string_pretty(&json!({"message": format!("Error serializing to json: {}", e)})).unwrap()) } // TODO: can this unwrap be removed?
                            }
                        },
                        Err(e) => { (500, to_string_pretty(&json!({"message": format!("Error getting program usage: {}", e)})).unwrap()) }
                    };
                    (status, json_str)
                },

                "/toplimit" => {
                    let (status, json_str) = match top_foo() {
                        Ok(progs) => {
                            match serde_json::to_string(&progs) {
                                Ok(json_str) => { (200, json_str) },
                                Err(e) => { (500, to_string_pretty(&json!({"message": format!("Error serializing to json: {}", e)})).unwrap()) } // TODO: can this unwrap be removed?
                            }
                        },
                        Err(e) => { (500, to_string_pretty(&json!({"message": format!("Error getting program usage: {}", e)})).unwrap()) }
                    };
                    (status, json_str)
                },

                "/timein" => {
                    let (status, json_str) = match time_in() {
                        Ok(progs) => {
                            match serde_json::to_string(&progs) {
                                Ok(json_str) => { (200, json_str) },
                                Err(e) => { (500, to_string_pretty(&json!({"message": format!("Error serializing to json: {}", e)})).unwrap()) } // TODO: can this unwrap be removed?
                            }
                        },
                        Err(e) => { (500, to_string_pretty(&json!({"message": format!("Error getting time in data: {}", e)})).unwrap()) }
                    };
                    (status, json_str)
                }

                req_path => {
                    (404, to_string_pretty(&json!({"message": format!("Unknown resource {}", req_path)})).unwrap())
                },
            }
        },
        Err(_) => {
            (404, to_string_pretty(&json!({"message": "Must specify resource"})).unwrap())
        },
    };
    // body.push_str("\n");
    // for (key, value) in env::vars() {
    //     body.push_str(&format!("{}: {}\n", key, value));
    // }

    // let body = format!("{:?}\n\n{}", SystemTime::now(), json_str);
    let body = format!("{}\n", json_str);

    //"Status: 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}\r\n",
    let headers = [
        &format!("Status: {}", status),
        "Content-type: application/json; charset=utf-8",
        &format!("Content-Length: {}", body.len()),
        "Connection: close",
        "Access-Control-Allow-Origin: *",
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

#[derive(Debug, Serialize)]
struct ProgramUsageMetric2 {
    hour_of_day: i32,
    program: String,
    window_title: String,
    count: i32,
}

#[derive(Debug, Serialize)]
struct TimeInMetric {
    // Abbreviated day of the week, i.e. Mon, Tue, etc.
    day_of_week: String,

    // average minutes after midnight that I start interacting with my work computer in the morning
    avg_minutes: f64,
}

fn time_in() -> Result<Vec<TimeInMetric>, String> {
    let conn_str = "postgres://Garrett@garspace.com/Garrett";
    let conn = Connection::connect(conn_str, TlsMode::None)
        .map_err(|e| format!("Error setting up connection with connection string '{}': {}", conn_str, e))?;

    let query =
"select
    day_name, avg_total_minutes
from (
    select 
        date_part('dow', first_input) as day_num,
        avg(total_minutes) as avg_total_minutes
    from (
        select
            first_input,
            date_part('hour', first_input) * 60 + date_part('minute', first_input) as total_minutes
        from (
            select min(timestamp) as first_input
            from test_metrics2
            where idle_time_ms < 120 * 1000 and lower(program) not like '%lockapp%'
            group by date_part('year', timestamp), date_part('doy', timestamp)
            order by min(timestamp)
        ) as t5
        where date_part('hour', first_input) <= 10 -- remove outliers
    ) as t4
    group by date_part('dow', first_input)
) as t1
join (
    select column1 as day_num, column2 as day_name
    from (
        values
            (0, 'Sunday'),
            (1, 'Monday'),
            (2, 'Tuesday'),
            (3, 'Wedneday'),
            (4, 'Thursday'),
            (5, 'Friday'),
            (6, 'Saturday')
    ) as t2
) as t3 on t3.day_num = t1.day_num
order by t1.day_num";

    let results = conn.query(query, &[])
        .map_err(|e| format!("Error executing query '{}': {}", query, e))?
        .iter()
        .map(|row| TimeInMetric {
            day_of_week: row.get(0),
            avg_minutes: row.get(1),
        })
        .collect::<Vec<TimeInMetric>>();
    Ok(results)
}

fn top_foo() -> Result<Vec<ProgramUsageMetric2>, String> {
    let conn_str = "postgres://Garrett@garspace.com/Garrett";
    let conn = Connection::connect(conn_str, TlsMode::None)
        .map_err(|e| format!("Error setting up connection with connection string '{}': {}", conn_str, e))?;

    let query =
"select 
    column1 as hour_of_day, 
    column2 as program, 
    column3 as window_title, 
    column4 as count
from (
    values
        (9, 'Sunday', 'foobar', 2),
        (9, 'Sunday', 'foobar', 3),
        (9, 'Sunday', 'foobar', 4),
        (10, 'Monday', 'foobar', 2),
        (10, 'Monday', 'foobar', 3),
        (10, 'Monday', 'foobar', 4),
        (11, 'Tuesday', 'foobar', 2),
        (11, 'Tuesday', 'foobar', 3),
        (11, 'Tuesday', 'foobar', 4),
        (12, 'Wedneday', 'foobar', 2),
        (12, 'Wedneday', 'foobar', 3),
        (12, 'Wedneday', 'foobar', 4),
        (13, 'Thursday', 'foobar', 2),
        (13, 'Thursday', 'foobar', 3),
        (13, 'Thursday', 'foobar', 4),
        (14, 'Friday', 'foobar', 2),
        (14, 'Friday', 'foobar', 3),
        (14, 'Friday', 'foobar', 4),
        (15, 'Saturday', 'foobar', 2),
        (15, 'Saturday', 'foobar', 3),
        (15, 'Saturday', 'foobar', 4)

) as t";

    let results = conn.query(query, &[])
        .map_err(|e| format!("Error executing query '{}': {}", query, e))?
        .iter()
        .map(|row| ProgramUsageMetric2 {
            hour_of_day: row.get(0),
            program: row.get(1),
            window_title: row.get(2),
            count: row.get(3),
        })
        .collect::<Vec<ProgramUsageMetric2>>();
    Ok(results)
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


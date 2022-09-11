use postgres::tls::NoTls;
use postgres::Client;

const CONN_STR: &str = "postgres://Garrett@localhost/Garrett";

fn get_db_client() -> Result<Client, String> {
    Client::connect(CONN_STR, NoTls)
        .map_err(|e| format!("Error setting up connection with connection string '{}': {}", CONN_STR, e))
}

///////////////////
// Mostly unused //
///////////////////
#[allow(dead_code)]
pub mod computer_activity {
    use super::get_db_client;
    
    #[derive(Debug)]
    pub(crate) struct TimeInMetric {
        // Abbreviated day of the week, i.e. Mon, Tue, etc.
        day_of_week: String,

        // average minutes after midnight that I start interacting with my work computer in the morning
        avg_minutes: f64,
    }

    fn time_in_helper() -> Result<Vec<TimeInMetric>, String> {
        let mut client = get_db_client()?;

        let query = "select * from time_in()";

        let results: Vec<TimeInMetric> = client.query(query, &[])
            .map_err(|e| format!("Error executing query '{}': {}", query, e))?
            .iter()
            .map(|row| TimeInMetric {
                day_of_week: row.get(0),
                avg_minutes: row.get(1),
            })
            .collect();
        Ok(results)
    }

    pub(crate) fn time_in() -> Result<(i32, Vec<u8>, &'static str), (i32, String)> {
        // TODO: fix or remove this fn
        return Err((500, "too lazy to implement json serialization".to_string()));
        // let times = time_in_helper()
        //     .map_err(|e| (500, format!("Error getting time in data: {}", e)))?;
        // let json_str = Ok::<_, u8>(String::new())// serde_json::to_string(&times)
        //     .map_err(|e| (500, format!("Error serializing to json: {}", e)))?
        //     .into_bytes();
        // Ok((200, json_str, "application/json; charset=utf-8"))
    }

    #[derive(Debug)]
    pub(crate) struct ProgramUsageMetric2 {
        hour_of_day: i32,
        program: String,
        window_title: String,
        count: i32,
    }

    fn top_limit_helper() -> Result<Vec<ProgramUsageMetric2>, String> {
        let mut client = get_db_client()?;

        let query = "select * from top_foo()";

        let results: Vec<ProgramUsageMetric2> = client.query(query, &[])
            .map_err(|e| format!("Error executing query '{}': {}", query, e))?
            .iter()
            .map(|row| ProgramUsageMetric2 {
                hour_of_day: row.get(0),
                program: row.get(1),
                window_title: row.get(2),
                count: row.get(3),
            })
            .collect();
        Ok(results)
    }

    pub(crate) fn top_limit() -> Result<(i32, Vec<u8>, &'static str), (i32, String)> {
        // TODO: fix or remove this fn
        return Err((500, "too lazy to implement json serialization".to_string()));
        // let usage = top_limit_helper()
        //     .map_err(|e| (500, format!("Error getting program usage: {}", e)))?;
        // let json_str = Ok::<_, u8>(String::new()) // serde_json::to_string(&usage)
        //     .map_err(|e| (500, format!("Error serializing to json: {}", e)))?
        //     .into_bytes();
        // Ok((200, json_str, "application/json; charset=utf-8"))
    }


    #[derive(Debug)]
    pub(crate) struct ProgramUsageMetric {
        hour_of_day: f64,
        program: String,
        window_title: String,
        count: i64,
    }

    #[allow(dead_code)]
    fn program_usage_by_hour_helper() -> Result<Vec<ProgramUsageMetric>, String> {
        let mut client = get_db_client()?;

        let query = "select * from program_usage_by_hour()";

        let results: Vec<ProgramUsageMetric> = client.query(query, &[])
            .map_err(|e| format!("Error executing query '{}': {}", query, e))?
            .iter()
            .map(|row| ProgramUsageMetric {
                hour_of_day: row.get(0),
                program: row.get(1),
                window_title: row.get(2),
                count: row.get(3),
            })
            .collect();
        Ok(results)
    }

    pub(crate) fn program_usage_by_hour() -> Result<(i32, Vec<u8>, &'static str), (i32, String)> {
        // TODO: fix or remove this fn
        return Err((500, "too lazy to implement json serialization".to_string()));
        // let usage = program_usage_by_hour_helper()
        //     .map_err(|e| (500, format!("Error getting program usage: {}", e)))?;
        // let json_str = Ok::<_, u8>(String::new()) // serde_json::to_string(&usage)
        //     .map_err(|e| (500, format!("Error serializing to json: {}", e)))?
        //     .into_bytes();
        // Ok((200, json_str, "application/json; charset=utf-8"))
    }
}

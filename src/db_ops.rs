use std::collections::HashMap;
use std::env;
use deadpool_postgres::{Config, Pool};
use dotenv::dotenv;
use tokio_postgres::{NoTls, Error, Row};
use crate::data_types::CustomError;
use csv::Writer;
use std::fs::File;
use uuid::Uuid;

pub async fn make_db_pool() -> Pool {
    dotenv().ok();
    dotenv::from_filename("app.rust.env").ok();

    let mut cfg = Config::new();
    cfg.host = Option::from(env::var("PG.HOST").unwrap());
    cfg.user = Option::from(env::var("PG.USER").unwrap());
    cfg.password = Option::from(env::var("PG.PASSWORD").unwrap());
    cfg.dbname = Option::from(env::var("PG.DBNAME").unwrap());
    let pool: Pool = cfg.create_pool(None, tokio_postgres::NoTls).unwrap();
    pool
}

pub async fn get_table_columns(table_name: &str) -> Vec<String> {
    return match table_name {
        "table1" => {
            let mut cols = vec![];
            cols.push("random_num".to_string());
            cols.push("random_float".to_string());
            cols.push("md5".to_string());
            cols
        },
        "table2" => {
            let mut cols = vec![];
            cols.push("my_date".to_string());
            cols.push("my_data".to_string());
            cols
        },
        _ => {
            let mut cols = vec![];
            cols
        }
    };
}


pub async fn get_count(table_name: &str, pool: Pool) -> Result<i64, Error> {
    let conn = pool.get().await.unwrap();
    let sql_query = format!("select count(*) from {}", table_name);
    let stmt = conn.prepare(sql_query.as_str()).await.unwrap();
    let row = conn.query_one(&stmt, &[]).await.unwrap();
    Ok(row.get(0))
}

pub async fn get_db_pool_for_table(source_table: &str) -> Result<Pool,CustomError> {
    println!("source_table : {}", source_table.to_string());
    return match source_table {
        "table1" => {
            Ok(make_db_pool().await)
        },
        "table2" => {
            Ok(make_db_pool().await)
        },
        _ => {
            return Err(CustomError::InvalidTable)
        }
    };
}

pub async fn get_backend_table(table_short_name: &str) -> String {
    return match table_short_name {
        "table1" => {
            "t_random".to_string()
        },
        "table2" => {
            "t_data".to_string()
        }
        _ => {
            // default
            "t_random".to_string()
        }
    };
}

pub async fn get_backend_table_columns(table_short_name: &str) -> Vec<String> {
    return match table_short_name {
        "table1" => {
            let mut cols = vec![];
            cols.push("random_num".to_string());
            cols.push("random_float".to_string());
            cols.push("md5".to_string());
            cols
        },
        "table2" => {
            let mut cols = vec![];
            cols.push("my_date".to_string());
            cols.push("my_data".to_string());
            cols
        }
        _ => {
            let mut cols = vec![];
            cols.push("random_num".to_string());
            cols.push("random_float".to_string());
            cols.push("md5".to_string());
            cols
        }
    };
}

pub async fn get_table_column_mapping(table_short_name: &str) -> HashMap<String, String> {
    let mut map: HashMap<String, String> = HashMap::new();
    return match table_short_name {
        "table1" => {
            map.insert("0".to_string(), "random_num".to_string());
            map.insert("1".to_string(), "random_float".to_string());
            map.insert("2".to_string(), "md5".to_string());
            map
        },
        "table2" => {
            map.insert("0".to_string(), "my_date".to_string());
            map.insert("1".to_string(), "my_data".to_string());
            map
        },
        _ => {
            // default
            map.insert("0".to_string(), "random_num".to_string());
            map.insert("1".to_string(), "random_float".to_string());
            map.insert("2".to_string(), "md5".to_string());
            map
        },
    }
}

pub async fn export_table_to_csv(pool: Pool, table_name: &str, table_columns: Vec<String>, search_strings: Vec<String>, pattern_match: String, search_type: String) -> Result<String, Error> {
    // Get a connection from the pool
    let mut client = pool.get().await.unwrap();

    let mut main_query = "".to_string();

    let backend_table = get_backend_table(table_name).await;
    println!("backend_table : {}", backend_table);

    // '___' is sent from the UI : which tells the backend to export all the rows of the table
    if search_strings.len() == 1 && search_strings.get(0).unwrap().to_string() == "___".to_string() {
        main_query = format!("SELECT * FROM {}", backend_table);
    } else {
        let inner_query = get_inner_query(table_columns, search_strings, pattern_match, search_type).await.unwrap();
        main_query = format!("SELECT * FROM {} WHERE {}", backend_table, inner_query);
    }

    println!("main_query : |{}|", main_query);

    // Prepare your SQL query
    let stmt = client.prepare(main_query.as_str()).await.unwrap();

    // Execute the query
    let rows = client.query(&stmt, &[]).await.unwrap();

    let file_name = format!("{}.csv", Uuid::new_v4());
    let dir_name = "data_dir".to_string();
    let complete_file_path = format!("{}/{}", dir_name, file_name);

    // Create a writer to write to a CSV file
    let file = File::create(complete_file_path.clone()).expect("Unable to create file");
    let mut wtr = Writer::from_writer(file);

    // write headers first
    let columns = stmt.columns();
    let headers: Vec<&str> = columns.iter().map(|col| col.name()).collect();
    wtr.write_record(&headers).unwrap();

    write_csv_data(table_name, &rows, wtr).await;

    println!("CSV File Written : {}", complete_file_path.clone().to_string());

    Ok(complete_file_path)
}

/* ************************************************************************************* */

pub async fn get_inner_query(table_columns: Vec<String>, search_strings: Vec<String>, pattern_match: String, search_type: String) -> Result<String, CustomError> {
    let mut inner_query = "".to_string();

    if !(search_type == "and" || search_type == "or") {
        println!("error : search_type is neither 'and' nor 'or' !");
        return Err(CustomError::QueryError)
    }

    if !(pattern_match == "like" || pattern_match == "exact") {
        println!("error : pattern_match is neither 'like' nor 'exact' !");
        return Err(CustomError::QueryError)
    }

    if table_columns.len() == 0 {
        println!("error : table_columns length is ZERO !");
        return Err(CustomError::QueryError)
    }

    if search_strings.len() == 0 {
        println!("error : search_strings length is ZERO !");
        return Err(CustomError::QueryError)
    }

    if pattern_match.as_str() == "exact" { // exact string match
        // search all JSON fields for possible match
        // this can also be applied if there are other columns
        if search_type == "and" {
            inner_query = inner_query + " ( ";
            let mut search_string_counter = 1;
            for my_search_str in search_strings.to_owned() {
                inner_query = inner_query + " ( ";
                let mut column_counter = 1;
                // --------------------------------------
                for my_column in table_columns.to_owned() {
                    if column_counter == table_columns.len() as i32 {
                        inner_query = inner_query + format!(" lower({}::text) = lower('{}') ", my_column.to_string(), my_search_str.to_lowercase()).as_str();
                    } else {
                        inner_query = inner_query + format!(" lower({}::text) = lower('{}') OR ", my_column.to_string(), my_search_str.to_lowercase()).as_str();
                    }
                    column_counter += 1;
                }
                if search_string_counter == search_strings.len() as i32 {
                    inner_query = inner_query + " ) ";
                } else {
                    inner_query = inner_query + " ) AND ";
                }
                search_string_counter += 1;
                // --------------------------------------
            }
            inner_query = inner_query + " ) ";
        } else if search_type == "or" {
            inner_query = inner_query + " ( ";
            let mut search_string_counter = 1;
            for my_search_str in search_strings.to_owned() {
                inner_query = inner_query + " ( ";
                let mut column_counter = 1;
                // --------------------------------------
                for my_column in table_columns.to_owned() {
                    if column_counter == table_columns.len() as i32 {
                        inner_query = inner_query + format!(" lower({}::text) = lower('{}') ", my_column.to_string(), my_search_str.to_lowercase()).as_str();
                    } else {
                        inner_query = inner_query + format!(" lower({}::text) = lower('{}') OR ", my_column.to_string(), my_search_str.to_lowercase()).as_str();
                    }
                    column_counter += 1;
                }
                if search_string_counter == search_strings.len() as i32 {
                    inner_query = inner_query + " ) ";
                } else {
                    inner_query = inner_query + " ) OR ";
                }
                search_string_counter += 1;
                // --------------------------------------
            }
            inner_query = inner_query + " ) ";
        } else {
            return Err(CustomError::QueryError)
        }

    } else if pattern_match.as_str() == "like" { // pattern match
        // search all JSON fields for possible match
        // this can also be applied if there are other columns
        if search_type == "and" {
            inner_query = inner_query + " ( ";
            let mut search_string_counter = 1;
            for my_search_str in search_strings.to_owned() {
                inner_query = inner_query + " ( ";
                let mut column_counter = 1;
                // --------------------------------------
                for my_column in table_columns.to_owned() {
                    if column_counter == table_columns.len() as i32 {
                        inner_query = inner_query + format!(" lower({}::text) like lower('%{}%') ", my_column.to_string(), my_search_str.to_lowercase()).as_str();
                    } else {
                        inner_query = inner_query + format!(" lower({}::text) like lower('%{}%') OR ", my_column.to_string(), my_search_str.to_lowercase()).as_str();
                    }
                    column_counter += 1;
                }
                if search_string_counter == search_strings.len() as i32 {
                    inner_query = inner_query + " ) ";
                } else {
                    inner_query = inner_query + " ) AND ";
                }
                search_string_counter += 1;
                // --------------------------------------
            }
            inner_query = inner_query + " ) ";
        } else if search_type == "or" {
            inner_query = inner_query + " ( ";
            let mut search_string_counter = 1;
            for my_search_str in search_strings.to_owned() {
                inner_query = inner_query + " ( ";
                let mut column_counter = 1;
                // --------------------------------------
                for my_column in table_columns.to_owned() {
                    if column_counter == table_columns.len() as i32 {
                        inner_query = inner_query + format!(" lower({}::text) like lower('%{}%') ", my_column.to_string(), my_search_str.to_lowercase()).as_str();
                    } else {
                        inner_query = inner_query + format!(" lower({}::text) like lower('%{}%') OR ", my_column.to_string(), my_search_str.to_lowercase()).as_str();
                    }
                    column_counter += 1;
                }
                if search_string_counter == search_strings.len() as i32 {
                    inner_query = inner_query + " ) ";
                } else {
                    inner_query = inner_query + " ) OR ";
                }
                search_string_counter += 1;
                // --------------------------------------
            }
            inner_query = inner_query + " ) ";
        } else {
            return Err(CustomError::QueryError)
        }
    } else {
        return Err(CustomError::QueryError)
    }
    Ok(inner_query)
}

/* ************************************************************************************* */

async fn write_csv_data(table_name: &str, rows: &Vec<Row>, mut wtr: Writer<File>) {
    match table_name {
        "table1" => {
            // Iterate over the rows and write to the CSV
            for row in rows {
                let random_num: i32 = row.get("random_num");
                let random_float: f64 = row.get("random_float");
                let md5: String = row.get("md5");
                wtr.write_record(&[random_num.to_string(), random_float.to_string(), md5.to_string()]).unwrap();
            }
        },
        "table2" => {
            // Iterate over the rows and write to the CSV
            for row in rows {
                let my_date: String = row.get("my_date");
                let my_data: String = row.get("my_data");
                wtr.write_record(&[my_date.to_string(), my_data.to_string()]).unwrap();
            }
        },
        _ => {
            // default
            for row in rows {
                let random_num: i32 = row.get("random_num");
                let random_float: f64 = row.get("random_float");
                let md5: String = row.get("md5");
                wtr.write_record(&[random_num.to_string(), random_float.to_string(), md5.to_string()]).unwrap();
            }
        }
    };

    // Flush the writer to ensure all data is written to the file
    wtr.flush().unwrap();
}



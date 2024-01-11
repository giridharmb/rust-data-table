use std::collections::HashMap;
use std::env;
use deadpool_postgres::{Config, Pool};
use dotenv::dotenv;
use tokio_postgres::{NoTls, Error, Row};
use crate::data_types::{CustomError, Data1, Data2, ExportResults};
use csv::Writer;
use std::fs::File;
use std::time::Instant;
// use sqlx::FromRow;
use uuid::Uuid;
use async_trait::async_trait;

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

pub async fn get_table_columns(table_short_name: &str) -> Vec<String> {
    let mut cols = vec![];
    let mapping = get_table_column_mapping(table_short_name).await;
    for(_, column_name) in &mapping {
        cols.push(column_name.to_string());
    }
    cols
}

pub async fn get_count_of_records(total_count_query: String, pool: Pool) -> Result<i64, Error> {
    let conn = pool.get().await.unwrap();
    let sql_query = total_count_query.to_string();
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

async fn get_cols_for_table(table_short_name: &str) -> Vec<String> {
    let mut cols_vec = vec![];
    return match table_short_name {
        "table1" => {
            cols_vec.push("random_num".to_string());
            cols_vec.push("random_float".to_string());
            cols_vec.push("md5".to_string());
            cols_vec
        },
        "table2" => {
            cols_vec.push("my_date".to_string());
            cols_vec.push("my_data".to_string());
            cols_vec
        },
        _ => {
            cols_vec.push("random_num".to_string());
            cols_vec.push("random_float".to_string());
            cols_vec.push("md5".to_string());
            cols_vec
        }
    }
}

pub async fn get_table_column_mapping(table_short_name: &str) -> HashMap<String, String> {
    let mut map: HashMap<String, String> = HashMap::new();
    let cols_vec = get_cols_for_table(table_short_name).await;
    for (index, value) in cols_vec.iter().enumerate() {
        map.insert(index.to_string(), value.to_string());
    }
    map
}

pub async fn get_backend_table_columns(table_short_name: &str) -> Vec<String> {
    let mut cols = vec![];
    let mapping = get_table_column_mapping(table_short_name).await;
    for(_, column_name) in &mapping {
        cols.push(column_name.to_string());
    }
    cols
}

pub async fn export_table_to_csv(pool: Pool, table_name: &str, table_columns: Vec<String>, search_strings: Vec<String>, pattern_match: String, search_type: String) -> Result<ExportResults, Error> {
    let start = Instant::now();
    // Get a connection from the pool
    let mut client = pool.get().await.unwrap();

    let mut main_query = "".to_string();

    let backend_table = get_backend_table(table_name).await;
    println!("backend_table : {}", backend_table);

    let table_columns_separated_by_commas = table_columns.join(",");

    // '___' is sent from the UI : which tells the backend to export all the rows of the table
    if search_strings.len() == 1 && search_strings.get(0).unwrap().to_string() == "___".to_string() {
        main_query = format!("SELECT {} FROM {}", table_columns_separated_by_commas, backend_table);
    } else {
        let inner_query = get_inner_query(table_columns, search_strings, pattern_match, search_type).await.unwrap();
        main_query = format!("SELECT {} FROM {} WHERE {}", table_columns_separated_by_commas, backend_table, inner_query);
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

    ///////////////////

    let mut data_rows_1:Vec<Data1> = Vec::new();
    let mut data_rows_2:Vec<Data2> = Vec::new();

    match table_name {
        "table1" => {
            data_rows_1 = fetch(&client, main_query.as_str()).await.unwrap();
            write_to_csv(data_rows_1, complete_file_path.as_str(), wtr).await.unwrap();
        },
        "table2" => {
            data_rows_2 = fetch(&client, main_query.as_str()).await.unwrap();
            write_to_csv(data_rows_2, complete_file_path.as_str(), wtr).await.unwrap();
        },
        _ => {
            // default is 'table1'
            data_rows_1 = fetch(&client, main_query.as_str()).await.unwrap();
            write_to_csv(data_rows_1, complete_file_path.as_str(), wtr).await.unwrap();
        },
    };

    ///////////////////

    // write_csv_data(table_name, &rows, wtr).await;

    println!("CSV File Written : {}", complete_file_path.clone().to_string());

    let total_rows = rows.len() as i32;

    let duration = start.elapsed().as_secs_f64();

    let csv_export_results = ExportResults {
        csv_file_path: complete_file_path,
        rows: total_rows,
        time_taken_for_export: duration,
    };

    Ok(csv_export_results)
}

/* ************************************************************************************* */

#[async_trait]
pub trait FromRow: Sized {
    async fn from_row(row: &tokio_postgres::Row) -> Result<Self, tokio_postgres::Error>
    where Self: Sized;
}

#[async_trait]
impl FromRow for Data1 {
    async fn from_row(row: &tokio_postgres::Row) -> Result<Self, tokio_postgres::Error> {
        let random_num = row.try_get("random_num").unwrap_or_else(|_| 0);
        let random_float = row.try_get("random_float").unwrap_or_else(|_| 0.0);
        let md5 = row.try_get("md5").unwrap_or_else(|_| "missing_md5".to_string());

        // let random_num = row.get("random_num");
        // let random_float = row.get("random_float");
        // let md5 = row.get("md5");

        Ok(Data1{
            random_num,
            random_float,
            md5,
        })
    }
}

#[async_trait]
impl FromRow for Data2 {
    async fn from_row(row: &tokio_postgres::Row) -> Result<Self, tokio_postgres::Error> {
        let my_date = row.try_get("my_date").unwrap_or_else(|_| "missing_my_date".to_string());
        let my_data = row.try_get("my_data").unwrap_or_else(|_| "missing_my_data".to_string());

        Ok(Data2{
            my_date,
            my_data,
        })
    }
}

/*
Note the use of + Send in the generic type constraint.
This is required because the async_trait macro transforms async trait methods into methods that return Send futures.
*/

pub async fn fetch<T: FromRow + Send>(client: &deadpool_postgres::Client, query: &str) -> Result<Vec<T>, tokio_postgres::Error> {
    let rows = client.query(query, &[]).await?;
    let mut results = Vec::new();

    for row in rows {
        results.push(T::from_row(&row).await?);
    }

    Ok(results)
}

/* ---- csv ---- */

#[async_trait]
pub trait ToCsvRow {
    async fn to_csv_row(&self) -> Vec<String>;
}

#[async_trait]
impl ToCsvRow for Data1 {
    async fn to_csv_row(&self) -> Vec<String> {
        vec![
            self.random_num.to_string(),
            self.random_float.to_string(),
            self.md5.to_string(),
        ]
    }
}

#[async_trait]
impl ToCsvRow for Data2 {
    async fn to_csv_row(&self) -> Vec<String> {
        vec![
            self.my_date.to_string(),
            self.my_data.to_string(),
        ]
    }
}

async fn write_to_csv<T: FromRow + Send + ToCsvRow>(items: Vec<T>, file_path: &str, mut wtr: Writer<File>) -> Result<(), Box<dyn std::error::Error>> {
    // let mut wtr = Writer::from_path(file_path).unwrap();
    for item in items {
        wtr.write_record(item.to_csv_row().await).unwrap();
    }
    wtr.flush().unwrap();
    Ok(())
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
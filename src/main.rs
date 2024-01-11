// main.rs

use std::collections::HashMap;
use std::env;
use std::fs::File;
use actix_web::{get, post, web, App, HttpResponse, HttpServer, Responder, http, ResponseError, HttpRequest};
use actix_web::{error, Result};
use actix_cors::Cors;
use actix_web::http::header::CONTENT_TYPE;

use actix_web::web::get;
use serde::{Deserialize, Serialize};
use tokio_postgres::{NoTls, Error, Row};
use deadpool_postgres::Client;
use tokio::runtime::Runtime;
use serde_json::json;
use regex::Regex;

use async_std::task;
use std::time::Duration;
use actix_web::web::Query;
use futures::{join, select, StreamExt};
use futures::future::FutureExt;
use futures::stream;
use futures::pin_mut;
use futures::try_join;
use async_std;
use deadpool_postgres::{Config, Pool};
use derive_more::{Display, From};
use dotenv::dotenv;
use tera::{Context, Tera};
use actix_web::{web::Data};
use actix_files::Files;
use csv::Writer;
use uuid::Uuid;
use std::error::Error as StdError;
use serde_json::Error as SerdeError;
use serde_json::error::Category;
use sqlx::Value;
use std::path::Path;

mod string_ops;
mod db_ops;
mod data_types;

use crate::data_types::{CustomError, CustomErrorType, Data1, Data2, ExportData, JsonResponseWithCSVExportData};
use crate::data_types::{FormData, GenericError, JsonResponse, SearchStringData};
use crate::db_ops::{export_table_to_csv, get_backend_table, get_backend_table_columns, get_count_of_records};
use crate::db_ops::{fetch};
use crate::db_ops::{get_db_pool_for_table, get_inner_query, get_table_column_mapping};
use crate::string_ops::{remove_leading_and_trailing_spaces, sanitize_string, split_string};

#[post("/echo")]
async fn echo(req_body: String) -> impl Responder {
    HttpResponse::Ok().body(req_body)
}


#[post("/query")]
async fn query_data(form: web::Form<FormData>) -> impl Responder {

    // let form_data = &form.into_inner();

    let mut draw: u32 = 0;
    let mut length: u32 = 0;
    let mut start: u32 = 0;
    let mut search_string = "".to_string();
    let mut exact_search = "".to_string();

    let mut search_strings:Vec<String> = vec![];

    let mut sanitized_search_strings: Vec<String> = vec![];

    let mut search_string_for_reference = "".to_string();

    let mut search_type = "or".to_string();

    let mut sanitized_search_strings = vec![];
    let mut search_strings_with_leading_and_trailing_spaces_removed = vec![];

    let mut search_string_for_reference = "".to_string();

    let mut sort_column_index = "".to_string();
    let mut sort_column_name = "".to_string();
    let mut sort_column_order = "".to_string();

    let mut table_short_name = "".to_string();

    // Access all form fields dynamically
    for (key, value) in &form.fields {
        println!("Field: {} : {}", key, value);

        if key == "length" {
            length = value.parse::<u32>().unwrap();
        }
        if key == "draw" {
            draw = value.parse::<u32>().unwrap() as u32;
        }
        if key == "start" {
            start = value.parse::<u32>().unwrap() as u32;
        }

        // -------------- column sorting | start ------------
        if key == "order[0][dir]" {
            sort_column_order = value.to_string(); // 'asc' or 'desc'
        }

        if key == "order[0][column]" {
            sort_column_index = value.to_string(); // '0' or '1'  or '2' etc...
        }
        // -------------- column sorting | end ------------

        if key == "search[value]" {
            let search_string = value.to_string();
            search_string_for_reference = value.to_string();
            println!("search_string_for_reference: {}", search_string_for_reference);

            if !search_string.is_empty() {
                // you cannot have both AND ('+') and OR ('|') based search
                // it should be either '+' or '|'
                if search_string.contains("+") && search_string.contains("|") {
                    println!("error : cannot search, search_string contains both '+' (AND search) and '|' (OR search) !");
                    return HttpResponse::BadRequest().finish()
                }
            }

            if search_string.contains("+") { // AND search
                search_strings = split_string(search_string.as_str(), "+").await;
                search_type = "and".to_string();
            } else if search_string.contains("|") { // OR search
                search_strings = split_string(search_string.as_str(), "|").await;
                search_type = "or".to_string();
            } else { // default search, which means (search_string) does not contain either '+' or '|'
                search_type = "or".to_string();
                search_strings = Vec::from([search_string]);
            }

            // below sanitize_string(...) will ensure to remove
            // all characters except these : "_./-@,#:;"
            // so that way, DB does not get any characters that are not needed
            // let mut search_strings_with_leading_and_trailing_spaces_removed: Vec<String> = search_strings.iter().map(|s| remove_leading_and_trailing_spaces(s)).collect();

            let futures_1: Vec<_> = search_strings.iter().map(|s| async {
                remove_leading_and_trailing_spaces(s).await
            }).collect();
            search_strings_with_leading_and_trailing_spaces_removed = futures::future::join_all(futures_1).await;

            println!("@ length of search_strings_with_leading_and_trailing_spaces_removed >> {}", search_strings_with_leading_and_trailing_spaces_removed.len());

            // //////////////////////////

            // sanitized_search_strings = search_strings_with_leading_and_trailing_spaces_removed.iter().map(|s| sanitize_string(s)).collect();

            let futures_2: Vec<_> = search_strings_with_leading_and_trailing_spaces_removed.iter().map(|s| async {
                sanitize_string(s).await
            }).collect();
            sanitized_search_strings = futures::future::join_all(futures_2).await;

            println!("@ length of sanitized_search_strings >> {}", sanitized_search_strings.len());

            // //////////////////////////

            println!("sanitized_search_strings: {:#?}", sanitized_search_strings);

            println!("@ length of sanitized_search_strings >> {}", sanitized_search_strings.len());
        }

        if key == "exactsearch" {
            exact_search = value.to_string();
        }

        if key == "tablename" {
            table_short_name = value.to_string();
        }
    }

    let table_column_mapping = get_table_column_mapping(table_short_name.as_str()).await;
    let column_name_to_sort = table_column_mapping.get(sort_column_index.as_str()).unwrap().to_string();

    let mut actual_db_table = get_backend_table(table_short_name.as_str()).await;
    println!("actual_db_table: [ {} ]", actual_db_table.to_string());

    let mut default_query = "".to_string();

    let mut table_columns = vec![];

    // async fn get_backend_table_columns(table_short_name: &str) -> Vec<String>

    table_columns = get_backend_table_columns(table_short_name.as_str()).await;

    // table_columns.push("random_num".to_string());
    // table_columns.push("random_float".to_string());
    // table_columns.push("md5".to_string());

    let mut pattern_match = "".to_string();
    if exact_search == "true" {
        pattern_match = "exact".to_string();
    } else if exact_search == "false" {
        pattern_match = "like".to_string();
    } else {
        return HttpResponse::BadRequest().finish()
    }

    println!("pattern_match: [ {} ]", pattern_match.to_string());

    let mut inner_query = "".to_string();

    let mut total_count_query = "".to_string();

    if search_string_for_reference.is_empty() {
        default_query = format!("SELECT * FROM {} ORDER BY {} {} LIMIT {} OFFSET {}", actual_db_table, column_name_to_sort, sort_column_order, length, start);
        println!("default_query (1) : [ {} ]", default_query.to_string());
        total_count_query = format!("SELECT count(*) FROM {}", actual_db_table);
    } else {
        println!("table_columns : {:#?} , sanitized_search_strings : {:#?} , pattern_match : {} , search_type : {}", table_columns, sanitized_search_strings, pattern_match, search_type);
        inner_query = get_inner_query(table_columns, sanitized_search_strings, pattern_match, search_type).await.unwrap();
        println!("inner_query (2) : {}", inner_query.to_string());

        default_query = format!("SELECT * FROM {} WHERE {} ORDER BY {} {} LIMIT {} OFFSET {}", actual_db_table, inner_query, column_name_to_sort, sort_column_order, length, start);
        println!("default_query (2) : {}", default_query.to_string());
        total_count_query = format!("SELECT count(*) FROM {} WHERE {}", actual_db_table, inner_query);
    }

    let my_db_pool = get_db_pool_for_table(table_short_name.as_str()).await.unwrap();

    let client: deadpool_postgres::Client = my_db_pool.get().await.unwrap();

    let rows = client.query(default_query.as_str(), &[]).await.map_err(|_| HttpResponse::BadRequest().finish()).unwrap();

    let mut data_rows_1:Vec<Data1> = Vec::new();
    let mut data_rows_2:Vec<Data2> = Vec::new();

    match table_short_name.as_str() {
        "table1" => {
            data_rows_1 = fetch(&client, default_query.as_str()).await.unwrap();
        },
        "table2" => {
            data_rows_2 = fetch(&client, default_query.as_str()).await.unwrap();
        },
        _ => {
            // default is 'table1'
            data_rows_1 = fetch(&client, default_query.as_str()).await.unwrap();
        },
    };
    
    let records_total = get_count_of_records(total_count_query, my_db_pool).await.unwrap();
    println!("records_total : {}", records_total);
    let records_filtered = records_total;

    match table_short_name.as_str() {
        "table1" => {
            let response = json!({
                "data": data_rows_1,
                "draw": draw,
                "recordsFiltered": records_filtered,
                "recordsTotal": records_total,
            });
            HttpResponse::Ok().json(response)
        },
        "table2" => {
            let response = json!({
                "data": data_rows_2,
                "draw": draw,
                "recordsFiltered": records_filtered,
                "recordsTotal": records_total,
            });
            HttpResponse::Ok().json(response)
        },
        _ => {
            // default
            let response = json!({
                "data": data_rows_1,
                "draw": draw,
                "recordsFiltered": records_filtered,
                "recordsTotal": records_total,
            });
            HttpResponse::Ok().json(response)
        },
    }
}

async fn index(tera: web::Data<Tera>) -> impl Responder {
    let mut context = Context::new();
    context.insert("title", "Data Table");
    context.insert("message", "PS Table");

    let mut rendered = "".to_string();

    rendered = tera.render("index.html", &context).unwrap();

    HttpResponse::Ok().content_type("text/html").body(rendered)
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {

    let path = Path::new("/etc/app.rust.env");
    if path.exists() {
        dotenv::from_filename("/etc/app.rust.env").ok();
    } else {
        dotenv().ok();
        dotenv::from_filename("app.rust.env").ok();
    }

    std::env::set_var("RUST_LOG", "debug");
    env_logger::init();

    let tera = Tera::new("templates/**/*").unwrap();

    // let my_db_pool = get_db_pool_for_table("table1").await.unwrap();

    HttpServer::new(move || {
        let cors = Cors::permissive();

        App::new()
            .app_data(actix_web::web::Data::new(tera.clone()))
            .app_data(web::JsonConfig::default().error_handler(handle_deserialize_error))
            .wrap(cors)
            .service(query_data)
            .route("/tables", web::get().to(index))
            .service(Files::new("/data_dir", "./data_dir").show_files_listing())
            .route("/export_csv", web::post().to(handle_post))
    })
    .bind("0.0.0.0:5050")?
    .run()
    .await
}


/* ************************************************************************************* */

async fn get_valid_search_strings(input_search_string: &str) -> Result<SearchStringData,CustomErrorType> {
    let mut search_string_for_reference = "".to_string();
    let mut search_strings:Vec<String> = vec![];
    let search_string = input_search_string.to_string();
    let mut search_type = "or".to_string();
    search_string_for_reference = input_search_string.to_string();
    let mut sanitized_search_strings: Vec<String> = vec![];

    let mut search_strings_with_leading_and_trailing_spaces_removed = vec![];

    println!("search_string_for_reference: {}", search_string_for_reference);

    if !search_string.is_empty() {
        // you cannot have both AND ('+') and OR ('|') based search
        // it should be either '+' or '|'
        if search_string.contains("+") && search_string.contains("|") {
            let msg = "error : cannot search, search_string contains both '+' (AND search) and '|' (OR search) !".to_string();
            println!("{}", msg.to_string());
            let custom_err = CustomErrorType {
                err_type: GenericError::InvalidInput,
                err_msg: msg.to_string(),
            };
        }
    }

    if search_string.contains("+") { // AND search
        search_strings = split_string(search_string.as_str(), "+").await;
        search_type = "and".to_string();
    } else if search_string.contains("|") { // OR search
        search_strings = split_string(search_string.as_str(), "|").await;
        search_type = "or".to_string();
    } else { // default search, which means (search_string) does not contain either '+' or '|'
        search_type = "or".to_string();
        search_strings = Vec::from([search_string]);
    }

    // below sanitize_string(...) will ensure to remove
    // all characters except these : "_./-@,#:;"
    // so that way, DB does not get any characters that are not needed
    // let mut search_strings_with_leading_and_trailing_spaces_removed: Vec<String> = search_strings.iter().map(|s| remove_leading_and_trailing_spaces(s)).collect();

    let futures_1: Vec<_> = search_strings.iter().map(|s| async {
        remove_leading_and_trailing_spaces(s).await
    }).collect();
    search_strings_with_leading_and_trailing_spaces_removed = futures::future::join_all(futures_1).await;

    println!("@ length of search_strings_with_leading_and_trailing_spaces_removed >> {}", search_strings_with_leading_and_trailing_spaces_removed.len());

    // //////////////////////////

    // sanitized_search_strings = search_strings_with_leading_and_trailing_spaces_removed.iter().map(|s| sanitize_string(s)).collect();

    let futures_2: Vec<_> = search_strings_with_leading_and_trailing_spaces_removed.iter().map(|s| async {
        sanitize_string(s).await
    }).collect();
    sanitized_search_strings = futures::future::join_all(futures_2).await;

    println!("@ length of sanitized_search_strings >> {}", sanitized_search_strings.len());

    // //////////////////////////

    println!("sanitized_search_strings: {:#?}", sanitized_search_strings);

    println!("@ length of sanitized_search_strings >> {}", sanitized_search_strings.len());

    let result = SearchStringData {
        search_string: sanitized_search_strings,
        search_type,
    };

    Ok(result)
}

/* ************************************************************************************* */

fn handle_deserialize_error(err: actix_web::error::JsonPayloadError, _: &actix_web::HttpRequest) -> actix_web::Error {
    let detail = err.to_string();
    if let error::JsonPayloadError::Deserialize(ref serde_err) = err {
        match serde_err.classify() {
            Category::Data => {
                // Handle missing field or other data-related errors
                return error::InternalError::from_response(
                    "",
                    HttpResponse::BadRequest().content_type("application/json").body("{\"response\":\"invalid request, please check payload\"}")
                ).into();
            },
            _ => (),
        }
    }

    // Fallback: handle other kinds of errors
    error::InternalError::from_response(
        err,
        // HttpResponse::InternalServerError().json("Internal server error")
        HttpResponse::InternalServerError().content_type("application/json").body("{\"response\":\"invalid request\"}")
    ).into()
}

/* ************************************************************************************* */


async fn handle_post(item: web::Json<ExportData>) -> impl Responder {
    println!("Received : [ search_string : ({}) ] , [ table_name : ({}) ], [ pattern_match : ({}) ]", item.search_string, item.table_name, item.pattern_match);

    let search_type = get_valid_search_strings(item.search_string.as_str()).await.unwrap().search_type;
    println!("search_type : {}", search_type);

    let search_strings = get_valid_search_strings(item.search_string.as_str()).await.unwrap().search_string;
    println!("search_strings : {:#?}", search_strings);

    let backend_table = get_backend_table(item.table_name.as_str()).await;
    println!("backend_table : {}", backend_table);

    let columns = get_backend_table_columns(item.table_name.as_str()).await;
    println!("columns : {:#?}", columns);

    let my_db_pool = get_db_pool_for_table(item.table_name.as_str()).await.unwrap();

    // if search_strings.len() == 0 {
    //     if let Err(e) = export_table_to_csv(my_db_pool, "").await {
    //         eprintln!("failed to export table : {:#?}", e);
    //     };
    // }

    let my_table = item.table_name.as_str();

    let result = match export_table_to_csv(my_db_pool, my_table, columns, search_strings, item.pattern_match.to_string(), search_type).await {
        Ok(d) => {
            let response_data = JsonResponseWithCSVExportData {
                message: d.csv_file_path,
                status: 200,
                rows: d.rows,
                time_taken_for_export: d.time_taken_for_export,
            };
            web::Json(response_data)
        },
        Err(e) => {
            println!("error : could not export CSV file : {:#?}", e);
            let response_data = JsonResponseWithCSVExportData {
                message: "error : could not export CSV file".to_string(),
                status: 400,
                rows: 0,
                time_taken_for_export: 0.0,
            };
            web::Json(response_data)
        },
    };

    web::Json(result)
}
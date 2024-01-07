use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use std::error::Error as StdError;
use std::fmt::{Display, Formatter};
use actix_web::{HttpResponse, ResponseError};
use serde_json::Error as SerdeError;
use derive_more::{Display, From};

#[derive(Serialize, Deserialize, Debug)]
pub struct TRandom {
    pub random_num: i32,
    pub random_float: f64,
    pub md5: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Data1 {
    // Define your data structure
    pub random_num: i32,
    pub random_float: f64,
    pub md5: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Data2 {
    // Define your data structure
    pub my_date: String,
    pub my_data: String,
}


#[derive(Serialize, Deserialize, Debug)]
pub struct PaginationParams {
    pub page: u32,
    pub page_size: u32,
}

// -----------------------
#[derive(Debug)]
pub enum GenericError {
    InvalidInput,
}

#[derive(Debug)]
pub struct CustomErrorType {
    pub err_type: GenericError,
    pub err_msg: String
}

// -----------------------

#[derive(Serialize, Deserialize, Debug)]
pub struct QueryParams {
    // pagination: PaginationParams,
    // page: u32,
    // page_size: u32,
    pub start: u32,
    pub length: u32,
    pub draw: u32,
}

#[derive(Deserialize)]
pub struct FormData {
    // No specific fields defined
    // Allows capturing all fields dynamically
    #[serde(flatten)]
    pub fields: HashMap<String, String>,
}

#[derive(Deserialize, Serialize)]
pub struct ExportData {
    pub search_string: String,  // values : any search string >> examples : 'xyz' (or) '1234' (or) 'xyz | 123' (or) 'xxx + yyy'
    pub table_name: String,     // valid values >> 'table1'
    pub pattern_match: String,  // valid values >> 'like' | 'exact'
}


#[derive(Deserialize, Serialize)]
pub struct SearchStringData {
    pub search_string: Vec<String>,  // a vector of search strings
    pub search_type: String,         // valid values >> 'and' | 'or'
}

#[derive(Serialize)]
pub struct JsonResponse {
    pub message: String,
    pub status: u32,
}

#[derive(Serialize)]
pub struct JsonResponseWithCSVExportData {
    pub message: String,
    pub status: u32,
    pub rows: i32,
    pub time_taken_for_export: f64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ExportResults {
    pub csv_file_path: String,
    pub rows: i32,
    pub time_taken_for_export: f64,
}



#[derive(Display, From, Debug)]
pub enum CustomError {
    DatabaseError,
    InvalidData,
    QueryError,
    InvalidTable,
}


impl std::error::Error for CustomError {}

impl ResponseError for CustomError {
    fn error_response(&self) -> HttpResponse {
        match *self {
            CustomError::DatabaseError => HttpResponse::InternalServerError().finish(),
            CustomError::InvalidData => HttpResponse::BadRequest().finish(),
            CustomError::QueryError => HttpResponse::BadRequest().finish(),
            CustomError::InvalidTable => HttpResponse::BadRequest().finish(),
        }
    }
}

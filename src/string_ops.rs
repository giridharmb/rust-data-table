use regex::Regex;

pub fn get_sanitized_string(text: &str) -> String {
    let re = Regex::new(r"[^a-zA-Z0-9-._@/,;:\s]+").unwrap();
    let result = re.replace_all(&text, "");
    result.to_string()
}

/* ************************************************************************************* */

pub async fn sanitize_string(input: &str) -> String {
    input.chars()
        .filter(|&c| c.is_ascii_alphanumeric() || "_./-@,#:;".contains(c))
        .collect()
}

pub async fn remove_leading_trailing_characters(input: &str) -> String {
    input.trim_start_matches(",").trim_end_matches(",").to_string()
}

pub async fn replace_multiple_characters(input: &str) -> String {
    let re = Regex::new(r",+").unwrap();
    re.replace_all(input, ",").to_string()
}

pub async fn split_string(input: &str, split_char: &str) -> Vec<String> {
    input.split(split_char).map(|s| s.to_string()).collect()
}

pub async fn remove_leading_and_trailing_spaces(my_str: &str) -> String {
    my_str.trim_start().trim_end().to_string()
}

use regex::Regex;
use std::ffi::CString;

pub fn to_cstring(buffer: Vec<u8>) -> String {
    let str = unsafe { CString::from_vec_unchecked(buffer) }
        .to_string_lossy()
        .to_string();
    return str;
}

pub fn hexstring_to_bytes(hexstr: &str) -> Vec<u8> {
    let re = Regex::new(r"((\d|[a-f]){2})").unwrap();
    let mut bytes: Vec<u8> = vec![];
    for cap in re.captures_iter(hexstr) {
        bytes.push(u8::from_str_radix(cap.get(1).unwrap().as_str(), 16).unwrap());
    }
    return bytes;
}

pub fn hexdump_to_bytes(op_msg_hexstr: &str) -> Vec<u8> {
    let re = Regex::new(r"\d{4}\s{3}(((\d|[a-f]){2}\s)+)\s{2}.*").unwrap();
    let mut bytes: Vec<u8> = vec![];
    for cap in re.captures_iter(op_msg_hexstr) {
        bytes.extend(hexstring_to_bytes(cap.get(1).unwrap().as_str()));
    }
    return bytes;
}

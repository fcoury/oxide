use std::ffi::CString;

pub fn to_cstring(buffer: Vec<u8>) -> String {
    let str = unsafe { CString::from_vec_unchecked(buffer) }
        .to_string_lossy()
        .to_string();
    return str;
}

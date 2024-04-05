use std::ffi::{CStr, CString};
use std::ptr::null_mut;

use libc::c_char;

use meos_sys as ffi;

pub fn init() {
    unsafe {
        ffi::meos_initialize(null_mut(), None);
    }
}

pub fn finalize() {
    unsafe {
        ffi::meos_finalize();
    }
    eprintln!("finalized")
}

fn c_str_to_slice(c: &*const c_char) -> Option<&str> {
    if c.is_null() {
        None
    } else {
        std::str::from_utf8(unsafe { CStr::from_ptr(*c).to_bytes() }).ok()
    }
}

fn to_c_str(n: &str) -> CString {
    CString::new(n.as_bytes()).unwrap()
}

pub mod tgeo;

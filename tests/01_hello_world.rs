use std::error::Error;
use std::ffi::{c_char, CString};
use std::ptr::null_mut;

// todo;; safe interface
use meos_sys::*;

const INST_WKT: &str = "POINT(1 1)@2000-01-01";

#[test]
fn main() -> Result<(), Box<dyn Error>> {
    unsafe {
        meos_initialize(null_mut(), None);
        let c_str = CString::new(INST_WKT).unwrap();
        let c_world: *const c_char = c_str.as_ptr() as *const c_char;
        let t = tgeompoint_in(c_world);

        let inst_mfjson = temporal_as_mfjson(t, true, 3, 6, null_mut());
        let inst_mfjson = CString::from_raw(inst_mfjson);

        println!("--------------------");
        println!("| Temporal Instant |");
        println!("--------------------\n");
        println!("WKT:");
        println!("----\n{INST_WKT}\n");
        println!("MF-JSON:");
        println!("--------\n{}", inst_mfjson.into_string().expect("foo"));
    }

    Ok(())
}

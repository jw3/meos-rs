use crate::{c_str_to_slice, try_cstr_to_str, TPtrCtr, Type};
use libc::{c_char, free};
use meos_sys as ffi;
use meos_sys::WKB_EXTENDED;

use crate::error::Error;
use std::ptr::null_mut;

#[allow(private_bounds)]
pub trait Temporal: TPtrCtr {
    fn from_wkt(wkt: &str) -> Result<Self, Error>
    where
        Self: Sized;

    fn ttype(&self) -> Type;

    fn to_mf_json(&self) -> Result<String, Error> {
        unsafe { try_cstr_to_str(ffi::temporal_as_mfjson(self.ptr(), true, 0, 6, null_mut())) }
    }

    fn as_bytes(&self) -> &[u8] {
        let mut szout: usize = 0;
        unsafe {
            let bytes =
                ffi::temporal_as_wkb(self.ptr(), WKB_EXTENDED as u8, &mut szout) as *const u8;
            std::slice::from_raw_parts(bytes, szout)
        }
    }

    fn as_hex(&self) -> Option<String> {
        let mut szout: usize = 0;
        unsafe {
            let bytes = ffi::temporal_as_hexwkb(self.ptr().cast(), WKB_EXTENDED as u8, &mut szout);
            let r = c_str_to_slice(&(bytes as *const c_char)).map(|s| s.to_owned());
            free(bytes.cast());
            r
        }
    }

    fn as_json(&self) -> Option<String> {
        unsafe {
            let bytes = ffi::temporal_as_mfjson(self.ptr().cast(), false, 0, 6, null_mut());
            let r = c_str_to_slice(&(bytes as *const c_char)).map(|s| s.to_owned());
            free(bytes.cast());
            r
        }
    }

    /// returns the starting timestamp
    fn ts(&self) -> i64 {
        unsafe { ffi::temporal_start_timestamptz(self.ptr()) }
    }
}

// ----------------------------------

// temporal pointer to a boxed iface
// fn temp_from(p: NonNull<ffi::Temporal>) -> Box<dyn Temporal> {
//     let t: ffi::tempSubtype = unsafe { (*p.as_ptr()).subtype.into() };
//     match t {
//         ffi::tempSubtype_TINSTANT => Box::new(TInst { ptr: p.cast() }),
//         ffi::tempSubtype_TSEQUENCE => Box::new(TSeq { ptr: p.cast() }),
//         ffi::tempSubtype_TSEQUENCESET => Box::new(TSet { ptr: p.cast() }),
//         _ => unreachable!("invalid tempSubtype: probably ANYTEMPSUBTYPE"),
//     }
// }

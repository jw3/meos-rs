use std::ffi::{CStr, CString};
use std::fmt::{Display, Formatter};
use std::ptr::null_mut;

use libc::{c_char, free};

use crate::error::Error;
use crate::error::Error::FfiStringError;
use meos_sys as ffi;

mod error;
pub mod prelude;
pub mod set;
pub mod span;
pub mod tbox;
mod temp;
mod tinst;
mod tseq;
mod tset;
pub mod tz;

pub(crate) type TPtr = *mut ffi::Temporal;

pub(crate) trait TPtrCtr {
    fn ptr(&self) -> TPtr;
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum Type {
    Instant,
    Sequence,
    SequenceSet,
}

impl Display for Type {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Type::Instant => "Instant",
            Type::Sequence => "Sequence",
            Type::SequenceSet => "SequenceSet",
        };
        f.write_str(s)
    }
}

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

fn to_c_str(n: &str) -> Result<CString, Error> {
    CString::new(n.as_bytes()).map_err(|e| FfiStringError(format!("{e}")))
}

fn try_cstr_to_str(p: *mut c_char) -> Result<String, Error> {
    unsafe {
        let cstr = CStr::from_ptr(p);
        let s = CString::new(cstr.to_bytes())
            .map_err(|e| FfiStringError(format!("{e}")))?
            .into_string()
            .map_err(|e| FfiStringError(format!("{e}")))?;
        // todo;; error will short circuit the free
        free(p.cast());
        Ok(s)
    }
}

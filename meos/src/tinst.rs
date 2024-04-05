use crate::temp::Temporal;
use crate::{TPtrCtr, Type};
use libc::free;
use meos_sys as ffi;
use std::cmp::Ordering;
use std::ffi::CString;
use std::ptr::NonNull;
#[derive(Eq)]
pub struct TInst {
    ptr: NonNull<ffi::TInstant>,
}

impl TPtrCtr for TInst {
    fn ptr(&self) -> *mut meos_sys::Temporal {
        self.ptr.as_ptr().cast()
    }
}
impl Temporal for TInst {
    fn from_wkt(wkt: &str) -> Result<Self, ()> {
        unsafe {
            let cstr = CString::new(wkt).map_err(|_| ())?;
            let ptr = ffi::tgeompoint_in(cstr.as_ptr());
            if ptr.is_null() {
                return Err(());
            }
            let t = Self {
                ptr: NonNull::new(ptr).unwrap().cast(),
            };
            if (*t.ptr.as_ptr()).subtype == ffi::tempSubtype_TINSTANT as u8 {
                Ok(t)
            } else {
                return Err(());
            }
        }
    }

    fn ttype(&self) -> Type {
        Type::Instant
    }
}

impl Drop for TInst {
    fn drop(&mut self) {
        unsafe {
            free(self.ptr.as_ptr().cast());
        }
    }
}

impl PartialEq for TInst {
    fn eq(&self, other: &Self) -> bool {
        self.ttype() == other.ttype() && unsafe { ffi::temporal_eq(self.ptr(), other.ptr()) }
    }
}

impl PartialOrd for TInst {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.ts().partial_cmp(&other.ts())
    }
}
impl Ord for TInst {
    fn cmp(&self, other: &Self) -> Ordering {
        self.ts().cmp(&other.ts())
    }
}

impl TInst {}

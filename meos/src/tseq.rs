use crate::error::Error;
use crate::error::Error::{MeosError, WrongTemporalType};
use crate::temp::Temporal;
use crate::{to_c_str, TPtr, TPtrCtr, Type};
use libc::free;
use meos_sys as ffi;
use std::cmp::Ordering;
use std::ffi::CString;
use std::ptr::NonNull;
use std::str::Utf8Error;

#[derive(Eq)]
pub struct TSeq {
    ptr: NonNull<ffi::TSequence>,
}
impl TPtrCtr for TSeq {
    fn ptr(&self) -> *mut meos_sys::Temporal {
        self.ptr.as_ptr().cast()
    }
}
impl Temporal for TSeq {
    fn from_wkt(wkt: &str) -> Result<Self, Error>
    where
        Self: Sized,
    {
        unsafe {
            let cstr = to_c_str(wkt)?;
            let ptr = ffi::tgeompoint_in(cstr.as_ptr());
            if ptr.is_null() {
                // todo;; check the meos error
                return Err(MeosError(-999));
            }
            let t = Self {
                ptr: NonNull::new(ptr).unwrap().cast(),
            };
            if (*t.ptr.as_ptr()).subtype == ffi::tempSubtype_TSEQUENCE as u8 {
                Ok(t)
            } else {
                return Err(WrongTemporalType);
            }
        }
    }

    fn ttype(&self) -> Type {
        Type::Sequence
    }
}

impl TSeq {
    pub fn make<T: Temporal>(ts: &Vec<T>) -> Option<Self> {
        let v: Vec<TPtr> = ts.iter().map(|t| t.ptr()).collect();
        let arr = v.as_slice();
        let p = unsafe {
            ffi::tsequence_make(
                arr.as_ptr() as *mut *const ffi::TInstant,
                arr.len().try_into().unwrap(),
                true,
                true,
                ffi::interpType_LINEAR,
                false,
            )
        };
        NonNull::new(p).map(|p| TSeq { ptr: p })
    }

    fn out(&self) -> Result<String, Utf8Error> {
        unsafe {
            let temp_out = ffi::tsequence_out(self.ptr().cast(), 15);
            let x = CString::from_raw(temp_out);
            x.to_str().map(|x| x.to_owned())
        }
    }
}

impl Drop for TSeq {
    fn drop(&mut self) {
        unsafe {
            free(self.ptr.as_ptr().cast());
        }
    }
}

impl PartialEq for TSeq {
    fn eq(&self, other: &Self) -> bool {
        self.ttype() == other.ttype() && unsafe { ffi::temporal_eq(self.ptr(), other.ptr()) }
    }
}

impl PartialOrd for TSeq {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.ts().partial_cmp(&other.ts())
    }
}
impl Ord for TSeq {
    fn cmp(&self, other: &Self) -> Ordering {
        self.ts().cmp(&other.ts())
    }
}

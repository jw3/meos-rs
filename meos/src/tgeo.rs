use crate::c_str_to_slice;
use libc::{c_char, free};
use meos_sys as ffi;
use meos_sys::WKB_EXTENDED;
use std::cmp::Ordering;
use std::error::Error;
use std::ffi::{CStr, CString};
use std::fmt::{Display, Formatter};
use std::ptr::{null_mut, NonNull};
use std::str::Utf8Error;

type TPtr = *mut ffi::Temporal;

trait TPtrCtr {
    fn ptr(&self) -> TPtr;
}

#[allow(private_bounds)]
pub trait Temporal: TPtrCtr {
    fn from_wkt(wkt: &str) -> Result<Self, ()>
    where
        Self: Sized;

    fn ttype(&self) -> Type;

    fn to_mf_json(&self) -> Result<String, Box<dyn Error>> {
        unsafe {
            let p = ffi::temporal_as_mfjson(self.ptr(), true, 0, 6, null_mut());
            let cstr = CStr::from_ptr(p);
            let cstring = CString::new(cstr.to_bytes())?.into_string()?;
            free(p.cast());
            Ok(cstring)
        }
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
    fn from_wkt(wkt: &str) -> Result<Self, ()>
    where
        Self: Sized,
    {
        unsafe {
            let cstr = CString::new(wkt).map_err(|_| ())?;
            let ptr = ffi::tgeompoint_in(cstr.as_ptr());
            if ptr.is_null() {
                return Err(());
            }
            let t = Self {
                ptr: NonNull::new(ptr).unwrap().cast(),
            };
            if (*t.ptr.as_ptr()).subtype == ffi::tempSubtype_TSEQUENCE as u8 {
                Ok(t)
            } else {
                return Err(());
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

#[derive(Eq)]
pub struct TSet {
    ptr: NonNull<ffi::TSequenceSet>,
}

impl TPtrCtr for TSet {
    fn ptr(&self) -> *mut meos_sys::Temporal {
        self.ptr.as_ptr().cast()
    }
}

impl Temporal for TSet {
    fn from_wkt(wkt: &str) -> Result<Self, ()>
    where
        Self: Sized,
    {
        unsafe {
            let cstr = CString::new(wkt).map_err(|_| ())?;
            let ptr = ffi::tgeompoint_in(cstr.as_ptr());
            if ptr.is_null() {
                return Err(());
            }
            let t = Self {
                ptr: NonNull::new(ptr).unwrap().cast(),
            };
            if (*t.ptr.as_ptr()).subtype == ffi::tempSubtype_TSEQUENCESET as u8 {
                Ok(t)
            } else {
                return Err(());
            }
        }
    }

    fn ttype(&self) -> Type {
        Type::SequenceSet
    }
}

impl TSet {}

impl Drop for TSet {
    fn drop(&mut self) {
        unsafe {
            free(self.ptr.as_ptr().cast());
        }
    }
}

impl PartialEq for TSet {
    fn eq(&self, other: &Self) -> bool {
        self.ttype() == other.ttype() && unsafe { ffi::temporal_eq(self.ptr(), other.ptr()) }
    }
}

impl PartialOrd for TSet {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.ts().partial_cmp(&other.ts())
    }
}
impl Ord for TSet {
    fn cmp(&self, other: &Self) -> Ordering {
        self.ts().cmp(&other.ts())
    }
}

// ----------------------------------

// temporal pointer to a boxed iface
fn temp_from(p: NonNull<ffi::Temporal>) -> Box<dyn Temporal> {
    let t: ffi::tempSubtype = unsafe { (*p.as_ptr()).subtype.into() };
    match t {
        ffi::tempSubtype_TINSTANT => Box::new(TInst { ptr: p.cast() }),
        ffi::tempSubtype_TSEQUENCE => Box::new(TSeq { ptr: p.cast() }),
        ffi::tempSubtype_TSEQUENCESET => Box::new(TSet { ptr: p.cast() }),
        _ => unreachable!("invalid tempSubtype: probably ANYTEMPSUBTYPE"),
    }
}

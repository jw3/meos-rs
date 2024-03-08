use ffi::{Temporal, WKB_EXTENDED, WKB_NDR};
use libc::free;
use meos_sys as ffi;
use std::error::Error;
use std::ffi::{CStr, CString};
use std::fmt::{Display, Formatter};
use std::ptr::null_mut;
use std::str::Utf8Error;

pub const WKB_VARIANT: u8 = 0u8 | WKB_NDR as u8 | WKB_EXTENDED as u8  /* | WKB_HEX*/;

pub fn init() {
    unsafe {
        ffi::meos_initialize(null_mut(), None);
    }
}

pub fn finalize() {
    unsafe {
        ffi::meos_finalize();
    }
}
pub type TGeomPtr = *mut ffi::Temporal;
pub struct TGeom {
    pub ptr: *mut ffi::Temporal,
}

impl TGeom {
    /// format: wkt-point@wkt-time
    pub fn new(wkt: &str) -> Result<Self, ()> {
        let ptr = unsafe {
            let cstr = CString::new(wkt).map_err(|_| ())?;
            let ptr = ffi::tgeompoint_in(cstr.as_ptr());
            if ptr.is_null() {
                return Err(());
            }
            ptr
        };
        Ok(TGeom { ptr })
    }

    pub fn make(lat: f64, lon: f64, t: String, srid: u32) -> Result<String, Box<dyn Error>> {
        unsafe {
            let t_ptr = CString::new(t.clone())?;
            let ts = ffi::pg_timestamp_in(t_ptr.as_ptr(), -1);
            let t_out = ffi::pg_timestamp_out(ts);
            let t_str = CString::from_raw(t_out);
            Ok(format!(
                "SRID={};Point({} {})@{}+00",
                srid,
                lon,
                lat,
                t_str.to_str()?
            ))
        }
    }

    pub fn ttype(&self) -> TemporalSubtype {
        let mt: ffi::tempSubtype = unsafe { (*self.ptr).subtype.into() };
        TemporalSubtype::from(mt)
    }
}
impl Drop for TGeom {
    fn drop(&mut self) {
        unsafe {
            free(self.ptr.cast());
        }
    }
}

pub struct TPointBuf {
    lat: f64,
    lon: f64,
    t: String,
    srid: u32,
}

impl TPointBuf {
    pub fn new(lat: f64, lon: f64, t: String, srid: u32) -> Self {
        Self { lat, lon, t, srid }
    }

    pub fn formatted(&self) -> Result<String, Box<dyn Error>> {
        unsafe {
            let t_ptr = CString::new(self.t.clone())?;
            let ts = ffi::pg_timestamp_in(t_ptr.as_ptr(), -1);
            let t_out = ffi::pg_timestamp_out(ts);
            let t_str = CString::from_raw(t_out);
            Ok(format!(
                "SRID={};Point({} {})@{}+00",
                self.srid,
                self.lon,
                self.lat,
                t_str.to_str()?
            ))
        }
    }
}

pub fn to_mf_json(t: &TGeom) -> Result<String, Box<dyn Error>> {
    unsafe {
        let p = ffi::temporal_as_mfjson(t.ptr, true, 0, 6, null_mut());
        let cstr = CStr::from_ptr(p);
        let cstring = CString::new(cstr.to_bytes())?.into_string()?;
        free(p.cast());
        Ok(cstring)
    }
}

pub struct TSeq {
    p: *mut ffi::TSequence,
}

impl TSeq {
    pub fn make(gs: Vec<TGeom>) -> Self {
        let v: Vec<TGeomPtr> = gs.iter().map(|g| g.ptr).collect();
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
        TSeq { p }
    }

    pub fn out(&self) -> Result<String, Utf8Error> {
        unsafe {
            let temp_out = ffi::tsequence_out(self.p, 15);
            let x = CString::from_raw(temp_out);
            x.to_str().map(|x| x.to_owned())
        }
    }

    pub fn as_bytes(&self) -> &[u8] {
        let mut szout: usize = 0;
        unsafe {
            let bytes =
                ffi::temporal_as_wkb(self.p as *const Temporal, crate::WKB_VARIANT, &mut szout)
                    as *const u8;
            std::slice::from_raw_parts(bytes, szout)
        }
    }
}

impl Drop for TSeq {
    fn drop(&mut self) {
        unsafe { free(self.p.cast()) }
    }
}

#[derive(Copy, Clone)]
pub enum TemporalSubtype {
    TAny,
    TInstant,
    TSequence,
    TSequenceSet,
}

impl From<ffi::tempSubtype> for TemporalSubtype {
    fn from(value: ffi::tempSubtype) -> Self {
        use TemporalSubtype::*;
        match value {
            ffi::tempSubtype_ANYTEMPSUBTYPE => TAny,
            ffi::tempSubtype_TINSTANT => TInstant,
            ffi::tempSubtype_TSEQUENCE => TSequence,
            ffi::tempSubtype_TSEQUENCESET => TSequenceSet,
            _ => unreachable!("invalid tempSubtype"),
        }
    }
}

impl Display for TemporalSubtype {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        use TemporalSubtype::*;
        f.write_str(match self {
            TAny => "Any",
            TInstant => "Instant",
            TSequence => "Sequence",
            TSequenceSet => "SequenceSet",
        })
    }
}

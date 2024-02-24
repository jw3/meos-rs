use meos_sys;
use meos_sys::{
    free, meos_initialize, pg_timestamp_in, pg_timestamp_out, tgeompoint_in, TSequence, Temporal,
};
use std::error::Error;
use std::ffi::CString;
use std::ptr::null_mut;
use std::time::Instant;

pub fn init() {
    unsafe {
        meos_initialize(null_mut(), None);
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
            let ts = unsafe { pg_timestamp_in(t_ptr.as_ptr(), -1) };
            let t_out = pg_timestamp_out(ts);
            let t_str = CString::from_raw(t_out);
            Ok(format!(
                "SRID=4326;Point({} {})@{}+00",
                self.lon,
                self.lat,
                t_str.to_str()?
            ))
        }
    }
}

struct TPoint {
    p: *mut Temporal,
}

impl TPoint {
    pub fn make(pb: String) -> Self {
        let pb_ptr = CString::new(pb).expect("CString");
        unsafe {
            let p = tgeompoint_in(pb_ptr.as_ptr());
            Self { p }
        }
    }
}

impl Drop for TPoint {
    fn drop(&mut self) {
        unsafe { free(self.p.cast()) }
    }
}

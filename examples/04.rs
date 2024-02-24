use meos::TPointBuf;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::error::Error;
use std::ffi::{c_char, CString};
use std::fs::File;
use std::ptr::null_mut;

// todo;; safe interface
use meos_sys::*;

const INST_WKT: &str = "POINT(1 1)@2000-01-01";

#[derive(Debug, serde::Deserialize)]
struct AisRecord {
    t: String,
    mmsi: i64,
    latitude: f64,
    longitude: f64,
    sog: f64,
}

struct TripRecord {
    mmsi: i64,
    trip: *mut TSequence,
}

impl Drop for TripRecord {
    fn drop(&mut self) {
        unsafe {
            free(self.trip.cast());
        }
    }
}

// Number of instants to send in batch to the file
// DO NOT OVERFLOW
const INSTANTS_BATCH_SIZE: i32 = 100;

// Number of instants to keep when restarting a sequence
const INSTANT_KEEP_COUNT: i32 = 2;

fn main() -> Result<(), Box<dyn Error>> {
    meos::init();

    let file = File::open("tests/data/ais_instants_med.csv")?;
    let mut rdr = csv::Reader::from_reader(file);

    let mut i = 0;
    let mut trips: HashMap<i64, TripRecord> = HashMap::new();
    for result in rdr.deserialize() {
        i += 1;
        let record: AisRecord = result?;
        let mmsi = record.mmsi;

        unsafe {
            // todo;; need some kind of geom representation, geom -> temporal
            let pb = TPointBuf::new(record.latitude, record.longitude, record.t, 4326);

            // todo;; this is replacd by TPoint
            let gp_ptr = CString::new(pb.formatted()?)?;
            let inst = tgeompoint_in(gp_ptr.as_ptr());
            match trips.entry(record.mmsi) {
                Entry::Occupied(mut t) => {
                    let mut r = t.get_mut();
                    if (*r.trip).count == INSTANTS_BATCH_SIZE {
                        print!("[{i}]{mmsi},");
                        tsequence_restart(r.trip, INSTANT_KEEP_COUNT);
                    }

                    // todo;; append needs done by a wrapper that frees the underlying mem
                    //        trip.append(inst, maxd, madt, expand)
                    r.trip = tsequence_append_tinstant(
                        r.trip,
                        inst as *mut TInstant,
                        0.0,
                        null_mut(),
                        true,
                    )
                    .cast();
                }
                Entry::Vacant(t) => {
                    let arr = [inst];
                    // todo;; needs to create a wrapper
                    //        Trip::new(inst_arr, cnt, max_size, ....)
                    let trip = tsequence_make_exp(
                        arr.as_ptr() as *mut *const TInstant,
                        1,
                        INSTANTS_BATCH_SIZE,
                        true,
                        true,
                        interpType_LINEAR,
                        false,
                    );

                    let r = TripRecord {
                        mmsi,
                        trip: trip.cast(),
                    };

                    t.insert(r);
                }
            }
            // todo;; --fixed-- leak
            free(inst.cast());
        }
    }

    println!("Total trips: {}", trips.len());

    unsafe {
        // todo;; --fixed-- leak
        meos_finalize();
    }
    println!("FINALIZED");

    Ok(())
}

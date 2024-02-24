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
        //print!("{i} [");
        i += 1;
        let record: AisRecord = result?;
        let mmsi = record.mmsi;

        unsafe {
            let pb = TPointBuf::new(record.latitude, record.longitude, record.t, 4326);

            //print!("{mmsi} - {point_buffer}");
            let gp_ptr = CString::new(pb.formatted()?)?;
            match trips.entry(record.mmsi) {
                Entry::Occupied(mut t) => {
                    let mut r = t.get_mut();
                    if (*r.trip).count == INSTANTS_BATCH_SIZE {
                        // let temp = tsequence_out(r.trip, INSTANTS_BATCH_SIZE);
                        // let temp = CString::from_raw(temp);
                        // //println!("{mmsi} {}", temp.to_str()?);
                        print!("{i},");
                        tsequence_restart(r.trip, INSTANT_KEEP_COUNT);
                    }

                    let inst = tgeompoint_in(gp_ptr.as_ptr());
                    let prev = r.trip;
                    r.trip = tsequence_append_tinstant(
                        r.trip,
                        inst as *mut TInstant,
                        0.0,
                        null_mut(),
                        true,
                    )
                    .cast();
                    free(inst.cast());
                    //free(prev.cast());
                }
                Entry::Vacant(t) => {
                    //  TInstant *inst = (TInstant *) tgeogpoint_in(point_buffer);
                    let inst = tgeompoint_in(gp_ptr.as_ptr());

                    let arr = [inst];
                    let trip = tsequence_make_exp(
                        arr.as_ptr() as *mut *const TInstant,
                        1,
                        INSTANTS_BATCH_SIZE,
                        true,
                        true,
                        interpType_LINEAR,
                        false,
                    );

                    let ttrip = tsequence_append_tinstant(
                        trip,
                        inst as *mut TInstant,
                        0.0,
                        null_mut(),
                        true,
                    );
                    // todo;; --fixed-- leak
                    free(trip.cast());
                    free(inst.cast());
                    let r = TripRecord {
                        mmsi,
                        trip: ttrip.cast(),
                    };

                    t.insert(r);
                }
            }
        }
        //println!("]");
    }

    println!("Total trips: {}", trips.len());

    unsafe {
        meos_finalize();
    }
    println!("FINALIZED");

    Ok(())
}

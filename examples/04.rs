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

const NO_INSTANTS_BATCH: i32 = 1000;

fn main() -> Result<(), Box<dyn Error>> {
    unsafe {
        meos_initialize(null_mut(), None);
    }
    let file = File::open("tests/data/ais_instants.csv")?;
    let mut rdr = csv::Reader::from_reader(file);

    let mut i = 0;
    let mut trips: HashMap<i64, TripRecord> = HashMap::new();
    for result in rdr.deserialize() {
        print!("{i} [");
        i += 1;
        let record: AisRecord = result?;
        let mmsi = record.mmsi;
        let t_ptr = CString::new(record.t)?;
        let ts = unsafe { pg_timestamp_in(t_ptr.as_ptr(), -1) };

        unsafe {
            let t_out = pg_timestamp_out(ts);
            let t_out = CString::from_raw(t_out);
            let point_buffer = format!(
                "SRID=4326;Point({} {})@{}+00",
                record.longitude,
                record.latitude,
                t_out.to_str()?
            );
            print!("{mmsi} - {point_buffer}");
            let gp_ptr = CString::new(point_buffer)?;
            match trips.entry(record.mmsi) {
                Entry::Occupied(t) => {
                    let r = t.get();
                    let inst = tgeompoint_in(gp_ptr.as_ptr());
                    if !inst.is_null() && !r.trip.is_null() {
                        tsequence_append_tinstant(
                            r.trip,
                            inst as *mut TInstant,
                            0.0,
                            null_mut(),
                            true,
                        );
                        print!(".");
                    } else {
                        print!("x")
                    }
                }
                Entry::Vacant(t) => {
                    //  TInstant *inst = (TInstant *) tgeogpoint_in(point_buffer);
                    let inst = tgeompoint_in(gp_ptr.as_ptr());

                    let arr = [inst];
                    let trip = tsequence_make_exp(
                        arr.as_ptr() as *mut *const TInstant,
                        1,
                        NO_INSTANTS_BATCH * 1000,
                        true,
                        true,
                        interpType_LINEAR,
                        false,
                    );

                    let r = TripRecord { mmsi, trip };
                    tsequence_append_tinstant(trip, inst as *mut TInstant, 0.0, null_mut(), true);

                    t.insert(r);
                }
            }
        }
        println!("]");
    }

    println!("Total trips: {}", trips.len());

    Ok(())
}

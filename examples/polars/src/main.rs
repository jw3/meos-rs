use deadpool_postgres::{Config, ManagerConfig, RecyclingMethod, Runtime};
use meos::TPointBuf;
use polars::io::mmap::MmapBytesReader;
use polars::prelude::*;
use std::error::Error;
use std::ffi::{CStr, CString};
use std::fmt::format;
use std::ptr::null_mut;
use tokio_postgres::types::Type;
use tokio_postgres::{Client, NoTls};

use meos_sys::{
    free, interpType_LINEAR, pg_timestamp_in, pg_timestamp_out, tgeompoint_in,
    tsequence_append_tinstant, tsequence_make_exp, tsequence_out, tsequence_restart, TInstant,
    TSequence, Temporal,
};

struct Posit {
    ptr: *mut Temporal,
}

impl Drop for Posit {
    fn drop(&mut self) {
        unsafe {
            free(self.ptr.cast());
        }
    }
}

async fn create_trip_table(client: &Client) -> Result<(), tokio_postgres::Error> {
    //print!("+");
    client
        .batch_execute(
            &[
                "SELECT pg_catalog.set_config('search_path', '', false)",
                "DROP TABLE IF EXISTS ais.trips",
                "CREATE TABLE ais.trips (MMSI integer PRIMARY KEY, trip public.tgeogpoint)",
            ]
            .join(";"),
        )
        .await
}

const INSTANTS_BATCH_SIZE: i32 = 100;
const INSTANT_KEEP_COUNT: i32 = 2;

async fn insert_trip(client: &Client, mmsi: i32, trip: &str) -> Result<u64, tokio_postgres::Error> {
    let q = format!("INSERT INTO ais.trips (MMSI, trip) VALUES ({mmsi}, '{trip}') ON CONFLICT (MMSI) DO UPDATE SET trip = public.update(trips.trip, EXCLUDED.trip, true)");
    client.execute(&q, &[]).await
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    meos::init();

    let df = LazyCsvReader::new("/home/wassj/dev/data/AIS_2020_01_01.csv")
        .has_header(true)
        .finish()?;

    let mut cfg = Config::new();
    cfg.host = Some("localhost".to_string());
    cfg.port = Some(5432);

    // cfg.host = Some("compute-node.database.svc.cluster.local".to_string());
    // cfg.port = Some(55433);

    cfg.dbname = Some("ais".to_string());
    cfg.user = Some("hippo".to_string());
    cfg.password = Some("hippo".to_string());
    cfg.manager = Some(ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    });
    let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls).unwrap();
    tokio::spawn({
        let pool = pool.clone();
        async move {
            if let Err(e) = pool.get().await {
                eprintln!("connection error: {}", e);
            }
        }
    });

    let df = df
        .select([col("MMSI"), col("T"), col("LAT"), col("LON")])
        .group_by(["MMSI"])
        .agg([
            len(),
            col("T").sort(false),
            concat_str([col("LON"), col("LAT")], " ", true).alias("P"),
        ])
        .filter(col("len").gt(lit(1)))
        .sort("len", Default::default())
        .collect()?;

    let sz = df.height();
    if sz > 0 {
        let insert_statement = {
            let client = pool.get().await?;
            create_trip_table(&client).await?;
            // let statement =
            //     "INSERT INTO ais.trips (MMSI, trip) VALUES ($1, $2) ON CONFLICT (MMSI) DO UPDATE SET trip = public.update(trips.trip, EXCLUDED.trip, true)";
            // client.prepare(&statement).await.expect("prepare")
        };
        use AnyValue::*;
        if let [m, l, t, p] = df.get_columns() {
            for i in 0..sz {
                match (m.get(i)?, l.get(i)?, t.get(i)?, p.get(i)?) {
                    (Int64(mmsi), UInt32(len), List(ts), List(pt)) => {
                        println!("========== {i}: {mmsi} - {len} ==========");

                        let t = ts.get(0)?;
                        let p = pt.get(0)?;
                        let posit = make_posit(t.get_str().unwrap(), p.get_str().unwrap())?;

                        unsafe {
                            let arr = [posit.ptr];
                            let mut trip = unsafe {
                                tsequence_make_exp(
                                    arr.as_ptr() as *mut *const TInstant,
                                    1,
                                    INSTANTS_BATCH_SIZE,
                                    true,
                                    true,
                                    interpType_LINEAR,
                                    false,
                                )
                            };

                            for i in 1..len as usize {
                                let t = ts.get(i)?;
                                let p = pt.get(i)?;

                                // todo;; figure out how to remove dupes in df, the
                                //        trick is deriving the correct len afterwards
                                if t == ts.get(i - 1)? {
                                    continue;
                                }

                                // println!("\t {i}: Point({p})@{t}+00");
                                print!(".");

                                let posit = make_posit(t.get_str().unwrap(), p.get_str().unwrap())?;

                                if (*trip).count == INSTANTS_BATCH_SIZE {
                                    let temp_out = tsequence_out(trip, 15);
                                    let q_str = CString::from_raw(temp_out)
                                        .to_str()
                                        .expect("temp out qstr")
                                        .to_owned();

                                    // todo;; write q_str;
                                    let client = pool.get().await?;
                                    // client
                                    //     .execute(&insert_statement, &[&(mmsi as i32), &q_str])
                                    //     .await?;
                                    insert_trip(&client, mmsi as i32, &q_str)
                                        .await
                                        .expect("insert trip");

                                    tsequence_restart(trip, INSTANT_KEEP_COUNT);
                                    print!("+");
                                }

                                trip = tsequence_append_tinstant(
                                    trip,
                                    posit.ptr as *mut TInstant,
                                    0.0,
                                    null_mut(),
                                    true,
                                )
                                .cast();
                            }

                            let temp_out = tsequence_out(trip, 15);
                            let q_str = CString::from_raw(temp_out)
                                .to_str()
                                .expect("temp out qstr")
                                .to_owned();

                            // todo;; write q_str;
                            let client = pool.get().await?;
                            // client
                            //     .execute(&insert_statement, &[&(mmsi as i32), &q_str])
                            //     .await?;
                            insert_trip(&client, mmsi as i32, &q_str).await?;
                            println!(";")
                        }
                    }
                    x => {
                        println!("fail- {x:?}");
                    }
                }
            }
        }
    } else {
        unreachable!("col mismatch")
    }

    Ok(())
}

fn make_posit(t: &str, p: &str) -> Result<Posit, Box<dyn Error>> {
    unsafe {
        let t_ptr = CString::new(t)?;
        let ts = pg_timestamp_in(t_ptr.as_ptr(), -1);
        let t_out = pg_timestamp_out(ts);
        let t_str = CString::from_raw(t_out);
        let formatted = format!("SRID=4326;Point({p})@{t}+00");
        // println!("{formatted}");
        let gp_ptr = CString::new(formatted)?;
        Ok(Posit {
            ptr: tgeompoint_in(gp_ptr.as_ptr()),
        })
    }
}

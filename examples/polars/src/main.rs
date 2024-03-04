use deadpool_postgres::{Config, ManagerConfig, RecyclingMethod, Runtime};
use meos::*;
use polars::io::mmap::MmapBytesReader;
use polars::prelude::*;
use std::error::Error;
use std::ffi::{CStr, CString};
use std::fmt::format;
use std::ptr::null_mut;
use std::str::Utf8Error;
use tokio_postgres::types::Type;
use tokio_postgres::{Client, NoTls};

use meos_sys::{
    free, interpType_LINEAR, pg_timestamp_in, pg_timestamp_out, tgeompoint_in,
    tsequence_append_tinstant, tsequence_make, tsequence_make_exp, tsequence_out,
    tsequence_restart, TInstant, TSequence, Temporal,
};

type Posit = TGeom;

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

const INSTANTS_BATCH_SIZE: usize = 100;
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
                        let posit = make_posit(t.get_str().unwrap(), p.get_str().unwrap());

                        unsafe {
                            let source: Vec<_> = (0..len as usize)
                                .into_iter()
                                .map(|i| (ts.get(i).unwrap(), pt.get(i).unwrap()))
                                .collect();

                            for chunk in source.chunks(INSTANTS_BATCH_SIZE) {
                                let mut trip = vec![];

                                for (t, p) in chunk {
                                    // todo;; figure out how to remove dupes in df, the
                                    //        trick is deriving the correct len afterwards
                                    let ts = t.get_str().unwrap();
                                    if trip.last().is_some_and(|(pts, _)| pts == ts) {
                                        continue;
                                    }

                                    // println!("\t {i}: Point({p})@{t}+00");
                                    print!(".");
                                    trip.push((
                                        ts.to_string(),
                                        make_posit(ts, p.get_str().unwrap()),
                                    ));
                                }

                                // todo;; figure out how to remove dupes in df, and this map goes away
                                let seq = TSeq::make(trip.into_iter().map(|(_, g)| g).collect());
                                let q_str = seq.out()?;

                                // todo;; write q_str;
                                let client = pool.get().await?;
                                // client
                                //     .execute(&insert_statement, &[&(mmsi as i32), &q_str])
                                //     .await?;
                                insert_trip(&client, mmsi as i32, &q_str).await?;
                                println!(";")
                            }
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

fn make_posit(t: &str, p: &str) -> TGeom {
    let wkt = format!("SRID=4326;Point({p})@{t}+00");
    TGeom::new(&wkt).expect("")
}

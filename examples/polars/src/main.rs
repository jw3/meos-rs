use clap::Parser;
use deadpool_postgres::{Config, ManagerConfig, RecyclingMethod, Runtime};
use meos::*;
use meos_sys::Temporal;
use polars::prelude::*;
use std::error::Error;
use std::io;
use std::io::Write;
use std::time::Instant;
use tokio_postgres::{Client, NoTls};

async fn create_trip_table(client: &Client) -> Result<(), tokio_postgres::Error> {
    //print!("+");
    client
        .batch_execute(
            &[
                "SELECT pg_catalog.set_config('search_path', '', false)",
                "DROP TABLE IF EXISTS ais.trips",
                "CREATE TABLE ais.trips (MMSI integer PRIMARY KEY, trip public.tgeompoint)",
            ]
            .join(";"),
        )
        .await
}

async fn insert_trip(client: &Client, mmsi: i32, trip: &str) -> Result<u64, tokio_postgres::Error> {
    let q = format!("INSERT INTO ais.trips (MMSI, trip) VALUES ({mmsi}, '{trip}') ON CONFLICT (MMSI) DO UPDATE SET trip = public.update(trips.trip, EXCLUDED.trip, true)");
    client.execute(&q, &[]).await
}

/// Take AIS data from CSV to MobilityDB with Polars
///
/// ## The program will show simplified output
///
/// - `.` represents 500 vessels processed
///
/// - `+` represents 10,000 posits processed
#[derive(Clone, Debug, Parser)]
struct Opts {
    /// Path to the input CSV file
    csv: String,

    /// Database name
    #[clap(long, default_value = "postgres")]
    db: Option<String>,
    #[clap(long, default_value = "localhost")]
    host: Option<String>,
    #[clap(long, default_value = "5432")]
    port: Option<u16>,
    #[clap(short, long, default_value = "postgres")]
    username: Option<String>,
    #[clap(short, long, default_value = "postgres")]
    password: Option<String>,

    /// Maximum number of records to read from csv
    #[clap(short, long)]
    limit: Option<u32>,

    #[clap(long, default_value = "50")]
    batch_size: usize,

    #[clap(long)]
    max_trip_size: Option<usize>,
}

impl From<Opts> for Config {
    fn from(value: Opts) -> Self {
        let mut cfg = Config::new();
        cfg.host = value.host;
        cfg.port = value.port;
        cfg.dbname = value.db;
        cfg.user = value.username;
        cfg.password = value.password;
        cfg.manager = Some(ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        });
        cfg
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    meos::init();

    let opts: Opts = Opts::parse();

    let cfg: Config = opts.clone().into();
    let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls).unwrap();
    tokio::spawn({
        let pool = pool.clone();
        async move {
            if let Err(e) = pool.get().await {
                eprintln!("db connection error: {}", e);
            }
        }
    });

    let start = Instant::now();
    let mut df = LazyCsvReader::new(&opts.csv).has_header(true).finish()?;
    if let Some(limit) = opts.limit {
        df = df.limit(limit);
    }

    let df = df
        .select([
            col("MMSI"),
            col("BaseDateTime").alias("T"),
            col("LAT"),
            col("LON"),
        ])
        .group_by(["MMSI"])
        .agg([
            // todo;; figure out how to remove dupes in df, the
            //        trick is deriving the correct len afterwards
            len(),
            col("T").sort(false),
            concat_str([col("LON"), col("LAT")], " ", true).alias("P"),
        ])
        .filter(col("len").gt(lit(1)))
        .sort("len", Default::default())
        .collect()?;
    let duration = start.elapsed();
    println!("loaded {} in {:?}", &opts.csv, duration);

    let mut metric_mmsi_cnt = 0;
    let mut metric_total_posit_cnt = 0;
    let mut metric_last_posit_report = 0;

    let sz = df.height();
    if sz > 0 {
        let insert_statement = {
            let client = pool.get().await?;
            create_trip_table(&client).await?;
            // todo;; need to go from tgeopoint to a byte array for the prepared statement
            // use tgeompointFromHexEWKB to convert from byte array
            // use temporal_to_wkb_buf(tptr, buff, WKB_NDR | (uint8_t) WKB_EXTENDED | (uint8_t) WKB_HEX) to go from temporal
            let statement =
                "INSERT INTO ais.trips (MMSI, trip) VALUES ($1, public.tgeompointFromBinary($2)) ON CONFLICT (MMSI) DO UPDATE SET trip = public.update(trips.trip, EXCLUDED.trip, true)";
            client.prepare(&statement).await.expect("prepare")
        };

        use AnyValue::*;
        if let [m, l, t, p] = df.get_columns() {
            for i in 0..sz {
                let mut metric_trip_sz = 0;
                match (m.get(i)?, l.get(i)?, t.get(i)?, p.get(i)?) {
                    (Int64(mmsi), UInt32(len), List(ts), List(pt)) => {
                        // println!("========== {i}: {mmsi} - {len} ==========");
                        let source: Vec<_> = (0..len as usize)
                            .into_iter()
                            .map(|i| (ts.get(i).unwrap(), pt.get(i).unwrap()))
                            .collect();

                        for chunk in source.chunks(opts.batch_size) {
                            let mut trip = vec![];

                            for (t, p) in chunk {
                                // todo;; remove dupes in df and this check goes away
                                let ts = t.get_str().unwrap();
                                if trip.last().is_some_and(|(pts, _)| pts == ts) {
                                    continue;
                                }
                                // println!("\t {i}: Point({p})@{t}+00");
                                // print!(".");
                                trip.push((ts.to_string(), make_posit(ts, p.get_str().unwrap())));

                                metric_total_posit_cnt += 1;
                                metric_trip_sz += 1;
                                if opts.max_trip_size.is_some_and(|max| metric_trip_sz >= max) {
                                    break;
                                }
                            }

                            if !trip.is_empty() {
                                // todo;; remove dupes in df, and this map goes away
                                let seq = TSeq::make(trip.into_iter().map(|(_, g)| g).collect());
                                let q_str = seq.out()?;

                                let mut szout: usize = 0;
                                unsafe {
                                    let bytes = meos::temporal_as_wkb(
                                        seq.p as *const Temporal,
                                        meos::MY_VARIANT,
                                        &mut szout,
                                    ) as *const u8;

                                    let arr = std::slice::from_raw_parts(bytes, szout);

                                    //println!("CONVERTED! {} bytes arr {} ", szout, arr.len());

                                    let client = pool.get().await?;
                                    client
                                        .execute(&insert_statement, &[&(mmsi as i32), &arr])
                                        .await?;
                                }

                                // insert_trip(&client, mmsi as i32, &q_str).await?;
                                // println!(";")
                            }
                        }
                        metric_mmsi_cnt += 1;
                        if metric_mmsi_cnt % 500 == 0 {
                            print!(".");
                            let _ = io::stdout().flush();
                        }
                        if metric_total_posit_cnt - metric_last_posit_report > 10000 {
                            metric_last_posit_report = metric_total_posit_cnt;
                            print!("+");
                            let _ = io::stdout().flush();
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

    let duration = start.elapsed();
    println!(
        "loaded {} posits across {} mmsi in {:?}",
        metric_total_posit_cnt, metric_mmsi_cnt, duration
    );

    meos::finalize();
    Ok(())
}

fn make_posit(t: &str, p: &str) -> TGeom {
    let wkt = format!("SRID=4326;Point({p})@{t}+00");
    TGeom::new(&wkt).expect("")
}

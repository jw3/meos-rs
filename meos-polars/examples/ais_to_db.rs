use clap::Parser;
use deadpool_postgres::{Config, ManagerConfig, RecyclingMethod, Runtime};
use meos::prelude::*;
use polars::prelude::*;
use std::error::Error;
use std::io;
use std::io::Write;
use std::path::PathBuf;
use std::time::Instant;
use tokio_postgres::{Client, NoTls};

async fn create_trip_table(client: &Client) -> Result<(), tokio_postgres::Error> {
    client
        .batch_execute(
            &[
                "CREATE SCHEMA IF NOT EXISTS ais",
                "SELECT pg_catalog.set_config('search_path', '', false)",
                "DROP TABLE IF EXISTS ais.trips",
                "CREATE TABLE ais.trips (MMSI integer PRIMARY KEY, trip public.tgeompoint)",
            ]
            .join(";"),
        )
        .await
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
    /// Path to the input CSV
    /// May be a single file or a directory
    /// If is a directory then all csv are globbed
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

    /// Maximum number of vessls to read from csv
    #[clap(short, long)]
    limit: Option<u32>,

    #[clap(long, default_value = "50")]
    batch_size: usize,

    /// filter out trips with less than
    #[clap(long, default_value = "1")]
    min_trip_size: u32,

    /// truncate trips over
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

    let input = PathBuf::from(&opts.csv);
    let data = if input.is_file() {
        vec![input]
    } else if input.is_dir() {
        print!("searching dir {} for csv... ", opts.csv);
        let mut res = vec![];
        for f in std::fs::read_dir(input)?.flatten().map(|e| e.path()) {
            if f.is_file() && f.extension().expect("extension").to_str().expect("str") == "csv" {
                res.push(f);
            }
        }
        println!("found {}", res.len());
        res
    } else {
        unreachable!("input was not a file or dir")
    };

    let start = Instant::now();
    let df = LazyCsvReader::new_paths(data.into())
        .has_header(true)
        .finish()?;
    let full_size = df.clone().collect().unwrap().height();

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
        .filter(col("len").gt(lit(opts.min_trip_size)))
        .sort("len", Default::default())
        .limit(opts.limit.unwrap_or(IdxSize::MAX))
        .collect()?;
    let duration = start.elapsed();

    let sz = df.height();
    println!(
        "{}: aggregated {} records over {} csv rows taking {:?}",
        sz, full_size, &opts.csv, duration
    );
    let start = Instant::now();

    let mut metric_mmsi_cnt = 0;
    let mut metric_total_posit_cnt = 0;
    let mut metric_last_posit_report = 0;

    if sz > 0 {
        let insert_statement = {
            let client = pool.get().await?;
            create_trip_table(&client).await?;
            let statement =
                "INSERT INTO ais.trips (MMSI, trip) VALUES ($1, public.tgeompointFromBinary($2)) ON CONFLICT (MMSI) DO UPDATE SET trip = public.update(trips.trip, EXCLUDED.trip, true)";
            client.prepare(&statement).await.expect("prepare")
        };

        use AnyValue::*;
        if let [m, l, t, p] = df.get_columns() {
            'df: for i in 0..sz {
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
                                let geoms: Vec<_> = trip.into_iter().map(|(_, g)| g).collect();
                                let seq = TSeq::make(&geoms).unwrap();

                                let bytes = seq.as_bytes();
                                let client = pool.get().await?;
                                match client
                                    .execute(&insert_statement, &[&(mmsi as i32), &bytes])
                                    .await
                                {
                                    Ok(_) => {}
                                    Err(e) => {
                                        eprintln!("\nerror: {e}");
                                        break 'df;
                                    }
                                }
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
        "\ncommitted {} tracks containing {} posits in {:?}",
        metric_mmsi_cnt, metric_total_posit_cnt, duration
    );

    meos::finalize();
    Ok(())
}

fn make_posit(t: &str, p: &str) -> TInst {
    TInst::from_wkt(&format!("SRID=4326;Point({p})@{t}+00")).expect("")
}

use clap::Parser;
use deadpool_postgres::{Config, ManagerConfig, RecyclingMethod, Runtime};
use meos::*;
use polars::prelude::*;
use std::error::Error;
use std::time::Instant;
use tokio_postgres::{Client, NoTls};

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

    let sz = df.height();
    if sz > 0 {
        let _insert_statement = {
            let client = pool.get().await?;
            create_trip_table(&client).await?;
            // todo;; need to go from tgeopoint to a byte array for the prepared statement
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
                                print!(".");
                                trip.push((ts.to_string(), make_posit(ts, p.get_str().unwrap())));
                            }

                            // todo;; remove dupes in df, and this map goes away
                            let seq = TSeq::make(trip.into_iter().map(|(_, g)| g).collect());
                            let q_str = seq.out()?;

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

    meos::finalize();
    Ok(())
}

fn make_posit(t: &str, p: &str) -> TGeom {
    let wkt = format!("SRID=4326;Point({p})@{t}+00");
    TGeom::new(&wkt).expect("")
}

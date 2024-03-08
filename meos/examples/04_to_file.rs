use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::io::Write;
use std::str::FromStr;

use clap::Parser;

use meos::{TGeom, TSeq};

#[derive(Debug, serde::Deserialize)]
struct AisRecord {
    #[serde(alias = "BaseDateTime")]
    t: String,
    #[serde(alias = "MMSI")]
    mmsi: i64,
    #[serde(alias = "LAT")]
    latitude: f64,
    #[serde(alias = "LON")]
    longitude: f64,
}

#[derive(Copy, Clone, Debug, Default)]
enum OutFmt {
    #[default]
    Hex,
    MfJson,
}
//
// impl Display for OutFmt {
//     fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
//         f.write_fmt(format_args!("{:?}", self))
//     }
// }

impl FromStr for OutFmt {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "hex" | "0x" => Ok(Self::Hex),
            "json" | "mf-json" => Ok(Self::MfJson),
            _ => Err("Invalid output format".to_owned()),
        }
    }
}

#[derive(Clone, Debug, Parser)]
struct Opts {
    /// Path to the input CSV file
    input: String,

    /// Path to the input CSV file
    output: String,

    /// Maximum number of records to read from input
    #[clap(short, long)]
    limit: Option<usize>,

    /// Maximum number of posits per output record
    #[clap(long, default_value = "50")]
    batch_size: usize,

    /// Filter out trips with less than this number posits
    #[clap(long, default_value = "1")]
    min_trip_size: u32,

    #[clap(short, long)]
    format: OutFmt,
}

fn main() -> Result<(), Box<dyn Error>> {
    let opts: Opts = Opts::parse();

    meos::init();

    let file = File::open(&opts.input)?;
    let mut rdr = csv::Reader::from_reader(file);

    let output = File::options()
        .create(true)
        .write(true)
        .open(&opts.output)?;

    let mut trips: HashMap<i64, Vec<TGeom>> = HashMap::new();
    for (records_in, result) in rdr
        .deserialize()
        .take(opts.limit.unwrap_or(usize::MAX))
        .enumerate()
    {
        let rec: AisRecord = result?;

        let lon_lat = format!("{} {}", rec.longitude, rec.latitude);
        let posit = make_posit(&rec.t, &lon_lat);
        match trips.entry(rec.mmsi) {
            Entry::Occupied(mut trip) => {
                let v = trip.get_mut();
                if v.len() == opts.batch_size {
                    print!("[{records_in}] {},", rec.mmsi);
                    let seq = TSeq::make(v);
                    v.clear();
                    write_record(&output, rec.mmsi, seq, opts.format).expect("write rec");
                }
                v.push(posit);
            }
            Entry::Vacant(trip) => {
                trip.insert(vec![posit]);
            }
        }
    }

    println!("Total vessels: {}", trips.len());
    for (mmsi, trip) in trips {
        let seq = TSeq::make(&trip);
        write_record(&output, mmsi, seq, opts.format).expect("write rec");
    }

    meos::finalize();

    Ok(())
}

fn write_record(mut file: &File, mmsi: i64, seq: TSeq, fmt: OutFmt) -> Result<(), Box<dyn Error>> {
    let output = match fmt {
        OutFmt::Hex => seq.as_hex().unwrap(),
        OutFmt::MfJson => seq.as_json().unwrap(),
    };
    writeln!(file, "{},{}", mmsi, output)?;
    Ok(())
}

fn make_posit(t: &str, p: &str) -> TGeom {
    let wkt = format!("SRID=4326;Point({p})@{t}+00");
    TGeom::new(&wkt).expect("")
}

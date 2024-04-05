use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::io::Write;
use std::str::FromStr;

use clap::Parser;
use meos::tgeo::{TInst, TSeq, Temporal};

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
    #[serde(alias = "VesselType", default)]
    vessel_type: Option<u32>,
}

#[derive(Copy, Clone, Debug, Default)]
enum OutFmt {
    #[default]
    Hex,
    MfJson,
}

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

    /// Filter out trips with less than this number posits
    #[clap(long, default_value = "1")]
    min_trip_size: usize,

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

    let mut trips: HashMap<i64, Vec<TInst>> = HashMap::new();
    let mut vtype: HashMap<i64, u32> = HashMap::new();
    for (_, result) in rdr
        .deserialize()
        .take(opts.limit.unwrap_or(usize::MAX))
        .enumerate()
    {
        let rec: AisRecord = result?;

        let lon_lat = format!("{} {}", rec.longitude, rec.latitude);
        let posit = make_posit(&rec.t, &lon_lat);
        vtype.insert(rec.mmsi, rec.vessel_type.unwrap_or(0));
        match trips.entry(rec.mmsi) {
            Entry::Occupied(mut trip) => {
                let v = trip.get_mut();
                if let Some(prev) = v.last() {
                    if *prev >= posit {
                        continue;
                    }
                }
                // if v.len() == opts.batch_size {
                //     let seq = TSeq::make(v).expect("tseq::make failed");
                //     v.clear();
                //     let vt = vtype.get(&rec.mmsi).unwrap_or(&0);
                //     write_record(&output, rec.mmsi, *vt, seq, opts.format).expect("write rec");
                // }
                v.push(posit);
            }
            Entry::Vacant(trip) => {
                trip.insert(vec![posit]);
            }
        }
    }

    for (&mmsi, trip) in trips.iter().filter(|(_, t)| t.len() >= opts.min_trip_size) {
        print!("{},", trip.len());
        let seq = TSeq::make(&trip);
        let vt = vtype.get(&mmsi).unwrap_or(&0);
        write_record(
            &output,
            mmsi,
            *vt,
            seq.expect("tseq::make failed"),
            opts.format,
        )
        .expect("write rec");
    }
    println!("\nTotal vessels: {}", trips.len());

    meos::finalize();

    Ok(())
}

fn write_record(
    mut file: &File,
    mmsi: i64,
    vtype: u32,
    seq: TSeq,
    fmt: OutFmt,
) -> Result<(), Box<dyn Error>> {
    let output = match fmt {
        OutFmt::Hex => seq.as_hex().unwrap(),
        OutFmt::MfJson => seq.as_json().unwrap(),
    };
    writeln!(file, r#"{{"id":{mmsi},"vt":{vtype},"json":{output}}}"#)?;
    Ok(())
}

fn make_posit(t: &str, p: &str) -> TInst {
    TInst::from_wkt(&format!("SRID=4326;Point({p})@{t}+00")).expect("posit")
}

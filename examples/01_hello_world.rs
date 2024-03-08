use std::error::Error;

use meos::{to_mf_json, TGeom};

fn main() -> Result<(), Box<dyn Error>> {
    meos::init();

    // 1
    let wkt = "POINT(1 1)@2000-01-01";
    let inst = TGeom::new(wkt).expect("()");
    let mf_json = to_mf_json(&inst)?;
    println!("===========\n{}\n===========\n{mf_json}", inst.ttype());

    // 2
    let wkt = "{POINT(1 1)@2000-01-01, POINT(2 2)@2000-01-02}";
    let seq = TGeom::new(wkt).expect("()");
    let mf_json = to_mf_json(&seq)?;
    println!(
        "===========\n{} with Discrete Interpolation\n===========\n{mf_json}",
        seq.ttype()
    );

    // 3
    let wkt = "[POINT(1 1)@2000-01-01, POINT(2 2)@2000-01-02]";
    let seq = TGeom::new(wkt).expect("()");
    let mf_json = to_mf_json(&seq)?;
    println!(
        "===========\n{} with Linear Interpolation\n===========\n{mf_json}",
        seq.ttype()
    );

    // 4
    let wkt = "Interp=Step;[POINT(1 1)@2000-01-01, POINT(2 2)@2000-01-02]";
    let seq = TGeom::new(wkt).expect("()");
    let mf_json = to_mf_json(&seq)?;
    println!(
        "===========\n{} with Step Interpolation\n===========\n{mf_json}",
        seq.ttype()
    );

    // 5
    let wkt = "{[POINT(1 1)@2000-01-01, POINT(2 2)@2000-01-02], [POINT(3 3)@2000-01-03, POINT(3 3)@2000-01-04]}";
    let seq = TGeom::new(wkt).expect("()");
    let mf_json = to_mf_json(&seq)?;
    println!(
        "===========\n{} Set with Linear Interpolation\n===========\n{mf_json}",
        seq.ttype()
    );

    // 6
    let wkt = "Interp=Step;{[POINT(1 1)@2000-01-01, POINT(2 2)@2000-01-02], [POINT(3 3)@2000-01-03, POINT(3 3)@2000-01-04]}";
    let seq = TGeom::new(wkt).expect("()");
    let mf_json = to_mf_json(&seq)?;
    println!(
        "===========\n{} Set with Step Interpolation\n===========\n{mf_json}",
        seq.ttype()
    );

    meos::finalize();

    Ok(())
}

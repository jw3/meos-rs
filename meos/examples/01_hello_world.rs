use meos::tgeo::{TInst, TSeq, TSet, Temporal};
use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    meos::init();

    // 1
    let wkt = "POINT(1 1)@2000-01-01";
    let inst = TInst::from_wkt(wkt).expect("()");
    let mf_json = inst.to_mf_json()?;
    println!("===========\n{}\n===========\n{mf_json}", inst.ttype());

    // // 2
    let wkt = "{POINT(1 1)@2000-01-01, POINT(2 2)@2000-01-02}";
    let seq = TSeq::from_wkt(wkt).expect("()");
    let mf_json = seq.to_mf_json()?;
    println!(
        "===========\n{} with Discrete Interpolation\n===========\n{mf_json}",
        seq.ttype()
    );

    // // 3
    let wkt = "[POINT(1 1)@2000-01-01, POINT(2 2)@2000-01-02]";
    let seq = TSeq::from_wkt(wkt).expect("()");
    let mf_json = seq.to_mf_json()?;
    println!(
        "===========\n{} with Linear Interpolation\n===========\n{mf_json}",
        seq.ttype()
    );

    // 4
    let wkt = "Interp=Step;[POINT(1 1)@2000-01-01, POINT(2 2)@2000-01-02]";
    let seq = TSeq::from_wkt(wkt).expect("()");
    let mf_json = seq.to_mf_json()?;
    println!(
        "===========\n{} with Step Interpolation\n===========\n{mf_json}",
        seq.ttype()
    );

    // // 5
    let wkt = "{[POINT(1 1)@2000-01-01, POINT(2 2)@2000-01-02], [POINT(3 3)@2000-01-03, POINT(3 3)@2000-01-04]}";
    let set = TSet::from_wkt(wkt).expect("()");
    let mf_json = set.to_mf_json()?;
    println!(
        "===========\n{} with Linear Interpolation\n===========\n{mf_json}",
        set.ttype()
    );

    // // 6
    let wkt = "Interp=Step;{[POINT(1 1)@2000-01-01, POINT(2 2)@2000-01-02], [POINT(3 3)@2000-01-03, POINT(3 3)@2000-01-04]}";
    let set = TSet::from_wkt(wkt).expect("()");
    let mf_json = set.to_mf_json()?;
    println!(
        "===========\n{} with Step Interpolation\n===========\n{mf_json}",
        set.ttype()
    );

    meos::finalize();

    Ok(())
}

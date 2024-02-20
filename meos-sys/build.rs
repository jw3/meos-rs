use std::env;

fn main() {
    let project_dir = env::var("CARGO_MANIFEST_DIR").unwrap();

    println!("cargo:rustc-link-search={project_dir}/mobdb/lib");
    println!("cargo:rustc-link-lib=static=meos");
    println!("cargo:rustc-link-lib=dylib=json-c");
    println!("cargo:rustc-link-lib=dylib=proj");
    println!("cargo:rustc-link-lib=dylib=geos_c");
    println!("cargo:rustc-link-lib=dylib=gsl");
    println!("cargo:rustc-link-lib=dylib=gslcblas");
    println!("cargo:rustc-link-lib=dylib=stdc++");
    println!("cargo:rustc-link-lib=dylib=sqlite3");
    println!("cargo:rustc-link-lib=dylib=curl");
    println!("cargo:rustc-link-lib=dylib=tiff");

    let bindings = bindgen::Builder::default()
        .header("mobdb/include/meos.h")
        //
        .allowlist_function("meos_initialize")
        .allowlist_type("Temporal")
        .allowlist_function("tgeompoint_in")
        .allowlist_function("temporal_as_mfjson")
        //
        .generate()
        .expect("Unable to generate bindings");

    bindings
        .write_to_file("src/bindings.rs")
        .expect("Couldn't write bindings!");
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/rsqlite/v1/sqlite.proto");
    tonic_build::configure()
        .compile_protos(
            &["proto/rsqlite/v1/sqlite.proto"],
            &["proto"],
        )?;
    Ok(())
}

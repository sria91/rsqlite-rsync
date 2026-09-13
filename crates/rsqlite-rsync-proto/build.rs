fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/rsqlite/v1/sqlite.proto");
    if std::env::var_os("PROTOC").is_none() {
        std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
    }
    tonic_prost_build::configure()
        .compile_protos(&["proto/rsqlite/v1/sqlite.proto"], &["proto"])?;
    Ok(())
}

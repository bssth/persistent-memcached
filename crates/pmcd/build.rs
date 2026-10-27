use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../proto");
    let files = ["pmc/v1/memcache.proto", "pmc/cluster/v1/cluster.proto"].map(|f| root.join(f));
    for f in &files {
        println!("cargo:rerun-if-changed={}", f.display());
    }
    // protox is a pure-Rust protobuf compiler, so building does not need `protoc`.
    let fds = protox::compile(&files, [&root])?;
    tonic_prost_build::configure().bytes(".").compile_fds(fds)?;
    Ok(())
}

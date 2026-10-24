use std::path::{Path, PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?);
    let proto_root = manifest_dir.join("../../contract/proto");
    let proto_dir = proto_root.join("mftiko");

    let mut protos = Vec::new();
    collect_protos(&proto_dir, &mut protos)?;
    protos.sort();
    if protos.is_empty() {
        return Err(format!("no .proto files under {}", proto_dir.display()).into());
    }
    for proto in &protos {
        println!("cargo:rerun-if-changed={}", proto.display());
    }

    let mut config = prost_build::Config::new();
    config.protoc_executable(protoc_bin_vendored::protoc_bin_path()?);
    config.include_file("mftiko.rs");
    config.compile_protos(&protos, &[proto_root])?;
    Ok(())
}

fn collect_protos(dir: &Path, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_dir() {
            collect_protos(&path, out)?;
        } else if path.extension().is_some_and(|ext| ext == "proto") {
            out.push(path);
        }
    }
    Ok(())
}

use std::path::Path;
use std::path::PathBuf;

fn find_protoc(manifest_dir: &Path) -> Option<PathBuf> {
    if let Some(env_protoc) = std::env::var_os("PROTOC") {
        let p = PathBuf::from(env_protoc);
        if p.exists() {
            return Some(p);
        }
    }

    let exe_name = if cfg!(windows) {
        "protoc.exe"
    } else {
        "protoc"
    };
    let packaged = manifest_dir
        .join("../../../openvmm/.packages/Google.Protobuf.Tools/tools")
        .join(exe_name);
    if packaged.exists() {
        return Some(packaged);
    }

    None
}

fn main() {
    let manifest_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    let proto_dir = manifest_dir.join("../../../openvmm/openvmm/openvmm_ttrpc_vmservice/src");
    let proto_file = proto_dir.join("vmservice.proto");

    if !proto_file.exists() {
        panic!("vmservice.proto not found at {}", proto_file.display());
    }

    if let Some(protoc) = find_protoc(&manifest_dir) {
        // SAFETY: build scripts are single-threaded before invoking prost-build.
        unsafe {
            std::env::set_var("PROTOC", &protoc);
        }
    }

    let include_dir = manifest_dir.join("../../../openvmm/.packages/Google.Protobuf.Tools/tools/include");
    let mut includes = vec![proto_dir];
    if include_dir.exists() {
        includes.push(include_dir);
    }

    prost_build::Config::new()
        .compile_protos(&[&proto_file], &includes)
        .expect("failed to compile vmservice.proto");

    println!("cargo:rerun-if-changed={}", proto_file.display());
}

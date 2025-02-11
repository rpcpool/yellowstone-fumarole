use std::{env, path::Path};

fn main() {
    let package_root = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let mut path = Path::new(&package_root).join("yellowstone-api/proto");
    // get to the Cargo.lock

    while !path.join("Cargo.lock").exists() {
        let parent = path.parent().unwrap();
        if parent == path {
            panic!("Could not find Cargo.lock");
        }
        path = parent.to_path_buf();
    }
    let yellowstone_api_proto_dir = path.join("yellowstone-api/proto");
    env::set_var("PROTOC", protobuf_src::protoc());
    tonic_build::configure()
        .build_server(false)
        .compile_protos(
            &[yellowstone_api_proto_dir.join("fumarole.proto")],
            &[yellowstone_api_proto_dir],
        )
        .expect("Failed to compile protos");
}

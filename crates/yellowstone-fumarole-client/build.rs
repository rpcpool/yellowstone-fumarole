use std::{env, path::Path};

fn main() {
    let package_root = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let mut path = Path::new(&package_root).join("yellowstone-grpc/proto");

    // Locate the Cargo.lock file
    while !path.join("Cargo.lock").exists() {
        let parent = path.parent().unwrap();
        if parent == path {
            panic!("Could not find Cargo.lock");
        }
        path = parent.to_path_buf();
    }
    // let yellowstone_api_proto_dir = path.join("yellowstone-api/proto");
    let yellowstone_grpc_proto_dir = path.join("yellowstone-grpc/yellowstone-grpc-proto/proto");
    let proto_dir = path.join("proto");
    // let proto_dir = yellowstone_api_proto_dir.to_str().unwrap();
    env::set_var("PROTOC", protobuf_src::protoc());

    tonic_build::configure()
        .build_server(false)
        .compile_protos(
            &[proto_dir.join("fumarole.proto")],
            &[proto_dir, yellowstone_grpc_proto_dir],
        )
        .expect("Failed to compile protos");
}

use std::{env, path::Path};

fn main() {
    let package_root = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let path = Path::new(&package_root);

    let yellowstone_grpc_proto_dir = path.join("yellowstone-grpc-proto");
    let proto_dir = path.join("proto");
    // TODO: Audit that the environment access only happens in single-threaded code.
    unsafe { env::set_var("PROTOC", protobuf_src::protoc()) };

    tonic_build::configure()
        .build_server(false)
        .compile_protos(
            &[
                proto_dir.join("fumarole.proto"),
            ],
            &[proto_dir, yellowstone_grpc_proto_dir],
        )
        .expect("Failed to compile protos");
}

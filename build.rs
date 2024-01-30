use std::io::Result;
use std::env;

use protoc_bin_vendored;

fn main() -> Result<()> {
    std::env::set_var("SOURCE_DIR", "/Users/vbukovshin/Documents/projects/personal/rust/rpc_service/");

    let base_path = std::env::var("SOURCE_DIR").unwrap_or(String::from("/"));
    let proto_path = base_path.clone() + "/src/proto/storage_service.proto";

    println!("base path is {}", base_path);
    println!("proto path is {}", proto_path);

    env::set_var("OUT_DIR", base_path.clone() + "/src");

    protobuf_codegen::Codegen::new()
    // Use `protoc` parser, optional.
    .pure()
    // Use `protoc-bin-vendored` bundled protoc command, optional.
    .protoc_path(&protoc_bin_vendored::protoc_bin_path().unwrap())
    // All inputs and imports from the inputs must reside in `includes` directories.
    .includes(&["src/proto"])
    // Inputs must reside in some of include paths.
    .input("src/proto/storage_service.proto")
    // Specify output directory relative to Cargo output directory.
    .cargo_out_dir("storage_service")
    .run_from_script();

    Ok(())
}

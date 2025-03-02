use prost_build::Config;
use std::env;
use std::io::Error;
use std::path::PathBuf;

fn main() {
    build_proto().unwrap()
}

fn build_proto() -> Result<(), Error> {
    let current_dir = env::current_dir()?;
    let proto_path = PathBuf::from(current_dir.as_path()).join("proto");
    let proto_file = PathBuf::from(proto_path.as_path()).join("pacifica_rpc.proto");
    let out_dir = PathBuf::from(current_dir.as_path()).join("src");
    println!("out_dir = {}", out_dir.to_str().unwrap());
    let proto_files = &[proto_file.to_str().unwrap()];
    let includes = &[proto_path.to_str().unwrap()];
    let mut config = Config::new();
    tonic_build::configure()
        .out_dir(out_dir)
        .build_server(true)
        .build_client(true)
        .compile_protos_with_config(config, proto_files, includes)?;
    Ok(())
}

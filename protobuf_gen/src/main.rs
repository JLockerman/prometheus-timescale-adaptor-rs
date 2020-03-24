
extern crate protoc_rust;

use protoc_rust::Customize;

fn main() {
    protoc_rust::run(protoc_rust::Args {
	    out_dir: ".",
        input: &["prom_protos/remote.proto", "prom_protos/types.proto"],
        includes: &["prom_protos"],
	    customize: Customize {
            carllerche_bytes_for_bytes: Some(true),
            carllerche_bytes_for_string: Some(true),
	        ..Default::default()
	    },
	}).expect("protoc");
}

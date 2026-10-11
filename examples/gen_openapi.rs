//! Write the OpenAPI document to `openapi.json` at the repository root.
//! Run: `cargo run --example gen_openapi`
fn main() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("openapi.json");
    std::fs::write(&path, baserunna::api::openapi::document_json()).expect("write openapi.json");
    println!("wrote {}", path.display());
}

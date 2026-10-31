//! Object-store requests per operation and the resulting cost per database
//! per month (SPEC §13.5).
//!
//! ```text
//! cargo run --release --example cost_model
//! ```

use baserunna::sim::cost::{LATENCY_MS, cost_table, measure_all};

fn main() {
    let r = measure_all();
    println!("Object-store requests per operation ({LATENCY_MS} ms per request, production configuration):\n");
    println!("{}", r.table());
    println!("Cost per database per month (S3 Standard prices; an upper bound, see sim::cost::monthly_cost):\n");
    println!("{}", cost_table(&r));
}

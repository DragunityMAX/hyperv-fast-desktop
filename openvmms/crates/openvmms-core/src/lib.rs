pub mod api_client;
pub mod disk;
pub mod models;
pub mod process;
pub mod store;
pub mod ttrpc;

pub mod vmservice {
    include!(concat!(env!("OUT_DIR"), "/vmservice.rs"));
}

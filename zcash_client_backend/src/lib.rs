//! *A crate for implementing Zcash light clients.*
//!
//! `zcash_client_backend` contains Rust structs and traits for creating shielded Zcash
//! light clients.

// Catch documentation errors caused by code changes.
#![deny(broken_intra_doc_links)]
// Temporary until we have addressed all Result<T, ()> cases.
#![allow(clippy::result_unit_err)]

#[cfg(not(feature = "wasm-extra"))]
pub mod address;
pub mod data_api;
#[cfg(not(feature = "wasm-extra"))]
mod decrypt;
#[cfg(not(feature = "wasm-extra"))]
pub mod encoding;
#[cfg(not(feature = "wasm-extra"))]
pub mod keys;
#[cfg(not(feature = "wasm-extra"))]
pub mod proto;
#[cfg(not(feature = "wasm-extra"))]
pub mod wallet;
#[cfg(not(feature = "wasm-extra"))]
pub mod welding_rig;
#[cfg(not(feature = "wasm-extra"))]
pub mod zip321;

pub use decrypt::{decrypt_transaction, DecryptedOutput};

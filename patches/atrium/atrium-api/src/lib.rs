#![cfg_attr(docsrs, feature(doc_cfg))]
#![doc = include_str!("../README.md")]
pub use atrium_xrpc as xrpc;
#[cfg_attr(docsrs, doc(cfg(feature = "agent")))]
#[cfg(feature = "agent")]
pub mod agent;
pub mod app;
pub mod chat;
pub mod client;
pub mod com;
pub mod did_doc;
pub mod error;
pub mod record;
pub mod tools;
pub mod types;

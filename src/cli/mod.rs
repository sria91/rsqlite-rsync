//! CLI interface definitions, subcommands, and helpers.

pub mod client;

pub use client::{
    ClientCommand, ClientConnectionArgs, OutputFormat, run_client_command, run_sql_shorthand,
};

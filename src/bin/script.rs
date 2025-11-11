use pensieve_rs::pensieve::Pensieve;
use pensieve_rs::script::{PensieveScript, write_csv};
use pensieve_rs::script::last_non_null::LastNonNullScript;
use std::env;

/// Binary that executes a user-defined script.
/// You likely want to write your own script and then invoke it using this binary.
/// Check the script directory for examples of scripts.
/// TODO: Write a better main(), this will get hard to maintain as the number of scripts increases
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    
    if args.len() < 2 {
        eprintln!("Usage: script <script-name> [options]");
        eprintln!("Available scripts:");
        eprintln!("  last-non-null --table <name> --column <name> --output <file.csv>");
        return Ok(());
    }
    
    let script_name = &args[1];
    
    match script_name.as_str() {
        "last-non-null" => run_last_non_null(&args[2..])?,
        _ => {
            eprintln!("Unknown script: {}", script_name);
            return Ok(());
        }
    }
    
    Ok(())
}

fn run_last_non_null(args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    let mut table_name = "books".to_string();
    let mut column_name = "price".to_string();
    let mut output = "results.csv".to_string();
    let mut snapshot_timestamp = "251111 01:45:00".to_string();
    let mut window_hours = 1;
    
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--table" => {
                i += 1;
                table_name = args[i].clone();
            }
            "--column" => {
                i += 1;
                column_name = args[i].clone();
            }
            "--output" => {
                i += 1;
                output = args[i].clone();
            }
            "--timestamp" => {
                i += 1;
                snapshot_timestamp = args[i].clone();
            }
            "--window" => {
                i += 1;
                window_hours = args[i].parse().unwrap_or(6);
            }
            _ => {}
        }
        i += 1;
    }
    
    println!("=== Last Non-Null Value Finder ===");
    println!("Table: {}", table_name);
    println!("Column: {}", column_name);
    println!();
    
    println!("Loading snapshot and binlog...");
    let pensieve = Pensieve::new(&snapshot_timestamp, window_hours)?;
    
    let mut manager = pensieve.into_manager();
    
    let mut script = LastNonNullScript {
        table_name,
        column_name,
    };
    
    let results = script.execute(&mut manager)?;
    
    println!("Writing results to {}...", output);
    write_csv(&results, &output)?;
    
    println!("Done! Results written to {}", output);
    Ok(())
}


use pensieve_rs::pensieve::Pensieve;
use pensieve_rs::script::{PensieveScript, write_csv};
use pensieve_rs::script::last_non_null::{run_last_non_null, LastNonNullScript};
use std::env;

/// Binary that executes a user-defined script.
/// You likely want to write your own script and then invoke it using this binary.
/// Check the script directory for examples of scripts.
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

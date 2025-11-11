use pensieve_rs::pensieve::Pensieve;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let snapshot_timestamp = "251108 17:03:00";
    let window_hours = 6;
    
    let pensieve = Pensieve::new(snapshot_timestamp, window_hours)?;


    Ok(())
}

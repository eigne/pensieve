pub mod last_non_null;

use crate::snapshot_manager::SnapshotManager;

#[derive(Debug, Clone)]
pub struct ScriptResult {
    pub columns: Vec<String>,
    pub values: Vec<String>,
}

pub trait PensieveScript {
    fn execute(&mut self, manager: &mut SnapshotManager) -> Result<Vec<ScriptResult>, Box<dyn std::error::Error>>;
    fn headers(&self) -> Vec<String>;
}

pub fn write_csv(results: &[ScriptResult], output_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    use std::fs::File;
    use std::io::Write;
    
    let mut file = File::create(output_path)?;
    
    if let Some(first) = results.first() {
        writeln!(file, "{}", first.columns.join(","))?;
    }
    
    for result in results {
        writeln!(file, "{}", result.values.join(","))?;
    }
    
    Ok(())
}


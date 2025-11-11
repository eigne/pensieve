use std::collections::HashMap;
use crate::script::{PensieveScript, ScriptResult};
use crate::snapshot_manager::SnapshotManager;

pub struct LastNonNullScript {
    pub table_name: String,
    pub column_name: String,
}

impl PensieveScript for LastNonNullScript {
    fn execute(&mut self, manager: &mut SnapshotManager) -> Result<Vec<ScriptResult>, Box<dyn std::error::Error>> {
        let mut last_values: HashMap<i64, String> = HashMap::new();

        manager.goto_position(0)?;
        
        let total_ops = manager.operation_count();
        println!("Analyzing {} operations", total_ops);
        
        for pos in 0..total_ops {
            if pos % 10 == 0 {
                println!("Progress: {}/{}", pos, total_ops);
            }
            
            manager.step_forward()?;
            let conn = manager.get_connection();
            
            let query = format!(
                "SELECT id, CAST({} AS VARCHAR) FROM {} WHERE {} IS NOT NULL",
                self.column_name, self.table_name, self.column_name
            );
            
            if let Ok(mut stmt) = conn.prepare(&query) {
                if let Ok(mut rows) = stmt.query([]) {
                    while let Ok(Some(row)) = rows.next() {
                        if let (Ok(file_id), Ok(value)) = (row.get::<usize, i64>(0), row.get::<usize, String>(1)) {
                            if let Some(existing_value) = last_values.get(&file_id) {
                                if *existing_value != value {
                                    last_values.insert(file_id, value);
                                }
                            } else {
                                last_values.insert(file_id, value);
                            }
                        }
                    }
                }
            }
        }
        
        let mut results = Vec::new();
        let mut file_ids: Vec<_> = last_values.keys().collect();
        file_ids.sort();
        
        for file_id in file_ids {
            let value = last_values.get(file_id).unwrap();
            
            results.push(ScriptResult {
                columns: self.headers(),
                values: vec![
                    file_id.to_string(),
                    value.clone(),
                ],
            });
        }
        
        println!("Analysis complete! Found {} results", results.len());
        Ok(results)
    }

    fn headers(&self) -> Vec<String> {
        vec![
            "id".to_string(),
            "last_non_null_value".to_string(),
        ]
    }
}


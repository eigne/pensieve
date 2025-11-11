use duckdb::Connection;
use crate::parser::text_binlog_parser::TextBinlogParser;
use crate::snapshot_normaliser::timestamp_normaliser::TimestampNormaliser;
use crate::snapshot_manager::SnapshotManager;
use crate::loader::parquet_loader;
use std::path::PathBuf;
use std::fs;

/// Pensieve takes a MySQL binlog and DB snapshot in parquet format.
/// It parses this data and generates an in-memory DuckDB table.
/// This table can be moved forwards and backwards in time, within the limits of the supplied binlog.
/// This table can be queried at each point in time, enabling queries over time.
///
/// You can use Pensieve's API to write your own scripts in the script directory and invoke them with the
/// binary in bin/script.rs.
///
/// Pensieve automatically discovers parquet and binlog files in the db_data directory.
/// You must place them following this hierarchy:
/// db_data
///  L my_table
///    L binlog.sql
///    L snapshot-part-01.parquet
///    L snapshot-part-02.parquet
///
/// Pensieve uses this hierarchy to infer the name of your table. (Pensieve currently only supports one table).
pub struct Pensieve {
    manager: SnapshotManager,
    table_name: String,
}

impl Pensieve {
    /// Creates a new Pensieve by discovering and loading data from db_data directory
    /// 
    /// # Arguments
    /// * `snapshot_timestamp` - Approximate timestamp of snapshot (format: "YYMMDD HH:MM:SS")
    /// * `window_hours` - Size of window to search around snapshot (e.g., 6 hours)
    /// 
    /// # Returns
    /// A Pensieve instance with normalized snapshot ready for querying
    pub fn new(
        snapshot_timestamp: &str,
        window_hours: i64,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let current_dir = std::env::current_dir()?;
        let db_data_path = current_dir.join("db_data");
        
        println!("Looking for db_data at: {:?}", db_data_path);
        
        let tables = Self::discover_tables(&db_data_path)?;
        println!("Found tables: {:?}", tables);
        
        if tables.is_empty() {
            return Err("No tables found in db_data".into());
        }
        
        // Use the first table (TODO: Add support for multiple tables)
        let table_name = tables.first().unwrap().clone();
        println!("\n=== Loading table: {} ===", table_name);
        
        let table_path = db_data_path.join(&table_name);
        
        let parquet_files = Self::discover_parquet_files(&table_path)?;
        println!("Found {} parquet file(s)", parquet_files.len());
        
        let binlog_file = Self::discover_binlog_file(&table_path)?;
        println!("Found binlog file: {}", binlog_file);
        
        println!("\n=== Loading Parquet Files ===");
        let parquet_refs: Vec<&str> = parquet_files.iter().map(|s| s.as_str()).collect();
        let conn = parquet_loader::load_table_from_parquet_files(&table_name, &parquet_refs)?;
        
        println!("\n=== Parsing Binlog ===");
        let mut parser = TextBinlogParser::new(conn);
        let operations = parser.parse_file(&binlog_file)?;
        
        println!("Parsed {} operations from binlog", operations.len());
        println!("First 5 operations:");
        for (i, op) in operations.iter().take(5).enumerate() {
            println!("  {}: {}", i, op);
        }
        
        println!("\n=== Normalizing Snapshot ===");
        let conn = parser.into_connection();
        
        let (conn, operations, tx_zero_idx) = TimestampNormaliser::normalize(
            conn,
            operations,
            snapshot_timestamp,
            window_hours,
        )?;
        
        let manager = SnapshotManager::new(conn, operations, tx_zero_idx);
        
        println!("\n=== Snapshot Normalized ===");
        println!("Snapshot position: {}", manager.get_position());
        println!("Snapshot timestamp: {:?}", manager.get_timestamp());
        
        Ok(Self { manager, table_name })
    }
    
    /// Discovers table directories in db_data folder
    fn discover_tables(db_data_path: &PathBuf) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let mut tables = Vec::new();
        
        if !db_data_path.exists() {
            return Err(format!("db_data directory not found at: {:?}", db_data_path).into());
        }
        
        for entry in fs::read_dir(db_data_path)? {
            let entry = entry?;
            let path = entry.path();
            
            if path.is_dir() {
                if let Some(table_name) = path.file_name().and_then(|n| n.to_str()) {
                    tables.push(table_name.to_string());
                }
            }
        }
        
        if tables.is_empty() {
            return Err("No table directories found in db_data".into());
        }
        
        Ok(tables)
    }
    
    /// Discovers parquet files in a table directory
    fn discover_parquet_files(table_path: &PathBuf) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let mut parquet_files = Vec::new();
        
        for entry in fs::read_dir(table_path)? {
            let entry = entry?;
            let path = entry.path();
            
            if path.is_file() {
                if let Some(ext) = path.extension() {
                    if ext == "parquet" {
                        if let Some(path_str) = path.to_str() {
                            parquet_files.push(path_str.to_string());
                        }
                    }
                }
            }
        }
        
        if parquet_files.is_empty() {
            return Err(format!("No parquet files found in {:?}", table_path).into());
        }
        
        // Sort for consistent ordering
        parquet_files.sort();
        Ok(parquet_files)
    }
    
    /// Discovers SQL binlog file in a table directory
    fn discover_binlog_file(table_path: &PathBuf) -> Result<String, Box<dyn std::error::Error>> {
        for entry in fs::read_dir(table_path)? {
            let entry = entry?;
            let path = entry.path();
            
            if path.is_file() {
                if let Some(ext) = path.extension() {
                    if ext == "sql" {
                        if let Some(path_str) = path.to_str() {
                            return Ok(path_str.to_string());
                        }
                    }
                }
            }
        }
        
        Err(format!("No SQL binlog file found in {:?}", table_path).into())
    }
    
    pub fn get_snapshot_position(&self) -> usize {
        self.manager.get_position()
    }
    
    pub fn get_snapshot_timestamp(&self) -> Option<&String> {
        self.manager.get_timestamp()
    }
    
    pub fn get_connection(&self) -> &Connection {
        self.manager.get_connection()
    }
    
    pub fn get_table_name(&self) -> &str {
        &self.table_name
    }
    
    /// Get mutable access to the underlying SnapshotManager
    pub fn get_manager_mut(&mut self) -> &mut SnapshotManager {
        &mut self.manager
    }
    
    /// Get the underlying SnapshotManager
    pub fn into_manager(self) -> SnapshotManager {
        self.manager
    }
}

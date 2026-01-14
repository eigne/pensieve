use duckdb::Connection;
use std::collections::HashMap;
use crate::binlog::{BinlogOperation, OperationType};

/// Handles applying binlog operations to a DuckDB connection
pub struct OperationApplier {
    conn: Connection,
    schema_cache: HashMap<String, Vec<String>>,
    type_cache: HashMap<String, Vec<String>>,
}

impl OperationApplier {
    pub fn new(conn: Connection) -> Self {
        Self {
            conn,
            schema_cache: HashMap::new(),
            type_cache: HashMap::new(),
        }
    }

    pub fn get_connection(&self) -> &Connection {
        &self.conn
    }

    pub fn into_connection(self) -> Connection {
        self.conn
    }

    /// Get table schema (columns and types) with caching
    fn get_table_schema(&mut self, table_name: &str) -> (Vec<String>, Vec<String>) {
        if let (Some(cols), Some(types)) = (self.schema_cache.get(table_name), self.type_cache.get(table_name)) {
            return (cols.clone(), types.clone());
        }

        let query = format!("PRAGMA table_info('{}')", table_name);
        let Ok(mut stmt) = self.conn.prepare(&query) else {
            return (Vec::new(), Vec::new())
        };

        let Ok(rows) = stmt.query_map([], |row| {
            let name: String = row.get(1)?;
            let col_type: String = row.get(2)?;
            Ok((name, col_type))
        }) else {
            return (Vec::new(), Vec::new());
        };

        let mut columns = Vec::new();
        let mut types = Vec::new();

        for row in rows {
            if let Ok((name, col_type)) = row {
                columns.push(name);
                types.push(col_type);
            }
        }

        self.schema_cache.insert(table_name.to_string(), columns.clone());
        self.type_cache.insert(table_name.to_string(), types.clone());

        (columns, types)
    }

    /// Generate SQL statement from a binlog operation
    pub fn generate_sql(&self, op: &BinlogOperation) -> String {
        match op.operation_type {
            OperationType::Insert => {
                let vals = op.after_values.as_ref().unwrap();
                format!(
                    "INSERT INTO {} ({}) VALUES ({});",
                    op.table_name,
                    op.columns.join(", "),
                    vals.join(", ")
                )
            }
            OperationType::Update => {
                let before = op.before_values.as_ref().unwrap();
                let after = op.after_values.as_ref().unwrap();
                
                let set_parts: Vec<String> = op.columns.iter()
                    .zip(after.iter())
                    .map(|(col, val)| format!("{} = {}", col, val))
                    .collect();
                    
                let where_parts: Vec<String> = op.columns.iter()
                    .zip(before.iter())
                    .filter(|(_, val)| *val != "NULL")
                    .map(|(col, val)| format!("{} = {}", col, val))
                    .collect();
                
                if where_parts.is_empty() {
                    format!(
                        "UPDATE {} SET {};",
                        op.table_name,
                        set_parts.join(", ")
                    )
                } else {
                    format!(
                        "UPDATE {} SET {} WHERE {};",
                        op.table_name,
                        set_parts.join(", "),
                        where_parts.join(" AND ")
                    )
                }
            }
            OperationType::Delete => {
                let before = op.before_values.as_ref().unwrap();
                let where_parts: Vec<String> = op.columns.iter()
                    .zip(before.iter())
                    .filter(|(_, val)| *val != "NULL")
                    .map(|(col, val)| format!("{} = {}", col, val))
                    .collect();
                
                if where_parts.is_empty() {
                    format!("DELETE FROM {};", op.table_name)
                } else {
                    format!(
                        "DELETE FROM {} WHERE {};",
                        op.table_name,
                        where_parts.join(" AND ")
                    )
                }
            }
        }
    }

    /// Fetch the current row from database matching the identifying values
    pub fn fetch_current_row(
        &mut self,
        table: &str,
        columns: &[String],
        identifying_values: &[String],
    ) -> Result<Option<Vec<String>>, Box<dyn std::error::Error>> {
        let where_parts: Vec<String> = columns.iter()
            .zip(identifying_values.iter())
            .filter(|(_, val)| *val != "NULL")
            .map(|(col, val)| format!("{} = {}", col, val))
            .collect();
        
        if where_parts.is_empty() {
            return Ok(None);
        }
        
        let (_, types) = self.get_table_schema(table);
        if types.is_empty() {
            return Ok(None);
        }
        
        let select_parts: Vec<String> = columns.iter()
            .map(|col| format!("CAST({} AS VARCHAR)", col))
            .collect();
        
        let query = format!(
            "SELECT {} FROM {} WHERE {} LIMIT 1",
            select_parts.join(", "),
            table,
            where_parts.join(" AND ")
        );
        
        let mut stmt = match self.conn.prepare(&query) {
            Ok(s) => s,
            Err(_) => return Ok(None),
        };
        
        let mut rows = stmt.query([])?;
        
        if let Some(row) = rows.next()? {
            let mut values = Vec::new();
            
            for i in 0..columns.len() {
                let col_type = types.get(i).map(|s| s.as_str()).unwrap_or("");
                let string_val: Option<String> = row.get(i)?;
                
                let value = match string_val {
                    Some(v) => {
                        if col_type.contains("VARCHAR") || col_type.contains("TEXT") || col_type.contains("CHAR")
                            || col_type.contains("TIMESTAMP") || col_type.contains("DATE") {
                            format!("'{}'", v)
                        } else if col_type.contains("BOOL") {
                            if v == "true" || v == "t" {
                                "1".to_string()
                            } else if v == "false" || v == "f" {
                                "0".to_string()
                            } else {
                                v
                            }
                        } else {
                            v
                        }
                    }
                    None => "NULL".to_string(),
                };
                
                values.push(value);
            }
            
            Ok(Some(values))
        } else {
            Ok(None)
        }
    }

    /// Check if an operation should be applied based on current database state
    /// If not, the operation can be safely skipped
    pub fn should_apply(&mut self, op: &BinlogOperation) -> Result<bool, Box<dyn std::error::Error>> {
        match op.operation_type {
            OperationType::Insert => {
                let after_vals = op.after_values.as_ref().unwrap();
                let current = self.fetch_current_row(&op.table_name, &op.columns, after_vals)?;
                
                match current {
                    None => Ok(true),
                    Some(current_vals) => Ok(&current_vals != after_vals)
                }
            }
            OperationType::Update | OperationType::Delete => {
                let before_vals = op.before_values.as_ref().unwrap();
                let current = self.fetch_current_row(&op.table_name, &op.columns, before_vals)?;
                
                match current {
                    None => Ok(false),
                    Some(current_vals) => Ok(&current_vals == before_vals),
                }
            }
        }
    }

    /// Apply an operation conditionally (only if it would actually make a change to the table)
    pub fn apply_operation_conditionally(&mut self, op: &BinlogOperation) -> Result<bool, Box<dyn std::error::Error>> {
        if self.should_apply(op)? {
            let sql = self.generate_sql(op);
            self.conn.execute(&sql, [])?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}


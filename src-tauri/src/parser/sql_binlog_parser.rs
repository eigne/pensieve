use mysql_binlog_connector_rust::binlog_parser::BinlogParser;
use mysql_binlog_connector_rust::column::column_value::ColumnValue;
use mysql_binlog_connector_rust::event::event_data::EventData;
use std::collections::HashMap;
use std::fs::File;
use duckdb::Connection;
use chrono::{TimeZone, Utc};
use crate::binlog::{BinlogOperation, OperationType};

/// Parser for binary MySQL binlog files
///
/// Column names are resolved from the DuckDB schema
/// since MySQL binlogs only contain positional column values, not column names.
pub struct BinaryBinlogParser {
    conn: Connection,
    schema_cache: HashMap<String, Vec<String>>,
}

// (operations, pending_operations, in_transaction)
type ParseResult = (Vec<BinlogOperation>, Vec<BinlogOperation>, bool);

impl BinaryBinlogParser {
    pub fn new(conn: Connection) -> Self {
        Self {
            conn,
            schema_cache: HashMap::new(),
        }
    }

    /// Take ownership of the connection (for use after parsing)
    pub fn into_connection(self) -> Connection {
        self.conn
    }

    /// Parse multiple binary binlog files and return a list of operations.
    ///
    /// # Example
    /// ```ignore
    /// let files = vec![
    ///     "binlog.000029",
    ///     "binlog.000027",
    ///     "binlog.000028",
    /// ];
    /// let operations = parser.parse_files(&files)?;
    /// // Files are parsed in order: 000027, 000028, 000029
    /// ```
    pub fn parse_files(&mut self, filepaths: &[&str]) -> Result<Vec<BinlogOperation>, Box<dyn std::error::Error>> {
        if filepaths.is_empty() {
            return Ok(Vec::new());
        }

        let mut sorted_paths: Vec<&str> = filepaths.to_vec();
        sorted_paths.sort();

        println!("Parsing {} binlog files in order:", sorted_paths.len());
        for (i, path) in sorted_paths.iter().enumerate() {
            println!("  {}: {}", i + 1, path);
        }

        let mut all_operations = Vec::new();
        let mut pending_operations: Vec<BinlogOperation> = Vec::new();
        let mut in_transaction = false;

        for filepath in sorted_paths {
            println!("Parsing binlog file: {}", filepath);
            
            let (ops, pending, still_in_tx) = self.parse_file_internal(
                filepath,
                pending_operations,
                in_transaction,
            )?;
            eprintln!("ops: {}", ops.len());

            all_operations.extend(ops);
            pending_operations = pending;
            in_transaction = still_in_tx;
        }

        if in_transaction && !pending_operations.is_empty() {
            eprintln!(
                "Warning: Discarding {} uncommitted operations from incomplete transaction",
                pending_operations.len()
            );
        }

        Ok(all_operations)
    }

    /// Parse a single binary binlog file and return a list of operations.
    pub fn parse_file(&mut self, filepath: &str) -> Result<Vec<BinlogOperation>, Box<dyn std::error::Error>> {
        self.parse_files(&[filepath])
    }

    /// Internal parsing method that maintains transaction state across files.
    fn parse_file_internal(
        &mut self,
        filepath: &str,
        mut pending_operations: Vec<BinlogOperation>,
        mut in_transaction: bool,
    ) -> Result<ParseResult, Box<dyn std::error::Error>> {
        let mut file = File::open(filepath)?;

        let mut parser = BinlogParser {
            checksum_length: 4,
            table_map_event_by_table_id: HashMap::new(),
        };

        parser.check_magic(&mut file)?;

        let mut operations = Vec::new();

        while let Ok((header, data)) = parser.next(&mut file) {
            // Extract timestamp from event header
            let current_timestamp = Some(format_timestamp(header.timestamp));
            let position = Some(header.next_event_position);

            match data {
                EventData::Query(query_event) => {
                    let sql = query_event.query.trim().to_uppercase();
                    if sql == "BEGIN" {
                        in_transaction = true;
                        pending_operations.clear();
                    } else if sql == "COMMIT" {
                        if in_transaction {
                            operations.append(&mut pending_operations);
                        }
                        in_transaction = false;
                    } else if sql == "ROLLBACK" {
                        pending_operations.clear();
                        in_transaction = false;
                    }
                    // DDL statements (CREATE, ALTER, DROP) are ignored for now
                }

                EventData::TableMap(_) => {
                    // TableMap events are automatically tracked by the parser's
                    // table_map_event_by_table_id HashMap - no action needed
                }

                EventData::WriteRows(write_event) => {
                    if let Some(table_map) = parser.table_map_event_by_table_id.get(&write_event.table_id) {
                        let table_name = table_map.table_name.clone();
                        let database = table_map.database_name.clone();
                        let columns = self.get_table_schema(&table_name);

                        // Skip tables not in our snapshot
                        if columns.is_empty() {
                            continue;
                        }

                        for row in &write_event.rows {
                            let after_values = convert_row_to_sql_values(&row.column_values);

                            let op = BinlogOperation {
                                timestamp: current_timestamp.clone(),
                                position,
                                operation_type: OperationType::Insert,
                                table_name: table_name.clone(),
                                database: database.clone(),
                                columns: columns.clone(),
                                before_values: None,
                                after_values: Some(after_values),
                            };

                            if in_transaction {
                                pending_operations.push(op);
                            } else {
                                operations.push(op);
                            }
                        }
                    }
                }

                EventData::UpdateRows(update_event) => {
                    if let Some(table_map) = parser.table_map_event_by_table_id.get(&update_event.table_id) {
                        let table_name = table_map.table_name.clone();
                        let database = table_map.database_name.clone();
                        let columns = self.get_table_schema(&table_name);

                        // Skip tables not in our snapshot
                        if columns.is_empty() {
                            continue;
                        }

                        for row_pair in &update_event.rows {
                            let before_row = &row_pair.0;
                            let after_row = &row_pair.1;

                            let before_values = convert_row_to_sql_values(&before_row.column_values);
                            let after_values = convert_row_to_sql_values(&after_row.column_values);

                            let op = BinlogOperation {
                                timestamp: current_timestamp.clone(),
                                position,
                                operation_type: OperationType::Update,
                                table_name: table_name.clone(),
                                database: database.clone(),
                                columns: columns.clone(),
                                before_values: Some(before_values),
                                after_values: Some(after_values),
                            };

                            if in_transaction {
                                pending_operations.push(op);
                            } else {
                                operations.push(op);
                            }
                        }
                    }
                }

                EventData::DeleteRows(delete_event) => {
                    if let Some(table_map) = parser.table_map_event_by_table_id.get(&delete_event.table_id) {
                        let table_name = table_map.table_name.clone();
                        let database = table_map.database_name.clone();
                        let columns = self.get_table_schema(&table_name);

                        // Skip tables not in our snapshot
                        if columns.is_empty() {
                            continue;
                        }

                        for row in &delete_event.rows {
                            let before_values = convert_row_to_sql_values(&row.column_values);

                            let op = BinlogOperation {
                                timestamp: current_timestamp.clone(),
                                position,
                                operation_type: OperationType::Delete,
                                table_name: table_name.clone(),
                                database: database.clone(),
                                columns: columns.clone(),
                                before_values: Some(before_values),
                                after_values: None,
                            };

                            if in_transaction {
                                pending_operations.push(op);
                            } else {
                                operations.push(op);
                            }
                        }
                    }
                }

                EventData::Xid(_) => {
                    // XID event marks the end of a transaction (similar to COMMIT)
                    if in_transaction {
                        operations.append(&mut pending_operations);
                    }
                    in_transaction = false;
                }

                _ => {
                    // Ignore other event types (ROTATE, FORMAT_DESCRIPTION, etc.)
                }
            }
        }

        Ok((operations, pending_operations, in_transaction))
    }

    /// Get column names for a table from DuckDB schema (with caching)
    fn get_table_schema(&mut self, table_name: &str) -> Vec<String> {
        if let Some(cols) = self.schema_cache.get(table_name) {
            return cols.clone();
        }

        let query = format!("PRAGMA table_info('{}')", table_name);
        let Ok(mut stmt) = self.conn.prepare(&query) else {
            return Vec::new();
        };

        let Ok(rows) = stmt.query_map([], |row| {
            let name: String = row.get(1)?;
            Ok(name)
        }) else {
            return Vec::new();
        };

        let mut columns = Vec::new();
        for row in rows {
            if let Ok(name) = row {
                columns.push(name);
            }
        }

        self.schema_cache.insert(table_name.to_string(), columns.clone());
        columns
    }
}

/// Convert Unix timestamp to binlog timestamp format (YYMMDD HH:MM:SS)
fn format_timestamp(unix_timestamp: u32) -> String {
    match Utc.timestamp_opt(unix_timestamp as i64, 0).single() {
        Some(dt) => dt.format("%y%m%d %H:%M:%S").to_string(),
        None => format!("{}", unix_timestamp),
    }
}

/// Convert a row's column values to SQL-compatible string representations
fn convert_row_to_sql_values(column_values: &[ColumnValue]) -> Vec<String> {
    column_values.iter().map(column_value_to_sql).collect()
}

/// Convert a single ColumnValue to its SQL string representation
fn column_value_to_sql(value: &ColumnValue) -> String {
    match value {
        // NULL handling
        ColumnValue::None => "NULL".to_string(),

        // Integer types
        ColumnValue::Tiny(v) => v.to_string(),
        ColumnValue::Short(v) => v.to_string(),
        ColumnValue::Long(v) => v.to_string(),
        ColumnValue::LongLong(v) => v.to_string(),

        // Floating point types
        ColumnValue::Float(v) => v.to_string(),
        ColumnValue::Double(v) => v.to_string(),

        // Decimal type (stored as string in the library)
        ColumnValue::Decimal(v) => v.to_string(),

        // Date and time types (stored as strings in the library)
        ColumnValue::Date(v) => format!("'{}'", v),
        ColumnValue::Time(v) => format!("'{}'", v),
        ColumnValue::DateTime(v) => format!("'{}'", v),
        
        // Timestamp as Unix epoch - convert to datetime string
        ColumnValue::Timestamp(v) => {
            match Utc.timestamp_opt(*v, 0).single() {
                Some(dt) => format!("'{}'", dt.format("%Y-%m-%d %H:%M:%S")),
                None => format!("{}", v),
            }
        }

        // Year type
        ColumnValue::Year(v) => v.to_string(),

        // String type (Vec<u8> - may contain UTF-8 or binary data)
        ColumnValue::String(v) => {
            match String::from_utf8(v.clone()) {
                Ok(s) => format!("'{}'", s.replace('\'', "''")),
                // If not valid UTF-8, treat as binary
                Err(_) => format!("X'{}'", bytes_to_hex(v)),
            }
        }

        // Binary/Blob types
        ColumnValue::Blob(v) => {
            if v.is_empty() {
                "''".to_string()
            } else {
                // Try to interpret as UTF-8 string first
                match String::from_utf8(v.clone()) {
                    Ok(s) => format!("'{}'", s.replace('\'', "''")),
                    // Otherwise use hex encoding
                    Err(_) => format!("X'{}'", bytes_to_hex(v)),
                }
            }
        }

        // Bit type
        ColumnValue::Bit(v) => format!("b'{:b}'", v),

        // Enum and Set (stored as integers in binlog)
        ColumnValue::Enum(v) => v.to_string(),
        ColumnValue::Set(v) => v.to_string(),

        // JSON type (stored as Vec<u8>)
        ColumnValue::Json(v) => {
            // Try to parse as UTF-8 JSON string
            match String::from_utf8(v.clone()) {
                Ok(s) => format!("'{}'", s.replace('\'', "''")),
                Err(_) => format!("X'{}'", bytes_to_hex(v)),
            }
        }
    }
}

/// Convert bytes to hexadecimal string
fn bytes_to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02X}", b)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operation_applier::OperationApplier;

    // ===========================================
    // Helper Functions
    // ===========================================

    fn create_test_db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name VARCHAR,
                email VARCHAR,
                age INTEGER,
                balance DECIMAL(10,2),
                is_active BOOLEAN,
                created_at TIMESTAMP
            )"
        ).unwrap();
        
        conn.execute_batch(
            "INSERT INTO users VALUES 
                (1, 'Alice', 'alice@example.com', 30, 1000.50, true, '2024-01-01 10:00:00'),
                (2, 'Bob', 'bob@example.com', 25, 500.00, true, '2024-01-02 11:00:00'),
                (3, 'Charlie', 'charlie@example.com', 35, 1500.75, false, '2024-01-03 12:00:00')"
        ).unwrap();
        
        conn
    }

    // ===========================================
    // ColumnValue Conversion Tests
    // ===========================================

    #[test]
    fn test_column_value_to_sql_integers() {
        assert_eq!(column_value_to_sql(&ColumnValue::Tiny(42)), "42");
        assert_eq!(column_value_to_sql(&ColumnValue::Short(-100)), "-100");
        assert_eq!(column_value_to_sql(&ColumnValue::Long(123456)), "123456");
        assert_eq!(column_value_to_sql(&ColumnValue::LongLong(9999999999)), "9999999999");
    }

    #[test]
    fn test_column_value_to_sql_floats() {
        assert_eq!(column_value_to_sql(&ColumnValue::Float(3.14)), "3.14");
        assert_eq!(column_value_to_sql(&ColumnValue::Double(2.71828)), "2.71828");
    }

    #[test]
    fn test_column_value_to_sql_strings() {
        assert_eq!(
            column_value_to_sql(&ColumnValue::String(b"hello".to_vec())),
            "'hello'"
        );
        assert_eq!(
            column_value_to_sql(&ColumnValue::String(b"it's a test".to_vec())),
            "'it''s a test'"
        );
    }

    #[test]
    fn test_column_value_to_sql_null() {
        assert_eq!(column_value_to_sql(&ColumnValue::None), "NULL");
    }

    #[test]
    fn test_column_value_to_sql_date() {
        assert_eq!(
            column_value_to_sql(&ColumnValue::Date("2024-01-15".to_string())),
            "'2024-01-15'"
        );
    }

    #[test]
    fn test_column_value_to_sql_datetime() {
        assert_eq!(
            column_value_to_sql(&ColumnValue::DateTime("2024-01-15 10:30:45".to_string())),
            "'2024-01-15 10:30:45'"
        );
    }

    #[test]
    fn test_column_value_to_sql_blob() {
        // Binary data that's not valid UTF-8
        assert_eq!(
            column_value_to_sql(&ColumnValue::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF])),
            "X'DEADBEEF'"
        );
    }

    #[test]
    fn test_bytes_to_hex() {
        assert_eq!(bytes_to_hex(&[0x00, 0xFF, 0xAB]), "00FFAB");
        assert_eq!(bytes_to_hex(&[]), "");
    }

    #[test]
    fn test_format_timestamp() {
        // Unix timestamp for 2024-01-15 12:30:45 UTC
        let timestamp = 1705321845;
        let formatted = format_timestamp(timestamp);
        assert_eq!(formatted, "240115 12:30:45");
    }

    // ===========================================
    // Operation Inversion Tests
    // ===========================================

    #[test]
    fn test_invert_insert_to_delete() {
        let insert_op = BinlogOperation {
            timestamp: Some("251020 10:00:00".to_string()),
            position: Some(100),
            operation_type: OperationType::Insert,
            table_name: "users".to_string(),
            database: "main".to_string(),
            columns: vec!["id".to_string(), "name".to_string()],
            before_values: None,
            after_values: Some(vec!["10".to_string(), "'NewUser'".to_string()]),
        };
        
        let inverted = insert_op.invert();
        
        assert_eq!(inverted.operation_type, OperationType::Delete);
        assert_eq!(inverted.before_values, insert_op.after_values);
        assert!(inverted.after_values.is_none());
        assert_eq!(inverted.table_name, insert_op.table_name);
    }

    #[test]
    fn test_invert_update_swaps_before_after() {
        let update_op = BinlogOperation {
            timestamp: Some("251020 10:00:00".to_string()),
            position: Some(200),
            operation_type: OperationType::Update,
            table_name: "users".to_string(),
            database: "main".to_string(),
            columns: vec!["id".to_string(), "name".to_string()],
            before_values: Some(vec!["1".to_string(), "'Alice'".to_string()]),
            after_values: Some(vec!["1".to_string(), "'Alice Smith'".to_string()]),
        };
        
        let inverted = update_op.invert();
        
        assert_eq!(inverted.operation_type, OperationType::Update);
        assert_eq!(inverted.before_values, update_op.after_values);
        assert_eq!(inverted.after_values, update_op.before_values);
    }

    #[test]
    fn test_invert_delete_to_insert() {
        let delete_op = BinlogOperation {
            timestamp: Some("251020 10:00:00".to_string()),
            position: Some(300),
            operation_type: OperationType::Delete,
            table_name: "users".to_string(),
            database: "main".to_string(),
            columns: vec!["id".to_string(), "name".to_string()],
            before_values: Some(vec!["3".to_string(), "'Charlie'".to_string()]),
            after_values: None,
        };
        
        let inverted = delete_op.invert();
        
        assert_eq!(inverted.operation_type, OperationType::Insert);
        assert!(inverted.before_values.is_none());
        assert_eq!(inverted.after_values, delete_op.before_values);
    }

    // ===========================================
    // SQL Generation Tests
    // ===========================================

    #[test]
    fn test_generate_insert_sql() {
        let conn = create_test_db();
        let applier = OperationApplier::new(conn);
        
        let insert_op = BinlogOperation {
            timestamp: None,
            position: None,
            operation_type: OperationType::Insert,
            table_name: "users".to_string(),
            database: "main".to_string(),
            columns: vec!["id".to_string(), "name".to_string(), "email".to_string()],
            before_values: None,
            after_values: Some(vec!["4".to_string(), "'David'".to_string(), "'david@test.com'".to_string()]),
        };
        
        let sql = applier.generate_sql(&insert_op);

        assert_eq!(sql, "INSERT INTO users (id, name, email) VALUES (4, 'David', 'david@test.com');");
    }

    #[test]
    fn test_generate_update_sql() {
        let conn = create_test_db();
        let applier = OperationApplier::new(conn);
        
        let update_op = BinlogOperation {
            timestamp: None,
            position: None,
            operation_type: OperationType::Update,
            table_name: "users".to_string(),
            database: "main".to_string(),
            columns: vec!["id".to_string(), "name".to_string()],
            before_values: Some(vec!["1".to_string(), "'Alice'".to_string()]),
            after_values: Some(vec!["1".to_string(), "'Alice Smith'".to_string()]),
        };
        
        let sql = applier.generate_sql(&update_op);

        assert_eq!(sql, "UPDATE users SET id = 1, name = 'Alice Smith' WHERE id = 1 AND name = 'Alice';");
    }

    #[test]
    fn test_generate_delete_sql() {
        let conn = create_test_db();
        let applier = OperationApplier::new(conn);
        
        let delete_op = BinlogOperation {
            timestamp: None,
            position: None,
            operation_type: OperationType::Delete,
            table_name: "users".to_string(),
            database: "main".to_string(),
            columns: vec!["id".to_string(), "name".to_string()],
            before_values: Some(vec!["3".to_string(), "'Charlie'".to_string()]),
            after_values: None,
        };
        
        let sql = applier.generate_sql(&delete_op);

        assert_eq!(sql, "DELETE FROM users WHERE id = 3 AND name = 'Charlie';");
    }

    // ===========================================
    // Should Apply Tests
    // ===========================================

    #[test]
    fn test_should_apply_insert_for_new_row() {
        let conn = create_test_db();
        let mut applier = OperationApplier::new(conn);
        
        let new_insert = BinlogOperation {
            timestamp: None,
            position: None,
            operation_type: OperationType::Insert,
            table_name: "users".to_string(),
            database: "main".to_string(),
            columns: vec!["id".to_string(), "name".to_string(), "email".to_string(), 
                         "age".to_string(), "balance".to_string(), "is_active".to_string(), 
                         "created_at".to_string()],
            before_values: None,
            after_values: Some(vec!["10".to_string(), "'NewUser'".to_string(), 
                                   "'new@test.com'".to_string(), "25".to_string(), 
                                   "100.0".to_string(), "1".to_string(), 
                                   "'2024-01-01 10:00:00'".to_string()]),
        };
        
        let should_apply = applier.should_apply(&new_insert).unwrap();
        assert!(should_apply, "Should apply INSERT for non-existent row");
    }

    #[test]
    fn test_should_not_apply_update_when_before_image_mismatches() {
        let conn = create_test_db();
        let mut applier = OperationApplier::new(conn);
        
        // Current DB has: Alice, age 30
        // This UPDATE expects: WrongName, age 99 (doesn't match current state)
        let invalid_update = BinlogOperation {
            timestamp: None,
            position: None,
            operation_type: OperationType::Update,
            table_name: "users".to_string(),
            database: "main".to_string(),
            columns: vec!["id".to_string(), "name".to_string(), "email".to_string(), 
                         "age".to_string(), "balance".to_string(), "is_active".to_string(), 
                         "created_at".to_string()],
            before_values: Some(vec!["1".to_string(), "'WrongName'".to_string(), 
                                    "'alice@example.com'".to_string(), "99".to_string(), 
                                    "999.99".to_string(), "0".to_string(), 
                                    "'2024-01-01 10:00:00'".to_string()]),
            after_values: Some(vec!["1".to_string(), "'Alice Smith'".to_string(), 
                                   "'alice@example.com'".to_string(), "31".to_string(), 
                                   "1000.5".to_string(), "1".to_string(), 
                                   "'2024-01-01 10:00:00'".to_string()]),
        };
        
        let should_apply = applier.should_apply(&invalid_update).unwrap();
        assert!(!should_apply, "Should not apply UPDATE when before-image doesn't match current state");
    }

    #[test]
    fn test_should_not_apply_delete_when_row_missing() {
        let conn = create_test_db();
        let mut applier = OperationApplier::new(conn);
        
        // Try to delete row with id=99 (doesn't exist)
        let delete_nonexistent = BinlogOperation {
            timestamp: None,
            position: None,
            operation_type: OperationType::Delete,
            table_name: "users".to_string(),
            database: "main".to_string(),
            columns: vec!["id".to_string(), "name".to_string(), "email".to_string(), 
                         "age".to_string(), "balance".to_string(), "is_active".to_string(), 
                         "created_at".to_string()],
            before_values: Some(vec!["99".to_string(), "'Nobody'".to_string(), 
                                    "'none@test.com'".to_string(), "0".to_string(), 
                                    "0.0".to_string(), "0".to_string(), 
                                    "'2024-01-01 10:00:00'".to_string()]),
            after_values: None,
        };
        
        let should_apply = applier.should_apply(&delete_nonexistent).unwrap();
        assert!(!should_apply, "Should not apply DELETE when row doesn't exist");
    }

    // ===========================================
    // Integration Tests
    // ===========================================

    #[test]
    fn test_bidirectional_integration() {
        let conn = create_test_db();
        let mut applier = OperationApplier::new(conn);
        
        // Step 1: Create an UPDATE operation (Alice 30 → Alice Smith 31)
        let update_op = BinlogOperation {
            timestamp: None,
            position: None,
            operation_type: OperationType::Update,
            table_name: "users".to_string(),
            database: "main".to_string(),
            columns: vec!["id".to_string(), "name".to_string(), "email".to_string(), 
                         "age".to_string(), "balance".to_string(), "is_active".to_string(), 
                         "created_at".to_string()],
            before_values: Some(vec!["1".to_string(), "'Alice'".to_string(), 
                                    "'alice@example.com'".to_string(), "30".to_string(), 
                                    "1000.50".to_string(), "1".to_string(), 
                                    "'2024-01-01 10:00:00'".to_string()]),
            after_values: Some(vec!["1".to_string(), "'Alice Smith'".to_string(), 
                                   "'alice@example.com'".to_string(), "31".to_string(), 
                                   "1000.50".to_string(), "1".to_string(), 
                                   "'2024-01-01 10:00:00'".to_string()]),
        };
        
        // Step 2: Apply forward (should work - before-image matches)
        let applied = applier.apply_operation_conditionally(&update_op).unwrap();
        assert!(applied, "Operation should be applied");
        
        // Step 3: Verify change was applied
        let mut stmt = applier.get_connection().prepare("SELECT name, age FROM users WHERE id = 1").unwrap();
        let mut rows = stmt.query([]).unwrap();
        let row = rows.next().unwrap().unwrap();
        let name: String = row.get(0).unwrap();
        let age: i32 = row.get(1).unwrap();
        assert_eq!(name, "Alice Smith");
        assert_eq!(age, 31);
        
        // Step 4: Try to apply again (should be skipped - before-image no longer matches)
        let applied_again = applier.apply_operation_conditionally(&update_op).unwrap();
        assert!(!applied_again, "Operation should be skipped on second application");
        
        // Step 5: Invert the operation and apply (revert the change)
        let inverted = update_op.invert();
        let reverted = applier.apply_operation_conditionally(&inverted).unwrap();
        assert!(reverted, "Inverted operation should be applied");
        
        // Step 6: Verify we're back to original state
        let mut stmt = applier.get_connection().prepare("SELECT name, age FROM users WHERE id = 1").unwrap();
        let mut rows = stmt.query([]).unwrap();
        let row = rows.next().unwrap().unwrap();
        let name: String = row.get(0).unwrap();
        let age: i32 = row.get(1).unwrap();
        assert_eq!(name, "Alice");
        assert_eq!(age, 30);
    }

    #[test]
    fn test_skip_already_applied_operations() {
        let conn = create_test_db();
        let mut applier = OperationApplier::new(conn);
        
        // Scenario: Snapshot already contains this INSERT
        // Current DB has Bob (id=2)
        // Try to INSERT Bob again
        let already_applied_insert = BinlogOperation {
            timestamp: None,
            position: None,
            operation_type: OperationType::Insert,
            table_name: "users".to_string(),
            database: "main".to_string(),
            columns: vec!["id".to_string(), "name".to_string(), "email".to_string(), 
                         "age".to_string(), "balance".to_string(), "is_active".to_string(), 
                         "created_at".to_string()],
            before_values: None,
            after_values: Some(vec!["2".to_string(), "'Bob'".to_string(), 
                                   "'bob@example.com'".to_string(), "25".to_string(), 
                                   "500.00".to_string(), "1".to_string(), 
                                   "'2024-01-02 11:00:00'".to_string()]),
        };
        
        // This should be skipped (row already exists with same values)
        assert!(!applier.should_apply(&already_applied_insert).unwrap());
    }

    // ===========================================
    // Schema Cache Tests
    // ===========================================

    #[test]
    fn test_get_table_schema_caching() {
        let conn = create_test_db();
        let mut parser = BinaryBinlogParser::new(conn);
        
        // First call should query the database
        let columns1 = parser.get_table_schema("users");
        assert_eq!(columns1.len(), 7);
        assert_eq!(columns1[0], "id");
        assert_eq!(columns1[1], "name");
        
        // Second call should use cache
        let columns2 = parser.get_table_schema("users");
        assert_eq!(columns1, columns2);
        
        // Non-existent table should return empty
        let empty = parser.get_table_schema("nonexistent_table");
        assert!(empty.is_empty());
    }

    #[test]
    fn test_convert_row_to_sql_values() {
        let row = vec![
            ColumnValue::Long(1),
            ColumnValue::String(b"Alice".to_vec()),
            ColumnValue::None,
            ColumnValue::Double(100.50),
        ];
        
        let sql_values = convert_row_to_sql_values(&row);
        
        assert_eq!(sql_values.len(), 4);
        assert_eq!(sql_values[0], "1");
        assert_eq!(sql_values[1], "'Alice'");
        assert_eq!(sql_values[2], "NULL");
        assert_eq!(sql_values[3], "100.5");
    }

    // ===========================================
    // Multi-File Parsing Tests
    // ===========================================

    #[test]
    fn test_binlog_file_sorting() {
        // Test that binlog files are sorted alphanumerically
        let mut files = vec![
            "binlog.000031",
            "binlog.000027",
            "binlog.000029",
            "binlog.000028",
            "binlog.000030",
        ];
        files.sort();
        
        assert_eq!(files, vec![
            "binlog.000027",
            "binlog.000028",
            "binlog.000029",
            "binlog.000030",
            "binlog.000031",
        ]);
    }

    #[test]
    fn test_binlog_file_sorting_with_paths() {
        // Test sorting with full paths
        let mut files = vec![
            "/var/lib/mysql/binlog.000031",
            "/var/lib/mysql/binlog.000027",
            "/var/lib/mysql/binlog.000029",
        ];
        files.sort();
        
        assert_eq!(files, vec![
            "/var/lib/mysql/binlog.000027",
            "/var/lib/mysql/binlog.000029",
            "/var/lib/mysql/binlog.000031",
        ]);
    }

    #[test]
    fn test_parse_files_empty_list() {
        let conn = create_test_db();
        let mut parser = BinaryBinlogParser::new(conn);
        
        let files: Vec<&str> = vec![];
        let operations = parser.parse_files(&files).unwrap();
        
        assert!(operations.is_empty());
    }
}

use duckdb::Connection;
use regex::Regex;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::io;
use std::io::{BufRead, BufReader, BufWriter, Write};
use crate::binlog::{BinlogOperation, OperationType};

#[derive(Debug)]
pub struct NoSchemaTypesFoundError;

impl Display for NoSchemaTypesFoundError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "PRAGMA query returned no table column types")
    }
}

impl std::error::Error for NoSchemaTypesFoundError {}

/// Parser for text-format MySQL binlog files
/// Binlog must have been generated with the --verbose and --base64-output=DECODE-ROWS options
pub struct TextBinlogParser {
    conn: Connection,
    schema_cache: HashMap<String, Vec<String>>,
    timestamp_regex: Regex,
    position_regex: Regex,
    update_regex: Regex,
    insert_regex: Regex,
    delete_regex: Regex,
    table_name_regex: Regex,
    column_value_regex: Regex,
    begin_regex: Regex,
    commit_regex: Regex,
    rollback_regex: Regex,
}

impl TextBinlogParser {
    pub fn new(conn: Connection) -> Self {
        Self {
            conn,
            schema_cache: HashMap::new(),
            timestamp_regex: Regex::new(r"^#(\d{6})\s+(\d{1,2}:\d{2}:\d{2})").unwrap(),
            position_regex: Regex::new(r"end_log_pos\s+(\d+)").unwrap(),
            update_regex: Regex::new(r"^### UPDATE\s+(.+)").unwrap(),
            insert_regex: Regex::new(r"^### INSERT INTO\s+(.+)").unwrap(),
            delete_regex: Regex::new(r"^### DELETE FROM\s+(.+)").unwrap(),
            table_name_regex: Regex::new(r"`([^`]+)`\.`([^`]+)`").unwrap(),
            column_value_regex: Regex::new(r"^###\s+@(\d+)=(.*)$").unwrap(),
            begin_regex: Regex::new(r"^BEGIN").unwrap(),
            commit_regex: Regex::new(r"^COMMIT").unwrap(),
            rollback_regex: Regex::new(r"^ROLLBACK").unwrap(),
        }
    }

    /// Take ownership of the connection (for use after parsing)
    pub fn into_connection(self) -> Connection {
        self.conn
    }

    pub fn parse_file(&mut self, filepath: &str) -> Result<Vec<BinlogOperation>, Box<dyn std::error::Error>> {
        let file = File::open(filepath)?;
        let reader = BufReader::with_capacity(10 * 1024 * 1024, file);
        
        let mut operations = Vec::new();
        // Use a manual line reader that handles binary data
        let lines = reader.split(b'\n').map(|line_result| {
            line_result.map(|bytes| String::from_utf8_lossy(&bytes).to_string())
        });
        let mut lines = lines.peekable();
        
        let mut current_timestamp: Option<String> = None;
        let mut current_position: Option<u32> = None;

        // These two variables help us keep track of whether a transaction is committed or rolled back.
        // We only consider transactions that are successfully committed.
        let mut in_transaction = false;
        let mut pending_operations: Vec<BinlogOperation> = Vec::new();

        // These two variables are just for logging.
        let mut writer = BufWriter::new(io::stdout().lock());
        let mut i = 0;

        while let Some(Ok(line)) = lines.next() {
            writeln!(writer, "LINE #{}", i).unwrap();
            i += 1;
            if i % 100000 == 0 {
                writer.flush()?;
            }

            if self.begin_regex.is_match(&line) {
                in_transaction = true;
                pending_operations.clear();
                continue;
            }
            
            if self.commit_regex.is_match(&line) {
                if in_transaction {
                    operations.append(&mut pending_operations);
                }
                in_transaction = false;
                pending_operations.clear();
                continue;
            }
            
            if self.rollback_regex.is_match(&line) {
                if in_transaction {
                    pending_operations.clear();
                }
                in_transaction = false;
                continue;
            }
            
            if let Some(captures) = self.timestamp_regex.captures(&line) {
                let date = &captures[1];
                let time = &captures[2];
                current_timestamp = Some(format!("{} {}", date, time));
            }
            
            if let Some(captures) = self.position_regex.captures(&line) {
                if let Ok(pos) = captures[1].parse::<u32>() {
                    current_position = Some(pos);
                }
            }
            
            if let Some(captures) = self.update_regex.captures(&line) {
                let table_path = captures[1].to_string();
                if let Some(op) = self.parse_update(&mut lines, &table_path, &current_timestamp, current_position)? {
                    if in_transaction {
                        pending_operations.push(op);
                    } else {
                        // This probably never executes, since all UPDATEs must be part of a transaction...
                        operations.push(op);
                    }
                }
            }
            
            if let Some(captures) = self.insert_regex.captures(&line) {
                let table_path = captures[1].to_string();
                if let Some(op) = self.parse_insert(&mut lines, &table_path, &current_timestamp, current_position)? {
                    if in_transaction {
                        pending_operations.push(op);
                    } else {
                        // This probably never executes, since all INSERTs must be part of a transaction...
                        operations.push(op);
                    }
                }
            }
            
            if let Some(captures) = self.delete_regex.captures(&line) {
                let table_path = captures[1].to_string();
                if let Some(op) = self.parse_delete(&mut lines, &table_path, &current_timestamp, current_position)? {
                    if in_transaction {
                        pending_operations.push(op);
                    } else {
                        // This probably never executes, since all DELETEs must be part of a transaction...
                        operations.push(op);
                    }
                }
            }
        }
        
        Ok(operations)
    }

    fn parse_update<I>(
        &mut self,
        lines: &mut std::iter::Peekable<I>,
        table_path: &str,
        timestamp: &Option<String>,
        position: Option<u32>,
    ) -> Result<Option<BinlogOperation>, Box<dyn std::error::Error>>
    where
        I: Iterator<Item = Result<String, io::Error>>
    {
        let (db, table) = self.extract_table_name(table_path);
        let columns = self.get_table_schema(&table);

        // Columns will be empty if the table was not found in the parquet snapshot, and hence,
        // not loaded into DuckDB
        if columns.is_empty() {
            self.skip_to_next_sql_operation(lines);
            return Ok(None);
        }
        
        // Parse WHERE clause
        let mut where_values: HashMap<usize, String> = HashMap::new();
        let mut found_set = false;
        
        while let Some(Ok(line)) = lines.peek() {
            if !line.starts_with("###") {
                break;
            }
            
            // Stop if we hit another SQL statement
            if line.contains("### UPDATE") || line.contains("### INSERT INTO") || line.contains("### DELETE FROM") {
                break;
            }
            
            if line.contains("### SET") {
                found_set = true;
                lines.next(); // Consume the SET line
                break;
            }
            
            let line = lines.next().unwrap().unwrap();
            if let Some(captures) = self.column_value_regex.captures(&line) {
                let col_num: usize = captures[1].parse()?;
                let value = captures[2].to_string();
                where_values.insert(col_num, value);
            }
        }
        
        // Parse SET clause
        let mut set_values: HashMap<usize, String> = HashMap::new();
        if found_set {
            while let Some(Ok(line)) = lines.peek() {
                if !line.starts_with("###") {
                    break;
                }
                
                // Stop if we hit another SQL statement
                if line.contains("### UPDATE") || line.contains("### INSERT INTO") || line.contains("### DELETE FROM") {
                    break;
                }
                
                let line = lines.next().unwrap().unwrap();
                if let Some(captures) = self.column_value_regex.captures(&line) {
                    let col_num: usize = captures[1].parse()?;
                    let value = captures[2].to_string();
                    set_values.insert(col_num, value);
                }
            }
        }
        
        // Convert HashMap to Vec (ordered by column index)
        let mut before_vals = vec!["NULL".to_string(); columns.len()];
        let mut after_vals = vec!["NULL".to_string(); columns.len()];
        
        for (i, _col) in columns.iter().enumerate() {
            let col_idx = i + 1; // @1 = column 0, etc.
            if let Some(val) = where_values.get(&col_idx) {
                before_vals[i] = val.clone();
            }
            if let Some(val) = set_values.get(&col_idx) {
                after_vals[i] = val.clone();
            }
        }
        
        Ok(Some(BinlogOperation {
            timestamp: timestamp.clone(),
            position,
            operation_type: OperationType::Update,
            table_name: table,
            database: db,
            columns,
            before_values: Some(before_vals),
            after_values: Some(after_vals),
        }))
    }


    fn parse_insert<I>(
        &mut self,
        lines: &mut std::iter::Peekable<I>,
        table_path: &str,
        timestamp: &Option<String>,
        position: Option<u32>,
    ) -> Result<Option<BinlogOperation>, Box<dyn std::error::Error>>
    where
        I: Iterator<Item = Result<String, std::io::Error>>
    {
        let (db, table) = self.extract_table_name(table_path);
        let columns = self.get_table_schema(&table);
        
        if columns.is_empty() {
            self.skip_to_next_sql_operation(lines);
            return Ok(None);
        }
        
        // Parse SET clause (for INSERT it's the values)
        let mut values: HashMap<usize, String> = HashMap::new();
        while let Some(Ok(line)) = lines.peek() {
            if !line.starts_with("###") {
                break;
            }
            
            // Stop if we hit another SQL statement
            if line.contains("### UPDATE") || line.contains("### INSERT INTO") || line.contains("### DELETE FROM") {
                break;
            }
            
            let line = lines.next().unwrap().unwrap();
            if let Some(captures) = self.column_value_regex.captures(&line) {
                let col_num: usize = captures[1].parse()?;
                let value = captures[2].to_string();
                values.insert(col_num, value);
            }
        }
        
        // Convert HashMap to Vec (ordered by column index)
        let mut vals = vec!["NULL".to_string(); columns.len()];
        for i in 0..columns.len() {
            let col_idx = i + 1;
            if let Some(val) = values.get(&col_idx) {
                vals[i] = val.clone();
            }
        }
        
        Ok(Some(BinlogOperation {
            timestamp: timestamp.clone(),
            position,
            operation_type: OperationType::Insert,
            table_name: table,
            database: db,
            columns,
            before_values: None,
            after_values: Some(vals),
        }))
    }

    fn parse_delete<I>(
        &mut self,
        lines: &mut std::iter::Peekable<I>,
        table_path: &str,
        timestamp: &Option<String>,
        position: Option<u32>,
    ) -> Result<Option<BinlogOperation>, Box<dyn std::error::Error>>
    where
        I: Iterator<Item = Result<String, std::io::Error>>
    {
        let (db, table) = self.extract_table_name(table_path);
        let columns = self.get_table_schema(&table);
        
        if columns.is_empty() {
            self.skip_to_next_sql_operation(lines);
            return Ok(None);
        }
        
        // Parse WHERE clause
        let mut where_values: HashMap<usize, String> = HashMap::new();
        while let Some(Ok(line)) = lines.peek() {
            if !line.starts_with("###") {
                break;
            }
            
            // Stop if we hit another SQL statement
            if line.contains("### UPDATE") || line.contains("### INSERT INTO") || line.contains("### DELETE FROM") {
                break;
            }
            
            let line = lines.next().unwrap().unwrap();
            if let Some(captures) = self.column_value_regex.captures(&line) {
                let col_num: usize = captures[1].parse()?;
                let value = captures[2].to_string();
                where_values.insert(col_num, value);
            }
        }
        
        // Convert HashMap to Vec (ordered by column index)
        let mut before_vals = vec!["NULL".to_string(); columns.len()];
        for (i, _col) in columns.iter().enumerate() {
            let col_idx = i + 1;
            if let Some(val) = where_values.get(&col_idx) {
                before_vals[i] = val.clone();
            }
        }
        
        Ok(Some(BinlogOperation {
            timestamp: timestamp.clone(),
            position,
            operation_type: OperationType::Delete,
            table_name: table,
            database: db,
            columns,
            before_values: Some(before_vals),
            after_values: None,
        }))
    }

    pub(crate) fn extract_table_name(&self, table_path: &str) -> (String, String) {
        if let Some(captures) = self.table_name_regex.captures(table_path) {
            let db = captures[1].to_string();
            let table = captures[2].to_string();
            (db, table)
        } else {
            ("".to_string(), table_path.to_string())
        }
    }

    /// Get table schema (columns only) - used during parsing to know expected columns
    fn get_table_schema(&mut self, table_name: &str) -> Vec<String> {
        if let Some(cols) = self.schema_cache.get(table_name) {
            return cols.clone();
        }

        let query = format!("PRAGMA table_info('{}')", table_name);
        let Ok(mut stmt) = self.conn.prepare(&query) else {
            return Vec::new()
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

    fn skip_to_next_sql_operation<I>(&self, lines: &mut std::iter::Peekable<I>)
    where
        I: Iterator<Item = Result<String, std::io::Error>>
    {
        while let Some(Ok(line)) = lines.peek() {
            if !line.starts_with("###") {
                break;
            }
            if line.contains("### UPDATE") || line.contains("### INSERT INTO") || line.contains("### DELETE FROM") {
                break;
            }
            lines.next();
        }
    }
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

    fn create_temp_binlog(content: &str) -> std::path::PathBuf {
        use std::io::Write;
        let temp_dir = std::env::temp_dir();
        let file_path = temp_dir.join(format!("test_binlog_{}.sql", std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()));
        let mut file = File::create(&file_path).unwrap();
        file.write_all(content.as_bytes()).unwrap();
        file_path
    }

    #[test]
    fn test_parse_update_to_structured_data() {
        let conn = create_test_db();
        let mut parser = TextBinlogParser::new(conn);
        
        let binlog_content = r#"
#251020 19:43:32 server id 123  end_log_pos 1000
### UPDATE `main`.`users`
### WHERE
###   @1=1
###   @2='Alice'
###   @3='alice@example.com'
###   @4=30
###   @5=1000.50
###   @6=1
###   @7='2024-01-01 10:00:00'
### SET
###   @1=1
###   @2='Alice Smith'
###   @3='alice@example.com'
###   @4=31
###   @5=1000.50
###   @6=1
###   @7='2024-01-01 10:00:00'
"#;
        
        let temp_file = create_temp_binlog(binlog_content);
        let operations = parser.parse_file(temp_file.to_str().unwrap()).unwrap();
        
        assert_eq!(operations.len(), 1, "Should parse exactly one operation");
        let op = &operations[0];
        
        // Check operation metadata
        assert_eq!(op.operation_type, OperationType::Update);
        assert_eq!(op.table_name, "users");
        assert_eq!(op.database, "main");
        assert_eq!(op.timestamp, Some("251020 19:43:32".to_string()));
        assert_eq!(op.position, Some(1000));
        
        // Check structured data
        assert_eq!(op.columns.len(), 7);
        assert!(op.before_values.is_some());
        assert!(op.after_values.is_some());
        
        let before = op.before_values.as_ref().unwrap();
        assert_eq!(before[0], "1");      // id
        assert_eq!(before[1], "'Alice'"); // name
        assert_eq!(before[3], "30");      // age
        
        let after = op.after_values.as_ref().unwrap();
        assert_eq!(after[0], "1");             // id (unchanged)
        assert_eq!(after[1], "'Alice Smith'"); // name (changed)
        assert_eq!(after[3], "31");            // age (changed)
        
        std::fs::remove_file(temp_file).ok();
    }

    #[test]
    fn test_parse_insert_to_structured_data() {
        let conn = create_test_db();
        let mut parser = TextBinlogParser::new(conn);
        
        let binlog_content = r#"
#251020 19:43:32 server id 123  end_log_pos 2000
### INSERT INTO `main`.`users`
### SET
###   @1=4
###   @2='David'
###   @3='david@example.com'
###   @4=28
###   @5=750.25
###   @6=1
###   @7='2024-01-04 13:00:00'
"#;
        
        let temp_file = create_temp_binlog(binlog_content);
        let operations = parser.parse_file(temp_file.to_str().unwrap()).unwrap();
        
        assert_eq!(operations.len(), 1);
        let op = &operations[0];
        
        assert_eq!(op.operation_type, OperationType::Insert);
        assert_eq!(op.table_name, "users");
        assert!(op.before_values.is_none(), "INSERT should have no before-image");
        assert!(op.after_values.is_some());
        
        let after = op.after_values.as_ref().unwrap();
        assert_eq!(after[0], "4");
        assert_eq!(after[1], "'David'");
        assert_eq!(after[3], "28");
        
        std::fs::remove_file(temp_file).ok();
    }

    #[test]
    fn test_parse_delete_to_structured_data() {
        let conn = create_test_db();
        let mut parser = TextBinlogParser::new(conn);
        
        let binlog_content = r#"
#251020 19:43:32 server id 123  end_log_pos 3000
### DELETE FROM `main`.`users`
### WHERE
###   @1=3
###   @2='Charlie'
###   @3='charlie@example.com'
###   @4=35
###   @5=1500.75
###   @6=0
###   @7='2024-01-03 12:00:00'
"#;
        
        let temp_file = create_temp_binlog(binlog_content);
        let operations = parser.parse_file(temp_file.to_str().unwrap()).unwrap();
        
        assert_eq!(operations.len(), 1);
        let op = &operations[0];
        
        assert_eq!(op.operation_type, OperationType::Delete);
        assert_eq!(op.table_name, "users");
        assert!(op.before_values.is_some());
        assert!(op.after_values.is_none(), "DELETE should have no after-image");
        
        let before = op.before_values.as_ref().unwrap();
        assert_eq!(before[0], "3");
        assert_eq!(before[1], "'Charlie'");
        assert_eq!(before[4], "1500.75");
        
        std::fs::remove_file(temp_file).ok();
    }

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
}

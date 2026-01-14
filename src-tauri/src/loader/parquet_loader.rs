use duckdb::{Connection, Result};

#[derive(Debug, Clone)]
pub enum ParquetLoadError {
    ConnectionError(String),
    ExecutionError(String),
}

impl std::fmt::Display for ParquetLoadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParquetLoadError::ConnectionError(msg) => write!(f, "Connection error: {}", msg),
            ParquetLoadError::ExecutionError(msg) => write!(f, "Execution error: {}", msg),
        }
    }
}

impl std::error::Error for ParquetLoadError {}

pub fn load_table_from_parquet_files(table_name: &str, parquet_file_paths: &[&str]) -> Result<Connection, ParquetLoadError> {
    let conn = Connection::open_in_memory()
        .map_err(|e| ParquetLoadError::ConnectionError(e.to_string()))?;
    let files_list = parquet_file_paths
        .iter()
        .map(|path| format!("'{}'", path))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!("CREATE TABLE {table_name} AS SELECT * FROM read_parquet([{files_list}]);");
    println!("{sql}");
    conn.execute(&sql, [])
        .map_err(|e| ParquetLoadError::ExecutionError(e.to_string()))?;

    Ok(conn)

}

/// Creates an in-memory DuckDB connection and executes a series of SQL statements.
/// The first statement should typically be a CREATE TABLE statement with column definitions.
/// Subsequent statements can be INSERT, UPDATE, etc.
pub fn load_table_from_sql(sql_statements: Vec<&str>) -> Result<Connection, ParquetLoadError> {
    let conn = Connection::open_in_memory()
        .map_err(|e| ParquetLoadError::ConnectionError(e.to_string()))?;
    
    for (i, stmt) in sql_statements.iter().enumerate() {
        println!("Executing SQL statement {}: {}", i + 1, stmt);
        conn.execute(stmt, [])
            .map_err(|e| ParquetLoadError::ExecutionError(format!("Failed on statement {}: {} - Error: {}", i + 1, stmt, e)))?;
    }

    Ok(conn)
}

#[cfg(test)]
mod tests {
    use crate::loader::parquet_loader::{load_table_from_parquet_files, load_table_from_sql};

    #[test]
    fn loads_sample_table() {
        // vns:
        // __| title             | developer      | year
        // 1 | Ever17            | KID            | 2002
        // 2 | ファタモルガーナの館 | NOVECT         | 2010
        // 3 | うみねこのなく頃に   | 07th Expansion | 2009
        //
        let conn = load_table_from_parquet_files("test_table", &["./test_data/test_table_1.parquet"]).unwrap();

        let mut statement = conn.prepare("SELECT COUNT(*) FROM test_table WHERE title = 'Ever17' AND developer = 'KID' AND year = '2002';").unwrap();
        let mut rows = statement.query([]).unwrap();

        let row0 = rows.next().unwrap().unwrap();
        assert_eq!(row0.get(0), Ok(1));
    }

    #[test]
    fn test_load_table_from_sql_creates_and_populates_table() {
        let sql_statements = vec![
            "CREATE TABLE products (
                id INTEGER PRIMARY KEY,
                name VARCHAR NOT NULL,
                price DECIMAL(10, 2),
                in_stock BOOLEAN,
                category VARCHAR,
                created_at TIMESTAMP
            )",
            "INSERT INTO products VALUES (1, 'Laptop', 999.99, true, 'Electronics', '2024-01-01 10:00:00')",
            "INSERT INTO products VALUES (2, 'Mouse', 29.99, true, 'Electronics', '2024-01-02 11:00:00')",
            "INSERT INTO products VALUES (3, 'Desk', 299.99, false, 'Furniture', '2024-01-03 12:00:00')",
            "INSERT INTO products VALUES (4, 'Chair', 199.99, true, 'Furniture', '2024-01-04 13:00:00')",
        ];

        let conn = load_table_from_sql(sql_statements).expect("Failed to create and populate table");

        // Verify total row count
        let count: i32 = conn.query_row("SELECT COUNT(*) FROM products", [], |row| row.get(0))
            .expect("Failed to count rows");
        assert_eq!(count, 4, "Should have 4 products");

        // Verify specific product by name
        let laptop_price: f64 = conn.query_row(
            "SELECT price FROM products WHERE name = 'Laptop'", 
            [], 
            |row| row.get(0)
        ).expect("Failed to get laptop price");
        assert_eq!(laptop_price, 999.99, "Laptop price should be 999.99");

        // Count products in stock
        let in_stock_count: i32 = conn.query_row(
            "SELECT COUNT(*) FROM products WHERE in_stock = true", 
            [], 
            |row| row.get(0)
        ).expect("Failed to count in-stock products");
        assert_eq!(in_stock_count, 3, "Should have 3 products in stock");

        // Count by category
        let electronics_count: i32 = conn.query_row(
            "SELECT COUNT(*) FROM products WHERE category = 'Electronics'", 
            [], 
            |row| row.get(0)
        ).expect("Failed to count electronics");
        assert_eq!(electronics_count, 2, "Should have 2 electronics");

        // Verify we can query multiple columns
        let mut stmt = conn.prepare("SELECT name, price, category FROM products WHERE id = 3")
            .expect("Failed to prepare query");
        let mut rows = stmt.query([]).expect("Failed to execute query");
        
        if let Some(row) = rows.next().expect("Failed to get row") {
            let name: String = row.get(0).expect("Failed to get name");
            let price: f64 = row.get(1).expect("Failed to get price");
            let category: String = row.get(2).expect("Failed to get category");
            
            assert_eq!(name, "Desk");
            assert_eq!(price, 299.99);
            assert_eq!(category, "Furniture");
        } else {
            panic!("Expected to find product with id=3");
        }
    }

    #[test]
    fn test_load_table_from_sql_with_updates() {
        // Test that we can also execute UPDATE statements
        let sql_statements = vec![
            "CREATE TABLE inventory (
                item_id INTEGER PRIMARY KEY,
                item_name VARCHAR,
                quantity INTEGER
            )",
            "INSERT INTO inventory VALUES (1, 'Widget', 100)",
            "INSERT INTO inventory VALUES (2, 'Gadget', 50)",
            "UPDATE inventory SET quantity = 75 WHERE item_id = 2",
            "INSERT INTO inventory VALUES (3, 'Doohickey', 25)",
        ];

        let conn = load_table_from_sql(sql_statements).expect("Failed to execute SQL");

        // Verify the update worked
        let gadget_qty: i32 = conn.query_row(
            "SELECT quantity FROM inventory WHERE item_name = 'Gadget'",
            [],
            |row| row.get(0)
        ).expect("Failed to get gadget quantity");
        
        assert_eq!(gadget_qty, 75, "Gadget quantity should be updated to 75");

        // Verify all rows are present
        let total: i32 = conn.query_row("SELECT COUNT(*) FROM inventory", [], |row| row.get(0))
            .expect("Failed to count");
        assert_eq!(total, 3, "Should have 3 items");
    }
}
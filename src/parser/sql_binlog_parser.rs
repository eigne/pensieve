use mysql_binlog_connector_rust::binlog_parser::BinlogParser;
use mysql_binlog_connector_rust::event::event_data::EventData;
use std::collections::HashMap;
use std::fs::File;
use duckdb::Connection;

/*
This is an attempt at to parse binlogs directly from binary format, but I don't think it works.
I'll delete and reimplement this at some point, since parsing the text binlog format is risky.
 */

pub fn parse_binlog_file() {
    let file_path = "./test_data/mysql-bin-changelog.000098";
    let mut file = File::open(file_path).unwrap();

    let mut parser = BinlogParser {
        checksum_length: 4,
        table_map_event_by_table_id: HashMap::new(),
    };

    assert!(parser.check_magic(&mut file).is_ok());
    while let Ok((header, data)) = parser.next(&mut file) {
        println!("header: {:?}", header);
        println!("data: {:?}", data);
        println!("");
    }
}

/// Get column names for a table from DuckDB
fn get_column_names(conn: &Connection, table_name: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut stmt = conn.prepare(&format!("DESCRIBE {}", table_name))?;
    let columns: Vec<String> = stmt
        .query_map([], |row| row.get(0))?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(columns)
}

/// Build a mapping of all tables to their column names
fn build_table_column_mapping(conn: &Connection) -> Result<HashMap<String, Vec<String>>, Box<dyn std::error::Error>> {
    let mut table_columns = HashMap::new();
    
    let mut stmt = conn.prepare("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'")?;
    let table_names: Vec<String> = stmt
        .query_map([], |row| row.get(0))?
        .collect::<Result<Vec<_>, _>>()?;
    
    for table_name in table_names {
        let columns = get_column_names(conn, &table_name)?;
        table_columns.insert(table_name, columns);
    }
    
    Ok(table_columns)
}

/// Convert a value to SQL string representation using Debug formatting
fn value_to_sql<T: std::fmt::Debug>(value: &T) -> String {
    let debug_str = format!("{:?}", value);
    
    if debug_str == "None" || debug_str == "Null" {
        return "NULL".to_string();
    }
    
    let debug_str = if debug_str.starts_with("Some(") && debug_str.ends_with(")") {
        &debug_str[5..debug_str.len()-1]
    } else {
        &debug_str
    };
    
    // If it starts with a quote, it's already a string
    if debug_str.starts_with('"') && debug_str.ends_with('"') {
        // Remove debug quotes and add SQL quotes, escape single quotes
        let unquoted = &debug_str[1..debug_str.len()-1];
        return format!("'{}'", unquoted.replace("'", "''"));
    }
    
    // If it's a plain value (number, etc), use as-is
    // But add quotes for safety in SQL
    format!("'{}'", debug_str.replace("'", "''"))
}

/// Build UPDATE SQL statement from before/after values
fn build_update_sql<T: std::fmt::Debug + PartialEq>(
    table_name: &str,
    column_names: &[String],
    before_values: &[T],
    after_values: &[T],
) -> String {
    // Build SET clause (only include changed columns)
    let set_parts: Vec<String> = column_names
        .iter()
        .zip(after_values.iter())
        .enumerate()
        .filter(|(i, _)| before_values[*i] != after_values[*i])
        .map(|(_, (col_name, value))| {
            format!("{} = {}", col_name, value_to_sql(value))
        })
        .collect();
    
    // Build WHERE clause (use all columns from before to identify the row)
    let where_parts: Vec<String> = column_names
        .iter()
        .zip(before_values.iter())
        .map(|(col_name, value)| {
            let debug_str = format!("{:?}", value);
            if debug_str == "None" || debug_str == "Null" {
                format!("{} IS NULL", col_name)
            } else {
                format!("{} = {}", col_name, value_to_sql(value))
            }
        })
        .collect();
    
    format!(
        "UPDATE {} SET {} WHERE {}",
        table_name,
        set_parts.join(", "),
        where_parts.join(" AND ")
    )
}

pub fn process_binlog_updates(
    conn: &Connection,
    binlog_path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut file = File::open(binlog_path)?;

    let mut parser = BinlogParser {
        checksum_length: 4,
        table_map_event_by_table_id: HashMap::new(),
    };

    parser.check_magic(&mut file)?;
    
    // Build mapping of table names to column names from DuckDB
    println!("Building table column mapping from DuckDB...");
    let table_columns = build_table_column_mapping(conn)?;
    println!("Found {} tables in DuckDB", table_columns.len());
    
    let mut update_count = 0;
    
    while let Ok((_header, data)) = parser.next(&mut file) {
        match data {
            EventData::TableMap(table_map_event) => {
                println!("TableMap event received for table: {}", table_map_event.table_name);
            }

            EventData::UpdateRows(update_event) => {
                if let Some(table_map) = parser.table_map_event_by_table_id.get(&update_event.table_id) {
                    let table_name = table_map.table_name.clone();
                    
                    // Get column names for this table
                    if let Some(column_names) = table_columns.get(&table_name) {
                        for row_pair in &update_event.rows {
                            let before_row = &row_pair.0;
                            let after_row = &row_pair.1;

                            // RowEvent has a `column_values` field that contains the actual values
                            let update_sql = build_update_sql(
                                &table_name,
                                column_names,
                                &before_row.column_values,
                                &after_row.column_values,
                            );

                            println!("Executing: {}", update_sql);
                            conn.execute(&update_sql, [])?;
                            update_count += 1;
                        }
                    } else {
                        eprintln!("Warning: Table '{}' not found in DuckDB snapshot", table_name);
                    }
                }
            }
            
            _ => {
                // Ignore other events for now
            }
        }
    }
    
    println!("Applied {} UPDATE statements", update_count);
    Ok(())
}
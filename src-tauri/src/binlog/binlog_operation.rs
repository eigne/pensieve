use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq)]
pub enum OperationType {
    Insert,
    Update,
    Delete,
}

impl Display for OperationType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            OperationType::Insert => write!(f, "INSERT"),
            OperationType::Update => write!(f, "UPDATE"),
            OperationType::Delete => write!(f, "DELETE"),
        }
    }
}
#[derive(Debug, Clone)]
pub struct BinlogOperation {
    pub timestamp: Option<String>,
    pub position: Option<u32>,
    pub operation_type: OperationType,
    pub table_name: String,
    pub database: String,
    pub columns: Vec<String>,
    pub before_values: Option<Vec<String>>,  // WHERE clause values
    pub after_values: Option<Vec<String>>,   // SET clause values
}

impl BinlogOperation {
    pub fn invert(&self) -> Self {
        match self.operation_type {
            OperationType::Insert => {
                // INSERT → DELETE
                BinlogOperation {
                    operation_type: OperationType::Delete,
                    before_values: self.after_values.clone(),
                    after_values: None,
                    timestamp: self.timestamp.clone(),
                    position: self.position,
                    table_name: self.table_name.clone(),
                    database: self.database.clone(),
                    columns: self.columns.clone(),
                }
            }
            OperationType::Update => {
                // UPDATE → UPDATE with swapped images
                BinlogOperation {
                    before_values: self.after_values.clone(),
                    after_values: self.before_values.clone(),
                    timestamp: self.timestamp.clone(),
                    position: self.position,
                    operation_type: OperationType::Update,
                    table_name: self.table_name.clone(),
                    database: self.database.clone(),
                    columns: self.columns.clone(),
                }
            }
            OperationType::Delete => {
                // DELETE → INSERT
                BinlogOperation {
                    operation_type: OperationType::Insert,
                    before_values: None,
                    after_values: self.before_values.clone(),
                    timestamp: self.timestamp.clone(),
                    position: self.position,
                    table_name: self.table_name.clone(),
                    database: self.database.clone(),
                    columns: self.columns.clone(),
                }
            }
        }
    }
}

impl Display for BinlogOperation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let timestamp = self.timestamp.clone().unwrap_or("null".to_string());
        let position = self.position.clone().unwrap_or(0);
        write!(f, "{} {} {} {} {}", timestamp, position, self.operation_type, self.database, self.table_name)
    }
}
use duckdb::Connection;
use crate::binlog::BinlogOperation;
use crate::operation_applier::OperationApplier;

/// Manages a database snapshot and enables time navigation through binlog operations
pub struct SnapshotManager {
    applier: OperationApplier,
    operations: Vec<BinlogOperation>,
    current_position: usize,
}

impl SnapshotManager {
    pub fn new(conn: Connection, operations: Vec<BinlogOperation>, initial_position: usize) -> Self {
        Self {
            applier: OperationApplier::new(conn),
            operations,
            current_position: initial_position,
        }
    }

    pub fn get_position(&self) -> usize {
        self.current_position
    }

    pub fn get_timestamp(&self) -> Option<&String> {
        self.operations.get(self.current_position)
            .and_then(|op| op.timestamp.as_ref())
    }

    pub fn get_connection(&self) -> &Connection {
        self.applier.get_connection()
    }

    pub fn operation_count(&self) -> usize {
        self.operations.len()
    }

    pub fn step_forward(&mut self) -> Result<bool, Box<dyn std::error::Error>> {
        if self.current_position + 1 >= self.operations.len() {
            return Ok(false);
        }

        let next_op = &self.operations[self.current_position + 1];
        self.applier.apply_operation_conditionally(next_op)?;

        self.current_position += 1;
        Ok(true)
    }

    pub fn step_backward(&mut self) -> Result<bool, Box<dyn std::error::Error>> {
        if self.current_position == 0 {
            return Ok(false); // Already at the beginning
        }

        let current_op = &self.operations[self.current_position];
        let inverted = current_op.invert();
        self.applier.apply_operation_conditionally(&inverted)?;

        self.current_position -= 1;
        Ok(true)
    }

    pub fn step_forward_by(&mut self, count: usize) -> Result<usize, Box<dyn std::error::Error>> {
        let mut steps_taken = 0;
        for _ in 0..count {
            if self.step_forward()? {
                steps_taken += 1;
            } else {
                break;
            }
        }
        Ok(steps_taken)
    }

    pub fn step_backward_by(&mut self, count: usize) -> Result<usize, Box<dyn std::error::Error>> {
        let mut steps_taken = 0;
        for _ in 0..count {
            if self.step_backward()? {
                steps_taken += 1;
            } else {
                break;
            }
        }
        Ok(steps_taken)
    }

    pub fn goto_position(&mut self, target_position: usize) -> Result<(), Box<dyn std::error::Error>> {
        if target_position >= self.operations.len() {
            return Err("Target position out of bounds".into());
        }

        if target_position > self.current_position {
            let steps = target_position - self.current_position;
            self.step_forward_by(steps)?;
        } else if target_position < self.current_position {
            let steps = self.current_position - target_position;
            self.step_backward_by(steps)?;
        }

        Ok(())
    }

    /// Go to a specific timestamp (finds closest operation)
    pub fn goto_timestamp(&mut self, target_timestamp: &str) -> Result<(), Box<dyn std::error::Error>> {
        let mut closest_idx = 0;
        let mut closest_diff = i64::MAX;

        for (idx, op) in self.operations.iter().enumerate() {
            if let Some(ts) = &op.timestamp {
                if ts == target_timestamp {
                    closest_idx = idx;
                    break;
                }
                // Simple string comparison for now
                let diff = (ts.as_str().cmp(target_timestamp) as i64).abs();
                if diff < closest_diff {
                    closest_diff = diff;
                    closest_idx = idx;
                }
            }
        }

        self.goto_position(closest_idx)
    }

    pub fn get_operation(&self, index: usize) -> Option<&BinlogOperation> {
        self.operations.get(index)
    }

    pub fn get_operations_range(&self, start: usize, end: usize) -> &[BinlogOperation] {
        let end = end.min(self.operations.len());
        &self.operations[start..end]
    }
}


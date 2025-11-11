use duckdb::Connection;
use crate::binlog::{BinlogOperation, BinlogTimestamp};
use crate::operation_applier::OperationApplier;

/// Normalizes a database snapshot to a specific timestamp using binlog operations
pub struct TimestampNormaliser {
}
/// Normalises a snapshot to a known position based on timestamp.
///
/// Normalising is done by applying past transactions (skipping those that have no effect),
/// and reversing future transactions.
/// Normalising can be done using the entire binlog, but this is slow.
///
/// We may not know the exact position of the snapshot, but it is generally possible to estimate
/// its timestamp to sometime within a 1-hour period, depending on how long it took to generate
/// the snapshot. We will refer to this period as a 'window'.
/// (For example, AWS takes around 20 minutes to generate a snapshot for my use case).
///
/// It is guaranteed that all transactions before the window will have no overall effect when
/// applied to the snapshot, because they are chronologically behind the snapshot.
///
/// It is guaranteed that all transactions after the window will have no overall effect when
/// inverted and applied to the snapshot, because they are chronologically after the snapshot.
///
/// By these assumptions, we can normalise the snapshot using only the transactions within the window.
impl TimestampNormaliser {
    /// Normalises a snapshot to a known position based on timestamp.
    ///
    /// Check the TimestampNormaliser documentation for an explanation of the normalisation algorithm.
    ///
    /// This function arbitrarily chooses the midpoint of the transactions in the window and normalises
    /// the snapshot to this point. This lets us identify exactly which transactions come immediately
    /// before and after the snapshot.
    ///
    /// # Arguments
    /// * `conn` - DuckDB connection with loaded snapshot
    /// * `operations` - All parsed binlog operations
    /// * `snapshot_timestamp` - Approximate timestamp of snapshot (format: "YYMMDD HH:MM:SS")
    /// * `window_hours` - Size of window to search around snapshot (e.g., 1 hour)
    /// 
    /// # Returns
    /// A tuple of (Connection, operations, normalized position index)
    pub fn normalize(
        conn: Connection,
        operations: Vec<BinlogOperation>,
        snapshot_timestamp: &str,
        window_hours: i64,
    ) -> Result<(Connection, Vec<BinlogOperation>, usize), Box<dyn std::error::Error>> {
        let mut applier = OperationApplier::new(conn);
        
        println!("Normalizing to timestamp: {}", snapshot_timestamp);
        
        let snapshot_ts = BinlogTimestamp::parse(snapshot_timestamp)
            .map_err(|e| format!("Failed to parse snapshot timestamp: {}", e))?;
        
        // Calculate window bounds
        let ts_lower = snapshot_ts.subtract_hours(window_hours);
        let ts_upper = snapshot_ts.add_hours(window_hours);
        
        println!("Window range: {} to {}", ts_lower, ts_upper);
        
        // Find operations within window
        let window_ops: Vec<usize> = operations.iter()
            .enumerate()
            .filter(|(_, op)| {
                if let Some(ts_str) = &op.timestamp {
                    if let Ok(op_ts) = BinlogTimestamp::parse(ts_str) {
                        return op_ts >= ts_lower && op_ts <= ts_upper;
                    }
                }
                false
            })
            .map(|(idx, _)| idx)
            .collect();

        if window_ops.is_empty() {
            println!("No operations found in window. Skipping normalization");
            let tx_zero_idx = if operations.is_empty() { 0 } else { operations.len() - 1 };
            return Ok((applier.into_connection(), operations, tx_zero_idx));
        }
        println!("Found {} operations in {}-hour window around snapshot", window_ops.len(), window_hours * 2);

        let tx_zero_idx = window_ops[window_ops.len() / 2];
        println!("Selected transaction zero at index {} (timestamp: {:?})", 
                 tx_zero_idx, operations[tx_zero_idx].timestamp);
        
        // Apply operations BEFORE and INCLUDING tx_zero (forward)
        println!("\n=== Phase 1: Applying operations forward (up to tx_zero) ===");
        let mut applied_forward = 0;
        let mut skipped_forward = 0;
        
        for idx in window_ops.iter().filter(|&&i| i <= tx_zero_idx) {
            if applier.apply_operation_conditionally(&operations[*idx])? {
                applied_forward += 1;
            } else {
                skipped_forward += 1;
            }
        }
        
        println!("Applied {} operations, skipped {}", applied_forward, skipped_forward);
        
        // Apply operations AFTER tx_zero (inverted)
        println!("\n=== Phase 2: Applying operations inverted (after tx_zero) ===");
        let mut applied_inverted = 0;
        let mut skipped_inverted = 0;

        let mut after_indices: Vec<usize> = window_ops.iter()
            .filter(|&&i| i > tx_zero_idx)
            .copied()
            .collect();
        after_indices.reverse();

        for idx in after_indices {
            let inverted = operations[idx].invert();
            if applier.apply_operation_conditionally(&inverted)? {
                applied_inverted += 1;
            } else {
                skipped_inverted += 1;
            }
        }

        println!("Applied {} inverted operations, skipped {}", applied_inverted, skipped_inverted);
        println!("\n=== Snapshot normalized to position {} ===", tx_zero_idx);
        
        let conn = applier.into_connection();
        Ok((conn, operations, tx_zero_idx))
    }
}
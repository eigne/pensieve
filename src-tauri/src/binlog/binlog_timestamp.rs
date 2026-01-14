use chrono::{NaiveDateTime, Duration, Datelike, Timelike};
use std::fmt;

/// Represents a MySQL binlog timestamp in the format "YYMMDD HH:MM:SS"
/// 
/// This wrapper provides convenient methods for parsing, manipulating,
/// and formatting timestamps used in MySQL binlog files.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct BinlogTimestamp {
    datetime: NaiveDateTime,
}

impl BinlogTimestamp {
    /// Parse a timestamp string in the format "YYMMDD HH:MM:SS"
    /// 
    /// # Examples
    /// ```
    /// let ts = BinlogTimestamp::parse("251108 17:03:00").unwrap();
    /// ```
    pub fn parse(timestamp: &str) -> Result<Self, String> {
        let parts: Vec<&str> = timestamp.split(' ').collect();
        if parts.len() != 2 {
            return Err(format!("Invalid timestamp format: expected 'YYMMDD HH:MM:SS', got '{}'", timestamp));
        }
        
        let date_part = parts[0];
        let time_part = parts[1];
        
        // Parse date: YYMMDD
        if date_part.len() != 6 {
            return Err(format!("Invalid date format: expected 6 digits (YYMMDD), got '{}'", date_part));
        }
        
        let year = format!("20{}", &date_part[0..2])
            .parse::<i32>()
            .map_err(|e| format!("Invalid year: {}", e))?;
        let month = date_part[2..4]
            .parse::<u32>()
            .map_err(|e| format!("Invalid month: {}", e))?;
        let day = date_part[4..6]
            .parse::<u32>()
            .map_err(|e| format!("Invalid day: {}", e))?;
        
        // Parse time: HH:MM:SS
        let time_components: Vec<&str> = time_part.split(':').collect();
        if time_components.len() != 3 {
            return Err(format!("Invalid time format: expected 'HH:MM:SS', got '{}'", time_part));
        }
        
        let hour = time_components[0]
            .parse::<u32>()
            .map_err(|e| format!("Invalid hour: {}", e))?;
        let minute = time_components[1]
            .parse::<u32>()
            .map_err(|e| format!("Invalid minute: {}", e))?;
        let second = time_components[2]
            .parse::<u32>()
            .map_err(|e| format!("Invalid second: {}", e))?;
        
        // Create NaiveDateTime
        let datetime = NaiveDateTime::new(
            chrono::NaiveDate::from_ymd_opt(year, month, day)
                .ok_or_else(|| format!("Invalid date: {}-{:02}-{:02}", year, month, day))?,
            chrono::NaiveTime::from_hms_opt(hour, minute, second)
                .ok_or_else(|| format!("Invalid time: {:02}:{:02}:{:02}", hour, minute, second))?,
        );
        
        Ok(Self { datetime })
    }
    
    /// Add hours to the timestamp
    pub fn add_hours(&self, hours: i64) -> Self {
        Self {
            datetime: self.datetime + Duration::hours(hours),
        }
    }
    
    /// Subtract hours from the timestamp
    pub fn subtract_hours(&self, hours: i64) -> Self {
        Self {
            datetime: self.datetime - Duration::hours(hours),
        }
    }
    
    /// Convert back to the binlog string format "YYMMDD HH:MM:SS"
    pub fn to_binlog_format(&self) -> String {
        format!(
            "{:02}{:02}{:02} {:02}:{:02}:{:02}",
            self.datetime.year() % 100,
            self.datetime.month(),
            self.datetime.day(),
            self.datetime.hour(),
            self.datetime.minute(),
            self.datetime.second()
        )
    }
    
    /// Get the underlying NaiveDateTime
    pub fn as_datetime(&self) -> &NaiveDateTime {
        &self.datetime
    }
}

impl fmt::Display for BinlogTimestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_binlog_format())
    }
}

impl From<NaiveDateTime> for BinlogTimestamp {
    fn from(datetime: NaiveDateTime) -> Self {
        Self { datetime }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_parse_valid_timestamp() {
        let ts = BinlogTimestamp::parse("251108 17:03:00").unwrap();
        assert_eq!(ts.to_binlog_format(), "251108 17:03:00");
    }
    
    #[test]
    fn test_parse_invalid_format() {
        assert!(BinlogTimestamp::parse("invalid").is_err());
        assert!(BinlogTimestamp::parse("25110817:03:00").is_err());
        assert!(BinlogTimestamp::parse("251108 17:03").is_err());
    }
    
    #[test]
    fn test_add_hours_within_day() {
        let ts = BinlogTimestamp::parse("251108 10:00:00").unwrap();
        let new_ts = ts.add_hours(5);
        assert_eq!(new_ts.to_binlog_format(), "251108 15:00:00");
    }
    
    #[test]
    fn test_add_hours_across_days() {
        let ts = BinlogTimestamp::parse("251108 20:00:00").unwrap();
        let new_ts = ts.add_hours(6);
        assert_eq!(new_ts.to_binlog_format(), "251109 02:00:00");
    }
    
    #[test]
    fn test_add_hours_across_months() {
        let ts = BinlogTimestamp::parse("251130 20:00:00").unwrap();
        let new_ts = ts.add_hours(6);
        assert_eq!(new_ts.to_binlog_format(), "251201 02:00:00");
    }
    
    #[test]
    fn test_subtract_hours_within_day() {
        let ts = BinlogTimestamp::parse("251108 15:00:00").unwrap();
        let new_ts = ts.subtract_hours(5);
        assert_eq!(new_ts.to_binlog_format(), "251108 10:00:00");
    }
    
    #[test]
    fn test_subtract_hours_across_days() {
        let ts = BinlogTimestamp::parse("251108 02:00:00").unwrap();
        let new_ts = ts.subtract_hours(6);
        assert_eq!(new_ts.to_binlog_format(), "251107 20:00:00");
    }
    
    #[test]
    fn test_large_hour_addition() {
        let ts = BinlogTimestamp::parse("251108 10:00:00").unwrap();
        let new_ts = ts.add_hours(100); // ~4 days
        assert_eq!(new_ts.to_binlog_format(), "251112 14:00:00");
    }
    
    #[test]
    fn test_comparison() {
        let ts1 = BinlogTimestamp::parse("251108 10:00:00").unwrap();
        let ts2 = BinlogTimestamp::parse("251108 15:00:00").unwrap();
        let ts3 = BinlogTimestamp::parse("251109 10:00:00").unwrap();
        
        assert!(ts1 < ts2);
        assert!(ts2 < ts3);
        assert!(ts1 < ts3);
    }
    
    #[test]
    fn test_display() {
        let ts = BinlogTimestamp::parse("251108 17:03:00").unwrap();
        assert_eq!(format!("{}", ts), "251108 17:03:00");
    }
}


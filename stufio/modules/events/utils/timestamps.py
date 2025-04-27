"""
Utilities for working with timestamps in events.
"""
from typing import Optional, Union
from datetime import datetime


def parse_event_timestamp(timestamp_value: Union[datetime, str, None]) -> Optional[float]:
    """
    Parse a timestamp from an event message into a Unix timestamp (seconds since epoch).
    
    Handles various formats:
    - datetime objects
    - ISO-8601 formatted strings (with or without timezone)
    - Custom datetime strings
    
    Args:
        timestamp_value: The timestamp value to parse, can be datetime object or string
        
    Returns:
        Unix timestamp as float, or None if parsing fails
    """
    if timestamp_value is None:
        return None
        
    try:
        # Handle datetime objects directly
        if isinstance(timestamp_value, datetime):
            return timestamp_value.timestamp()
            
        # Handle string timestamps
        elif isinstance(timestamp_value, str):
            # Try common formats
            formats = [
                '%Y-%m-%d %H:%M:%S.%f',     # Standard format with microseconds
                '%Y-%m-%dT%H:%M:%S.%fZ',    # ISO format with Z
                '%Y-%m-%dT%H:%M:%S.%f',     # ISO format without Z
                '%Y-%m-%d %H:%M:%S',        # Standard format without microseconds
                '%Y-%m-%dT%H:%M:%SZ',       # ISO format without microseconds, with Z
                '%Y-%m-%dT%H:%M:%S',        # ISO format without microseconds or Z
            ]
            
            # Try each format until one works
            for fmt in formats:
                try:
                    dt = datetime.strptime(timestamp_value, fmt)
                    return dt.timestamp()
                except ValueError:
                    continue
                    
            # If we get here, none of the formats matched
            return None
    except (ValueError, TypeError):
        return None
        
    return None
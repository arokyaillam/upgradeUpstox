"""
Time Utilities
Helper functions for timezone conversion and timestamp management.
Ensures strict alignment to Indian Standard Time (IST).
"""

from datetime import datetime
import pytz

# Constants
IST_TIMEZONE = pytz.timezone('Asia/Kolkata')


def get_ist_time(epoch_ms: int = None) -> datetime:
    """
    Get current time or convert epoch milliseconds to IST datetime.
    
    Args:
        epoch_ms: Optional timestamp in milliseconds (e.g., from Upstox LTT)
        
    Returns:
        datetime: Timezone-aware datetime object in IST
    """
    if epoch_ms is not None:
        # Convert ms to seconds
        dt_utc = datetime.fromtimestamp(epoch_ms / 1000, pytz.utc)
        return dt_utc.astimezone(IST_TIMEZONE)
    else:
        # Get current time in IST
        return datetime.now(IST_TIMEZONE)


def get_seconds_to_next_minute() -> float:
    """
    Calculate seconds remaining until the next clock minute (XX:XX:00).
    Used for aligning archiver/scheduler to exact minute boundaries.
    
    Returns:
        float: Seconds to sleep
    """
    now = datetime.now()
    seconds = 60 - now.second - (now.microsecond / 1_000_000)
    return max(0.1, seconds)  # Ensure non-negative

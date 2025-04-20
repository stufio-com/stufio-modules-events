from pydantic import BaseModel, Field
from typing import Dict, Optional, Any, List, Union
from datetime import datetime
import uuid


class ErrorBase(BaseModel):
    """Base model for error information."""
    correlation_id: uuid.UUID
    error_type: str
    severity: str
    source: str
    error_message: str
    error_stack: Optional[str] = None
    request_path: str
    request_method: str
    status_code: int
    request_data: Optional[str] = None
    actor_id: Optional[str] = None


class ErrorLogCreate(ErrorBase):
    """Schema for creating an error log entry."""
    timestamp: datetime = Field(default_factory=datetime.utcnow)


class ErrorLogUpdate(BaseModel):
    """Schema for updating an error log entry."""
    error_type: Optional[str] = None
    severity: Optional[str] = None
    error_message: Optional[str] = None
    error_stack: Optional[str] = None
    resolution_status: Optional[str] = None
    resolution_notes: Optional[str] = None
    resolved_by: Optional[str] = None
    resolved_at: Optional[datetime] = None


class ErrorLog(ErrorBase):
    """Schema for a complete error log entry."""
    id: Optional[str] = None
    timestamp: datetime
    resolution_status: Optional[str] = None
    resolution_notes: Optional[str] = None
    resolved_by: Optional[str] = None
    resolved_at: Optional[datetime] = None

    class Config:
        from_attributes = True


class ErrorSummary(BaseModel):
    """Summary statistics about errors."""
    total_errors: int
    errors_by_type: Dict[str, int]
    errors_by_path: Dict[str, int]
    errors_by_severity: Dict[str, int]
    recent_errors: List[ErrorLog]


class HTTPErrorDetails(BaseModel):
    """Details about an HTTP error."""
    status_code: int
    type: str
    message: Optional[str] = None
    details: Optional[Dict[str, Any]] = None


class APIErrorResponse(BaseModel):
    """Standardized API error response."""
    error: HTTPErrorDetails
    correlation_id: str
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat())

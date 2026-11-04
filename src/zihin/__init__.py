"""Official Python client for the Zihin.ai agent platform."""

from ._client import Zihin
from ._errors import ZihinError, is_commercial_block
from ._http import RetryInfo
from ._invoke import (
    AgentStream,
    classify_stream_error_kind,
    error_from_done_event,
    error_from_outcome,
    error_from_stream_event,
)
from ._types import (
    Agent,
    AgentOutcome,
    AgentResult,
    DispatchResult,
    ErrorKind,
    ExecutionStatus,
    FindOrCreateResult,
    IgnoredField,
    StreamEvent,
    TriggerInfo,
    TriggerRef,
    Usage,
)

__version__ = "0.2.0"

__all__ = [
    "AgentStream",
    "Agent",
    "AgentOutcome",
    "AgentResult",
    "DispatchResult",
    "ErrorKind",
    "ExecutionStatus",
    "FindOrCreateResult",
    "IgnoredField",
    "RetryInfo",
    "StreamEvent",
    "TriggerInfo",
    "TriggerRef",
    "Usage",
    "Zihin",
    "ZihinError",
    "classify_stream_error_kind",
    "error_from_done_event",
    "error_from_outcome",
    "error_from_stream_event",
    "is_commercial_block",
    "__version__",
]

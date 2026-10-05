"""Official Python client for the Zihin.ai agent platform."""

from ._client import Agent, AgentResult, StreamEvent, Usage, Zihin
from ._errors import ZihinError

__version__ = "0.1.0"

__all__ = ["Agent", "AgentResult", "StreamEvent", "Usage", "Zihin", "ZihinError", "__version__"]

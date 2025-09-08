from typing import Mapping


class FumaroleClientError(Exception):
    """Base class for all Fumarole-related exceptions."""
    pass


class SubscribeError(FumaroleClientError):
    """Base class for all errors related to subscription."""
    pass


class DownloadSlotError(SubscribeError):
    """Exception raised for errors in the download slot process."""
    
    def __init__(self, message: str, ctx: Mapping[str, any]):
        super().__init__(message)
        self.ctx = ctx
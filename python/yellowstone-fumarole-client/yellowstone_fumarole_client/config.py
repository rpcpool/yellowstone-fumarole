from dataclasses import dataclass
from typing import Dict, Literal, Optional
import yaml


SUPPORTED_COMPRESSION = ["gzip"]
SupportedCompression = Literal["gzip"]


@dataclass
class FumaroleConfig:
    endpoint: str
    x_token: Optional[str] = None
    max_decoding_message_size_bytes: int = 512_000_000
    x_metadata: Dict[str, str] = None
    response_compression: Optional[SupportedCompression] = None

    def __post_init__(self):
        self.x_metadata = self.x_metadata or {}

    @classmethod
    def from_yaml(cls, fileobj) -> "FumaroleConfig":
        data = yaml.safe_load(fileobj)
        response_compression = data.get(
            "response_compression", cls.response_compression
        )
        if (
            response_compression is not None
            and response_compression not in SUPPORTED_COMPRESSION
        ):
            raise ValueError(f"response_compression must be in {SUPPORTED_COMPRESSION}")
        return cls(
            endpoint=data["endpoint"],
            x_token=data.get("x-token") or data.get("x_token"),
            max_decoding_message_size_bytes=data.get(
                "max_decoding_message_size_bytes", cls.max_decoding_message_size_bytes
            ),
            x_metadata=data.get("x-metadata", {}),
            response_compression=data.get(
                "response_compression", cls.response_compression
            ),
        )

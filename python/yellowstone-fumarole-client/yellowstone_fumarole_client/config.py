from dataclasses import dataclass
from typing import Dict, Optional
import yaml


@dataclass
class FumaroleConfig:
    endpoint: str
    x_token: Optional[str] = None
    max_decoding_message_size_bytes: int = 512_000_000
    x_metadata: Dict[str, str] = None

    def __post_init__(self):
        self.x_metadata = self.x_metadata or {}

    @classmethod
    def from_yaml(cls, fileobj) -> "FumaroleConfig":
        data = yaml.safe_load(fileobj)
        return cls(
            endpoint=data["endpoint"],
            x_token=data.get("x-token") or data.get("x_token"),
            max_decoding_message_size_bytes=data.get(
                "max_decoding_message_size_bytes", cls.max_decoding_message_size_bytes
            ),
            x_metadata=data.get("x-metadata", {}),
        )

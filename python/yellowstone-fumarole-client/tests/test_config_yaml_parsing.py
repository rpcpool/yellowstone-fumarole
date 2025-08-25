import io
from yellowstone_fumarole_client.config import FumaroleConfig


def test_valid_yaml_file():
    yaml_str = io.StringIO(
        """
        endpoint: https://localhost:50051
        x-token: test-token
        max_decoding_message_size_bytes: 512000000
        x-metadata:
            key: value
        response_compression: gzip
        """
    )
    yaml_str.seek(0)

    config = FumaroleConfig.from_yaml(yaml_str)
    
    assert config.endpoint == "https://localhost:50051"
    assert config.x_token == "test-token"
    assert config.max_decoding_message_size_bytes == 512_000_000
    assert config.x_metadata == {"key": "value"}
    assert config.response_compression == "gzip"


def test_default_values():
    yaml_str = io.StringIO(
        """
        endpoint: https://localhost:50051
        """
    )
    yaml_str.seek(0)

    config = FumaroleConfig.from_yaml(yaml_str)

    assert config.endpoint == "https://localhost:50051"
    assert config.x_token is None
    assert config.max_decoding_message_size_bytes == FumaroleConfig.max_decoding_message_size_bytes
    assert config.x_metadata == {}
    assert config.response_compression is None
import logging
from typing import Optional
import grpc
from yellowstone_fumarole_client.config import FumaroleConfig
from yellowstone_fumarole_proto.fumarole_pb2_grpc import FumaroleStub

X_TOKEN_HEADER = "x-token"


def _triton_sign_request(
    callback: grpc.AuthMetadataPluginCallback,
    x_token: Optional[str],
    error: Optional[Exception],
):
    # WARNING: metadata is a 1d-tuple (<value>,), the last comma is necessary
    metadata = ((X_TOKEN_HEADER, x_token),)
    return callback(metadata, error)


class TritonAuthMetadataPlugin(grpc.AuthMetadataPlugin):
    """Metadata wrapper for raw access token credentials."""

    def __init__(self, x_token: str):
        self.x_token = x_token

    def __call__(
        self,
        context: grpc.AuthMetadataContext,
        callback: grpc.AuthMetadataPluginCallback,
    ):
        return _triton_sign_request(callback, self.x_token, None)


# Because of a bug in grpcio library, multiple inheritance of ClientInterceptor subclasses does not work.
# You have to create a new class for each type of interceptor you want to use.
class MetadataInterceptor(
    grpc.aio.UnaryStreamClientInterceptor,
    grpc.aio.StreamUnaryClientInterceptor,
    grpc.aio.StreamStreamClientInterceptor,
    grpc.aio.UnaryUnaryClientInterceptor,
):

    def __init__(self, metadata):
        if isinstance(metadata, dict):
            metadata = metadata.items()
        self.metadata = list(metadata)

    async def intercept_unary_unary(
        self, continuation, client_call_details: grpc.aio.ClientCallDetails, request
    ):
        # logging.debug("intercept_unary_unary")
        new_details = client_call_details._replace(
            metadata=self._merge_metadata(client_call_details.metadata)
        )
        return await continuation(new_details, request)

    async def intercept_unary_stream(
        self, continuation, client_call_details: grpc.aio.ClientCallDetails, request
    ):
        # logging.debug("intercept_unary_stream")
        new_details = client_call_details._replace(
            metadata=self._merge_metadata(client_call_details.metadata)
        )
        return await continuation(new_details, request)

    async def intercept_stream_unary(
        self, continuation, client_call_details: grpc.aio.ClientCallDetails, request
    ):
        # logging.debug("intercept_stream_unary")
        new_details = client_call_details._replace(
            metadata=self._merge_metadata(client_call_details.metadata)
        )
        return await continuation(new_details, request)

    async def intercept_stream_stream(
        self, continuation, client_call_details: grpc.aio.ClientCallDetails, request
    ):
        # logging.debug("intercept_stream_stream")
        new_details = client_call_details._replace(
            metadata=self._merge_metadata(client_call_details.metadata)
        )
        return await continuation(new_details, request)

    def unary_stream_interceptor(self) -> grpc.aio.UnaryStreamClientInterceptor:
        this = self

        class Interceptor(grpc.aio.UnaryStreamClientInterceptor):
            async def intercept_unary_stream(self, *args):
                return await this.intercept_unary_stream(*args)

        return Interceptor()

    def stream_unary_interceptor(self) -> grpc.aio.StreamUnaryClientInterceptor:
        this = self

        class Interceptor(grpc.aio.StreamUnaryClientInterceptor):
            async def intercept_stream_unary(self, *args):
                return await this.intercept_stream_unary(*args)

        return Interceptor()

    def stream_stream_interceptor(self) -> grpc.aio.StreamStreamClientInterceptor:
        this = self

        class Interceptor(grpc.aio.StreamStreamClientInterceptor):
            async def intercept_stream_stream(self, *args):
                return await this.intercept_stream_stream(*args)

        return Interceptor()

    def unary_unary_interceptor(self) -> grpc.aio.UnaryUnaryClientInterceptor:
        this = self

        class Interceptor(grpc.aio.UnaryUnaryClientInterceptor):
            async def intercept_unary_unary(self, *args):
                return await this.intercept_unary_unary(*args)

        return Interceptor()

    def interceptors(self) -> list[grpc.aio.ClientInterceptor]:
        return [
            self.unary_unary_interceptor(),
            self.unary_stream_interceptor(),
            self.stream_unary_interceptor(),
            self.stream_stream_interceptor(),
        ]

    def _merge_metadata(self, existing):
        result = list(existing or []) + self.metadata
        return result


class FumaroleGrpcConnector:
    logger = logging.getLogger(__name__)

    def __init__(self, config: FumaroleConfig, endpoint: str):
        self.config = config
        self.endpoint = endpoint

    async def connect(self, *grpc_options) -> FumaroleStub:
        options = [("grpc.max_receive_message_length", 111111110), *grpc_options]
        interceptors = MetadataInterceptor(self.config.x_metadata).interceptors()
        compression = (
            grpc.Compression.Gzip
            if self.config.response_compression == "gzip"
            else None
        )
        if self.config.x_token is not None:
            auth = TritonAuthMetadataPlugin(self.config.x_token)
            # ssl_creds allow you to use our https endpoint
            # grpc.ssl_channel_credentials with no arguments will look through your CA trust store.
            ssl_creds = grpc.ssl_channel_credentials()

            # call credentials will be sent on each request if setup with composite_channel_credentials.
            call_creds: grpc.CallCredentials = grpc.metadata_call_credentials(auth)

            # Combined creds will store the channel creds aswell as the call credentials
            combined_creds = grpc.composite_channel_credentials(ssl_creds, call_creds)
            FumaroleGrpcConnector.logger.debug(
                "Using secure channel with x-token authentication"
            )
            channel = grpc.aio.secure_channel(
                self.endpoint,
                credentials=combined_creds,
                options=options,
                compression=compression,
                interceptors=interceptors,
            )
        else:
            FumaroleGrpcConnector.logger.debug(
                "Using insecure channel without authentication"
            )
            channel = grpc.aio.insecure_channel(
                self.endpoint,
                options=options,
                interceptors=interceptors,
                compression=compression,
            )

        return FumaroleStub(channel)

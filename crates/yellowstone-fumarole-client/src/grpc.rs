use {
    crate::{
        config::FumaroleConfig, proto::fumarole_client::FumaroleClient,
        string_pairs_to_metadata_header, FumeInterceptor,
    },
    tonic::{
        service::interceptor::InterceptedService,
        transport::{Channel, Endpoint},
    },
};

#[derive(Clone)]
pub struct FumaroleGrpcConnector {
    pub config: FumaroleConfig,
    pub endpoint: Endpoint,
}

impl FumaroleGrpcConnector {
    pub async fn connect(
        &self,
    ) -> Result<FumaroleClient<InterceptedService<Channel, FumeInterceptor>>, tonic::transport::Error>
    {
        let channel = self.endpoint.connect().await?;
        let interceptor = FumeInterceptor {
            x_token: self
                .config
                .x_token
                .as_ref()
                .map(|token| token.try_into())
                .transpose()
                .unwrap(),
            metadata: string_pairs_to_metadata_header(self.config.x_metadata.clone()).unwrap(),
        };
        Ok(FumaroleClient::with_interceptor(channel, interceptor)
            .max_decoding_message_size(self.config.max_decoding_message_size_bytes))
    }
}

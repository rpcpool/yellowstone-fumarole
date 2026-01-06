use {
    crate::{
        FumeInterceptor, config::FumaroleConfig, proto::fumarole_client::FumaroleClient,
        string_pairs_to_metadata_header,
    },
    rand::{SeedableRng, seq::IndexedRandom},
    tonic::{
        service::interceptor::InterceptedService,
        transport::{Channel, Endpoint},
    },
};

#[derive(Clone)]
pub struct FumaroleGrpcConnector {
    pub config: FumaroleConfig,
    pub endpoints: Vec<Endpoint>,
}

impl FumaroleGrpcConnector {
    pub async fn connect(
        &self,
    ) -> Result<FumaroleClient<InterceptedService<Channel, FumeInterceptor>>, tonic::transport::Error>
    {
        // Pick a random endpoint from the list
        let mut rng = rand::rngs::StdRng::from_os_rng();
        let endpoint = self
            .endpoints
            .choose(&mut rng)
            .expect("at least one endpoint must be provided");
        let (channel, tx_channel) =
            Channel::balance_channel::<String>(self.config.connection_per_host.get());
        tracing::debug!("Connecting to fumarole endpoint: {}", endpoint.uri());

        for (i, endpoint) in self
            .endpoints
            .iter()
            .cycle()
            .take(self.config.connection_per_host.get())
            .enumerate()
        {
            let key = format!("{}-{}", endpoint.uri(), i);
            tx_channel
                .send(tonic::transport::channel::Change::Insert(
                    key,
                    endpoint.clone(),
                ))
                .await
                .expect("service closed");
        }

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
        let client = FumaroleClient::with_interceptor(channel, interceptor)
            .max_decoding_message_size(self.config.max_decoding_message_size_bytes);

        let client = if let Some(encoding) = self.config.response_compression {
            client.accept_compressed(encoding)
        } else {
            client
        };

        let client = if let Some(encoding) = self.config.request_compression {
            client.send_compressed(encoding)
        } else {
            client
        };

        Ok(client)
    }
}

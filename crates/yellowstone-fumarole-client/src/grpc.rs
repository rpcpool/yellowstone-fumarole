use {
    crate::{
        FumeInterceptor, config::FumaroleConfig, proto::fumarole_client::FumaroleClient,
        string_pairs_to_metadata_header,
    }, rand::{SeedableRng, seq::IndexedRandom}, std::sync::{Arc, atomic::AtomicU64}, tonic::{
        service::interceptor::InterceptedService,
        transport::{Channel, Endpoint},
    }
};

#[derive(Clone)]
pub struct FumaroleGrpcConnector {
    pub config: FumaroleConfig,
    pub endpoints: Vec<Endpoint>,
    pub connect_cnt: Arc<AtomicU64>,
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
            Channel::balance_channel::<String>(self.config.connection_per_host.get() *  self.endpoints.len());
        tracing::debug!("Connecting to fumarole endpoint: {}", endpoint.uri());
        let endpoint_len = self.endpoints.len() as u64;
        let mut idx = self.connect_cnt.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % endpoint_len;
        let mut i = 0;
        while i < self.config.connection_per_host.get() * endpoint_len as usize {

            let endpoint = self.endpoints
                .get(idx as usize)
                .expect("at least one endpoint must be provided");
            let key = format!("{}-{}", endpoint.uri(), idx);
            tracing::debug!("Adding channel endpoint: {} with key {}", endpoint.uri(), key);
            tx_channel
                .send(tonic::transport::channel::Change::Insert(
                    key,
                    endpoint.clone(),
                ))
                .await
                .expect("service closed");
            idx = self.connect_cnt.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % endpoint_len;
            i += 1;
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

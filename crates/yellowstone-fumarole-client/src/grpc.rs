use {
    crate::{
        FumeInterceptor, config::FumaroleConfig, proto::fumarole_client::FumaroleClient,
        string_pairs_to_metadata_header,
    },
    std::sync::{Arc, atomic::AtomicU64},
    tonic::{
        service::interceptor::InterceptedService,
        transport::{Channel, Endpoint},
    },
};

#[derive(Clone)]
pub struct FumaroleGrpcConnector {
    pub config: FumaroleConfig,
    pub endpoints: Vec<Endpoint>,
    pub connect_cnt: Arc<AtomicU64>,
}

impl FumaroleGrpcConnector {
    pub fn connect_lazy(&self) -> FumaroleClient<InterceptedService<Channel, FumeInterceptor>> {
        let endpoint_idx = self
            .connect_cnt
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            % (self.endpoints.len() as u64);

        let endpoint = self
            .endpoints
            .get(endpoint_idx as usize)
            .expect("at least one endpoint must be provided");

        #[cfg(feature = "prometheus")]
        {
            crate::metrics::inc_endpoint_connection_count(endpoint.uri().to_string().as_str());
        }

        let channel = endpoint.connect_lazy();

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

        if let Some(encoding) = self.config.request_compression {
            client.send_compressed(encoding)
        } else {
            client
        }
    }

    pub async fn connect(
        &self,
    ) -> Result<FumaroleClient<InterceptedService<Channel, FumeInterceptor>>, tonic::transport::Error>
    {
        let endpoint_idx = self
            .connect_cnt
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            % (self.endpoints.len() as u64);

        let endpoint = self
            .endpoints
            .get(endpoint_idx as usize)
            .expect("at least one endpoint must be provided");

        #[cfg(feature = "prometheus")]
        {
            crate::metrics::inc_endpoint_connection_count(endpoint.uri().to_string().as_str());
        }

        let channel = endpoint.connect().await?;

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

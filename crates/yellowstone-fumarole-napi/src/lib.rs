use {
    futures::SinkExt as _,
    napi::bindgen_prelude::*,
    napi_derive::napi,
    prost::Message as ProstMessage,
    std::{
        num::{NonZeroU8, NonZeroUsize},
        sync::Arc,
        time::Duration,
    },
    tokio::sync::{Mutex, mpsc},
    yellowstone_fumarole_client::{
        FumaroleClient as RustFumaroleClient, FumaroleEvent as RustFumaroleEvent, FumaroleSink,
        FumaroleSubscribeConfig as RustFumaroleSubscribeConfig,
        config::FumaroleConfig as RustFumaroleConfig, proto as fumarole_proto,
    },
    yellowstone_grpc_proto::geyser,
};

// ─── Config ──────────────────────────────────────────────────────────────────

/// Connection configuration for a Fumarole client.
#[napi(object)]
pub struct FumaroleConfigOptions {
    pub endpoint: String,
    pub x_token: Option<String>,
    /// Maximum protobuf message size in bytes (default: 512 MB).
    pub max_decoding_message_size_bytes: Option<u32>,
}

/// Tuning options for a [`FumaroleSubscription`].
#[napi(object)]
#[derive(Default)]
pub struct FumaroleSubscribeConfigOptions {
    /// Number of parallel data-plane TCP connections (default: 1).
    pub num_data_plane_tcp_connections: Option<u8>,
    /// Max concurrent downloads per TCP connection (default: 2).
    pub concurrent_download_limit_per_tcp: Option<u32>,
    /// Offset commit interval in milliseconds (default: 10 000).
    pub commit_interval_ms: Option<u32>,
    /// Max consecutive failed slot downloads before the session fails (default: 3).
    pub max_failed_slot_download_attempt: Option<u32>,
    /// GC tick interval (default: 100).
    pub gc_interval: Option<u32>,
    /// How many slots of dedup memory to keep (default: 1 000).
    pub slot_memory_retention: Option<u32>,
    /// Disable offset commits entirely.
    pub no_commit: Option<bool>,
    /// Automatically commit progress after each event.
    pub auto_commit: Option<bool>,
}

// ─── Event ───────────────────────────────────────────────────────────────────

/// A single event yielded by a [`FumaroleSubscription`].
#[napi(object)]
pub struct FumaroleEvent {
    pub slot: BigInt,
    /// `true` when the slot has finished streaming (no more data for this slot).
    pub is_slot_ended: bool,
    /// Protobuf-encoded `geyser.SubscribeUpdate` bytes.
    /// Present only when `is_slot_ended` is `false`.
    pub update: Option<Buffer>,
}

// ─── Internal channel payload ─────────────────────────────────────────────────

struct RawEvent {
    slot: u64,
    is_slot_ended: bool,
    update_bytes: Option<Vec<u8>>,
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn napi_err(msg: impl std::fmt::Display) -> Error {
    Error::from_reason(msg.to_string())
}

fn to_rust_config(options: FumaroleConfigOptions) -> Result<RustFumaroleConfig> {
    use serde_yaml::Value;
    let mut map = serde_yaml::Mapping::new();
    map.insert(
        Value::String("endpoint".to_string()),
        Value::String(options.endpoint),
    );
    if let Some(token) = options.x_token {
        map.insert(Value::String("x-token".to_string()), Value::String(token));
    }
    if let Some(max_size) = options.max_decoding_message_size_bytes {
        map.insert(
            Value::String("max_decoding_message_size_bytes".to_string()),
            Value::Number(max_size.into()),
        );
    }
    serde_yaml::from_value(Value::Mapping(map)).map_err(napi_err)
}

fn to_rust_subscribe_config(
    options: FumaroleSubscribeConfigOptions,
) -> RustFumaroleSubscribeConfig {
    #[allow(deprecated)]
    let mut config = RustFumaroleSubscribeConfig::default();
    if let Some(n) = options.num_data_plane_tcp_connections {
        if let Some(nz) = NonZeroU8::new(n) {
            config.num_data_plane_tcp_connections = nz;
        }
    }
    if let Some(n) = options.concurrent_download_limit_per_tcp {
        if let Some(nz) = NonZeroUsize::new(n as usize) {
            config.concurrent_download_limit_per_tcp = nz;
        }
    }
    if let Some(ms) = options.commit_interval_ms {
        config.commit_interval = Duration::from_millis(ms as u64);
    }
    if let Some(n) = options.max_failed_slot_download_attempt {
        config.max_failed_slot_download_attempt = n as usize;
    }
    if let Some(n) = options.gc_interval {
        config.gc_interval = n as usize;
    }
    if let Some(n) = options.slot_memory_retention {
        config.slot_memory_retention = n as usize;
    }
    if let Some(b) = options.no_commit {
        config.no_commit = b;
    }
    if let Some(b) = options.auto_commit {
        config.auto_commit = b;
    }
    config
}

// ─── FumaroleSubscription ────────────────────────────────────────────────────

/// An active Fumarole subscription.
///
/// Call [`next`] in a loop to receive events, or use `for await` on the
/// async iterator. Send [`geyser.SubscribeRequest`] updates via [`send`] to
/// change filters while the stream is live.
#[napi]
pub struct FumaroleSubscription {
    event_rx: Arc<Mutex<mpsc::Receiver<std::result::Result<RawEvent, String>>>>,
    sink: Arc<Mutex<FumaroleSink>>,
}

#[napi]
impl FumaroleSubscription {
    /// Returns the next event from the subscription, or `null` when the stream ends.
    #[napi]
    pub async fn next(&self) -> Result<Option<FumaroleEvent>> {
        let mut rx = self.event_rx.lock().await;
        match rx.recv().await {
            None => Ok(None),
            Some(Ok(raw)) => Ok(Some(FumaroleEvent {
                slot: BigInt {
                    sign_bit: false,
                    words: vec![raw.slot],
                },
                is_slot_ended: raw.is_slot_ended,
                update: raw.update_bytes.map(Buffer::from),
            })),
            Some(Err(msg)) => Err(Error::from_reason(msg)),
        }
    }

    /// Update the active subscription filters.
    ///
    /// `request` must be a protobuf-encoded `geyser.SubscribeRequest`.
    #[napi]
    pub async fn send(&self, request: Buffer) -> Result<()> {
        let request = geyser::SubscribeRequest::decode(request.as_ref())
            .map_err(|e| napi_err(format!("failed to decode SubscribeRequest: {e}")))?;
        let mut guard = self.sink.lock().await;
        guard.send(request).await.map_err(napi_err)
    }
}

// ─── FumaroleClient ──────────────────────────────────────────────────────────

/// A client connected to a Fumarole service.
#[napi]
pub struct FumaroleClient {
    inner: Arc<Mutex<RustFumaroleClient>>,
}

#[napi]
impl FumaroleClient {
    /// Connect to a Fumarole service.
    #[napi(factory)]
    pub async fn connect(config: FumaroleConfigOptions) -> Result<Self> {
        let rust_config = to_rust_config(config)?;
        let client = RustFumaroleClient::connect(rust_config)
            .await
            .map_err(napi_err)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(client)),
        })
    }

    /// Returns the service version as a protobuf-encoded `VersionResponse`.
    #[napi]
    pub async fn version(&self) -> Result<Buffer> {
        let response = self.inner.lock().await.version().await.map_err(napi_err)?;
        Ok(Buffer::from(response.encode_to_vec()))
    }

    /// Subscribe to a stream of updates using default config.
    ///
    /// `request` must be a protobuf-encoded `geyser.SubscribeRequest`.
    #[napi]
    pub async fn subscribe(
        &self,
        subscriber_name: String,
        request: Buffer,
    ) -> Result<FumaroleSubscription> {
        self.subscribe_with_config(subscriber_name, request, Default::default())
            .await
    }

    /// Subscribe to a stream of updates with custom tuning.
    ///
    /// `request` must be a protobuf-encoded `geyser.SubscribeRequest`.
    #[napi]
    pub async fn subscribe_with_config(
        &self,
        subscriber_name: String,
        request: Buffer,
        config: FumaroleSubscribeConfigOptions,
    ) -> Result<FumaroleSubscription> {
        let subscribe_request = geyser::SubscribeRequest::decode(request.as_ref())
            .map_err(|e| napi_err(format!("failed to decode SubscribeRequest: {e}")))?;
        let rust_config = to_rust_subscribe_config(config);

        let subscription = self
            .inner
            .lock()
            .await
            .subscribe_with_config(subscriber_name, subscribe_request, rust_config)
            .await
            .map_err(napi_err)?;

        let (sink, stream) = subscription.split();
        let (event_tx, event_rx) = mpsc::channel::<std::result::Result<RawEvent, String>>(256);

        tokio::spawn(async move {
            use futures::StreamExt as _;
            let mut stream = std::pin::pin!(stream);
            while let Some(result) = stream.next().await {
                let raw = match result {
                    Ok(RustFumaroleEvent::Data { slot, update }) => Ok(RawEvent {
                        slot,
                        is_slot_ended: false,
                        update_bytes: Some(update.encode_to_vec()),
                    }),
                    Ok(RustFumaroleEvent::SlotEnded(slot)) => Ok(RawEvent {
                        slot,
                        is_slot_ended: true,
                        update_bytes: None,
                    }),
                    Err(e) => Err(format!("{e}")),
                };
                let is_err = raw.is_err();
                if event_tx.send(raw).await.is_err() || is_err {
                    break;
                }
            }
        });

        Ok(FumaroleSubscription {
            event_rx: Arc::new(Mutex::new(event_rx)),
            sink: Arc::new(Mutex::new(sink)),
        })
    }

    /// Returns a protobuf-encoded `ListConsumerGroupsResponse`.
    ///
    /// `request` must be a protobuf-encoded `ListConsumerGroupsRequest`.
    #[napi]
    pub async fn list_consumer_groups(&self, request: Buffer) -> Result<Buffer> {
        let req = fumarole_proto::ListConsumerGroupsRequest::decode(request.as_ref())
            .map_err(|e| napi_err(format!("failed to decode request: {e}")))?;
        let response = self
            .inner
            .lock()
            .await
            .list_consumer_groups(req)
            .await
            .map_err(napi_err)?
            .into_inner();
        Ok(Buffer::from(response.encode_to_vec()))
    }

    /// Returns a protobuf-encoded `ConsumerGroupInfo`.
    ///
    /// `request` must be a protobuf-encoded `GetConsumerGroupInfoRequest`.
    #[napi]
    pub async fn get_consumer_group_info(&self, request: Buffer) -> Result<Buffer> {
        let req = fumarole_proto::GetConsumerGroupInfoRequest::decode(request.as_ref())
            .map_err(|e| napi_err(format!("failed to decode request: {e}")))?;
        let response = self
            .inner
            .lock()
            .await
            .get_consumer_group_info(req)
            .await
            .map_err(napi_err)?
            .into_inner();
        Ok(Buffer::from(response.encode_to_vec()))
    }

    /// Returns a protobuf-encoded `DeleteConsumerGroupResponse`.
    ///
    /// `request` must be a protobuf-encoded `DeleteConsumerGroupRequest`.
    #[napi]
    pub async fn delete_consumer_group(&self, request: Buffer) -> Result<Buffer> {
        let req = fumarole_proto::DeleteConsumerGroupRequest::decode(request.as_ref())
            .map_err(|e| napi_err(format!("failed to decode request: {e}")))?;
        let response = self
            .inner
            .lock()
            .await
            .delete_consumer_group(req)
            .await
            .map_err(napi_err)?
            .into_inner();
        Ok(Buffer::from(response.encode_to_vec()))
    }

    /// Returns a protobuf-encoded `CreateConsumerGroupResponse`.
    ///
    /// `request` must be a protobuf-encoded `CreateConsumerGroupRequest`.
    #[napi]
    pub async fn create_consumer_group(&self, request: Buffer) -> Result<Buffer> {
        let req = fumarole_proto::CreateConsumerGroupRequest::decode(request.as_ref())
            .map_err(|e| napi_err(format!("failed to decode request: {e}")))?;
        let response = self
            .inner
            .lock()
            .await
            .create_consumer_group(req)
            .await
            .map_err(napi_err)?
            .into_inner();
        Ok(Buffer::from(response.encode_to_vec()))
    }

    /// Returns a protobuf-encoded `GetChainTipResponse`.
    ///
    /// `request` must be a protobuf-encoded `GetChainTipRequest`.
    #[napi]
    pub async fn get_chain_tip(&self, request: Buffer) -> Result<Buffer> {
        let req = fumarole_proto::GetChainTipRequest::decode(request.as_ref())
            .map_err(|e| napi_err(format!("failed to decode request: {e}")))?;
        let response = self
            .inner
            .lock()
            .await
            .get_chain_tip(req)
            .await
            .map_err(napi_err)?
            .into_inner();
        Ok(Buffer::from(response.encode_to_vec()))
    }

    /// Returns a protobuf-encoded `GetSlotRangeResponse`.
    #[napi]
    pub async fn get_slot_range(&self) -> Result<Buffer> {
        let response = self
            .inner
            .lock()
            .await
            .get_slot_range()
            .await
            .map_err(napi_err)?
            .into_inner();
        Ok(Buffer::from(response.encode_to_vec()))
    }
}

use {
    lazy_static::lazy_static,
    prometheus::{Histogram, HistogramOpts, IntCounter, IntGauge, IntGaugeVec, Opts},
    std::time::Duration,
};

lazy_static! {
    pub(crate) static ref FAILED_SLOT_DOWNLOAD_ATTEMPT: IntCounter = IntCounter::new(
        "fumarole_failed_slot_download_attempt",
        "Number of failed slot download attempts from Fumarole",
    )
    .unwrap();
    pub(crate) static ref SLOT_DOWNLOAD_COUNT: IntCounter = IntCounter::new(
        "fumarole_slot_download_count",
        "Number of slots downloaded from Fumarole",
    )
    .unwrap();
    pub(crate) static ref INFLIGHT_SLOT_DOWNLOAD: IntGauge = IntGauge::new(
        "fumarole_inflight_slot_download",
        "Number of parallel inflight slots downloaded from Fumarole",
    )
    .unwrap();

    pub(crate) static ref SLOT_DOWNLOAD_DURATION: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "fumarole_slot_download_duration_ms",
            "Slot download duration distribution from Fumarole in milliseconds",
        )
        .buckets(vec![
            1.0,
            10.0,
            20.0,
            40.0,
            80.0,
            160.0,
            200.0,
            240.0,
            280.0,
            320.0,
            400.0,
            800.0,
            1000.0,
            2000.0,
            f64::INFINITY
        ]),
    )
    .unwrap();

    pub(crate) static ref MAX_SLOT_DETECTED: IntGauge = IntGauge::new(
        "fumarole_max_slot_detected",
        "Max slot detected from Fumarole SDK runtime, can be used to detect rough slot lag",
    )
    .unwrap();

    pub(crate) static ref OFFSET_COMMITMENT_COUNT: IntCounter = IntCounter::new(
        "fumarole_offset_commitment_count",
        "Number of offset commitment done to remote Fumarole service",
    )
    .unwrap();

    pub(crate) static ref SKIP_OFFSET_COMMITMENT_COUNT: IntCounter = IntCounter::new(
        "fumarole_skip_offset_commitment_count",
        "Number of skipped offset commitment done to remote Fumarole service",
    )
    .unwrap();
    pub(crate) static ref TOTAL_EVENT_DOWNLOADED: IntCounter = IntCounter::new(
        "fumarole_total_event_downloaded",
        "Total number of events downloaded from Fumarole",
    )
    .unwrap();

    pub(crate) static ref SLOT_STATUS_OFFSET_PROCESSED_CNT: IntCounter = IntCounter::new(
        "fumarole_slot_status_offset_processed_count",
        "Number of offset processed from Fumarole runtime",
    )
    .unwrap();

    pub(crate) static ref PROCESSED_SLOT_STATUS_OFFSET_QUEUE: IntGauge = IntGauge::new(
        "fumarole_processed_slot_status_offset_queue",
        "The number of slot status offset that is blocked from commitment, waiting for missing offset to be acknowledged",
    )
    .unwrap();

    pub(crate) static ref SLOT_STATUS_UPDATE_QUEUE_LEN: IntGauge = IntGauge::new(
        "fumarole_slot_status_update_queue_len",
        "The number of slot status update that is waiting to be ack",
    ).unwrap();



    pub(crate) static ref MAX_OFFSET_COMMITTED: IntGauge = IntGauge::new(
        "fumarole_max_offset_committed",
        "Max offset committed to Fumarole runtime",
    ).unwrap();


    pub(crate) static ref FUMAROLE_BLOCKCHAIN_OFFSET_TIP: IntGauge = IntGauge::new(
        "fumarole_blockchain_offset_tip",
        "The current offset tip of the Fumarole blockchain",
    ).unwrap();

    pub(crate) static ref FUMAROLE_OFFSET_LAG_FROM_TIP: IntGauge = IntGauge::new(
        "fumarole_offset_lag_from_tip",
        "The difference between last committed offset and the current tip of the Fumarole blockchain",
    ).unwrap();


    pub(crate) static ref DOWNLOAD_QUEUE_FULL_DETECTION_COUNT: IntCounter = IntCounter::new(
        "fumarole_download_queue_full_detection_count",
        "The number of times the download queue is full",
    ).unwrap();

    pub(crate) static ref POLL_HISTORY_CALL_COUNT: IntCounter = IntCounter::new(
        "fumarole_poll_history_call_count",
        "The number of times the poll history command was sent",
    ).unwrap();

    pub(crate) static ref DOWNLOAD_SHARD_DOWNLOAD_TIME: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "fumarole_download_shard_download_time_ms",
            "Cumulative wait duration distribution for download shard recv in milliseconds",
        )
        .buckets(vec![
            20.0,
            40.0,
            60.0,
            80.0,
            160.0,
            320.0,
            400.0,
            500.0,
            600.0,
            700.0,
            800.0,
            1000.0,
            2000.0,
            4000.0,
            f64::INFINITY
        ]),
    ).unwrap();

    pub(crate) static ref INFLIGHT_SLOT_SHARD_DOWNLOAD: IntGauge = IntGauge::new(
        "fumarole_inflight_slot_shard_download",
        "Number of parallel inflight slot shards being downloaded from Fumarole",
    ).unwrap();

    pub(crate) static ref ENDPOINT_CONNECTION_COUNT: IntGaugeVec = IntGaugeVec::new(
        Opts::new(
            "fumarole_endpoint_connection_count",
            "Number of active connections to Fumarole endpoints",
        ),
        &["endpoint"],
    ).unwrap();
}

pub(crate) fn inc_endpoint_connection_count(endpoint: &str) {
    ENDPOINT_CONNECTION_COUNT
        .with_label_values(&[endpoint])
        .inc();
}

pub(crate) fn set_inflight_slot_shard_download(count: usize) {
    INFLIGHT_SLOT_SHARD_DOWNLOAD.set(count as i64);
}

pub(crate) fn inc_poll_history_call_count() {
    POLL_HISTORY_CALL_COUNT.inc();
}

pub(crate) fn incr_download_queue_full_detection_count() {
    DOWNLOAD_QUEUE_FULL_DETECTION_COUNT.inc();
}

pub(crate) fn observe_download_shard_download_time(duration: Duration) {
    DOWNLOAD_SHARD_DOWNLOAD_TIME.observe(duration.as_millis() as f64);
}

pub(crate) fn set_fumarole_blockchain_offset_tip(offset: i64) {
    FUMAROLE_BLOCKCHAIN_OFFSET_TIP.set(offset);
    update_fumarole_offset_lag_from_tip();
}

fn update_fumarole_offset_lag_from_tip() {
    let tip = FUMAROLE_BLOCKCHAIN_OFFSET_TIP.get();

    let committed = MAX_OFFSET_COMMITTED.get();

    let tip = tip.max(0) as u64;

    let lag = tip.saturating_sub(committed.max(0) as u64);

    FUMAROLE_OFFSET_LAG_FROM_TIP.set(lag as i64);
}

pub(crate) fn set_max_offset_committed(offset: i64) {
    MAX_OFFSET_COMMITTED.set(offset);
    update_fumarole_offset_lag_from_tip();
}

pub(crate) fn inc_total_event_downloaded(amount: usize) {
    TOTAL_EVENT_DOWNLOADED.inc_by(amount as u64);
}

pub(crate) fn set_max_slot_detected(slot: u64) {
    MAX_SLOT_DETECTED.set(slot as i64);
}

pub(crate) fn inc_slot_download_count() {
    SLOT_DOWNLOAD_COUNT.inc();
}

pub(crate) fn inc_inflight_slot_download() {
    INFLIGHT_SLOT_DOWNLOAD.inc();
}

pub(crate) fn dec_inflight_slot_download() {
    INFLIGHT_SLOT_DOWNLOAD.dec();
}

pub(crate) fn inc_offset_commitment_count() {
    OFFSET_COMMITMENT_COUNT.inc();
}

pub(crate) fn observe_slot_download_duration(duration: Duration) {
    SLOT_DOWNLOAD_DURATION.observe(duration.as_millis() as f64);
}

pub(crate) fn inc_failed_slot_download_attempt() {
    FAILED_SLOT_DOWNLOAD_ATTEMPT.inc();
}

pub(crate) fn inc_skip_offset_commitment_count() {
    SKIP_OFFSET_COMMITMENT_COUNT.inc();
}

pub(crate) fn inc_slot_status_offset_processed_count() {
    SLOT_STATUS_OFFSET_PROCESSED_CNT.inc();
}

pub(crate) fn set_processed_slot_status_offset_queue_len(len: usize) {
    PROCESSED_SLOT_STATUS_OFFSET_QUEUE.set(len as i64);
}

pub(crate) fn set_slot_status_update_queue_len(len: usize) {
    SLOT_STATUS_UPDATE_QUEUE_LEN.set(len as i64);
}

///
/// Register Fumarole metrics to the given registry.
///
pub fn register_metrics(registry: &prometheus::Registry) {
    registry
        .register(Box::new(SLOT_DOWNLOAD_COUNT.clone()))
        .unwrap();
    registry
        .register(Box::new(INFLIGHT_SLOT_DOWNLOAD.clone()))
        .unwrap();
    registry
        .register(Box::new(SLOT_DOWNLOAD_DURATION.clone()))
        .unwrap();
    registry
        .register(Box::new(MAX_SLOT_DETECTED.clone()))
        .unwrap();
    registry
        .register(Box::new(OFFSET_COMMITMENT_COUNT.clone()))
        .unwrap();
    registry
        .register(Box::new(FAILED_SLOT_DOWNLOAD_ATTEMPT.clone()))
        .unwrap();
    registry
        .register(Box::new(TOTAL_EVENT_DOWNLOADED.clone()))
        .unwrap();
    registry
        .register(Box::new(SKIP_OFFSET_COMMITMENT_COUNT.clone()))
        .unwrap();
    registry
        .register(Box::new(SLOT_STATUS_OFFSET_PROCESSED_CNT.clone()))
        .unwrap();
    registry
        .register(Box::new(PROCESSED_SLOT_STATUS_OFFSET_QUEUE.clone()))
        .unwrap();
    registry
        .register(Box::new(SLOT_STATUS_UPDATE_QUEUE_LEN.clone()))
        .unwrap();
    registry
        .register(Box::new(MAX_OFFSET_COMMITTED.clone()))
        .unwrap();
    registry
        .register(Box::new(FUMAROLE_BLOCKCHAIN_OFFSET_TIP.clone()))
        .unwrap();
    registry
        .register(Box::new(FUMAROLE_OFFSET_LAG_FROM_TIP.clone()))
        .unwrap();
    registry
        .register(Box::new(DOWNLOAD_QUEUE_FULL_DETECTION_COUNT.clone()))
        .unwrap();
    registry
        .register(Box::new(POLL_HISTORY_CALL_COUNT.clone()))
        .unwrap();
    registry
        .register(Box::new(DOWNLOAD_SHARD_DOWNLOAD_TIME.clone()))
        .unwrap();
    registry
        .register(Box::new(INFLIGHT_SLOT_SHARD_DOWNLOAD.clone()))
        .unwrap();
    registry
        .register(Box::new(ENDPOINT_CONNECTION_COUNT.clone()))
        .unwrap();
}

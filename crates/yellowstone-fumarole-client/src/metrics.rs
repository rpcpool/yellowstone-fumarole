use {
    lazy_static::lazy_static,
    prometheus::{HistogramOpts, HistogramVec, IntCounterVec, IntGaugeVec, Opts},
    std::time::Duration,
};

lazy_static! {
    pub(crate) static ref FAILED_SLOT_DOWNLOAD_ATTEMPT: IntCounterVec = IntCounterVec::new(
        Opts::new(
            "fumarole_failed_slot_download_attempt",
            "Number of failed slot download attempts from Fumarole",
        ),
        &["runtime"],
    )
    .unwrap();
    pub(crate) static ref SLOT_DOWNLOAD_COUNT: IntCounterVec = IntCounterVec::new(
        Opts::new(
            "fumarole_slot_download_count",
            "Number of slots downloaded from Fumarole",
        ),
        &["runtime"],
    )
    .unwrap();
    pub(crate) static ref INFLIGHT_SLOT_DOWNLOAD: IntGaugeVec = IntGaugeVec::new(
        Opts::new(
            "fumarole_inflight_slot_download",
            "Number of parallel inflight slots downloaded from Fumarole",
        ),
        &["runtime"],
    )
    .unwrap();
    pub(crate) static ref SLOT_DOWNLOAD_DURATION: HistogramVec = HistogramVec::new(
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
            320.0,
            400.0,
            800.0,
            1000.0,
            2000.0,
            f64::INFINITY
        ]),
        &["runtime"],
    )
    .unwrap();
    pub(crate) static ref MAX_SLOT_DETECTED: IntGaugeVec = IntGaugeVec::new(
        Opts::new(
            "fumarole_max_slot_detected",
            "Max slot detected from Fumarole SDK runtime, can be used to detect rough slot lag",
        ),
        &["runtime"],
    )
    .unwrap();
    pub(crate) static ref OFFSET_COMMITMENT_COUNT: IntCounterVec = IntCounterVec::new(
        Opts::new(
            "fumarole_offset_commitment_count",
            "Number of offset commitment done to remote Fumarole service",
        ),
        &["runtime"],
    )
    .unwrap();
    pub(crate) static ref SKIP_OFFSET_COMMITMENT_COUNT: IntCounterVec = IntCounterVec::new(
        Opts::new(
            "fumarole_skip_offset_commitment_count",
            "Number of skipped offset commitment done to remote Fumarole service",
        ),
        &["runtime"],
    )
    .unwrap();
    pub(crate) static ref TOTAL_EVENT_DOWNLOADED: IntCounterVec = IntCounterVec::new(
        Opts::new(
            "fumarole_total_event_downloaded",
            "Total number of events downloaded from Fumarole",
        ),
        &["runtime"],
    )
    .unwrap();
    pub(crate) static ref SLOT_STATUS_OFFSET_PROCESSED_CNT: IntCounterVec = IntCounterVec::new(
        Opts::new(
            "fumarole_slot_status_offset_processed_count",
            "Number of offset processed from Fumarole runtime",
        ),
        &["runtime"],
    )
    .unwrap();
    pub(crate) static ref PROCESSED_SLOT_STATUS_OFFSET_QUEUE: IntGaugeVec = IntGaugeVec::new(
        Opts::new(
            "fumarole_processed_slot_status_offset_queue",
            "The number of slot status offset that is blocked from commitment, waiting for missing offset to be acknowledged",
        ),
        &["runtime"],
    ).unwrap();
    pub(crate) static ref SLOT_STATUS_UPDATE_QUEUE_LEN: IntGaugeVec = IntGaugeVec::new(
        Opts::new(
            "fumarole_slot_status_update_queue_len",
            "The number of slot status update that is waiting to be ack",
        ),
        &["runtime"],
    ).unwrap();

    pub(crate) static ref MAX_OFFSET_COMMITTED: IntGaugeVec = IntGaugeVec::new(
        Opts::new(
            "fumarole_max_offset_committed",
            "Max offset committed to Fumarole runtime",
        ),
        &["runtime"],
    ).unwrap();

    pub(crate) static ref FUMAROLE_BLOCKCHAIN_OFFSET_TIP: IntGaugeVec = IntGaugeVec::new(
        Opts::new(
            "fumarole_blockchain_offset_tip",
            "The current offset tip of the Fumarole blockchain",
        ),
        &["runtime"],
    ).unwrap();

    pub(crate) static ref FUMAROLE_OFFSET_LAG_FROM_TIP: IntGaugeVec = IntGaugeVec::new(
        Opts::new(
            "fumarole_offset_lag_from_tip",
            "The difference between last committed offset and the current tip of the Fumarole blockchain",
        ),
        &["runtime"],
    ).unwrap();
}

pub(crate) fn set_fumarole_blockchain_offset_tip(name: impl AsRef<str>, offset: i64) {
    FUMAROLE_BLOCKCHAIN_OFFSET_TIP
        .with_label_values(&[name.as_ref()])
        .set(offset);
    update_fumarole_offset_lag_from_tip(name);
}

fn update_fumarole_offset_lag_from_tip(name: impl AsRef<str>) {
    let tip = FUMAROLE_BLOCKCHAIN_OFFSET_TIP
        .get_metric_with_label_values(&[name.as_ref()])
        .map(|m| m.get())
        .unwrap_or(0);

    let committed = MAX_OFFSET_COMMITTED
        .get_metric_with_label_values(&[name.as_ref()])
        .map(|m| m.get())
        .unwrap_or(0);

    let tip = tip.max(0) as u64;

    let lag = tip.saturating_sub(committed.max(0) as u64);

    FUMAROLE_OFFSET_LAG_FROM_TIP
        .with_label_values(&[name.as_ref()])
        .set(lag as i64);
}

pub(crate) fn set_max_offset_committed(name: impl AsRef<str>, offset: i64) {
    MAX_OFFSET_COMMITTED
        .with_label_values(&[name.as_ref()])
        .set(offset);
    update_fumarole_offset_lag_from_tip(name);
}

pub(crate) fn inc_total_event_downloaded(name: impl AsRef<str>, amount: usize) {
    TOTAL_EVENT_DOWNLOADED
        .with_label_values(&[name.as_ref()])
        .inc_by(amount as u64);
}

pub(crate) fn set_max_slot_detected(name: impl AsRef<str>, slot: u64) {
    MAX_SLOT_DETECTED
        .with_label_values(&[name.as_ref()])
        .set(slot as i64);
}

pub(crate) fn inc_slot_download_count(name: impl AsRef<str>) {
    SLOT_DOWNLOAD_COUNT
        .with_label_values(&[name.as_ref()])
        .inc();
}

pub(crate) fn inc_inflight_slot_download(name: impl AsRef<str>) {
    INFLIGHT_SLOT_DOWNLOAD
        .with_label_values(&[name.as_ref()])
        .inc();
}

pub(crate) fn dec_inflight_slot_download(name: impl AsRef<str>) {
    INFLIGHT_SLOT_DOWNLOAD
        .with_label_values(&[name.as_ref()])
        .dec();
}

pub(crate) fn inc_offset_commitment_count(name: impl AsRef<str>) {
    OFFSET_COMMITMENT_COUNT
        .with_label_values(&[name.as_ref()])
        .inc();
}

pub(crate) fn observe_slot_download_duration(name: impl AsRef<str>, duration: Duration) {
    SLOT_DOWNLOAD_DURATION
        .with_label_values(&[name.as_ref()])
        .observe(duration.as_millis() as f64);
}

pub(crate) fn inc_failed_slot_download_attempt(name: impl AsRef<str>) {
    FAILED_SLOT_DOWNLOAD_ATTEMPT
        .with_label_values(&[name.as_ref()])
        .inc();
}

pub(crate) fn inc_skip_offset_commitment_count(name: impl AsRef<str>) {
    SKIP_OFFSET_COMMITMENT_COUNT
        .with_label_values(&[name.as_ref()])
        .inc();
}

pub(crate) fn inc_slot_status_offset_processed_count(name: impl AsRef<str>) {
    SLOT_STATUS_OFFSET_PROCESSED_CNT
        .with_label_values(&[name.as_ref()])
        .inc();
}

pub(crate) fn set_processed_slot_status_offset_queue_len(name: impl AsRef<str>, len: usize) {
    PROCESSED_SLOT_STATUS_OFFSET_QUEUE
        .with_label_values(&[name.as_ref()])
        .set(len as i64);
}

pub(crate) fn set_slot_status_update_queue_len(name: impl AsRef<str>, len: usize) {
    SLOT_STATUS_UPDATE_QUEUE_LEN
        .with_label_values(&[name.as_ref()])
        .set(len as i64);
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
}

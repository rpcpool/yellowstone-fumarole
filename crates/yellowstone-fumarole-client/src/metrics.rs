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
    pub(crate) static ref SLOT_DOWNLOAD_QUEUE_SIZE: IntGaugeVec = IntGaugeVec::new(
        Opts::new(
            "fumarole_slot_download_queue",
            "Number slot download requests in the queue, waiting to be downloaded",
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
    pub(crate) static ref TOTAL_EVENT_DOWNLOADED: IntCounterVec = IntCounterVec::new(
        Opts::new(
            "fumarole_total_event_downloaded",
            "Total number of events downloaded from Fumarole",
        ),
        &["runtime"],
    )
    .unwrap();
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

pub(crate) fn set_slot_download_queue_size(name: impl AsRef<str>, size: usize) {
    SLOT_DOWNLOAD_QUEUE_SIZE
        .with_label_values(&[name.as_ref()])
        .set(size as i64);
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
        .register(Box::new(SLOT_DOWNLOAD_QUEUE_SIZE.clone()))
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
}

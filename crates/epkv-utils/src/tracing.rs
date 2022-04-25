#[inline(never)]
pub fn setup_tracing() {
    use tracing_subscriber::fmt::{format::Format, time::UtcTime};
    use tracing_subscriber::util::SubscriberInitExt;
    use tracing_subscriber::EnvFilter;

    tracing_subscriber::fmt()
        .event_format(Format::default().pretty())
        .with_env_filter(EnvFilter::from_default_env())
        .with_timer(UtcTime::rfc_3339())
        .finish()
        .init();
}

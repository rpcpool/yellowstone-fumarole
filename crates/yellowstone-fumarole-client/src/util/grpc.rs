use {tokio::sync::mpsc, tonic::Streaming};

pub fn into_bounded_mpsc_rx<T>(
    capacity: usize,
    mut streaming: Streaming<T>,
) -> mpsc::Receiver<Result<T, tonic::Status>>
where
    T: Send + 'static,
{
    let (tx, rx) = mpsc::channel(capacity);
    tokio::spawn(async move {
        while let Some(result) = streaming.message().await.transpose() {
            if tx.send(result).await.is_err() {
                break;
            }
        }
    });
    rx
}

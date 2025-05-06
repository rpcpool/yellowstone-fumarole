use {
    http_body_util::Full,
    hyper::{body::Bytes, server::conn::http1, service::service_fn, Request, Response},
    hyper_util::rt::TokioIo,
    prometheus::{Registry, TextEncoder},
    std::{convert::Infallible, net::SocketAddr},
    tokio::net::TcpListener,
};

pub async fn prometheus_service_fn(
    registry: &Registry,
    _: Request<hyper::body::Incoming>,
) -> Result<hyper::Response<Full<Bytes>>, Infallible> {
    let metrics = TextEncoder::new().encode_to_string(&registry.gather());

    match metrics {
        Ok(metrics) => Ok(Response::new(Full::new(Bytes::from(metrics)))),
        Err(e) => {
            Ok(Response::new(Full::new(Bytes::from(format!(
                "Failed to encode metrics: {}",
                e
            )))))
        }
    }
}

pub async fn prometheus_server(bind_addr: SocketAddr, registry: Registry) {
    // We create a TcpListener and bind it to 127.0.0.1:3000
    let listener = TcpListener::bind(bind_addr)
        .await
        .expect("Failed to bind TCP listener");
    let addr = listener.local_addr().expect("Failed to get local address");
    println!("Prometheus listening on http://{}", addr);
    // We start a loop to continuously accept incoming connections
    loop {
        let (stream, _) = listener
            .accept()
            .await
            .expect("Failed to accept connection");

        // Use an adapter to access something implementing `tokio::io` traits as if they implement
        // `hyper::rt` IO traits.
        let io = TokioIo::new(stream);
        let registry2 = registry.clone();
        // Spawn a tokio task to serve multiple connections concurrently
        tokio::task::spawn(async move {
            let svc_fn = |req| {
                let registry = registry2.clone();
                async move { prometheus_service_fn(&registry, req).await }
            };

            let result = http1::Builder::new()
                // `service_fn` converts our function in a `Service`
                .serve_connection(io, service_fn(svc_fn))
                .await;

            if let Err(err) = result {
                eprintln!("Error serving connection: {:?}", err);
            }
        });
    }
}

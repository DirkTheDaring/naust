//! End-to-end proof of the KI-01 criterion: a renewed certificate is served
//! WITHOUT a listener restart. Serves via the same axum-server `RustlsConfig`
//! mechanism the supervisor uses, swaps certificates through the TlsWatcher
//! tick, and verifies the handshake-visible certificate changed.

use naust::tls_manager::TlsWatcher;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;

fn write_cert(dir: &Path, name: &str) -> Vec<u8> {
    let k = rcgen::generate_simple_self_signed(vec![name.to_string()]).unwrap();
    std::fs::write(dir.join("cert.pem"), k.cert.pem()).unwrap();
    std::fs::write(dir.join("key.pem"), k.signing_key.serialize_pem()).unwrap();
    k.cert.der().to_vec()
}

/// TLS handshake against `addr` returning the leaf certificate DER.
fn handshake_peer_cert(addr: std::net::SocketAddr, server_name: &str) -> Vec<u8> {
    // Trust nothing, verify nothing — we only want to observe the presented leaf.
    #[derive(Debug)]
    struct NoVerify(rustls::crypto::CryptoProvider);
    impl rustls::client::danger::ServerCertVerifier for NoVerify {
        fn verify_server_cert(
            &self,
            _end_entity: &rustls::pki_types::CertificateDer<'_>,
            _intermediates: &[rustls::pki_types::CertificateDer<'_>],
            _server_name: &rustls::pki_types::ServerName<'_>,
            _ocsp: &[u8],
            _now: rustls::pki_types::UnixTime,
        ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
            Ok(rustls::client::danger::ServerCertVerified::assertion())
        }
        fn verify_tls12_signature(
            &self,
            message: &[u8],
            cert: &rustls::pki_types::CertificateDer<'_>,
            dss: &rustls::DigitallySignedStruct,
        ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
            rustls::crypto::verify_tls12_signature(
                message,
                cert,
                dss,
                &self.0.signature_verification_algorithms,
            )
        }
        fn verify_tls13_signature(
            &self,
            message: &[u8],
            cert: &rustls::pki_types::CertificateDer<'_>,
            dss: &rustls::DigitallySignedStruct,
        ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
            rustls::crypto::verify_tls13_signature(
                message,
                cert,
                dss,
                &self.0.signature_verification_algorithms,
            )
        }
        fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
            self.0.signature_verification_algorithms.supported_schemes()
        }
    }

    let provider = rustls::crypto::aws_lc_rs::default_provider();
    let config = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NoVerify(provider)))
        .with_no_client_auth();
    let server = rustls::pki_types::ServerName::try_from(server_name.to_string()).unwrap();
    let mut conn = rustls::ClientConnection::new(Arc::new(config), server).unwrap();
    let mut sock = std::net::TcpStream::connect(addr).unwrap();
    // Drive the handshake.
    while conn.is_handshaking() {
        if conn.wants_write() {
            conn.write_tls(&mut sock).unwrap();
        }
        if conn.is_handshaking() && conn.wants_read() {
            conn.read_tls(&mut sock).unwrap();
            conn.process_new_packets().unwrap();
        }
    }
    let der = conn.peer_certificates().unwrap()[0].as_ref().to_vec();
    // Politely close.
    conn.send_close_notify();
    let _ = conn.write_tls(&mut sock);
    let _ = sock.read(&mut [0u8; 1]);
    der
}

#[tokio::test]
async fn renewed_certificate_is_served_without_restart() {
    naust::install_rustls_crypto_provider();
    let dir = tempfile::tempdir().unwrap();
    let der_a = write_cert(dir.path(), "reg.example.com");

    let tls = axum_server::tls_rustls::RustlsConfig::from_pem_file(
        dir.path().join("cert.pem"),
        dir.path().join("key.pem"),
    )
    .await
    .unwrap();

    // Bind ONCE; the listener is never restarted below.
    let handle = axum_server::Handle::new();
    let app = axum::Router::new().route("/", axum::routing::get(|| async { "ok" }));
    let server = axum_server::bind_rustls("127.0.0.1:0".parse().unwrap(), tls.clone())
        .handle(handle.clone());
    tokio::spawn(async move {
        server.serve(app.into_make_service()).await.unwrap();
    });
    let addr = handle.listening().await.unwrap();

    let watcher = TlsWatcher::new(
        dir.path().join("cert.pem"),
        dir.path().join("key.pem"),
        Some(vec!["reg.example.com".to_string()]),
        None,
        tls,
    );

    // Baseline: cert A is served.
    let dir_path = dir.path().to_path_buf();
    let served = tokio::task::spawn_blocking(move || handshake_peer_cert(addr, "reg.example.com"))
        .await
        .unwrap();
    assert_eq!(served, der_a, "baseline must serve cert A");

    // Soak: five successive renewals, each picked up by a watcher tick with no rebind.
    let mut previous = der_a;
    for i in 0..5 {
        let der_new = write_cert(&dir_path, "reg.example.com");
        assert_ne!(der_new, previous, "renewal {i} must produce a new cert");
        watcher.tick_once().await.unwrap();
        let served =
            tokio::task::spawn_blocking(move || handshake_peer_cert(addr, "reg.example.com"))
                .await
                .unwrap();
        assert_eq!(
            served, der_new,
            "renewal {i}: renewed cert must be served without restart"
        );
        previous = der_new;
    }

    // A wrong-name "renewal" must be refused: the old cert keeps serving.
    let _der_bad = write_cert(&dir_path, "evil.example");
    watcher.tick_once().await.unwrap(); // refusal is swallowed + logged
    let served = tokio::task::spawn_blocking(move || handshake_peer_cert(addr, "reg.example.com"))
        .await
        .unwrap();
    assert_eq!(served, previous, "SAN-mismatched cert must NOT be served");

    handle.shutdown();
}

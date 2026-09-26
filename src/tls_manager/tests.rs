use super::*;

fn write_self_signed(dir: &Path, names: &[&str]) -> (PathBuf, PathBuf) {
    let key =
        rcgen::generate_simple_self_signed(names.iter().map(|s| s.to_string()).collect::<Vec<_>>())
            .expect("generate cert");
    let cert_path = dir.join("cert.pem");
    let key_path = dir.join("key.pem");
    std::fs::write(&cert_path, key.cert.pem()).unwrap();
    std::fs::write(&key_path, key.signing_key.serialize_pem()).unwrap();
    (cert_path, key_path)
}

#[test]
fn uncovered_names_exact_wildcard_and_case() {
    let sans = vec![
        "registry.example.com".to_string(),
        "*.mirror.example".to_string(),
    ];
    // Exact match, case-insensitive.
    assert!(uncovered_names(&sans, &["Registry.Example.COM".to_string()]).is_empty());
    // Wildcard covers exactly one label.
    assert!(uncovered_names(&sans, &["a.mirror.example".to_string()]).is_empty());
    // Wildcard does NOT cover multiple labels or the apex.
    assert_eq!(
        uncovered_names(&sans, &["a.b.mirror.example".to_string()]),
        vec!["a.b.mirror.example".to_string()]
    );
    assert_eq!(
        uncovered_names(&sans, &["mirror.example".to_string()]),
        vec!["mirror.example".to_string()]
    );
    // Unrelated name is uncovered.
    assert_eq!(
        uncovered_names(&sans, &["other.example".to_string()]),
        vec!["other.example".to_string()]
    );
}

#[test]
fn inspect_and_preflight() {
    let dir = tempfile::tempdir().unwrap();
    let (cert, _key) = write_self_signed(dir.path(), &["reg.example.com"]);

    let summary = inspect_cert_pem(&cert).expect("inspect");
    assert_eq!(summary.sans, vec!["reg.example.com".to_string()]);
    assert!(!summary.not_after.is_empty());

    // Covered names pass.
    preflight_startup(&cert, &["reg.example.com".to_string()], false).expect("covered");
    // Mismatch fails closed…
    let err = preflight_startup(&cert, &["other.example.com".to_string()], false).unwrap_err();
    assert!(err.contains("does not cover"), "{err}");
    // …unless break-glass is set.
    preflight_startup(&cert, &["other.example.com".to_string()], true).expect("break-glass");
}

#[test]
fn preflight_rejects_garbage_pem() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cert.pem");
    std::fs::write(&path, b"not a pem at all").unwrap();
    assert!(preflight_startup(&path, &[], false).is_err());
}

async fn run_tick(
    cert: &Path,
    expected: Option<&[String]>,
    last: &mut Option<[u8; 32]>,
    reloads: &std::sync::Arc<std::sync::atomic::AtomicUsize>,
) -> Result<bool, String> {
    let reloads = reloads.clone();
    tick(
        cert,
        expected,
        last,
        None::<fn() -> std::future::Ready<Result<(), String>>>,
        move || {
            let reloads = reloads.clone();
            async move {
                reloads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            }
        },
    )
    .await
}

#[tokio::test]
async fn tick_reloads_only_on_valid_change() {
    use std::sync::atomic::Ordering;
    let dir = tempfile::tempdir().unwrap();
    let (cert, _key) = write_self_signed(dir.path(), &["reg.example.com"]);
    let names = vec!["reg.example.com".to_string()];
    let reloads = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut last = fingerprint(&cert);

    // 1. No change -> no reload.
    assert!(
        !run_tick(&cert, Some(&names), &mut last, &reloads)
            .await
            .unwrap()
    );
    assert_eq!(reloads.load(Ordering::SeqCst), 0);

    // 2. Renewed cert with covering SANs -> exactly one reload.
    let renewed = rcgen::generate_simple_self_signed(vec!["reg.example.com".to_string()]).unwrap();
    std::fs::write(&cert, renewed.cert.pem()).unwrap();
    assert!(
        run_tick(&cert, Some(&names), &mut last, &reloads)
            .await
            .unwrap()
    );
    assert_eq!(reloads.load(Ordering::SeqCst), 1);

    // 3. Same content again -> fingerprint remembered, no second reload.
    assert!(
        !run_tick(&cert, Some(&names), &mut last, &reloads)
            .await
            .unwrap()
    );
    assert_eq!(reloads.load(Ordering::SeqCst), 1);

    // 4. "Renewed" cert with WRONG names -> refused, no reload, fingerprint
    //    not consumed (the refusal repeats on the next tick).
    let wrong = rcgen::generate_simple_self_signed(vec!["evil.example".to_string()]).unwrap();
    std::fs::write(&cert, wrong.cert.pem()).unwrap();
    let err = run_tick(&cert, Some(&names), &mut last, &reloads)
        .await
        .unwrap_err();
    assert!(err.contains("refusing reload"), "{err}");
    assert_eq!(reloads.load(Ordering::SeqCst), 1);
    let err2 = run_tick(&cert, Some(&names), &mut last, &reloads)
        .await
        .unwrap_err();
    assert!(err2.contains("refusing reload"), "{err2}");

    // 5. Garbage on disk -> refused before reload.
    std::fs::write(&cert, b"garbage").unwrap();
    assert!(
        run_tick(&cert, Some(&names), &mut last, &reloads)
            .await
            .is_err()
    );
    assert_eq!(reloads.load(Ordering::SeqCst), 1);

    // 6. Without expected names (external mode), any parseable cert reloads.
    let external =
        rcgen::generate_simple_self_signed(vec!["whatever.example".to_string()]).unwrap();
    std::fs::write(&cert, external.cert.pem()).unwrap();
    assert!(run_tick(&cert, None, &mut last, &reloads).await.unwrap());
    assert_eq!(reloads.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn tick_runs_renewal_before_checking_and_survives_renewal_failure() {
    let dir = tempfile::tempdir().unwrap();
    let (cert, _key) = write_self_signed(dir.path(), &["reg.example.com"]);
    let mut last = fingerprint(&cert);

    // Renewal callback rewrites the cert (simulating ACME) — the SAME tick
    // must pick the change up and reload.
    let cert_for_renew = cert.clone();
    let reloaded = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let r2 = reloaded.clone();
    let renewed = tick(
        &cert,
        None,
        &mut last,
        Some(move || async move {
            let k =
                rcgen::generate_simple_self_signed(vec!["reg.example.com".to_string()]).unwrap();
            std::fs::write(&cert_for_renew, k.cert.pem()).unwrap();
            Ok(())
        }),
        move || async move {
            r2.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        },
    )
    .await
    .unwrap();
    assert!(renewed);
    assert_eq!(reloaded.load(std::sync::atomic::Ordering::SeqCst), 1);

    // A failing renewal is swallowed (log-and-retry) and does not block the tick.
    let ok = tick(
        &cert,
        None,
        &mut last,
        Some(|| async { Err::<(), String>("upstream down".into()) }),
        || async { Ok(()) },
    )
    .await
    .unwrap();
    assert!(!ok);
}

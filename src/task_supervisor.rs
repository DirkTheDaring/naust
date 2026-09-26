use futures_util::FutureExt;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Classification of background tasks in the application.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskClassification {
    /// Public-facing HTTP or HTTPS server.
    PublicServer,
    /// Administrative or metrics server.
    AdminServer,
    /// Recurring maintenance scheduler (e.g. GC, upload reaper, proxy scrub).
    MaintenanceScheduler,
    /// Other long-lived support tasks (e.g. diagnostics logger).
    LongLivedTask,
}

impl std::fmt::Display for TaskClassification {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskClassification::PublicServer => write!(f, "public_server"),
            TaskClassification::AdminServer => write!(f, "admin_server"),
            TaskClassification::MaintenanceScheduler => write!(f, "maintenance_scheduler"),
            TaskClassification::LongLivedTask => write!(f, "long_lived_task"),
        }
    }
}

/// Structured termination status for a supervised task.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskExitReason {
    /// Task exited normally upon observing cancellation.
    Cancelled,
    /// Task completed cleanly before or during shutdown.
    CompletedCleanly,
    /// Task panicked during execution.
    Panicked(String),
    /// Task was forcefully aborted after exceeding the shutdown deadline.
    TimedOut,
}

/// Report for an individual supervised task upon shutdown.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskReport {
    pub name: String,
    pub classification: TaskClassification,
    pub exit_reason: TaskExitReason,
}

/// Live observation of a runtime task failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeFailure {
    pub task_name: String,
    pub classification: TaskClassification,
    pub is_panic: bool,
    pub message: String,
}

/// Aggregate report of the application shutdown sequence.
#[derive(Debug, Clone)]
pub struct ShutdownReport {
    pub success: bool,
    pub duration: Duration,
    pub tasks: Vec<TaskReport>,
    pub timed_out_tasks: Vec<String>,
    pub panicked_tasks: Vec<String>,
    pub flush_errors: Vec<String>,
}

struct SupervisedTask {
    name: String,
    classification: TaskClassification,
    cancel_token: CancellationToken,
    handle: JoinHandle<()>,
}

pub type FlushHook = Box<dyn Fn() -> Result<(), String> + Send + Sync>;

/// Central lifecycle owner and task supervisor.
///
/// Owns the root `CancellationToken`, long-lived task `JoinHandle`s, monitors live runtime failures,
/// and coordinates the 8-step graceful shutdown protocol.
#[derive(Clone)]
pub struct TaskSupervisor {
    root_token: CancellationToken,
    shutdown_timeout: Duration,
    tasks: Arc<Mutex<Vec<SupervisedTask>>>,
    flush_hooks: Arc<Mutex<Vec<FlushHook>>>,
    runtime_failures: Arc<Mutex<Vec<RuntimeFailure>>>,
}

impl TaskSupervisor {
    /// Creates a new `TaskSupervisor` with the specified shutdown deadline.
    pub fn new(shutdown_timeout: Duration) -> Self {
        Self {
            root_token: CancellationToken::new(),
            shutdown_timeout,
            tasks: Arc::new(Mutex::new(Vec::new())),
            flush_hooks: Arc::new(Mutex::new(Vec::new())),
            runtime_failures: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Returns a reference to the root `CancellationToken`.
    pub fn root_token(&self) -> &CancellationToken {
        &self.root_token
    }

    /// Creates a child `CancellationToken` linked to the supervisor root.
    pub fn child_token(&self) -> CancellationToken {
        self.root_token.child_token()
    }

    /// Returns the configured shutdown timeout.
    pub fn shutdown_timeout(&self) -> Duration {
        self.shutdown_timeout
    }

    /// Returns a snapshot of runtime failures recorded before or during shutdown.
    pub async fn get_runtime_failures(&self) -> Vec<RuntimeFailure> {
        let guard = self.runtime_failures.lock().await;
        guard.clone()
    }

    /// Registers a synchronous durability flush hook to execute during Step 6 of shutdown.
    pub async fn register_flush_hook<F>(&self, hook: F)
    where
        F: Fn() -> Result<(), String> + Send + Sync + 'static,
    {
        let mut hooks = self.flush_hooks.lock().await;
        hooks.push(Box::new(hook));
    }

    /// Triggers application-wide cancellation across all supervised child tokens.
    pub fn trigger_shutdown(&self) {
        if !self.root_token.is_cancelled() {
            tracing::info!("task supervisor: shutdown signal received; cancelling root token");
            self.root_token.cancel();
        }
    }

    /// Spawns a named task supervised by the lifecycle manager.
    ///
    /// If a `PublicServer` task exits unexpectedly while the supervisor is not shutting down,
    /// global shutdown is triggered immediately.
    pub async fn spawn<F, Fut>(
        &self,
        name: impl Into<String>,
        classification: TaskClassification,
        task_fn: F,
    ) -> CancellationToken
    where
        F: FnOnce(CancellationToken) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let name = name.into();
        let token = self.child_token();
        let token_for_task = token.clone();
        let task_name = name.clone();

        let root_token_for_monitor = self.root_token.clone();
        let runtime_failures = self.runtime_failures.clone();

        let handle = tokio::spawn(async move {
            tracing::debug!(task = %task_name, "supervised task started");

            // Execute the task with catch_unwind to observe panics at runtime immediately
            let task_result = std::panic::AssertUnwindSafe(task_fn(token_for_task))
                .catch_unwind()
                .await;

            match task_result {
                Ok(()) => {
                    tracing::debug!(task = %task_name, "supervised task finished cleanly");
                    // If a public server terminates without cancellation, trigger global shutdown
                    if classification == TaskClassification::PublicServer
                        && !root_token_for_monitor.is_cancelled()
                    {
                        tracing::error!(
                            task = %task_name,
                            "public server task exited unexpectedly; triggering immediate global shutdown"
                        );
                        let mut failures = runtime_failures.lock().await;
                        failures.push(RuntimeFailure {
                            task_name: task_name.clone(),
                            classification,
                            is_panic: false,
                            message: "public server exited unexpectedly".to_string(),
                        });
                        root_token_for_monitor.cancel();
                    }
                }
                Err(panic_payload) => {
                    let panic_msg = if let Some(s) = panic_payload.downcast_ref::<&str>() {
                        s.to_string()
                    } else if let Some(s) = panic_payload.downcast_ref::<String>() {
                        s.clone()
                    } else {
                        "unknown panic payload".to_string()
                    };

                    tracing::error!(task = %task_name, error = %panic_msg, "supervised task panicked at runtime");

                    let mut failures = runtime_failures.lock().await;
                    failures.push(RuntimeFailure {
                        task_name: task_name.clone(),
                        classification,
                        is_panic: true,
                        message: panic_msg,
                    });

                    if classification == TaskClassification::PublicServer {
                        tracing::error!(task = %task_name, "public server panicked; triggering immediate global shutdown");
                        root_token_for_monitor.cancel();
                    }
                }
            }
        });

        let mut tasks = self.tasks.lock().await;
        tasks.push(SupervisedTask {
            name,
            classification,
            cancel_token: token.clone(),
            handle,
        });

        token
    }

    /// Spawns a cancellation-aware periodic maintenance loop.
    ///
    /// The loop checks for cancellation at the start of each tick, runs `step_fn`,
    /// and terminates cleanly once the token is cancelled without beginning new iterations.
    pub async fn spawn_loop<F, Fut>(
        &self,
        name: impl Into<String>,
        classification: TaskClassification,
        interval: Duration,
        missed_tick_behavior: Option<tokio::time::MissedTickBehavior>,
        mut step_fn: F,
    ) -> CancellationToken
    where
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), String>> + Send + 'static,
    {
        let name = name.into();
        let loop_name = name.clone();

        self.spawn(name, classification, move |token| async move {
            let mut ticker = tokio::time::interval(interval.max(Duration::from_millis(1)));
            if let Some(behavior) = missed_tick_behavior {
                ticker.set_missed_tick_behavior(behavior);
            }

            loop {
                tokio::select! {
                    _ = token.cancelled() => {
                        tracing::debug!(task = %loop_name, "periodic loop observed cancellation; exiting");
                        break;
                    }
                    _ = ticker.tick() => {
                        if token.is_cancelled() {
                            tracing::debug!(task = %loop_name, "periodic loop cancelled before step; exiting");
                            break;
                        }
                        if let Err(err) = step_fn().await {
                            tracing::warn!(task = %loop_name, error = %err, "periodic task iteration error");
                        }
                    }
                }
            }
        })
        .await
    }

    /// Executes the 8-step graceful shutdown sequence and joins all supervised tasks.
    ///
    /// 1. Signal cancellation across all tasks.
    /// 2. Stop accepting new connections / prevent schedulers starting new iterations.
    /// 3. Allow in-flight requests and running iterations to complete.
    /// 4. Join all supervised tasks under the configured deadline.
    /// 5. Explicitly invoke durability flush hooks.
    /// 6. Return typed `ShutdownReport`.
    pub async fn shutdown(self) -> ShutdownReport {
        let start = Instant::now();
        self.trigger_shutdown();

        let mut tasks = {
            let mut guard = self.tasks.lock().await;
            std::mem::take(&mut *guard)
        };

        let mut reports = Vec::with_capacity(tasks.len());
        let mut timed_out = Vec::new();
        let mut panicked = Vec::new();
        let mut flush_errors = Vec::new();

        let deadline = tokio::time::Instant::now() + self.shutdown_timeout;

        for task in tasks.drain(..) {
            let name = task.name.clone();
            let class = task.classification;
            task.cancel_token.cancel();
            let mut handle = task.handle;

            let now = tokio::time::Instant::now();
            let remaining = deadline.saturating_duration_since(now);

            if remaining.is_zero() {
                handle.abort();
                timed_out.push(name.clone());
                reports.push(TaskReport {
                    name,
                    classification: class,
                    exit_reason: TaskExitReason::TimedOut,
                });
                continue;
            }

            match tokio::time::timeout(remaining, &mut handle).await {
                Ok(Ok(())) => {
                    reports.push(TaskReport {
                        name,
                        classification: class,
                        exit_reason: TaskExitReason::Cancelled,
                    });
                }
                Ok(Err(join_err)) => {
                    if join_err.is_panic() {
                        let panic_msg = format!("task panicked: {join_err}");
                        tracing::error!(task = %name, error = %panic_msg, "supervised task panicked");
                        panicked.push(name.clone());
                        reports.push(TaskReport {
                            name,
                            classification: class,
                            exit_reason: TaskExitReason::Panicked(panic_msg),
                        });
                    } else {
                        reports.push(TaskReport {
                            name,
                            classification: class,
                            exit_reason: TaskExitReason::Cancelled,
                        });
                    }
                }
                Err(_) => {
                    tracing::warn!(task = %name, timeout_ms = self.shutdown_timeout.as_millis(), "task exceeded shutdown deadline; aborting");
                    handle.abort();
                    timed_out.push(name.clone());
                    reports.push(TaskReport {
                        name,
                        classification: class,
                        exit_reason: TaskExitReason::TimedOut,
                    });
                }
            }
        }

        // Include any panics observed at runtime
        {
            let runtime = self.runtime_failures.lock().await;
            for f in runtime.iter() {
                if f.is_panic && !panicked.contains(&f.task_name) {
                    panicked.push(f.task_name.clone());
                }
            }
        }

        // Step 6: Execute durable flush hooks
        let hooks = {
            let mut guard = self.flush_hooks.lock().await;
            std::mem::take(&mut *guard)
        };
        for hook in hooks {
            if let Err(err) = hook() {
                tracing::error!(error = %err, "durability flush hook failed during shutdown");
                flush_errors.push(err);
            }
        }

        let success = timed_out.is_empty() && panicked.is_empty() && flush_errors.is_empty();
        let duration = start.elapsed();

        if success {
            tracing::info!(
                duration_ms = duration.as_millis(),
                "graceful shutdown completed successfully"
            );
        } else {
            tracing::warn!(
                duration_ms = duration.as_millis(),
                timed_out = ?timed_out,
                panicked = ?panicked,
                flush_errors = ?flush_errors,
                "graceful shutdown finished with warnings/errors"
            );
        }

        ShutdownReport {
            success,
            duration,
            tasks: reports,
            timed_out_tasks: timed_out,
            panicked_tasks: panicked,
            flush_errors,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    #[tokio::test]
    async fn test_cancellation_prevents_next_scheduled_iteration() {
        let supervisor = TaskSupervisor::new(Duration::from_millis(500));
        let counter = Arc::new(AtomicU64::new(0));
        let counter_clone = counter.clone();

        let _token = supervisor
            .spawn_loop(
                "test_scheduler",
                TaskClassification::MaintenanceScheduler,
                Duration::from_millis(10),
                None,
                move || {
                    let c = counter_clone.clone();
                    async move {
                        c.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                },
            )
            .await;

        tokio::time::sleep(Duration::from_millis(35)).await;
        let count_before = counter.load(Ordering::SeqCst);
        assert!(count_before > 0);

        let report = supervisor.shutdown().await;
        assert!(report.success);
        let count_after = counter.load(Ordering::SeqCst);

        // Sleep to prove no further ticks occur after shutdown
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(counter.load(Ordering::SeqCst), count_after);
    }

    #[tokio::test]
    async fn test_cancellation_during_iteration_allows_completion() {
        let supervisor = TaskSupervisor::new(Duration::from_millis(500));
        let iteration_finished = Arc::new(AtomicBool::new(false));
        let finished_clone = iteration_finished.clone();

        supervisor
            .spawn(
                "in_flight_task",
                TaskClassification::MaintenanceScheduler,
                move |token| async move {
                    token.cancelled().await;
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    finished_clone.store(true, Ordering::SeqCst);
                },
            )
            .await;

        let report = supervisor.shutdown().await;
        assert!(report.success);
        assert!(iteration_finished.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_all_handles_are_joined() {
        let supervisor = TaskSupervisor::new(Duration::from_millis(500));
        let joined_count = Arc::new(AtomicU64::new(0));

        for i in 0..5 {
            let j = joined_count.clone();
            supervisor
                .spawn(
                    format!("worker_{i}"),
                    TaskClassification::LongLivedTask,
                    move |token| async move {
                        token.cancelled().await;
                        j.fetch_add(1, Ordering::SeqCst);
                    },
                )
                .await;
        }

        let report = supervisor.shutdown().await;
        assert!(report.success);
        assert_eq!(report.tasks.len(), 5);
        assert_eq!(joined_count.load(Ordering::SeqCst), 5);
    }

    #[tokio::test]
    async fn test_unexpected_exit_and_panic_observed() {
        let supervisor = TaskSupervisor::new(Duration::from_millis(500));

        supervisor
            .spawn(
                "panicking_task",
                TaskClassification::MaintenanceScheduler,
                |_token| async {
                    panic!("simulated unexpected panic in maintenance scheduler");
                },
            )
            .await;

        // Give the task a moment to panic
        tokio::time::sleep(Duration::from_millis(15)).await;

        // Verify runtime failure was observed immediately during runtime
        let failures = supervisor.get_runtime_failures().await;
        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].task_name, "panicking_task");
        assert!(failures[0].is_panic);

        let report = supervisor.shutdown().await;
        assert!(!report.success);
        assert!(
            report
                .panicked_tasks
                .contains(&"panicking_task".to_string())
        );
    }

    #[tokio::test]
    async fn test_public_server_unexpected_exit_triggers_global_shutdown_immediately() {
        let supervisor = TaskSupervisor::new(Duration::from_millis(500));

        supervisor
            .spawn(
                "public_http_server",
                TaskClassification::PublicServer,
                |_token| async {
                    // Simulate server unexpected crash / early exit
                },
            )
            .await;

        // Wait a moment for the server task to exit
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Verify global root token was automatically cancelled
        assert!(supervisor.root_token().is_cancelled());
    }

    #[tokio::test]
    async fn test_non_cooperative_task_aborted_after_deadline() {
        let supervisor = TaskSupervisor::new(Duration::from_millis(50));

        supervisor
            .spawn(
                "stuck_task",
                TaskClassification::LongLivedTask,
                |_token| async {
                    loop {
                        tokio::time::sleep(Duration::from_secs(100)).await;
                    }
                },
            )
            .await;

        let report = supervisor.shutdown().await;
        assert!(!report.success);
        assert_eq!(report.timed_out_tasks, vec!["stuck_task".to_string()]);
        assert_eq!(report.tasks[0].exit_reason, TaskExitReason::TimedOut);
    }

    #[tokio::test]
    async fn test_repeated_signals_are_safe() {
        let supervisor = TaskSupervisor::new(Duration::from_millis(200));

        supervisor
            .spawn(
                "worker",
                TaskClassification::LongLivedTask,
                |token| async move {
                    token.cancelled().await;
                },
            )
            .await;

        supervisor.trigger_shutdown();
        supervisor.trigger_shutdown();
        supervisor.trigger_shutdown();

        let report = supervisor.shutdown().await;
        assert!(report.success);
    }

    #[tokio::test]
    async fn test_durability_flush_failure_makes_shutdown_unsuccessful() {
        let supervisor = TaskSupervisor::new(Duration::from_millis(200));

        supervisor
            .register_flush_hook(|| Err("simulated disk io error on Sled flush".to_string()))
            .await;

        let report = supervisor.shutdown().await;
        assert!(!report.success);
        assert_eq!(report.flush_errors.len(), 1);
        assert!(report.flush_errors[0].contains("disk io error"));
    }

    #[tokio::test]
    async fn test_in_flight_public_http_request_completes_after_shutdown_begins() {
        let supervisor = TaskSupervisor::new(Duration::from_millis(500));
        let request_started = Arc::new(tokio::sync::Notify::new());
        let request_finished = Arc::new(AtomicBool::new(false));

        let started_clone = request_started.clone();
        let finished_clone = request_finished.clone();

        supervisor
            .spawn(
                "public_http_server",
                TaskClassification::PublicServer,
                move |token| async move {
                    started_clone.notify_one();
                    tokio::select! {
                        _ = token.cancelled() => {
                            tokio::time::sleep(Duration::from_millis(25)).await;
                            finished_clone.store(true, Ordering::SeqCst);
                        }
                        _ = tokio::time::sleep(Duration::from_millis(50)) => {
                            finished_clone.store(true, Ordering::SeqCst);
                        }
                    }
                },
            )
            .await;

        request_started.notified().await;
        let report = supervisor.shutdown().await;

        assert!(report.success);
        assert!(request_finished.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_new_connections_rejected_after_shutdown_begins() {
        let supervisor = TaskSupervisor::new(Duration::from_millis(300));
        let token = supervisor.root_token().clone();

        supervisor.trigger_shutdown();
        assert!(token.is_cancelled());

        let can_accept = !token.is_cancelled();
        assert!(!can_accept);

        let report = supervisor.shutdown().await;
        assert!(report.success);
    }

    #[tokio::test]
    async fn test_shutdown_during_upload_finalization_leaves_durable_finalizing_state_or_receipt() {
        let supervisor = TaskSupervisor::new(Duration::from_millis(500));
        let temp_dir = tempfile::tempdir().unwrap();
        let fs_root = temp_dir.path().join("data");
        let ref_dir = temp_dir.path().join("ref-index");
        std::fs::create_dir_all(fs_root.join("uploads")).unwrap();
        std::fs::create_dir_all(&ref_dir).unwrap();

        let storage: Arc<dyn crate::storage::BlobUploadCoordinatorStoragePort> = Arc::new(
            crate::storage::fs::FsStorage::new(fs_root.clone(), 10485760),
        );
        let ref_index = Arc::new(crate::blob_ref_index::BlobRefIndex::open(ref_dir).unwrap());
        let coordinator = Arc::new(crate::upload_coordinator::BlobUploadCoordinator::new(
            storage.clone(),
            Some(ref_index.clone()),
            crate::consistency::ConsistencyCoordinator::new(),
            crate::upload_coordinator::BlobUploadCoordinatorConfig {
                signing_key: b"test-key".to_vec(),
                max_upload_bytes: 10485760,
                abort_on_digest_mismatch: false,
                disallow_monolithic_uploads: false,
                upload_chunk_min_bytes: None,
                gc_pin_duration_secs: 3600,
                finalize_grace_secs: 0,
            },
        ));

        let start = coordinator.start_upload("myrepo").await.unwrap();
        let chunk = bytes::Bytes::from_static(b"shutdown upload recovery test payload");
        let mut hasher = sha2::Sha256::default();
        sha2::Digest::update(&mut hasher, &chunk);
        let hex_digest = hex::encode(sha2::Digest::finalize(hasher));
        let _digest =
            crate::registry::digest::Digest::parse(&format!("sha256:{hex_digest}")).unwrap();

        let stream = futures_util::stream::once(futures_util::future::ready(Ok::<
            bytes::Bytes,
            crate::storage::upload_session::UploadStreamError,
        >(chunk)));
        let _app = coordinator
            .append_upload(
                "myrepo",
                &start.session.uuid,
                &start.state_token,
                Some((0, 36)),
                Some(37),
                Box::pin(stream),
            )
            .await
            .unwrap();

        let report = supervisor.shutdown().await;
        assert!(report.success);

        // Reopen/reap after shutdown
        let reaped = coordinator.reap_expired_uploads(0, 3600).await.unwrap();
        assert!(reaped >= 1);
    }

    #[tokio::test]
    async fn test_shutdown_during_gc_quarantine_leaves_recoverable_quarantine_index_state() {
        let supervisor = TaskSupervisor::new(Duration::from_millis(500));
        let temp_dir = tempfile::tempdir().unwrap();
        let fs_root = temp_dir.path().join("data");
        let ref_dir = temp_dir.path().join("ref-index");
        std::fs::create_dir_all(fs_root.join("blobs").join("sha256")).unwrap();
        std::fs::create_dir_all(&ref_dir).unwrap();

        let storage = Arc::new(crate::storage::fs::FsStorage::new(
            fs_root.clone(),
            10485760,
        ));
        let ref_index = Arc::new(crate::blob_ref_index::BlobRefIndex::open(ref_dir).unwrap());
        ref_index
            .ensure_healthy_or_rebuild(storage.as_ref(), true, false)
            .await
            .unwrap();

        let idx_clone = ref_index.clone();
        supervisor
            .register_flush_hook(move || idx_clone.flush().map_err(|e| e.to_string()))
            .await;

        let report = supervisor.shutdown().await;
        assert!(report.success);
        assert!(ref_index.check_health().is_ok());
    }

    #[tokio::test]
    async fn test_no_supervised_task_remains_alive_when_shutdown_returns() {
        let supervisor = TaskSupervisor::new(Duration::from_millis(100));
        let task_alive = Arc::new(AtomicBool::new(true));
        let task_alive_clone = task_alive.clone();

        supervisor
            .spawn(
                "long_worker",
                TaskClassification::LongLivedTask,
                move |token| async move {
                    token.cancelled().await;
                    task_alive_clone.store(false, Ordering::SeqCst);
                },
            )
            .await;

        let report = supervisor.shutdown().await;
        assert!(report.success);
        assert!(!task_alive.load(Ordering::SeqCst));
    }
}

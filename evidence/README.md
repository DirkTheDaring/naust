# Verification evidence (KI-20 convention)

Committed, per-revision run reports for conformance and live-MinIO
qualification. One directory per qualification event, named
`YYYY-MM-DD-<campaign>`, each containing a `RECORD.md` (revision, commands,
results) plus the JUnit/exit-code artifacts of the runs. Raw HTML reports and
registry logs stay in the gitignored `tests/compliance/results/`; this
directory holds the durable, reviewable digest.

# ADR-012: Product Naming — Naust

* **Status:** Accepted (2026-09-26)
* **Scope:** Product, repository, crate, binary, and packaging naming

## Decision

The product is named **Naust** (Old Norse: the boathouse where vessels are hauled ashore and stored — the storage house for containers). Verified free on crates.io (`naust`, `naust-core`) and without meaningful GitHub collisions at decision time.

Naming scheme:

| Artifact | Name |
|---|---|
| GitHub repository | `DirkTheDaring/naust` |
| Server crate + binary | `naust` (was `registry-rust`) |
| Primitives crate | `naust-core` (was `registry-core`) |
| systemd unit / packages | `naust.service`, RPM/DEB `naust` |
| Config/state dirs | `/etc/naust/`, `/var/lib/naust/` |
| License | MIT (whole workspace) |

Deliberately unchanged: `REGISTRY__…` environment variables and the `registry.core.toml`/`registry.auth.toml` config file names (they describe function, not brand); sibling crates `storage-core`/`storage-fs`/`storage-s3`/`acmecert-core` keep their names on GitHub and are renamed under the `naust-` prefix only if/when published to crates.io (their current names are too generic for that namespace).

## Historical documents

ADR-001…011, `docs/outdated/`, `plans/`, and `evidence/` retain the historical names `registry-rust`/`registry-core`; they describe the code as it was. Machine-specific `file:///home/...` links in old documents were rewritten to repo-relative paths (content-neutral privacy redaction, along with neutralizing personal hostnames/emails in examples).

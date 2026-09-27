NAME := naust
VERSION_FILE ?= $(shell if [ -f VERSION ]; then echo VERSION; elif [ -f version ]; then echo version; else echo ""; fi)
ifneq ($(VERSION_FILE),)
VERSION := $(shell tr -d '[:space:]' < $(VERSION_FILE))
else
VERSION := $(shell sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -n 1)
endif
RELEASE ?= 1

CONTAINER_ENGINE ?= $(shell (command -v podman >/dev/null 2>&1 && echo podman) || (command -v docker >/dev/null 2>&1 && echo docker) || echo podman)
IMAGE_NAME ?= naust
IMAGE_TAG ?= $(VERSION)

DEB_VERSION := $(VERSION)-$(RELEASE)
DEB_ARCH := $(shell (command -v dpkg >/dev/null 2>&1 && dpkg --print-architecture) || (uname -m | sed -e 's/x86_64/amd64/' -e 's/aarch64/arm64/' -e 's/armv7l/armhf/'))
DEB_MAINTAINER ?= $(shell sh -c 'n=$$(git config --get user.name 2>/dev/null || true); e=$$(git config --get user.email 2>/dev/null || true); if [ -n "$$n" ] && [ -n "$$e" ]; then printf "%s <%s>" "$$n" "$$e"; else printf "Registry Rust Maintainers <naust@example.com>"; fi')

TOPDIR := $(CURDIR)/dist/rpmbuild
SOURCES := $(TOPDIR)/SOURCES
SPECS := $(TOPDIR)/SPECS

SPEC := packaging/rpm/$(NAME).spec
TARBALL := $(SOURCES)/$(NAME)-$(VERSION).tar.gz

.PHONY: sync-version bump-version container image docker-build

.PHONY: conformance
conformance:
	tests/compliance/run.sh

# ADR-010 allowlist gate: core modules may import only core modules (incl. test code).
.PHONY: core-boundary
core-boundary:
	scripts/check-core-boundary.sh

# Inputs `make rpm` / `make deb` install, plus the local compose contract (ADR-017).
.PHONY: packaging-check
packaging-check:
	@set -euo pipefail; \
	missing=0; \
	for f in \
		packaging/systemd/naust.service \
		packaging/systemd/sysusers.d/naust.conf \
		packaging/systemd/tmpfiles.d/naust.conf \
		packaging/sysconfig/naust \
		packaging/deb/systemd/naust.service \
		packaging/deb/default/naust \
		packaging/deb/doc/naust.1 \
		packaging/deb/lintian/naust \
		packaging/config/registry.core.toml \
		packaging/config/registry.auth.toml \
		packaging/rpm/naust.spec \
		docs/operations.md \
		; do \
		if [ ! -f "$$f" ]; then echo "missing packaging input: $$f" >&2; missing=1; fi; \
	done; \
	[ "$$missing" -eq 0 ]; \
	grep -q '^LimitNOFILE=65536$$' packaging/systemd/naust.service; \
	grep -q '^LimitNOFILE=65536$$' packaging/deb/systemd/naust.service; \
	grep -q '^StartLimitBurst=5$$' packaging/systemd/naust.service; \
	grep -q '^StartLimitBurst=5$$' packaging/deb/systemd/naust.service; \
	grep -q '127.0.0.1:5000:5000' docker-compose.yml; \
	grep -q 'TOKEN_SIGNING_KEY:' docker-compose.yml; \
	if grep -E '^[[:space:]]*REGISTRY_PASSWORD:' docker-compose.yml >/dev/null; then \
		echo "docker-compose.yml must not set a default push password" >&2; \
		exit 1; \
	fi; \
	echo "packaging-check: ok"

# Stage sibling path-dependencies into vendor/ for container builds (KI-10).
# acmecert: crates/acmecert-core; storage-layer-rust: all three crates + the
# workspace manifest (the crates use workspace field inheritance).
.PHONY: vendor-sync
vendor-sync:
	@rm -rf vendor/acmecert vendor/storage-layer-rust vendor/naust-core
	@mkdir -p vendor/acmecert/crates vendor/storage-layer-rust/crates vendor/naust-core
	@cp -a ../acmecert/Cargo.toml vendor/acmecert/Cargo.toml
	@cp -a ../acmecert/crates/acmecert-core vendor/acmecert/crates/
	@cp -a ../acmecert/crates/acmecert vendor/acmecert/crates/
	@cp -a ../storage-layer-rust/Cargo.toml vendor/storage-layer-rust/Cargo.toml
	@cp -a ../storage-layer-rust/crates/storage-core vendor/storage-layer-rust/crates/
	@cp -a ../storage-layer-rust/crates/storage-fs vendor/storage-layer-rust/crates/
	@cp -a ../storage-layer-rust/crates/storage-s3 vendor/storage-layer-rust/crates/
	@cp -a ../naust-core/Cargo.toml ../naust-core/Cargo.lock ../naust-core/LICENSE vendor/naust-core/ 2>/dev/null || cp -a ../naust-core/Cargo.toml ../naust-core/LICENSE vendor/naust-core/
	@cp -a ../naust-core/src vendor/naust-core/src
	@cp -a ../naust-core/examples vendor/naust-core/examples
	@find vendor -name target -type d -prune -exec rm -rf {} + 2>/dev/null || true
	@echo "vendor/ refreshed from ../acmecert and ../storage-layer-rust"

.PHONY: rpm rpm-tarball rpm-dirs clean-rpm

.PHONY: rpmlint

.PHONY: rpm-container rpmlint-container

.PHONY: deb deb-dirs clean-deb

.PHONY: deb-container

rpm: sync-version rpm-dirs $(TARBALL)
	rpmbuild -bb $(SPEC) \
		--define "_topdir $(TOPDIR)" \
		--define "version_override $(VERSION)" \
		--define "release_override $(RELEASE)"
	@echo "RPM(s) written under: $(TOPDIR)/RPMS"

rpmlint: rpm
	@if ! command -v rpmlint >/dev/null 2>&1; then \
		echo "error: rpmlint not found (try: make rpmlint-container)" >&2; \
		exit 1; \
	fi
	rpmlint -c packaging/rpm/rpmlint.toml \
		$(TOPDIR)/SRPMS/$(NAME)-$(VERSION)-$(RELEASE)*.src.rpm \
		$(TOPDIR)/RPMS/*/$(NAME)-$(VERSION)-$(RELEASE)*.rpm

rpm-dirs:
	mkdir -p $(TOPDIR)/{BUILD,BUILDROOT,RPMS,SOURCES,SPECS,SRPMS}

$(TARBALL): rpm-dirs
	cargo build --release
	@rm -rf dist/rpmstage
	@mkdir -p dist/rpmstage/$(NAME)-$(VERSION)/bin
	@mkdir -p dist/rpmstage/$(NAME)-$(VERSION)/etc/naust
	@mkdir -p dist/rpmstage/$(NAME)-$(VERSION)/systemd
	@mkdir -p dist/rpmstage/$(NAME)-$(VERSION)/sysusers.d
	@mkdir -p dist/rpmstage/$(NAME)-$(VERSION)/tmpfiles.d
	@mkdir -p dist/rpmstage/$(NAME)-$(VERSION)/sysconfig
	@mkdir -p dist/rpmstage/$(NAME)-$(VERSION)/man
	@cp -a target/release/naust dist/rpmstage/$(NAME)-$(VERSION)/bin/naust
	@cp -a packaging/config/*.toml dist/rpmstage/$(NAME)-$(VERSION)/etc/naust/
	@cp -a packaging/systemd/naust.service dist/rpmstage/$(NAME)-$(VERSION)/systemd/naust.service
	@cp -a packaging/systemd/sysusers.d/naust.conf dist/rpmstage/$(NAME)-$(VERSION)/sysusers.d/naust.conf
	@cp -a packaging/systemd/tmpfiles.d/naust.conf dist/rpmstage/$(NAME)-$(VERSION)/tmpfiles.d/naust.conf
	@cp -a packaging/sysconfig/naust dist/rpmstage/$(NAME)-$(VERSION)/sysconfig/naust
	@cp -a README.md dist/rpmstage/$(NAME)-$(VERSION)/README.md
	@cp -a docs/operations.md dist/rpmstage/$(NAME)-$(VERSION)/operations.md
	@cp -a packaging/deb/doc/naust.1 dist/rpmstage/$(NAME)-$(VERSION)/man/naust.1
	@tar -C dist/rpmstage -czf $(TARBALL) $(NAME)-$(VERSION)
	@cp -a $(SPEC) $(SPECS)/$(NAME).spec

clean-rpm:
	rm -rf dist/rpmstage dist/rpmbuild

RPM_CONTAINER_FEDORA ?= 42
# 0 = rootless (default when possible)
# 1 = rootful (sudo podman)
# auto = choose rootful when Podman is rootless (workaround for glibc mmap/noexec issues)
RPM_CONTAINER_ROOTFUL ?= auto

rpm-container:
	@$(MAKE) sync-version
	@chmod +x packaging/docker/build-rpm-in-fedora.sh
	@rootful_mode="$(RPM_CONTAINER_ROOTFUL)"; \
	if [ "$$rootful_mode" = "auto" ]; then \
		if command -v podman >/dev/null 2>&1; then \
			if podman info --format '{{.Host.Security.Rootless}}' 2>/dev/null | grep -qi true; then \
				rootful_mode=1; \
			else \
				rootful_mode=0; \
			fi; \
		else \
			rootful_mode=0; \
		fi; \
	fi; \
	if [ "$$rootful_mode" = "1" ]; then \
		echo "rpm-container: using --rootful (sudo podman)"; \
		packaging/docker/build-rpm-in-fedora.sh --rootful $(RPM_CONTAINER_FEDORA); \
	else \
		echo "rpm-container: using rootless container engine"; \
		packaging/docker/build-rpm-in-fedora.sh $(RPM_CONTAINER_FEDORA); \
	fi

rpmlint-container:
	@$(MAKE) sync-version
	@chmod +x packaging/docker/build-rpm-in-fedora.sh
	@rootful_mode="$(RPM_CONTAINER_ROOTFUL)"; \
	if [ "$$rootful_mode" = "auto" ]; then \
		if command -v podman >/dev/null 2>&1; then \
			if podman info --format '{{.Host.Security.Rootless}}' 2>/dev/null | grep -qi true; then \
				rootful_mode=1; \
			else \
				rootful_mode=0; \
			fi; \
		else \
			rootful_mode=0; \
		fi; \
	fi; \
	if [ "$$rootful_mode" = "1" ]; then \
		echo "rpmlint-container: using --rootful (sudo podman)"; \
		packaging/docker/build-rpm-in-fedora.sh --rootful $(RPM_CONTAINER_FEDORA) -- rpmlint; \
	else \
		echo "rpmlint-container: using rootless container engine"; \
		packaging/docker/build-rpm-in-fedora.sh $(RPM_CONTAINER_FEDORA) -- rpmlint; \
	fi

DEB_STAGE := dist/debstage/$(NAME)_$(DEB_VERSION)_$(DEB_ARCH)
DEB_OUT := dist/$(NAME)_$(DEB_VERSION)_$(DEB_ARCH).deb

deb-dirs:
	mkdir -p dist
	mkdir -p dist/debstage

deb: deb-dirs
	@if ! command -v dpkg-deb >/dev/null 2>&1; then \
		echo "error: dpkg-deb not found (use packaging/docker/build-deb-in-debian.sh)" >&2; \
		exit 1; \
	fi
	@$(MAKE) sync-version
	cargo build --release
	@rm -rf $(DEB_STAGE)
	@mkdir -p $(DEB_STAGE)/DEBIAN
	@mkdir -p $(DEB_STAGE)/usr/bin
	@mkdir -p $(DEB_STAGE)/etc/naust
	@mkdir -p $(DEB_STAGE)/etc/default
	@mkdir -p $(DEB_STAGE)/usr/lib/systemd/system
	@mkdir -p $(DEB_STAGE)/usr/share/doc/naust
	@mkdir -p $(DEB_STAGE)/usr/share/man/man1
	@mkdir -p $(DEB_STAGE)/usr/share/lintian/overrides
	@install -m 0755 target/release/naust $(DEB_STAGE)/usr/bin/naust
	@if command -v strip >/dev/null 2>&1; then strip --strip-unneeded $(DEB_STAGE)/usr/bin/naust || true; fi
	@install -m 0644 packaging/config/registry.core.toml $(DEB_STAGE)/etc/naust/registry.core.toml
	@install -m 0644 packaging/config/registry.auth.toml $(DEB_STAGE)/etc/naust/registry.auth.toml
	@install -m 0644 packaging/deb/default/naust $(DEB_STAGE)/etc/default/naust
	@install -m 0644 packaging/deb/systemd/naust.service $(DEB_STAGE)/usr/lib/systemd/system/naust.service
	@install -m 0644 README.md $(DEB_STAGE)/usr/share/doc/naust/README.md
	@install -m 0644 docs/operations.md $(DEB_STAGE)/usr/share/doc/naust/operations.md
	@install -m 0644 packaging/deb/doc/copyright $(DEB_STAGE)/usr/share/doc/naust/copyright
	@install -m 0644 packaging/deb/doc/changelog.Debian $(DEB_STAGE)/usr/share/doc/naust/changelog.Debian
	@if command -v gzip >/dev/null 2>&1; then gzip -9n -f $(DEB_STAGE)/usr/share/doc/naust/changelog.Debian; fi
	@install -m 0644 packaging/deb/doc/naust.1 $(DEB_STAGE)/usr/share/man/man1/naust.1
	@if command -v gzip >/dev/null 2>&1; then gzip -9n -f $(DEB_STAGE)/usr/share/man/man1/naust.1; fi
	@install -m 0644 packaging/deb/lintian/naust $(DEB_STAGE)/usr/share/lintian/overrides/naust
	@installed_size=$$(du -sk --exclude=DEBIAN $(DEB_STAGE) 2>/dev/null | awk '{print $$1}'); \
	if [ -z "$$installed_size" ]; then installed_size=$$(du -sk $(DEB_STAGE) | awk '{print $$1}'); fi; \
	sed \
		-e 's/@VERSION@/$(DEB_VERSION)/g' \
		-e 's/@ARCH@/$(DEB_ARCH)/g' \
		-e 's/@MAINTAINER@/$(DEB_MAINTAINER)/g' \
		-e "s/@INSTALLED_SIZE@/$$installed_size/g" \
		packaging/deb/DEBIAN/control.in > $(DEB_STAGE)/DEBIAN/control
	@install -m 0644 packaging/deb/DEBIAN/conffiles $(DEB_STAGE)/DEBIAN/conffiles
	@install -m 0755 packaging/deb/DEBIAN/postinst $(DEB_STAGE)/DEBIAN/postinst
	@install -m 0755 packaging/deb/DEBIAN/prerm $(DEB_STAGE)/DEBIAN/prerm
	@install -m 0755 packaging/deb/DEBIAN/postrm $(DEB_STAGE)/DEBIAN/postrm
	@cd $(DEB_STAGE) && find . -type f ! -path './DEBIAN/*' -print0 | xargs -0 md5sum > DEBIAN/md5sums
	@dpkg-deb --root-owner-group --build $(DEB_STAGE) $(DEB_OUT)
	@echo "DEB written to: $(DEB_OUT)"

clean-deb:
	rm -rf dist/debstage dist/*.deb

DEB_CONTAINER_SUITE ?= trixie
# 0 = rootless (default when possible)
# 1 = rootful (sudo podman)
# auto = choose rootful when Podman is rootless (workaround for glibc mmap/noexec issues)
DEB_CONTAINER_ROOTFUL ?= auto

deb-container:
	@$(MAKE) sync-version
	@chmod +x packaging/docker/build-deb-in-debian.sh
	@rootful_mode="$(DEB_CONTAINER_ROOTFUL)"; \
	if [ "$$rootful_mode" = "auto" ]; then \
		if command -v podman >/dev/null 2>&1; then \
			if podman info --format '{{.Host.Security.Rootless}}' 2>/dev/null | grep -qi true; then \
				rootful_mode=1; \
			else \
				rootful_mode=0; \
			fi; \
		else \
			rootful_mode=0; \
		fi; \
	fi; \
	if [ "$$rootful_mode" = "1" ]; then \
		echo "deb-container: using --rootful (sudo podman)"; \
		packaging/docker/build-deb-in-debian.sh --rootful $(DEB_CONTAINER_SUITE); \
	else \
		echo "deb-container: using rootless container engine"; \
		packaging/docker/build-deb-in-debian.sh $(DEB_CONTAINER_SUITE); \
	fi

container: sync-version
	$(CONTAINER_ENGINE) build -t $(IMAGE_NAME):$(IMAGE_TAG) -t $(IMAGE_NAME):latest .

image: container

docker-build: container

sync-version:
	@set -euo pipefail; \
	ver="$(VERSION)"; rel="$(RELEASE)"; debver="$(DEB_VERSION)"; maint="$(DEB_MAINTAINER)"; \
	cargo_ver="$$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -n 1)"; \
	if [ "$$cargo_ver" != "$$ver" ]; then \
		echo "Updating Cargo.toml version: $$cargo_ver -> $$ver"; \
		tmp="$$(mktemp)"; \
		awk -v new_ver="$$ver" 'BEGIN{in_pkg=0; done=0} /^\[package\][[:space:]]*$$/{in_pkg=1; print; next} /^\[[^]]+\][[:space:]]*$$/ && $$0 !~ /^\[package\][[:space:]]*$$/{in_pkg=0; print; next} in_pkg && !done && $$0 ~ /^version[[:space:]]*=[[:space:]]*"[^"]+"[[:space:]]*$$/{print "version = \"" new_ver "\""; done=1; next} {print} END{if(!done) exit 3}' Cargo.toml > "$$tmp"; \
		mv "$$tmp" Cargo.toml; \
	fi; \
	spec_file="$(SPEC)"; \
	if [ -f "$$spec_file" ]; then \
		sed -i -E "s/^(Version:[[:space:]]+%\{\?version_override\}%\{!\?version_override:)[0-9][0-9A-Za-z\._-]*(\})/\1$${ver}\2/" "$$spec_file"; \
		if ! awk -v want="$${ver}-$${rel}%{?dist}" 'BEGIN{in_section=0; found=0} /^%changelog[[:space:]]*$$/{in_section=1; next} in_section { if($$0 ~ /^[[:space:]]*$$/) next; if($$0 ~ /^\* / && index($$0, want)>0) found=1; exit } END{exit found?0:1}' "$$spec_file"; then \
			today="$$(LC_ALL=C date '+%a %b %d %Y')"; \
			tmp="$$(mktemp)"; \
			awk -v header="* $$today $(NAME) packaging - $${ver}-$${rel}%{?dist}" -v body="- Bump version to $${ver}" '{print; if($$0 ~ /^%changelog[[:space:]]*$$/){print header; print body; print ""}}' "$$spec_file" > "$$tmp"; \
			mv "$$tmp" "$$spec_file"; \
		fi; \
	fi; \
	deb_changelog="packaging/deb/doc/changelog.Debian"; \
	first="$$(head -n 1 "$$deb_changelog" 2>/dev/null || true)"; \
	if ! printf "%s" "$$first" | grep -q "($(DEB_VERSION))"; then \
		tmp="$$(mktemp)"; \
		{ \
			echo "$(NAME) ($${debver}) unstable; urgency=medium"; \
			echo; \
			echo "  * Bump version to $${ver}."; \
			echo; \
			echo " -- $${maint}  $$(LC_ALL=C date -R)"; \
			echo; \
			if [ -f "$$deb_changelog" ]; then cat "$$deb_changelog"; fi; \
		} > "$$tmp"; \
		mv "$$tmp" "$$deb_changelog"; \
	fi

# Bump Cargo.toml package version and propagate it into packaging metadata.
#
# Usage:
#   make bump-version NEW=0.2.0
bump-version:
	@if [ -z "$(NEW)" ]; then \
		echo "usage: make bump-version NEW=x.y.z" >&2; \
		exit 2; \
	fi
	@set -euo pipefail; \
	new_ver="$(NEW)"; \
	if [ -f VERSION ]; then \
		echo "$$new_ver" > VERSION; \
	elif [ -f version ]; then \
		echo "$$new_ver" > version; \
	else \
		echo "$$new_ver" > VERSION; \
	fi; \
	$(MAKE) sync-version VERSION="$$new_ver"

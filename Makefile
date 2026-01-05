NAME := registry-rust
VERSION := $(shell sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -n 1)
RELEASE ?= 1

DEB_VERSION := $(VERSION)-$(RELEASE)
DEB_ARCH := $(shell (command -v dpkg >/dev/null 2>&1 && dpkg --print-architecture) || (uname -m | sed -e 's/x86_64/amd64/' -e 's/aarch64/arm64/' -e 's/armv7l/armhf/'))
DEB_MAINTAINER ?= $(shell sh -c 'n=$$(git config --get user.name 2>/dev/null || true); e=$$(git config --get user.email 2>/dev/null || true); if [ -n "$$n" ] && [ -n "$$e" ]; then printf "%s <%s>" "$$n" "$$e"; else printf "Registry Rust Maintainers <registry-rust@example.com>"; fi')

TOPDIR := $(CURDIR)/dist/rpmbuild
SOURCES := $(TOPDIR)/SOURCES
SPECS := $(TOPDIR)/SPECS

SPEC := packaging/rpm/$(NAME).spec
TARBALL := $(SOURCES)/$(NAME)-$(VERSION).tar.gz

.PHONY: rpm rpm-tarball rpm-dirs clean-rpm

.PHONY: deb deb-dirs clean-deb

.PHONY: deb-container

rpm: rpm-dirs $(TARBALL)
	rpmbuild -bb $(SPEC) \
		--define "_topdir $(TOPDIR)" \
		--define "version_override $(VERSION)" \
		--define "release_override $(RELEASE)"
	@echo "RPM(s) written under: $(TOPDIR)/RPMS"

rpm-dirs:
	mkdir -p $(TOPDIR)/{BUILD,BUILDROOT,RPMS,SOURCES,SPECS,SRPMS}

$(TARBALL): rpm-dirs
	cargo build --release
	@rm -rf dist/rpmstage
	@mkdir -p dist/rpmstage/$(NAME)-$(VERSION)/bin
	@mkdir -p dist/rpmstage/$(NAME)-$(VERSION)/etc/registry-rust
	@mkdir -p dist/rpmstage/$(NAME)-$(VERSION)/systemd
	@mkdir -p dist/rpmstage/$(NAME)-$(VERSION)/sysusers.d
	@mkdir -p dist/rpmstage/$(NAME)-$(VERSION)/tmpfiles.d
	@mkdir -p dist/rpmstage/$(NAME)-$(VERSION)/sysconfig
	@cp -a target/release/registry-rust dist/rpmstage/$(NAME)-$(VERSION)/bin/registry-rust
	@cp -a etc/registry-rust/*.toml dist/rpmstage/$(NAME)-$(VERSION)/etc/registry-rust/
	@cp -a packaging/systemd/registry-rust.service dist/rpmstage/$(NAME)-$(VERSION)/systemd/registry-rust.service
	@cp -a packaging/systemd/sysusers.d/registry-rust.conf dist/rpmstage/$(NAME)-$(VERSION)/sysusers.d/registry-rust.conf
	@cp -a packaging/systemd/tmpfiles.d/registry-rust.conf dist/rpmstage/$(NAME)-$(VERSION)/tmpfiles.d/registry-rust.conf
	@cp -a packaging/sysconfig/registry-rust dist/rpmstage/$(NAME)-$(VERSION)/sysconfig/registry-rust
	@cp -a README.md dist/rpmstage/$(NAME)-$(VERSION)/README.md
	@tar -C dist/rpmstage -czf $(TARBALL) $(NAME)-$(VERSION)
	@cp -a $(SPEC) $(SPECS)/$(NAME).spec

clean-rpm:
	rm -rf dist/rpmstage dist/rpmbuild

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
	cargo build --release
	@rm -rf $(DEB_STAGE)
	@mkdir -p $(DEB_STAGE)/DEBIAN
	@mkdir -p $(DEB_STAGE)/usr/bin
	@mkdir -p $(DEB_STAGE)/etc/registry-rust
	@mkdir -p $(DEB_STAGE)/etc/default
	@mkdir -p $(DEB_STAGE)/usr/lib/systemd/system
	@mkdir -p $(DEB_STAGE)/usr/share/doc/registry-rust
	@mkdir -p $(DEB_STAGE)/usr/share/man/man1
	@mkdir -p $(DEB_STAGE)/usr/share/lintian/overrides
	@install -m 0755 target/release/registry-rust $(DEB_STAGE)/usr/bin/registry-rust
	@if command -v strip >/dev/null 2>&1; then strip --strip-unneeded $(DEB_STAGE)/usr/bin/registry-rust || true; fi
	@install -m 0644 etc/registry-rust/registry.core.toml $(DEB_STAGE)/etc/registry-rust/registry.core.toml
	@install -m 0644 etc/registry-rust/registry.auth.toml $(DEB_STAGE)/etc/registry-rust/registry.auth.toml
	@install -m 0644 packaging/deb/default/registry-rust $(DEB_STAGE)/etc/default/registry-rust
	@install -m 0644 packaging/deb/systemd/registry-rust.service $(DEB_STAGE)/usr/lib/systemd/system/registry-rust.service
	@install -m 0644 README.md $(DEB_STAGE)/usr/share/doc/registry-rust/README.md
	@install -m 0644 packaging/deb/doc/copyright $(DEB_STAGE)/usr/share/doc/registry-rust/copyright
	@printf "registry-rust (%s) UNRELEASED; urgency=medium\n\n  * Initial package.\n\n -- %s  %s\n" \
		"$(DEB_VERSION)" "$(DEB_MAINTAINER)" "$$(date -R)" \
		> $(DEB_STAGE)/usr/share/doc/registry-rust/changelog.Debian
	@if command -v gzip >/dev/null 2>&1; then gzip -9n -f $(DEB_STAGE)/usr/share/doc/registry-rust/changelog.Debian; fi
	@install -m 0644 packaging/deb/doc/registry-rust.1 $(DEB_STAGE)/usr/share/man/man1/registry-rust.1
	@if command -v gzip >/dev/null 2>&1; then gzip -9n -f $(DEB_STAGE)/usr/share/man/man1/registry-rust.1; fi
	@install -m 0644 packaging/deb/lintian/registry-rust $(DEB_STAGE)/usr/share/lintian/overrides/registry-rust
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

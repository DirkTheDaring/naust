NAME := registry-rust
VERSION := $(shell sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -n 1)
RELEASE ?= 1

TOPDIR := $(CURDIR)/dist/rpmbuild
SOURCES := $(TOPDIR)/SOURCES
SPECS := $(TOPDIR)/SPECS

SPEC := packaging/rpm/$(NAME).spec
TARBALL := $(SOURCES)/$(NAME)-$(VERSION).tar.gz

.PHONY: rpm rpm-tarball rpm-dirs clean-rpm

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

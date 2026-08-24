Name:           registry-rust
Version:        %{?version_override}%{!?version_override:0.8.16}
Release:        %{?release_override}%{!?release_override:1}%{?dist}
Summary:        Minimal Docker/OCI registry (Distribution v2 compatible) in Rust

# We package a prebuilt release binary (built outside of rpmbuild). On Fedora/RHEL,
# automatic debuginfo/debugsource package generation can fail when the binary has
# no DWARF symbols.
%global debug_package %{nil}

License:        LicenseRef-Proprietary
URL:            https://example.invalid/registry-rust
Source0:        https://example.invalid/registry-rust/releases/download/v%{version}/%{name}-%{version}.tar.gz

BuildRequires:  systemd-rpm-macros
Requires:       ca-certificates
%{?systemd_requires}

# sysusers is executed in %%pre; ensure systemd is present at install time.
Requires(pre):  systemd

%description
registry-rust is a minimal Docker/OCI registry implementation in Rust.

It also provides maintenance subcommands via the same binary, e.g.
`registry-rust ref-index ...` and `registry-rust blob-gc ...`.

%prep
%setup -q

%build
# This spec packages the already-built binary produced by `make rpm`.

%install
rm -rf %{buildroot}

install -D -m 0755 bin/registry-rust %{buildroot}%{_bindir}/registry-rust
strip --strip-unneeded %{buildroot}%{_bindir}/registry-rust || :

install -d %{buildroot}%{_sysconfdir}/registry-rust
cp -a etc/registry-rust/registry.core.toml %{buildroot}%{_sysconfdir}/registry-rust/
cp -a etc/registry-rust/registry.auth.toml %{buildroot}%{_sysconfdir}/registry-rust/

install -D -m 0644 systemd/registry-rust.service %{buildroot}%{_unitdir}/registry-rust.service

install -D -m 0644 sysusers.d/registry-rust.conf %{buildroot}%{_sysusersdir}/registry-rust.conf
install -D -m 0644 tmpfiles.d/registry-rust.conf %{buildroot}%{_tmpfilesdir}/registry-rust.conf

install -D -m 0644 sysconfig/registry-rust %{buildroot}%{_sysconfdir}/sysconfig/registry-rust

install -D -m 0644 README.md %{buildroot}%{_docdir}/%{name}/README.md
install -D -m 0644 blob-gc.md %{buildroot}%{_docdir}/%{name}/blob-gc.md
install -D -m 0644 man/registry-rust.1 %{buildroot}%{_mandir}/man1/registry-rust.1

%files
%doc %{_docdir}/%{name}/README.md
%doc %{_docdir}/%{name}/blob-gc.md
%{_bindir}/registry-rust
%config(noreplace) %{_sysconfdir}/registry-rust/registry.core.toml
%config(noreplace) %{_sysconfdir}/registry-rust/registry.auth.toml
%config(noreplace) %{_sysconfdir}/sysconfig/registry-rust
%{_unitdir}/registry-rust.service
%{_sysusersdir}/registry-rust.conf
%{_tmpfilesdir}/registry-rust.conf
%{_mandir}/man1/registry-rust.1*
%ghost %dir %attr(0755,registry,registry) %{_localstatedir}/lib/registry-rust

%pre
%sysusers_create %{_sysusersdir}/registry-rust.conf

%post
%tmpfiles_create %{_tmpfilesdir}/registry-rust.conf
%systemd_post registry-rust.service

%preun
%systemd_preun registry-rust.service

%postun
%systemd_postun_with_restart registry-rust.service

%changelog
* Mon Aug 24 2026 registry-rust packaging - 0.8.16-1%{?dist}
- Bump version to 0.8.16

* Mon Aug 24 2026 registry-rust packaging - 0.8.15-1%{?dist}
- Bump version to 0.8.15

* Mon Aug 24 2026 registry-rust packaging - 0.8.14-1%{?dist}
- Bump version to 0.8.14

* Mon Aug 24 2026 registry-rust packaging - 0.8.13-1%{?dist}
- Bump version to 0.8.13

* Mon Aug 24 2026 registry-rust packaging - 0.8.12-1%{?dist}
- Bump version to 0.8.12

* Mon Aug 24 2026 registry-rust packaging - 0.8.11-1%{?dist}
- Bump version to 0.8.11

* Mon Aug 24 2026 registry-rust packaging - 0.8.10-1%{?dist}
- Bump version to 0.8.10

* Mon Aug 24 2026 registry-rust packaging - 0.8.9-1%{?dist}
- Bump version to 0.8.9

* Mon Aug 24 2026 registry-rust packaging - 0.8.8-1%{?dist}
- Bump version to 0.8.8

* Mon Aug 24 2026 registry-rust packaging - 0.8.7-1%{?dist}
- Bump version to 0.8.7

* Mon Aug 24 2026 registry-rust packaging - 0.8.6-1%{?dist}
- Bump version to 0.8.6

* Mon Aug 24 2026 registry-rust packaging - 0.8.5-1%{?dist}
- Bump version to 0.8.5

* Mon Aug 24 2026 registry-rust packaging - 0.8.4-1%{?dist}
- Bump version to 0.8.4

* Mon Aug 24 2026 registry-rust packaging - 0.8.3-1%{?dist}
- Bump version to 0.8.3

* Mon Aug 24 2026 registry-rust packaging - 0.8.2-1%{?dist}
- Bump version to 0.8.2

* Mon Aug 24 2026 registry-rust packaging - 0.8.1-1%{?dist}
- Bump version to 0.8.1

* Mon Aug 24 2026 registry-rust packaging - 0.8.0-1%{?dist}
- Bump version to 0.8.0

* Mon Aug 24 2026 registry-rust packaging - 0.7.5-1%{?dist}
- Bump version to 0.7.5

* Mon Aug 24 2026 registry-rust packaging - 0.7.4-1%{?dist}
- Bump version to 0.7.4

* Mon Aug 24 2026 registry-rust packaging - 0.7.3-1%{?dist}
- Bump version to 0.7.3

* Mon Aug 24 2026 registry-rust packaging - 0.7.2-1%{?dist}
- Bump version to 0.7.2

* Mon Aug 24 2026 registry-rust packaging - 0.7.1-1%{?dist}
- Bump version to 0.7.1

* Mon Aug 24 2026 registry-rust packaging - 0.7.0-1%{?dist}
- Bump version to 0.7.0

* Mon Apr 06 2026 registry-rust packaging - 0.6.11-1%{?dist}
- Bump version to 0.6.11

* Mon Apr 06 2026 registry-rust packaging - 0.6.10-1%{?dist}
- Bump version to 0.6.10

* Mon Apr 06 2026 registry-rust packaging - 0.6.9-1%{?dist}
- Bump version to 0.6.9

* Mon Apr 06 2026 registry-rust packaging - 0.6.8-1%{?dist}
- Bump version to 0.6.8

* Mon Apr 06 2026 registry-rust packaging - 0.6.7-1%{?dist}
- Bump version to 0.6.7

* Mon Apr 06 2026 registry-rust packaging - 0.6.6-1%{?dist}
- Bump version to 0.6.6

* Sun Jan 11 2026 registry-rust packaging - 0.6.5-1%{?dist}
- Bump version to 0.6.5

* Sun Jan 11 2026 registry-rust packaging - 0.6.4-1%{?dist}
- Bump version to 0.6.4

* Sun Jan 11 2026 registry-rust packaging - 0.6.3-1%{?dist}
- Bump version to 0.6.3

* Sat Jan 10 2026 registry-rust packaging - 0.6.2-1%{?dist}
- Bump version to 0.6.2

* Tue Jan 06 2026 registry-rust packaging - 0.6.1-1%{?dist}
- Bump version to 0.6.1

* Tue Jan 06 2026 registry-rust packaging - 0.6.0-1%{?dist}
- Bump version to 0.6.0

* Mon Jan 05 2026 registry-rust packaging - 0.5.0-1%{?dist}
- Bump version to 0.5.0

* Mon Jan 05 2026 registry-rust packaging - 0.1.0-1%{?dist}
- Initial RPM packaging for local builds

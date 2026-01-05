Name:           registry-rust
Version:        %{?version_override}%{!?version_override:0.1.0}
Release:        %{?release_override}%{!?release_override:1}%{?dist}
Summary:        Minimal Docker/OCI registry (Distribution v2-ish) in Rust

# We package a prebuilt release binary (built outside of rpmbuild). On Fedora/RHEL,
# automatic debuginfo/debugsource package generation can fail when the binary has
# no DWARF symbols.
%global debug_package %{nil}

License:        Proprietary
URL:            https://example.invalid/registry-rust
Source0:        %{name}-%{version}.tar.gz

BuildRequires:  systemd-rpm-macros
Requires:       ca-certificates
%{?systemd_requires}

# sysusers is executed in %pre; ensure systemd is present at install time.
Requires(pre):  systemd

%description
registry-rust is a minimal Docker/OCI registry implementation in Rust.

%prep
%setup -q

%build
# This spec packages the already-built binary produced by `make rpm`.

%install
rm -rf %{buildroot}

install -D -m 0755 bin/registry-rust %{buildroot}%{_bindir}/registry-rust

install -d %{buildroot}%{_sysconfdir}/registry-rust
cp -a etc/registry-rust/registry.core.toml %{buildroot}%{_sysconfdir}/registry-rust/
cp -a etc/registry-rust/registry.auth.toml %{buildroot}%{_sysconfdir}/registry-rust/

install -D -m 0644 systemd/registry-rust.service %{buildroot}%{_unitdir}/registry-rust.service

install -D -m 0644 sysusers.d/registry-rust.conf %{buildroot}%{_sysusersdir}/registry-rust.conf
install -D -m 0644 tmpfiles.d/registry-rust.conf %{buildroot}%{_tmpfilesdir}/registry-rust.conf

install -D -m 0644 sysconfig/registry-rust %{buildroot}%{_sysconfdir}/sysconfig/registry-rust

install -D -m 0644 README.md %{buildroot}%{_docdir}/%{name}/README.md

%files
%doc %{_docdir}/%{name}/README.md
%{_bindir}/registry-rust
%config(noreplace) %{_sysconfdir}/registry-rust/registry.core.toml
%config(noreplace) %{_sysconfdir}/registry-rust/registry.auth.toml
%config(noreplace) %{_sysconfdir}/sysconfig/registry-rust
%{_unitdir}/registry-rust.service
%{_sysusersdir}/registry-rust.conf
%{_tmpfilesdir}/registry-rust.conf

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
* Mon Jan 05 2026 registry-rust packaging
- Initial RPM packaging for local builds

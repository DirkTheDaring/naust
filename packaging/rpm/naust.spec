Name:           naust
Version:        %{?version_override}%{!?version_override:0.9.0}
Release:        %{?release_override}%{!?release_override:1}%{?dist}
Summary:        Minimal Docker/OCI registry (Distribution v2 compatible) in Rust

# We package a prebuilt release binary (built outside of rpmbuild). On Fedora/RHEL,
# automatic debuginfo/debugsource package generation can fail when the binary has
# no DWARF symbols.
%global debug_package %{nil}

License:        MIT
URL:            https://github.com/DirkTheDaring/naust
# Local source archive produced by `make rpm` (binary + packaging inputs).
Source0:        %{name}-%{version}.tar.gz

BuildRequires:  systemd-rpm-macros
Requires:       ca-certificates
%{?systemd_requires}

# sysusers is executed in %%pre; ensure systemd is present at install time.
Requires(pre):  systemd

%description
naust is a minimal Docker/OCI registry implementation in Rust.

It also provides maintenance commands via the same binary, for example
`naust ref-index` and blob garbage collection.

%prep
%setup -q

%build
# This spec packages the already-built binary produced by `make rpm`.

%install
rm -rf %{buildroot}

install -D -m 0755 bin/naust %{buildroot}%{_bindir}/naust
strip --strip-unneeded %{buildroot}%{_bindir}/naust || :

install -d %{buildroot}%{_sysconfdir}/naust
cp -a etc/naust/registry.core.toml %{buildroot}%{_sysconfdir}/naust/
cp -a etc/naust/registry.auth.toml %{buildroot}%{_sysconfdir}/naust/

install -D -m 0644 systemd/naust.service %{buildroot}%{_unitdir}/naust.service

install -D -m 0644 sysusers.d/naust.conf %{buildroot}%{_sysusersdir}/naust.conf
install -D -m 0644 tmpfiles.d/naust.conf %{buildroot}%{_tmpfilesdir}/naust.conf

install -D -m 0644 sysconfig/naust %{buildroot}%{_sysconfdir}/sysconfig/naust

install -D -m 0644 README.md %{buildroot}%{_docdir}/%{name}/README.md
install -D -m 0644 operations.md %{buildroot}%{_docdir}/%{name}/operations.md
install -D -m 0644 man/naust.1 %{buildroot}%{_mandir}/man1/naust.1

%files
%doc %{_docdir}/%{name}/README.md
%doc %{_docdir}/%{name}/operations.md
%{_bindir}/naust
%config(noreplace) %{_sysconfdir}/naust/registry.core.toml
%config(noreplace) %{_sysconfdir}/naust/registry.auth.toml
%config(noreplace) %{_sysconfdir}/sysconfig/naust
%{_unitdir}/naust.service
%{_sysusersdir}/naust.conf
%{_tmpfilesdir}/naust.conf
%{_mandir}/man1/naust.1*
%ghost %dir %attr(0755,registry,registry) %{_localstatedir}/lib/naust

%pre
%sysusers_create %{_sysusersdir}/naust.conf

%post
%tmpfiles_create %{_tmpfilesdir}/naust.conf
%systemd_post naust.service

%preun
%systemd_preun naust.service

%postun
%systemd_postun_with_restart naust.service

%changelog
* Sun Sep 06 2026 naust packaging - 0.9.0-1%{?dist}
- Bump version to 0.9.0

* Mon Aug 24 2026 naust packaging - 0.8.18-1%{?dist}
- Bump version to 0.8.18

* Mon Aug 24 2026 naust packaging - 0.8.17-1%{?dist}
- Bump version to 0.8.17

* Mon Aug 24 2026 naust packaging - 0.8.16-1%{?dist}
- Bump version to 0.8.16

* Mon Aug 24 2026 naust packaging - 0.8.15-1%{?dist}
- Bump version to 0.8.15

* Mon Aug 24 2026 naust packaging - 0.8.14-1%{?dist}
- Bump version to 0.8.14

* Mon Aug 24 2026 naust packaging - 0.8.13-1%{?dist}
- Bump version to 0.8.13

* Mon Aug 24 2026 naust packaging - 0.8.12-1%{?dist}
- Bump version to 0.8.12

* Mon Aug 24 2026 naust packaging - 0.8.11-1%{?dist}
- Bump version to 0.8.11

* Mon Aug 24 2026 naust packaging - 0.8.10-1%{?dist}
- Bump version to 0.8.10

* Mon Aug 24 2026 naust packaging - 0.8.9-1%{?dist}
- Bump version to 0.8.9

* Mon Aug 24 2026 naust packaging - 0.8.8-1%{?dist}
- Bump version to 0.8.8

* Mon Aug 24 2026 naust packaging - 0.8.7-1%{?dist}
- Bump version to 0.8.7

* Mon Aug 24 2026 naust packaging - 0.8.6-1%{?dist}
- Bump version to 0.8.6

* Mon Aug 24 2026 naust packaging - 0.8.5-1%{?dist}
- Bump version to 0.8.5

* Mon Aug 24 2026 naust packaging - 0.8.4-1%{?dist}
- Bump version to 0.8.4

* Mon Aug 24 2026 naust packaging - 0.8.3-1%{?dist}
- Bump version to 0.8.3

* Mon Aug 24 2026 naust packaging - 0.8.2-1%{?dist}
- Bump version to 0.8.2

* Mon Aug 24 2026 naust packaging - 0.8.1-1%{?dist}
- Bump version to 0.8.1

* Mon Aug 24 2026 naust packaging - 0.8.0-1%{?dist}
- Bump version to 0.8.0

* Mon Aug 24 2026 naust packaging - 0.7.5-1%{?dist}
- Bump version to 0.7.5

* Mon Aug 24 2026 naust packaging - 0.7.4-1%{?dist}
- Bump version to 0.7.4

* Mon Aug 24 2026 naust packaging - 0.7.3-1%{?dist}
- Bump version to 0.7.3

* Mon Aug 24 2026 naust packaging - 0.7.2-1%{?dist}
- Bump version to 0.7.2

* Mon Aug 24 2026 naust packaging - 0.7.1-1%{?dist}
- Bump version to 0.7.1

* Mon Aug 24 2026 naust packaging - 0.7.0-1%{?dist}
- Bump version to 0.7.0

* Mon Apr 06 2026 naust packaging - 0.6.11-1%{?dist}
- Bump version to 0.6.11

* Mon Apr 06 2026 naust packaging - 0.6.10-1%{?dist}
- Bump version to 0.6.10

* Mon Apr 06 2026 naust packaging - 0.6.9-1%{?dist}
- Bump version to 0.6.9

* Mon Apr 06 2026 naust packaging - 0.6.8-1%{?dist}
- Bump version to 0.6.8

* Mon Apr 06 2026 naust packaging - 0.6.7-1%{?dist}
- Bump version to 0.6.7

* Mon Apr 06 2026 naust packaging - 0.6.6-1%{?dist}
- Bump version to 0.6.6

* Sun Jan 11 2026 naust packaging - 0.6.5-1%{?dist}
- Bump version to 0.6.5

* Sun Jan 11 2026 naust packaging - 0.6.4-1%{?dist}
- Bump version to 0.6.4

* Sun Jan 11 2026 naust packaging - 0.6.3-1%{?dist}
- Bump version to 0.6.3

* Sat Jan 10 2026 naust packaging - 0.6.2-1%{?dist}
- Bump version to 0.6.2

* Tue Jan 06 2026 naust packaging - 0.6.1-1%{?dist}
- Bump version to 0.6.1

* Tue Jan 06 2026 naust packaging - 0.6.0-1%{?dist}
- Bump version to 0.6.0

* Mon Jan 05 2026 naust packaging - 0.5.0-1%{?dist}
- Bump version to 0.5.0

* Mon Jan 05 2026 naust packaging - 0.1.0-1%{?dist}
- Initial RPM packaging for local builds

%define _rpmfilename %%{NAME}-%%{VERSION}-%%{RELEASE}.%{OS_NAME_RELEASE}.%%{ARCH}.rpm

Name:       statshouse
Version:    %{BUILD_VERSION}
Release:    1
Summary:    Highly-available, scalable, multi-tenant monitoring system.
License:    Mozilla Public License 2.0

%description
Provides the main daemon of the StatsHouse monitoring system.

%prep
%build

%install
mkdir -p %{buildroot}/usr/bin/
mkdir -p %{buildroot}/usr/share/engine/bin/
mkdir -p %{buildroot}/usr/lib/systemd/system/
mkdir -p %{buildroot}/usr/lib/statshouse-api/statshouse-ui/
install -m 755 %{_topdir}/target/statshouse-api %{buildroot}/usr/bin/
install -m 755 %{_topdir}/target/statshouse %{buildroot}/usr/share/engine/bin
install -m 755 %{_topdir}/target/statshouse-metadata %{buildroot}/usr/share/engine/bin
install -m 755 %{_topdir}/target/statshouse-igp %{buildroot}/usr/share/engine/bin
install -m 755 %{_topdir}/target/statshouse-agg %{buildroot}/usr/share/engine/bin
cp -a %{_topdir}/statshouse-ui/build/* %{buildroot}/usr/lib/statshouse-api/statshouse-ui/
install -m 444 %{_topdir}/cmd/statshouse-api/statshouse-api.service %{buildroot}/usr/lib/systemd/system/
install -m 444 %{_topdir}/cmd/statshouse/statshouse.service %{buildroot}/usr/lib/systemd/system/
install -m 444 %{_topdir}/cmd/statshouse/statshouse.service %{buildroot}/usr/lib/systemd/system/statshouse-igp.service
install -m 444 %{_topdir}/cmd/statshouse/statshouse.service %{buildroot}/usr/lib/systemd/system/statshouse-agg.service

%files
/usr/share/engine/bin/statshouse
/usr/lib/systemd/system/statshouse.service

%changelog

%package api
Summary: Provides HTTP API and frontend for StatsHouse.
Group: misc
%description api
%files api
/usr/bin/statshouse-api
/usr/lib/statshouse-api/statshouse-ui/*
/usr/lib/systemd/system/statshouse-api.service

%package metadata
Summary: Provides storage of metadata for StatsHouse.
Group: misc
%description metadata
%files metadata
/usr/share/engine/bin/statshouse-metadata

%package igp
Summary: Provides ingress proxy for StatsHouse.
%description igp
%files igp
/usr/share/engine/bin/statshouse-igp
/usr/lib/systemd/system/statshouse-igp.service

%package agg
Summary: Provides aggregator for StatsHouse.
%description agg
%files agg
/usr/share/engine/bin/statshouse-agg
/usr/lib/systemd/system/statshouse-agg.service

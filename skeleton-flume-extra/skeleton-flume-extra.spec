%define jar_name  skeleton-flume-extra.jar
%define library_version flume-extra-1.5

# Specfile
Summary:        skeleton flume jar dependency
Name:           skeleton-flume-extra
Version:        1.5
Release:        1%{?dist}
License:        ASL 2.0
URL:            https://git.cern.ch/web/?p=skeleton/flume-extra.git
Group:          System Environment/Base
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)
BuildArch:      noarch
Source0:        %{library_version}-bin.tar.gz

BuildRequires:  jpackage-utils

Requires:       jpackage-utils
Requires:       java >= 1:1.6.0

%description
This is package gets the necessary jar dependencies for skeleton flume resources, including sources, sinks and serializers

%prep
%setup -q -n %{library_version}

%build

%install
rm -rf %{buildroot}
mkdir -p %{buildroot}%{_javadir}
install -m 644 %{library_version}.jar %{buildroot}%{_javadir}/%{jar_name}

%clean
rm -rf %{buildroot}

%files
%defattr(-,root,root)
%{_javadir}/%{jar_name}

%changelog
* Mon Jul 15 2013 Massimo Paladin <massimo.paladin@cern.ch> - 0.0-1
- Initial release.

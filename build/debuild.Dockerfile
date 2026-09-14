FROM debian:bullseye

RUN printf "deb http://snapshot.debian.org/archive/debian/20260824T000000Z bullseye main\n\
deb http://snapshot.debian.org/archive/debian-security/20260824T000000Z bullseye-security main\n\
deb http://snapshot.debian.org/archive/debian/20260824T000000Z bullseye-updates main\n" > /etc/apt/sources.list \
  && printf 'Acquire::Check-Valid-Until "false";\n' > /etc/apt/apt.conf.d/99no-check-valid-until

# packages for debuild
RUN apt-get update \
  && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends devscripts build-essential dh-exec \
  && rm -rf /var/lib/apt/lists/*

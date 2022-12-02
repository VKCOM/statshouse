FROM debian:bullseye

# packages for debuild
RUN apt-get update \
  && DEBIAN_FRONTEND=noninteractive apt-get install -y devscripts build-essential \
  && rm -rf /var/lib/apt/lists/*

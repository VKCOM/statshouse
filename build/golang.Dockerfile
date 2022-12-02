ARG PARENT=debian
FROM $PARENT

# curl
RUN apt-get update \
  && DEBIAN_FRONTEND=noninteractive apt-get install -y curl \
  && rm -rf /var/lib/apt/lists/*

# gcc for cgo
RUN curl -L https://packagecloud.io/github/git-lfs/gpgkey | apt-key add - \
  && curl -s https://packagecloud.io/install/repositories/github/git-lfs/script.deb.sh | bash \
  && apt-get update \
  && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
	g++ \
	gcc \
	git \
	libc6-dev \
	cmake \
	make \
	pkg-config \
	ssh \
	git-lfs \
  && rm -rf /var/lib/apt/lists/*

# golang
ARG GOLANG_URL=""
ARG GOLANG_SHA256=""
RUN set -eux; \
	curl -o go.tgz -L "$GOLANG_URL"; \
	echo "${GOLANG_SHA256} go.tgz" | sha256sum --check --status; \
	tar --strip-components=1 -C /usr/local -xzf go.tgz; \
	rm go.tgz; \
	export PATH="/usr/local/go/bin:$PATH"; \
	go version
ENV GOPATH /go
ENV PATH $GOPATH/bin:/usr/local/go/bin:$PATH
RUN mkdir -p "$GOPATH/src" "$GOPATH/bin" "$GOPATH/pkg" && chmod -R 777 "$GOPATH"

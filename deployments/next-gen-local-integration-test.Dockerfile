# Specify the image architecture explicitly,
# otherwise it will not work correctly on other architectures.
FROM amd64/rockylinux:9.3 as downloader

ARG BRANCH
ENV BRANCH=$BRANCH

ARG COMMUNITY
ENV COMMUNITY=$COMMUNITY

ARG VERSION
ENV VERSION=$VERSION

ARG OS
ENV OS=$OS

ARG ARCH
ENV ARCH=$ARCH

USER root
WORKDIR /root/download

# Installing dependencies.
#RUN dnf install -y wget
#COPY ./tests/scripts/download-integration-test-binaries.sh .
# Download all binaries into bin dir.
#RUN ./download-integration-test-binaries.sh $BRANCH $COMMUNITY $VERSION $OS $ARCH
#RUN ls ./bin

# Download go into /usr/local dir.
ENV GOLANG_VERSION 1.23.0
ENV GOLANG_DOWNLOAD_URL https://dl.google.com/go/go$GOLANG_VERSION.linux-amd64.tar.gz
RUN curl -fsSL "$GOLANG_DOWNLOAD_URL" -o golang.tar.gz \
	&& tar -C /usr/local -xzf golang.tar.gz \
	&& rm golang.tar.gz

FROM amd64/rockylinux:9.3

USER root
WORKDIR /root

# Installing dependencies.
# Base image is changed from centos:7 to rockylinux:9.
# yum is replaced by dnf.
# musl-dev is removed as it is not available on rocky9.
# mysql 5.7 is not available on rocky 9, switched to default mysql client from rocky repo.
RUN dnf update -y && dnf install -y epel-release && \
    dnf install -y \
	nmap-ncat \
	git \
	bash-completion \
	wget \
    	which \
	gcc \
	make \
    	tar \
	sudo \
	python3 \
    	psmisc \
    	procps \
    	s3cmd \
        mysql \
    	java-1.8.0-openjdk \
    	java-1.8.0-openjdk-devel && \
    	dnf clean all

# Copy go form downloader.
COPY --from=downloader /usr/local/go /usr/local/go
ENV GOPATH /go
ENV GOROOT /usr/local/go
ENV PATH $GOPATH/bin:$GOROOT/bin:$PATH

WORKDIR /go/src/github.com/pingcap/ticdc
COPY . .

#RUN --mount=type=cache,target=/root/.cache/go-build,target=/go/pkg/mod make integration_test_build cdc
#COPY --from=downloader /root/download/bin/* ./bin/
#RUN --mount=type=cache,target=/root/.cache/go-build,target=/go/pkg/mod make check_third_party_binary
#COPY ./bin/ ./bin/
#COPY --from=downloader /root/download/bin/* ./bin/
CMD ["/bin/bash", "-c", "NEXT_GEN=1 make integration_test"]

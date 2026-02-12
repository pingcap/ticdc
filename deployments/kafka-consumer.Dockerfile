FROM golang:1.25-alpine AS builder
# `binutils-gold` provides `ld.gold`. Without it, the kafka-consumer build on arm64 fails
# at the final link step with: `collect2: fatal error: cannot find 'ld'`.
#
# `cyrus-sasl-dev` provides `libsasl2.so` which is required by the vendored `librdkafka` archive.
RUN apk add --no-cache make bash git build-base binutils-gold cyrus-sasl-dev
WORKDIR /go/src/github.com/pingcap/ticdc
COPY . .

RUN --mount=type=cache,target=/go/pkg/mod go mod download
# The vendored `librdkafka` archive requires `libsasl2` symbols on arm64.
RUN --mount=type=cache,target=/root/.cache/go-build CGO_LDFLAGS="-lsasl2" make kafka_consumer

FROM alpine:3.15

# `libsasl` is required at runtime because the vendored `librdkafka` archive is linked with `-lsasl2`.
RUN apk update && apk add tzdata curl libsasl

ENV TZ=Asia/Shanghai

COPY --from=builder  /go/src/github.com/pingcap/ticdc/bin/cdc_kafka_consumer /cdc_kafka_consumer

ENTRYPOINT ["tail", "-f", "/dev/null"]

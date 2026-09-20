FROM golang:1.25-alpine AS builder
# `binutils-gold` provides `ld.gold`. Without it, the kafka-consumer build on arm64 fails
# at the final link step with: `collect2: fatal error: cannot find 'ld'`.
RUN apk add --no-cache make bash git build-base binutils-gold
WORKDIR /go/src/github.com/pingcap/ticdc
COPY . .

RUN --mount=type=cache,target=/go/pkg/mod go mod download
RUN --mount=type=cache,target=/root/.cache/go-build make kafka_consumer

FROM alpine:3.15

RUN apk add --no-cache tzdata curl

ENV TZ=Asia/Shanghai

COPY --from=builder  /go/src/github.com/pingcap/ticdc/bin/cdc_kafka_consumer /cdc_kafka_consumer

ENTRYPOINT ["tail", "-f", "/dev/null"]

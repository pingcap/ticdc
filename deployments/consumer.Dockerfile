FROM golang:1.26.9-alpine AS builder
# binutils-gold provides ld.gold for the arm64 CGO link step.
RUN apk add --no-cache make bash git build-base binutils-gold
WORKDIR /go/src/github.com/pingcap/ticdc
COPY . .

RUN --mount=type=cache,target=/go/pkg/mod go mod download
RUN --mount=type=cache,target=/root/.cache/go-build make consumer

FROM alpine:3.24
RUN apk add --no-cache ca-certificates tzdata curl
ENV TZ=Asia/Shanghai

COPY --from=builder /go/src/github.com/pingcap/ticdc/bin/cdc_consumer /cdc_consumer
ENTRYPOINT ["/cdc_consumer"]

FROM golang:1.23-alpine as builder
RUN apk add --no-cache git make bash
WORKDIR /go/src/github.com/pingcap/ticdc
COPY . .
ENV CDC_ENABLE_VENDOR=0
ARG NEXT_GEN
RUN if [ -n "${NEXT_GEN}" ]; then NEXT_GEN=1 make cdc; else make cdc; fi

FROM alpine:3.15
RUN apk add --no-cache tzdata bash curl socat
COPY --from=builder /go/src/github.com/pingcap/ticdc/bin/cdc /cdc
EXPOSE 8300
CMD [ "/cdc" ]

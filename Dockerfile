FROM golang:1.27.4-alpine3.24@sha256:70bd7ddea8b6cbe5d6943cde279d6b67feb10cc924fb9100d1d8757c1575b280 AS builder

RUN apk add --no-cache \
    build-base=0.5-r4 \
    linux-headers=7.0.0-r1 \
    ceph19-dev=19.2.4-r1

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY *.go ./
RUN CGO_ENABLED=1 go build -trimpath -ldflags="-s -w" -o restic-rados-server .

FROM alpine:3.24.4@sha256:f59abe656b692d901e184be3445e084786602108599d2a51d229c0f5a48c71bf

RUN apk add --no-cache \
    librados19=19.2.4-r1

COPY --from=builder /app/restic-rados-server /usr/local/bin/restic-rados-server

LABEL org.opencontainers.image.title="restic-rados-server"
LABEL org.opencontainers.image.description="A restic repository backend that stores data in raw Ceph RADOS"
LABEL org.opencontainers.image.source="https://github.com/josh/restic-rados-server"
LABEL org.opencontainers.image.licenses="MIT"

USER 65534:65534

ENTRYPOINT ["/usr/local/bin/restic-rados-server"]

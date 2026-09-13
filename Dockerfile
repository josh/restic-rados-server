FROM golang:1.27.1-alpine3.23@sha256:d9e2f2f07b10cc922da3e80e035c3058810b328d5aef82d2c63680967c5e2ec9 AS builder

RUN apk add --no-cache \
    build-base \
    linux-headers \
    ceph19-dev

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY *.go ./
RUN CGO_ENABLED=1 go build -trimpath -ldflags="-s -w" -o restic-rados-server .

FROM alpine:3.24.1@sha256:28bd5fe8b56d1bd048e5babf5b10710ebe0bae67db86916198a6eec434943f8b

RUN apk add --no-cache \
    librados19

COPY --from=builder /app/restic-rados-server /usr/local/bin/restic-rados-server

LABEL org.opencontainers.image.title="restic-rados-server"
LABEL org.opencontainers.image.description="A restic repository backend that stores data in raw Ceph RADOS"
LABEL org.opencontainers.image.source="https://github.com/josh/restic-rados-server"
LABEL org.opencontainers.image.licenses="MIT"

USER 65534:65534

ENTRYPOINT ["/usr/local/bin/restic-rados-server"]

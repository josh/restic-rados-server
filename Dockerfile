FROM golang:1.27.1-alpine3.23@sha256:0908ac9b9319e09d7c238aabe914e0395c51d63c4e3d0ae8c554fda9158a5769 AS builder

RUN apk add --no-cache \
    build-base=0.5-r3 \
    linux-headers=6.16.12-r0 \
    ceph19-dev=19.2.3-r3

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY *.go ./
RUN CGO_ENABLED=1 go build -trimpath -ldflags="-s -w" -o restic-rados-server .

FROM alpine:3.24.1@sha256:28bd5fe8b56d1bd048e5babf5b10710ebe0bae67db86916198a6eec434943f8b

RUN apk add --no-cache \
    librados19=19.2.4-r1

COPY --from=builder /app/restic-rados-server /usr/local/bin/restic-rados-server

LABEL org.opencontainers.image.title="restic-rados-server"
LABEL org.opencontainers.image.description="A restic repository backend that stores data in raw Ceph RADOS"
LABEL org.opencontainers.image.source="https://github.com/josh/restic-rados-server"
LABEL org.opencontainers.image.licenses="MIT"

USER 65534:65534

ENTRYPOINT ["/usr/local/bin/restic-rados-server"]

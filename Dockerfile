FROM golang:1.26.4-alpine3.23@sha256:f23e8b227fb4493eabe03bede4d5a32d04092da71962f1fb79b5f7d1e6c2a17f AS builder

RUN apk add --no-cache \
    build-base=0.5-r3 \
    linux-headers=6.16.12-r0 \
    ceph19-dev=19.2.3-r3

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY *.go ./
RUN CGO_ENABLED=1 go build -trimpath -ldflags="-s -w" -o restic-rados-server .

FROM alpine:3.24@sha256:a2d49ea686c2adfe3c992e47dc3b5e7fa6e6b5055609400dc2acaeb241c829f4

RUN apk add --no-cache \
    librados19=19.2.3-r3

COPY --from=builder /app/restic-rados-server /usr/local/bin/restic-rados-server

LABEL org.opencontainers.image.title="restic-rados-server"
LABEL org.opencontainers.image.description="A restic repository backend that stores data in raw Ceph RADOS"
LABEL org.opencontainers.image.source="https://github.com/josh/restic-rados-server"
LABEL org.opencontainers.image.licenses="MIT"

USER 65534:65534

ENTRYPOINT ["/usr/local/bin/restic-rados-server"]

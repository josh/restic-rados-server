FROM golang:1.25.5-alpine3.23@sha256:ac09a5f469f307e5da71e766b0bd59c9c49ea460a528cc3e6686513d64a6f1fb AS builder

RUN apk add --no-cache \
    build-base=0.5-r3 \
    linux-headers=6.16.12-r0 \
    ceph19-dev=19.2.3-r3

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY *.go ./
RUN CGO_ENABLED=1 go build -trimpath -ldflags="-s -w" -o restic-rados-server .

FROM alpine:3.23@sha256:5b10f432ef3da1b8d4c7eb6c487f2f5a8f096bc91145e68878dd4a5019afde11

RUN apk add --no-cache \
    librados19=19.2.3-r3

COPY --from=builder /app/restic-rados-server /usr/local/bin/restic-rados-server

LABEL org.opencontainers.image.title="restic-rados-server"
LABEL org.opencontainers.image.description="A restic repository backend that stores data in raw Ceph RADOS"
LABEL org.opencontainers.image.source="https://github.com/josh/restic-rados-server"
LABEL org.opencontainers.image.licenses="MIT"

USER 65534:65534

ENTRYPOINT ["/usr/local/bin/restic-rados-server"]

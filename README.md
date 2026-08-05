# restic-rados-server

`restic-rados-server` implements restic's
[REST backend API](https://restic.readthedocs.io/en/latest/100_references.html#rest-backend)
using Ceph RADOS. Standard restic clients connect with a `rest:` repository
URL, such as `rest:https://backup.example.com/`.

It is intended for existing Ceph installations that need a shared restic
endpoint, from a single server process to a replicated Kubernetes deployment.

## Features

- Compatible with normal restic repository operations.
- Multiple named repositories on one endpoint.
- Read-only, read-append, and read-write access ceilings.
- Multiple server replicas sharing the same Ceph-backed repositories.
- TCP, Unix socket, systemd socket activation, and Tailscale service listeners.
- Liveness and connection-state readiness endpoints.

## Requirements

- Reachable Ceph monitors and one or more existing RADOS pools.
- A Ceph client ID and keyring with read/write access to the configured pools.
- A current restic client wherever backups and restores will run.

The typical configuration maps the whole repository to one pool named
`restic`. The systemd example does this with `--pool restic`. See
[Advanced repository mapping](#advanced-repository-mapping) only if the
deployment needs split pools, namespaces, or multiple repositories.

Use the systemd guide for a single-host setup or the Helm guide for a
replicated Kubernetes deployment.

> [!IMPORTANT]
> A normal TCP listener serves plain HTTP and does not authenticate clients.
> The repository `access` setting is an authorization ceiling, not an identity
> check. Keep the listener private or place it behind a TLS endpoint that
> authenticates clients. Anyone who can reach a read-write listener can alter
> or delete repository data.

## Use case 1: systemd service

This path runs one server process on a host with Ceph client connectivity.

### 1. Install the server

The host needs the librados runtime. Building from source additionally requires
Go 1.26.4 or newer and the librados development files:

```sh
go build -o restic-rados-server .
sudo install -m 0755 restic-rados-server /usr/local/bin/restic-rados-server
```

Create a dedicated, unprivileged `restic-rados-server` service account. That
account must be able to read `ceph.conf` and the Ceph keyring. Keep the keyring
readable only by the service account or its group.

### 2. Add the socket and service units

Create `/etc/systemd/system/restic-rados-server.socket`:

```ini
[Unit]
Description=Restic RADOS repository socket

[Socket]
ListenStream=127.0.0.1:8000
Accept=no
NoDelay=true

[Install]
WantedBy=sockets.target
```

Create `/etc/systemd/system/restic-rados-server.service`:

```ini
[Unit]
Description=Restic RADOS repository server
Requires=restic-rados-server.socket
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
User=restic-rados-server
Group=restic-rados-server
ExecStart=/usr/local/bin/restic-rados-server \
    --ceph-conf /etc/ceph/ceph.conf \
    --keyring /etc/ceph/ceph.client.restic.keyring \
    --id restic \
    --pool restic \
    --access rw
Restart=on-failure
RestartSec=5s
TimeoutStopSec=75s
NoNewPrivileges=true
PrivateTmp=true
ProtectHome=true
ProtectSystem=full
```

The socket unit owns the listener and starts the service on the first
connection. The server automatically consumes the socket inherited from
systemd, so `ExecStart` does not need `--listen`.

`--pool restic` maps the entire default repository to the `restic` pool.
`--id restic` authenticates as `client.restic`. Adjust the Ceph paths, client
ID, and pool name for the cluster.

The loopback listener is safe for a local reverse proxy or an SSH tunnel. If
you bind to a non-loopback address, restrict network access and terminate TLS
and client authentication before traffic reaches the server.

### 3. Start and verify

Load the unit and start the server:

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now restic-rados-server.socket
sudo systemctl status restic-rados-server.socket
```

Check both health endpoints:

```sh
curl --fail http://127.0.0.1:8000/healthz
curl --fail http://127.0.0.1:8000/readyz
```

`/healthz` reports that the HTTP process is alive. `/readyz` reports whether
the server has initialized Ceph connection state. It returns 503 during initial
connection failures and reconnect attempts; it does not actively probe Ceph.

The first request activates `restic-rados-server.service`. Check its status and
follow its logs with:

```sh
sudo systemctl status restic-rados-server.service
sudo journalctl -u restic-rados-server.service -f
```

### 4. Initialize and use the repository

For a local smoke test or while connected through a tunnel:

```sh
export RESTIC_REPOSITORY=rest:http://127.0.0.1:8000/
export RESTIC_PASSWORD_FILE=/secure/path/restic-password

restic init
restic backup /path/to/data
restic snapshots
restic check
```

For regular remote use, change `RESTIC_REPOSITORY` to the authenticated HTTPS
URL exposed by your proxy, for example
`rest:https://backup.example.com/`.

## Use case 2: Helm on Kubernetes

The chart in [`charts/restic-rados-server`](charts/restic-rados-server) creates
a Deployment, one or more ClusterIP Services, a ConfigMap, a ServiceAccount,
and optional NetworkPolicies. It does not create Ceph credentials, an Ingress,
a Gateway, or TLS certificates.

The chart requires Kubernetes 1.25 or newer. Pods must be able to reach the
Ceph monitors and OSDs.

### 1. Create the Ceph keyring Secret

Create the target namespace and a Secret whose `keyring` entry contains the
Ceph keyring file:

```sh
kubectl create namespace restic
kubectl -n restic create secret generic restic-rados-ceph-keyring \
  --from-file=keyring=/secure/path/ceph.client.restic.keyring
```

For a production cluster, manage this Secret through the same encrypted or
external secret workflow used for other long-lived credentials. The chart
references an existing Secret and never generates one.

### 2. Create production values

Save the following as `values-production.yaml` and replace the monitor
addresses, cluster ID, pool names, and namespace labels:

```yaml
replicaCount: 3

ceph:
  clusterID: "00000000-0000-0000-0000-000000000000"
  monitors:
    - "ceph-mon-a.storage.svc.cluster.local:6789"
    - "ceph-mon-b.storage.svc.cluster.local:6789"
    - "ceph-mon-c.storage.svc.cluster.local:6789"
  clientID: restic
  keyring:
    secret:
      name: restic-rados-ceph-keyring

config:
  repos:
    default:
      pools:
        restic-data:
          - data
          - index
        restic-metadata:
          - config
          - keys
          - locks
          - snapshots

services:
  rw:
    port: 8000
    targetPort: 8000
    access: rw
    networkPolicy:
      ingressFrom:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: backup-jobs
  ro:
    port: 8000
    targetPort: 8001
    access: r
    networkPolicy:
      ingressFrom:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: restore-jobs
```

The `config` block uses the server's JSON configuration schema. The chart
renders it as `restic-rados-server.json` in a ConfigMap and mounts it into each
pod. This example sends `data` and `index` objects to `restic-data` and uses
`restic-metadata` for `config`, `keys`, `locks`, and `snapshots`.

When `services` is non-empty, the chart generates `config.listen` from the
map and creates one Service per entry. The example creates
`restic-rados-server-rw` and `restic-rados-server-ro`. Both expose port 8000,
but they target distinct pod ports whose listeners enforce read-write and
read-only access ceilings. Each `targetPort` must be unique. The Service
`port` defaults to its `targetPort`, `access` defaults to `rw`, and
`type` defaults to `service.type`.

When `networkPolicy.enabled` is true, the chart creates a default-deny ingress
policy and a separate allow policy for each non-empty
`services.<name>.networkPolicy.ingressFrom` list. Entries are Kubernetes
NetworkPolicy peers. The chart always scopes each allow policy to that
Service's TCP `targetPort`, so the read-only and read-write paths can have
different callers. The example grants `backup-jobs` access to the read-write
port and `restore-jobs` access to the read-only port.

Leave `services` empty to keep the legacy single Service. In that mode,
`config.listen`, `service`, and `networkPolicy.ingress` retain their
existing behavior.

### 3. Install or upgrade the release

From this repository:

```sh
helm upgrade --install restic-rados-server ./charts/restic-rados-server \
  --namespace restic \
  --values values-production.yaml \
  --wait \
  --timeout 5m
```

Inspect the rollout and readiness:

```sh
kubectl -n restic rollout status deployment/restic-rados-server
kubectl -n restic get pods,service
kubectl -n restic logs deployment/restic-rados-server
```

For a local smoke test, forward the Service and query its readiness endpoint:

```sh
kubectl -n restic port-forward service/restic-rados-server-rw 8000:8000
curl --fail http://127.0.0.1:8000/readyz
```

After an authenticated HTTPS route is in place, initialize the repository from
a restic client:

```sh
export RESTIC_REPOSITORY=rest:https://backup.example.com/
export RESTIC_PASSWORD_FILE=/secure/path/restic-password

restic init
restic backup /path/to/data
```

### Production considerations

- Keep the Services as `ClusterIP` and publish them through existing gateways
  or proxies that provide TLS and client authentication. Restrict each
  NetworkPolicy to that component and the appropriate in-cluster clients.
- Multiple replicas are safe: they share state through Ceph, while restic's
  repository locks coordinate client operations. Use anti-affinity or other
  placement rules so a single node failure does not remove every replica.
- The server allocates a 16 MiB read buffer and a 16 MiB write buffer for each
  in-flight transfer by default. Account for concurrency when setting pod
  memory limits, or tune `config.read_buffer_size` and
  `config.write_buffer_size`.
- `/healthz` is the liveness endpoint. `/readyz` reports cached Ceph connection
  state rather than actively probing the cluster. The chart configures both
  probes.
- Changes to rendered server or Ceph configuration update the pod-template
  checksum, causing a Deployment rollout.
- Pin and review chart and image versions together. The chart defaults the
  image tag to `v` followed by its `appVersion`.

## Advanced repository mapping

The systemd example stores the entire repository in one Ceph pool. The Helm
example demonstrates a split-pool configuration for installations with
different Ceph policies for bulk and metadata objects.

Use advanced mapping when repositories need separate placement, retention, or
Ceph authorization policies. The server can map the six restic object
types—`config`, `keys`, `locks`, `snapshots`, `data`, and `index`—to different
pools, namespaces, or object prefixes.

The `default` repository is served at `/`. Named repositories are served at
their matching path:

```text
repos.default  -> rest:https://backup.example.com/
repos.archive  -> rest:https://backup.example.com/archive
```

A pool key may include a RADOS namespace as `pool/namespace`. Never point two
repositories at the same pool, namespace, and prefix: their object names would
overlap.

### One pool

Use `"*"` to place every restic object type in one pool:

```json
{
  "repos": {
    "default": {
      "pools": {
        "restic": ["*"]
      }
    }
  }
}
```

### Split object types across pools

An installation with distinct Ceph policies for bulk and metadata objects can
split one repository across two pools:

```json
{
  "repos": {
    "default": {
      "pools": {
        "restic-data": ["data", "index"],
        "restic-metadata": ["*"]
      }
    }
  }
}
```

The explicit mapping sends `data` and `index` objects to `restic-data`. The
`"*"` catch-all sends the remaining types to `restic-metadata`. Each object
type can have only one destination, and a repository can have only one
catch-all pool.

### Multiple named repositories

RADOS namespaces let several isolated repositories share a pool:

```json
{
  "repos": {
    "default": {
      "pools": {
        "restic/default": ["*"]
      }
    },
    "archive": {
      "pools": {
        "restic/archive": ["*"]
      }
    }
  }
}
```

The corresponding restic URLs end in `/` and `/archive`.

### Dynamic repository names

A single `*` repository pattern can serve repositories created at arbitrary
matching paths. Dynamic repositories must include `{repo}` or `{repo_match}`
in their namespace or object prefix so their storage cannot overlap:

```json
{
  "repos": {
    "*": {
      "pools": {
        "restic/{repo}": ["*"]
      }
    }
  }
}
```

With this configuration, `rest:https://backup.example.com/laptop` uses the
`laptop` RADOS namespace and
`rest:https://backup.example.com/server-01` uses `server-01`.

### Access ceilings

Each repository accepts one of three access levels:

- `rw` or `read-write`: normal backup, restore, forget, and prune workflows.
- `ra` or `read-append`: reads and new objects are allowed; destructive changes
  are blocked, except lock deletion needed for restic coordination.
- `r` or `read-only`: writes and deletes are blocked.

The default is `rw`. These values limit what the endpoint may do; they do not
authenticate the caller.

Each listener can add its own access ceiling. This lets one server process
serve separate read-only and read-write TCP ports:

```json
{
  "listen": [
    {
      "address": "0.0.0.0:8000",
      "access": "rw"
    },
    {
      "address": "0.0.0.0:8001",
      "access": "r"
    }
  ]
}
```

Listener access accepts the same `r`/`read-only`, `ra`/`read-append`, and
`rw`/`read-write` values. Omitting it leaves that listener without an
additional ceiling. The effective access for a request is the most restrictive
of the repository setting, the listener setting, and any grant from a trusted
capability header; none of these can elevate another ceiling.

## Optional Tailscale service listener

The server can listen on a Tailscale service with an address such as
`tailscale+svc:restic`. To enforce Tailscale application capabilities, use a
listener object in the JSON configuration:

```json
{
  "listen": [
    {
      "address": "tailscale+svc:restic",
      "trusted_tailscale_caps": "github.com/josh/restic-rados-server"
    }
  ]
}
```

The application-capability grant must target the serving node's tag, not the
service. Reachability still targets the service:

```jsonc
{ "src": ["autogroup:member"], "dst": ["svc:restic"], "ip": ["tcp:443"] },
{
  "src": ["autogroup:member"],
  "dst": ["tag:restic"],
  "app": {
    "github.com/josh/restic-rados-server": [
      { "*": "rw" }
    ]
  }
}
```

The serving nodes must carry `tag:restic`. Capability values are `r`, `ra`, or
`rw`, keyed by repository name or `*`. See
[tailscale/tailscale#19618](https://github.com/tailscale/tailscale/issues/19618)
for the service-VIP capability-resolution behavior.

## Command-line configuration

Run `restic-rados-server --help` for all flags. Common flags include:

```text
--config PATH       JSON configuration file
--listen ADDRESS    TCP address or Unix socket; repeatable
--pool SPEC         pool[/namespace]:types mapping for the default repository
--access LEVEL      r, ra, or rw for the default repository
--ceph-conf PATH    Ceph configuration file
--keyring PATH      Ceph keyring file
--id ID             Ceph client ID without the client. prefix
--verbose           enable debug logging
```

Command-line values override environment values, and environment values
override the JSON configuration. Flags are sufficient for a single repository,
as shown in the systemd guide. Use JSON for multiple repositories or advanced
mapping; the Helm chart renders that configuration automatically.

The project is licensed under the [MIT License](LICENSE).

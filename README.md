# restic-rados-server

A restic repository backend that stores data in raw Ceph RADOS

## Tailscale service ACLs

When served as a Tailscale service (`tailscale+svc:<name>`) with
`trusted_tailscale_caps`, the app-capability grant must target the serving
**node's tag**, not the service. tailscaled resolves a peer's capabilities
against the node's own Tailscale address, never the service VIP the client
dialed, so a grant scoped to `svc:<name>` produces no
`Tailscale-App-Capabilities` header and every request is denied with 403.

Split it into two grants — reachability on the service, capability on the tag:

```jsonc
// reachability: allowed to dial the service VIP
{ "src": ["autogroup:member"], "dst": ["svc:restic"], "ip": ["tcp:443"] },
// capability: resolved against the node identity, so target the node's tag
{
  "src": ["autogroup:member"],
  "dst": ["tag:restic"],
  "app": { "github.com/josh/restic-rados-server": [ { "*": "rw" } ] }
}
```

The serving nodes must actually carry that tag (`tag:restic`). Access values are
`r`, `ra`, or `rw`, keyed per repo name or `*`.

See https://github.com/tailscale/tailscale/issues/19618.

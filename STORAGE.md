# Storage layout

This document specifies how a restic repository is stored in RADOS. It is the contract for anyone inspecting objects directly or writing another implementation that shares pools with this server. It does not describe the server's configuration or HTTP behaviour; see the README for those.

## Isolation model

A repository is a set of `(pool, namespace)` targets, one per restic object type: `config`, `keys`, `locks`, `snapshots`, `data`, `index`. Several types may share a target, and all six usually do. An empty namespace is the RADOS default namespace.

Object names carry no repository name, prefix, magic value, or format version. Two repositories that share a `(pool, namespace)` overlap completely and corrupt each other. Namespaces, not name prefixes, are the isolation mechanism.

## Object names

| restic path          | RADOS object name       |
|----------------------|-------------------------|
| `config`             | `config`                |
| `keys/<id>`          | `keys/<id>`             |
| `locks/<id>`         | `locks/<id>`            |
| `snapshots/<id>`     | `snapshots/<id>`        |
| `data/<id>`          | `data/<id>`             |
| `index/<id>`         | `index/<id>`            |

`<id>` is exactly 64 lowercase hexadecimal characters, the SHA-256 of the object's content. RADOS names are flat; the `/` is an ordinary character. There is no two-level `data/ab/abcd…` nesting as in restic's local layout.

A name that does not match one of these shapes (or the striped shape below) is foreign. The server never reads, lists, or deletes foreign objects.

## Plain objects

A plain object holds the blob's bytes verbatim. It has no xattrs and no omap.

- Created exclusively: an existing object is never overwritten.
- Written by append in chunks; a completed object is byte-identical to the restic blob.
- Maximum size is the object size limit in effect for the type (see *Striped objects*). A blob exactly at the limit is stored plain.

`config`, `keys`, and `locks` are always plain. Their limit is the cluster's `osd_max_object_size` (default 128 MiB); larger blobs are refused.

## Striped objects

`snapshots`, `data`, and `index` blobs whose declared length exceeds the object size limit are split across several RADOS objects. The layout is the one used by Ceph's libradosstriper, restricted to its simplest geometry.

### Physical names

```
<type>/<id>.0000000000000000
<type>/<id>.0000000000000001
<type>/<id>.0000000000000002
…
```

The suffix is `.` followed by the zero-based stripe index as 16 lowercase hexadecimal digits (17 characters total). Stripe 0 always exists.

### Xattrs on stripe 0

Only stripe 0 carries metadata. All values are ASCII decimal integers.

| xattr                         | value                                  |
|-------------------------------|----------------------------------------|
| `striper.layout.object_size`  | bytes per stripe                       |
| `striper.layout.stripe_unit`  | always equal to `object_size`          |
| `striper.layout.stripe_count` | always `1`                             |
| `striper.size`                | total logical length of the blob       |

Any other geometry (`stripe_count` ≠ 1, `stripe_unit` ≠ `object_size`, or `object_size` = 0) is unsupported. The server fails reads and deletes of such objects and skips them in listings. No `striper.lock` xattr is ever taken.

### Layout

With `S = object_size` and `L = striper.size`:

- stripe *n* holds bytes `[n·S, (n+1)·S)` of the blob
- every stripe except the last is exactly `S` bytes
- number of stripes is `ceil(L / S)`, and at least 1

`S` is chosen per write from the configured limit, rounded down to a multiple of the pool's required alignment on erasure-coded pools. Because geometry is recorded on each object, changing the limit later needs no migration.

### Write sequence

1. Create stripe 0 exclusively with all four xattrs in one operation, `striper.size` set to `0`.
2. Append data to stripe 0, then stripe 1, and so on.
3. Set `striper.size` to the final length.

A stripe 0 whose `striper.size` is `0` while stripes hold data is an interrupted upload.

### Read

Clamp the request to `striper.size`, then read each stripe in turn. A stripe that is missing or shorter than the layout implies is an error, not a hole of zeros.

### Delete

1. Read `striper.size` and `object_size` to compute the stripe count `N`.
2. Delete stripes `N-1` down to `1`.
3. Delete stripe 0 last, so the blob disappears atomically for readers.
4. Probe stripes `N`, `N+1`, … until the first missing one, deleting any found. These are orphans left by an earlier write with a larger `object_size`.

### Coexistence with plain objects

A plain `data/<id>` and a striped `data/<id>.0000000000000000` may both exist, for example after toggling striping. Readers probe the plain form first. Listings report the id once, with the plain size.

## Integrity

The id in the name is the SHA-256 of the content. Writers verify it and remove the object on mismatch. Readers verify only on full reads and only warn. `config` is exempt. No other checksum is stored.

## Overlay layers

A type may map to two targets, `upper` and `lower`, to migrate between pools or namespaces without copying.

| operation | behaviour                                                       |
|-----------|-----------------------------------------------------------------|
| read      | upper first, then lower                                         |
| write     | always upper; refused if the id exists in either layer          |
| delete    | lower first, then upper; both plain and striped forms in each   |
| list      | union of both; an id in upper shadows the same id in lower      |

## Listing

A listing scans every object in the target's namespace and keeps names starting with `<type>/`. Ids come from plain names and from `.0000000000000000` stripes. Continuation stripes are ignored. Any other name is logged and skipped. There is no index object and no omap.

Cost is proportional to the number of objects in the namespace, which is why each repository needs its own namespace.

## Repository lifecycle

- **Create** writes nothing. A repository exists when `config` exists. `config` cannot be replaced.
- **Delete** of a single blob is idempotent. There are no tombstones; absence is the only record.
- **Purge** removes every owned object from every target, lower layers first. It is refused while any `snapshots/…` or `locks/…` object exists. Owned names are `config`, `config.<stripe suffix>`, and `<type>/…` for each configured type. Foreign objects are left in place and counted in the log.

## Remnants

Objects an inspector may find that are not complete blobs:

- stripe 0 with `striper.size` = `0`: interrupted striped upload
- partial plain object: interrupted upload whose cleanup also failed
- stripes with index ≥ `ceil(size / object_size)`: orphans from a previous larger `object_size`, swept on the next delete of that id
- stripe 0 with unsupported geometry: written by another tool; the server leaves it alone

## Compatibility checklist

An alternative implementation must:

- name objects exactly as in *Object names*; ids lowercase hex only
- treat `(pool, namespace)` as the repository boundary and add no prefix
- never overwrite an existing object; create exclusively
- stripe only `snapshots`, `data`, `index`, and only above the limit
- write stripe 0 with all four xattrs before any data, `striper.size` last
- write `stripe_count` = `1` and `stripe_unit` = `object_size`
- delete stripe 0 last and sweep orphans upward
- probe plain before striped, and accept both forms for the same id
- ignore names it does not recognise

It must not:

- store metadata in omap, in a manifest object, or on plain objects
- rely on a repository marker; `config` is the only existence signal
- read missing stripes as zeros

## Inspecting with the rados CLI

Substitute the repository's pool and namespace.

```sh
rados -p restic -N laptop ls
rados -p restic -N laptop stat data/<id>
rados -p restic -N laptop get data/<id> blob.bin
```

### Striped objects

A 300 MiB blob written with a 128 MiB `object_size` appears as three stripes:

```sh
rados -p restic -N laptop ls | grep '^data/'
data/<id>.0000000000000000
data/<id>.0000000000000001
data/<id>.0000000000000002
```

Metadata lives in ordinary xattrs on stripe 0:

```sh
rados -p restic -N laptop listxattr data/<id>.0000000000000000
striper.layout.object_size
striper.layout.stripe_count
striper.layout.stripe_unit
striper.size

rados -p restic -N laptop getxattr data/<id>.0000000000000000 striper.size
314572800
rados -p restic -N laptop getxattr data/<id>.0000000000000000 striper.layout.object_size
134217728
```

`--striper` reassembles the blob from the logical name without a suffix:

```sh
rados -p restic -N laptop --striper stat data/<id>
rados -p restic -N laptop --striper get data/<id> blob.bin
sha256sum blob.bin
```

`--striper ls` strips the suffix, so each blob appears once per stripe:

```sh
rados -p restic -N laptop --striper ls | sort -u
```

Because `stripe_count` is 1 and `stripe_unit` equals `object_size`, stripes are contiguous chunks. Concatenating them in order gives the same bytes:

```sh
for i in 0 1 2; do
  rados -p restic -N laptop get data/<id>.$(printf '%016x' "$i") -
done > blob.bin
```

`rados --striper put` with default geometry writes objects the server reads and lists. An object written with `stripe_count` = `2` is readable by `rados --striper` but rejected by the server.

### Remnants

```sh
rados -p restic -N laptop ls | grep -E '\.[0-9a-f]{16}$' | grep -v '\.0000000000000000$'
rados -p restic -N laptop getxattr data/<id>.0000000000000000 striper.size
```

The first lists continuation stripes; compare their count against `ceil(striper.size / object_size)`. A `striper.size` of `0` marks an interrupted upload.

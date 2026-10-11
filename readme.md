# CertMagic-S3

CertMagic S3-compatible driver written in Go.

This fork of ss098/certmagic-s3 serializes certificate issuance across every
instance that shares a bucket, thus concurrent renewals do not run into issuer
rate limits.

## Guide

Build:

```sh
xcaddy build --output ./caddy --with github.com/stepbrobd/certmagic-s3
```

Build container:

```dockerfile
FROM caddy:builder AS builder
RUN xcaddy build --with github.com/stepbrobd/certmagic-s3 --with ...

FROM caddy
COPY --from=builder /usr/bin/caddy /usr/bin/caddy
```

Run:

```sh
caddy run --config caddy.json
```

Caddyfile example:

```caddyfile
# global options

{
    storage s3 {
        host "s3.example.com"
        bucket "my-cert-bucket"
        access_id "Access ID"
        secret_key "Secret Key"
        prefix "ssl"
        insecure false # plain http when true
    }
}
```

Caddyfile example with AWS IAM role::::

```caddyfile
# global options

{
    storage s3 {
        host "s3.example.com"
        bucket "my-cert-bucket"
        use_iam_provider true
        prefix "ssl"
    }
}
```

JSON config example:

```json
{
  "storage": {
    "module": "s3",
    "host": "s3.example.com",
    "bucket": "my-cert-bucket",
    "access_id": "Access ID",
    "secret_key": "Secret Key",
    "prefix": "ssl",
    "insecure": false
  },
  "app": {
    ...
  }
}
```

From the environment, each variable fills the field the config leaves empty:

```sh
S3_HOST
S3_BUCKET
S3_ACCESS_ID
S3_SECRET_KEY
S3_PREFIX
S3_INSECURE
S3_USE_IAM_PROVIDER
```

## Locking

Certificate issuance, storage cleaning, ACME account registration and ECH key
rotation are serialized across every instance sharing a bucket.

Locks are objects under the `locks/` prefix. A lock is created with
`If-None-Match: *`, only one instance can take a given name, and the holder
rewrites it every 15 seconds with `If-Match` on the version it last wrote. A
lock whose last write is older than 1 minute is stale, and a single waiter
reclaims it with the same compare-and-swap. A release writes a released marker
the same way instead of deleting the object, a late release can never remove a
lock another instance reclaimed in the meantime. A swap that fails because one
of the holder's own writes landed unanswered, such as a refresh the release
canceled, reads the lock back and continues from the stored version while it
still names the holder. Waiters poll at 1 second intervals, and a transient
failure is retried a few times before an acquisition or a release fails.

Staleness is judged from the object's last modification time on the server, the
holder's clock never enters the protocol. Waiters compare that time with their
own clock, which assumes the instances keep their clock drift within a few
seconds of the server.

Released locks stay in the bucket, one object per lock name. A lifecycle rule
that expires objects under `locks/` after 1 day removes them, a held lock is
rewritten every 15 seconds and never gets that old.

The backend has to honor `If-None-Match` and `If-Match` on PutObject, and the
plugin writes one probe object at startup to prove it, refusing to start when
the backend accepts a write it had to refuse. Any other failure, a name that
does not resolve yet, an endpoint that does not answer or a certificate rejected
by a clock not yet set, is retried for 1 minute before the start fails.
Cloudflare R2, AWS S3, MinIO and SeaweedFS honor the headers. Garage 1.3.1
accepts the headers and ignores them.

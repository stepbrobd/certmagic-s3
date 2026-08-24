# CertMagic-S3

CertMagic S3-compatible driver written in Go.

## Guide

Build

    go install github.com/caddyserver/xcaddy/cmd/xcaddy@latest

    xcaddy build --output ./caddy --with github.com/stepbrobd/certmagic-s3

Build container

    FROM caddy:builder AS builder
    RUN xcaddy build --with github.com/stepbrobd/certmagic-s3 --with ...

    FROM caddy
    COPY --from=builder /usr/bin/caddy /usr/bin/caddy

Run

    caddy run --config caddy.json

Caddyfile Example

    # Global Config

    {
        storage s3 {
            host "s3.example.com"
            bucket "my-cert-bucket"
            access_id "Access ID"
            secret_key "Secret Key"
            prefix "ssl"
            insecure false #disables SSL if true
        }
    }

JSON Config Example

    {
      "storage": {
        "module": "s3",
        "host": "s3.example.com",
        "bucket": "my-cert-bucket",
        "access_id": "Access ID",
        "secret_key": "Secret Key",
        "prefix": "ssl",
        "insecure": false
      }
      "app": {
        ...
      }
    }

From Environment

    S3_HOST
    S3_BUCKET
    S3_ACCESS_ID
    S3_SECRET_KEY
    S3_PREFIX
    S3_INSECURE

## Locking

Certificate issuance is serialized across every instance sharing a bucket.

Locks are objects under the `locks/` prefix. They are created with
`If-None-Match: *` so exactly one instance can take a given lock, and the holder
refreshes its own lock every 15s. An instance that stops refreshing loses the
lock after 60s, and a single waiter reclaims it with a compare-and-swap on the
object's ETag.

This needs a backend that honours conditional writes on PutObject. Verified
against Cloudflare R2 and MinIO. AWS S3 has supported them since November 2024.

AWS IAM Provider Example

Caddyfile Example

    # Global Config

    {
        storage s3 {
            host "s3.example.com"
            bucket "my-cert-bucket"
            use_iam_provider true
            prefix "ssl"
            insecure false #disables SSL if true
        }
    }

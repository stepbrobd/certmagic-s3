package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/zap"
)

// these exercise the locker against a real endpoint
// without S3_TEST_HOST they skip so `go test ./...` stays green offline
func newTestStorage(t *testing.T, prefix string) *S3 {
	t.Helper()

	host := envOr("S3_TEST_HOST", "")
	if host == "" {
		t.Skip("S3_TEST_HOST is not set")
	}

	client, err := minio.New(host, &minio.Options{
		Creds:  credentials.NewStaticV4(envOr("S3_TEST_ACCESS_ID", ""), envOr("S3_TEST_SECRET_KEY", ""), ""),
		Secure: envOr("S3_TEST_INSECURE", "") == "",
	})
	if err != nil {
		t.Fatalf("minio client: %v", err)
	}

	bucket := envOr("S3_TEST_BUCKET", "certmagic-test")

	exists, err := client.BucketExists(t.Context(), bucket)
	if err != nil {
		t.Fatalf("bucket exists: %v", err)
	}
	if !exists {
		if err := client.MakeBucket(t.Context(), bucket, minio.MakeBucketOptions{}); err != nil {
			t.Fatalf("make bucket: %v", err)
		}
	}

	return &S3{
		logger: zap.NewNop(),
		client: client,
		gates:  make(map[string]chan struct{}),
		locks:  make(map[string]*heldLock),
		Bucket: bucket,
		Prefix: prefix,
	}
}

func testPrefix(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("certmagic-test/%s-%d", t.Name(), time.Now().UnixNano())
}

// the block is parsed by the dispenser, so a typo, a missing or a stray
// argument is an error at adapt time rather than a silent fallback
func TestUnmarshalCaddyfile(t *testing.T) {
	var storage S3

	err := storage.UnmarshalCaddyfile(caddyfile.NewTestDispenser(`s3 {
		host s3.example.com:9000
		bucket certs
		access_id id
		secret_key secret
		prefix caddy
		insecure true
		use_iam_provider false
	}`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	got := []any{storage.Host, storage.Bucket, storage.AccessID, storage.SecretKey, storage.Prefix, storage.Insecure, storage.UseIamProvider}
	want := []any{"s3.example.com:9000", "certs", "id", "secret", "caddy", true, false}
	if !slices.Equal(got, want) {
		t.Fatalf("parsed %v, want %v", got, want)
	}

	for _, bad := range []string{
		"s3 {\n\tacess_id id\n}",
		"s3 {\n\tbucket\n}",
		"s3 {\n\tbucket a b\n}",
		"s3 {\n\tinsecure maybe\n}",
		"s3 extra {\n\tbucket certs\n}",
	} {
		if err := new(S3).UnmarshalCaddyfile(caddyfile.NewTestDispenser(bad)); err == nil {
			t.Errorf("%q parsed without an error", bad)
		}
	}
}

// Provision trims the prefix and proves the backend honors conditional writes
// before any lock depends on them
func TestProvision(t *testing.T) {
	prefix := testPrefix(t)
	backend := newTestStorage(t, prefix)

	ctx, cancel := caddy.NewContext(caddy.Context{Context: t.Context()})
	defer cancel()

	storage := &S3{
		Host:      envOr("S3_TEST_HOST", ""),
		Bucket:    backend.Bucket,
		AccessID:  envOr("S3_TEST_ACCESS_ID", ""),
		SecretKey: envOr("S3_TEST_SECRET_KEY", ""),
		Prefix:    "/" + prefix + "/",
		Insecure:  envOr("S3_TEST_INSECURE", "") != "",
	}
	if err := storage.Provision(ctx); err != nil {
		t.Fatalf("provision: %v", err)
	}

	if storage.Prefix != prefix {
		t.Fatalf("prefix %q was not trimmed, got %q", "/"+prefix+"/", storage.Prefix)
	}
	if storage.Exists(t.Context(), lockPrefix) {
		t.Fatal("the probe object was left behind")
	}
}

// mutual exclusion is the entire point of the locker
func TestLockIsMutuallyExclusive(t *testing.T) {
	const instances = 8

	prefix := testPrefix(t)
	name := "issue_cert_example.com"

	// build every instance up front, t.Skip and t.Fatal are not valid in goroutines
	nodes := make([]*S3, instances)
	for i := range nodes {
		nodes[i] = newTestStorage(t, prefix)
	}

	var concurrent, overlaps, acquired atomic.Int32
	var wg sync.WaitGroup

	for _, node := range nodes {
		wg.Add(1)
		go func(node *S3) {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()

			if err := node.Lock(ctx, name); err != nil {
				t.Errorf("lock: %v", err)
				return
			}

			if concurrent.Add(1) != 1 {
				overlaps.Add(1)
			}
			acquired.Add(1)
			time.Sleep(150 * time.Millisecond)
			concurrent.Add(-1)

			if err := node.Unlock(ctx, name); err != nil {
				t.Errorf("unlock: %v", err)
			}
		}(node)
	}

	wg.Wait()

	if got := overlaps.Load(); got != 0 {
		t.Fatalf("lock was held concurrently %d times", got)
	}
	if got := acquired.Load(); got != instances {
		t.Fatalf("%d of %d instances acquired the lock", got, instances)
	}
}

// a holder keeps rewriting its lock, so a waiter polling past the expiry never
// takes it, and a release hands it over at once
func TestLockHeldPastExpiry(t *testing.T) {
	prefix := testPrefix(t)
	name := "issue_cert_held.example.com"

	holder := newTestStorage(t, prefix)
	waiter := newTestStorage(t, prefix)

	shorten(t, &lockRefreshInterval, time.Second)
	shorten(t, &lockTTL, 3*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := holder.Lock(ctx, name); err != nil {
		t.Fatalf("holder lock: %v", err)
	}

	if held, err := waiter.TryLock(ctx, name); held || err != nil {
		t.Fatalf("try lock while held: %v, %v", held, err)
	}

	acquired := make(chan error, 1)
	go func() { acquired <- waiter.Lock(ctx, name) }()

	// two expiries pass while the holder keeps refreshing
	select {
	case err := <-acquired:
		t.Fatalf("waiter took a held lock: %v", err)
	case <-time.After(7 * time.Second):
	}

	if err := holder.Unlock(ctx, name); err != nil {
		t.Fatalf("holder unlock: %v", err)
	}

	select {
	case err := <-acquired:
		if err != nil {
			t.Fatalf("waiter lock: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("waiter never acquired the lock after release")
	}

	if err := waiter.Unlock(ctx, name); err != nil {
		t.Fatalf("waiter unlock: %v", err)
	}

	if held, err := holder.TryLock(ctx, name); !held || err != nil {
		t.Fatalf("try lock while free: %v, %v", held, err)
	}
	if err := holder.Unlock(ctx, name); err != nil {
		t.Fatalf("holder unlock after try: %v", err)
	}
}

// an instance that died mid-issuance must not wedge the lock forever
func TestLockReclaimsAbandoned(t *testing.T) {
	prefix := testPrefix(t)
	name := "issue_cert_abandoned.example.com"

	storage := newTestStorage(t, prefix)
	key := storage.lockKey(name)

	// the seeded lock is stale once its write is older than the expiry
	shorten(t, &lockTTL, 2*time.Second)

	abandoned, err := json.Marshal(lockInfo{
		Owner:   "dead-node/deadbeef",
		Created: time.Now().Add(-2 * time.Hour),
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	if _, err := storage.client.PutObject(t.Context(), storage.Bucket, key,
		bytes.NewReader(abandoned), int64(len(abandoned)), minio.PutObjectOptions{}); err != nil {
		t.Fatalf("seed abandoned lock: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if err := storage.Lock(ctx, name); err != nil {
		t.Fatalf("lock: %v", err)
	}
	defer storage.Unlock(context.Background(), name)

	stored, _, err := storage.loadLock(context.Background(), key)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if stored.Owner == "dead-node/deadbeef" {
		t.Fatal("abandoned lock was not reclaimed")
	}
}

// shorten sets a lock interval for one test so it runs in seconds
func shorten(t *testing.T, interval *time.Duration, d time.Duration) {
	t.Helper()
	previous := *interval
	*interval = d
	t.Cleanup(func() { *interval = previous })
}

// envOr keeps the test setup free of repeated os.Getenv noise
func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

// certmagic walks storage by path component, so a non-recursive listing returns
// the children of a directory and never a sibling sharing a string prefix
func TestList(t *testing.T) {
	prefix := testPrefix(t)
	storage := newTestStorage(t, prefix)

	ctx := t.Context()

	for _, key := range []string{
		"certificates/issuer/a.example.com/a.example.com.crt",
		"certificates/issuer/a.example.com/a.example.com.key",
		"certificates/issuer/b.example.com/b.example.com.crt",
		"certificates_backup/x",
	} {
		if err := storage.Store(ctx, key, []byte("x")); err != nil {
			t.Fatalf("store %s: %v", key, err)
		}
	}

	// a live lock must not show up as stored data
	if err := storage.Lock(ctx, "issue_cert_a"); err != nil {
		t.Fatalf("lock: %v", err)
	}
	defer storage.Unlock(ctx, "issue_cert_a")

	cases := []struct {
		prefix    string
		recursive bool
		want      []string
	}{
		{"certificates", false, []string{"certificates/issuer"}},
		{"certificates/issuer", false, []string{"certificates/issuer/a.example.com", "certificates/issuer/b.example.com"}},
		{"certificates/issuer/a.example.com", false, []string{"certificates/issuer/a.example.com/a.example.com.crt", "certificates/issuer/a.example.com/a.example.com.key"}},
		{"certificates", true, []string{
			"certificates/issuer/a.example.com/a.example.com.crt",
			"certificates/issuer/a.example.com/a.example.com.key",
			"certificates/issuer/b.example.com/b.example.com.crt",
		}},
		{"", false, []string{"certificates", "certificates_backup"}},
		{"missing", false, nil},
	}

	for _, c := range cases {
		got, err := storage.List(ctx, c.prefix, c.recursive)
		if err != nil {
			t.Fatalf("list %q: %v", c.prefix, err)
		}
		sort.Strings(got)
		if !slices.Equal(got, c.want) {
			t.Errorf("List(%q, %v) = %q, want %q", c.prefix, c.recursive, got, c.want)
		}
	}

	// the whole bucket must not expose the lock subtree either
	all, err := storage.List(ctx, "", true)
	if err != nil {
		t.Fatalf("list all: %v", err)
	}
	for _, key := range all {
		if strings.HasPrefix(key, lockPrefix+"/") {
			t.Fatalf("lock key %q leaked into a data listing", key)
		}
	}
}

// a missing key is fs.ErrNotExist, a stored value is terminal and loads back,
// and a key with other keys below it is a directory that exists
func TestStatAndLoad(t *testing.T) {
	prefix := testPrefix(t)
	storage := newTestStorage(t, prefix)

	ctx := t.Context()

	if _, err := storage.Stat(ctx, "certificates/absent.crt"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("expected fs.ErrNotExist for a missing key, got %v", err)
	}
	if _, err := storage.Load(ctx, "certificates/absent.crt"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("expected fs.ErrNotExist for a missing load, got %v", err)
	}

	if err := storage.Store(ctx, "certificates/present.crt", []byte("hello")); err != nil {
		t.Fatalf("store: %v", err)
	}

	got, err := storage.Load(ctx, "certificates/present.crt")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if !bytes.Equal(got, []byte("hello")) {
		t.Fatalf("loaded %q, want %q", got, "hello")
	}

	info, err := storage.Stat(ctx, "certificates/present.crt")
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Key != "certificates/present.crt" {
		t.Fatalf("expected the unprefixed key back, got %q", info.Key)
	}
	if info.Size != 5 {
		t.Fatalf("expected size 5, got %d", info.Size)
	}
	if !info.IsTerminal {
		t.Fatal("a stored value must be terminal")
	}
	if info.Modified.IsZero() {
		t.Fatal("modified time is missing")
	}

	info, err = storage.Stat(ctx, "certificates")
	if err != nil {
		t.Fatalf("stat directory: %v", err)
	}
	if info.IsTerminal {
		t.Fatal("a directory must not be terminal")
	}
	if info.Key != "certificates" {
		t.Fatalf("expected the directory key back, got %q", info.Key)
	}

	if !storage.Exists(ctx, "certificates") {
		t.Fatal("a directory must exist")
	}
	if storage.Exists(ctx, "cert") {
		t.Fatal("a string prefix of a directory must not exist")
	}
}

// certmagic removes a directory by naming it and expects everything below to go
func TestDeleteRemovesEverythingBelow(t *testing.T) {
	prefix := testPrefix(t)
	storage := newTestStorage(t, prefix)

	ctx := t.Context()

	keys := []string{
		"ech/configs/abc",
		"ech/configs/abc/key.bin",
		"ech/configs/abc/config.bin",
		"ech/configs/abc/meta.json",
	}
	for _, key := range keys {
		if err := storage.Store(ctx, key, []byte("x")); err != nil {
			t.Fatalf("store %s: %v", key, err)
		}
	}

	// a sibling sharing the name as a string prefix must survive
	if err := storage.Store(ctx, "ech/configs/abcdef", []byte("x")); err != nil {
		t.Fatalf("store sibling: %v", err)
	}

	if err := storage.Delete(ctx, "ech/configs/abc"); err != nil {
		t.Fatalf("delete: %v", err)
	}

	for _, key := range keys {
		if storage.Exists(ctx, key) {
			t.Fatalf("%s survived the delete", key)
		}
	}

	if !storage.Exists(ctx, "ech/configs/abcdef") {
		t.Fatal("delete of ech/configs/abc also removed ech/configs/abcdef")
	}

	// deleting something already gone is not an error
	if err := storage.Delete(ctx, "ech/configs/abc"); err != nil {
		t.Fatalf("second delete: %v", err)
	}
}

// certmagic takes short lived locks constantly for storage cleaning and ARI,
// so a waiter routinely reads a lock its holder releases mid read
// that must look like a free lock, never like an error, and never overlap
func TestLockChurn(t *testing.T) {
	prefix := testPrefix(t)
	ctx := t.Context()

	const (
		contenders = 6
		rounds     = 12
	)

	nodes := make([]*S3, contenders)
	for i := range nodes {
		nodes[i] = newTestStorage(t, prefix)
	}

	var (
		concurrent, overlaps atomic.Int32
		failures             atomic.Int64
		first                atomic.Value
		wg                   sync.WaitGroup
	)

	for i, node := range nodes {
		wg.Go(func() {
			for range rounds {
				if err := node.Lock(ctx, "churn"); err != nil {
					failures.Add(1)
					first.CompareAndSwap(nil, fmt.Sprintf("contender %d: %v", i, err))

					continue
				}

				if concurrent.Add(1) != 1 {
					overlaps.Add(1)
				}
				concurrent.Add(-1)

				if err := node.Unlock(ctx, "churn"); err != nil {
					failures.Add(1)
					first.CompareAndSwap(nil, fmt.Sprintf("contender %d unlock: %v", i, err))
				}
			}
		})
	}

	wg.Wait()

	if n := overlaps.Load(); n != 0 {
		t.Fatalf("lock was held concurrently %d times", n)
	}
	if n := failures.Load(); n != 0 {
		t.Fatalf("lock churn produced %d errors, first was %v", n, first.Load())
	}
}

package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
		locks:  make(map[string]*heldLock),
		Bucket: bucket,
		Prefix: prefix,
	}
}

func testPrefix(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("certmagic-test/%s-%d", t.Name(), time.Now().UnixNano())
}

// mutual exclusion is the entire point of the locker
// with no-op Lock/Unlock every instance enters at once and this fails
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

// a lock held by a live instance must block everyone else until released
func TestLockBlocksWhileHeld(t *testing.T) {
	prefix := testPrefix(t)
	name := "issue_cert_held.example.com"

	holder := newTestStorage(t, prefix)
	waiter := newTestStorage(t, prefix)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := holder.Lock(ctx, name); err != nil {
		t.Fatalf("holder lock: %v", err)
	}

	acquired := make(chan error, 1)
	go func() { acquired <- waiter.Lock(ctx, name) }()

	select {
	case err := <-acquired:
		t.Fatalf("waiter took a held lock: %v", err)
	case <-time.After(3 * time.Second):
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
}

// an instance that died mid-issuance must not wedge the lock forever
func TestLockReclaimsAbandoned(t *testing.T) {
	prefix := testPrefix(t)
	name := "issue_cert_abandoned.example.com"

	storage := newTestStorage(t, prefix)
	key := storage.LockKey(name)

	abandoned, err := json.Marshal(lockInfo{
		Owner:   "dead-node/deadbeef",
		Created: time.Now().Add(-2 * time.Hour),
		Expires: time.Now().Add(-1 * time.Hour),
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

// concurrent writers must not tear a cert and key pair the way the outage did
func TestLockSerializesWriters(t *testing.T) {
	const writers = 6

	prefix := testPrefix(t)
	name := "issue_cert_paired.example.com"

	nodes := make([]*S3, writers)
	for i := range nodes {
		nodes[i] = newTestStorage(t, prefix)
	}

	var wg sync.WaitGroup
	for i, node := range nodes {
		wg.Add(1)
		go func(i int, node *S3) {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()

			if err := node.Lock(ctx, name); err != nil {
				t.Errorf("lock: %v", err)
				return
			}
			defer node.Unlock(ctx, name)

			// the delays are staggered so that unserialized writers always finish
			// in a different order for the two halves of the pair, which is how
			// the outage put one order's key next to another order's certificate
			// under a working lock the two stores stay atomic and the pair matches
			stamp := fmt.Appendf(nil, "writer-%d", i)

			time.Sleep(time.Duration(i) * 10 * time.Millisecond)
			if err := node.Store(ctx, "pair.key", stamp); err != nil {
				t.Errorf("store key: %v", err)
				return
			}

			time.Sleep(time.Duration(writers-1-i) * 40 * time.Millisecond)
			if err := node.Store(ctx, "pair.crt", stamp); err != nil {
				t.Errorf("store crt: %v", err)
			}
		}(i, node)
	}
	wg.Wait()

	reader := newTestStorage(t, prefix)

	key, err := reader.Load(t.Context(), "pair.key")
	if err != nil {
		t.Fatalf("load key: %v", err)
	}
	crt, err := reader.Load(t.Context(), "pair.crt")
	if err != nil {
		t.Fatalf("load crt: %v", err)
	}

	if !bytes.Equal(key, crt) {
		t.Fatalf("torn pair: key came from %q but crt came from %q", key, crt)
	}
}

// envOr keeps the test setup free of repeated os.Getenv noise
func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

// List used to size its slice with len of a channel, so every result carried
// leading empty keys, and lock objects leaked into data listings
func TestListReturnsOnlyRealKeys(t *testing.T) {
	prefix := testPrefix(t)
	storage := newTestStorage(t, prefix)

	ctx := t.Context()

	for _, key := range []string{"certificates/a.crt", "certificates/a.key", "certificates/b.crt"} {
		if err := storage.Store(ctx, key, []byte("x")); err != nil {
			t.Fatalf("store %s: %v", key, err)
		}
	}

	// a live lock must not show up as stored data
	if err := storage.Lock(ctx, "issue_cert_a"); err != nil {
		t.Fatalf("lock: %v", err)
	}
	defer storage.Unlock(ctx, "issue_cert_a")

	keys, err := storage.List(ctx, "certificates", true)
	if err != nil {
		t.Fatalf("list: %v", err)
	}

	if len(keys) != 3 {
		t.Fatalf("expected 3 keys, got %d: %q", len(keys), keys)
	}

	for _, key := range keys {
		if key == "" {
			t.Fatalf("empty key in %q", keys)
		}
		if strings.HasPrefix(key, "/") {
			t.Fatalf("key %q keeps a leading separator", key)
		}
		if !strings.HasPrefix(key, "certificates/") {
			t.Fatalf("unexpected key %q", key)
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

// Stat used to swallow every failure and answer with a zero KeyInfo and a nil
// error, so a missing key was indistinguishable from an empty one
func TestStatReportsMissingKeys(t *testing.T) {
	prefix := testPrefix(t)
	storage := newTestStorage(t, prefix)

	ctx := t.Context()

	if _, err := storage.Stat(ctx, "certificates/absent.crt"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("expected fs.ErrNotExist for a missing key, got %v", err)
	}

	if err := storage.Store(ctx, "certificates/present.crt", []byte("hello")); err != nil {
		t.Fatalf("store: %v", err)
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
}

// Load must still report a missing key as fs.ErrNotExist now that it no longer
// asks Exists first
func TestLoadMissingKey(t *testing.T) {
	prefix := testPrefix(t)
	storage := newTestStorage(t, prefix)

	ctx := t.Context()

	if _, err := storage.Load(ctx, "certificates/absent.crt"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("expected fs.ErrNotExist, got %v", err)
	}

	want := []byte("certificate bytes")
	if err := storage.Store(ctx, "certificates/present.crt", want); err != nil {
		t.Fatalf("store: %v", err)
	}

	got, err := storage.Load(ctx, "certificates/present.crt")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("loaded %q, want %q", got, want)
	}
}

// certmagic removes a directory by naming it, deleting only the single object
// left expired ech configs and old certificate folders behind forever
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

// certmagic takes short lived locks constantly for storage cleaning and ARI
// so a waiter routinely reads a lock that its holder deletes mid read
// that must look like a free lock, never like an error
func TestLockChurnNeverErrors(t *testing.T) {
	storage := newTestStorage(t, testPrefix(t))
	ctx := t.Context()

	const (
		contenders = 6
		rounds     = 12
	)

	var (
		failures atomic.Int64
		first    atomic.Value
		wg       sync.WaitGroup
	)

	for i := range contenders {

		wg.Go(func() {

			for range rounds {
				if err := storage.Lock(ctx, "churn"); err != nil {
					failures.Add(1)
					first.CompareAndSwap(nil, fmt.Sprintf("contender %d: %v", i, err))

					continue
				}

				if err := storage.Unlock(ctx, "churn"); err != nil {
					failures.Add(1)
					first.CompareAndSwap(nil, fmt.Sprintf("contender %d unlock: %v", i, err))
				}
			}
		})
	}

	wg.Wait()

	if n := failures.Load(); n != 0 {
		t.Fatalf("lock churn produced %d errors, first was %v", n, first.Load())
	}
}

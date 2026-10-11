package s3

import (
	"bytes"
	"cmp"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/certmagic"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/zap"
)

var (
	// implementing these interfaces
	_ caddy.Module          = (*S3)(nil)
	_ certmagic.Storage     = (*S3)(nil)
	_ certmagic.Locker      = (*S3)(nil)
	_ certmagic.TryLocker   = (*S3)(nil)
	_ caddy.Provisioner     = (*S3)(nil)
	_ caddyfile.Unmarshaler = (*S3)(nil)
)

const (
	// locks live under their own prefix so List never mixes them into data results
	lockPrefix = "locks"

	// how many transient s3 failures to absorb before failing an acquisition or a release
	lockRetryLimit = 5

	// how long removing the probe object may take, the probe deadline may have passed
	probeCleanupTimeout = 10 * time.Second
)

// the tests shrink these to run in seconds
var (
	// a lock whose last write is older than this is stale and may be reclaimed
	lockTTL = time.Minute

	// how often a holder rewrites the lock it owns
	lockRefreshInterval = 15 * time.Second

	// how often a waiter retries acquisition
	lockPollInterval = time.Second

	// how long Provision retries a probe that got no definite answer, a boot can
	// start caddy before dns answers or the clock is set
	probeTimeout = time.Minute
)

// errUnconditionalWrites is the one probe result that retrying cannot change
var errUnconditionalWrites = errors.New("it does not honor conditional writes and locks would exclude nobody")

// lockInfo is the body of a lock object
// staleness is judged from the object's last write on the server, no holder
// clock enters the protocol
type lockInfo struct {
	Owner     string    `json:"owner"`
	Created   time.Time `json:"created"`
	Refreshed time.Time `json:"refreshed,omitzero"`
	Released  bool      `json:"released,omitempty"`
}

// heldLock is a lock this instance owns
// etag is the version its last write produced, the refresher owns it until
// done is closed and a release replaces exactly that version
type heldLock struct {
	owner   string
	created time.Time
	etag    string
	cancel  context.CancelFunc
	done    chan struct{}
}

func init() {
	caddy.RegisterModule(&S3{})
}

type S3 struct {
	logger *zap.Logger
	client *minio.Client

	// one holder per name in this process, taken before the s3 protocol so
	// every Unlock pairs with its own Lock
	gates map[string]chan struct{}

	// locks currently owned by this instance, keyed by lock object key
	locks   map[string]*heldLock
	locksMu sync.Mutex

	// S3 configuration
	Host           string `json:"host"`
	Bucket         string `json:"bucket"`
	AccessID       string `json:"access_id"`
	SecretKey      string `json:"secret_key"`
	Prefix         string `json:"prefix,omitempty"`
	Insecure       bool   `json:"insecure"`
	UseIamProvider bool   `json:"use_iam_provider"`
}

func (s3 *S3) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	// the string fields are replaced in Provision, a placeholder in a boolean
	// has to resolve before it reaches strconv
	repl := caddy.NewReplacer()

	if !d.Next() {
		return d.Err("expected tokens")
	}
	if d.NextArg() {
		return d.ArgErr()
	}

	for d.NextBlock(0) {
		key := d.Val()

		if !d.NextArg() {
			return d.ArgErr()
		}
		value := d.Val()
		if d.NextArg() {
			return d.ArgErr()
		}

		switch key {
		case "host":
			s3.Host = value
		case "bucket":
			s3.Bucket = value
		case "access_id":
			s3.AccessID = value
		case "secret_key":
			s3.SecretKey = value
		case "prefix":
			s3.Prefix = value
		case "insecure", "use_iam_provider":
			flag, err := strconv.ParseBool(repl.ReplaceKnown(value, ""))
			if err != nil {
				return d.Errf("%s: %v", key, err)
			}
			if key == "insecure" {
				s3.Insecure = flag
			} else {
				s3.UseIamProvider = flag
			}
		default:
			return d.Errf("unrecognized subdirective %s", key)
		}
	}

	return nil
}

func (s3 *S3) Provision(ctx caddy.Context) error {
	repl := caddy.NewReplacer()

	s3.Host = repl.ReplaceKnown(s3.Host, "")
	s3.Bucket = repl.ReplaceKnown(s3.Bucket, "")
	s3.AccessID = repl.ReplaceKnown(s3.AccessID, "")
	s3.SecretKey = repl.ReplaceKnown(s3.SecretKey, "")
	s3.Prefix = repl.ReplaceKnown(s3.Prefix, "")

	s3.logger = ctx.Logger(s3)

	// Load Environment
	if s3.Host == "" {
		s3.Host = os.Getenv("S3_HOST")
	}

	err := validateHost(s3.Host)
	if err != nil {
		return err
	}

	if !s3.UseIamProvider {
		if value := os.Getenv("S3_USE_IAM_PROVIDER"); value != "" {
			s3.UseIamProvider, err = strconv.ParseBool(value)
			if err != nil {
				return fmt.Errorf("S3_USE_IAM_PROVIDER: %w", err)
			}
		}
	}

	if s3.Bucket == "" {
		s3.Bucket = os.Getenv("S3_BUCKET")
		if s3.Bucket == "" {
			return errors.New("bucket is empty")
		}
	}

	if s3.AccessID == "" {
		s3.AccessID = os.Getenv("S3_ACCESS_ID")
		if s3.AccessID == "" && !s3.UseIamProvider {
			return errors.New("access_id is empty and use_iam_provider is false")
		}
	}

	if s3.SecretKey == "" {
		s3.SecretKey = os.Getenv("S3_SECRET_KEY")
		if s3.SecretKey == "" && !s3.UseIamProvider {
			return errors.New("secret_key is empty and use_iam_provider is false")
		}
	}

	if s3.Prefix == "" {
		s3.Prefix = os.Getenv("S3_PREFIX")
	}
	// a separator at either end puts every key under a "/" directory that listings never find
	s3.Prefix = strings.Trim(s3.Prefix, "/")

	if !s3.Insecure {
		if value := os.Getenv("S3_INSECURE"); value != "" {
			s3.Insecure, err = strconv.ParseBool(value)
			if err != nil {
				return fmt.Errorf("S3_INSECURE: %w", err)
			}
		}
	}

	var creds *credentials.Credentials
	if s3.UseIamProvider {
		s3.logger.Info("using iam aws provider for credentials")
		creds = credentials.NewIAM("")
	} else {
		s3.logger.Info("using secret_key and access_id for credentials")
		creds = credentials.NewStaticV4(s3.AccessID, s3.SecretKey, "")
	}

	// S3 Client
	client, err := minio.New(s3.Host, &minio.Options{
		Creds:  creds,
		Secure: !s3.Insecure,
	})
	if err != nil {
		return err
	}

	s3.client = client
	s3.gates = make(map[string]chan struct{})
	s3.locks = make(map[string]*heldLock)

	return s3.verifyConditionalWrites(ctx)
}

// verifyConditionalWrites fails when the backend accepts a conditional put it
// had to refuse, since locks would then exclude nobody
// any other failure leaves that question open and is retried until probeTimeout
func (s3 *S3) verifyConditionalWrites(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()

	gaveUp := func(err error) error {
		return fmt.Errorf("gave up after %s: %w", probeTimeout, err)
	}

	var last error
	for {
		err := s3.probeConditionalWrites(ctx)
		if err == nil || errors.Is(err, errUnconditionalWrites) {
			return err
		}
		// a request the deadline cut short says less than the failure before it
		if ctx.Err() != nil {
			return gaveUp(cmp.Or(last, err))
		}
		last = err

		s3.logger.Warn("probing the bucket failed, retrying", zap.String("bucket", s3.Bucket), zap.Error(err))

		if wait(ctx, lockPollInterval) != nil {
			return gaveUp(last)
		}
	}
}

// probeConditionalWrites makes one attempt, each under a fresh key so a write
// that landed unseen in an earlier attempt cannot fail this one
func (s3 *S3) probeConditionalWrites(ctx context.Context) error {
	owner, err := newLockOwner()
	if err != nil {
		return err
	}

	key := s3.lockKey("probe-" + path.Base(owner))
	probe := lockInfo{Owner: owner, Created: time.Now()}

	etag, err := s3.writeLock(ctx, key, probe, "")
	if err != nil {
		return fmt.Errorf("writing a probe object to bucket %s: %w", s3.Bucket, err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), probeCleanupTimeout)
		defer cancel()

		if err := s3.client.RemoveObject(ctx, s3.Bucket, key, minio.RemoveObjectOptions{}); err != nil {
			s3.logger.Warn("removing the probe object failed", zap.String("key", key), zap.Error(err))
		}
	}()

	if _, err := s3.writeLock(ctx, key, probe, ""); !isPreconditionFailed(err) {
		return conditionalWriteError("a create of an existing object", err)
	}
	if _, err := s3.writeLock(ctx, key, probe, strings.Repeat("0", len(etag))); !isPreconditionFailed(err) {
		return conditionalWriteError("a replace with a wrong etag", err)
	}

	return nil
}

// conditionalWriteError describes a probe the backend had to refuse with a 412
func conditionalWriteError(what string, err error) error {
	if err == nil {
		return fmt.Errorf("the backend accepted %s, %w", what, errUnconditionalWrites)
	}

	return fmt.Errorf("the backend answered %s with %w instead of a 412", what, err)
}

func (*S3) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID: "caddy.storage.s3",
		New: func() caddy.Module {
			return &S3{}
		},
	}
}

func (s3 *S3) CertMagicStorage() (certmagic.Storage, error) {
	return s3, nil
}

// Lock blocks until this instance owns the named lock or ctx ends
func (s3 *S3) Lock(ctx context.Context, name string) error {
	key := s3.lockKey(name)

	owner, err := newLockOwner()
	if err != nil {
		return err
	}

	gate := s3.gate(key)
	select {
	case gate <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}

	err = s3.retry(ctx, key, "acquiring lock", func() (bool, error) {
		return s3.acquire(ctx, key, owner)
	})
	if err != nil {
		<-gate
	}

	return err
}

// TryLock takes the named lock when it is free and reports false without
// waiting when a live holder has it
func (s3 *S3) TryLock(ctx context.Context, name string) (bool, error) {
	key := s3.lockKey(name)

	owner, err := newLockOwner()
	if err != nil {
		return false, err
	}

	gate := s3.gate(key)
	select {
	case gate <- struct{}{}:
	default:
		return false, nil
	}

	held, err := s3.acquire(ctx, key, owner)
	if !held {
		<-gate
	}

	return held, err
}

// Unlock stops refreshing the named lock and marks it released
// the release replaces exactly the version this instance last wrote, so a
// lock that went stale and was reclaimed meanwhile stays with its new holder
func (s3 *S3) Unlock(ctx context.Context, name string) error {
	key := s3.lockKey(name)

	s3.locksMu.Lock()
	held, mine := s3.locks[key]
	delete(s3.locks, key)
	s3.locksMu.Unlock()

	if !mine {
		s3.logger.Debug("unlock of a lock not held here, leaving it to expire", zap.String("key", key))
		return nil
	}
	defer func() { <-s3.gate(key) }()

	// no refresh may land after the version the release replaces was read
	held.cancel()
	<-held.done

	release := lockInfo{Owner: held.owner, Created: held.created, Released: true}

	return s3.retry(ctx, key, "releasing lock", func() (bool, error) {
		_, err := s3.writeLock(ctx, key, release, held.etag)
		switch {
		case err == nil, isNotExist(err):
			return true, nil
		case isPreconditionFailed(err):
			s3.logger.Warn("lock was reclaimed, leaving it", zap.String("key", key))
			return true, nil
		}
		return false, err
	})
}

// acquire makes one attempt at the lock
// false with a nil error means a live holder has it
func (s3 *S3) acquire(ctx context.Context, key, owner string) (bool, error) {
	claim := lockInfo{Owner: owner, Created: time.Now()}

	etag, err := s3.writeLock(ctx, key, claim, "")
	if err == nil {
		s3.trackLock(ctx, key, claim, etag)
		return true, nil
	}
	if !isPreconditionFailed(err) {
		return false, err
	}

	existing, stat, err := s3.loadLock(ctx, key)
	if err != nil {
		// released between the create and this read, the next attempt creates it
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}
		return false, err
	}

	if existing.Owner == owner {
		// the create went through and only its response was lost
		s3.trackLock(ctx, key, claim, stat.ETag)
		return true, nil
	}

	stale := time.Since(stat.LastModified) > lockTTL
	if !existing.Released && !stale {
		return false, nil
	}
	if stale {
		s3.logger.Warn("reclaiming an abandoned lock", zap.String("key", key), zap.String("owner", existing.Owner), zap.Time("last_write", stat.LastModified))
	}

	// swapping on the etag lets exactly one waiter take a free lock
	etag, err = s3.writeLock(ctx, key, claim, stat.ETag)
	if err == nil {
		s3.trackLock(ctx, key, claim, etag)
		return true, nil
	}
	if isPreconditionFailed(err) {
		return false, nil
	}
	return false, err
}

// retry runs attempt until it reports done, absorbing up to lockRetryLimit
// transient failures and pausing a poll interval between attempts
func (s3 *S3) retry(ctx context.Context, key, what string, attempt func() (bool, error)) error {
	transient := 0

	for {
		done, err := attempt()
		if done {
			return err
		}
		if err == nil {
			// a healthy wait behind a live holder pays for no earlier hiccup
			transient = 0
		} else {
			// a hiccup talking to s3 must not fail issuance, a persistent fault must
			transient++
			if transient > lockRetryLimit {
				return err
			}
			s3.logger.Warn(what+" failed, retrying", zap.String("key", key), zap.Error(err))
		}

		if err := wait(ctx, lockPollInterval); err != nil {
			return err
		}
	}
}

// writeLock stores body at key and returns the version written
// an empty etag creates the object only when none exists, otherwise exactly
// the version carrying etag is replaced
func (s3 *S3) writeLock(ctx context.Context, key string, body lockInfo, etag string) (string, error) {
	encoded, err := json.Marshal(body)
	if err != nil {
		return "", err
	}

	opts := minio.PutObjectOptions{ContentType: "application/json"}
	if etag == "" {
		opts.SetMatchETagExcept("*")
	} else {
		opts.SetMatchETag(etag)
	}

	info, err := s3.client.PutObject(ctx, s3.Bucket, key, bytes.NewReader(encoded), int64(len(encoded)), opts)
	if err != nil {
		return "", err
	}

	return info.ETag, nil
}

// loadLock returns the stored lock and the object it was read from
func (s3 *S3) loadLock(ctx context.Context, key string) (lockInfo, minio.ObjectInfo, error) {
	var stored lockInfo

	object, err := s3.client.GetObject(ctx, s3.Bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return stored, minio.ObjectInfo{}, err
	}
	defer object.Close()

	// reading first keeps this to one request, a stat before the read makes
	// minio-go condition the read on the etag it saw and fail once the lock moves
	body, err := io.ReadAll(object)
	if err != nil {
		if isNotExist(err) {
			return stored, minio.ObjectInfo{}, fs.ErrNotExist
		}
		return stored, minio.ObjectInfo{}, err
	}

	stat, err := object.Stat()
	if err != nil {
		return stored, minio.ObjectInfo{}, err
	}

	// an unreadable body still ages out by its last write
	if err := json.Unmarshal(body, &stored); err != nil {
		s3.logger.Warn("lock is unreadable", zap.String("key", key), zap.Error(err))
		return lockInfo{}, stat, nil
	}

	return stored, stat, nil
}

// trackLock records the lock and starts rewriting it until Unlock
func (s3 *S3) trackLock(ctx context.Context, key string, claim lockInfo, etag string) {
	// the refresher has to outlive the caller context, Unlock is what stops it
	refresh, cancel := context.WithCancel(context.WithoutCancel(ctx))

	held := &heldLock{owner: claim.Owner, created: claim.Created, etag: etag, cancel: cancel, done: make(chan struct{})}

	s3.locksMu.Lock()
	s3.locks[key] = held
	s3.locksMu.Unlock()

	go s3.keepLockFresh(refresh, key, held)
}

// gate returns the in-process semaphore of a lock key
func (s3 *S3) gate(key string) chan struct{} {
	s3.locksMu.Lock()
	defer s3.locksMu.Unlock()

	gate, ok := s3.gates[key]
	if !ok {
		gate = make(chan struct{}, 1)
		s3.gates[key] = gate
	}

	return gate
}

// keepLockFresh rewrites the lock while this instance owns it, so a live
// holder never looks stale, and records the version of every write
func (s3 *S3) keepLockFresh(ctx context.Context, key string, held *heldLock) {
	defer close(held.done)

	// a failed write is retried at the poll interval so a short outage does not
	// eat the whole margin between two refreshes
	interval := lockRefreshInterval

	for {
		if err := wait(ctx, interval); err != nil {
			return
		}

		fresh := lockInfo{Owner: held.owner, Created: held.created, Refreshed: time.Now()}

		etag, err := s3.writeLock(ctx, key, fresh, held.etag)
		if err == nil {
			held.etag = etag
			interval = lockRefreshInterval
			continue
		}
		// the swap fails only once the lock was reclaimed or removed
		if isPreconditionFailed(err) || isNotExist(err) {
			s3.logger.Warn("lock moved on, stopping refresh", zap.String("key", key), zap.Error(err))
			return
		}
		s3.logger.Warn("extending a held lock failed, retrying", zap.String("key", key), zap.Error(err))
		interval = lockPollInterval
	}
}

// lockKey returns the object key backing a named lock
func (s3 *S3) lockKey(name string) string {
	return path.Join(s3.Prefix, lockPrefix, name+".lock")
}

// newLockOwner identifies this process and attempt so a holder can tell its own
// lock from one another instance reclaimed
func newLockOwner() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}

	host, err := os.Hostname()
	if err != nil {
		return "", err
	}

	return host + "/" + hex.EncodeToString(buf), nil
}

// wait sleeps for d unless ctx ends first
func wait(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// minio only translates NoSuchKey on some paths so every read has to test it
// a bare 404 counts too, a head carries no body to name the code
func isNotExist(err error) bool {
	response := minio.ToErrorResponse(err)

	return response.Code == minio.NoSuchKey || response.StatusCode == http.StatusNotFound
}

// a conditional put that lost its race answers 412
// a 409 is a concurrent request that has to be retried, not a lost race
func isPreconditionFailed(err error) bool {
	response := minio.ToErrorResponse(err)

	return response.Code == minio.PreconditionFailed || response.StatusCode == http.StatusPreconditionFailed
}

func (s3 *S3) Store(ctx context.Context, key string, value []byte) error {
	key = s3.KeyPrefix(key)
	length := int64(len(value))

	s3.logger.Debug("storing", zap.String("key", key), zap.Int64("bytes", length))

	_, err := s3.client.PutObject(ctx, s3.Bucket, key, bytes.NewReader(value), length, minio.PutObjectOptions{})

	return err
}

func (s3 *S3) Load(ctx context.Context, key string) ([]byte, error) {
	prefixed := s3.KeyPrefix(key)

	s3.logger.Debug("loading", zap.String("key", prefixed))

	object, err := s3.client.GetObject(ctx, s3.Bucket, prefixed, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}

	defer object.Close()

	body, err := io.ReadAll(object)
	if err != nil {
		if isNotExist(err) {
			return nil, fs.ErrNotExist
		}

		return nil, err
	}

	return body, nil
}

func (s3 *S3) Delete(ctx context.Context, key string) error {
	prefixed := s3.KeyPrefix(key)

	s3.logger.Debug("deleting", zap.String("key", prefixed))

	if err := s3.client.RemoveObject(ctx, s3.Bucket, prefixed, minio.RemoveObjectOptions{}); err != nil && !isNotExist(err) {
		return err
	}

	// a directory is deleted by name and everything below goes with it
	listing := s3.client.ListObjects(ctx, s3.Bucket, minio.ListObjectsOptions{
		Prefix:    s3.dirPrefix(key),
		Recursive: true,
	})

	var below []minio.ObjectInfo

	for object := range listing {
		if object.Err != nil {
			return object.Err
		}

		below = append(below, object)
	}

	objects := make(chan minio.ObjectInfo, len(below))
	for _, object := range below {
		objects <- object
	}
	close(objects)

	for removal := range s3.client.RemoveObjects(ctx, s3.Bucket, objects, minio.RemoveObjectsOptions{}) {
		if removal.Err != nil {
			return removal.Err
		}
	}

	return nil
}

func (s3 *S3) Exists(ctx context.Context, key string) bool {
	_, err := s3.Stat(ctx, key)

	// certmagic treats a failed check as a missing key and asks the CA for a new certificate
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		s3.logger.Error("checking a key failed, reporting it missing", zap.String("key", key), zap.Error(err))
	}

	return err == nil
}

func (s3 *S3) List(ctx context.Context, prefix string, recursive bool) ([]string, error) {
	objects := s3.client.ListObjects(ctx, s3.Bucket, minio.ListObjectsOptions{
		Prefix:    s3.dirPrefix(prefix),
		Recursive: recursive,
	})

	keys := make([]string, 0)

	for object := range objects {
		if object.Err != nil {
			return nil, object.Err
		}

		// a non-recursive listing reports a directory with a trailing separator
		key := s3.CutKeyPrefix(strings.TrimSuffix(object.Key, "/"))

		// locks share the bucket but are bookkeeping, not stored data
		if key == "" || key == lockPrefix || strings.HasPrefix(key, lockPrefix+"/") {
			continue
		}

		keys = append(keys, key)
	}

	return keys, nil
}

func (s3 *S3) Stat(ctx context.Context, key string) (certmagic.KeyInfo, error) {
	prefixed := s3.KeyPrefix(key)

	object, err := s3.client.StatObject(ctx, s3.Bucket, prefixed, minio.StatObjectOptions{})
	if err == nil {
		s3.logger.Debug("stat", zap.String("key", prefixed), zap.Int64("bytes", object.Size))

		return certmagic.KeyInfo{
			Key:        key,
			Modified:   object.LastModified,
			Size:       object.Size,
			IsTerminal: true,
		}, nil
	}
	if !isNotExist(err) {
		return certmagic.KeyInfo{}, err
	}

	// a key with no value of its own is a directory when other keys live below it
	dir, err := s3.hasChildren(ctx, key)
	if err != nil {
		return certmagic.KeyInfo{}, err
	}
	if !dir {
		return certmagic.KeyInfo{}, fs.ErrNotExist
	}

	return certmagic.KeyInfo{Key: key}, nil
}

// hasChildren reports whether any key is stored below key
func (s3 *S3) hasChildren(ctx context.Context, key string) (bool, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for object := range s3.client.ListObjects(ctx, s3.Bucket, minio.ListObjectsOptions{
		Prefix:    s3.dirPrefix(key),
		Recursive: true,
		MaxKeys:   1,
	}) {
		return object.Err == nil, object.Err
	}

	return false, nil
}

func (s3 *S3) KeyPrefix(key string) string {
	return path.Join(s3.Prefix, key)
}

// dirPrefix is the listing prefix matching every key below key and no string sibling
func (s3 *S3) dirPrefix(key string) string {
	if prefixed := s3.KeyPrefix(key); prefixed != "" {
		return prefixed + "/"
	}

	return ""
}

// CutKeyPrefix returns a key the way it was stored, without the prefix or the
// separator joining them
func (s3 *S3) CutKeyPrefix(key string) string {
	cut, _ := strings.CutPrefix(key, s3.Prefix)

	return strings.TrimPrefix(cut, "/")
}

func (s3 *S3) String() string {
	return fmt.Sprintf("S3 Storage Host: %s, Bucket: %s, Prefix: %s", s3.Host, s3.Bucket, s3.Prefix)
}

// validateHost rejects a scheme, the client dials host or host:port over http or https as configured
func validateHost(host string) error {
	if host == "" {
		return errors.New("host is empty")
	}
	if strings.Contains(host, "://") {
		return fmt.Errorf("host must be a hostname or host:port without a scheme, got %q", host)
	}

	return nil
}

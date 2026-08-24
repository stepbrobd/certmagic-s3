package s3

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/url"
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
	_ caddy.Provisioner     = (*S3)(nil)
	_ caddyfile.Unmarshaler = (*S3)(nil)
)

const (
	// locks live under their own prefix so List never mixes them into data results
	lockPrefix = "locks"

	// a lock whose holder stopped extending it becomes reclaimable after this
	lockTTL = time.Minute

	// how often a holder extends the lock it owns
	lockRefreshInterval = 15 * time.Second

	// how often a waiter retries acquisition
	lockPollInterval = time.Second
)

// lockInfo is the body of a lock object
type lockInfo struct {
	Owner   string    `json:"owner"`
	Created time.Time `json:"created"`
	Expires time.Time `json:"expires"`
}

// heldLock is a lock this instance owns, cancel stops the refresher
type heldLock struct {
	owner  string
	cancel context.CancelFunc
}

func init() {
	caddy.RegisterModule(&S3{})
}

type S3 struct {
	logger *zap.Logger
	client *minio.Client

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
	for d.Next() {

		key := d.Val()

		var value string
		if !d.Args(&value) {
			continue
		}

		switch key {
		case "host":
			s3.Host = value
			err := validateHost(s3.Host)
			if err != nil {
				return d.Err("Invalid usage of host in s3-storage config: " + err.Error())
			}
		case "bucket":
			s3.Bucket = value
		case "access_id":
			s3.AccessID = value
		case "secret_key":
			s3.SecretKey = value
		case "prefix":
			s3.Prefix = value
		case "insecure":
			insecure, err := strconv.ParseBool(value)
			if err != nil {
				return d.Err("Invalid usage of insecure in s3-storage config: " + err.Error())
			}
			s3.Insecure = insecure
		case "use_iam_provider":
			boolValue, err := strconv.ParseBool(value)
			if err != nil {
				return d.Err("Invalid usage of use_iam_provider in s3-storage config: " + err.Error())
			}
			s3.UseIamProvider = boolValue
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
		boolVal := os.Getenv("S3_USE_IAM_PROVIDER")
		if boolVal != "" {
			s3.UseIamProvider, err = strconv.ParseBool(boolVal)

			if err != nil {
				s3.UseIamProvider = false // default value
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

	if !s3.Insecure {
		insecure := os.Getenv("S3_INSECURE")
		if insecure != "" {
			s3.Insecure, err = strconv.ParseBool(insecure)

			if err != nil {
				s3.Insecure = false // default value
			}
		}
	}
	secure := !s3.Insecure

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
		Secure: secure,
	})
	if err != nil {
		return err
	}

	s3.client = client
	s3.locks = make(map[string]*heldLock)

	return nil
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
// the create is conditional on the object not existing, so concurrent
// instances cannot all believe they hold it
// a lock whose holder stopped extending it past its expiry is reclaimed by
// exactly one waiter
func (s3 *S3) Lock(ctx context.Context, name string) error {
	key := s3.LockKey(name)

	owner, err := newLockOwner()
	if err != nil {
		return err
	}

	for {
		createErr := s3.createLock(ctx, key, owner)
		if createErr == nil {
			s3.trackLock(ctx, key, owner)
			return nil
		}
		if !isPreconditionFailed(createErr) {
			return createErr
		}

		existing, etag, loadErr := s3.loadLock(ctx, key)
		if errors.Is(loadErr, fs.ErrNotExist) {
			// released between the create and this read, retry at once
			continue
		}
		if loadErr != nil {
			return loadErr
		}

		if time.Now().Before(existing.Expires) {
			if waitErr := wait(ctx, lockPollInterval); waitErr != nil {
				return waitErr
			}
			continue
		}

		s3.logger.Warn(fmt.Sprintf("reclaiming lock %s abandoned by %s", key, existing.Owner))

		// swapping on the etag keeps two waiters from both reclaiming it
		replaceErr := s3.replaceLock(ctx, key, owner, etag)
		if replaceErr == nil {
			s3.trackLock(ctx, key, owner)
			return nil
		}
		if !isPreconditionFailed(replaceErr) {
			return replaceErr
		}

		if waitErr := wait(ctx, lockPollInterval); waitErr != nil {
			return waitErr
		}
	}
}

// Unlock stops extending the named lock and removes it
// a lock this instance no longer owns is left for its expiry to clear so a
// late Unlock cannot release someone else
func (s3 *S3) Unlock(ctx context.Context, name string) error {
	key := s3.LockKey(name)

	s3.locksMu.Lock()
	current, mine := s3.locks[key]
	delete(s3.locks, key)
	s3.locksMu.Unlock()

	if !mine {
		s3.logger.Debug(fmt.Sprintf("Unlock: %s is not held here, leaving it to expire", key))
		return nil
	}

	current.cancel()

	existing, _, err := s3.loadLock(ctx, key)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}

	if existing.Owner != current.owner {
		s3.logger.Warn(fmt.Sprintf("lock %s was reclaimed by %s, leaving it", key, existing.Owner))
		return nil
	}

	return s3.client.RemoveObject(ctx, s3.Bucket, key, minio.RemoveObjectOptions{})
}

// createLock writes the lock only when no object exists at the key
func (s3 *S3) createLock(ctx context.Context, key, owner string) error {
	return s3.putLock(ctx, key, owner, func(opts *minio.PutObjectOptions) {
		opts.SetMatchETagExcept("*")
	})
}

// replaceLock overwrites the lock only while it still carries the given etag
func (s3 *S3) replaceLock(ctx context.Context, key, owner, etag string) error {
	return s3.putLock(ctx, key, owner, func(opts *minio.PutObjectOptions) {
		opts.SetMatchETag(etag)
	})
}

func (s3 *S3) putLock(ctx context.Context, key, owner string, condition func(*minio.PutObjectOptions)) error {
	now := time.Now()

	body, err := json.Marshal(lockInfo{
		Owner:   owner,
		Created: now,
		Expires: now.Add(lockTTL),
	})
	if err != nil {
		return err
	}

	opts := minio.PutObjectOptions{ContentType: "application/json"}
	condition(&opts)

	_, err = s3.client.PutObject(ctx, s3.Bucket, key, bytes.NewReader(body), int64(len(body)), opts)

	return err
}

// loadLock returns the stored lock and the etag it must be swapped against
// a body that cannot be parsed is reported as a zero lock so that it reads as
// expired and gets reclaimed rather than blocking every waiter forever
func (s3 *S3) loadLock(ctx context.Context, key string) (lockInfo, string, error) {
	var stored lockInfo

	object, err := s3.client.GetObject(ctx, s3.Bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return stored, "", err
	}
	defer object.Close()

	stat, err := object.Stat()
	if err != nil {
		if minio.ToErrorResponse(err).Code == minio.NoSuchKey {
			return stored, "", fs.ErrNotExist
		}
		return stored, "", err
	}

	body, err := io.ReadAll(object)
	if err != nil {
		return stored, "", err
	}

	if err := json.Unmarshal(body, &stored); err != nil {
		s3.logger.Warn(fmt.Sprintf("lock %s is unreadable, treating it as expired: %v", key, err))
		return lockInfo{}, stat.ETag, nil
	}

	return stored, stat.ETag, nil
}

// trackLock records the lock and starts extending it until Unlock
func (s3 *S3) trackLock(ctx context.Context, key, owner string) {
	// the refresher has to outlive the caller context, Unlock is what stops it
	refresh, cancel := context.WithCancel(context.WithoutCancel(ctx))

	s3.locksMu.Lock()
	if previous, ok := s3.locks[key]; ok {
		previous.cancel()
	}
	s3.locks[key] = &heldLock{owner: owner, cancel: cancel}
	s3.locksMu.Unlock()

	go s3.keepLockFresh(refresh, key, owner)
}

// keepLockFresh extends the lock while this instance still owns it
// without it every lock would look abandoned one TTL after acquisition
func (s3 *S3) keepLockFresh(ctx context.Context, key, owner string) {
	for {
		if err := wait(ctx, lockRefreshInterval); err != nil {
			return
		}

		current, etag, err := s3.loadLock(ctx, key)
		if err != nil {
			if !errors.Is(err, fs.ErrNotExist) {
				s3.logger.Error(fmt.Sprintf("reading lock %s to extend it: %v", key, err))
			}
			return
		}

		if current.Owner != owner {
			s3.logger.Warn(fmt.Sprintf("lock %s is now held by %s, stopping refresh", key, current.Owner))
			return
		}

		if err := s3.replaceLock(ctx, key, owner, etag); err != nil {
			s3.logger.Error(fmt.Sprintf("extending lock %s: %v", key, err))
			return
		}
	}
}

// LockKey returns the object key backing a named lock
func (s3 *S3) LockKey(name string) string {
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
		host = "unknown"
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

// a conditional put that lost the race answers 412, some backends answer 409
func isPreconditionFailed(err error) bool {
	response := minio.ToErrorResponse(err)

	return response.Code == minio.PreconditionFailed ||
		response.Code == minio.Conflict ||
		response.StatusCode == http.StatusPreconditionFailed ||
		response.StatusCode == http.StatusConflict
}

func (s3 *S3) Store(ctx context.Context, key string, value []byte) error {
	key = s3.KeyPrefix(key)
	length := int64(len(value))

	s3.logger.Debug(fmt.Sprintf("Store: %s, %d bytes", key, length))

	_, err := s3.client.PutObject(ctx, s3.Bucket, key, bytes.NewReader(value), length, minio.PutObjectOptions{})

	return err
}

func (s3 *S3) Load(ctx context.Context, key string) ([]byte, error) {
	if !s3.Exists(ctx, key) {
		return nil, fs.ErrNotExist
	}

	key = s3.KeyPrefix(key)

	s3.logger.Debug(fmt.Sprintf("Load key: %s", key))

	object, err := s3.client.GetObject(ctx, s3.Bucket, key, minio.GetObjectOptions{})

	if err != nil {
		return nil, err
	}

	defer object.Close()

	return io.ReadAll(object)
}

func (s3 *S3) Delete(ctx context.Context, key string) error {
	key = s3.KeyPrefix(key)

	s3.logger.Debug(fmt.Sprintf("Delete key: %s", key))

	return s3.client.RemoveObject(ctx, s3.Bucket, key, minio.RemoveObjectOptions{})
}

func (s3 *S3) Exists(ctx context.Context, key string) bool {
	key = s3.KeyPrefix(key)

	_, err := s3.client.StatObject(ctx, s3.Bucket, key, minio.StatObjectOptions{})

	exists := err == nil

	s3.logger.Debug(fmt.Sprintf("Check exists: %s, %t", key, exists))

	return exists
}

func (s3 *S3) List(ctx context.Context, prefix string, recursive bool) ([]string, error) {

	objects := s3.client.ListObjects(ctx, s3.Bucket, minio.ListObjectsOptions{
		Prefix:    s3.KeyPrefix(prefix),
		Recursive: recursive,
	})

	keys := make([]string, len(objects))

	for object := range objects {
		keys = append(keys, s3.CutKeyPrefix(object.Key))
	}

	return keys, nil
}

func (s3 *S3) Stat(ctx context.Context, key string) (certmagic.KeyInfo, error) {
	key = s3.KeyPrefix(key)

	object, err := s3.client.StatObject(ctx, s3.Bucket, key, minio.StatObjectOptions{})

	if err != nil {
		s3.logger.Error(fmt.Sprintf("Stat key: %s, error: %v", key, err))

		return certmagic.KeyInfo{}, nil
	}

	s3.logger.Debug(fmt.Sprintf("Stat key: %s, size: %d bytes", key, object.Size))

	return certmagic.KeyInfo{
		Key:        object.Key,
		Modified:   object.LastModified,
		Size:       object.Size,
		IsTerminal: strings.HasSuffix(object.Key, "/"),
	}, err
}

func (s3 *S3) KeyPrefix(key string) string {
	return path.Join(s3.Prefix, key)
}
func (s3 *S3) CutKeyPrefix(key string) string {
	cutted, _ := strings.CutPrefix(key, s3.Prefix)
	return cutted
}

func (s3 *S3) String() string {
	return fmt.Sprintf("S3 Storage Host: %s, Bucket: %s, Prefix: %s", s3.Host, s3.Bucket, s3.Prefix)
}

func validateHost(h string) error {
	x, _, err := net.SplitHostPort(h)
	if err == nil && x != "" {
		h = x
	}
	u, err := url.Parse(h)
	if err != nil {
		return fmt.Errorf("invalid host: must be a hostname: %w", err)
	}
	if u.Scheme != "" {
		return errors.New("host must not contain a scheme prefix like https://")
	}
	return nil
}

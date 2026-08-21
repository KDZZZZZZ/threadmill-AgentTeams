package artifacts

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"time"
)

// S3BlobPublisherConfig is process-local object-store configuration. SecretKey
// is intentionally not serializable and must never be put in a Runtime record
// or outbox event. It supports the S3-compatible API exposed by MinIO.
type S3BlobPublisherConfig struct {
	Endpoint   string
	Bucket     string
	Prefix     string
	Region     string
	AccessKey  string       `json:"-"`
	SecretKey  string       `json:"-"`
	HTTPClient *http.Client `json:"-"`
}

// S3BlobPublisher publishes content-addressed artifacts to an S3-compatible
// object store. Object identity is independent of a Worker workspace and so
// remains usable after a process, Worker, or execution-epoch replacement.
type S3BlobPublisher struct {
	endpoint  *url.URL
	bucket    string
	prefix    string
	region    string
	accessKey string
	secretKey string
	client    *http.Client
	now       func() time.Time
}

func NewS3BlobPublisher(config S3BlobPublisherConfig) (*S3BlobPublisher, error) {
	endpoint, err := url.Parse(config.Endpoint)
	if err != nil || endpoint.Scheme == "" || endpoint.Host == "" || endpoint.RawQuery != "" || endpoint.Fragment != "" {
		return nil, errors.New("valid object-store endpoint is required")
	}
	if config.Bucket == "" || strings.Contains(config.Bucket, "/") || config.AccessKey == "" || config.SecretKey == "" {
		return nil, errors.New("object-store bucket and credentials are required")
	}
	prefix, err := cleanObjectPrefix(config.Prefix)
	if err != nil {
		return nil, err
	}
	region := config.Region
	if region == "" {
		region = "us-east-1"
	}
	client := config.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	return &S3BlobPublisher{endpoint: endpoint, bucket: config.Bucket, prefix: prefix, region: region, accessKey: config.AccessKey, secretKey: config.SecretKey, client: client, now: time.Now}, nil
}

// Publish rehashes the opened source before upload, uploads to its immutable
// content-addressed key, then verifies the object with a signed HEAD request.
// It returns only an opaque durable S3 reference, never the local source path.
func (p *S3BlobPublisher) Publish(ctx context.Context, sourcePath, expectedHash string) (string, error) {
	if p == nil || p.endpoint == nil || sourcePath == "" || !validSHA256(expectedHash) {
		return "", errors.New("publisher, source path, and sha256 content hash are required")
	}
	f, err := os.Open(sourcePath)
	if err != nil {
		return "", err
	}
	defer f.Close()
	hasher := sha256.New()
	if _, err = io.Copy(hasher, f); err != nil {
		return "", err
	}
	if actual := hex.EncodeToString(hasher.Sum(nil)); actual != expectedHash {
		return "", errors.New("artifact source hash changed before publish")
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return "", err
	}
	key := p.objectKey(expectedHash)
	if err = p.put(ctx, key, f, expectedHash); err != nil {
		return "", err
	}
	if err = p.verify(ctx, key, expectedHash); err != nil {
		return "", err
	}
	return "s3://" + p.bucket + "/" + key, nil
}

func (p *S3BlobPublisher) objectKey(hash string) string {
	return path.Join(p.prefix, "sha256", hash)
}

func (p *S3BlobPublisher) put(ctx context.Context, key string, body io.Reader, hash string) error {
	req, err := p.newRequest(ctx, http.MethodPut, key, body, hash)
	if err != nil {
		return err
	}
	req.Header.Set("If-None-Match", "*")
	req.Header.Set("x-amz-meta-content-sha256", hash)
	response, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode >= http.StatusOK && response.StatusCode < http.StatusMultipleChoices || response.StatusCode == http.StatusPreconditionFailed {
		return nil
	}
	return fmt.Errorf("object-store publish failed: status %d", response.StatusCode)
}

func (p *S3BlobPublisher) verify(ctx context.Context, key, hash string) error {
	req, err := p.newRequest(ctx, http.MethodHead, key, nil, emptySHA256)
	if err != nil {
		return err
	}
	response, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("object-store verification failed: status %d", response.StatusCode)
	}
	if response.Header.Get("x-amz-meta-content-sha256") != hash {
		return errors.New("object-store verification hash mismatch")
	}
	return nil
}

func (p *S3BlobPublisher) newRequest(ctx context.Context, method, key string, body io.Reader, payloadHash string) (*http.Request, error) {
	u := *p.endpoint
	u.Path = path.Join("/", p.endpoint.Path, p.bucket, key)
	u.RawPath = ""
	req, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return nil, err
	}
	now := p.now().UTC()
	req.Header.Set("Host", u.Host)
	req.Header.Set("x-amz-content-sha256", payloadHash)
	req.Header.Set("x-amz-date", now.Format("20060102T150405Z"))
	p.sign(req, payloadHash, now)
	return req, nil
}

func (p *S3BlobPublisher) sign(req *http.Request, payloadHash string, now time.Time) {
	date := now.UTC().Format("20060102")
	canonicalHeaders := "host:" + req.URL.Host + "\n" +
		"x-amz-content-sha256:" + payloadHash + "\n" +
		"x-amz-date:" + now.UTC().Format("20060102T150405Z") + "\n"
	signedHeaders := "host;x-amz-content-sha256;x-amz-date"
	canonicalRequest := strings.Join([]string{req.Method, req.URL.EscapedPath(), "", canonicalHeaders, signedHeaders, payloadHash}, "\n")
	scope := strings.Join([]string{date, p.region, "s3", "aws4_request"}, "/")
	stringToSign := strings.Join([]string{"AWS4-HMAC-SHA256", now.UTC().Format("20060102T150405Z"), scope, hexHash(canonicalRequest)}, "\n")
	signingKey := hmacSHA256(hmacSHA256(hmacSHA256(hmacSHA256([]byte("AWS4"+p.secretKey), date), p.region), "s3"), "aws4_request")
	signature := hex.EncodeToString(hmacSHA256(signingKey, stringToSign))
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+p.accessKey+"/"+scope+", SignedHeaders="+signedHeaders+", Signature="+signature)
}

const emptySHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

func cleanObjectPrefix(prefix string) (string, error) {
	prefix = strings.Trim(prefix, "/")
	if prefix == "" {
		return "threadmill/artifacts", nil
	}
	for _, component := range strings.Split(prefix, "/") {
		if component == "" || component == "." || component == ".." {
			return "", errors.New("object-store prefix must not contain traversal")
		}
	}
	return prefix, nil
}

func validSHA256(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func hexHash(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func hmacSHA256(key []byte, value string) []byte {
	h := hmac.New(sha256.New, key)
	_, _ = h.Write([]byte(value))
	return h.Sum(nil)
}

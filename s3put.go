package main

import (
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/voxelbrain/goptions"
)

const (
	VERSION = "2.1.0"
)

var (
	options = struct {
		Concurrency  int           `goptions:"-c, --concurrency, description='Number of coroutines'"`
		Continue     bool          `goptions:"--continue, description='Continue on error'"`
		Prefix       string        `goptions:"-p, --prefix, description='Prefix to apply to remote storage'"`
		CacheControl string        `goptions:"--cache-control, description='Set Cache-Control header on upload'"`
		AccessKey    string        `goptions:"-k, --access-key, obligatory, description='AWS Access Key ID'"`
		SecretKey    string        `goptions:"-s, --secret-key, obligatory, description='AWS Secret Access Key'"`
		Bucket       string        `goptions:"-b, --bucket, obligatory, description='Bucket URL to push to'"`
		Help         goptions.Help `goptions:"-h, --help, description='Show this help'"`
		goptions.Remainder

		goptions.Verbs
		Put struct{} `goptions:"put"`
		Get struct{} `goptions:"get"`
	}{
		Concurrency: 10,
	}
)

func init() {
	err := goptions.Parse(&options)
	if err != nil || len(options.Remainder) <= 0 || len(options.Verbs) <= 0 {
		if err != goptions.ErrHelpRequest && err != nil {
			log.Printf("Error: %s", err)
		}
		goptions.PrintHelp()
		os.Exit(1)
	}

	if options.CacheControl != "" {
		log.Printf("Monkey patching default transport...")
		monkeyPatchDefaultTransport()
	}
}

func main() {
	var s Storage
	var err error
	verb := string(options.Verbs)
	switch {
	case strings.HasPrefix(options.Bucket, "gcs:"):
		bucket := strings.TrimPrefix(options.Bucket, "gcs://")
		s, err = NewGcsStorage(options.AccessKey, options.SecretKey, "https://"+bucket, options.Prefix)
	case strings.HasPrefix(options.Bucket, "s3:"):
		bucket := strings.TrimPrefix(options.Bucket, "s3://")
		log.Printf("Prefix: %s", bucket)
		s, err = NewS3Storage(options.AccessKey, options.SecretKey, "https://"+bucket, options.Prefix)
	}
	if err != nil {
		log.Fatalf("Invalid storage credentials: %s", err)
	}

	var dst Storage
	var items <-chan *Item
	switch verb {
	case "put":
		dst = s
		ls := &LocalStorage{options.Remainder[0]}
		items = ls.ListFiles()
	case "get":
		dst = &LocalStorage{options.Remainder[0]}
		items = s.ListFiles()
	default:
		log.Fatalf("Invalid/Missing `put` or `get`")
	}
	CopyItems(dst, items, options.Concurrency, options.Continue)
}

type HeaderPatchRoundTripper struct {
	http.RoundTripper
	Headers http.Header
}

func (hprt *HeaderPatchRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	for h, vs := range hprt.Headers {
		for _, v := range vs {
			r.Header.Add(h, v)
		}
	}
	return hprt.RoundTripper.RoundTrip(r)
}

func monkeyPatchDefaultTransport() {
	http.DefaultTransport = &HeaderPatchRoundTripper{
		RoundTripper: http.DefaultTransport,
		Headers: http.Header{
			"Cache-Control": []string{options.CacheControl},
		},
	}
}

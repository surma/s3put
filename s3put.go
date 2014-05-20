package main

import (
	"log"
	"net/http"
	"os"

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
		Help         goptions.Help `goptions:"-h, --help, description='Show this help'"`
		goptions.Remainder

		goptions.Verbs
		S3 struct {
			AccessKey string `goptions:"-k, --access-key, obligatory, description='AWS Access Key ID'"`
			SecretKey string `goptions:"-s, --secret-key, obligatory, description='AWS Secret Access Key'"`
			Bucket    string `goptions:"-b, --bucket, obligatory, description='Bucket URL to push to'"`

			goptions.Verbs
			Put struct{} `goptions:"put"`
			Get struct{} `goptions:"get"`
		} `goptions:"s3"`
		GCS struct {
			AccessKey string `goptions:"-k, --access-key, obligatory, description='GCS Interop Access Key ID'"`
			SecretKey string `goptions:"-s, --secret-key, obligatory, description='GCS Interop Access Key'"`
			Bucket    string `goptions:"-b, --bucket, obligatory, description='Bucket URL to push to'"`

			goptions.Verbs
			Put struct{} `goptions:"put"`
			Get struct{} `goptions:"get"`
		} `goptions:"gcs"`
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
	var verb string
	switch options.Verbs {
	case "gcs":
		s, err = NewGcsStorage(options.GCS.AccessKey, options.GCS.SecretKey, options.GCS.Bucket, options.Prefix)
		verb = string(options.GCS.Verbs)
	case "s3":
		s, err = NewS3Storage(options.S3.AccessKey, options.S3.SecretKey, options.S3.Bucket, options.Prefix)
		verb = string(options.S3.Verbs)
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

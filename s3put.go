package main

import (
	"github.com/voxelbrain/goptions"
	"io"
	"launchpad.net/goamz/aws"
	"launchpad.net/goamz/s3"
	"log"
	"mime"
	"os"
	"path/filepath"
	"sync"
)

const (
	VERSION = "1.0"
)

type Item struct {
	Prefix string
	Path   string
	os.FileInfo
}

var (
	options = struct {
		AccessKey   string `goptions:"-k, --access-key, obligatory, description='AWS Access Key ID'"`
		SecretKey   string `goptions:"-s, --secret-key, obligatory, description='AWS Secret Access Key'"`
		Region      string `goptions:"-r, --region, description='API Region name (default: us-west-1)'"`
		Bucket      string `goptions:"-b, --bucket, obligatory, description='Bucket to push to'"`
		Concurrency int    `goptions:"-c, --concurrency, description='Number of coroutines (default: 10)'"`
		goptions.Remainder
		goptions.Verbs
		Put struct {
			Prefix string `goptions:"-p, --prefix, description='Prefix to prepend to the items'"`
		} `goptions:"put"`
		Get struct {
			Prefix string `goptions:"-p, --prefix, description='Only get items starting with prefix'"`
		} `goptions:"get"`
	}{
		Concurrency: 10,
		Region:      aws.USWest.Name,
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

}

func main() {
	auth := aws.Auth{
		AccessKey: options.AccessKey,
		SecretKey: options.SecretKey,
	}

	region, ok := aws.Regions[options.Region]
	if !ok {
		log.Fatalf("Invalid region name %s", options.Region)
	}

	s3i := s3.New(auth, region)
	bucket := s3i.Bucket(options.Bucket)

	switch options.Verbs {
	case "put":
		c := listLocalFiles(options.Remainder...)
		putFiles(bucket, c)
	case "get":
		c := listBucketFiles(bucket)
		getFiles(bucket, c)
	}
}

func listLocalFiles(path ...string) <-chan *Item {
	c := make(chan *Item)
	go func() {
		for _, prefix := range options.Remainder {
			newprefix, err := filepath.Abs(prefix)
			if err != nil {
				log.Printf("Path %s could not be made absolute: %s. Skipping...", prefix, err)
				continue
			}
			log.Printf("Traversing %s...", newprefix)
			filepath.Walk(newprefix, func(path string, info os.FileInfo, err error) error {
				if info.IsDir() {
					return nil
				}
				c <- &Item{
					Prefix:   newprefix,
					Path:     path,
					FileInfo: info,
				}
				return nil
			})
		}
		close(c)
	}()
	return c
}

func listBucketFiles(bucket *s3.Bucket) <-chan *Item {
	c := make(chan *Item)
	go func() {
		resp, err := bucket.List(options.Get.Prefix, "", "", 1000000)
		if err != nil {
			log.Printf("Could not list items in bucket: %s", err)
		}
		for _, item := range resp.Contents {
			c <- &Item{
				Prefix:   options.Get.Prefix,
				Path:     item.Key,
				FileInfo: nil,
			}
		}
		close(c)
	}()
	return c
}

func putFiles(bucket *s3.Bucket, c <-chan *Item) {
	var wg sync.WaitGroup
	wg.Add(options.Concurrency)
	for i := 0; i < options.Concurrency; i++ {
		go func() {
			for item := range c {
				func() {
					f, err := os.Open(item.Path)
					if err != nil {
						log.Printf("Pushing %s failed: %s", item.Path, err)
					}
					defer f.Close()

					path := item.Path[len(item.Prefix)+1:]
					err = bucket.PutReader(options.Put.Prefix+path, f, item.FileInfo.Size(), mime.TypeByExtension(filepath.Ext(item.Path)), s3.BucketOwnerFull)
					if err != nil {
						log.Printf("Uploading %s failed: %s", path, err)
						return
					}
					log.Printf("Uploading %s done", path)
				}()
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func getFiles(bucket *s3.Bucket, c <-chan *Item) {
	var wg sync.WaitGroup
	wg.Add(options.Concurrency)
	for i := 0; i < options.Concurrency; i++ {
		go func() {
			for item := range c {
				func() {
					itempath := item.Path[len(item.Prefix):]
					dirname, fname := filepath.Split(itempath)
					dirname = filepath.Join(options.Remainder[0], dirname)

					os.MkdirAll(dirname, os.FileMode(0755))
					f, err := os.Create(filepath.Join(dirname, fname))
					if err != nil {
						log.Printf("Opening %s failed: %s", item.Path, err)
					}
					defer f.Close()

					rc, err := bucket.GetReader(item.Path)
					if err != nil {
						log.Printf("Downloading %s failed: %s", item.Path, err)
						return
					}
					defer rc.Close()
					io.Copy(f, rc)
					log.Printf("Downloading %s done", item.Path)
				}()
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

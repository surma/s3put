package main

import (
	"github.com/voxelbrain/goptions"
	"launchpad.net/goamz/aws"
	"launchpad.net/goamz/s3"
	"log"
	"mime"
	"os"
	"path/filepath"
	"sync"
)

const (
	VERSION = "0.1.1"
)

type Item struct {
	Prefix string
	Path   string
	os.FileInfo
}

func main() {
	options := struct {
		AccessKey   string `goptions:"-k, --access-key, obligatory, description='AWS Access Key ID'"`
		SecretKey   string `goptions:"-s, --secret-key, obligatory, description='AWS Secret Access Key'"`
		Region      string `goptions:"-r, --region, description='API Region name (default: us-west-1)'"`
		Bucket      string `goptions:"-b, --bucket, obligatory, description='Bucket to push to'"`
		Prefix      string `goptions:"-p, --prefix, description='Prefix to prepend to the items'"`
		Concurrency int    `goptions:"-c, --concurrency, description='Number of coroutines (default: 10)'"`
		goptions.Remainder
	}{
		Concurrency: 10,
		Region:      aws.USWest.Name,
	}

	err := goptions.Parse(&options)
	if err != nil || len(options.Remainder) <= 0 {
		if err != goptions.ErrHelpRequest {
			log.Printf("Error: %s", err)
		}
		goptions.PrintHelp()
		return
	}

	c := make(chan Item)
	go func() {
		for _, prefix := range options.Remainder {
			newprefix, err := filepath.Abs(prefix)
			if err != nil {
				log.Printf("Path %s could not be made absolute: %s", prefix, err)
				continue
			}
			log.Printf("Traversing %s...", newprefix)
			filepath.Walk(newprefix, func(path string, info os.FileInfo, err error) error {
				if info.IsDir() {
					return nil
				}
				c <- Item{
					Prefix:   newprefix,
					Path:     path,
					FileInfo: info,
				}
				return nil
			})
		}
		close(c)
	}()

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
					err = bucket.PutReader(options.Prefix+path, f, item.FileInfo.Size(), mime.TypeByExtension(filepath.Ext(item.Path)), s3.BucketOwnerFull)
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

package main

import (
	"fmt"
	"io"
	"log"
	"mime"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"launchpad.net/goamz/aws"
	"launchpad.net/goamz/s3"
)

type Item struct {
	Prefix string
	Path   string
	Size   int64
	io.ReadCloser
}

func (i *Item) String() string {
	return fmt.Sprintf("(Prefix: %s) %s", i.Prefix, i.Path)
}

type Storage interface {
	// Lists all files in the storage system. Any kind of
	// chrooting/prefixing has to be implemented and enforced manually.
	ListFiles() <-chan *Item
	// Saves a file to the storage system. Any kind of
	// chrooting/prefixing has to be implemented and enforced manually.
	PutFile(item *Item) error
}

type S3Storage struct {
	bucket *s3.Bucket
	prefix string
}

func NewS3Storage(accessKey, secretKey, bucketUrl string, prefix string) (*S3Storage, error) {
	auth := aws.Auth{
		AccessKey: accessKey,
		SecretKey: secretKey,
	}
	u, err := url.Parse(bucketUrl)
	if err != nil {
		return nil, err
	}

	bucketname := strings.SplitN(strings.TrimPrefix(u.Path, "/"), "/", 2)[0]
	region, err := s3RegionByEndpoint(u.Scheme + "://" + u.Host)
	if err != nil {
		return nil, err
	}
	s3i := s3.New(auth, region)
	b := s3i.Bucket(bucketname)
	return &S3Storage{
		bucket: b,
		prefix: prefix,
	}, nil
}

func (s *S3Storage) ListFiles() <-chan *Item {
	c := make(chan *Item)
	go func() {
		marker := ""
		defer close(c)
		for {
			resp, err := s.bucket.List(s.prefix, "", marker, 1000)
			if err != nil {
				log.Printf("Could not list items in bucket %s: %s", s.bucket.Name, err)
				return
			}
			for _, item := range resp.Contents {
				rc, err := s.bucket.GetReader(item.Key)
				if err != nil {
					log.Printf("Could not receive %s: %s", item, err)
					continue
				}
				c <- &Item{
					Prefix:     s.prefix,
					Path:       item.Key,
					Size:       item.Size,
					ReadCloser: rc,
				}
				marker = item.Key
			}
			if !resp.IsTruncated {
				break
			}
		}
	}()
	return c
}

func (s *S3Storage) PutFile(item *Item) error {
	defer item.Close()
	path := strings.TrimPrefix(item.Path, item.Prefix)
	key := filepath.Join(s.prefix, path)
	err := s.bucket.PutReader(key, item, item.Size, mime.TypeByExtension(filepath.Ext(item.Path)), s3.BucketOwnerFull)
	if err != nil {
		return err
	}
	return nil
}

func NewGcsStorage(accessKey, secretKey, bucketUrl string, prefix string) (*S3Storage, error) {
	auth := aws.Auth{
		AccessKey: accessKey,
		SecretKey: secretKey,
	}
	u, err := url.Parse(bucketUrl)
	if err != nil {
		return nil, err
	}

	bucketname := strings.SplitN(strings.TrimPrefix(u.Path, "/"), "/", 2)[0]
	region, err := gcsRegionByEndpoint(u.Scheme + "://" + u.Host)
	if err != nil {
		return nil, err
	}
	s3i := s3.New(auth, region)
	b := s3i.Bucket(bucketname)
	return &S3Storage{
		bucket: b,
		prefix: prefix,
	}, nil
}

type LocalStorage struct {
	Prefix string
}

func (s *LocalStorage) ListFiles() <-chan *Item {
	c := make(chan *Item)
	go func() {
		defer close(c)
		newprefix, err := filepath.Abs(s.Prefix)
		if err != nil {
			log.Printf("Path %s could not be made absolute: %s", newprefix, err)
			return
		}
		f, err := os.Open(newprefix)
		if err != nil {
			log.Printf("Could not open %s: %s", newprefix, err)
			return
		}
		fi, err := f.Stat()
		if err != nil {
			log.Printf("Could not stat %s: %s", newprefix, err)
			return
		}
		if !fi.IsDir() {
			c <- &Item{
				Prefix:     filepath.Dir(newprefix),
				Path:       newprefix,
				Size:       fi.Size(),
				ReadCloser: f,
			}
			return
		}
		log.Printf("Traversing %s...", newprefix)
		filepath.Walk(newprefix, func(path string, info os.FileInfo, err error) error {
			if info.IsDir() {
				return nil
			}
			f, err := os.Open(path)
			if err != nil {
				log.Printf("Could not open %s: %s", path, err)
				return nil
			}
			c <- &Item{
				Prefix:     newprefix,
				Path:       path,
				Size:       info.Size(),
				ReadCloser: f,
			}
			return nil
		})
	}()
	return c
}

func (s *LocalStorage) PutFile(item *Item) error {
	defer item.Close()
	itempath := strings.TrimPrefix(item.Path, item.Prefix)
	dirname, fname := filepath.Split(itempath)
	dirname = filepath.Join(s.Prefix, dirname)

	err := os.MkdirAll(dirname, os.FileMode(0755))
	if err != nil {
		return err
	}

	f, err := os.Create(filepath.Join(dirname, fname))
	if err != nil {
		return err
	}
	defer f.Close()

	io.Copy(f, item)
	return nil
}

func CopyItems(dst Storage, items <-chan *Item, concurrency int, continueOnError bool) {
	wg := &sync.WaitGroup{}
	wg.Add(concurrency)
	log.Printf("Starting %d goroutines...", concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			for item := range items {
				log.Printf("Transfering %s...", item)
				err := dst.PutFile(item)
				if err != nil {
					log.Printf("Could not transfer %s: %s", item, err)
					if continueOnError {
						continue
					} else {
						log.Fatalf("Aborted.")
						return
					}
				}
				log.Printf("Transfer of %s done", item)
			}
		}()
	}
	wg.Wait()
}

func s3RegionByEndpoint(ep string) (aws.Region, error) {
	for _, region := range aws.Regions {
		if region.S3Endpoint == ep {
			return region, nil
		}
	}
	return aws.Region{}, fmt.Errorf("Unknown region endpoint %s", ep)
}

func gcsRegionByEndpoint(ep string) (aws.Region, error) {
	if ep != "https://storage.googleapis.com" {
		return aws.Region{}, fmt.Errorf("Unknown region endpoint %s", ep)
	}
	return aws.Region{
		Name:       "Google Cloud Storage",
		S3Endpoint: ep,
	}, nil
}

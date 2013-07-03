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
	os.FileInfo
	io.ReadCloser
}

func (i *Item) String() string {
	return fmt.Sprintf("(Prefix: %s) %s", i.Prefix, i.Path)
}

type Storage interface {
	ListFiles(prefix string) <-chan *Item
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
	region, err := regionByEndpoint(u.Scheme + "://" + u.Host)
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

func regionByEndpoint(ep string) (aws.Region, error) {
	for _, region := range aws.Regions {
		if region.S3Endpoint == ep {
			return region, nil
		}
	}
	return aws.Region{}, fmt.Errorf("Unknown region endpoint %s", ep)
}

func (s *S3Storage) ListFiles(prefix string) <-chan *Item {
	c := make(chan *Item)
	go func() {
		marker := ""
		defer close(c)
		for {
			resp, err := s.bucket.List(prefix, "", marker, 1000)
			if err != nil {
				log.Printf("Could not list items in bucket: %s", err)
				return
			}
			for _, item := range resp.Contents {
				rc, err := s.bucket.GetReader(item.Key)
				if err != nil {
					log.Printf("Could not receive %s: %s", item, err)
				}
				c <- &Item{
					Prefix:     prefix,
					Path:       item.Key,
					FileInfo:   nil,
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
	err := s.bucket.PutReader(key, item, item.Size(), mime.TypeByExtension(filepath.Ext(item.Path)), s3.BucketOwnerFull)
	if err != nil {
		return err
	}
	return nil
}

type GcsStorage struct {
}

func NewGcsStorage() (*GcsStorage, error) {
	return &GcsStorage{}, nil
}

func (s *GcsStorage) ListFiles(prefix string) <-chan *Item {
	log.Printf("Listing %s not implemented", prefix)
	return make(chan *Item)
}

func (s *GcsStorage) PutFile(item *Item) error {
	log.Printf("Putting %s not implemented", item)
	return nil
}

type LocalStorage struct {
	Path string
}

func (s *LocalStorage) ListFiles(prefix string) <-chan *Item {
	c := make(chan *Item)
	go func() {
		defer close(c)
		newprefix := filepath.Join(s.Path, prefix)
		newprefix, err := filepath.Abs(newprefix)
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
				FileInfo:   fi,
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
				FileInfo:   info,
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
	dirname = filepath.Join(s.Path, dirname)

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

func CopyItems(dst Storage, items <-chan *Item, concurrency int) {
	wg := &sync.WaitGroup{}
	wg.Add(concurrency)
	log.Printf("Starting %d goroutines...", concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			for item := range items {
				dst.PutFile(item)
				log.Printf("Transfer of %s done", item)
			}
		}()
	}
	wg.Wait()
}

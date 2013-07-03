package main

import (
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"launchpad.net/goamz/aws"
	"launchpad.net/goamz/s3"
)

type Item struct {
	Prefix string
	Path   string
	os.FileInfo
}

func (i *Item) String() string {
	return fmt.Sprintf("(%s/)%s", i.Prefix, i.Path)
}

type Storage interface {
	ListFiles(prefix string) <-chan *Item
	PutFile(item *Item, content io.Reader) error
}

type S3Storage struct {
	bucket *s3.Bucket
}

func NewS3Storage(accessKey, secretKey, bucketUrl string) (*S3Storage, error) {
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
				logger("Could not list items in bucket: %s", err)
				return
			}
			for _, item := range resp.Contents {
				c <- &Item{
					Prefix:   prefix,
					Path:     item.Key,
					FileInfo: nil,
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

func (s *S3Storage) PutFile(item *Item, content io.Reader) error {
	log.Printf("Putting %s not implemented", item)
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

func (s *GcsStorage) PutFile(item *Item, content io.Reader) error {
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
		newprefix, err := filepath.Abs(prefix)
		if err != nil {
			logger("Path %s could not be made absolute: %s", prefix, err)
			return
		}
		if fi, err := os.Stat(newprefix); err != nil || !fi.IsDir() {
			if err != nil {
				logger("Could not stat %s: %s", newprefix, err)
			} else if !fi.IsDir() {
				c <- &Item{
					Prefix:   filepath.Dir(newprefix),
					Path:     newprefix,
					FileInfo: fi,
				}
			}
			return
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
	}()
	return c
}

func (s *LocalStorage) PutFile(item *Item, data io.ReadCloser) error {
	defer data.Close()
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

	io.Copy(f, data)
	return nil
}

package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"launchpad.net/goamz/aws"
	"launchpad.net/goamz/s3"

	"code.google.com/p/goauth2/oauth"
	"code.google.com/p/goauth2/oauth/jwt"
	gcsStorage "code.google.com/p/google-api-go-client/storage/v1beta2"
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

type GcsStorage struct {
	service *gcsStorage.Service
	bucket  string
	prefix  string
	client  *http.Client
}

func NewGcsStorage(clientId string, pem io.ReadCloser, bucket, prefix string) (*GcsStorage, error) {
	pemBytes, err := ioutil.ReadAll(pem)
	if err != nil {
		return nil, err
	}

	token := jwt.NewToken(clientId, gcsStorage.DevstorageRead_writeScope, pemBytes)
	c := &http.Client{}
	oauthToken, err := token.Assert(c)
	if err != nil {
		return nil, err
	}

	c.Transport = &oauth.Transport{
		Token: oauthToken,
	}
	service, err := gcsStorage.New(c)
	if err != nil {
		return nil, err
	}
	return &GcsStorage{
		service: service,
		bucket:  bucket,
		prefix:  prefix,
		client:  c,
	}, nil
}

func (s *GcsStorage) ListFiles() <-chan *Item {
	c := make(chan *Item)
	go func() {
		defer close(c)
		objs, err := s.service.Objects.List(s.bucket).Prefix(s.prefix).Do()
		if err != nil {
			log.Printf("Could not list items in bucket %s: %s", s.bucket, err)
			return
		}

		for _, obj := range objs.Items {
			resp, err := s.client.Get(obj.MediaLink)
			if err != nil {
				log.Printf("Could not get %s: %s", obj.Name, err)
				continue
			}
			c <- &Item{
				Prefix:     s.prefix,
				Path:       obj.Name,
				Size:       int64(obj.Size),
				ReadCloser: resp.Body,
			}
		}
	}()
	return c
}

func (s *GcsStorage) PutFile(item *Item) error {
	newprefix := strings.TrimPrefix(item.Path, item.Prefix)
	_, err := s.service.Objects.Insert(s.bucket, &gcsStorage.Object{}).Name(filepath.Join(s.prefix, newprefix)).Media(item).Do()
	if err != nil {
		log.Fatalf("Could not create new object: %s", err)
	}
	return nil
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

package s3test

import (
	"crypto/md5"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"time"

	"github.com/goamz/goamz/aws"
	"github.com/goamz/goamz/s3"
)

var (
	ErrS3NotFound = errors.New("Not found")
)

type GoamzS3Store struct {
	bucketName string
	bucket     *s3.Bucket
	keys       []string
}

func NewGoamzS3Store(regionStr string, bucket string) (*GoamzS3Store, error) {

	var region aws.Region
	if len(regionStr) == 0 {
		region = aws.USEast
	} else {
		var ok bool
		if region, ok = aws.Regions[regionStr]; !ok {
			return nil, fmt.Errorf("Region not found: %s\n", regionStr)
		}
	}
	// this will attempt to populate an Auth object by getting
	// credentials from (in order):
	//
	//   (1) credentials file
	//   (2) environment variables
	//   (3) instance role (this will be the case for production)
	//
	auth, err := aws.GetAuth("", "", "", time.Time{})
	if err != nil {
		return nil, err
	}
	httpClient := &http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 1000,
		},
	}
	b := s3.New(auth, region, httpClient).Bucket(bucket)
	err = b.PutBucket(s3.BucketOwnerFull)
	if err != nil {
		fmt.Printf("trying to create bucket %s\n", bucket)
		log.Fatal(err)
	} else {
		fmt.Printf("successfully created bucket %s\n", bucket)
	}

	return &GoamzS3Store{
		bucketName: bucket,
		bucket:     b,
	}, nil
}

func (h *GoamzS3Store) GetBucketName() string {
	return h.bucketName
}

func (h *GoamzS3Store) GetRandomKey(generator *rand.Rand) string {
	i := generator.Intn(len(h.keys))
	return h.keys[i]
}

func (h *GoamzS3Store) Get(key string) (buf []byte, err error) {
	var res *http.Response
	res, err = h.bucket.GetResponse(key)
	defer func() {
		if res != nil {
			io.Copy(ioutil.Discard, res.Body)
			res.Body.Close()
		}
	}()

	if err != nil {
		if s3err, ok := err.(*s3.Error); ok && s3err.StatusCode == http.StatusNotFound {
			return buf, ErrS3NotFound
		}
		return buf, err
	}

	if res.StatusCode != http.StatusOK {
		return buf, fmt.Errorf("S3 Get err")
	}

	if res.Body != nil {
		buf, err = ioutil.ReadAll(res.Body)
		if err != nil {
			return buf, err
		}
	}

	return buf, err
}

func (h *GoamzS3Store) Put(key string, buf []byte) error {
	md5val := md5.Sum(buf)
	o := s3.Options{
		ContentMD5: base64.StdEncoding.EncodeToString(md5val[:]),
	}
	err := h.bucket.Put(key, buf, "binary/octet-stream", s3.Private, o)
	if err != nil {
		return err
	}
	return nil
}

func (h *GoamzS3Store) ReadAllKeys(max int) (nRead int, err error) {
	var keys []string
	var next string
	for {
		keys, next, err = h.List("", next)
		if err != nil {
			return len(h.keys), err
		}
		h.keys = append(h.keys, keys...)
		if next == "" {
			break
		} else if len(h.keys) >= max {
			break
		}
	}
	return len(h.keys), nil
}

func (h *GoamzS3Store) List(prefix string, marker string) (
	keys []string, nextMarker string, err error) {
	var data *s3.ListResp
	//2nd argument is delimeter. Set to empty because I don't want results grouped
	//4th argument is the max key returned in a single response, 1000 is the S3 maximum
	data, err = h.bucket.List(prefix, "", marker, 1000)
	if err != nil {
		return keys, nextMarker, err
	}

	keys = make([]string, len(data.Contents))
	for i := 0; i < len(data.Contents); i++ {
		keys[i] = data.Contents[i].Key
	}

	if data.IsTruncated {
		nextMarker = data.NextMarker
		if nextMarker == "" && len(data.Contents) > 0 {
			nextMarker = data.Contents[len(data.Contents)-1].Key
		}
	} else {
		nextMarker = ""
	}

	return keys, nextMarker, err
}

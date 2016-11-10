package s3test

import (
	"bytes"
	"io"
	"io/ioutil"
	"math/rand"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type OfficialS3Store struct {
	svc        *s3.S3
	bucketName string
	keys       []string
}

func NewOfficialS3Store(regionStr string, bucket string, goFast bool) (*OfficialS3Store, error) {
	config := aws.NewConfig().WithRegion(regionStr).WithS3UseAccelerate(goFast)
	sess, err := session.NewSession(config)
	if err != nil {
		return nil, err
	}
	svc := s3.New(sess)
	store := &OfficialS3Store{svc, bucket, nil}
	return store, nil
}

func (h *OfficialS3Store) GetBucketName() string {
	return h.bucketName
}

func (h *OfficialS3Store) GetRandomKey(generator *rand.Rand) string {
	i := generator.Intn(len(h.keys))
	return h.keys[i]
}

func (h *OfficialS3Store) Put(key string, buf []byte) error {
	params := &s3.PutObjectInput{
		Bucket: aws.String(h.bucketName),
		Key:    aws.String(key),
		Body:   bytes.NewReader(buf),
	}
	_, err := h.svc.PutObject(params)
	if err != nil {
		return err
	}
	return nil
}

func (h *OfficialS3Store) Get(key string) (buf []byte, err error) {
	params := &s3.GetObjectInput{
		Bucket: aws.String(h.bucketName),
		Key:    aws.String(key),
	}
	res, err := h.svc.GetObject(params)
	if err != nil {
		return nil, err
	}
	defer func() {
		if res != nil {
			io.Copy(ioutil.Discard, res.Body)
			res.Body.Close()
		}
	}()
	if res.Body != nil {
		buf, err = ioutil.ReadAll(res.Body)
		if err != nil {
			return nil, err
		}
	}
	return buf, err
}

func (h *OfficialS3Store) ReadAllKeys(max int) (nRead int, err error) {
	err = h.svc.ListObjectsPages(&s3.ListObjectsInput{Bucket: &h.bucketName},
		func(p *s3.ListObjectsOutput, last bool) (shouldContinue bool) {
			for _, obj := range p.Contents {
				h.keys = append(h.keys, *obj.Key)
			}
			if len(h.keys) < max {
				return true
			}
			return false
		})
	return len(h.keys), err
}

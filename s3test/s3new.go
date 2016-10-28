package s3test

import (
	"github.com/aws/aws-sdk-go"
)

type OfficialS3Store struct {
	svc *s3.S3
	bucketName   string
	keys []string
}

func NewOfficialS3Store(regionStr string, bucket string) (*OfficialS3Store, error) {
	sess, err := session.NewSession()
	if err != nil {
		return nil, err
	}
	svc := s3.New(sess)
	store := &OfficialS3Store{svc, bucket}
	return store, nil
}

func (h *OfficialS3Store) Put(key string, buf []byte) error {
}

func (h *OfficialS3Store) Get(key string) (buf []byte, err error) {
}

func (h *OfficialS3Store) ReadAllKeys(max int) error {
	err = h.svc.ListObjectsPages(&s3.ListObjectsInput{Bucket: &os.Args[1]}, 
		func(p *s3.ListObjectsOutput, last bool) (shouldContinue bool) {
			for _, obj := range p.Contents {
				h.keys = append(h.keys, *obj.Key)
			}
			if len(h.keys) < max {
				 return true
			}	
			return false
	})
	return err
}

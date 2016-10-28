package s3test

import (
	"github.com/aws/aws-sdk-go"
)

type OfficialS3Store struct {
	svc        *s3.S3
	bucketName string
	keys       []string
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
	params := &s3.PutObjectInput{
		Bucket: aws.String(h.bucketName),
		Key:    aws.String(key),
		Body:   bytes.NewReader([]byte("PAYLOAD")),
	}
	resp, err := h.svc.PutObject(params)
	if err != nil {
		return err
	}
}

func (h *OfficialS3Store) Get(key string) (buf []byte, err error) {
	params := &s3.GetObjectInput{
		Bucket: aws.String(h.bucketName),
		Key:    aws.String(key),
	}
	resp, err := h.svc.GetObject(params)
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
			return buf, err
		}
	}

	//do i close?
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

package dataset

import (
	"fmt"
	"time"
)

// s3Storage는 JSON 파일을 AWS S3에 저장합니다 (prod 환경 기본값).
// 버킷이 Terraform으로 프로비저닝되기 전까지는 자리만 잡아둔 상태이며,
// 실제 업로드 로직(aws-sdk-go-v2 s3 클라이언트)은 버킷이 생긴 뒤 구현합니다.
type s3Storage struct {
	bucket string
}

// NewS3Storage는 bucket에 파일을 저장하는 Storage를 반환합니다.
func NewS3Storage(bucket string) Storage {
	return &s3Storage{bucket: bucket}
}

func (s *s3Storage) SaveBatch(b BatchFile, start, end time.Time) (string, error) {
	return "", s.notImplemented()
}

func (s *s3Storage) SaveStream(st StreamFile, start, end time.Time) (string, error) {
	return "", s.notImplemented()
}

func (s *s3Storage) notImplemented() error {
	return fmt.Errorf("S3 저장은 아직 구현되지 않았습니다 (버킷 %q 프로비저닝 이후 구현 예정)", s.bucket)
}

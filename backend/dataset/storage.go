package dataset

import "time"

// Storage는 batch/stream JSON을 어딘가에 저장하는 방법을 추상화합니다.
// dev 환경은 localStorage, prod 환경은 s3Storage를 씁니다 (환경 선택은 main.go에서).
type Storage interface {
	SaveBatch(b BatchFile, start, end time.Time) (string, error)
	SaveStream(s StreamFile, start, end time.Time) (string, error)
}

package dataset

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// localStorage는 JSON 파일을 로컬 디스크에 저장합니다 (dev 환경 기본값).
// 경로 구조는 S3 키 레이아웃({market}/{start}_{end}_{batch|stream}.json)과 동일하게 맞춰,
// 나중에 S3로 전환해도 상대적인 구조가 그대로 대응됩니다.
type localStorage struct {
	root string
}

// NewLocalStorage는 root 디렉터리 아래에 파일을 저장하는 Storage를 반환합니다.
func NewLocalStorage(root string) Storage {
	return &localStorage{root: root}
}

func (s *localStorage) SaveBatch(b BatchFile, start, end time.Time) (string, error) {
	return s.writeJSON(b, b.Market, start, end, "batch")
}

func (s *localStorage) SaveStream(st StreamFile, start, end time.Time) (string, error) {
	return s.writeJSON(st, st.Market, start, end, "stream")
}

func (s *localStorage) writeJSON(v any, market string, start, end time.Time, kind string) (string, error) {
	dir := filepath.Join(s.root, market)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", fmt.Errorf("디렉터리 생성 실패: %w", err)
	}

	filename := fmt.Sprintf("%s_%s_%s.json", formatFileTime(start), formatFileTime(end), kind)
	path := filepath.Join(dir, filename)

	bytes, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return "", fmt.Errorf("JSON 직렬화 실패: %w", err)
	}

	if err := os.WriteFile(path, bytes, 0644); err != nil {
		return "", fmt.Errorf("파일 쓰기 실패: %w", err)
	}

	return path, nil
}

// formatFileTime은 파일명에 안전한(콜론 없는) 시각 포맷을 반환합니다.
func formatFileTime(t time.Time) string {
	return t.UTC().Format("20060102T150405Z")
}

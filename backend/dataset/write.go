package dataset

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// localDataRoot는 로컬 저장 시 파일들이 쌓이는 루트 디렉터리입니다.
// 경로 구조는 CLAUDE.md에 정리된 S3 키 레이아웃({market}/{start}_{end}_{batch|stream}.json)과 동일하게 맞춰,
// 나중에 S3 업로드를 붙일 때 그대로 대응시킬 수 있게 합니다.
const localDataRoot = "data"

// WriteBatch는 배치 파일을 로컬 디스크에 저장하고 저장된 경로를 반환합니다.
func WriteBatch(b BatchFile, start, end time.Time) (string, error) {
	return writeJSON(b, b.Market, start, end, "batch")
}

// WriteStream은 스트림 파일을 로컬 디스크에 저장하고 저장된 경로를 반환합니다.
func WriteStream(s StreamFile, start, end time.Time) (string, error) {
	return writeJSON(s, s.Market, start, end, "stream")
}

func writeJSON(v any, market string, start, end time.Time, kind string) (string, error) {
	dir := filepath.Join(localDataRoot, market)
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

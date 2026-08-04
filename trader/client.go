package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

// NewHTTPClient는 모든 마켓 고루틴이 공유할 단일 HTTP 클라이언트를 만듭니다.
// 타임아웃을 넉넉히 잡은 이유: 캐시 미스 시 backend가 온디맨드 수집(ensureMarketCollected)을
// 마친 뒤 응답하므로, 마켓당 정상적으로 수십 초가 걸릴 수 있습니다.
func NewHTTPClient() *http.Client {
	return &http.Client{Timeout: 5 * time.Minute}
}

// FetchManifest는 GET /v1/markets/data?date=...를 호출합니다.
func FetchManifest(ctx context.Context, client *http.Client, baseURL, date string) (Manifest, error) {
	u := fmt.Sprintf("%s/v1/markets/data?date=%s", baseURL, url.QueryEscape(date))
	var m Manifest
	err := fetchJSON(ctx, client, u, &m)
	return m, err
}

// FetchBatch는 매니페스트가 돌려준 상대 경로(BatchURL)로 batch 파일을 받아옵니다.
func FetchBatch(ctx context.Context, client *http.Client, baseURL, path string) (BatchFile, error) {
	var b BatchFile
	err := fetchJSON(ctx, client, baseURL+path, &b)
	return b, err
}

// FetchStream은 매니페스트가 돌려준 상대 경로(StreamURL)로 stream 파일을 받아옵니다.
func FetchStream(ctx context.Context, client *http.Client, baseURL, path string) (StreamFile, error) {
	var s StreamFile
	err := fetchJSON(ctx, client, baseURL+path, &s)
	return s, err
}

func fetchJSON(ctx context.Context, client *http.Client, target string, v any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return err
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("요청 실패 (%s): status=%d", target, resp.StatusCode)
	}

	if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
		return fmt.Errorf("응답 파싱 실패 (%s): %w", target, err)
	}
	return nil
}

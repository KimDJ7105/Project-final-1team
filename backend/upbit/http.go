package upbit

import (
	"fmt"
	"net/http"
	"time"
)

// requestInterval은 업비트 API 요청 사이 최소 간격입니다 (초당 약 10회 제한 대응).
const requestInterval = 110 * time.Millisecond

const max429Retries = 5

// doRequest는 업비트 API 요청을 실행합니다. 모든 요청(단일/페이지네이션 무관) 사이에
// requestInterval만큼 간격을 두고, 429(Too Many Requests) 응답을 받으면 점점 늘어나는
// 대기 시간을 두고 재시도합니다.
func doRequest(req *http.Request) (*http.Response, error) {
	backoff := 500 * time.Millisecond

	for attempt := 0; ; attempt++ {
		resp, err := http.DefaultClient.Do(req)
		time.Sleep(requestInterval)
		if err != nil {
			return nil, err
		}

		if resp.StatusCode != http.StatusTooManyRequests {
			return resp, nil
		}

		resp.Body.Close()
		if attempt >= max429Retries {
			return nil, fmt.Errorf("업비트 API 요청 제한(429) 재시도 초과: %s", req.URL.String())
		}
		time.Sleep(backoff)
		backoff *= 2
	}
}

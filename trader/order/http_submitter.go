package order

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// HTTPOrderSubmitter는 생성된 주문을 실제 주문 접수 API(orderapi, POST /v1/orders)로 보냅니다.
type HTTPOrderSubmitter struct {
	Client  *http.Client
	BaseURL string
}

// orderRequest는 orderapi/server.go의 orderRequest와 필드가 정확히 대응됩니다.
type orderRequest struct {
	Market   string `json:"market"`
	Side     string `json:"side"`
	Price    string `json:"price"`
	Quantity string `json:"quantity"`
}

// Submit은 o를 orderapi에 신규 주문으로 접수합니다. trader에는 재시도 로직이 없어서
// (한 번의 Submit 호출 = 한 번의 신규 주문 시도) 매 호출마다 새 Idempotency-Key를 씁니다.
func (s HTTPOrderSubmitter) Submit(ctx context.Context, o Order) error {
	body, err := json.Marshal(orderRequest{
		Market:   o.Market,
		Side:     o.Side,
		Price:    o.Price,
		Quantity: o.Quantity,
	})
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.BaseURL+"/v1/orders", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", newIdempotencyKey())

	resp, err := s.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("주문 접수 실패 (status=%d): %s", resp.StatusCode, respBody)
	}
	return nil
}

// newIdempotencyKey는 orderapi/server.go의 requestID()와 같은 방식(crypto/rand -> hex)으로
// 매 요청마다 새 키를 만듭니다.
func newIdempotencyKey() string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("idem-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(buf)
}

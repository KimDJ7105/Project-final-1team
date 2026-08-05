package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
)

// orderRequest는 POST /v1/orders의 요청 본문입니다.
type orderRequest struct {
	Market   string `json:"market"`
	Side     string `json:"side"`
	Price    string `json:"price"`
	Quantity string `json:"quantity"`
}

// errorResponse는 docs/api-specification.md 4장의 공통 오류 응답 포맷입니다.
type errorResponse struct {
	ErrorCode string `json:"errorCode"`
	Message   string `json:"message"`
	RequestID string `json:"requestId"`
}

// cancelResponse는 DELETE /v1/orders/{orderId}의 200 응답 본문입니다.
type cancelResponse struct {
	OrderID          string `json:"orderId"`
	Status           string `json:"status"`
	CanceledQuantity string `json:"canceledQuantity"`
	CanceledAt       string `json:"canceledAt"`
}

// nowISO는 현재 시각을 docs/api-specification.md 1.2가 요구하는
// "ISO-8601 UTC, 밀리초 포함"(예: 2026-07-31T09:00:00.000Z) 형식으로 반환합니다.
// 이 API 자체 문서의 결정이라, market-data API의 KST 결정과는 별개입니다.
func nowISO() string {
	return time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
}

// requestID는 X-Request-Id 요청 헤더를 그대로 쓰거나, 없으면 새로 발급합니다.
func requestID(r *http.Request) string {
	if id := r.Header.Get("X-Request-Id"); id != "" {
		return id
	}
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("req-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(buf)
}

// acceptOrderHandler는 POST /v1/orders를 처리합니다 (docs/api-specification.md 2.1, 2.2).
func acceptOrderHandler(store *OrderStore, idem *IdempotencyStore, producer Publisher) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		reqID := requestID(r)
		w.Header().Set("X-Request-Id", reqID)

		key := r.Header.Get("Idempotency-Key")
		if key == "" {
			writeError(w, reqID, http.StatusBadRequest, "MISSING_IDEMPOTENCY_KEY", "Idempotency-Key 헤더가 필요합니다.")
			return
		}

		if cached, ok := idem.Get(key); ok {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(cached.status)
			w.Write(cached.body)
			return
		}

		var req orderRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, reqID, http.StatusBadRequest, "INVALID_REQUEST", "요청 본문 JSON 파싱 실패")
			return
		}

		if !isTargetMarket(req.Market) {
			writeErrorAndCache(w, idem, key, reqID, http.StatusBadRequest, "INVALID_MARKET", "대상 20개 마켓에 없는 마켓 코드입니다.")
			return
		}
		if code, msg, ok := validateSide(req.Side); !ok {
			writeErrorAndCache(w, idem, key, reqID, http.StatusBadRequest, code, msg)
			return
		}
		if _, code, msg, ok := validatePrice(req.Market, req.Price); !ok {
			writeErrorAndCache(w, idem, key, reqID, http.StatusBadRequest, code, msg)
			return
		}
		if _, code, msg, ok := validateQuantity(req.Quantity); !ok {
			writeErrorAndCache(w, idem, key, reqID, http.StatusBadRequest, code, msg)
			return
		}

		// TODO(NFR-13): 컨슈머 랙 기준치 초과 시 429 CONSUMER_LAG_EXCEEDED로 즉시 거절.
		// 아직 컨슈머(매칭 엔진 B)가 없어 랙 개념 자체가 없어서 지금은 구현하지 않음.

		now := time.Now()
		order := &Order{
			OrderID:    store.NewOrderID(now),
			Market:     req.Market,
			Side:       req.Side,
			Price:      req.Price,
			Quantity:   req.Quantity,
			Status:     StatusAccepted,
			AcceptedAt: nowISO(),
		}

		if err := producer.PublishNew(r.Context(), order); err != nil {
			// Kafka 발행 실패는 일시적일 수 있어(브로커 재시작 등, 또는 토픽 자동 생성
			// 중인 최초 1회 등) 멱등성 캐시에 남기지 않습니다 — 같은 키로 재시도하면
			// 다시 시도할 수 있어야 합니다. 실제로 dev-kafka에서 토픽이 갓 생성될 때
			// 첫 발행이 이 경로로 실패하고 재시도가 성공하는 걸 확인함.
			log.Printf("주문 발행 실패 (market=%s, orderId=%s): %v", order.Market, order.OrderID, err)
			writeError(w, reqID, http.StatusInternalServerError, "INTERNAL_ERROR", "주문 발행에 실패했습니다.")
			return
		}
		store.Save(order)
		log.Printf("주문 접수 완료 (market=%s, side=%s, orderId=%s)", order.Market, order.Side, order.OrderID)

		body, _ := json.Marshal(order)
		idem.Put(key, http.StatusAccepted, body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		w.Write(body)
	}
}

// cancelOrderHandler는 DELETE /v1/orders/{orderId}를 처리합니다 (docs/api-specification.md 2.3).
func cancelOrderHandler(store *OrderStore, producer Publisher) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		reqID := requestID(r)
		w.Header().Set("X-Request-Id", reqID)

		orderID := r.PathValue("orderId")
		order, ok := store.Get(orderID)
		if !ok {
			writeError(w, reqID, http.StatusNotFound, "ORDER_NOT_FOUND", "해당 주문을 찾을 수 없습니다.")
			return
		}

		if order.Status == StatusFilled {
			writeError(w, reqID, http.StatusConflict, "ORDER_ALREADY_FILLED", "이미 전량 체결된 주문은 취소할 수 없습니다.")
			return
		}

		// 이미 취소된 주문을 다시 취소 요청하면, 새로 처리하지 않고 그때 남긴 결과를
		// 그대로 돌려줍니다(재시도에 안전하게 만들기 위함).
		if order.Status != StatusCanceled {
			order.Status = StatusCanceled
			order.CanceledQuantity = order.Quantity
			order.CanceledAt = nowISO()

			if err := producer.PublishCancel(r.Context(), order.OrderID, order.Market); err != nil {
				log.Printf("취소 발행 실패 (market=%s, orderId=%s): %v", order.Market, order.OrderID, err)
				writeError(w, reqID, http.StatusInternalServerError, "INTERNAL_ERROR", "취소 발행에 실패했습니다.")
				return
			}
			log.Printf("주문 취소 완료 (market=%s, orderId=%s)", order.Market, order.OrderID)
		}

		writeJSON(w, http.StatusOK, cancelResponse{
			OrderID:          order.OrderID,
			Status:           order.Status,
			CanceledQuantity: order.CanceledQuantity,
			CanceledAt:       order.CanceledAt,
		})
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, reqID string, status int, errorCode, message string) {
	writeJSON(w, status, errorResponse{ErrorCode: errorCode, Message: message, RequestID: reqID})
}

// writeErrorAndCache는 검증 실패 응답을 클라이언트에 보내고, 같은 Idempotency-Key로
// 재요청해도 재검증 없이 같은 결과가 나오도록 캐싱합니다.
func writeErrorAndCache(w http.ResponseWriter, idem *IdempotencyStore, key, reqID string, status int, errorCode, message string) {
	body, _ := json.Marshal(errorResponse{ErrorCode: errorCode, Message: message, RequestID: reqID})
	idem.Put(key, status, body)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write(body)
}

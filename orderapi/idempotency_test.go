package main

import "testing"

func TestIdempotencyStorePutAndGet(t *testing.T) {
	s := NewIdempotencyStore()
	if _, ok := s.Get("key-1"); ok {
		t.Fatal("아직 아무것도 안 넣었는데 조회되면 안 됨")
	}

	s.Put("key-1", 202, []byte(`{"orderId":"ord_1"}`))
	got, ok := s.Get("key-1")
	if !ok {
		t.Fatal("Put한 키는 조회돼야 함")
	}
	if got.status != 202 || string(got.body) != `{"orderId":"ord_1"}` {
		t.Errorf("got = %+v", got)
	}
}

func TestIdempotencyStoreDoesNotOverwrite(t *testing.T) {
	s := NewIdempotencyStore()
	s.Put("key-1", 202, []byte("first"))
	s.Put("key-1", 500, []byte("second")) // 같은 키로 다시 Put — 무시돼야 함

	got, _ := s.Get("key-1")
	if got.status != 202 || string(got.body) != "first" {
		t.Errorf("최초 응답이 유지돼야 하는데 got = %+v", got)
	}
}

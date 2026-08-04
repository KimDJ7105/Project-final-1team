package main

import (
	"context"
	"flag"
	"log"
	"sync"
	"time"
)

func main() {
	backend := flag.String("backend", "http://localhost:8080", "백엔드 base URL")
	date := flag.String("date", "", "재생할 날짜 (YYYY-MM-DD, 필수)")
	speed := flag.Float64("speed", 60, "재생 배속 (이벤트 간 대기 시간을 이 값으로 나눔)")
	flag.Parse()

	if *date == "" {
		log.Fatal("-date는 필수입니다 (YYYY-MM-DD)")
	}
	if _, err := time.Parse("2006-01-02", *date); err != nil {
		log.Fatalf("-date 형식이 올바르지 않습니다: %v", err)
	}

	ctx := context.Background()
	client := NewHTTPClient()

	manifest, err := FetchManifest(ctx, client, *backend, *date)
	if err != nil {
		log.Fatalf("매니페스트 조회 실패: %v", err)
	}
	log.Printf("매니페스트 수신: %d개 마켓", len(manifest.Markets))

	var wg sync.WaitGroup
	var mu sync.Mutex
	var failed []string

	// 마켓당 고루틴 1개, 전부 NewHTTPClient가 만든 단일 클라이언트를 공유합니다.
	// 한 마켓의 실패가 다른 마켓 재생을 막지 않도록 에러는 로그로만 남기고 계속 진행합니다.
	for _, entry := range manifest.Markets {
		wg.Add(1)
		go func(entry ManifestEntry) {
			defer wg.Done()
			if err := ReplayMarket(ctx, client, *backend, entry, *speed); err != nil {
				log.Printf("[%s] 재생 실패: %v", entry.Market, err)
				mu.Lock()
				failed = append(failed, entry.Market)
				mu.Unlock()
			}
		}(entry)
	}

	wg.Wait()

	if len(failed) > 0 {
		log.Printf("전체 재생 완료 — 실패한 마켓(%d개): %v", len(failed), failed)
		return
	}
	log.Printf("전체 재생 완료 — %d개 마켓 전부 성공", len(manifest.Markets))
}

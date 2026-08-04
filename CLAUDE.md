# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

This is an **infrastructure load-testing project**, not a production trading system. The end goal: collect real historical market data from Upbit, feed it to a "trader" application that replays it as massive simulated trading traffic against the backend, and use that traffic to load-test the backend + infra. Three independent Go modules live in this repo: `backend/` (data collection + serving, see below), `trader/` (data-replay engine, see below), and eventually the order-matching/frontend pieces owned by other teammates. `frontend/` is a separate Vue/Vite app (see its own `frontend/README.md`) not yet wired to the backend.

`backend/` is the service that collects cryptocurrency market data from the Upbit exchange (REST + websocket APIs); Kafka wiring for this data was considered (see `backend/kafka/producer.go`) but **decided against** — see "Data flow" below for why.

The repo is mid-migration (see recent commit history): the Kafka producer exists but is **not currently called from `main.go`** (it was wired once, hit errors, and was pulled back out — see commit history). `.env`/`godotenv`-based config loading (`backend/config`) *is* active — `config.Load()` is called from `main.go` and reads `APP_ENV`, `S3_BUCKET`, `PORT`. Expect to find work-in-progress wiring elsewhere — check `main.go` before assuming the Kafka producer is active.

## Commands

`backend/` and `trader/` are **separate Go modules** (each has its own `go.mod` — `module backend`, `module trader`), so `cd` into whichever one you're working on before running Go commands; there is no shared workspace file tying them together.

```
cd backend
go build ./...        # build everything
go run .               # starts the HTTP server (see backend/main.go); POST /v1/collect triggers a collection
go vet ./...
go test ./...
go mod tidy
```

```
cd trader
go build ./...
go run . -backend=http://localhost:8080 -date=2026-07-27 -speed=60   # replays one day against a running backend
go test ./...
```

### Tests

Both modules have `_test.go` files now (table-driven, stdlib `testing` only — no external test/mock libraries). Convention: tests live beside the code they cover, same package (white-box, so unexported helpers like `order.tickTier` are reachable), `TestXxx` names, `t.Run` subtests for table cases. `go test` (and everything else that isn't `go build`/`go run`) automatically excludes `_test.go` files from the compiled binary — that's a Go toolchain guarantee based on the filename suffix, not something this project configures.

What's covered and why: pure/deterministic logic that's easy to get subtly wrong — `order.RoundToTick` (tick-size table + rounding), `bot.MarketState`'s circular buffer (wraparound indexing), `MarketMakerBot.Decide`, `dataset.BuildBatch`/`BuildStream` (Upbit field → normalized field mapping, event merge+sort), `dataset.localStorage` save/load round-trip, and `backend`'s `manifestHandler`/`fileHandler` (via an in-memory fake `dataset.Storage` in `server_test.go`, and — since `fileHandler`'s `r.PathValue()` only populates when matched through a real `http.ServeMux`, not on a bare handler call — a small mux built in the test with the same route pattern as `main.go`).

What's deliberately **not** covered, and why: anything whose real work is a live network/AWS call — `backend/upbit` (real Upbit REST calls), `dataset.s3Storage` (real S3), `collectMarket`/`collectAllMarkets`/`ensureMarketCollected` (wrap `upbit` calls, no injectable fetcher to substitute — would need a refactor to test meaningfully), `client.Fetch*`'s success path against a real backend. These stay verified by hand (spin up `go run .`, hit it with `curl`/`Invoke-RestMethod`/a real `trader` run) the same way they were before tests existed — see the git history in this repo for examples of that workflow (e.g. the concurrent batch+stream request that proved `ensureMarketCollected`'s dedup works). AI bot stubs (`MomentumAIBot`/`MeanReversionAIBot`) aren't tested either — there's no logic yet, just a `nil` return.

### Local Kafka for development

```
cd infra/dev-kafka
docker compose up -d
```

Brings up a single-node KRaft-mode Kafka broker (`apache/kafka:3.7.0`) on `localhost:9092`, auto-creating topics. `backend/.env` holds `KAFKA_BROKER` for connecting to it (gitignored — copy/create locally, it's not committed).

## Architecture

- **`backend/main.go`** — entrypoint. Starts an HTTP server (`net/http`, stdlib `ServeMux`, no router dependency) on `cfg.Port` (env `PORT`, default `8080`). Routes: `POST /v1/collect`, `GET /v1/markets/data`, `GET /v1/markets/{market}/{kind}`. Does not currently call Kafka.
- **`backend/server.go`** — `collectHandler`: parses `{"date": "YYYY-MM-DD"}` from the request body, computes the UTC `[date 00:00, date+1 00:00)` window, and calls `collectAllMarkets`. Returns 400 on a malformed body/date; otherwise 200 with per-market results (a market-level failure, e.g. requesting a date more than 7 days old, shows up as that market's `status: "error"` rather than failing the whole request). Also `manifestHandler`: validates the `date` query param and returns `batchUrl`/`streamUrl` for all 20 `upbit.TargetMarkets` — purely mechanical URL generation, does **not** touch `dataset.Storage` or check whether the files actually exist (see Data flow below for why that's deliberate). Also `fileHandler` (`{kind}` is `batch` or `stream`): tries `dataset.Storage.LoadBatch`/`LoadStream` first; on `dataset.ErrNotFound` calls `ensureMarketCollected` (see `collector.go`) to collect just that one market on demand, then reads and serves it. 400 on bad `kind`/`date`, 404 on a market outside `upbit.TargetMarkets`, 500 on collection/read failure.
- **`backend/collector.go`** — `collectAllMarkets` loops over `upbit.TargetMarkets`, calling `collectMarket` per market (fetches ticks + every candle granularity from `upbit`, builds batch/stream via `dataset.BuildBatch`/`BuildStream`, saves via the injected `dataset.Storage`). One market's error doesn't stop the others. Also `ensureMarketCollected`: wraps `collectMarket` in a `golang.org/x/sync/singleflight.Group` keyed on `market+start` so concurrent requests for the same market+date (e.g. `fileHandler` serving batch and stream at nearly the same time) collapse into a single `collectMarket` call instead of running it twice.
- **`backend/upbit/candles.go`** — REST client for Upbit's `/v1/candles/*` endpoints. `FetchCandlesInRange` paginates backwards in time (via the `to` cursor) to collect all candles in `[start, end)` for fine-grained units (seconds/minutes); `FetchRecentCandles` fetches the last N candles for coarse units (days/weeks/months) where a single page suffices. Both respect Upbit's ~10 req/sec rate limit with a 110ms sleep between paginated calls.
- **`backend/upbit/ticks.go`** — REST client for `/v1/trades/ticks` (individual trade executions). Also paginates backwards using the `cursor` (last page's `sequential_id`). Upbit only allows querying trade ticks for the last 7 days; `FetchTradeTicksForDate` enforces that and errors otherwise.
- **`backend/upbit/websocket.go`** — legacy real-time ticker client over Upbit's websocket API (`ConnectAndListen`), explicitly marked as legacy in a source comment. Not called from `main.go` currently; streams ticker updates onto a channel for a caller to consume.
- **`backend/kafka/producer.go`** — thin wrapper (`Producer`) around `segmentio/kafka-go`'s `Writer`, JSON-marshals whatever struct is passed to `SendMessage` and publishes it keyed by string. Originally built to carry Upbit data onto a Kafka topic; **not used** for that anymore (see Data flow below) and not invoked anywhere in `main.go`.
- **`trader/`** — separate Go module (`module trader`, own `go.mod`), the data-replay + bot engine described in `docs/market-data-api.md`. Split into role-based sub-packages (import path `trader/<pkg>`) with a layered, cycle-free dependency graph: `client` and `bot` are leaves; `order` depends on `bot` (needs `bot.Decision`); `replay` depends on all three; `main.go` (package `main`, module root) wires everything together. No package is named the same as a variable that holds one of its own values in scope (e.g. `main.go` binds the shared `*http.Client` to `httpClient`, not `client`) — that would shadow the package import within that scope.
  - **`trader/client/`** (`client.go`, `types.go`) — talks to the backend. `NewHTTPClient` (5-minute timeout, to tolerate on-demand collection latency on a cache miss — see `backend`'s `ensureMarketCollected`), `FetchManifest`/`FetchBatch`/`FetchStream`. `types.go` re-declares the JSON shapes backend returns (`Manifest`, `BatchFile`, `StreamFile`, ...) rather than importing `backend/dataset` — the two modules are meant to only ever talk over HTTP+JSON, never share Go types at compile time.
  - **`trader/bot/`** (`bot.go`, `marketmaker.go`, `noise.go`, `largeorder.go`, `ai_bot.go`) — judgment logic only (FR-16), no knowledge of HTTP or order wire format. `MarketState` (`bot.go`) is not just a single latest price — a fixed-size circular buffer (`PriceHistorySize`, currently 30) of recent prices behind a `sync.RWMutex`. `Price()` returns the most recent entry (fast path for the per-market algorithmic bots); `History()` returns the buffer oldest→newest (for the AI bots, which need a price *series* to reason about momentum/mean-reversion, not just a point value).
    - **Per-market bots (`Bot` interface)**: **`MarketMakerBot`/`NoiseBot`/`LargeOrderBot`** — plain rule-based algorithms, meant to run as one independent instance *per market* (60 total across 20 markets — see `replay.marketBots()`), each reading that market's own `MarketState`. Per-market is deliberate here: a market maker has to quote against one specific order book, it can't function "portfolio-wide".
    - **Global bots (`GlobalBot` interface, `ai_bot.go`)**: **`MomentumAIBot`/`MeanReversionAIBot`** are **not** per-market — decided deliberately, both for cost (2 Bedrock calls per judgment cycle instead of 40) and because "AI trader scanning the whole board and picking where to act" fits the concept better than 20 independent copies. `GlobalBot.Decide(states map[string]*MarketState) []GlobalDecision` receives *every* market's `MarketState` in one call and can return decisions tagged with whichever markets it wants to act on. Both `Decide` implementations currently always return `nil` — connection points for AWS Bedrock, marked with `TODO` comments, deliberately not implemented yet.
    - All parameter constants (spread %, quantities, fire probabilities, `PriceHistorySize`) are placeholder defaults, not derived from real data — expect to retune them.
  - **`trader/order/`** (`order.go`, `ticksize.go`) — turns a `bot.Decision` into a submittable `Order`. `NewOrder` calls `RoundToTick` (`ticksize.go`) on every price first: bots compute prices as arbitrary floats (e.g. `MarketMakerBot`'s spread math), but `docs/api-specification.md` requires order prices to be a multiple of the market's price-tier-dependent tick size (`INVALID_PRICE_UNIT` otherwise) — without this, essentially every market-maker order would get rejected once a real order API is wired up. `tickTiers` mirrors Upbit's published KRW tick-size table (bigger ticks at higher price bands); `RoundToTick` returns both the snapped price and the decimal-place count for that tier, and `NewOrder` formats with that exact precision (not `strconv.FormatFloat`'s shortest-representation mode) to avoid floating-point noise like `634.00000000001` in the serialized order. `OrderSubmitter` interface decouples "decided to trade" from "how the order actually gets sent" — `POST /v1/orders` readiness is unknown, so `main.go` currently injects `LogOnlySubmitter` (logs only). Swap in an `HTTPOrderSubmitter` behind the same interface once that endpoint is confirmed live; nothing else needs to change.
  - **`trader/replay/`** (`replay.go`, `globalbot.go`) — the orchestration layer, ties `client`+`bot`+`order` together. `ReplayMarket` fetches one market's batch+stream, then walks `stream.Events` in order, sleeping `(events[i].ts - events[i-1].ts) / speed` between events before logging each one and calling `state.Update(price)`; alongside that it runs `marketBots()` as goroutines on a `time.Ticker` at `bot.Interval()/speed`, sharing the market's replay lifetime (`context.WithCancel`, cancelled when the event loop ends). `RunGlobalBots`/`globalBots()` (`globalbot.go`) run the two AI bots the same way but scoped to *all* markets at once — started once from `main.go` (not inside any single market's `ReplayMarket`), sharing the `states` map `main.go` built, stopped only when *all* markets finish replaying. **Known unresolved issue** (marked `TODO` in `globalbot.go`): global bot tickers are scaled by `/speed` like the algorithmic bots, but real Bedrock call latency won't shrink just because replay is sped up — revisit when actually wiring Bedrock (either decouple the AI bots' cadence from `speed`, or skip a tick if the previous call is still in flight).
  - **`trader/main.go`** — parses `-backend`/`-date`/`-speed` flags, fetches the manifest, builds a `map[string]*bot.MarketState` (one per market, shared between that market's own replay goroutine and the global bots), then spawns one goroutine per market calling `replay.ReplayMarket`, all sharing a single `*http.Client`. One market's error is logged and collected into a failure summary at the end; it does not stop or cancel the others (plain `sync.WaitGroup` — see `wg.Go` usage throughout `trader/`, Go 1.24+ — deliberately not `errgroup`, whose `WithContext` cancels sibling goroutines on the first error, exactly the propagation this design avoids).
  - **Tick-size rounding (`ticksize.go`)**: bots compute prices as arbitrary floats (e.g. `MarketMakerBot`'s spread math), but `docs/api-specification.md` requires order prices to be a multiple of the market's price-tier-dependent tick size (`INVALID_PRICE_UNIT` otherwise) — without this, essentially every market-maker order would get rejected once a real order API is wired up. `NewOrder` (`order.go`) calls `RoundToTick` on every `Decision.Price` before it becomes an `Order`, so this is handled once centrally rather than per-bot. `tickTiers` mirrors Upbit's published KRW tick-size table (bigger ticks at higher price bands); `RoundToTick` returns both the snapped price and the decimal-place count for that tier, and `NewOrder` formats with that exact precision (not `strconv.FormatFloat`'s shortest-representation mode) to avoid floating-point noise like `634.00000000001` in the serialized order.

### Data flow

Upbit REST/websocket (`upbit` package) → Go structs (`Candle`, `TradeTick`, `UpbitTicker`) → **JSON files persisted to AWS S3** → served to the trader app on request. **Decided: this path does not use Kafka** — `backend/kafka/producer.go` was originally built to carry Upbit data onto a Kafka topic (per an earlier draft in `docs/requirements.md`), but market data here is historical/finite/already-`ts`-sorted and has exactly one consumer per request, none of which benefit from Kafka's pub-sub/backpressure/ordered-live-stream strengths — so it stays unused for this path. (Kafka remains the right tool for the *separate* live order flow — `orders`/`executions` topics — once that subsystem exists.)

The trader app is **pull-based and now exists** (`trader/`, see below): it does not receive data automatically at startup. It requests simulation data for a period from the backend, and the backend responds with the JSON file(s) covering that period (read from S3). See [docs/market-data-api.md](docs/market-data-api.md) for the full spec and sequence diagram; summary: `POST /v1/collect` triggers collection, `GET /v1/markets/data?date=...` returns a manifest of URLs for all 40 files (20 markets × batch/stream), the trader fetches them individually/in parallel via `GET /v1/markets/{market}/{batch|stream}` — backend proxies file bytes, no S3 presigned URLs, so the trader never needs S3 credentials — and per-market replay pacing (walking `events` by `ts` gaps) happens entirely client-side in memory, decoupled from the HTTP download.

**Decided and implemented**: the on-demand-generation trigger (hybrid design above) belongs to the per-file GET handler, not the manifest — the manifest stays cheap/fast (no existence checks, no `dataset.Storage` access at all) and `GET /v1/markets/{market}/{batch|stream}` (`fileHandler`) checks whether its file exists and triggers `collectMarket` for just that one market if not. Since `collectMarket` produces both batch and stream together in one call, concurrent batch+stream requests for the same market+date are deduped via `ensureMarketCollected`'s `singleflight.Group` (see `collector.go`) rather than running `collectMarket` twice — verified by firing concurrent batch/stream requests for an uncollected date and confirming the collection log line appeared exactly once.

### Output JSON file format (finalized design, not yet implemented)

Collected data is persisted as JSON, split into two files per requested period because the trader app consumes them differently:

- **Batch file** — day/week/month/year candles. Delivered as one lump snapshot; the trader app doesn't need it in time order.
- **Stream file** — second/minute candles + individual trade ticks, merged into a **single array sorted ascending by `ts`**. The trader app walks this array in order and paces its outgoing traffic by the gap between consecutive `ts` values, so all the merge/sort work happens at generation time, not in the trader app.

Field names are normalized across both files (see mapping table below) instead of reusing Upbit's raw field names, so the trader app has one consistent shape regardless of candle unit.

**Batch file**
```json
{
  "market": "KRW-BTC",
  "range": { "start": "2026-07-29T00:00:00Z", "end": "2026-07-30T00:00:00Z" },
  "candles": {
    "days":   [ { "ts": 1785283200000, "open": 0, "high": 0, "low": 0, "close": 0, "volume": 0 } ],
    "weeks":  [ ... ],
    "months": [ ... ],
    "years":  [ ... ]
  }
}
```

**Stream file**
```json
{
  "market": "KRW-BTC",
  "range": { "start": "2026-07-29T00:00:00Z", "end": "2026-07-30T00:00:00Z" },
  "events": [
    { "type": "trade_tick",    "ts": 1785283200123, "price": 0, "volume": 0, "side": "BUY" },
    { "type": "candle_second", "ts": 1785283201000, "open": 0, "high": 0, "low": 0, "close": 0, "volume": 0 },
    { "type": "candle_minute", "ts": 1785283260000, "open": 0, "high": 0, "low": 0, "close": 0, "volume": 0 }
  ]
}
```

**Field name mapping (Upbit raw → normalized JSON)** — kept explicit because `trade_price` means a different thing in each source struct: candle close vs. tick execution price.

Candle (all units — seconds/minutes/days/weeks/months/years):

| Upbit field | JSON field | Meaning |
|---|---|---|
| `timestamp` | `ts` | Candle time, UTC epoch ms |
| `opening_price` | `open` | Open |
| `high_price` | `high` | High |
| `low_price` | `low` | Low |
| `trade_price` | `close` | Close (**not** the same meaning as tick's `trade_price`) |
| `candle_acc_trade_volume` | `volume` | Volume for the candle period |

Trade tick:

| Upbit field | JSON field | Meaning |
|---|---|---|
| `timestamp` | `ts` | Execution time, UTC epoch ms |
| `trade_price` | `price` | Execution price (**not** the same meaning as candle's `trade_price`) |
| `trade_volume` | `volume` | Execution volume |
| `ask_bid` | `side` | `"BUY"` / `"SELL"` — remapped from Upbit's `"BID"`/`"ASK"` for clarity (BID→BUY, ASK→SELL) |

Storage target is **AWS S3** (not local disk) for prod. The bucket exists (see the Infra section below) and `dataset.s3Storage` is a real implementation (`aws-sdk-go-v2`, `HeadObject`-then-`PutObject` idempotency check, region hardcoded to `ap-northeast-2`) — verified end-to-end against an EC2 instance profile. Dev writes to local disk via `dataset.localStorage` instead; `APP_ENV` in `backend/config` selects between the two.

### Multi-market collection (implemented)

`upbit.TargetMarkets` (`backend/upbit/markets.go`) lists the ~20 target markets/coins; `collectAllMarkets` (`backend/collector.go`) loops over all of them for every request. Decisions so far:

- **One batch file + one stream file per market** (not one shared file for all markets) — keeps re-collection/re-generation of one coin from touching the others, and maps directly onto the S3 key layout below.
- On the trader-app side, the intended design is **one goroutine per market**, each independently walking its own stream file's `events` array in `ts` order and pacing sends by the gap between consecutive `ts` values — this is what actually produces the "many coins trading concurrently" load pattern, not just a speed optimization. Goroutines should share a single HTTP client / Kafka producer rather than each opening their own connection, and errors in one market's goroutine must not abort the others (isolate per-goroutine, collect via `WaitGroup`/`errgroup`).

### S3 storage design (decided so far, not yet implemented)

- **Key layout**: `s3://{bucket}/{market}/{start}_{end}_batch.json` and `.../{start}_{end}_stream.json` — period encoded in the filename, market as a path prefix.
- **Idempotency, not overwrite**: re-requesting the same market+period is not expected in normal use, but as a safety check, the backend should `HeadObject` before generating — if the file already exists, serve it as-is instead of regenerating/re-uploading.
- **No lifecycle policy**: this project's premise is re-running the *same* simulation dataset against evolving infra to measure improvement, and the project timeline is ~1 month, so objects must not expire or auto-transition to a cheaper storage class.
- **Access scope**: only the backend talks to S3 (IAM policy scoped to the backend). The trader app never gets its own S3 credentials — it only talks to the backend's request/response API. This includes the trader app's own simulation *result* data: for now the trader app sends results to the backend and **the backend writes them to S3**, rather than the trader app writing directly. Revisit only if that becomes a bottleneck.
- **Auth (dev vs. prod)**: final deployment target is AWS (EC2 or EKS), but local development also needs to work. The AWS SDK's default credential chain handles this without code branching — env vars/shared profile locally, IAM role automatically in AWS (EC2 instance profile, or **IRSA** for EKS — IRSA is a cluster/infra setup task: OIDC provider + IAM role trust policy + ServiceAccount annotation, not application code). The same env-var-driven pattern should extend to the planned dev-local-DB vs. prod-RDS split — connection target read from an env var (`.env` locally via the already-added `godotenv` dep, ConfigMap/Secret in EKS), not hardcoded or branched on environment in code.

### Upload/generation timing — hybrid (decided)

Simulation data is served via **both** pre-generated files and on-demand generation: some batch/stream files may be created ahead of time, but the trader app can also request a market+period that doesn't exist yet, triggering on-the-fly collection from Upbit → S3 upload → serve. The `HeadObject`-based idempotency check above is what makes this work either way. `POST /v1/collect` (see Architecture above) is the on-demand-collection trigger; it always runs across all 20 `upbit.TargetMarkets` for the requested date rather than accepting a single market — there's no per-market request shape yet. The trader-app-facing "give me the file(s) for this period" read/serve API is still not designed.

## Infra (Terraform)

`infra/` holds this project's Terraform. **This AWS account (`727646470302`, `ap-northeast-2`) is shared across multiple teams** — other teams' buckets/roles already exist there (`team5-*`, `team2-doro-*`, etc.), so anything created here is namespaced `team1-*` to avoid collisions.

- **`infra/bootstrap/`** — one-off bootstrap stack that creates the Terraform state bucket itself (chicken-and-egg: the state bucket can't be its own backend on first apply). Its own state now lives in that same bucket under `bootstrap/terraform.tfstate` (see below) — it was applied once with local state, and after the bucket existed, migrated onto S3 and the local `.tfstate` was removed. **Local Terraform state in this repo's working directory has already been lost once** (this folder appears to sync via OneDrive, which is suspected to have interfered with the local `.tfstate` file) — this is why bootstrap state was moved onto S3 immediately rather than left local.
- **`infra/`** (root) — main stack, backed by S3 remote state.

**S3 buckets (both created, both tagged `Team = team1`):**
- `team1-terraform-state-s3` — Terraform state. Versioning + SSE-S3 encryption + full public-access block. Holds two state files as separate keys in the same bucket:
  - `bootstrap/terraform.tfstate` — the bootstrap stack's own state (`infra/bootstrap/`)
  - `truss/terraform.tfstate` — the main stack's state (`infra/` root)
  
  Any future Terraform stack (e.g. for EC2/EKS compute) should follow the same convention: new `key = "<stack-name>/terraform.tfstate"` in this same bucket, rather than a new bucket.
- `team1-truss-market-data` — the batch/stream JSON data bucket described above. SSE-S3 encryption + full public-access block, **no lifecycle rules** (per the "no lifecycle policy" decision above).

**Both bucket resources have `lifecycle { prevent_destroy = true }`** in their Terraform config — an accidental `terraform destroy` or a config change that would force-replace the bucket will error out instead of deleting it. This does not protect against manual deletion via the console/CLI, only against Terraform-driven destroys.

**Decided**: the backend authenticates to `team1-truss-market-data` via whatever EC2 instance profile / EKS IRSA role ends up running it — not via a separate personal/dev IAM user. `infra/iam.tf` already provisions `aws_iam_role.team1_ec2_role` / `aws_iam_role_policy.team1_ec2_s3_policy` (scoped to just this bucket) / `aws_iam_instance_profile.team1_ec2_profile`, kept around specifically so the next compute (EC2 or EKS) can attach it without recreating it. This was verified once already: a temporary EC2 in `infra/network.tf`'s public subnet ran the backend with this instance profile and uploaded all 20 markets to S3 successfully, then was torn down (the EC2 instance, its keypair, and its security group — not the IAM role/policy/instance profile, which stay). Terraform itself authenticates as the shared student IAM user (`a-student-05`), which already has broad account permissions — that's fine for provisioning resources but is not the identity the backend uses at runtime.

## Conventions

- Comments and log/print messages in this codebase are written in Korean; match that style when editing existing files in `backend/upbit` and `backend/kafka`.
- Timestamps are handled in UTC throughout the `upbit` package (Upbit's API returns both UTC and KST fields; this codebase consistently parses/compares using the UTC ones).

# ICT Trading Bot — Rust

Thư viện Rust Edition 2021 cho chiến lược ICT trên crypto futures, có adapter Binance USD-M, Bybit V5 và OKX V5. Giá, khối lượng, sizing và các phép tính rủi ro dùng `rust_decimal::Decimal`; dữ liệu số từ sàn được đọc trực tiếp từ chuỗi.

Repository hiện có CLI **PAPER không cần API key**, các thành phần của cả năm lớp, journal/checkpoint/outbox để khôi phục trạng thái cục bộ và inventory scanner chỉ đọc cho ba sàn, nhưng **chưa phải bot chạy tiền thật hoàn chỉnh hoặc đã được xác nhận production-ready**. CLI PAPER chỉ mô phỏng; chưa có runner nối toàn bộ pipeline execution có xác thực, bộ đối soát toàn tài khoản đủ để mở quyền entry hay kết quả kiểm thử giao dịch demo/live. Khôi phục checkpoint hoặc hoàn tất một inventory scan không đồng nghĩa đã xác nhận tài khoản flat. Các kiểm thử execution dùng dữ liệu giả và máy chủ HTTP/WebSocket cục bộ; không đặt lệnh thật.

## Chạy PAPER với 100 USDT giả lập, không cần API key

Lệnh dưới đây đọc dữ liệu thị trường **công khai** của Bybit qua REST, chạy ICT → strategy → risk → broker mô phỏng trong máy và ghi JSONL. Không kết nối tài khoản, không đọc biến môi trường credentials, không nạp tiền và không gửi lệnh lên sàn. Từ `live` ở đây chỉ nói dữ liệu đang cập nhật, **không phải live trading**.

Chạy thử giới hạn 5 phút sau khi nạp warmup:

```powershell
cargo run --locked --release --bin paper -- live --seconds 300
```

Trên Windows có thể dùng wrapper từ thư mục repository:

```powershell
.\scripts\paper.ps1 live --seconds 300
```

Wrapper ưu tiên Cargo native. Nếu không có Cargo, cần tự mở Docker Desktop ở chế độ Linux container trước; wrapper dùng `rust:1.89-bookworm`, giữ cache Cargo/target bằng named volumes và chạy foreground, không phụ thuộc container kiểm thử có sẵn. Nó không chuyển tiếp credentials/biến môi trường host vào container. Lần build đầu có thể cần tải image/dependency. Các đường dẫn tương đối tính từ repository; ở chế độ Docker, `--input`/`--output` phải nằm trong repository. Wrapper theo dõi đúng container của lần chạy để dừng/dọn khi thoát; không xóa cache hay log phiên cũ. Đóng cưỡng bức cả terminal hoặc Docker daemon không phải shutdown đã xác nhận.

Mặc định: `ETHUSDT`, M1, tài khoản giả lập 100 USDT, warmup 500 nến đã đóng và poll mỗi 5 giây. Warmup chỉ khởi tạo detector/strategy, **không tạo giao dịch quá khứ**. Có thể đổi các giả định rõ ràng:

```powershell
.\scripts\paper.ps1 live --symbol ETHUSDT --balance 100 --seconds 300 --poll-seconds 5 --warmup 500 --fee-bps 6 --stop-slippage-bps 2 --output paper-runs\session-01.jsonl
```

Nếu bỏ `--seconds`, tiến trình chạy cho đến `Ctrl+C` hoặc lỗi/giới hạn an toàn; chạy trong foreground để giữ quyền dừng. Mỗi lần `live` bắt đầu tài khoản mới với số dư khởi tạo, mặc định 100 USDT; **không tự resume** vị thế hoặc số dư từ log trước. Log mặc định nằm trong `paper-runs/`, tên file được in ra console; `create_new` từ chối ghi đè file đã có. Shutdown hủy entry đang chờ nhưng **không tự đóng vị thế đã mở**: snapshot cuối vẫn thể hiện exposure và equity theo giá đóng gần nhất, không ghi nhận một giao dịch thoát tưởng tượng.

Replay offline dùng **file JSONL phiên PAPER đầy đủ do CLI tạo**, không phải JSONL nến tùy ý. Thay `LIVE_SESSION.jsonl` dưới đây bằng đường dẫn phiên đã in ra console, và chọn tên output chưa tồn tại:

```powershell
.\scripts\paper.ps1 replay --input paper-runs\LIVE_SESSION.jsonl --output paper-runs\replay-01.jsonl
# Hoặc không dùng wrapper:
cargo run --locked --release --bin paper -- replay --input paper-runs/LIVE_SESSION.jsonl --output paper-runs/replay-01.jsonl
```

Replay dựng lại cấu hình/metadata đã ghi, chạy lại nến theo thứ tự và so sánh kết quả xác định; không gọi market API và không sửa phiên gốc. Phiên thiếu record kết thúc, dữ liệu hỏng, phiên lỗi không tái lập được hoặc kết quả không khớp sẽ bị từ chối. JSONL không có chữ ký xác thực nguồn dữ liệu thị trường. Đây là kiểm tra tái lập phiên mô phỏng, không phải khôi phục tài khoản để tiếp tục giao dịch.

Các giả định cần hiểu trước khi đánh giá kết quả:

- Risk mặc định 1% mỗi trade, circuit breaker 5% theo ngày UTC; chỉ đánh giá equity khi xử lý **nến đã đóng**, không giám sát drawdown intrabar. Gap và slippage có thể khiến lỗ thực hiện vượt ngưỡng 5%; đây không phải bảo đảm mức lỗ tối đa.
- Phí cố định 6 bps **mỗi chiều** và stop slippage bất lợi 2 bps là giả định mô phỏng, không phải fee tier thực của tài khoản. Sizing kiểm tra phí/slippage, tick/step, min-notional và net RR tối thiểu 2. Entry notional cộng phí entry không vượt cash khả dụng; giới hạn 1x này không mô phỏng việc sàn phân bổ margin.
- Killzone mặc định theo `America/New_York`, có DST, thứ Hai đến thứ Sáu. Không có setup hợp lệ có thể dẫn đến **0 giao dịch**; CLI không tạo tín hiệu để ép có lệnh.
- Metadata lấy từ public endpoint, không dùng filter hard-code khi request thất bại. Nến phải liên tục, đúng symbol/timeframe và chính xác Decimal; chỉ coi là đóng khi cả đồng hồ local và timestamp phản hồi Bybit đã qua ranh giới nến. Thiếu server time hoặc lệch local/server quá 10 giây làm dừng phiên; cần đồng bộ đồng hồ hệ thống. Lỗi dữ liệu hoặc transport không tự retry vô hạn.
- Fill dùng quy tắc mô phỏng từ OHLC nến đóng, không có tick/order book, queue priority hay partial fill; chưa mô hình hóa funding, margin/liquidation. Poll 5 giây không phải execution low-latency; kết quả PAPER không chứng minh lợi nhuận hoặc chất lượng khớp lệnh thật.

### Kiểm chứng PAPER ngày 28/10/2026

Trên Rust 1.89, Linux container qua Docker Desktop Windows:

- `cargo test --all-features --locked`: **333 passed** (326 library + 7 CLI/session).
- `cargo test --no-default-features --locked`: **227 passed** (220 library + 7 CLI/session).
- `cargo fmt --all -- --check`, Clippy `--all-targets` với `-D warnings` ở cả hai cấu hình trên, Rustdoc `-D warnings` và release build: đạt.
- Đã chạy release binary với public Bybit ETHUSDT M1 trong **75 giây** sau 500 nến warmup: thêm 1 nến đóng, 1 tín hiệu detector, 0 entry intent, 0 giao dịch; balance/equity cuối cùng **100 USDT**, phí/PnL bằng 0. Đây chỉ là smoke test, chưa phải mẫu thống kê đánh giá chiến lược.
- Replay offline đối chiếu thành công toàn bộ phiên trên. Một phiên riêng kiểm tra `SIGTERM` cũng ghi được summary và replay thành công. Các test fixture riêng có giao dịch hoàn tất kiểm tra fill, phí, profit/loss và định danh qua replay; không gán kết quả fixture cho thị trường thật.
- Đã kiểm tra cú pháp `scripts/paper.ps1`. Chưa xác nhận native Windows build hoặc chạy end-to-end wrapper với cache Docker mới; lần đầu có thể phải tải dependencies/build.

Log kiểm chứng cục bộ: `paper-runs/verification-20261028-live-100usdt.jsonl`, bản replay `paper-runs/verification-20261028-live-replayed.jsonl`; phiên kiểm tra dừng nằm ở hai file `verification-20261028-sigterm-*.jsonl`. Các phiên này đã dừng, không có bot chạy ngầm. Không dùng API tài khoản hay gửi lệnh demo/tiền thật.

## Deploy PAPER lên Railway

Dockerfile ở root chỉ build release binary `paper` bằng Rust 1.89, `Cargo.lock` và `--no-default-features`; runtime không chứa Cargo/source code và không cần API key. `.dockerignore` chỉ cho phép Cargo metadata và source cần build vào context, loại log, credentials và cache cục bộ. Đây là **worker thử nghiệm PAPER**, chưa phải dịch vụ tự phục hồi 24/7: mỗi lần restart/redeploy là một phiên mới, số dư mới 100 USDT, không tiếp tục vị thế hoặc daily-risk baseline của phiên cũ.

Thiết lập trên Railway Dashboard:

1. Tạo service từ GitHub repository `minhhoang080100-beep/TradingBot`, branch `main`, root directory là root repository. Railway tự nhận `Dockerfile`; không cần custom Build Command. [Dockerfile detection](https://docs.railway.com/builds/dockerfiles)
2. Trước phiên cần giữ dữ liệu, gắn persistent **Volume tại `/data`**. Bot ghi JSONL ở `/data/paper-runs/`; filesystem ngoài volume không phải kho lưu trữ bền vững. Image dùng UID `0:0` để tương thích volume root-owned, không tự đổi ownership dữ liệu đã có. Đây là đánh đổi quyền của container, không phải cấu hình least-privilege đã harden. [Volume caveats](https://docs.railway.com/volumes/reference#caveats)
3. Chỉ chạy **1 instance**, không cần public domain, exposed port hoặc HTTP healthcheck vì đây không phải web server. Không thêm exchange API key/secret vào Variables.
4. Đặt **Restart Policy = Never** trong giai đoạn này. Bot dừng khi gặp lỗi dữ liệu/network; tự restart sẽ tạo tài khoản mới và làm mất tính liên tục của phép thử. Tắt **Serverless/app sleeping** nếu muốn phiên chạy liên tục. [Restart policy](https://docs.railway.com/deployments/restart-policy), [Serverless](https://docs.railway.com/deployments/serverless)
5. Lần đầu đặt Custom Start Command để thử 5 phút sau warmup:

   ```text
   /usr/local/bin/paper live --balance 100 --seconds 300
   ```

   Đây là lệnh đầy đủ vì Railway override `ENTRYPOINT` của Docker image. Khi đã kiểm tra log, volume và kết nối public Bybit, bỏ Custom Start Command để dùng mặc định chạy liên tục `paper live --balance 100`. Việc redeploy này vẫn bắt đầu **phiên mới**, không nối tiếp phiên thử. [Start command](https://docs.railway.com/deployments/start-command)
6. Xem deployment logs để xác nhận `PAPER`, đường dẫn JSONL, warmup và cập nhật nến; 0 giao dịch không tự chứng minh bot lỗi. Theo dõi Usage/credit còn lại và sao lưu JSONL trước khi trial/volume hết hạn. Limited Trial có thể hạn chế outbound; cần kiểm tra public Bybit từ chính môi trường triển khai và tuân thủ điều kiện truy cập theo khu vực, không suy ra từ kết nối trên máy cá nhân. [Trial limits](https://docs.railway.com/pricing/free-trial), [Bybit access restrictions](https://bybit-exchange.github.io/docs/v5/guide)

Không đặt `--output` thành một tên file cố định: CLI dùng `create_new` và sẽ từ chối file đã tồn tại. Volume giữ log qua deployment, **không bổ sung tính năng resume**. Khi lỗi, kiểm tra log trước khi chủ động bắt đầu phiên mới; không gộp các phiên 100 USDT thành một đường equity liên tục. Chưa có healthcheck theo độ mới dữ liệu, cảnh báo, tự khôi phục mạng/account PAPER hoặc rotation log tự động. Không coi trạng thái deployment `Active` là xác nhận chiến lược đang nhận dữ liệu mới. Sau khi nối GitHub, push tiếp theo có thể kích hoạt redeploy; cần kiểm soát auto-deploy nếu muốn giữ một phiên thử liên tục.

Kiểm tra image cục bộ (không gọi API tài khoản):

```sh
docker build -t ict-paper .
docker run --rm ict-paper --help
docker volume create ict-paper-data
docker run --rm --mount source=ict-paper-data,target=/data ict-paper live --balance 100 --seconds 300
```

Named volume trên máy chỉ dùng cho ví dụ cục bộ, không phải Railway volume. Không có thao tác tạo service, nâng gói hay deploy Railway tự động trong repository. Build image/tag và gói hệ điều hành chưa pin bằng digest/snapshot; `Cargo.lock` cố định dependency resolution, không bảo đảm build bit-for-bit.

## Kiến trúc hiện có

| Lớp | Module | Trách nhiệm |
| --- | --- | --- |
| Market data | `src/data_engine.rs`, `src/exchange/`, `src/candle_store.rs` | Public WebSocket của ba sàn, heartbeat/reconnect, chuẩn hóa nến, ring buffer theo instrument/timeframe, phát hiện dữ liệu thiếu hoặc trùng. |
| ICT | `src/ict_engine.rs`, `src/models.rs` | Swing fractal 3/5 nến, BOS/MSS, displacement, FVG và mitigation, sweep BSL/SSL, killzone và các domain model có kiểm tra invariant. |
| Strategy | `src/strategy.rs` | State machine `Idle` → `WaitingForRetracement` → `InPosition`; sweep → MSS/displacement + FVG → retracement; sinh `TradeIntent`, quản lý expiry và lifecycle. |
| Risk | `src/risk.rs` | Sizing bằng Decimal, tick/step rounding, SL ngoài sweep, TP tại opposing liquidity với RR ≥ 2, reservation và daily circuit breaker. |
| Execution | `src/execution.rs`, `src/execution/` | Ký REST, adapter order/user-stream, coordinator xử lý cumulative fill và bracket, HTTP transport, private WebSocket một phiên. |
| Durability liên lớp | `src/journal.rs`, `src/durability.rs`, `src/risk/checkpoint.rs`, `src/execution/coordinator/checkpoint.rs` | Journal khóa độc quyền, checkpoint risk/execution và outbox chung; sync trước khi giao side effect cho caller; khôi phục bằng chứng và replay tombstone, không tự gửi lại lệnh. |
| Inventory chỉ đọc | `src/reconciliation.rs`, `src/reconciliation/` | Quét GET theo phạm vi venue đã khai báo, pagination có giới hạn và so sánh định danh với execution cục bộ; không sửa lệnh, giải phóng risk hay cấp quyền entry. |
| PAPER keyless | `src/paper/`, `src/bin/paper.rs`, `src/bin/paper/session.rs` | Public Bybit REST → ICT/strategy/risk → broker mô phỏng, số dư/phí/equity, JSONL và replay offline; tách biệt với authenticated execution. |

Các actor dùng bounded `tokio::sync::mpsc`; candle store dùng `Arc<RwLock<_>>`. Phần tính toán và coordinator có API đồng bộ để kiểm thử độc lập với mạng. `OrderBlock` hiện là domain model; không nên hiểu sự có mặt của model là đã có một chiến lược order-block riêng.

Killzone dùng `America/New_York`, có chuyển EST/EDT theo DST: London `[02:00, 05:00)` và New York `[07:00, 10:00)`. Đây là giờ địa phương New York, không phải UTC−5 cố định cả năm. Lịch mặc định là thứ Hai đến thứ Sáu, có cấu hình ngày giao dịch.

## Risk và execution

Risk mặc định là 1% equity mỗi trade, tối đa 5% tổng planned risk đang được reservation, drawdown ngày 5% và RR tối thiểu 2. Quantity luôn làm tròn xuống theo step. SL được đẩy ít nhất một tick ra ngoài extreme của sweep; RR được kiểm tra lại sau rounding. Circuit breaker so với opening equity của ngày UTC và giữ trạng thái tripped trong ngày, kể cả khi equity phục hồi. Caller phải cung cấp baseline đúng của ngày và equity snapshot còn mới; snapshot intraday không tự đặt lại baseline.

`ExecutionCoordinator` quản lý một venue/account và chỉ cho một bracket đang hoạt động. Account cần dành riêng cho coordinator, ở chế độ one-way/net. Khi khởi tạo, coordinator chặn entry cho đến khi nhận một `FlatAccountSnapshot` còn mới, được đối soát đầy đủ sau khi subscribe: không có regular order, algo/conditional order hoặc position trên bất kỳ symbol nào. Struct này chỉ là contract của caller. Inventory scanner mới cung cấp bằng chứng đọc theo scope, không chuyển thành `FlatAccountSnapshot` và chưa thay thế đối soát toàn tài khoản.

REST ACK chỉ cho biết yêu cầu được tiếp nhận. Nó không xác nhận fill, không chứng minh cancellation đã hoàn thành và không giải phóng reservation. Coordinator dùng cumulative filled quantity từ private stream hoặc kết quả query đầy đủ để xử lý duplicate, partial fill và fill/cancel race. Với mỗi phần entry mới khớp, coordinator tạo riêng SL và TP reduce-only cho phần khối lượng đó; khi exit khớp, nó yêu cầu hủy nhánh còn lại phù hợp.

Bracket không được đặt nguyên tử và không phải OCO nguyên tử xuyên sàn. Có khoảng thời gian giữa entry fill và xác nhận protective orders. Khi request lỗi hoặc kết quả không rõ, coordinator chặn entry, yêu cầu cancellation/query và reconciliation; nó không tự kết luận tài khoản đã flat. Supervisor còn thiếu phải xử lý exposure đang mở, trường hợp SL/TP bị từ chối, cảnh báo và quy trình phục hồi.

| Sàn | Phạm vi execution | Chi tiết cần giữ đúng |
| --- | --- | --- |
| Binance | USD-M linear perpetual, one-way | LIMIT entry; conditional SL/TP qua `/fapi/v1/algoOrder`; phân biệt algo và child matching-engine order; private stream dùng listen key. |
| Bybit | Linear perpetual, `positionIdx=0` | LIMIT entry; conditional Market với `reduceOnly` + `closeOnTrigger`; trigger direction theo side; private `order.linear` và `position.linear`. |
| OKX | Linear SWAP, net mode, cross margin | Quantity là **số contract**; `InstrumentRiskSpec` phải dùng multiplier phù hợp `ctVal`; ordinary order/position ở private WS, algo lifecycle cần business WS riêng. |

Inverse, dated futures và hedge mode chưa được hỗ trợ bởi risk/execution hiện tại. Không suy ra quantity contract của OKX từ quantity base-asset của Binance/Bybit.

`RestExecutor` dùng origin cố định theo `ExecutionEnvironment::Demo` hoặc `Production`, tắt redirect và retry, có timeout và giới hạn response một MiB. Với OKX, cờ demo của adapter và header simulated-trading phải khớp environment của executor. Bybit Demo Trading dùng `api-demo.bybit.com` / `stream-demo.bybit.com`, khác với testnet.

`run_private_session` chạy đúng một kết nối: chờ auth thành công trước subscribe, hỗ trợ nhiều subscription ACK, heartbeat 20 giây, auth timeout 10 giây, inactivity timeout 60 giây và giới hạn frame/message một MiB. Nó phát `ReconciliationRequired` lúc bắt đầu và khi phiên kết thúc; heartbeat/auth ACK không mở lại quyền entry. Hàm không tự reconnect hoặc replay placement. Caller phải tạo lại chữ ký/listen key và đối soát trạng thái trước khi tiếp tục.

## Inventory chỉ đọc và phạm vi đối soát

`RestExecutor::inventory(adapter, limits, cancel)` dùng cùng credentials và origin đã pin của execution transport để thu thập `InventoryReport`. `scan_inventory` là API thấp hơn với `InventoryTransport` riêng cho mock hoặc tích hợp khác; caller chịu trách nhiệm ghép đúng venue, account và environment. Scanner chỉ chấp nhận GET không có body, ký mới từng request/page, đọc các partition tuần tự và không tự retry. Nó không đặt/hủy lệnh, đổi account mode hay lấy API key để ghi vào report.

| Scope / adapter | Các nhóm dữ liệu được đọc | Không được suy ra ngoài scope |
| --- | --- | --- |
| `BinanceUsdM` / `BinanceInventory` | Bốn GET: `/fapi/v1/positionSide/dual`, `/fapi/v1/openOrders`, `/fapi/v1/openAlgoOrders`, `/fapi/v3/positionRisk`; không lọc symbol. Các endpoint này được đọc không phân trang. | Không bao gồm Spot, COIN-M, Portfolio Margin, SAPI advanced-algo bots, subaccount khác hoặc các chiến lược do sàn quản lý. |
| `BybitLinearUsdtUsdc` / `BybitInventory` | Sáu partition: regular orders, `StopOrder` và positions, riêng cho USDT và USDC; dùng `/v5/order/realtime` với `openOnly=0` và `/v5/position/list`, theo `nextPageCursor` bắt buộc. | Không bao gồm Spot, inverse, options, lending hoặc đánh giá toàn bộ collateral/exposure của UTA. Mode cấu hình theo symbol: thấy `positionIdx=0` hoặc không có position **không chứng minh** account-wide one-way. |
| `OkxTrading` / `OkxInventory` | Mười một partition: account config, positions, regular pending orders và tám họ pending algo `conditional`, `oco`, `chase`, `trigger`, `move_order_stop`, `iceberg`, `twap`, `smart_iceberg`. Không lọc `instType`/`instId`; trang order đủ 100 dòng tiếp tục bằng `after` của ID cuối đến trang ngắn/rỗng. | Không bao gồm các endpoint riêng cho grid, recurring-buy, signal/copy trading, funding và các sản phẩm/hệ sinh thái khác. Không phải chứng nhận toàn bộ tài khoản không có hoạt động. |

Order thủ công, client ID ngoài bot, paused algo và nonzero position của sản phẩm ngoài chiến lược không bị bỏ qua. Position được đếm từng dòng; hai hedge leg không được cộng bù để kết luận không có exposure. `one_way_mode()` trả `Option<bool>`: Bybit thường là `None` khi chưa thấy hedge evidence, không tự mặc định `true`; có hedge evidence thì trả `false`. Bằng chứng mode mâu thuẫn làm scan thất bại.

`InventoryLimits::default()` giới hạn 100 trang mỗi partition, tổng 20.000 record và deadline 30 giây cho cả scan. `InventoryLimits::new` cho phép tối đa 1.000 trang/partition, 100.000 record và deadline không quá 120 giây, không chấp nhận giá trị zero. Ngoài ra có tối đa 32 partition và một MiB mỗi response. Scanner dừng khi gặp HTTP/protocol error, trường bắt buộc bị thiếu, decimal không chính xác, cursor rỗng/sai/lặp, record trùng hoặc vượt giới hạn. Hủy bằng `CancellationToken` hoặc hết deadline trả lỗi, không trả một report thiếu partition dưới dạng kết quả hoàn tất. Caller phải hủy scan khi private stream mất continuity; module chưa tự theo dõi stream thay caller.

`report.review(&execution, now, max_age)` chỉ so sánh định danh và kiểm tra độ mới từ **thời điểm bắt đầu scan**, không phải từ page cuối. Report chứa local receipt times, không phải timestamp của một snapshot nguyên tử trên sàn. Kết quả phân biệt order ngoài bot/xung đột ID (`unmatched_orders`), local order không có trong open lists (`absent_from_open_lists`), các local order cần query (`query_orders`), số open order và số position khác zero. Mọi local order chưa giải quyết vẫn cần query cumulative/terminal evidence, kể cả order có mặt trong inventory. Order biến mất khỏi open list **không chứng minh** nó bị hủy, chưa từng được nhận, đã khớp hoặc vị thế đã đóng; không được dùng việc vắng mặt để giải phóng reservation.

Report/review không thay đổi execution/risk, không tự hủy order ngoài bot, không có phép chuyển sang `FlatAccountSnapshot` và không mở lại entry. REST pagination không nguyên tử: tài khoản có thể thay đổi giữa các page dù không phát hiện cursor/record trùng. Vì vậy report rỗng vẫn cần đối soát các operation chưa rõ kết quả, history/fill, coverage ngoài scope, mode và continuity trước mọi quyết định vận hành.

## Journal, checkpoint và khôi phục cục bộ

`DurableAccount` sở hữu một `RiskManager`, một `ExecutionCoordinator` và outbox trong cùng checkpoint. `apply(DurableInput)` áp dụng thay đổi trên bản sao, cập nhật reservation từ lifecycle, kiểm tra tính nhất quán rồi ghi journal và `sync_all` trước khi công bố trạng thái mới. Mỗi checkpoint giữ daily-equity baseline, drawdown latch, risk reservation, order IDs, cumulative fills, các nhánh bảo vệ và tombstone của những giao dịch đã kết thúc. Restore kiểm tra phiên bản, cấu trúc, decimal dạng chuỗi, tính nhất quán kinh tế và quan hệ giữa risk/execution/outbox; deserialization riêng lẻ không cấp quyền giao dịch.

Quy trình gửi có hai lần ghi bền vững: thay đổi nghiệp vụ tạo outbox ở trạng thái `Queued`; `claim_next(now)` ghi và sync trạng thái `Attempted` trước khi trả `Delivery` cho sender. Chỉ gửi network action được trả về từ claim, không gửi trực tiếp từ `outbox()` hay một checkpoint đã đọc. `acknowledge` lưu ACK/receipt; `failed` ghi nhận kết quả thất bại hoặc không rõ mà không giải phóng reservation; `handled` dành cho notification không phải HTTP. Lifecycle trong notification đã được áp dụng cho risk nội bộ; consumer strategy cần xử lý idempotent theo delivery ID. REST ACK vẫn không phải bằng chứng fill hoặc đóng vị thế.

`DurableAccount::open` luôn khôi phục execution với `ready = false`. Mọi network operation chưa hoàn tất, kể cả trước đó là `Queued` hoặc `Attempted`, chuyển sang `RecoveryRequired` và không được claim để gửi lại. Notification chưa hoàn tất được đưa về `Queued` để consumer nhận lại theo ID ổn định. `recovery_queries()` chỉ trả query tham chiếu order, **không replay placement**; danh sách này không thay thế việc quét regular/algo orders và positions của toàn account. Một record đã ghi đầy đủ có thể tồn tại dù caller không nhận được acknowledgement trước crash: phải query/đối soát, không suy ra request chưa được gửi. Tương tự, hủy future đang chờ worker không rollback công việc đã commit.

Khởi tạo và phục hồi là hai thao tác riêng:

- `Journal::create` / `DurableAccount::create`: chỉ tạo file mới, không ghi đè. Thư mục cha phải có sẵn và do operator quản lý.
- `Journal::open` / `DurableAccount::open`: chỉ mở file có sẵn, không tự tạo trạng thái trắng khi file mất hoặc lỗi. Header được kiểm tra và file giữ khóa OS độc quyền suốt vòng đời handle; handle khác không được đồng thời làm writer.
- `JournalIdentity::new(exchange, account_label, demo)`: ràng buộc venue, nhãn account không bí mật và demo/production trong header cùng checkpoint. Đây là ràng buộc cục bộ; nhãn không tự xác minh credentials thuộc account nào trên sàn. Caller vẫn phải ghép đúng account/credentials/environment.

Journal dùng sequence liên tiếp và chuỗi checksum SHA-256, kiểm tra toàn file khi mở, chỉ giữ payload mới nhất trong bộ nhớ. Tail bị ghi dở, checksum sai hoặc sequence đứt làm recovery thất bại; không tự cắt bỏ tail để tiếp tục. Lỗi write/sync không rõ kết quả làm handle bị poisoned. Sau đó phải mở lại và đối soát; không retry placement dựa trên lỗi lưu trữ.

Các giới hạn hiện tại là **16 MiB payload mỗi record, 256 MiB mỗi file và 8.192 mục outbox**, tính cả receipt/tombstone đã hoàn tất. Không tự compaction, rotation hay xóa lịch sử; chạm giới hạn sẽ fail-closed. Full checkpoint được ghi lại mỗi lần commit, vì vậy đây chưa phải thiết kế đã benchmark cho throughput cao. Việc lưu trữ/archival tiếp theo phải giữ replay tombstone và có quy trình kiểm chứng, không xóa journal để “reset” bot.

`DurableAccount::spawn(capacity)` chuyển các thao tác đồng bộ sang một `spawn_blocking` worker có bounded `mpsc` (capacity từ 1 đến 4.096); caller async dùng `DurableHandle::request(DurableOperation)`. Tạo/mở account cũng là I/O blocking và phải thực hiện trên blocking thread. Worker không chạy HTTP và chưa thay thế runner/supervisor. Khi bỏ toàn bộ handle, worker xử lý hết yêu cầu đã vào hàng đợi rồi đóng journal; đây không phải quy trình shutdown tài khoản với sàn.

Checksum không phải xác thực chống sửa đổi có chủ đích. Cắt file đúng tại ranh giới của một record cũ vẫn có thể tạo prefix hợp lệ; cần committed-sequence anchor độc lập để phát hiện rollback kiểu này. `sequence()` đọc từ chính journal không phải anchor độc lập. Khóa file chỉ bảo vệ các writer hợp tác; thư mục cần ACL/quyền truy cập phù hợp, không cho process khác thay file. Kiểm tra symlink là best effort, không loại bỏ race trong thư mục không đáng tin cậy.

Trên Unix, file mới dùng mode `0600` và thư mục cha được sync sau khi tạo. Trên Windows, code dùng `File::sync_all` cho file và quyền truy cập kế thừa từ thư mục, không có bước sync thư mục tương đương trong implementation này. Không coi các thao tác đó là bảo đảm tuyệt đối trước mọi loại mất điện, lỗi filesystem hoặc storage controller. Các kiểm thử chạy trong Linux container không chứng minh native Windows locking/durability; cần kiểm chứng riêng trên filesystem triển khai. Xem tài liệu chính thức về [`File::try_lock`](https://doc.rust-lang.org/std/fs/struct.File.html#method.try_lock) và [`File::sync_all`](https://doc.rust-lang.org/std/fs/struct.File.html#method.sync_all).

## Ghép các thành phần

Luồng dưới đây là sơ đồ tích hợp **authenticated execution**, chưa phải runner có sẵn. CLI PAPER ở trên dùng broker mô phỏng riêng, không gửi outbox này lên sàn:

```text
DataEngine -> closed candles -> IctEngine -> detection events
    -> StrategyEngine -> TradeIntent -> DurableAccount::apply(Assess)
        -> RiskManager + ExecutionCoordinator + outbox -> journal sync
    -> claim_next -> Attempted + journal sync -> Delivery -> RestExecutor

Private WS / authoritative REST query -> DurableAccount::apply(User)
    -> risk/execution + lifecycle notification -> journal sync
    -> claimed notification -> idempotent StrategyEngine lifecycle

Restart -> DurableAccount::open -> ready=false + recovery-only operations
    -> RestExecutor::inventory -> scoped report.review_account (read-only)
    -> cumulative/history queries + complete account reconciliation (caller)
```

API chính cho host có durability là `DurableAccount::{create, open, apply, claim_next, acknowledge, failed, handled, recovery_queries}` và worker `DurableHandle::request`. Có thể kiểm thử offline bằng input giả theo thứ tự: đăng ký spec, cấp daily baseline/equity, cung cấp snapshot flat đã đối soát, `Assess`, claim delivery rồi đưa ACK/cumulative evidence giả trở lại. Các test của module `durability` minh họa luồng này mà không cần credentials. Không dùng snapshot giả để mở quyền giao dịch thật.

API inventory là `RestExecutor::inventory` hoặc `scan_inventory`, các adapter `BinanceInventory`/`BybitInventory`/`OkxInventory`, `InventoryLimits` và `InventoryReport::{review, review_account}`. `review_account` bao gồm cả operation chưa giải quyết trong durable outbox. So sánh dùng cả lịch sử ID đã kết thúc: nếu một order cũ xuất hiện trở lại, nó được đưa vào danh sách cần query, không bị coi nhầm là lệnh ngoài bot. Không query lại mọi tombstone nếu chúng không xuất hiện. Đây là bước đọc hỗ trợ recovery; caller vẫn phải giữ entry bị chặn, xử lý query/history có thẩm quyền và điều phối các stream. Không nối report rỗng trực tiếp vào `DurableInput::ReconcileFlat`.

`ExecutionCoordinator::handle`/`run` và `ExecutionInput` vẫn là API nền để kiểm thử riêng coordinator; gọi trực tiếp chúng rồi gửi HTTP sẽ bỏ qua ranh giới journal của `DurableAccount`. `RestExecutor::execute` và `run_private_session` vẫn do caller/supervisor nối vào. Một REST query chỉ trả `OrderAck` không cung cấp đủ bằng chứng fill: parser reconciliation đầy đủ của adapter hoặc supervisor phải đưa cumulative state về dưới dạng `UserEvent::Order`.

`EntryExposureReopened` chỉ được phát khi có phần cumulative entry fill **mới** sau lần đóng trước đó; RiskManager giữ lại toàn bộ reservation và chặn việc replay sự kiện đóng cũ để giải phóng nhầm. Supervisor phải xử lý trạng thái strategy tương ứng hoặc chuyển sang recovery, không coi đây là một entry mới. Sau cancel thất bại, `retry_cancellation` cần một query open-order có thẩm quyền và thời điểm nhận snapshot còn mới; số lần thử/backoff thuộc supervisor. Query thất bại không tự lặp vô hạn.

Mỗi `DurableAccount` và bộ credentials phải thuộc đúng một account; account label không thay thế định danh được sàn xác nhận. `RiskPlan` riêng lẻ chưa mang account ID, và các API nền vẫn tồn tại; caller không được phát cùng approval sang nhiều account hoặc đi vòng qua durable host. Không coi mock `FlatAccountSnapshot` trong test là cơ chế đối soát tài khoản thật.

## Build và kiểm thử

Toolchain tối thiểu được khai báo là Rust 1.89. `Cargo.lock` được giữ để tái lập dependency. Default features bật cả `binance`, `bybit`, `okx`.

```sh
cargo fmt --all -- --check
cargo test --all-features --locked
cargo test --no-default-features --locked
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo clippy --all-targets --no-default-features --locked -- -D warnings
cargo doc --no-deps --all-features --locked
cargo build --release --all-features --locked
```

Có thể chọn một adapter, ví dụ:

```sh
cargo test --no-default-features --features bybit --locked
```

Chạy riêng kiểm thử host offline, journal, checkpoint, inventory và PAPER:

```sh
cargo test --all-features --locked durability
cargo test --all-features --locked journal
cargo test --all-features --locked checkpoint
cargo test --all-features --locked reconciliation
cargo test --all-features --locked inventory_tests
cargo test --all-features --locked paper
```

Test bao phủ invariant của model, ICT, strategy/risk lifecycle, exact-decimal arithmetic, chữ ký, protocol parsing, cumulative/partial fill, cancellation race và transport giả lập. Bộ test durability bổ sung restart, claim/receipt, bảo toàn reservation/tombstone, recovery không replay lệnh, corrupt checkpoint, khóa file, torn tail và giới hạn dung lượng. Bộ test inventory dùng response/mock transport để kiểm tra ký GET, scope/filter, mode chưa rõ, pagination, hủy/deadline, giới hạn và so sánh order không làm thay đổi risk/entry. Mock test và release build không xác nhận latency thực tế, hành vi khớp lệnh hoặc độ tin cậy trên hạ tầng của sàn.

Mốc kiểm chứng **trước khi bổ sung PAPER**, sau inventory scanner và đối chiếu durable outbox, ngày 28/10/2026 trên Rust/Cargo 1.89.0, Linux container `rust:1.89-bookworm` qua Docker Desktop Windows. Các số dưới đây là mốc lịch sử; kết quả bản PAPER hiện tại nằm ở mục “Kiểm chứng PAPER ngày 28/10/2026” phía trên:

- `cargo test --all-features --locked`: **286 passed**, không có test thất bại hoặc bị bỏ qua.
- `cargo test --no-default-features --locked`: **180 passed**, không có test thất bại hoặc bị bỏ qua.
- Clippy `--all-targets ... -- -D warnings`: đạt ở cả `--all-features` và `--no-default-features`.
- `cargo clippy --all-targets --no-default-features --features <sàn> --locked -- -D warnings`: đạt riêng từng feature `binance`, `bybit`, `okx`.
- `cargo fmt --all -- --check`, Rustdoc với `RUSTDOCFLAGS="-D warnings"` và `cargo build --release --all-features --locked`: đạt.

So với mốc durability trước (244/170), cấu hình đầy đủ có thêm 42 test và cấu hình không bật adapter có thêm 10 test. Các test mới gồm 25 test adapter, 10 test collector/so sánh/durable integration và 7 test HTTP giả lập sử dụng adapter thật. Đây là kiểm chứng build/test Linux, không phải native Windows build, benchmark latency, native Windows durability hoặc demo/live E2E; không gọi API tài khoản riêng của sàn và không đặt lệnh thật.

## Phần cần hoàn thiện trước vận hành thực tế

- Archival/compaction có kiểm chứng, anchor độc lập chống rollback prefix, backup/recovery và đo chi phí full-checkpoint sync; kiểm thử crash/power-loss trên filesystem triển khai. Local journal/outbox và checkpoint restore đã có, chưa thay thế các quy trình này.
- Account reconciler đủ để quyết định admission: kết hợp scoped inventory đã có với query/history/fill cho order thiếu hoặc chưa rõ kết quả; xác minh remote account, mode, instrument metadata và phạm vi sản phẩm/bot còn ngoài scanner. Phải xử lý continuity và thay đổi đồng thời giữa các page; inventory scan không phải snapshot nguyên tử.
- Private-stream supervisor: reconnect với auth mới, Binance listen-key renewal, coordinated private/business stream của OKX, khôi phục continuity và shutdown có xác nhận terminal state.
- Authenticated runner kết nối cả năm lớp, routing lifecycle, scheduler expiry, circuit-breaker orchestration, configuration và observability. PAPER CLI hiện có không thay thế runner gửi lệnh thật.
- Mô hình kinh tế cho execution thật: fee tier, slippage, funding, margin/leverage, liquidation; kiểm tra min-notional và giới hạn venue từ metadata cập nhật. PAPER mới có giả định phí/stop slippage và public metadata Bybit; planned risk của execution thật chưa bao gồm toàn bộ các chi phí này.
- Chạy demo end-to-end có kiểm soát; benchmark latency, tải/backpressure, reconnect/fault injection và quy trình xử lý protective-order failure. Chưa có số đo để tuyên bố đạt mục tiêu low-latency.

Không đưa API key, secret, passphrase hoặc listen key vào repository, fixture, log hay issue. `ApiCredentials`, signed request và session config che thông tin nhạy cảm trong `Debug`; các getter/body/frame vẫn chứa bí mật và chỉ dùng tại biên transport. Cấp credential lúc runtime bằng môi trường triển khai hoặc secret store phù hợp.

## Tài liệu giao thức chính thức

- Binance: [Trade API, gồm algo orders](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/trade), [User Data Streams](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/user-data-streams).
- Binance inventory: [USD-M account API và current position mode](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/account), [open orders, open algo orders và position information](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/trade).
- Bybit: [Authentication](https://bybit-exchange.github.io/docs/v5/guide), [Create order](https://bybit-exchange.github.io/docs/v5/order/create-order), [Private WebSocket connection](https://bybit-exchange.github.io/docs/v5/ws/connect), [Order updates](https://bybit-exchange.github.io/docs/v5/websocket/private/order), [Demo Trading](https://bybit-exchange.github.io/docs/v5/demo).
- Bybit inventory: [Open/closed order query và cursor](https://bybit-exchange.github.io/docs/v5/order/open-order), [Position info và positionIdx](https://bybit-exchange.github.io/docs/v5/position).
- Bybit public PAPER: [Kline, thứ tự nến và giới hạn request](https://bybit-exchange.github.io/docs/v5/market/kline), [Instrument filters cho linear contracts](https://bybit-exchange.github.io/docs/v5/market/instrument).
- OKX: [V5 API documentation](https://www.okx.com/docs-v5/en/).
- OKX inventory: [Account config, positions, pending orders và pending algo families](https://www.okx.com/docs-v5/en/).
- Rust standard library: [`File::try_lock`](https://doc.rust-lang.org/std/fs/struct.File.html#method.try_lock), [`File::sync_all`](https://doc.rust-lang.org/std/fs/struct.File.html#method.sync_all), [`File::create_new`](https://doc.rust-lang.org/std/fs/struct.File.html#method.create_new).

# ICT Trading Bot — Rust

Thư viện Rust Edition 2021 cho chiến lược ICT trên crypto futures, có adapter Binance USD-M, Bybit V5 và OKX V5. Giá, khối lượng, sizing và các phép tính rủi ro dùng `rust_decimal::Decimal`; dữ liệu số từ sàn được đọc trực tiếp từ chuỗi.

Repository hiện có CLI **PAPER không cần API key**, các thành phần của cả năm lớp, journal/checkpoint/outbox để khôi phục trạng thái cục bộ và inventory scanner chỉ đọc cho ba sàn, nhưng **chưa phải bot chạy tiền thật hoàn chỉnh hoặc đã được xác nhận production-ready**. CLI PAPER chỉ mô phỏng; chưa có runner nối toàn bộ pipeline execution có xác thực, bộ đối soát toàn tài khoản đủ để mở quyền entry hay kết quả kiểm thử giao dịch demo/live. Khôi phục checkpoint hoặc hoàn tất một inventory scan không đồng nghĩa đã xác nhận tài khoản flat. Các kiểm thử execution dùng dữ liệu giả và máy chủ HTTP/WebSocket cục bộ; không đặt lệnh thật.

## PAPER portfolio v4: cho phép entry 24/7, vốn chung 100 USDT, x10

Chế độ `portfolio` dùng dữ liệu public Bybit M1, **một tài khoản giả lập 100 USDT chung**, không phải 100 USDT cho mỗi cặp. Không cần tài khoản sàn và không gửi lệnh lên sàn. Phiên mới mặc định `--entry-window always`: được xét tạo lệnh **24 giờ mỗi ngày, cả cuối tuần**, khi đủ setup và risk; không ép phải có lệnh hoặc luôn giữ vị thế. Dùng `--entry-window killzones` để giữ bộ lọc giờ thứ Hai–thứ Sáu theo New York. Chế độ đơn cặp `live` và replay log cũ giữ hành vi trước đây.

```powershell
cargo run --locked --release --no-default-features --bin paper -- portfolio --symbols "BTCUSDT,ETHUSDT" --leverage 10 --balance 100 --entry-window always --seconds 300
# Hoặc wrapper; Docker fallback không chuyển tiếp biến Telegram từ host:
.\scripts\paper.ps1 portfolio --symbols "BTCUSDT,ETHUSDT" --leverage 10 --balance 100 --entry-window always --seconds 300
```

- Chuỗi ICT giữ nguyên: liquidity sweep → displacement/MSS + FVG → limit retracement. SL ngoài extreme sweep, TP tại opposing liquidity đã xác nhận; không tạo TP/tín hiệu giả để ép giao dịch. Khi chọn `killzones`, giờ New York có DST và lịch thứ Hai–thứ Sáu như chế độ cũ; khi chọn `always`, chỉ bỏ điều kiện giờ/ngày, không nới các điều kiện setup khác.
- BTC/ETH có detector riêng và vòng đời strategy riêng cho mỗi intent, nhưng **chung một sổ vốn, risk manager và daily-risk baseline**. Tối đa **4 lệnh active, tối đa 2 mỗi symbol**; pending limit cũng chiếm chỗ như vị thế đã khớp. Mỗi setup có Trade ID/Entry/TP/SL riêng; setup mới không ghi đè hay tự thêm khối lượng vào lệnh cũ. Không tự DCA/martingale và không cho LONG + SHORT cùng symbol. Khi hai cặp cùng có setup, BTC được xét trước theo thứ tự cố định.
- Rủi ro kế hoạch tối đa **1% equity/lệnh, tổng reservation tối đa 3% equity tại lúc nhận lệnh mới**, có phí và stop slippage; net RR ≥ 2, circuit breaker drawdown ngày UTC 5%. Không chia đều mặc định thành 0,75% để ép đủ bốn lệnh: số lượng thực tế còn phụ thuộc risk còn lại, quantity step/minimum, net RR và cash. Trần risk là điều kiện admission, không phải cam kết lỗ tối đa: gap có thể làm lỗ vượt kế hoạch, equity giảm cũng có thể khiến tỷ lệ reservation đang giữ vượt 3%; bot chặn risk mới chứ không giả lập đóng cưỡng bức. Nến được đồng bộ theo thời điểm đóng của cả hai cặp trước khi cập nhật equity/risk.
- Khi nhận limit, sổ vốn giữ trước initial margin và phí entry/exit từ **chính số tiền chung còn khả dụng**, không tạo broker con với 100 USDT độc lập. Khi hủy/đóng, phần vốn còn lại và lãi/lỗ được trả về cùng sổ vốn. Lãi chưa thực hiện không tạo thêm cash để mở lệnh.
- x10 chỉ thay đổi khả năng ký quỹ: initial margin mô phỏng = entry notional / 10; còn dự trữ phí entry/exit. Quantity vẫn bị chặn bởi mức risk 1%, step/minimum của từng instrument và cash. PnL = chênh lệch giá × quantity, **không nhân thêm 10**; phí tính trên notional, không phải margin.
- Đây **không phải mô phỏng đầy đủ cơ chế margin của Bybit**: chưa có funding, mark-price maintenance margin hay liquidation. Không dùng kết quả để suy ra khoảng cách thanh lý thực. [Bybit initial margin](https://www.bybit.com/en/help-center/article/Initial-Margin-USDT-Contract)
- Warmup 500 nến mỗi cặp, poll 5 giây, **chờ thêm ít nhất 10 giây sau ranh giới đóng nến** trước khi đưa REST candle vào engine. Smoke test phát hiện close/volume vừa đóng phút còn thay đổi ở lần đọc kế tiếp; khoảng chờ giảm race này, không bảo đảm finality hay miễn trừ sai lệch đồng hồ. Thiếu/gap/sửa nến đã xử lý vẫn dừng để kiểm tra; lỗi HTTP tạm thời được thử lại có giới hạn như mục bên dưới. Log **v4** lưu rõ `entry_window` cùng giới hạn 4/2/1%/3%/5%, leverage, metadata, analysis, từng lifecycle và sổ vốn chung. `paper replay` tự nhận **v1/v2/v3/v4**, offline, không gửi Telegram; v1/v2 dùng engine cũ, v3 luôn khôi phục chế độ killzones đã dùng trước đây. Replay không phải resume.

Bỏ `--seconds` để chạy liên tục. **Mỗi restart/redeploy vẫn tạo tài khoản mới**; volume chỉ giữ log, chưa resume cash/vị thế/risk. Shutdown hủy pending nhưng không ghi một giao dịch đóng vị thế giả. Không gộp các phiên mới thành một lịch sử lợi nhuận liên tục.

Journal portfolio giới hạn 100.000 batch (gồm warmup), 256 MiB và 64 KiB mỗi record. Runner kiểm tra trước khi xử lý nến, giữ chỗ cho một batch tối đa và terminal record; hết giới hạn thì dừng trước khi engine tiến thêm, hủy pending và lưu summary nếu đĩa còn ghi được. Giới hạn batch tương đương khoảng 69 ngày sau warmup, chưa phải khả năng chạy vô hạn. Chưa có rotation hoặc tự xóa log cũ; dung lượng của nhiều phiên vẫn cộng dồn trên volume.

Nếu engine lỗi hoặc đã xử lý nến nhưng không ghi được kết quả, runner không được ghi `summary` như một phiên có thể replay đầy đủ: chỉ ghi `aborted` khi journal còn ghi an toàn được. Snapshot đó chỉ là bằng chứng chẩn đoán; replay từ chối. Lỗi I/O, ghi dở hoặc crash có thể khiến không ghi được cả terminal record; log thiếu summary cũng bị từ chối. Preflight dung lượng không bảo đảm filesystem còn chỗ hay không gặp lỗi. Signal dừng được đăng ký trước startup; dừng trong warmup không tạo giao dịch.

### Khôi phục lỗi public-data trong cùng phiên

Portfolio chỉ gọi REST khi nến kế tiếp có thể đủ điều kiện xử lý (đóng M1 + 10 giây), thay vì tải lại cùng dữ liệu mỗi 5 giây. `--poll-seconds` vẫn là nhịp kiểm tra; khi sàn công bố chậm, bot tiếp tục thử ở nhịp này và vẫn chịu deadline stale 120 giây. Khi feed bình thường và đã warmup, dự kiến một vòng BTC+ETH mỗi phút (2 GET), thay vì khoảng 24 GET/phút. Đây là giảm request trùng, không thay đổi timeframe hoặc đảm bảo hết rate limit.

Lịch chờ chủ động thức dậy tại mốc nến đủ điều kiện, kể cả khi nhịp polling dài sẽ bỏ qua mốc đó; heartbeat không đẩy lùi lịch đã đến hạn. Ví dụ `poll=60` và warmup cursor đang cũ 65 giây: lần lấy dữ liệu đến ở giây 70, thay vì chờ đến giây 125 rồi bị chặn stale. Nếu sàn vẫn thiếu dữ liệu, nhịp thử lại đã cấu hình và deadline 120 giây vẫn áp dụng; nên giữ mặc định 5 giây. Portfolio nhận `--warmup 50..=999`, để luôn còn một chỗ trong giới hạn REST 1.000 nến cho kiểm tra settlement. Giá trị 1.000 bị từ chối ngay khi đọc cấu hình thay vì khởi động thất thường trong 10 giây đầu phút; mặc định 500 không đổi.

Hai runner `live` và `portfolio` thử lại **chỉ các GET dữ liệu công khai**, tối đa 4 lượt thử cho mỗi thao tác trong tổng 60 giây; mỗi lượt portfolio gồm hai GET đồng thời cho BTC+ETH. Chờ nền 1/2/4 giây kèm jitter dương. HTTP 408/429/500/502/503/504, lỗi transport, Bybit `10000`/`10006`/`10016`, JSON không đọc được do syntax/EOF thuộc nhóm có thể thử lại. HTTP 403, lỗi tham số/quyền, cấu trúc success sai, sai instrument/Decimal, clock skew, nến trùng/gap/revision không được bỏ qua hoặc retry vô hạn. Đây là chính sách bảo thủ của bot, không phải bảo đảm rằng mọi lỗi trong nhóm đều tự hết. [Mã lỗi Bybit](https://bybit-exchange.github.io/docs/v5/error)

- Parser kiểm tra `retCode` **trước** schema success: lỗi venue với `result: {}`, `null` hoặc thiếu `result` không còn bị che thành lỗi JSON chung. Log lỗi decode chỉ chứa status, phase/category, dòng/cột và số byte; không ghi body, `retMsg`, token hay URL chứa bí mật.
- Tôn trọng `Retry-After` hợp lệ trên HTTP 429/503. Nếu thời gian chờ không nằm trong ngân sách phục hồi, bot dừng thay vì gửi sớm hơn yêu cầu của server. Header sai/không rõ cũng dừng; không đổi region, proxy hay endpoint để né giới hạn. [Retry-After](https://www.rfc-editor.org/rfc/rfc9110.html#name-retry-after)
- Mỗi lần thử lấy timestamp request mới. Portfolio thử lại **cả vòng BTC+ETH**, không ghép kết quả BTC cũ với ETH của lần gọi sau. Trong lúc chờ, không gọi engine, không tạo entry/fill và không đặt lại cash, position hay daily-risk baseline.
- Ngưỡng feed 120 giây tính từ nến cuối **đã xử lý**, có hiệu lực cả khi đang chờ request/backoff/heartbeat. Giữ deadline monotonic đến khi cursor tiến lên, đồng thời kiểm tra UTC; poll/heartbeat hoặc chỉnh lùi đồng hồ không gia hạn deadline của cùng một nến. Trước khi tiếp tục, toàn bộ cửa sổ hồi phục phải đủ, mới và liên tục. TP/SL paper không chạy theo tick trong thời gian mất dữ liệu; khi hồi phục hợp lệ, engine chỉ mô phỏng lại theo các nến nhận được.
- Nếu Telegram đang bật, portfolio gửi một tin `TẠM CHỜ DỮ LIỆU` khi bắt đầu mỗi đợt lỗi, giải thích nguyên nhân, lần thử tiếp theo và thời gian chờ. Tin `DỮ LIỆU ĐÃ PHỤC HỒI` chỉ được gửi sau khi batch đồng bộ mới (hoặc toàn bộ warmup lúc startup) đã được kiểm tra và ghi journal; HTTP thành công đơn thuần chưa đủ. Không gửi lại cảnh báo ở mọi lần retry. Console giữ chẩn đoán kỹ thuật đã làm sạch; không xuất URL/error thô hoặc bí mật.
- SIGTERM/Ctrl+C hủy được request/backoff cả trong startup. Thời hạn `--seconds` tính từ sau warmup và hủy được request/backoff của vòng chạy live. Hết số lần thử/ngân sách hoặc quá stale: bot ghi summary nếu còn ghi đĩa được, hủy pending và giữ vị thế mở dưới dạng marked như trước, sau đó báo lỗi. **Không tự restart hoặc khôi phục tài khoản sau process restart.**

### Telegram: theo dõi pending, Entry, TP, SL và PnL

Telegram chỉ là kênh thông báo, không điều khiển giao dịch. Tính năng mặc định **tắt**, chỉ bật trong `paper portfolio`. Tin dùng tiếng Việt, nhãn `[PAPER]` và giờ Việt Nam. Mã lệnh/mã phiên hiển thị 12 ký tự cuối để dễ đối chiếu; UUID đầy đủ vẫn có trong journal và log kỹ thuật.

Mặc định mỗi 30 phút có **một tin cập nhật tổng hợp** (`--heartbeat-seconds`, tối thiểu 60), gồm:

- Số dư, tổng tài sản, lãi/lỗ đã ghi nhận sau phí, lãi/lỗ đang mở, phí và số lệnh đã đóng.
- Từng lệnh **CHỜ KHỚP/ĐANG MỞ** của BTC/ETH: LONG/SHORT, Entry, TP, SL, khối lượng, rủi ro/vốn giữ chỗ; cùng số chỗ còn lại, vốn khả dụng và đòn bẩy x10. Hai lệnh cùng cặp vẫn có giá và mã riêng.
- Trạng thái dữ liệu, lịch vào lệnh, điều kiện đang chặn lệnh mới và số lần thử lại/phục hồi. `always` xét setup 24/7; `killzones` dùng giờ New York có DST, thứ Hai–thứ Sáu. Vị thế đang có vẫn được mô phỏng ngoài giờ tạo lệnh.
- BTC/ETH đang chờ sweep, MSS, FVG hay hồi giá; số setup đủ chuỗi, limit được nhận, lượt từ chối và nguyên nhân phổ biến nhất. Số liệu cộng dồn từ đầu phiên, bỏ warmup/nến trùng và bắt đầu lại sau restart. Tín hiệu sweep/MSS/FVG riêng lẻ không đồng nghĩa có setup đủ điều kiện; limit được nhận chưa có nghĩa đã khớp. Thống kê đầy đủ vẫn ở console, không gửi thêm hai tin riêng mỗi kỳ.

Chẩn đoán nhận lệnh phân biệt **RR trước phí**, **RR sau chi phí**, khối lượng tối thiểu, thiếu vốn, giới hạn risk/slot và thiếu mục tiêu TP. Khi risk manager trả số RR trước phí hoặc khối lượng cụ thể, tin hiển thị đúng số đó và ngưỡng so sánh; lỗi RR sau chi phí hiện chỉ có loại lỗi, **không tự suy đoán một số net RR**. Lần từ chối gần nhất có giờ nến theo Việt Nam: đây là lịch sử, không khẳng định điều kiện đó vẫn đang chặn setup hiện tại. Console mỗi nến chỉ thêm chi tiết từ chối phát sinh đúng nến đó. Bộ đếm admission chỉ tính intent bị từ chối cuối cùng; thử TP nghiên cứu thất bại rồi chọn được TP khác không bị ghi thành một lệnh bị loại. Các quan sát này không thay quyết định giao dịch hoặc schema journal V1–V4.

Thông báo vòng đời phân biệt rõ **ĐẶT LIMIT — CHƯA KHỚP**, **ĐÃ KHỚP LỆNH MÔ PHỎNG**, **ĐÃ HỦY LIMIT CHƯA KHỚP**, **ĐÃ CHỐT TP** và **ĐÃ CẮT SL**. Tin đóng lệnh có giá thoát và lãi/lỗ ròng của chính lệnh đó; tin khởi động/dừng hoặc tạm ngừng lệnh mới không được coi là đã đóng vị thế. Toàn bộ cập nhật dựa trên nến đóng, không phải giá tick hay xác nhận khớp lệnh từ sàn.

Thiết lập lần đầu bằng **bot mới dành riêng cho paper**:

1. Trên Telegram mở đúng [@BotFather](https://t.me/BotFather), gửi `/newbot`, đặt tên và username kết thúc bằng `bot`. Lưu token như mật khẩu; **không gửi vào chat hỗ trợ, commit Git, ảnh chụp hay URL trình duyệt**. [Hướng dẫn chính thức](https://core.telegram.org/bots/tutorial#obtain-your-bot-token)
2. Tại repository trên máy Windows chạy `.\scripts\telegram-setup.ps1`. Nhập token vào prompt ẩn; helper hiển thị username bot và một mã xác minh. Mở **chat riêng** với bot, bấm Start rồi gửi đúng mã đó, sau đó quay lại terminal để kiểm tra. Helper chỉ đọc `getMe`/`getUpdates`, không gửi tin, không lưu token, không xóa webhook hay acknowledge update; chỉ in chat ID sau khi xác nhận tin mới do chính chủ chat gửi. Không dùng helper để chọn một group bất kỳ trong lịch sử.
3. Thêm ba **Variables runtime** của service Railway:

   ```text
   TELEGRAM_ENABLED=true
   TELEGRAM_BOT_TOKEN=<token riêng từ BotFather>
   TELEGRAM_CHAT_ID=<số chat ID vừa xác minh>
   ```

   Các dòng trên là placeholder, không phải giá trị dùng được. Không thêm exchange API key. Nếu chưa tạo bot, bỏ các biến này hoặc để `TELEGRAM_ENABLED=false`.
4. Sau khi bản code mới được deploy, kiểm tra tin `[PAPER] ĐÃ KHỞI ĐỘNG`, rồi tin cập nhật. `ĐẶT LIMIT — CHƯA KHỚP` không có nghĩa đã có vị thế. Không cần mở port/domain/webhook cho notifier.

Notifier có hàng đợi riêng giới hạn 64 tin, timeout và phản hồi giới hạn kích thước; lỗi gửi không làm chậm vòng xử lý nến. Chỉ retry một lần với 429 có `retry_after` hợp lệ; lỗi mạng không được tự gửi lại để tránh nhân đôi kết quả chưa rõ. Không log token/URL chứa token. Đây là **best-effort, không phải durable outbox/exactly-once**: mạng lỗi, queue đầy, crash hoặc hết thời gian shutdown có thể mất tin; JSONL mới là bằng chứng phiên. Xem lỗi và delivery counters trong console. [Telegram Bot API](https://core.telegram.org/bots/api#sendmessage)

### Nâng bản Railway đang chạy

Docker CMD mới mặc định chạy portfolio BTC/ETH x10, nhưng **Custom Start Command cũ sẽ override mặc định này**. Sau khi chủ động chọn thời điểm kết thúc phiên cũ, dùng lệnh:

```text
/usr/local/bin/paper portfolio --symbols BTCUSDT,ETHUSDT --leverage 10 --balance 100 --entry-window always
```

Giữ volume `/data`, một instance và chính sách restart được mô tả bên dưới. Đặt `RAILWAY_DEPLOYMENT_DRAINING_SECONDS=15` để có thời gian xử lý SIGTERM, ghi summary và drain Telegram (tối đa khoảng 5 giây); overlap nên để 0 và chủ động dừng phiên cũ trước khi đổi bản. Railway mặc định có thể force-stop ngay nếu không cấu hình draining; tin dừng cuối không được bảo đảm khi SIGKILL/crash. [Deployment teardown](https://docs.railway.com/deployments/deployment-teardown)

Sao lưu log phiên cũ; rollout tạo phiên 100 USDT mới, không tự chuyển số dư/vị thế đang có. Code mới và Variables chỉ có hiệu lực trên deployment tương ứng; việc viết code cục bộ không thay đổi service đang chạy.

### Kiểm chứng nghiên cứu SMT (29/10/2026)

Trên Rust 1.89 native Windows/MSVC:

- **573 test all-features**: 467 thư viện + 79 CLI paper + 13 backtest + 14 collector/recovery.
- **467 test no-default-features**: 361 thư viện + 79 CLI paper + 13 backtest + 14 collector/recovery.
- Clippy `--all-targets ... -- -D warnings` đạt ở cả hai cấu hình; release cả ba binary đạt. Test bao phủ SMT hai chiều, dữ liệu đồng bộ, swing đã bị tiêu thụ, ghép đúng mô hình fractal, cấm dùng M5 chưa đóng, gate admission theo đúng bằng chứng, warmup và thứ tự xử lý khớp/hủy.
- Smoke public BTC/ETH 75 giây có 500 batch warmup + hai batch live, Telegram tắt, dừng đúng `duration_elapsed`, tài sản 100 USDT và không có vị thế. Replay V1/V2/V3/V4 và phiên smoke giữ SHA-256 tương ứng. Hồ sơ: `paper-runs/verification-20261029-smt-report.json`.
- Kiểm tra độc lập trên hai báo cáo SMT đạt **48.299 phép kiểm tra**, gồm **1.482 bằng chứng admission** qua các cấu hình/giai đoạn; đây không phải 1.482 giao dịch độc lập vì các kịch bản dùng chung dữ liệu. RR ròng thấp nhất tính lại từ order được nhận là **2,00063416**. Hai báo cáo chạy lặp lại giống nhau từng byte; 18 cấu hình cũ giữ nguyên metrics, snapshot, trade events và daily marks. Kiểm tra hướng và thời điểm M5 ghi nhận không thay thế một bản dựng lại toàn bộ state machine M5 độc lập. Chi tiết: `paper-runs/smt-research-verification.txt`. Có thể chạy lại bằng `python scripts/verify_smt_research.py` khi có đủ hai dataset, báo cáo SMT và các báo cáo Entry V2 nền tại những đường dẫn bên dưới; script chỉ đọc và dùng `Decimal`, không truy cập mạng.

Kiểm thử bao phủ giới hạn vốn/rủi ro/nhiều lệnh, entry 24/7 và killzone, retry/stale/deadline, thống kê setup, Telegram bốn lệnh dưới giới hạn 4.096 ký tự, dataset liên tục, phân tách thời gian và phí/PnL. Đây là kiểm chứng phần mềm cục bộ; không chứng minh lợi nhuận, độ trễ khớp lệnh hay bản triển khai Railway đã được cập nhật. Không gửi Telegram thử trong bộ kiểm tra.

CLI portfolio mặc định `always`; `MultiPortfolio::new` của thư viện và log V3 giữ killzones để tương thích. Replay V1–V4 dùng đúng cấu hình được ghi trong log, không đổi policy bằng tham số CLI. Bằng chứng kiểm chứng cũ lưu trong `paper-runs/` (Git bỏ qua); không cộng kết quả các phiên khởi động mới thành một tài khoản liên tục.

### Nghiên cứu chiến lược offline

`paper_history` thu thập nến public Bybit BTCUSDT/ETHUSDT M1 và metadata hiện tại. `paper_backtest` đọc dataset đã lưu, không gọi sàn/Telegram và không thay đổi bot đang chạy. Cả hai từ chối ghi đè đầu ra; chọn thư mục/file mới cho mỗi lần thực hiện.

#### Nghiên cứu SMT BTC–ETH bắt buộc

Yêu cầu nghiên cứu mới là **SMT BTC–ETH bắt buộc**, **RR sau phí và giả định trượt giá SL tối thiểu 1:2**, với mục tiêu trung bình **hơn 10 entry đã khớp mỗi ngày cho tổng danh mục BTC+ETH**. Số đề xuất hoặc limit mới đặt không được tính là lệnh khớp. Báo cáo đo số khớp trên thời gian quan sát thực tế, số khớp từng ngày kể cả ngày bằng không, số ngày trên 10 lệnh, số lệnh đã đóng và thời gian giữ vị thế. Đạt trung bình trên 10 không có nghĩa mọi ngày đều trên 10; mục tiêu tần suất không cho phép bỏ điều kiện SMT, RR hoặc hạn mức tài khoản.

Giữ tổng vốn paper **100 USDT dùng chung**, đòn bẩy mô phỏng **x10**, risk tối đa **1% mỗi lệnh / 3% tổng**, tối đa **bốn lệnh đang chờ hoặc đang mở / hai lệnh mỗi cặp**, circuit breaker drawdown ngày **5%**. TP vẫn lấy từ thanh khoản đối diện đã xác nhận và chưa bị quét, xét mục tiêu tiếp theo chỉ khi cần đáp ứng RR. Không tự tạo một mức TP tùy ý để ép lệnh vượt kiểm tra.

Evaluator hiện khai báo **34 cấu hình cố định**: giữ 18 cấu hình trước, thêm 16 cấu hình `smt_*` theo tích bốn lựa chọn:

| Thành phần | Hai lựa chọn được so sánh |
| --- | --- |
| Swing M1 cho cả BTC và ETH | Fractal 3 nến / 5 nến |
| Hướng M5 tại lúc nhận lệnh | Không yêu cầu / bắt buộc cùng hướng |
| Phí mỗi chiều và slippage SL | 6/2 bps / 10/5 bps |
| Khớp entry | Có thể từ nến kế tiếp / bỏ qua nến hợp lệ đầu tiên |

Mỗi cấu hình SMT vẫn yêu cầu **sweep hợp lệ → MSS và displacement M1 thật → FVG ở nến kế tiếp → limit tại 50% FVG**. Biến thể cùng hướng M5 là giả thuyết bắt nhịp hồi M1 trong hướng cấu trúc M5; không biến tín hiệu BOS M1 thành MSS và không vào trực tiếp chỉ vì có BOS. Các cấu hình chạy 24/7, dùng TP `nearest_viable`; các hạn mức rủi ro không được tối ưu trong phép thử này.

Quy tắc SMT được cố định trước khi đánh giá:

- Cả hai nến M1 phải đóng, liên tục và cùng khoảng UTC. Phân tích cả BTC và ETH xong mới xét phân kỳ hoặc cho phép nhận lệnh; không dùng nến hiện tại của một cặp với nến cũ của cặp kia.
- Cặp chủ thể quét đỉnh/đáy bằng wick rồi đóng trở lại phía ban đầu. Cặp còn lại không phá mức đỉnh/đáy tương ứng. Long dùng quét đáy; short dùng quét đỉnh. Không suy ra việc giữ mức của cặp còn lại chỉ từ việc thiếu một sự kiện sweep.
- Hai swing tham chiếu phải được xác nhận chậm nhất tại **giờ mở nến sweep**, cùng loại đỉnh/đáy và cùng mô hình fractal; chênh giờ tạo swing tối đa **ba phút**, tuổi swing tối đa **60 phút**. Swing của cặp đối chiếu được chọn theo khoảng cách thời gian gần nhất trước khi kiểm tra mức giá. Nếu swing được chọn đã bị phá, khác mô hình hoặc không còn hợp lệ, không thử một swing xa hơn cho tới khi tìm được phân kỳ thuận ý. Nhiều swing chủ thể cùng giá nhưng không phân biệt được danh tính cũng bị loại.
- Bất kỳ wick phá mức nào cũng tiêu thụ thanh khoản tham chiếu, kể cả không đóng trở lại. Cả hai cặp cùng phá các mức tương ứng không tạo SMT. Nếu xuất hiện đồng thời bằng chứng SMT mới ngược hướng trong cùng batch, không xác nhận các bằng chứng mới đó.
- Bằng chứng SMT giữ nguyên hai swing và hai nến nguồn, hết hiệu lực sau **12 phút từ lúc xác nhận**, hoặc sớm hơn khi cặp đối chiếu phá mức đã giữ hay chủ thể phá cực trị sweep gốc. Tại nhận lệnh, bot phải tìm đúng bằng chứng còn hiệu lực cho **ID sweep của intent**, đúng cặp và hướng; chỉ trùng hướng với một SMT khác không đủ.

M5 được ghép từ đúng năm nến M1 đã đóng liên tục theo biên UTC; bỏ đoạn năm phút đầu nếu dữ liệu bắt đầu giữa đoạn. Detector M5 dùng fractal 5 nến và BOS/MSS xác nhận theo giá đóng. Hướng trước đó giữ nguyên trong bốn nến M1 đầu, chỉ được cập nhật ở lần đóng nến M1 thứ năm. Điều kiện cùng hướng M5 được kiểm tra **tại lúc nhận lệnh**; M5 đổi hướng về sau không tự hủy limit. Việc chỉ dùng dữ liệu khung lớn đã xác nhận nhằm tránh thay đổi kết quả vì nến khung lớn còn đang hình thành, một nguyên nhân repainting được mô tả trong [tài liệu TradingView](https://www.tradingview.com/pine-script-docs/concepts/repainting/).

Broker xử lý OHLC của nến hiện tại trước khi xét SMT mất hiệu lực tại giờ đóng nến. Sau đó chỉ hủy những entry **vẫn chưa khớp** nếu bằng chứng SMT bị mất; không dùng thông tin ở cuối nến để xóa ngược một giao dịch có thể đã khớp trong chính nến đó. Bằng chứng xuất ra cho mỗi admission gồm intent MSS/FVG gốc, swing và nến SMT đã đóng băng, nến cặp đối chiếu tại admission, hướng/thời điểm M5 đã biết và order thực sự được book nhận. Warmup chỉ khởi tạo ngữ cảnh, không đặt lệnh lịch sử hay tăng số SMT live.

Đã chạy đủ 34 cấu hình trên hai dataset, mỗi dataset chia thành 23 ngày phát triển và bảy ngày cuối kỳ với **100 USDT độc lập cho từng phần/cấu hình**. Mỗi ô dưới đây là **số entry đã khớp trung bình/ngày / thay đổi tài sản sau phí (USDT)**, làm tròn bốn chữ số. Không cộng PnL giữa các cột thành một tài khoản liên tục. Mọi phân đoạn SMT đều kết thúc không còn vị thế hoặc lệnh chờ; ở các phân đoạn này số khớp bằng số lệnh đã đóng.

| Cấu hình SMT | 30/08–22/09 phát triển | 22/09–29/09 cuối kỳ | 29/09–22/10 phát triển | 22/10–29/10 cuối kỳ |
| --- | ---: | ---: | ---: | ---: |
| `smt_f3_base` | 2.0870 / −20.1025 | 1.7143 / −4.8264 | 2.3478 / −12.7562 | 1.8571 / +3.1981 |
| `smt_f3_stressed_costs` | 1.9130 / −27.9396 | 1.7143 / −7.2082 | 2.1739 / −24.3100 | 1.5714 / +1.7215 |
| `smt_f5_base` | 0.5217 / −1.7894 | 0.4286 / −2.6883 | 0.6522 / −7.2829 | 0.4286 / +0.2858 |
| `smt_f3_m5_aligned_base` | 0.8696 / −3.1381 | 0.5714 / −3.6628 | 1.0000 / −5.9972 | 0.5714 / +2.5906 |

Trong tất cả 16 cấu hình SMT trên bốn phân đoạn, **chưa cấu hình/phân đoạn nào đạt trung bình hơn 10 entry khớp/ngày**. Cao nhất là `smt_f3_base` ở phần phát triển gần nhất: **54 khớp / 23 ngày = 2,3478 khớp/ngày**, PnL **−12,7562 USDT**; cùng cấu hình ở phần phát triển tháng trước có **48 khớp / 23 ngày**, PnL **−20,1025 USDT**. Cả 16 cấu hình đều âm ở hai phần phát triển. Một số phần cuối kỳ tháng sau dương với số lệnh nhỏ; các giai đoạn này đã được xem ở nghiên cứu trước, nên không phải kiểm chứng mới độc lập.

Luồng quan sát của `smt_f3_base` trong 23 ngày gần nhất là **2.516 SMT thô → 101 intent → 85 limit được nhận → 54 lần khớp**. Chuỗi MSS/displacement/FVG sau SMT chỉ tạo khoảng 4,39 intent/ngày trước khi qua quản lý rủi ro. Vì vậy nút thắt tần suất trong phép thử này nằm chủ yếu ở chuỗi xác nhận setup và chờ khớp; bốn cấu hình base được so sánh không có lần từ chối do trần số lệnh hoặc tổng risk. SMT thô có thể gồm nhiều mức bị quét, không phải 2.516 setup độc lập hoàn chỉnh. Ngày có nhiều lần khớp nhất trong toàn bộ 64 phân đoạn SMT cũng chỉ có sáu lần; tăng giới hạn risk không được chứng minh là cách đạt mục tiêu.

Kết quả cho thấy các thử nghiệm hiện tại đáp ứng gate SMT và RR ước tính khi nhận lệnh, nhưng **chưa đạt mục tiêu tần suất và chưa xác lập lợi thế có lợi nhuận**. Đây là kết luận về các cấu hình/dữ liệu đã thử, không phải chứng minh mọi chiến lược SMT đều không thể đạt hơn 10 lệnh/ngày. Chưa chọn cấu hình để triển khai. Nghiên cứu chỉ có trong evaluator offline; chưa có tùy chọn kích hoạt trên runner Railway hoặc journal V1–V4.

Báo cáo `report_version=4` được lưu tại `paper-runs/research-20261029-30d/evaluation-smt.json` (SHA-256 `6a0d899730d627ff9b285cd44c219e244a0511a7f61aed96dfb4f1f9c862db7b`) và `paper-runs/research-20261029-entry-v2-prior30d/evaluation-smt.json` (SHA-256 `4436b5059451afa1d35bb9bcc17975a56c22ad0c99d0c44f2dd8fcae394c9271`). Cấu hình đầy đủ, số liệu chưa làm tròn, tần suất theo ngày và bằng chứng admission nằm trong hai file. Các số liệu Entry V2 bên dưới là bằng chứng cũ, không phải kết quả của 16 cấu hình SMT.

RR ròng hiện tính theo phí giao dịch và slippage SL của từng kịch bản, **chưa tính funding**. Vị thế perpetual giữ đúng thời điểm funding có thể trả hoặc nhận funding; chu kỳ phụ thuộc cặp và có thể thay đổi theo cơ chế của sàn. Do đó kết quả chưa mô phỏng funding không phải lợi nhuận ròng đầy đủ của futures thực. [Bybit: Funding Fee Calculation](https://www.bybit.com/en/help-center/article/Funding-fee-calculation).

#### Nghiên cứu Entry V2 trước khi bổ sung SMT

```powershell
# 500 nến warmup trước cửa sổ đánh giá 29/09–29/10/2026 UTC:
cargo run --locked --release --no-default-features --bin paper_history -- --start 2026-09-28T15:40:00Z --end 2026-10-29T00:00:00Z --output paper-runs/research-30d-new
cargo run --locked --release --no-default-features --bin paper_backtest -- --dataset paper-runs/research-30d-new/manifest.json --split-at 2026-10-22T00:00:00Z --output paper-runs/research-30d-new/evaluation.json
```

Dataset đã thu thập tại `paper-runs/research-20261029-30d/`: **43.700 nến mỗi cặp**, gồm 500 nến warmup và 30 ngày đánh giá `[29/09/2026 00:00, 29/10/2026 00:00)` UTC. Chia theo thời gian thành 23 ngày phát triển và 7 ngày kiểm tra sau cùng (holdout). Mỗi phần có tài khoản 100 USDT độc lập và chỉ warmup bằng 500 nến trước nó; không cộng dồn lợi nhuận của hai phần.

Báo cáo entry-v2 trước SMT dùng `report_version=3` với **18 cấu hình cố định**. Giữ nguyên 10 cấu hình của version 2: sáu cấu hình cũ (`always`/`killzones` hiện tại và TP nghiên cứu `nearest_viable`, mỗi loại ở hai mức chi phí), cộng bốn cấu hình `always` hiện tại/`nearest_viable` ở hai mức chi phí nhưng bỏ qua nến entry hợp lệ đầu tiên. Thêm tám cấu hình `entry_v2_*`, đều `always`, kết hợp hai cách chọn TP (`nearest`/`nearest_viable`), hai mức chi phí (phí mỗi chiều/stop slippage **6/2** hoặc **10/5 bps**) và hai cách tính thời điểm khớp (nến sau hoặc bỏ qua nến hợp lệ đầu tiên).

Entry-v2 là giả thuyết nghiên cứu `resting_on_confirmation`, thay đổi cách hình thành lệnh chờ:

- Cách cũ `legacy_retracement` chờ một nến đóng đã chạm 50% FVG (Consequent Encroachment, CE), sau đó mới tạo intent và đợi giá đi qua entry ở nến sau. Entry-v2 tạo intent LIMIT tại CE ngay lúc nến thứ ba xác nhận FVG đóng; giá LIMIT phải nằm phía thụ động so với giá đóng xác nhận. Việc tạo intent vẫn phải qua kiểm tra vốn, rủi ro và RR trước khi broker paper nhận lệnh.
- Giữ ngữ cảnh sweep đã có MSS/displacement hợp lệ khi xuất hiện sweep mới, thay vì thay setup đang chờ FVG. Nếu nến sau phá cực trị sweep gốc (low thấp hơn với long, high cao hơn với short), setup bị hủy. Vẫn giữ giới hạn tuổi setup tính từ sweep; không gia hạn lại khi có MSS.
- FVG phải hình thành ngay ở nến kế tiếp sau MSS, với nến displacement xác nhận MSS là nến giữa của FVG. Không mở rộng cửa sổ FVG hoặc bỏ kiểm tra thứ tự thời gian. Không ghi nhận khớp trên chính nến tạo FVG; các nến sau vẫn phải đi qua giá entry theo quy tắc broker paper.

Trì hoãn đúng một nến là phép thử độ nhạy riêng: không thay expiry hoặc protective exit, không tái tạo độ trễ thời gian thực hay giá trong thời gian xử lý bù dữ liệu, và không bảo đảm tạo ra cận dưới của PnL. Tám cấu hình mới kiểm tra chung chính sách tạo lệnh tại xác nhận và giữ ngữ cảnh MSS; chưa tách riêng tác động của hai thay đổi này.

Giả thuyết `nearest_viable` xét mục tiêu thanh khoản đối diện đã xác nhận tiếp theo chỉ khi mục tiêu gần nhất không đạt RR trước/sau chi phí; không thử mục tiêu xa hơn để vượt lỗi thiếu vốn, khối lượng hoặc số lệnh. Mọi cấu hình giữ vốn 100 USDT, x10, risk tối đa 1% mỗi lệnh/3% tổng, tối đa bốn lệnh (hai mỗi cặp), drawdown ngày 5% và net RR ≥ 2. **Entry-v2, TP nghiên cứu và trì hoãn một nến chỉ được bật trong evaluator offline; runner và constructor mặc định giữ `legacy_retracement`, V1–V4 vẫn replay theo quy tắc cũ.**

Đã bổ sung dataset hồi cứu liền trước tại `paper-runs/research-20261029-entry-v2-prior30d/`: **43.700 nến mỗi cặp**, thu thập `[29/08/2026 15:40, 29/09/2026 00:00)` UTC. Sau 500 nến warmup, cửa sổ đánh giá là `[30/08/2026 00:00, 29/09/2026 00:00)`; chia tại **22/09/2026 00:00 UTC** thành 23 ngày và 7 ngày, mỗi phần bắt đầu bằng tài khoản 100 USDT riêng. `collection-verification.json` lưu kiểm tra số lượng, khoảng thời gian liên tục, cờ đóng nến và SHA-256. Hai cửa sổ đánh giá không chồng lấn; 500 nến cuối của dataset bổ sung trùng phần warmup của dataset tháng sau.

```powershell
# Chạy evaluator trên dataset bổ sung đã có; chọn tên output chưa tồn tại:
cargo run --locked --release --no-default-features --bin paper_backtest -- --dataset paper-runs/research-20261029-entry-v2-prior30d/manifest.json --split-at 2026-09-22T00:00:00Z --output paper-runs/research-20261029-entry-v2-prior30d/evaluation-entry-v2-new.json
```

Đây là kiểm tra độ bền trên một giai đoạn quá khứ bổ sung, **không phải forward holdout mới** hoặc bằng chứng độc lập qua nhiều mùa thị trường.

Kết quả version 3 ngày 29/10/2026 đọc từ `evaluation-entry-v2.json` trong mỗi thư mục dataset. Mỗi ô là **số lệnh đã đóng / thay đổi tổng tài sản sau phí (USDT, làm tròn bốn chữ số)**; `*` nghĩa là còn một vị thế mở và kết quả có phần định giá theo nến cuối. Mỗi cột bắt đầu lại với 100 USDT; không cộng chúng thành lợi nhuận liên tục.

| Cấu hình 24/7 | 30/08–22/09 phát triển | 22/09–29/09 cuối kỳ | 29/09–22/10 phát triển | 22/10–29/10 cuối kỳ |
| --- | ---: | ---: | ---: | ---: |
| Legacy hiện tại, `nearest`, chi phí 6/2 | 0 / 0.0000 | 0 / 0.0000 | 1 / −0.9592 | 0 / 0.0000 |
| Entry-v2 `nearest`, chi phí 6/2 | 1 / −0.9083 | 0 / 0.0000 | 0 / 0.0000 | 0 / 0.0000 |
| Entry-v2 `nearest_viable`, chi phí 6/2 | 65 / −28.3739 | 16 / −6.9832* | 66 / −18.2177 | 18 / +0.4826* |
| Entry-v2 `nearest_viable`, chi phí 10/5 | 63 / −32.6082 | 15 / −9.4128* | 63 / −27.1132 | 18 / −1.7097 |
| Entry-v2 `nearest_viable`, bỏ nến entry đầu, chi phí 6/2 | 59 / −24.8679 | 14 / −5.3197* | 57 / −17.3899 | 17 / +1.1083* |
| Entry-v2 `nearest_viable`, bỏ nến entry đầu, chi phí 10/5 | 57 / −29.5631 | 13 / −7.7735* | 54 / −25.6295 | 17 / −0.8310 |

SHA-256 của báo cáo 29/09–29/10: `354a3610f52b271dd1fee2c6a5776bf0dc3bd06b1b7030fbd845faf1f3d92791`; báo cáo 30/08–29/09: `cc3d9f8249fcc68f2330a7499ca6098ff709026642a5ed89f592d9fcd4b21d95`. Các cấu hình còn lại và số liệu chưa làm tròn nằm trong hai file JSON; kết quả version 2 vẫn được giữ tại `evaluation-integrity.json`.

Chẩn đoán legacy trong kỳ 29/09–29/10 vẫn là **77 intent, 62 bị loại vì RR trước phí, 14 vì RR sau chi phí, chỉ một được nhận**; không quy chung mọi lần từ chối cho phí hoặc kết nối. Entry-v2 `nearest` ở chi phí 6/2 tạo **158 intent nhưng không nhận lệnh nào**: 134 không đủ RR trước phí, 22 không đủ RR sau chi phí, hai không có thanh khoản đối diện hợp lệ. Vì vậy sửa thời điểm vào lệnh chưa giải quyết được nút thắt TP/risk. Chọn TP đủ RR tạo thêm lệnh nhưng cả bốn cấu hình `nearest_viable` đều lỗ đáng kể ở hai phần phát triển, và lỗ ở phần cuối kỳ tháng trước; kết quả dương nhỏ tháng sau chuyển âm khi tăng chi phí. **Chưa có cơ sở đưa entry-v2 lên runner đang chạy.** Mọi báo cáo giữ trạng thái **`UNVALIDATED`**, không tự điều chỉnh hoặc thay chiến lược trên Railway.

Báo cáo lưu hash dataset, cấu hình và mã nguồn được biên dịch để đối chiếu. Các giới hạn khi đọc kết quả:

- Tick/step/minimum dùng metadata tại lúc thu thập, có thể khác metadata lịch sử. Phí/slippage là giả định kiểm tra, chưa xác minh theo fee tier tài khoản.
- Chưa mô phỏng funding, thanh lý, mark-price maintenance margin, hàng đợi lệnh, khớp một phần hoặc tác động thị trường. Drawdown chỉ đo ở giá đóng nến.
- Chưa mô phỏng độ trễ chờ REST ổn định, polling và nhận lệnh: OHLC nến kế tiếp có thể chứa giá xuất hiện trước lúc bot thực sự nhận setup. PAPER/backtest không chứng minh khả năng khớp lệnh thực tại những giá đó.
- So sánh nhiều cấu hình trên cùng holdout không tạo ra kiểm chứng độc lập cho cấu hình được chọn. Bảy ngày cuối đã được dùng ở lần đánh giá trước; phép thử mới không biến chúng thành dữ liệu kiểm tra chưa dùng. Cần thêm dữ liệu mới, đánh giá qua nhiều trạng thái thị trường và forward paper trước khi thay chiến lược đang chạy.

## Chế độ cũ: PAPER đơn cặp, 1x, không cần API key

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

## Deploy PAPER lên Railway

Dockerfile ở root chỉ build release binary `paper` bằng Rust 1.89, `Cargo.lock` và `--no-default-features`; runtime không chứa Cargo/source code và không cần API key. `.dockerignore` chỉ cho phép Cargo metadata và source cần build vào context, loại log, credentials và cache cục bộ. Đây là **worker thử nghiệm PAPER**, chưa phải dịch vụ tự phục hồi 24/7: mỗi lần restart/redeploy là một phiên mới, số dư mới 100 USDT, không tiếp tục vị thế hoặc daily-risk baseline của phiên cũ.

Thiết lập trên Railway Dashboard:

1. Tạo service từ GitHub repository `minhhoang080100-beep/TradingBot`, branch `main`, root directory là root repository. Railway tự nhận `Dockerfile`; không cần custom Build Command. [Dockerfile detection](https://docs.railway.com/builds/dockerfiles)
2. Trước phiên cần giữ dữ liệu, gắn persistent **Volume tại `/data`**. Bot ghi JSONL ở `/data/paper-runs/`; filesystem ngoài volume không phải kho lưu trữ bền vững. Image dùng UID `0:0` để tương thích volume root-owned, không tự đổi ownership dữ liệu đã có. Đây là đánh đổi quyền của container, không phải cấu hình least-privilege đã harden. [Volume caveats](https://docs.railway.com/volumes/reference#caveats)
3. Chỉ chạy **1 instance**, không cần public domain, exposed port hoặc HTTP healthcheck vì đây không phải web server. Không thêm exchange API key/secret vào Variables.
4. Đặt **Restart Policy = Never** trong giai đoạn này. Bot dừng khi gặp lỗi dữ liệu/network; tự restart sẽ tạo tài khoản mới và làm mất tính liên tục của phép thử. Tắt **Serverless/app sleeping** nếu muốn phiên chạy liên tục. [Restart policy](https://docs.railway.com/deployments/restart-policy), [Serverless](https://docs.railway.com/deployments/serverless)
5. Lần đầu đặt Custom Start Command để thử 5 phút sau warmup:

   ```text
   /usr/local/bin/paper portfolio --symbols BTCUSDT,ETHUSDT --leverage 10 --balance 100 --entry-window always --seconds 300
   ```

   Đây là lệnh đầy đủ vì Railway override `ENTRYPOINT` của Docker image. Khi đã kiểm tra log, volume và kết nối public Bybit, bỏ Custom Start Command để dùng mặc định portfolio BTC/ETH x10 liên tục, hoặc giữ lệnh trên nhưng bỏ `--seconds 300`. Việc redeploy này vẫn bắt đầu **phiên mới**, không nối tiếp phiên thử. [Start command](https://docs.railway.com/deployments/start-command)
6. Xem deployment logs để xác nhận `PAPER`, đường dẫn JSONL, warmup và cập nhật nến; 0 giao dịch không tự chứng minh bot lỗi. Theo dõi Usage/credit còn lại và sao lưu JSONL trước khi trial/volume hết hạn. Limited Trial có thể hạn chế outbound; cần kiểm tra public Bybit từ chính môi trường triển khai và tuân thủ điều kiện truy cập theo khu vực, không suy ra từ kết nối trên máy cá nhân. [Trial limits](https://docs.railway.com/pricing/free-trial), [Bybit access restrictions](https://bybit-exchange.github.io/docs/v5/guide)

Không đặt `--output` thành một tên file cố định: CLI dùng `create_new` và sẽ từ chối file đã tồn tại. Volume giữ log qua deployment, **không bổ sung tính năng resume**. Khi lỗi, kiểm tra log trước khi chủ động bắt đầu phiên mới; không gộp các phiên 100 USDT thành một đường equity liên tục. Portfolio có Telegram best-effort và phục hồi GET tạm thời có giới hạn nếu process còn sống, nhưng chưa có healthcheck HTTP, khôi phục account PAPER sau restart hoặc rotation log tự động. Không coi trạng thái deployment `Active` là xác nhận chiến lược đang nhận dữ liệu mới. Sau khi nối GitHub, push tiếp theo có thể kích hoạt redeploy; cần kiểm soát auto-deploy nếu muốn giữ một phiên thử liên tục.

Kiểm tra image cục bộ (không gọi API tài khoản):

```sh
docker build -t ict-paper .
docker run --rm ict-paper --help
docker volume create ict-paper-data
docker run --rm --mount source=ict-paper-data,target=/data ict-paper portfolio --symbols BTCUSDT,ETHUSDT --leverage 10 --balance 100 --seconds 300
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
| Portfolio/observer | `src/paper/book.rs`, `src/paper/multi_portfolio.rs`, `src/bin/paper/multi_session.rs`, `src/bin/paper/portfolio_runner.rs`, `src/telegram.rs` | V4: entry 24/7 hoặc killzones, BTC/ETH nhiều setup độc lập, vốn/risk chung, log/replay versioned, Telegram opt-in; giữ engine/journal v2 để replay lịch sử. |

Các actor dùng bounded `tokio::sync::mpsc`; candle store dùng `Arc<RwLock<_>>`. Phần tính toán và coordinator có API đồng bộ để kiểm thử độc lập với mạng. `OrderBlock` hiện là domain model; không nên hiểu sự có mặt của model là đã có một chiến lược order-block riêng.

Killzone dùng `America/New_York`, có chuyển EST/EDT theo DST: London `[02:00, 05:00)` và New York `[07:00, 10:00)`. Đây là giờ địa phương New York, không phải UTC−5 cố định cả năm. Lịch mặc định là thứ Hai đến thứ Sáu, có cấu hình ngày giao dịch.

## Risk và execution

Risk thư viện mặc định là 1% equity mỗi trade, tối đa 5% tổng planned risk đang được reservation; **runner PAPER portfolio v3/v4 đặt riêng tổng risk 3%**, drawdown ngày 5% và RR tối thiểu 2. Quantity luôn làm tròn xuống theo step. SL được đẩy ít nhất một tick ra ngoài extreme của sweep; RR được kiểm tra lại sau rounding. Circuit breaker so với opening equity của ngày UTC và giữ trạng thái tripped trong ngày, kể cả khi equity phục hồi. Caller phải cung cấp baseline đúng của ngày và equity snapshot còn mới; snapshot intraday không tự đặt lại baseline.

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

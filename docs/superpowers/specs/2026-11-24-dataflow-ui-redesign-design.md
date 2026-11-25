# Thiết kế lại UI KafkaLab quanh data flow

## 1. Mục đích

Người dùng muốn **nhìn rõ hơn data flow**. UI hiện tại trình bày broker, partition, producer và consumer thành các panel rời, nên người học phải tự ghép xem dữ liệu đi từ đâu đến đâu.

Bản mới dùng ngôn ngữ hình ảnh trong `design/` (design system `design/notebook_telemetry/DESIGN.md`) và đặt một bản đồ flow trực tiếp vào trung tâm. Nó phải trả lời được hai câu hỏi:

1. Hệ thống đang chảy thế nào, và nghẽn ở đâu?
2. Record này đã đi qua những đâu?

## 2. Mục tiêu, ngoài phạm vi, điều không được xảy ra

**Mục tiêu**
- Bản đồ flow: producer → partition và leader → consumer group → (với topic của service) service → DB.
- Bấm một record để xem hành trình của nó trong khung bên phải.
- Bốn màn hình: Playground, Lessons, Failure Scenarios, Live Dashboard.
- Giữ mọi khả năng của `pg.js`: tạo/xoá topic, producer, consumer, send, poll, commit, auto, kill/stop/start broker, dọn sân chơi, bài học, CLI tương đương, nhật ký.
- Đưa scenario từ `classic.html` vào UI mới, gồm reset baseline, chaos container và free-play produce.

**Ngoài phạm vi**
- Không đổi API, Go backend, scenario hay nội dung bài học.
- Không thêm bài học hoặc scenario mới (KRaft, Idempotent, Compaction, Replication Failure trong ảnh mẫu).
- Không làm CLI Console, Network/Sequence, nút tài khoản, CPU/disk của broker, biểu đồ 1h/6h.

**Điều không được xảy ra (thất bại dù app vẫn chạy)**
- UI vẽ điều backend không biết. Không dùng số liệu mẫu, không hiện animation sao chép từng record tới follower.
- Trạng thái không biết bị hiển thị như 0 hoặc như "khoẻ".
- Animation che mất số liệu hoặc khiến lỗi khó đọc hơn.

## 3. Ràng buộc

- Preact + htm từ `web/static/vendor`, ES modules, **không có bước build**, nhúng bằng `go:embed`.
- Chạy được offline: không dùng Tailwind CDN hay Google Fonts. Inter và JetBrains Mono (woff2) nằm trong `web/static/vendor/fonts`.
- Nội dung giao diện **tiếng Việt**, giữ thuật ngữ Kafka tiếng Anh (cùng ngôn ngữ với bài học và scenario).
- Key và value của record luôn render dạng text. Chỉ `reveal` từ server mới dùng HTML, như hiện nay.
- App vẫn phải dùng được khi thiếu `build/service.jar`, broker chết, hoặc `localStorage` bị chặn.

## 4. Các cách đã cân nhắc

| Cách | Được | Mất | Kết quả |
|---|---|---|---|
| A. Bản đồ flow | Thấy toàn cảnh, lag và lỗi | Khó theo một record | |
| B. Timeline theo record | Rõ nguyên nhân, thứ tự | Không thấy toàn hệ thống | |
| C. Bản đồ + hành trình record | Trả lời cả hai câu hỏi | Tốn công nhất | **Người dùng chọn** |

## 5. Nguồn dữ liệu và độ tin cậy

| Thông tin | Nguồn | Giới hạn phải thể hiện trên UI |
|---|---|---|
| Producer gửi record, partition, offset, thời gian, lỗi | `producers[].sends` | Chỉ có các lần gửi gần nhất trong bộ đệm |
| Log của partition | `topic.partitions[].records` | Chỉ phần đuôi; topic `lab.*` không có record |
| Leader, replicas, ISR, start, end | `topic.partitions[]` | `start`/`end` = -1 nghĩa là chưa biết, hiện `?` |
| Consumer nhận record, đọc lại | `consumers[].received` | Tối đa 30 record mỗi consumer, không có timestamp |
| Vị trí đọc, partition được gán | `consumers[]` | |
| Offset đã commit | `groups[].committed` | Chỉ biết đã commit tới đâu, không biết thời điểm chính xác |
| Service, DB, DLQ | SSE `snapshot` | **Chỉ là số tổng**, không theo từng record |
| Bước scenario | SSE `step`, `scenario` | |

Dữ liệu playground lấy bằng polling 700 ms như hiện nay. Dữ liệu service lấy qua SSE.

## 6. Bản đồ flow

```text
PRODUCERS          KAFKA CLUSTER                      GROUPS            SERVICE      DB
┌────┐      ┌───────────────────────────────┐     ┌──────────┐     ┌────────┐  ┌────┐
│ p1 │─────►│ P0 L:B1 ISR 3/3 [41|42|43|→]  │────►│ g1 lag 2 │────►│ svc-1  │─►│rows│
└────┘   └─►│ P1 L:B2 ISR 3/3 [17|18|→]     │────►│ c1 c2    │     └────────┘  └────┘
            │ P2 L:B3 ISR 2/3 [9|→]         │     └──────────┘
            │ B1 ●   B2 ●   B3 ✕            │
            └───────────────────────────────┘
```

- SVG cho đường nối, HTML cho node. Sơ đồ phẳng, trái sang phải, nền lưới 16 px theo design system. Không dùng khối isometric vì nhãn khó đọc.
- Cột Service và DB chỉ hiện khi topic đang chọn là topic của service (`lab.short-term`).
- **Màu đường nối** (xét từ trên xuống, luật đầu tiên khớp sẽ thắng):
  1. **Đỏ:** partition không có leader; broker đang được liệt kê là leader đã chết; ISR nhỏ hơn `minIsr`; hoặc lần gửi gần nhất trên đường này lỗi, trong vòng 30 giây và chưa có lần gửi thành công sau đó.
  2. **Vàng:** một replica chết hoặc nằm ngoài ISR nhưng ISR vẫn đạt `minIsr`; broker đã chết nhưng còn trong ISR; hoặc lag của group lớn hơn 0, không giảm trong ít nhất 10 giây, và group có member.
  3. **Xanh:** có thay đổi quan sát được (`end`, vị trí đọc hoặc commit) trong 5 giây gần nhất.
  4. **Chấm xám:** còn lại.
- **Lag** = `end - committed`, cùng định nghĩa với Kafka. Vị trí đọc của consumer hiện riêng, để thấy phần đã đọc nhưng chưa commit. Nếu `end` chưa biết thì lag là `?` và không dùng để tô màu.
- **Topic của service:** không có cột Producer vì backend không biết ai ghi vào. Service có nhánh riêng tới node **DLQ** hiện `dlqEnd`. Group `-tracking` là node riêng, không nối tới service.
- **Token chuyển động:** sinh ra khi hai lần đọc state khác nhau (send mới, consumer nhận record mới). Mỗi lần cập nhật vẽ tối đa 12 token, phần còn lại gộp thành nhãn `+N`. Tắt khi `prefers-reduced-motion`.
- Token biểu diễn sự thay đổi đã quan sát được, nên trễ tối đa khoảng một chu kỳ đọc. Chú thích trên bản đồ nói rõ điều này.
- Bản đồ không vẽ từng record sao chép sang follower, chỉ hiện trạng thái ISR của từng replica.
- Topic của scenario không có record, nên chỉ hiện số tổng và offset, không có token theo record.
- Bấm node (producer, partition, broker, group, consumer) mở inspector của node đó. Các thao tác như Send, Poll, Commit, Kill nằm trong inspector.

## 7. Hành trình record (inspector)

Ghép theo khoá `topic + partition + offset`:

```text
#42 · key=xe-1 · P0
✓ Gửi        p1 · acks=all · 12 ms · 10:01:02
✓ Ghi log    leader B1 · ISR khi xem [1,2,3]
✓ Consumer   c1 (g1) đã nhận · đọc lại: không
○ Commit     g1 mới commit tới 41, chưa bao gồm #42
```

- Một chặng chỉ có ✓ khi có bằng chứng. Thiếu dữ liệu thì ghi "không còn trong bộ đệm", không suy đoán.
- **Xác minh danh tính:** chặng Gửi và Consumer chỉ được ✓ khi `key` và `value` khớp với record trong log. Không khớp thì ghi "không xác minh được".
- Send có `acks=0` không bao giờ được ghép theo offset, vì client không nhận offset thật từ broker. Chặng Gửi ghi "acks=0: không có xác nhận".
- Khi trình duyệt thấy topic biến mất hoặc `end` giảm, mọi bằng chứng cũ của topic đó bị loại khỏi việc ghép. Sau khi tải lại trang, nếu topic đã bị tạo lại với record giống hệt key và value thì vẫn có thể ghép nhầm. Đây là giới hạn đã chấp nhận vì không đổi API, và inspector ghi chú "ghép theo topic, partition, offset, key, value".
- Commit xong khi `committed > offset` (committed trỏ tới record kế tiếp).
- Với nhiều group, mỗi group có dòng Consumer và Commit riêng.
- ISR là **trạng thái lúc đang xem**, không phải lúc record được ghi, và nhãn phải nói vậy.

## 8. Các màn hình

Thanh trên cùng: KafkaLab, bốn tab, trạng thái broker thật, nút **Tạm dừng hiển thị**. Nút này chỉ đóng băng màn hình để đọc, không dừng Kafka, và nhãn phải nói rõ điều đó.

- **Playground:** bản đồ flow + inspector. Thanh dưới: chọn topic, tạo topic, tạo producer/consumer, CLI tương đương, nhật ký, dọn sân chơi.
- **Lessons:** danh sách 8 bài bên trái; nội dung bài (đoán, các bước, mục tiêu, giải thích, CLI, tự kiểm tra) cùng **đúng workspace của Playground** (bản đồ, inspector, thanh công cụ), mở trên topic `orders`, nên làm bài ngay tại đây. Khi chưa có `orders`, bản đồ hiện nút tạo topic. Giữ tiến độ như hiện nay. Vì nội dung bài không đổi, UI phải giữ các tên mà bài học nhắc tới: ô Topic, ô Producer, ô Consumer, thẻ consumer theo tên (`c1`), hàng Broker, làn partition, cờ group (`g1`), cột "đã gửi".
- **Failure Scenarios:** danh sách theo module; stepper Dự đoán → Chạy → Quan sát → Giải thích; bản đồ flow của topic service; bước chạy trực tiếp. Giữ nguyên quy tắc giải thích mở sau khi chạy xong hoặc khi bấm "bỏ qua". Có reset baseline, free-play produce và điều khiển container. Khi thiếu service, hiện hướng dẫn `make jar && make up`.
- **Live Dashboard:** thẻ cluster, throughput, lag, DB rows; bảng broker, topic, group; biểu đồ throughput và lag. Throughput, lag và bảng group **chỉ tính cho topic đang chọn**, có nhãn nói rõ, vì API chỉ trả offset và group của một topic mỗi lần đọc. Bảng topic chỉ hiện tên, số partition, RF. Throughput tính từ chênh lệch `end` trong trình duyệt, lưu tối đa 15 phút, mất khi tải lại trang và có ghi chú như vậy. Lịch sử được lấy mẫu ở mọi tab nhưng riêng theo topic; khoảng không chọn topic đó hiện là đoạn đứt, không nối liền.

Responsive: dưới 1280 px inspector nằm dưới bản đồ; dưới 768 px các cột của bản đồ xếp dọc, không tràn ngang trang.

## 9. Cấu trúc file

- `web/static/index.html`: khung trang và importmap.
- `web/static/ui/`: ES modules, tách theo `api`, `store` (state + phát hiện thay đổi), `trace` (ghép hành trình), `flow`, `inspector`, và mỗi màn hình một file.
- `web/static/ui/theme.css`: token từ `DESIGN.md`; không có màu hard-code ngoài file này.
- `pg.js` và `pg.css` bị thay thế và xoá. `classic.html`, `app.js`, `style.css` được giữ, có link nhỏ ở footer, cho tới khi người dùng quyết định bỏ.

## 10. Kiểm chứng

- Unit test cho logic thuần (`trace`, phát hiện thay đổi, tính lag/throughput) bằng `node --test`, không thêm dependency.
- `go test ./...` vẫn pass.
- Chạy app thật trong trình duyệt ở 1440 px và 375 px: gửi, poll, commit, xem hành trình record; kill broker và xác nhận ISR, màu đường nối, lỗi hiển thị đúng; start lại broker.
- Scenario cần service chỉ kiểm tra được khi có `build/service.jar`. Nếu không có, báo rõ là chưa kiểm chứng.

## 11. Phần để người xây dựng tự quyết

Tên hàm, chia nhỏ component, thời lượng animation, kích thước node, thư viện vẽ biểu đồ (ưu tiên SVG tự viết, không thêm dependency).

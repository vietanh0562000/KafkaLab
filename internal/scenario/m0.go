package scenario

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"

	"kafkalab/internal/chaos"
	"kafkalab/internal/kafkax"
	"kafkalab/internal/lab"
	"kafkalab/internal/observe"
	"kafkalab/internal/payload"
)

func m0() []Scenario {
	return []Scenario{basics(), lagAndThroughput()}
}

func basics() Scenario {
	return Scenario{
		ID: "0.1", Module: "M0", Title: "Topic, partition, offset, group",
		Question: "Bạn sắp gửi 1.000 record vào topic 3 partition, key là permanentId của 100 sensor; 1 instance service đọc. " +
			"Khi xong: mỗi partition có đúng 333 record không? Offset của record cuối trong mỗi partition là bao nhiêu? " +
			"Committed offset của group gap-lab sẽ bằng mấy, và nó được ghi lúc nào so với lúc ghi DB?",
		Baseline: lab.ResetOpts{Instances: 1, Env: chaos.DefaultSvcEnv()},
		Steps: []Step{
			Action("Produce 1.000 record (100 sensor × 10 phút), acks=all", produceGood(1000, 100)),
			WaitUntil("Lag về 0 và DB có 1.000 row", 60*time.Second, func(s observe.Snapshot, _ *Ctx) bool {
				return len(s.Group.Lags) == 3 && lagSum(s) == 0 && s.DBRows == 1000
			}),
			Expect("Committed = end trên mọi partition", func(s observe.Snapshot, _ *Ctx) (bool, string) {
				for _, l := range s.Group.Lags {
					if l.Committed != l.End {
						return false, partitionSummary(s)
					}
				}
				return true, partitionSummary(s)
			}),
			Expect("Tổng end offset = 1.000 và partition nào cũng có record", func(s observe.Snapshot, _ *Ctx) (bool, string) {
				var sum int64
				ok := true
				for _, p := range s.Partitions {
					sum += p.End
					ok = ok && p.End > 0
				}
				return ok && sum == 1000, fmt.Sprintf("tổng end %d", sum)
			}),
		},
		Reveal: `<p><b>Partition</b> = murmur2(key) mod 3. Key là <code>permanentId</code>, nên một sensor luôn vào cùng một partition:
thứ tự record <i>của một sensor</i> được giữ, thứ tự <i>giữa các sensor</i> thì không. Số record mỗi partition lệch nhau
vì hash chia theo key, không chia đều theo record.</p>
<p><b>Offset</b> là số thứ tự <i>trong một partition</i>, không phải trong cả topic: P0, P1, P2 đều bắt đầu từ 0.</p>
<p><b>Committed offset</b> là offset tiếp theo group sẽ đọc. <b>Lag = end − committed</b>: số record đã nằm trên broker mà group
chưa xác nhận xử lý xong.</p>
<p>Service chỉ commit <i>sau khi</i> ghi DB xong: listener chạy <code>AckMode.MANUAL</code> (<code>KafkaConfiguration.java</code>),
và <code>ack.acknowledge()</code> chỉ được gọi khi handler trả về thành công (<code>FilteredKafkaDataSource.java</code>).
Đó là <i>at-least-once</i>: chết giữa chừng thì batch được đọc lại chứ không mất; bản trùng bị DB bỏ qua nhờ khoá chính
<code>(permanent_id, start_time)</code>.</p>
<p>Bảng Live còn hiện group thứ hai, <code>gap-lab-tracking</code>, đọc cùng topic với offset riêng. Nhiều group nghĩa là nhiều
"con trỏ đọc" độc lập trên cùng một log; Kafka không xoá record khi có người đọc.</p>`,
		JavaProps: []string{"enable.auto.commit=false", "spring.kafka.listener.ack-mode=manual", "max.poll.records=500",
			"auto.offset.reset=latest", "partitioner (mặc định): murmur2(key) % số partition"},
		CLI: []string{cliDescribeTopic, cliDescribeGroup},
		Confirm: []string{
			"Nếu service commit offset trước khi ghi DB rồi pod chết, chuyện gì xảy ra với batch đó?",
			"Tăng topic từ 3 lên 4 partition thì sensor nào đổi partition, và thứ tự record của sensor đó bị ảnh hưởng thế nào?",
			"Vì sao lag tính bằng end − committed chứ không phải bằng số record đã xử lý?",
		},
	}
}

func lagAndThroughput() Scenario {
	return Scenario{
		ID: "0.2", Module: "M0", Title: "Lag và throughput",
		Question: "Bắn 200.000 record một lúc vào 2 instance, sau đó gửi nhỏ giọt 1 record mỗi giây. Lag lên tới đâu và bao lâu thì về 0? " +
			"Khi nhỏ giọt, từ lúc producer nhận ack tới lúc row xuất hiện trong DB mất bao lâu: vài ms hay vài trăm ms? Vì sao?",
		Baseline: lab.ResetOpts{Instances: 2, Env: chaos.DefaultSvcEnv()},
		Steps: []Step{
			Action("Burst 200.000 record (1.000 sensor)", produceGood(200000, 1000)),
			WaitUntil("Lag về 0 và DB có 200.000 row", 240*time.Second, func(s observe.Snapshot, c *Ctx) bool {
				if lag := lagSum(s); lag > int64OrZero(c.Vars["peak"]) {
					c.Vars["peak"] = lag
				}
				return len(s.Group.Lags) == 3 && lagSum(s) == 0 && s.DBRows == 200000
			}),
			Expect("Lag đã tăng vọt trong lúc burst", func(_ observe.Snapshot, c *Ctx) (bool, string) {
				peak := int64OrZero(c.Vars["peak"])
				return peak > 10000, fmt.Sprintf("lag cao nhất thấy được: %d", peak)
			}),
			ActionObserved("Nhỏ giọt 10 record, cách nhau ngẫu nhiên 0,6–1,4 s, đo độ trễ ack → DB", trickle),
			Expect("Mỗi record chờ một khoảng cỡ fetch.max.wait (p90 ≥ 200 ms, max ≤ 2 s)", func(_ observe.Snapshot, c *Ctx) (bool, string) {
				lat, _ := c.Vars["latencies"].([]int64)
				p90, mx := percentile(lat, 0.9), percentile(lat, 1)
				return len(lat) == 10 && p90 >= 200 && mx <= 2000, fmt.Sprintf("p50 %d ms, p90 %d ms, max %d ms", percentile(lat, 0.5), p90, mx)
			}),
		},
		Reveal: `<p>Lúc burst, producer ghi nhanh hơn service ghi DB, nên lag vọt lên rồi giảm dần. Tốc độ giảm là tốc độ ghi DB
(COPY theo batch tối đa 500 record), không phải tốc độ của Kafka: Kafka chỉ là hàng đợi bền giữa hai bên.</p>
<p>Lúc nhỏ giọt, service cấu hình <code>fetch.min.bytes=5242880</code> (5 MB) và <code>fetch.max.wait.ms=500</code>. Broker giữ mỗi
fetch request lại cho tới khi đủ 5 MB <i>hoặc</i> hết 500 ms. 1 record/giây không bao giờ đủ 5 MB, nên mỗi record phải chờ fetch đang
mở hết hạn, tức 0–500 ms (trung bình khoảng 250 ms), trước khi service nhìn thấy nó.</p>
<p>Lab gửi lệch nhịp ngẫu nhiên vì một lý do đo được: nếu gửi đều đúng 1 record/giây, pha của nhịp gửi khớp với chu kỳ fetch 500 ms
và độ trễ gần như cố định ở một giá trị (đo được 159 ms lặp lại). Một con số đẹp và ổn định như vậy dễ bị đọc nhầm thành "độ trễ của Kafka".</p>
<p>Một điều nữa lab đã đo: nếu topic đặt <code>segment.ms</code> rất ngắn (10 s), broker trả fetch gần như ngay lập tức (~20 ms) và
<code>fetch.min.bytes</code> mất tác dụng. Cấu hình của topic cũng ảnh hưởng tới độ trễ phía consumer, không chỉ cấu hình của consumer.</p>
<p>Đây là đánh đổi có chủ đích: batch to giúp COPY nhanh (throughput), đổi lại độ trễ khi traffic thấp. Với dữ liệu theo phút (PT1M),
nửa giây không đáng kể; với hệ thống cần phản ứng tức thì thì có.</p>`,
		JavaProps: []string{"fetch.min.bytes=5242880", "fetch.max.wait.ms=500", "max.poll.records=500", "max.partition.fetch.bytes=10485760"},
		CLI:       []string{cliDescribeGroup + "   # chạy lặp lại trong lúc burst để thấy LAG giảm dần"},
		Confirm: []string{
			"Nếu hạ fetch.min.bytes xuống 1 thì độ trễ và số lần fetch thay đổi thế nào?",
			"Thêm instance thứ 3 có làm burst xong nhanh hơn không, khi DB mới là nút thắt?",
			"Lag 50.000 có đáng lo không? Bạn cần biết thêm con số nào để trả lời?",
		},
	}
}

func int64OrZero(v any) int64 {
	n, _ := v.(int64)
	return n
}

// trickle sends one record roughly per second and measures ack-to-row latency for each.
func trickle(ctx context.Context, c *Ctx) (string, error) {
	var lat []int64
	for i := 0; i < 10; i++ {
		// A steady 1/s rhythm locks onto the 500 ms fetch cycle and yields one repeated delay; jitter shows the range.
		gap := 600*time.Millisecond + time.Duration(rand.IntN(800))*time.Millisecond
		tick := time.Now()
		p := c.Lab.NextPoints(1, 1)[0]
		rec := kafkax.Record{Key: p.SensorID, Value: payload.ShortTerm(p.SensorID, p.Start, c.Lab.NextSeq())}
		if _, err := send(ctx, c, []kafkax.Record{rec}); err != nil {
			return "", err
		}
		acked := time.Now()
		for {
			ok, err := c.Lab.RowExists(ctx, p.SensorID, p.Start)
			if err != nil {
				return "", err
			}
			if ok {
				break
			}
			if time.Since(acked) > 5*time.Second {
				return "", fmt.Errorf("record %d not in DB after 5s", i)
			}
			time.Sleep(20 * time.Millisecond)
		}
		lat = append(lat, time.Since(acked).Milliseconds())
		if wait := gap - time.Since(tick); wait > 0 {
			time.Sleep(wait)
		}
	}
	c.Vars["latencies"] = lat
	return fmt.Sprintf("độ trễ (ms): %v", lat), nil
}

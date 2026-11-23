package scenario

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"kafkalab/internal/chaos"
	"kafkalab/internal/kafkax"
	"kafkalab/internal/lab"
	"kafkalab/internal/observe"
	"kafkalab/internal/payload"
)

func m3() []Scenario {
	return []Scenario{dbDown(), poisonPill(), dlq(), malformed()}
}

var bothSvc = []string{"gap-svc-1", "gap-svc-2"}

func pausedInstance(s observe.Snapshot) string {
	for _, in := range s.Instances {
		if in.State == "PAUSED" {
			return in.Name
		}
	}
	return ""
}

func allRunning(s observe.Snapshot) bool {
	n := 0
	for _, in := range s.Instances {
		if in.Container != "running" {
			continue
		}
		n++
		if in.State != "RUNNING" {
			return false
		}
	}
	return n > 0
}

func dbDown() Scenario {
	return Scenario{
		ID: "3.1", Module: "M3", Title: "DB sập giữa chừng",
		Question: "Service đang chạy ổn thì TimescaleDB dừng hẳn trong lúc record vẫn tiếp tục đổ vào. Service làm gì với batch đang xử lý: " +
			"bỏ qua, ghi vào DLQ, hay giữ lại? Offset có được commit không? Khi DB sống lại, có mất record nào không?",
		Baseline: lab.ResetOpts{Instances: 2, Env: chaos.DefaultSvcEnv()},
		Steps: []Step{
			Action("Produce 2.000 record", produceGood(2000, 200)),
			WaitUntil("Lag về 0, DB có 2.000 row", 60*time.Second, func(s observe.Snapshot, _ *Ctx) bool {
				return len(s.Group.Lags) == 3 && lagSum(s) == 0 && s.DBRows == 2000
			}),
			Action("docker stop lab-timescaledb", func(ctx context.Context, c *Ctx) error {
				c.Vars["stopAt"] = time.Now()
				return c.Lab.Chaos(ctx, "stop", "lab-timescaledb")
			}),
			Action("Produce thêm 2.000 record khi DB đang chết", produceGood(2000, 200)),
			ActionObserved("Chờ một instance chuyển sang PAUSED", func(ctx context.Context, c *Ctx) (string, error) {
				deadline := time.Now().Add(60 * time.Second)
				for time.Now().Before(deadline) {
					if name := pausedInstance(c.Snapshot()); name != "" {
						c.Vars["paused"] = name
						stopAt := c.Vars["stopAt"].(time.Time)
						return fmt.Sprintf("%s PAUSED sau %.0f s kể từ lúc dừng DB", name, time.Since(stopAt).Seconds()), nil
					}
					time.Sleep(250 * time.Millisecond)
				}
				return "", fmt.Errorf("no instance paused within 60s")
			}),
			WaitUntil("Instance đó thử lại (RECOVERING) rồi PAUSED lần nữa", 90*time.Second, func(s observe.Snapshot, c *Ctx) bool {
				in, _ := instance(s, c.Vars["paused"].(string))
				if in.State == "RECOVERING" {
					c.Vars["sawRecovering"] = true
				}
				return c.Vars["sawRecovering"] == true && in.State == "PAUSED"
			}),
			Action("docker start lab-timescaledb", func(ctx context.Context, c *Ctx) error {
				return c.Lab.Chaos(ctx, "start", "lab-timescaledb")
			}),
			WaitUntil("Mọi instance RUNNING, lag 0, DB có 4.000 row", 180*time.Second, func(s observe.Snapshot, _ *Ctx) bool {
				return allRunning(s) && lagSum(s) == 0 && s.DBRows == 4000
			}),
			Expect("Không mất record nào", func(s observe.Snapshot, c *Ctx) (bool, string) {
				produced, _ := c.Vars["produced"].(int)
				return s.DBRows == int64(produced), fmt.Sprintf("produce %d, DB %d", produced, s.DBRows)
			}),
		},
		Reveal: `<p>Lỗi kết nối DB được xếp vào loại <b>retryable</b> (<code>DefaultExceptionClassifier</code>): lỗi tạm thời, thử lại có thể
thành công. Service thử lại batch 2 lần ngay trên listener thread, cách nhau 1 s (<code>BatchRetryErrorHandler.retryLoop</code>).</p>
<p>Hết lượt thử, service <b>seek</b> consumer về đầu batch, <b>không commit</b> offset, và <b>pause</b> listener container
(container của Spring Kafka, không phải container Docker; <code>PipelineRecoverer.recover</code>). Batch không bị bỏ, không vào DLQ: nó vẫn nằm trên Kafka và sẽ được đọc lại.</p>
<p>Sau mỗi lần pause, service tự resume và thử lại (RECOVERING). Thất bại thì pause lâu gấp đôi:
<b>lab 10 → 20 → 40 → 60 s</b>, <b>prod 30 → 60 → 120 → 240 → 300 s</b>. Lag tăng dần trong lúc DB chết, vì producer vẫn ghi
vào Kafka. Đó chính là việc của Kafka: làm vùng đệm giữa bên ghi và bên đọc.</p>
<p>Thời gian tới lần PAUSED đầu ≈ 3 lần thử × <code>connection-timeout</code> + 2 × 1 s backoff: lab đo được khoảng 18 s
(3 × 5 s + 2 s), prod sẽ khoảng 92 s (3 × 30 s + 2 s). Container DB dừng hẳn thì tên <code>timescaledb</code> biến khỏi DNS của Docker,
nên mỗi lần tạo kết nối đều hỏng ngay (unknown host); dù vậy HikariCP vẫn chờ hết connection-timeout mới báo lỗi, vì pool tiếp tục thử
tạo kết nối mới trong suốt khoảng đó.</p>
<p>Có một ràng buộc ngầm: toàn bộ thời gian thử lại trên listener thread phải nhỏ hơn <code>max.poll.interval.ms</code> (300 s). Nếu
vượt quá, consumer bị coi là đã chết và group rebalance, chồng thêm một sự cố lên sự cố ban đầu. Thời gian đó ≈ (retries + 1) × thời gian một lần
thử + retries × backoff. Lúc khởi động, service chỉ kiểm tra phần retries × backoff (<code>PipelineWiring.java</code>) và bỏ qua thời gian
mỗi lần thử: ở prod với <code>max-retries=9</code>, phép kiểm tra thấy 9 s và cho qua, trong khi thực tế mất khoảng 10 × 30 s + 9 s ≈ 309 s.</p>`,
		JavaProps: []string{"max.poll.interval.ms=300000", "pipelines.error-handling.batch-retry.max-retries=2",
			"pipelines.error-handling.batch-retry.backoff-ms=1000", "pipelines.error-handling.pause-duration-ms=10000 (prod 30000)",
			"pipelines.error-handling.max-pause-duration-ms=60000 (prod 300000)"},
		CLI: []string{cliDescribeGroup + "   # CURRENT-OFFSET đứng yên, LAG tăng trong lúc DB chết"},
		Confirm: []string{
			"Service không commit offset khi pause. Vì sao chính điều đó giúp không mất data nếu pod restart giữa chừng?",
			"Nếu tăng max-retries lên 400 thì điều gì sẽ hỏng trước?",
			"DB chết 2 tiếng ở prod: lag lên tới đâu, và bạn cần kiểm tra cấu hình nào của topic để chắc data chưa bị xoá?",
		},
	}
}

// producePoisonBatch stops consumers, writes 499 good records plus one poison pill, and remembers where it landed.
func producePoisonBatch(ctx context.Context, c *Ctx) error {
	if err := c.Lab.Docker.StopSvc(ctx, bothSvc...); err != nil {
		return err
	}
	good := c.Lab.NextRecords(499, 100)
	p := c.Lab.NextPoints(1, 1)[0]
	poison := kafkax.Record{Key: "lab-sensor-007", Value: payload.Poison(p.Start, c.Lab.NextSeq())}
	recs := slices.Concat(good[:250], []kafkax.Record{poison}, good[250:])
	res, err := send(ctx, c, recs)
	if err != nil {
		return err
	}
	c.Vars["poisonPartition"], c.Vars["poisonOffset"] = res[250].Partition, res[250].Offset
	return c.Lab.Docker.ComposeUp(ctx, bothSvc...)
}

func poisonOwner(s observe.Snapshot, c *Ctx) (observe.Instance, bool) {
	p := c.Vars["poisonPartition"].(int32)
	for _, in := range s.Instances {
		if slices.Contains(in.Partitions, p) {
			return in, true
		}
	}
	return observe.Instance{}, false
}

func poisonPill() Scenario {
	return Scenario{
		ID: "3.2", Module: "M3", Title: "Poison pill",
		Question: "Trong 500 record có đúng 1 record có permanentId dài 300 ký tự (cột DB là varchar(255)). JSON hoàn toàn hợp lệ. " +
			"Chuyện gì xảy ra với 499 record còn lại, và với những record đến sau nó? Restart pod có gỡ được không?",
		Baseline: lab.ResetOpts{Instances: 2, Env: chaos.DefaultSvcEnv()},
		Steps: []Step{
			Action("Dừng consumer, produce 499 record tốt + 1 poison (key lab-sensor-007), bật lại consumer", producePoisonBatch),
			WaitUntil("Instance giữ partition chứa poison chuyển PAUSED", 120*time.Second, func(s observe.Snapshot, c *Ctx) bool {
				in, ok := poisonOwner(s, c)
				if ok && in.State == "PAUSED" {
					c.Vars["owner"] = in.Name
					return true
				}
				return false
			}),
			ActionObserved("Quan sát 40 s", func(ctx context.Context, c *Ctx) (string, error) {
				seen := map[string]bool{}
				for end := time.Now().Add(40 * time.Second); time.Now().Before(end); time.Sleep(time.Second) {
					in, _ := instance(c.Snapshot(), c.Vars["owner"].(string))
					seen[in.State] = true
				}
				c.Vars["ownerStates"] = seen
				var states []string
				for st := range seen {
					states = append(states, st)
				}
				slices.Sort(states)
				return fmt.Sprintf("%s đi qua các trạng thái: %s", c.Vars["owner"], strings.Join(states, ", ")), nil
			}),
			Expect("Instance đó không lần nào trở lại RUNNING", func(_ observe.Snapshot, c *Ctx) (bool, string) {
				seen := c.Vars["ownerStates"].(map[string]bool)
				return !seen["RUNNING"], fmt.Sprintf("RUNNING xuất hiện: %v", seen["RUNNING"])
			}),
			Expect("Committed offset của partition chứa poison không vượt qua poison", func(s observe.Snapshot, c *Ctx) (bool, string) {
				p, off := c.Vars["poisonPartition"].(int32), c.Vars["poisonOffset"].(int64)
				for _, l := range s.Group.Lags {
					if l.Partition == p {
						return l.Committed <= off, fmt.Sprintf("P%d committed %d, poison ở offset %d, lag %d", p, l.Committed, off, l.Lag)
					}
				}
				return false, "partition not found"
			}),
			Expect("DB thiếu record (cả batch chứa poison bị giữ lại)", func(s observe.Snapshot, _ *Ctx) (bool, string) {
				return s.DBRows < 500, fmt.Sprintf("DB %d / 500", s.DBRows)
			}),
			Expect("Instance còn lại vẫn RUNNING", func(s observe.Snapshot, c *Ctx) (bool, string) {
				for _, in := range s.Instances {
					if in.Name != c.Vars["owner"] && in.Container == "running" {
						return in.State == "RUNNING", fmt.Sprintf("%s %s, partitions %v", in.Name, in.State, in.Partitions)
					}
				}
				return false, "no other instance"
			}),
		},
		Reveal: `<p>JSON hợp lệ nên record vượt qua filter và parser. Nó chỉ hỏng ở bước ghi DB: <code>permanent_id varchar(255)</code>
từ chối với SQLState <code>22001</code>. Service xếp mọi SQLState <code>22xxx</code> (data exception) vào loại <b>non-retryable</b>:
thử lại cũng không bao giờ thành công.</p>
<p>Lỗi xảy ra ở mức <b>batch</b>: cả batch được ghi bằng một lệnh COPY. Service có tìm ra dòng hỏng
(<code>AbstractBulkInsertDao.identifyOffendingRecords</code> thử lại từng record mà không ghi gì, rồi log record gây lỗi), nhưng kết quả đó
chỉ để ghi log: error handler làm việc trên cả batch, nên cả batch bị giữ lại.</p>
<p>Không có DLQ (<code>pipelines.error-handling.dlq.enabled=false</code> là mặc định), nên service làm điều duy nhất còn lại để không mất
data: seek về đầu batch, không commit, pause, rồi resume, đọc lại đúng batch đó, hỏng lại, pause lâu hơn… mãi mãi.</p>
<p>Phạm vi ảnh hưởng là <b>mọi partition của consumer đó</b>, không chỉ một record hay một partition: listener container bị pause thì dừng
cả các partition khác nó đang giữ. Instance còn lại vẫn chạy, nên những sensor nằm ở partition do instance đó giữ không bị ảnh hưởng. Restart pod không
giúp gì: offset chưa commit, nên pod mới đọc lại đúng batch đó.</p>
<p>So với 3.4: JSON hỏng thì bị drop và commit (mất lặng lẽ). Record <i>parse được</i> nhưng không ghi được mới là thứ làm kẹt pipeline.</p>`,
		JavaProps: []string{"pipelines.error-handling.dlq.enabled=false (mặc định)",
			"exception-classifier.non-retryable-sql-state-prefixes: 22, 23, 42"},
		CLI: []string{cliDescribeGroup + "   # CURRENT-OFFSET của partition đó đứng yên, LAG tăng"},
		Confirm: []string{
			"Instance kia vẫn chạy. Vậy những sensor nào bị kẹt, những sensor nào không?",
			"Pause tối đa 60 s (prod 300 s) rồi thử lại: một poison pill tốn bao nhiêu lần COPY thất bại mỗi giờ, và log sẽ trông thế nào?",
			"Không bật DLQ, bạn sẽ gỡ kẹt bằng tay thế nào? Lệnh nào, và cái giá phải trả là gì?",
		},
	}
}

func dlq() Scenario {
	return Scenario{
		ID: "3.3", Module: "M3", Title: "Dead letter queue",
		Question: "Lặp lại 3.2 nhưng bật DLQ. Bao nhiêu record sẽ vào topic dlq-lab.short-term: 1 hay nhiều hơn? Pipeline còn kẹt không? " +
			"Muốn đưa 499 record tốt về DB thì phải làm gì, và thứ tự record theo sensor còn đúng không?",
		Baseline: lab.ResetOpts{Instances: 2, Env: chaos.SvcEnv{GroupID: kafkax.Group, DLQ: true}},
		Steps: []Step{
			Action("Dừng consumer, produce 499 record tốt + 1 poison, bật lại consumer (DLQ đang bật)", producePoisonBatch),
			WaitUntil("Lag về 0, mọi instance RUNNING, DLQ có record", 120*time.Second, func(s observe.Snapshot, _ *Ctx) bool {
				return len(s.Group.Lags) == 3 && lagSum(s) == 0 && allRunning(s) && s.DLQEnd > 0
			}),
			Expect("Cả batch vào DLQ, không chỉ poison; DB + DLQ = 500", func(s observe.Snapshot, c *Ctx) (bool, string) {
				c.Vars["dlqEnd"] = s.DLQEnd
				return s.DLQEnd > 1 && s.DBRows+s.DLQEnd == 500, fmt.Sprintf("DLQ %d record, DB %d row", s.DLQEnd, s.DBRows)
			}),
			ActionObserved("Replay DLQ về topic chính, bỏ record poison", replayDLQ),
			WaitUntil("DB có 499 row", 60*time.Second, func(s observe.Snapshot, _ *Ctx) bool {
				return lagSum(s) == 0 && s.DBRows == 499
			}),
		},
		Reveal: `<p><code>DlqPublisher</code> chuyển <b>cả batch</b> bị lỗi sang <code>dlq-&lt;topic&gt;</code>, kể cả các record tốt. Không phải vì
không tìm được dòng hỏng (service có log nó, xem 3.2), mà vì error handler và <code>DlqRouter</code> được thiết kế làm việc trên cả batch: đơn
giản hơn, đổi lại record tốt bị kéo theo. Sau khi DLQ xác nhận đã ghi, offset được commit và pipeline chạy tiếp.</p>
<p>Mỗi record trong DLQ giữ nguyên key, payload và header gốc, thêm <code>x-original-partition</code> /
<code>x-original-offset</code>, nên có thể replay có chọn lọc: bỏ poison, gửi lại phần còn lại.</p>
<p>Chỉ batch hỏng vì <b>dữ liệu</b> (non-retryable) mới vào DLQ. Batch hỏng vì <b>DB chết</b> (retry hết lượt) không bị chuyển sang DLQ;
nếu bị chuyển, một sự cố hạ tầng sẽ đổ hết data tốt vào DLQ (<code>route-retry-exhausted=false</code>).</p>
<p>Cái giá: record replay đến <b>sau</b> những record mới hơn của cùng sensor, nên thứ tự theo key bị phá. Với bảng lưu theo
<code>(permanent_id, start_time)</code> thì không sao, nhưng mọi xử lý kiểu "giá trị mới nhất thắng" sẽ sai.</p>
<p>DLQ chỉ có ích khi có người theo dõi và có công cụ replay; nếu không, nó chỉ là nơi data chết một cách gọn gàng hơn.</p>`,
		JavaProps: []string{"pipelines.error-handling.dlq.enabled=true", "pipelines.error-handling.dlq.topic-prefix=dlq-",
			"pipelines.error-handling.dlq.route-retry-exhausted=false", "pipelines.error-handling.dlq.publish-timeout-ms=10000"},
		CLI: []string{cli("kafka-console-consumer.sh", "--topic dlq-lab.short-term --from-beginning --property print.headers=true --property print.key=true --max-messages 3")},
		Confirm: []string{
			"Nếu replay luôn cả record poison thì chuyện gì xảy ra?",
			"Một bảng khác chỉ giữ giá trị mới nhất của mỗi sensor. Replay DLQ xong, bảng đó sai ở đâu, và bạn phát hiện bằng cách nào?",
			"Ai sẽ biết là DLQ vừa nhận record? Bạn sẽ đặt alert trên con số nào?",
		},
	}
}

// replayDLQ copies every DLQ record except poison pills back to the main topic, with the original key.
func replayDLQ(ctx context.Context, c *Ctx) (string, error) {
	want, _ := c.Vars["dlqEnd"].(int64)
	readCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	recs, err := kafkax.ReadAll(readCtx, kafkax.DLQTopic, want)
	if err != nil {
		return "", err
	}
	var keep []kafkax.Record
	for _, r := range recs {
		var v struct {
			Payload struct {
				PermanentID string `json:"permanentId"`
			} `json:"payload"`
		}
		if json.Unmarshal(r.Value, &v) == nil && len(v.Payload.PermanentID) <= 255 {
			keep = append(keep, r)
		}
	}
	if _, err := send(ctx, c, keep); err != nil {
		return "", err
	}
	return fmt.Sprintf("đọc %d record từ DLQ, gửi lại %d, bỏ %d", len(recs), len(keep), len(recs)-len(keep)), nil
}

func malformed() Scenario {
	return Scenario{
		ID: "3.4", Module: "M3", Title: "Dữ liệu hỏng và schema thay đổi",
		Question: "Gửi 100 record tốt cùng 4 record lạ: một record thiếu intervalLength, một record JSON hỏng, một record có thêm field lạ, " +
			"một record đổi tên permanentId thành permanentID. Record nào vào DB? Pipeline có bị kẹt như 3.2 không? Bạn sẽ thấy dấu vết ở đâu?",
		Baseline: lab.ResetOpts{Instances: 1, Env: chaos.DefaultSvcEnv()},
		Steps: []Step{
			Expect("Ghi lại counter trước khi gửi", func(s observe.Snapshot, c *Ctx) (bool, string) {
				in, ok := instance(s, "lab-gap-svc-1")
				c.Vars["before"] = in
				return ok && in.State == "RUNNING", fmt.Sprintf("drop %.0f, failed %v", in.FilteredDrop, in.Failed)
			}),
			Action("Produce 100 record tốt + 4 record lạ", func(ctx context.Context, c *Ctx) error {
				recs := c.Lab.NextRecords(100, 50)
				odd := c.Lab.NextPoints(3, 3)
				recs = append(recs,
					kafkax.Record{Key: odd[0].SensorID, Value: payload.NoInterval(odd[0].SensorID, odd[0].Start, c.Lab.NextSeq())},
					kafkax.Record{Key: "lab-sensor-broken", Value: payload.BrokenJSON(c.Lab.NextSeq())},
					kafkax.Record{Key: odd[1].SensorID, Value: payload.UnknownField(odd[1].SensorID, odd[1].Start, c.Lab.NextSeq())},
					kafkax.Record{Key: odd[2].SensorID, Value: payload.RenamedID(odd[2].SensorID, odd[2].Start, c.Lab.NextSeq())},
				)
				_, err := send(ctx, c, recs)
				return err
			}),
			WaitUntil("Lag về 0 (không bị kẹt)", 60*time.Second, func(s observe.Snapshot, c *Ctx) bool {
				if in, _ := instance(s, "lab-gap-svc-1"); in.State == "PAUSED" {
					c.Vars["paused"] = true
				}
				return len(s.Group.Lags) == 3 && lagSum(s) == 0 && s.DBRows == 101
			}),
			Expect("101 row: 100 tốt + record có field lạ", func(s observe.Snapshot, _ *Ctx) (bool, string) {
				return s.DBRows == 101, fmt.Sprintf("DB %d", s.DBRows)
			}),
			Expect("Ba record hỏng tăng ba counter khác nhau, mỗi counter +1", func(s observe.Snapshot, c *Ctx) (bool, string) {
				before := c.Vars["before"].(observe.Instance)
				in, _ := instance(s, "lab-gap-svc-1")
				drop := in.FilteredDrop - before.FilteredDrop
				deser := in.Failed["deserialization_error"] - before.Failed["deserialization_error"]
				missing := in.Failed["missing_required_fields"] - before.Failed["missing_required_fields"]
				return drop == 1 && deser == 1 && missing == 1,
					fmt.Sprintf("filtered drop +%.0f, deserialization_error +%.0f, missing_required_fields +%.0f", drop, deser, missing)
			}),
			Expect("Pipeline không lần nào PAUSED", func(_ observe.Snapshot, c *Ctx) (bool, string) {
				return c.Vars["paused"] != true, fmt.Sprintf("PAUSED: %v", c.Vars["paused"] == true)
			}),
		},
		Reveal: `<p>Bốn record lạ đi bốn đường khác nhau, nhưng không đường nào làm kẹt pipeline:</p>
<ul>
<li><b>Thiếu <code>intervalLength</code></b>: <code>IntervalFilter</code> tìm chuỗi byte <code>"intervalLength":"PT1M"</code> trong record
<i>trước khi</i> parse; không thấy thì loại record ngay. Dấu vết: <code>records_filtered_total{result="drop"}</code> và một dòng log.</li>
<li><b>JSON hỏng</b> (nhưng có chuỗi intervalLength): qua filter, hỏng ở Jackson. Dấu vết:
<code>records_failed_total{error_type="deserialization_error"}</code>.</li>
<li><b>Đổi tên field</b> <code>permanentId → permanentID</code>: Jackson phân biệt hoa thường, nên <code>permanentId</code> thành null. Dấu vết:
<code>records_failed_total{error_type="missing_required_fields"}</code>.</li>
<li><b>Thêm field lạ</b>: field đó bị bỏ qua (<code>FAIL_ON_UNKNOWN_PROPERTIES=false</code>, <code>JacksonConfiguration.java</code>),
record vào DB bình thường.</li>
</ul>
<p>Ba record hỏng đều bị <b>drop và commit</b>: không retry, không DLQ, chỉ còn một counter và một dòng log. Đối lập với 3.2: lỗi lúc
<i>parse</i> thì record bị bỏ và pipeline chạy tiếp; lỗi lúc <i>ghi DB</i> thì pipeline dừng. Nếu upstream đổi tên một field bắt buộc,
toàn bộ data sẽ biến mất lặng lẽ trong khi dashboard vẫn xanh, trừ khi có alert trên các counter này.</p>`,
		JavaProps: []string{"DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES=false"},
		CLI:       []string{"docker exec lab-gap-svc-1 wget -qO- localhost:8181/manage/prometheus | grep -E 'records_(filtered|failed)_total'"},
		Confirm: []string{
			"Upstream đổi tên một field bắt buộc lúc 2h sáng. Bạn phát hiện bằng cách nào, sau bao lâu?",
			"Vì sao thêm field mới thì an toàn còn đổi tên field thì không?",
			"Nên drop-và-commit hay dừng pipeline khi gặp record không parse được? Mỗi lựa chọn hỏng theo kiểu nào?",
		},
	}
}

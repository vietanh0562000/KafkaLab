package playground

import (
	"fmt"
	"slices"
	"sort"
	"strings"
)

// LessonTopic is the topic every lesson works on, so each lesson builds on what the previous one left.
const LessonTopic = "orders"

type LessonInput struct {
	Topic     *TopicView // nil until the topic exists
	Brokers   []BrokerView
	Consumers []ConsumerView
	Facts     Facts
}

type Lesson struct {
	ID       string   `json:"id"`
	Title    string   `json:"title"`
	Question string   `json:"question"`
	Steps    []string `json:"steps"`
	Goal     string   `json:"goal"`
	Reveal   string   `json:"reveal"`
	CLI      []string `json:"cli"`
	Confirm  []string `json:"confirm"`
	check    func(LessonInput) (bool, string)
}

type LessonStatus struct {
	ID     string `json:"id"`
	Done   bool   `json:"done"`
	Detail string `json:"detail"`
}

func (m *Manager) checkLessons(in LessonInput) []LessonStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]LessonStatus, 0, len(Lessons))
	for _, l := range Lessons {
		ok, detail := l.check(in)
		// A lesson stays done once passed: lesson 7 asks for a broker the learner restarts afterwards.
		if ok {
			m.done[l.ID] = true
		}
		out = append(out, LessonStatus{ID: l.ID, Done: m.done[l.ID], Detail: detail})
	}
	return out
}

func totalRecords(tv *TopicView) int64 {
	var n int64
	for _, p := range tv.Partitions {
		if p.Start >= 0 && p.End >= 0 {
			n += p.End - p.Start
		}
	}
	return n
}

func lessonConsumers(in LessonInput) []ConsumerView {
	var out []ConsumerView
	for _, c := range in.Consumers {
		if c.Topic == LessonTopic {
			out = append(out, c)
		}
	}
	return out
}

func brokersDown(bs []BrokerView) int {
	n := 0
	for _, b := range bs {
		if b.State != "" && b.State != "running" {
			n++
		}
	}
	return n
}

var cliTopicDescribe = Cli("kafka-topics.sh", "--describe --topic "+LessonTopic)

var Lessons = []Lesson{
	{
		ID: "1", Title: "Topic, partition, offset",
		Question: "Tạo topic orders có 3 partition rồi gửi 6 record với key k{i} (thành k1 … k6). Mỗi record nằm ở cả 3 partition hay chỉ một? " +
			"Offset của chúng là 0 tới 5 cho cả topic, hay mỗi partition đếm riêng? Sau đó gửi 6 record không có key: chúng được rải ra hay dồn lại?",
		Steps: []string{
			"Ô Topic: tên orders, 3 partition, RF 3, min ISR 2, rồi bấm Tạo.",
			"Ô Producer: tạo producer p1 (để mặc định acks=all).",
			"Gửi 6 record với key k{i} (số lượng 6): lab thay {i} bằng 1…6. Nhìn các làn partition: mỗi ô là một record, số trong ô là offset.",
			"Gửi tiếp 6 record, để trống key.",
		},
		Goal: "orders có 3 partition, ít nhất 6 record, và record nằm ở ít nhất 2 partition",
		check: func(in LessonInput) (bool, string) {
			if in.Topic == nil {
				return false, "chưa có topic orders"
			}
			used := 0
			for _, p := range in.Topic.Partitions {
				if p.End > p.Start {
					used++
				}
			}
			n := totalRecords(in.Topic)
			return len(in.Topic.Partitions) == 3 && n >= 6 && used >= 2,
				fmt.Sprintf("orders: %d partition, %d record, %d partition có dữ liệu", len(in.Topic.Partitions), n, used)
		},
		Reveal: `<p><b>Topic</b> chỉ là một cái tên. Dữ liệu thật nằm trong các <b>partition</b>: mỗi partition là một log chỉ ghi nối thêm vào
cuối (append-only), lưu trên đĩa của broker. Mỗi record vào <b>đúng một</b> partition.</p>
<p><b>Offset</b> là vị trí của record trong partition của nó, bắt đầu từ 0, và mỗi partition đếm riêng. Vì vậy nói "orders offset 3" là
chưa đủ; phải nói "orders-1 offset 3". Không có thứ tự nào giữa các partition với nhau.</p>
<p>Record <b>không có key</b> không được rải đều từng cái một. Producer dính vào một partition rồi mới đổi: Java client (từ Kafka 3.3)
đổi sau khi đã gửi khoảng <code>batch.size</code> (16 KB) vào partition đó. Lab đo với franz-go: 16 record nhỏ không key, gửi lần lượt
hay gửi một lượt, đều vào cùng một partition. Muốn biết record nằm đâu thì đừng đoán: mỗi lần gửi đều trả về partition và offset.</p>
<p>Đọc không làm mất record. Record chỉ bị xoá theo retention của topic (mặc định 7 ngày), nên log có điểm đầu (start offset) có thể lớn hơn 0.</p>`,
		CLI: []string{
			Cli("kafka-topics.sh", "--create --topic orders --partitions 3 --replication-factor 3 --config min.insync.replicas=2"),
			cliTopicDescribe,
			Cli("kafka-get-offsets.sh", "--topic orders"),
		},
		Confirm: []string{
			"Vì sao Kafka không có một offset chung cho cả topic?",
			"Muốn đọc lại record thứ 3 bạn vừa gửi, bạn cần biết những thông tin nào?",
		},
	},
	{
		ID: "2", Title: "Key quyết định partition",
		Question: "Gửi 5 record cùng key xe-1, rồi 5 record với key xe-{i} (thành xe-1 … xe-5). Record cùng key vào cùng partition hay được rải đều? " +
			"Nếu topic tăng lên 4 partition, key xe-1 còn nằm ở partition cũ không?",
		Steps: []string{
			"Gửi 5 record với key xe-1.",
			"Gửi 5 record với key xe-{i}: lab thay {i} bằng 1…5.",
			"So sánh partition của từng key ở danh sách “đã gửi” của producer và trên các làn partition.",
		},
		Goal: "một key được gửi ít nhất 3 lần, luôn vào cùng một partition; và các key khác nhau rơi vào ít nhất 2 partition",
		check: func(in LessonInput) (bool, string) {
			byKey := map[string]map[int32]int{}
			for _, s := range in.Facts.Sends {
				if s.Topic != LessonTopic || s.Err != "" || s.Key == "" {
					continue
				}
				if byKey[s.Key] == nil {
					byKey[s.Key] = map[int32]int{}
				}
				byKey[s.Key][s.Partition]++
			}
			stable, parts := false, map[int32]bool{}
			var notes []string
			for k, ps := range byKey {
				n := 0
				for p, c := range ps {
					n += c
					parts[p] = true
				}
				if len(ps) == 1 && n >= 3 {
					stable = true
				}
				notes = append(notes, fmt.Sprintf("%s→P%v", k, keysOf(ps)))
			}
			sort.Strings(notes)
			if len(notes) == 0 {
				return false, "chưa gửi record nào có key vào orders"
			}
			return stable && len(parts) >= 2, strings.Join(notes, ", ")
		},
		Reveal: `<p>Partition = <code>murmur2(key) mod số partition</code>. franz-go (client của lab) dùng đúng thuật toán của Java client,
nên cùng một key cho cùng một partition ở cả hai.</p>
<p>Cùng key thì cùng partition, nên <b>thứ tự giữa các record của một key được giữ</b>. Đây là lý do service gap dùng
<code>permanentId</code> làm key: mọi phép đo của một sensor được xử lý theo đúng thứ tự.</p>
<p>Đổi số partition thì phép <code>mod</code> đổi, nên phần lớn key chuyển sang partition khác. Record mới của key đó có thể được đọc
trước record cũ còn nằm ở partition cũ. Vì vậy số partition nên chọn đủ từ đầu; Kafka cho tăng nhưng không cho giảm.</p>
<p>Mặt trái: một key gửi quá nhiều (hot key) dồn hết vào một partition, và chỉ một consumer trong group được đọc partition đó.</p>`,
		CLI: []string{
			"docker exec -it lab-kafka-1 /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka-1:9092 --topic orders --property parse.key=true --property key.separator=:   # rồi gõ  xe-1:hello",
			Cli("kafka-console-consumer.sh", "--topic orders --from-beginning --property print.key=true --property print.partition=true --property print.offset=true --timeout-ms 5000"),
		},
		Confirm: []string{
			"Bạn có 1 triệu sensor và 3 partition. Vì sao thứ tự toàn cục giữa các sensor không được đảm bảo, và điều đó có thành vấn đề không?",
			"Một sensor gửi gấp 100 lần các sensor khác. Chuyện gì xảy ra với consumer group?",
		},
	},
	{
		ID: "3", Title: "Consumer đọc từ đâu",
		Question: "Tạo consumer c1 trong group g1 với auto.offset.reset=earliest, rồi bấm Poll vài lần. c1 nhận record theo thứ tự nào: " +
			"theo thời điểm gửi trên cả topic, hay theo offset trong từng partition? Nếu chọn latest thì c1 nhận gì?",
		Steps: []string{
			"Ô Consumer: tên c1, group g1, topic orders, earliest, rồi bấm Tạo.",
			"Bấm Poll. Lần đầu có thể mất vài giây vì c1 phải vào group và được chia partition.",
			"Bấm Poll thêm vài lần. Để ý cột P và offset của từng record nhận được.",
		},
		Goal: "một consumer của orders nhận được record từ ít nhất 2 partition",
		check: func(in LessonInput) (bool, string) {
			best := 0
			var detail string
			for _, c := range lessonConsumers(in) {
				parts := map[int32]int{}
				for _, r := range c.Received {
					parts[r.Partition]++
				}
				if len(parts) > best {
					best, detail = len(parts), fmt.Sprintf("%s nhận từ partition %v", c.Name, keysOf(parts))
				}
			}
			if detail == "" {
				detail = "chưa consumer nào của orders nhận được record"
			}
			return best >= 2, detail
		},
		Reveal: `<p>Poll gửi fetch request tới <b>leader</b> của từng partition được gán cho consumer, rồi trả về record theo offset tăng dần
<i>trong từng partition</i>. Record của các partition khác nhau đến xen kẽ nhau tuỳ thời điểm fetch: không có thứ tự chung.</p>
<p>Group mới chưa có <b>committed offset</b>, nên <code>auto.offset.reset</code> quyết định chỗ bắt đầu: <code>earliest</code> đọc từ
đầu log, <code>latest</code> chỉ đọc record đến sau khi consumer được gán partition. Mặc định của Java là <code>latest</code>: một
service mới triển khai sẽ bỏ qua toàn bộ dữ liệu cũ, trừ khi bạn đổi cấu hình này.</p>
<p>Vị trí đọc hiện tại (position) chỉ nằm trong bộ nhớ của consumer. Nó chưa được lưu ở đâu cả, cho tới khi consumer commit: đó là bài sau.</p>`,
		CLI: []string{
			Cli("kafka-console-consumer.sh", "--topic orders --group g1 --from-beginning --property print.partition=true --property print.offset=true --timeout-ms 5000") +
				"   # chú ý: console consumer tự commit (enable.auto.commit=true)",
		},
		Confirm: []string{
			"Vì sao auto.offset.reset chỉ có tác dụng với group chưa từng commit?",
			"Service mới deploy với latest, nhưng producer đã gửi dữ liệu từ hôm qua. Dữ liệu đó đi đâu?",
		},
	},
	{
		ID: "4", Title: "Commit offset",
		Question: "Poll vài record nhưng KHÔNG commit, rồi xoá c1 và tạo lại c1 cùng group g1. Poll lại: c1 nhận record mới hay những record đã nhận? " +
			"Sau đó Poll rồi Commit, xoá và tạo lại lần nữa: lần này thì sao?",
		Steps: []string{
			"Gửi thêm vài record vào orders nếu cần. Poll c1 một lần, không bấm Commit.",
			"Xoá c1, tạo lại c1 (group g1, earliest), rồi Poll. Record đọc lại được đánh dấu “đọc lại”.",
			"Poll rồi bấm Commit. Nhìn cờ g1 trên các làn partition: nó chỉ vào offset nào?",
			"Xoá và tạo lại c1 lần nữa, rồi Poll.",
		},
		Goal: "thấy record bị đọc lại trong orders, và commit được ít nhất một lần",
		check: func(in LessonInput) (bool, string) {
			re, cm := in.Facts.Has(LessonTopic, factRedelivered), in.Facts.Has(LessonTopic, factCommitted)
			return re && cm, fmt.Sprintf("đọc lại: %s · đã commit: %s", yes(re), yes(cm))
		},
		Reveal: `<p><b>Committed offset</b> là câu "group này đã xử lý xong tới đây", lưu trên broker trong topic nội bộ
<code>__consumer_offsets</code>. Nó là offset của record <i>kế tiếp</i> sẽ đọc, không phải của record cuối đã đọc. Consumer mới vào group
bắt đầu đọc từ committed offset.</p>
<p>Chưa commit mà consumer biến mất thì record được đọc lại: đó là <b>at-least-once</b>. Commit <i>trước</i> khi xử lý xong thì
ngược lại: chết giữa chừng là mất record (<b>at-most-once</b>). Service gap chọn commit sau khi ghi DB (<code>AckMode.MANUAL</code>),
và dùng khoá chính của bảng để bản ghi trùng không gây hại.</p>
<p><b>Lag</b> = end offset − committed offset: số record đã nằm trên broker mà group chưa xác nhận xử lý.</p>`,
		CLI: []string{
			Cli("kafka-consumer-groups.sh", "--describe --group g1"),
			Cli("kafka-consumer-groups.sh", "--group g1 --topic orders --reset-offsets --to-earliest --execute") + "   # chỉ chạy được khi group không còn member",
		},
		Confirm: []string{
			"Consumer poll 10 record, xử lý xong 6 thì chết. Sau khi khởi động lại, 4 record kia và 6 record đã xử lý thì sao?",
			"Vì sao committed offset là offset kế tiếp chứ không phải offset vừa xử lý?",
		},
	},
	{
		ID: "5", Title: "Consumer group và rebalance",
		Question: "Thêm c2, rồi c3, rồi c4 vào cùng group g1 (topic có 3 partition). Mỗi lần thêm, partition được chia lại thế nào? " +
			"c4 làm gì? Xoá c2 thì partition của nó đi đâu?",
		Steps: []string{
			"Tạo c2 (group g1, orders). Đợi vài giây, nhìn cột “partition” của từng consumer.",
			"Tạo c3, rồi c4, cùng group g1.",
			"Xoá c2 và nhìn partition của nó chuyển sang ai.",
		},
		Goal: "trong một group, số consumer nhiều hơn số partition: mọi partition đều có chủ và có consumer không được gán partition nào",
		check: func(in LessonInput) (bool, string) {
			if in.Topic == nil {
				return false, "chưa có topic orders"
			}
			byGroup := map[string][]ConsumerView{}
			for _, c := range lessonConsumers(in) {
				byGroup[c.Group] = append(byGroup[c.Group], c)
			}
			n := len(in.Topic.Partitions)
			best := "chưa có group nào có nhiều consumer hơn số partition"
			for g, cs := range byGroup {
				covered, idle := map[int32]bool{}, 0
				for _, c := range cs {
					if len(c.Assigned) == 0 {
						idle++
					}
					for _, p := range c.Assigned {
						covered[p] = true
					}
				}
				if len(cs) > n && len(covered) == n && idle > 0 {
					return true, fmt.Sprintf("%s: %d consumer, %d partition, %d consumer ngồi chơi", g, len(cs), n, idle)
				}
				if len(cs) > 1 {
					best = fmt.Sprintf("%s: %d consumer, %d/%d partition có chủ, %d chưa có partition", g, len(cs), len(covered), n, idle)
				}
			}
			return false, best
		},
		Reveal: `<p>Trong một group, mỗi partition chỉ thuộc về <b>một</b> consumer tại một thời điểm. Vì vậy số consumer có ích tối đa bằng số
partition: c4 vẫn là member, vẫn gửi heartbeat, nhưng không đọc gì. Nó chỉ có ích như hàng dự bị khi một member khác rời đi.</p>
<p>Mỗi lần member vào hoặc rời group là một <b>rebalance</b>: coordinator (một broker) gom danh sách member, một member chạy
<i>assignor</i> để chia partition. Lab dùng <code>range</code>, giống phần tử đầu tiên trong danh sách mặc định của Java. Với các assignor
kiểu eager như range, mọi member phải nhả <i>toàn bộ</i> partition trong lúc chia lại, nên cả group ngừng đọc một lúc;
<code>cooperative-sticky</code> chỉ di chuyển những partition cần chuyển.</p>
<p>Partition chuyển sang consumer khác thì consumer mới bắt đầu từ <b>committed offset</b>. Record mà chủ cũ đã poll nhưng chưa commit
sẽ được đọc lại ở chủ mới.</p>
<p>Ở Java còn một luật nữa: consumer không gọi <code>poll()</code> trong <code>max.poll.interval.ms</code> (mặc định 5 phút) sẽ tự rời group.
Lab không mô phỏng luật đó.</p>`,
		CLI: []string{Cli("kafka-consumer-groups.sh", "--describe --group g1 --members --verbose")},
		Confirm: []string{
			"Topic có 3 partition, service chạy 5 pod. Thêm pod thứ 6 có làm xử lý nhanh hơn không?",
			"Vì sao một rebalance có thể làm record bị xử lý hai lần?",
		},
	},
	{
		ID: "6", Title: "Nhiều group đọc cùng topic",
		Question: "Tạo consumer d1 trong group mới g2 (earliest) rồi Poll. d1 có nhận lại những record g1 đã đọc và commit không? " +
			"Commit của g1 có ảnh hưởng gì tới g2 không?",
		Steps: []string{
			"Tạo d1: group g2, topic orders, earliest.",
			"Poll d1 vài lần rồi Commit.",
			"Nhìn hai cờ g1 và g2 trên các làn partition.",
		},
		Goal: "ít nhất 2 group khác nhau đã đọc được record của orders",
		check: func(in LessonInput) (bool, string) {
			gs := in.Facts.GroupsThatRead(LessonTopic)
			if len(gs) == 0 {
				return false, "chưa group nào đọc orders"
			}
			return len(gs) >= 2, "group đã đọc orders: " + strings.Join(gs, ", ")
		},
		Reveal: `<p>Mỗi group có committed offset riêng: nó là một <b>con trỏ đọc</b> riêng trên cùng một log. g2 đọc lại từ đầu dù g1 đã đọc xong,
và commit của nhóm này không đụng tới nhóm kia.</p>
<p>Đây là điểm khác lớn nhất so với message queue kiểu RabbitMQ, nơi message bị lấy khỏi queue khi được ack. Ở Kafka, đọc không
xoá gì: một log phục vụ được nhiều hệ thống độc lập (ghi DB, tính thống kê, đồng bộ sang hệ khác), mỗi hệ một group.
Service gap cũng làm vậy: group <code>gap-lab-tracking</code> đọc cùng topic với group chính.</p>
<p>Trong <i>một</i> group, record được chia cho các member (mỗi record một người đọc). Giữa <i>các</i> group, mỗi group nhận đủ mọi record.</p>`,
		CLI: []string{
			Cli("kafka-consumer-groups.sh", "--list"),
			Cli("kafka-consumer-groups.sh", "--describe --group g2"),
		},
		Confirm: []string{
			"Bạn muốn chạy thử phiên bản mới của service trên dữ liệu thật mà không làm phiền bản đang chạy. Dùng group nào?",
			"Hai pod của cùng một service lỡ cấu hình hai group.id khác nhau. Chuyện gì xảy ra với dữ liệu trong DB?",
		},
	},
	{
		ID: "7", Title: "Replica, leader và ISR",
		Question: "orders có RF 3: mỗi partition có 3 bản sao trên 3 broker, một bản là leader. Kill broker đang là leader của partition 0. " +
			"Leader và ISR thay đổi thế nào, sau bao lâu? Gửi record với acks=all ngay sau đó có thành công không?",
		Steps: []string{
			"Nhìn làn partition 0: broker nào là leader (L), ISR gồm những broker nào.",
			"Ở hàng Broker, bấm kill trên broker đó.",
			"Gửi vài record bằng p1 (acks=all). Theo dõi leader và ISR của từng partition trong khoảng 15 giây.",
			"Bấm start để bật lại broker và xem nó quay lại ISR.",
			"Lệnh CLI của lab chạy trong lab-kafka-1. Nếu broker 1 đang tắt, đổi thành lab-kafka-2 và kafka-2:9092.",
		},
		Goal: "ISR của orders bị co lại khi một broker chết, và một lần gửi acks=all vẫn thành công trong lúc đó",
		check: func(in LessonInput) (bool, string) {
			shr, sent := in.Facts.Has(LessonTopic, factIsrShrunk), in.Facts.Has(LessonTopic, factAcksAllWithBrokerDown)
			return shr && sent, fmt.Sprintf("broker đang tắt: %d · ISR từng co lại: %s · gửi acks=all thành công khi có broker tắt: %s",
				brokersDown(in.Brokers), yes(shr), yes(sent))
		},
		Reveal: `<p>Producer và consumer chỉ nói chuyện với <b>leader</b> của partition. Các follower liên tục kéo dữ liệu từ leader; follower nào
theo kịp thì nằm trong <b>ISR</b> (in-sync replicas).</p>
<p>Khi broker chết, controller (KRaft) phải biết trước đã. Broker bị <code>kill</code> không kịp báo gì, nên controller chỉ phát hiện khi
hết <code>broker.session.timeout.ms</code> (mặc định 9 s): lab đo được leader chuyển sau 8,7 s, và trong khoảng đó partition vẫn trỏ về
leader đã chết. Broker tắt có trật tự (<code>docker stop</code>) tự báo trước và nhường leader: lab đo được khoảng 1 s. Leader mới luôn được
chọn trong ISR.</p>
<p>Với <code>acks=all</code>, leader chỉ xác nhận khi mọi replica <i>trong ISR</i> đã có record. Ngay sau khi một follower bị kill, nó vẫn
còn trong ISR, nên leader phải đợi nó cho tới khi ISR co lại: lab đo được các lần gửi bị treo 6,8–8,4 s. Sau đó ISR còn 2, vẫn ≥
<code>min.insync.replicas=2</code>, nên ghi lại nhanh như thường. Để ý cột "đã gửi" của producer: thời gian của mỗi lần gửi nằm ở đó.</p>
<p>Broker sống lại thì follower đuổi kịp rồi vào lại ISR. Leader không chuyển về ngay: Kafka đưa leader về replica "ưa thích" (đứng đầu danh
sách replica) theo chu kỳ, mặc định 5 phút (<code>leader.imbalance.check.interval.seconds</code>).</p>`,
		CLI: []string{cliTopicDescribe + "   # cột Leader và Isr", "docker kill lab-kafka-2", "docker start lab-kafka-2"},
		Confirm: []string{
			"Vì sao Kafka chỉ chọn leader mới trong ISR, không chọn một replica bất kỳ còn sống?",
			"Broker bị kill và broker bị docker stop khác nhau thế nào với producer đang gửi?",
		},
	},
	{
		ID: "8", Title: "acks và min.insync.replicas",
		Question: "Kill thêm một broker nữa, chỉ còn 1 broker sống. Gửi bằng p1 (acks=all) thì chuyện gì xảy ra? Tạo p2 với acks=1 (tắt idempotence) " +
			"rồi gửi: có thành công không? Cái giá của acks=1 là gì?",
		Steps: []string{
			"Kill thêm một broker để chỉ còn 1 broker chạy. Đợi khoảng 10 giây cho leader chuyển xong.",
			"Gửi 1 record bằng p1 (acks=all). Lần gửi sẽ treo tới hết delivery.timeout.ms (10 giây ở lab).",
			"Tạo p2: acks=1, bỏ chọn idempotence. Gửi 1 record bằng p2.",
			"Start lại cả hai broker.",
		},
		Goal: "khi 2 broker tắt: acks=all bị từ chối và acks=1 thành công",
		check: func(in LessonInput) (bool, string) {
			rej, acc := in.Facts.Has(LessonTopic, factAcksAllRejected), in.Facts.Has(LessonTopic, factAcksOneAccepted)
			var lastErr string
			for _, s := range slices.Backward(in.Facts.Sends) {
				if s.Topic == LessonTopic && s.Err != "" {
					lastErr = " · lỗi gần nhất: " + s.Err
					break
				}
			}
			return rej && acc, fmt.Sprintf("broker đang tắt: %d · acks=all bị từ chối: %s · acks=1 thành công: %s%s",
				brokersDown(in.Brokers), yes(rej), yes(acc), lastErr)
		},
		Reveal: `<p><code>min.insync.replicas=2</code> nghĩa là: với <code>acks=all</code>, leader <b>từ chối ghi</b> khi ISR còn ít hơn 2. Log của broker
ghi rõ: <code>NotEnoughReplicasException: The size of the current ISR Set(1) is insufficient to satisfy the min.isr requirement of 2</code>.
Nhưng lỗi bạn nhận được lại là timeout ("records have timed out…"): producer coi <code>NOT_ENOUGH_REPLICAS</code> là lỗi tạm thời, thử lại
liên tục (lab đếm được 75 lần trong log broker) cho tới hết <code>delivery.timeout.ms</code> (10 s ở lab, Java mặc định 120 s), rồi mới báo
lỗi cho code. Java client cũng báo <code>TimeoutException</code> trong trường hợp này. Khi điều tra sự cố, lý do thật nằm ở log broker và
ở ISR, không nằm ở thông báo lỗi phía producer.</p>
<p><code>acks=1</code> không quan tâm ISR: leader ghi vào log của chính nó là xác nhận (lab đo được 272 ms, trong khi acks=all đợi 10 s
rồi thất bại). Ghi được, nhưng nếu leader chết trước khi
follower kịp sao chép, record <i>đã được xác nhận</i> sẽ mất. <code>acks=0</code> còn không chờ leader trả lời.</p>
<p>Đây là đánh đổi giữa sẵn sàng (vẫn ghi được khi cluster yếu) và bền vững (đã xác nhận là không mất). Cấu hình phổ biến cho dữ liệu
quan trọng: RF 3, <code>min.insync.replicas=2</code>, <code>acks=all</code>: chịu được 1 broker chết mà vẫn ghi, mất 2 broker thì
thà dừng ghi còn hơn mất dữ liệu.</p>
<p>Idempotence (<code>enable.idempotence=true</code>, mặc định từ Java client 3.0) đòi hỏi <code>acks=all</code>, nên muốn dùng
<code>acks=1</code> phải tắt nó.</p>`,
		CLI: []string{
			"docker kill lab-kafka-3",
			"docker exec -it lab-kafka-1 /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka-1:9092 --topic orders --producer-property acks=1",
			cliTopicDescribe,
		},
		Confirm: []string{
			"RF 3 và min.insync.replicas=3. Một broker vào bảo trì thì chuyện gì xảy ra với producer acks=all?",
			"Producer acks=1 nhận ack xong thì leader chết. Record đó còn không? Phụ thuộc vào điều gì?",
		},
	},
}

func keysOf[V any](m map[int32]V) []int32 {
	out := make([]int32, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

func yes(b bool) string {
	if b {
		return "có"
	}
	return "chưa"
}

// Cli is the kafka tool command a learner can run against the lab unchanged.
func Cli(tool, args string) string {
	return "docker exec lab-kafka-1 /opt/kafka/bin/" + tool + " --bootstrap-server kafka-1:9092 " + args
}

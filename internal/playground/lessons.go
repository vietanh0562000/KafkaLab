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
			"Bấm “Tạo topic”. Giá trị mặc định đã đúng: tên orders, 3 partition, replication factor 3, min.insync.replicas 2.",
			"Ở cột Producer, bấm “Tạo producer” để tạo p1 (giữ mặc định acks=all).",
			"Gửi 6 record với key k{i} (số lượng 6): lab thay {i} bằng 1…6. Nhìn các hàng P0, P1, P2: mỗi ô là một record, số nhỏ ở góc ô là offset.",
			"Gửi tiếp 6 record, để trống key.",
		},
		Goal: "orders có 3 partition, RF 3, min ISR 2, ít nhất 6 record, và record nằm ở ít nhất 2 partition",
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
			n, rf := totalRecords(in.Topic), 0
			if len(in.Topic.Partitions) > 0 {
				rf = len(in.Topic.Partitions[0].Replicas)
			}
			detail := fmt.Sprintf("orders: %d partition, RF %d, min ISR %d, %d record, %d partition có dữ liệu",
				len(in.Topic.Partitions), rf, in.Topic.MinISR, n, used)
			// Lessons 7 and 8 kill brokers and rely on RF 3 with min ISR 2.
			if rf != 3 || in.Topic.MinISR != 2 {
				return false, detail + " · bài 7 và 8 cần RF 3, min ISR 2: xoá orders rồi tạo lại"
			}
			return len(in.Topic.Partitions) == 3 && n >= 6 && used >= 2, detail
		},
		Reveal: `<p><b>Topic</b> chỉ là một cái tên. Dữ liệu thật nằm trong các <b>partition</b>: mỗi partition là một log chỉ ghi nối thêm vào
cuối (append-only), lưu trên đĩa của broker. Mỗi record vào <b>đúng một</b> partition.</p>
<p><b>Offset</b> là vị trí của record trong partition của nó, bắt đầu từ 0, và mỗi partition đếm riêng. Vì vậy nói "orders offset 3" là
chưa đủ; phải nói "orders-1 offset 3". Giữa các partition không có thứ tự chung.</p>
<p>Record <b>không có key</b> không được rải đều từng cái một. Producer bám một partition một thời gian rồi mới đổi (sticky partitioning):
Java client (từ Kafka 3.3) đổi sau khi đã gửi khoảng <code>batch.size</code> (16 KB) vào partition đó. Lab đo được với franz-go: 16 record
nhỏ không key, gửi lần lượt hay gửi một lượt, đều vào cùng một partition. Muốn biết record nằm đâu thì đừng đoán: lần gửi nào cũng trả về
partition và offset.</p>
<p>Đọc không làm mất record. Record chỉ bị xoá theo retention của topic (mặc định 7 ngày), nên điểm đầu của log (start offset) có thể lớn hơn 0.</p>`,
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
			"So sánh partition của từng key ở danh sách lần gửi dưới producer và trên các hàng partition. Ô cùng màu sọc là cùng key.",
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
nên một key rơi vào cùng một partition dù gửi bằng client nào.</p>
<p>Cùng key thì cùng partition, nên <b>thứ tự giữa các record của một key được giữ</b>. Đây là lý do service gap dùng
<code>permanentId</code> làm key: mọi phép đo của một sensor được xử lý theo đúng thứ tự.</p>
<p>Đổi số partition thì phép <code>mod</code> đổi, nên phần lớn key chuyển sang partition khác. Record mới của key đó có thể được đọc
trước record cũ còn nằm ở partition cũ. Vì vậy nên chọn đủ số partition ngay từ đầu; Kafka cho tăng nhưng không cho giảm.</p>
<p>Mặt trái: một key gửi quá nhiều (hot key) dồn hết vào một partition, và chỉ một consumer trong group được đọc partition đó.</p>`,
		CLI: []string{
			"docker exec -it lab-kafka-1 /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka-1:9092 --topic orders --property parse.key=true --property key.separator=:   # rồi gõ  xe-1:hello",
			Cli("kafka-console-consumer.sh", "--topic orders --from-beginning --property print.key=true --property print.partition=true --property print.offset=true --timeout-ms 5000"),
			Cli("kafka-topics.sh", "--alter --topic orders --partitions 4") + "   # rồi gửi lại key xe-1 xem nó vào partition nào",
		},
		Confirm: []string{
			"Bạn có 1 triệu sensor và 3 partition. Vì sao thứ tự toàn cục giữa các sensor không được đảm bảo, và khi nào điều đó thành vấn đề?",
			"Một sensor gửi gấp 100 lần các sensor khác. Chuyện gì xảy ra với consumer group?",
		},
	},
	{
		ID: "3", Title: "Consumer đọc từ đâu",
		Question: "Tạo consumer c1 trong group g1 với auto.offset.reset=earliest, rồi bấm Poll vài lần. c1 nhận record theo thứ tự nào: " +
			"theo thời điểm gửi trên cả topic, hay theo offset trong từng partition? Nếu chọn latest thì c1 nhận gì?",
		Steps: []string{
			"Ở cột Consumer, tạo c1: group g1, topic orders, earliest.",
			"Bấm Poll. Nhìn các chip P0/P1/P2 trên thẻ c1: đó là partition c1 được gán.",
			"Bấm Poll thêm vài lần. Để ý partition và offset của từng record nhận được (dạng P1@3).",
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
		Reveal: `<p>Consumer gửi fetch request tới <b>leader</b> của từng partition được gán cho nó; cả Java client lẫn franz-go đều fetch sẵn
ở nền, còn poll lấy những record đã về. Record trả ra theo offset tăng dần <i>trong từng partition</i>. Record của các partition khác nhau
đến xen kẽ nhau tuỳ thời điểm fetch: không có thứ tự chung.</p>
<p>Điểm bắt đầu đọc được chọn <b>theo từng partition</b>. Có committed offset thì đọc từ đó. Chưa có, hoặc offset đó đã bị retention xoá
khỏi log, thì <code>auto.offset.reset</code> quyết định: <code>earliest</code> đọc từ đầu log, <code>latest</code> chỉ đọc record đến sau.
Java mặc định <code>latest</code>, nên một service mới triển khai với group chưa từng commit sẽ bỏ qua toàn bộ dữ liệu cũ, trừ khi bạn đổi
cấu hình này.</p>
<p>Vị trí đọc hiện tại (position) chỉ nằm trong bộ nhớ của consumer. Nó chưa được lưu ở đâu cả, cho tới khi consumer commit: đó là chuyện
của bài sau.</p>`,
		CLI: []string{
			Cli("kafka-console-consumer.sh", "--topic orders --group g1 --from-beginning --property print.partition=true --property print.offset=true --timeout-ms 5000") +
				"   # chú ý: console consumer tự commit (enable.auto.commit=true)",
		},
		Confirm: []string{
			"Group g1 đã commit P0 nhưng chưa commit P1. Consumer mới của g1 với latest sẽ bắt đầu đọc P0 và P1 từ đâu?",
			"Service mới deploy với latest, nhưng producer đã gửi dữ liệu từ hôm qua. Dữ liệu đó đi đâu?",
		},
	},
	{
		ID: "4", Title: "Commit offset",
		Question: "Poll vài record nhưng không bấm Commit, rồi xoá c1 và tạo lại c1 trong cùng group g1. Poll lại: c1 nhận record mới hay nhận lại những record cũ? " +
			"Sau đó Poll rồi Commit, xoá và tạo lại lần nữa: lần này thì sao?",
		Steps: []string{
			"Gửi thêm vài record vào orders nếu cần. Poll c1 một lần, không bấm Commit.",
			"Xoá c1, tạo lại c1 (group g1, earliest), rồi Poll. Record đọc lại được đánh dấu “đọc lại”.",
			"Poll rồi bấm Commit. Nhìn nhãn tím g1 dưới các hàng partition: nó nằm ở offset nào?",
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
			"Tạo c2 (group g1, orders). Đợi vài giây, nhìn các chip P0/P1/P2 trên thẻ của từng consumer.",
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
<p>Mỗi lần có member vào hoặc rời group là một lần <b>rebalance</b>: coordinator (một broker) gom danh sách member, một member chạy
<i>assignor</i> để chia partition. Lab dùng <code>range</code>, giống assignor đứng đầu danh sách mặc định của Java
(<code>partition.assignment.strategy</code>). Với các assignor
kiểu eager như range, mọi member phải nhả <i>toàn bộ</i> partition trong lúc chia lại, nên cả group ngừng đọc một lúc;
<code>cooperative-sticky</code> chỉ di chuyển những partition cần chuyển.</p>
<p>Partition chuyển sang consumer khác thì consumer mới bắt đầu từ <b>committed offset</b>. Record mà chủ cũ đã poll nhưng chưa commit
sẽ được đọc lại ở chủ mới.</p>
<p>Consumer của lab (franz-go) vào group ngay khi được tạo, nên bạn thấy partition được chia mà chưa cần Poll. <code>KafkaConsumer</code>
của Java chỉ vào group bên trong <code>poll()</code>; Spring gọi poll ngay khi khởi động, nên nhìn từ ngoài thì giống nhau. Lab cũng đặt
<code>group.initial.rebalance.delay.ms=0</code>; mặc định broker chờ 3 s trước lần chia đầu tiên của một group mới để gom member.</p>
<p>Java còn một luật nữa: consumer nào không gọi <code>poll()</code> trong <code>max.poll.interval.ms</code> (mặc định 5 phút) sẽ tự rời group.
Lab không mô phỏng luật đó.</p>`,
		CLI: []string{Cli("kafka-consumer-groups.sh", "--describe --group g1 --members --verbose")},
		Confirm: []string{
			"Topic có 3 partition, service đang chạy 5 pod. Pod thứ 6 sẽ làm gì, và lúc nào nó mới có ích?",
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
			"Nhìn hai nhãn tím g1 và g2 dưới các hàng partition.",
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
và commit của group này không đụng tới group kia.</p>
<p>Đây là chỗ Kafka khác hẳn message queue kiểu RabbitMQ, nơi message bị lấy khỏi queue khi được ack. Ở Kafka, đọc không
xoá gì: một log phục vụ được nhiều hệ thống độc lập (ghi DB, tính thống kê, đồng bộ sang hệ khác), mỗi hệ một group.
Service gap cũng làm vậy: group <code>gap-lab-tracking</code> đọc cùng topic với group chính.</p>
<p>Trong <i>một</i> group, record được chia cho các member (mỗi record chỉ một member đọc). Giữa <i>các</i> group, mỗi group nhận đủ mọi record.</p>`,
		CLI: []string{
			Cli("kafka-consumer-groups.sh", "--list"),
			Cli("kafka-consumer-groups.sh", "--describe --group g2"),
		},
		Confirm: []string{
			"Bạn muốn chạy thử phiên bản mới của service trên dữ liệu thật mà không làm phiền bản đang chạy. Dùng group nào, và group đó nên bắt đầu đọc từ đâu?",
			"Hai pod của cùng một service lỡ cấu hình hai group.id khác nhau. Chuyện gì xảy ra với dữ liệu trong DB?",
		},
	},
	{
		ID: "7", Title: "Replica, leader và ISR",
		Question: "orders có RF 3: mỗi partition có 3 bản sao (replica) trên 3 broker, một replica là leader. Kill broker đang là leader của partition 0. " +
			"Leader và ISR thay đổi thế nào, sau bao lâu? Gửi record với acks=all ngay sau đó có thành công không?",
		Steps: []string{
			"Nhìn hàng P0: ô replica tô đậm là leader, các ô viền xanh là replica trong ISR.",
			"Trong khung Cluster, bấm “Kill” trên broker đó. Nếu đó là broker 1, các lệnh CLI bên dưới phải đổi lab-kafka-1 và kafka-1:9092 " +
				"thành lab-kafka-2 và kafka-2:9092.",
			"Gửi vài record bằng p1 (acks=all). Thời gian chờ ack của mỗi lần gửi hiện ở cuối dòng, trong danh sách lần gửi dưới producer. " +
				"Theo dõi leader và ISR của từng partition trong khoảng 15 giây.",
			"Bấm “Start” để bật lại broker và xem nó quay lại ISR.",
		},
		Goal: "ISR của orders bị co lại khi một broker chết, và một lần gửi acks=all vẫn thành công trong lúc đó",
		check: func(in LessonInput) (bool, string) {
			shr, sent := in.Facts.Has(LessonTopic, factIsrShrunk), in.Facts.Has(LessonTopic, factAcksAllWithBrokerDown)
			return shr && sent, fmt.Sprintf("broker đang tắt: %d · ISR từng co lại: %s · gửi acks=all thành công khi có broker tắt: %s",
				brokersDown(in.Brokers), yes(shr), yes(sent))
		},
		Reveal: `<p>Mặc định, producer và consumer chỉ nói chuyện với <b>leader</b> của partition. Các follower liên tục kéo dữ liệu từ leader; follower nào
theo kịp thì nằm trong <b>ISR</b> (in-sync replicas).</p>
<p>Muốn chuyển leader, controller (KRaft) phải biết broker đã chết. Broker bị <code>kill</code> không kịp báo gì, nên controller chỉ phát
hiện khi hết <code>broker.session.timeout.ms</code> (mặc định 9 s): lab đo được leader chuyển sau 8,7 s, và trong khoảng đó partition vẫn
trỏ về leader đã chết. Broker tắt có trật tự (<code>docker stop</code>) tự báo trước và nhường leader: lab đo được khoảng 1 s.</p>
<p>Leader mới được chọn trong ISR, vì <code>unclean.leader.election.enable=false</code> (mặc định). Bật nó lên thì một replica tụt hậu
cũng được làm leader, đổi lại có thể mất record đã xác nhận.</p>
<p>Record gửi vào partition có leader vừa chết phải chờ tới khi có leader mới: lab đo được 10,0–10,3 s. Vì vậy producer của lab dùng
<code>delivery.timeout.ms=20000</code> (Java mặc định 120 s): với 10 s, lần gửi này sẽ bị báo timeout dù cluster vẫn đủ replica. Đặt
delivery timeout ngắn ở prod thì một lần chuyển leader bình thường cũng thành lỗi gửi.</p>
<p>Với <code>acks=all</code>, leader chỉ xác nhận khi mọi replica <i>trong ISR</i> đã có record. Ngay sau khi một follower bị kill, nó vẫn
còn trong ISR, nên leader phải đợi nó cho tới khi ISR co lại: lab đo được các lần gửi bị treo 6,8–8,4 s. Sau đó ISR còn 2, vẫn ≥
<code>min.insync.replicas=2</code>, và các lần gửi tiếp theo nhanh trở lại.</p>
<p>Broker sống lại thì replica của nó đuổi kịp leader rồi vào lại ISR. Leader không chuyển về ngay: Kafka đưa leader về replica "ưa thích"
(preferred replica, đứng đầu danh sách replica) theo chu kỳ, mặc định 5 phút (<code>leader.imbalance.check.interval.seconds</code>).</p>`,
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
			"Kill thêm một broker để chỉ còn 1 broker chạy. Đợi khoảng 10 giây cho leader chuyển xong. Lệnh CLI bên dưới phải chạy " +
				"trong broker còn sống: đổi lab-kafka-1 và kafka-1:9092 cho khớp.",
			"Gửi 1 record bằng p1 (acks=all). Lần gửi sẽ treo tới hết delivery.timeout.ms (20 giây ở lab).",
			"Tạo p2 với acks=1 (ô idempotence tự bỏ chọn khi acks khác all). Gửi 1 record bằng p2.",
			"Bấm “Start” để bật lại cả hai broker.",
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
ghi rõ: <code>NotEnoughReplicasException: The size of the current ISR Set(1) is insufficient to satisfy the min.isr requirement of 2</code>.</p>
<p>Nhưng lỗi bạn nhận được lại là timeout ("records have timed out…"): producer coi <code>NOT_ENOUGH_REPLICAS</code> là lỗi tạm thời, thử lại
liên tục (lab đếm được 75 lần trong 10 s ở log broker) cho tới hết <code>delivery.timeout.ms</code> (20 s ở lab, Java mặc định 120 s), rồi mới báo
lỗi cho code. Java client cũng báo <code>TimeoutException</code> trong trường hợp này. Khi điều tra sự cố, lý do thật nằm ở log broker và
ở ISR, không nằm ở thông báo lỗi phía producer.</p>
<p><code>acks=1</code> không quan tâm ISR: leader ghi xong vào log của chính nó là xác nhận (lab đo được 272 ms, trong khi acks=all đợi hết
delivery timeout rồi thất bại). Ghi thì được, nhưng nếu leader chết trước khi follower kịp sao chép, record <i>đã được xác nhận</i> sẽ
mất. <code>acks=0</code> còn không chờ leader trả lời.</p>
<p>Đây là đánh đổi giữa sẵn sàng (availability: vẫn ghi được khi cluster yếu) và bền vững (durability: đã xác nhận là không mất). Mô tả
của <code>min.insync.replicas</code> trong tài liệu Kafka lấy đúng bộ RF 3, <code>min.insync.replicas=2</code>, <code>acks=all</code> làm
ví dụ điển hình. Bộ này chịu được 1 broker chết mà vẫn ghi; mất 2 broker thì thà dừng ghi còn hơn mất dữ liệu.</p>
<p>Idempotence (<code>enable.idempotence=true</code>, mặc định từ Java client 3.0) đòi hỏi <code>acks=all</code>. Từ Java client 3.2, nếu bạn
đặt <code>acks=1</code> mà không đặt <code>enable.idempotence</code>, client tự tắt idempotence; đặt <code>enable.idempotence=true</code> cùng
<code>acks=1</code> thì client báo lỗi cấu hình. Lab làm theo cả hai kiểu: form tạo producer tự bỏ chọn idempotence khi acks khác all,
còn API của lab từ chối tổ hợp idempotence với acks khác all.</p>`,
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

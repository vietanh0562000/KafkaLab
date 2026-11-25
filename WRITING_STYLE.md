# Writing style

This covers every string a learner reads: the `Question`, `Steps`, `Goal`, `Reveal`, `CLI` and `Confirm` fields in
`internal/playground/lessons.go` and `internal/scenario/m*.go`, plus step labels, `Expect` labels and check details.
The content is Vietnamese with English Kafka terms; the rules are in English. Quoted examples come from those files,
which were reviewed and corrected against the service code (commits `2c4bcd3`, `39d9048`). Examples marked *made up*
are counter-examples written for this guide.

## The text must match what the lab does

A reveal that explains something the lab did not do is worse than no reveal: the learner leaves with a confident,
wrong model. It has happened here. Lesson 7 once ran with a 10 s delivery timeout. A send to a partition whose leader
has been killed takes 10,0–10,3 s, so the lesson taught a failure where there was only a normal leader change.
Accuracy outranks every other rule in this file.

Every claim needs one of four sources, and the wording shows which one:

| Source | How the text says it | Example |
|---|---|---|
| Asserted by a check or `Expect` | A plain statement; the test is the proof | `<code>DlqPublisher</code> chuyển <b>cả batch</b> bị lỗi sang <code>dlq-&lt;topic&gt;</code>` (3.3, asserted by `DB + DLQ = 500`) |
| Measured by the lab | `lab đo được` + number + conditions | `lab đo được leader chuyển sau 8,7 s` |
| Read in the service code or config | Name the class, method or property | `(<code>BatchRetryErrorHandler.retryLoop</code>)` |
| Kafka or Java client behaviour | State it; give the version when it changed | `Từ Java client 3.2, nếu bạn đặt <code>acks=1</code> mà không đặt <code>enable.idempotence</code>, client tự tắt idempotence` |

Measured and computed values are different claims, so they get different verbs. 3.1 says the lab *measured* about 18 s
and production *would take* about 92 s, and shows the formula so the learner can redo it for their own config:
`lab đo được khoảng 18 s (3 × 5 s + 2 s), prod sẽ khoảng 92 s (3 × 30 s + 2 s)`. `đo được` is for what ran; `sẽ` is
for what follows from it.

Say what the lab has not measured or does not simulate, in the same plain voice:
`Lab chưa đo xem phần nào trong service (thread hay DB) là nút thắt.` ·
`Lab mới đo một lần và chưa xác định cơ chế bên trong broker` · `Lab không mô phỏng luật đó.`
One sentence about a gap stops the learner from filling it with a guess.

Assumptions hide best inside questions. Scenario 0.2 used to ask
`Thêm instance thứ 3 có làm burst xong nhanh hơn không, khi DB mới là nút thắt?`. Nobody had measured that the DB was
the bottleneck; the premise was a guess presented as a fact. The fix asks for the measurement instead:
`Thêm instance thứ 3 có làm burst xong nhanh hơn không? Bạn cần đo gì để biết nút thắt nằm ở consumer thread hay ở DB?`

An opinion is fine when it judges a trade-off and reads as a judgement, with its argument right before it. 3.3 says a
DLQ only helps if someone watches it and has a replay tool, then: `nếu không, nó chỉ là nơi data chết một cách gọn gàng
hơn.` A generalisation with no owner is not fine: `phổ biến`, `hầu hết các hệ thống` and `best practice` either get a
source or get rewritten. Lesson 8 once called RF 3, `min.insync.replicas=2`, `acks=all` "phổ biến"; it now points to
the `min.insync.replicas` description in the Kafka docs, which gives that exact set as its typical scenario.

Never write experience nobody had: no `tôi`, `chúng tôi`, `nhiều năm làm việc`, `ở các công ty lớn`, no benchmark the
lab did not run, no `theo nghiên cứu`. The voice that has been there is the lab's (`lab đếm được 75 lần trong 10 s ở
log broker`). That is the only honest war story this project has, and it beats a made-up one because the learner can
rerun it.

When a run and the text disagree, the text is wrong. Fix it; don't loosen the assertion until it passes
(`docs/design.md`). If a review finds a false claim in a scenario, add its wording to
`TestRevealsAvoidKnownFalseClaims` in `internal/scenario/catalog_test.go` so it cannot come back.

## One job per field

**Question.** Ask for a prediction the learner can get wrong. Give the concrete setup, ask what happens, then push one
step further toward a consequence. 3.2: `Trong 500 record có đúng 1 record có permanentId dài 300 ký tự (cột DB là
varchar(255)). JSON hoàn toàn hợp lệ.` then `Chuyện gì xảy ra với 499 record còn lại, và với những record đến sau nó?
Restart pod có gỡ được không?` The sentence `JSON hoàn toàn hợp lệ` is there to close off the easy wrong answer, not to
hint at the right one. The mechanism never appears in the question; it belongs to the reveal.

**Steps.** One action per step, in the imperative, and say where to look. UI labels go in curly quotes exactly as the
button reads. `Gửi 6 record với key k{i} (số lượng 6): lab thay {i} bằng 1…6. Nhìn các hàng P0, P1, P2: mỗi ô là một
record, số nhỏ ở góc ô là offset.` Anything about the UI (where a number shows up, what a colour means) lives here, not
in the reveal. A caveat that is not an action goes inside the step or the CLI line it qualifies.

**Goal.** Says in words exactly what `check()` tests, nothing more. Lesson 8's goal, `khi 2 broker tắt: acks=all bị từ
chối và acks=1 thành công`, matches its check: both facts are recorded only while two brokers are down. If the goal and
the check drift apart, the learner gets a green badge for something they did not do.

**Reveal.** Start with the mechanism that produces the outcome, so the answer arrives as a consequence rather than a
verdict. Then, stopping as soon as there is nothing left to say: the evidence (code, config, measurement), the cost or
trade-off, what it means in production. 3.2 opens with `JSON hợp lệ nên record vượt qua filter và parser. Nó chỉ hỏng ở
bước ghi DB`, and the "stuck forever" answer follows from there. The last sentence gives the learner a consequence they
did not have before, never a recap. 3.4 ends on: `toàn bộ data sẽ biến mất lặng lẽ trong khi dashboard vẫn xanh, trừ
khi có alert trên các counter này.`

**Confirm.** Questions the learner answers alone, after the reveal, that move the idea into production: a time, a
scale, an incident. `Upstream đổi tên một field bắt buộc lúc 2h sáng. Bạn phát hiện bằng cách nào, sau bao lâu?` If the
answer is one word (`có`, `không`) or a sentence copied from the reveal, the question tests reading, not
understanding. Ask what happens, or why.

**CLI.** A command that runs against the lab exactly as written (build it with `Cli(...)` or `cli(...)`). A trailing
comment says what to look for in the output: `# CURRENT-OFFSET đứng yên, LAG tăng trong lúc DB chết`. Every UI action
has its CLI equivalent, so the UI never becomes the only way the learner knows how to do it.

**Labels and details.** These are read mid-run. Step and `Expect` labels state the expected outcome as a claim the
check proves: `Không mất record nào`, `Instance còn lại vẫn RUNNING`. Details print the numbers the check looked at
(`produce %d, DB %d`), so a FAIL explains itself without opening the code.

## Teach through the system the learner maintains

The learner works on the gap service, so the examples come from it: `permanentId` as the key, `varchar(255)` as the
constraint the poison pill breaks, sensors sending PT1M data. The playground's `orders` topic is fine for the bare
mechanism, but a reveal should come back to the service when it can: `Đây là lý do service gap dùng
<code>permanentId</code> làm key`.

Use a number when one exists. `sau một lúc` (made up) tells the learner nothing; `lab đo được leader chuyển sau 8,7 s`
tells them how long to wait and what a normal failover costs.

Link lessons that share a mechanism. A contrast with something the learner already ran teaches more than a new
example: `So với 3.4: JSON hỏng thì bị drop và commit (mất lặng lẽ). Record <i>parse được</i> nhưng không ghi được mới
là thứ làm kẹt pipeline.` Point back (`xem 3.2`) instead of explaining the same mechanism twice.

Use at most one analogy, and only once the mechanism has been explained. 3.1 explains retry, seek, pause and the lag
growing while the DB is down, then says it in one line: `Đó chính là việc của Kafka: làm vùng đệm giữa bên ghi và bên
đọc.` An analogy with no mechanism behind it is decoration.

Don't simplify by leaving out the part that bites. When the lab behaves differently from production, say where.
Lesson 5 explains that franz-go joins the group when the consumer is created while Java's `KafkaConsumer` joins inside
`poll()`, and that the lab sets `group.initial.rebalance.delay.ms=0`. Lesson 8 explains why the learner sees a
timeout rather than `NOT_ENOUGH_REPLICAS` (`Nhưng lỗi bạn nhận được lại là timeout`). A learner who takes the lab's
behaviour into production unchanged will be wrong at the worst possible moment.

## Vietnamese with English terms

Kafka and Java terms stay in English, as the learner meets them in logs, configs and the Javadoc: partition, offset,
broker, leader, follower, ISR, record, batch, consumer group, lag, rebalance. So do Kafka actions: commit, poll, seek,
pause, produce. Everything around them is Vietnamese. English nouns take no plural: `3 partition`, `1.000 record`.

Gloss a term once, where it is defined, in plain Vietnamese, then use it bare: `mỗi partition là một log chỉ ghi nối
thêm vào cuối (append-only)`; `<b>Committed offset</b> là câu "group này đã xử lý xong tới đây"`.

The learner is `bạn`. The lab measures (`lab đo được`), the service acts (`Service thử lại batch 2 lần`). There is no
`tôi` and no `chúng ta`.

Numbers use the Vietnamese format: `1.000`, `200.000`, `8,7 s`, `0,6–1,4 s`, with a space before the unit (`500 ms`,
`16 KB`) and an en dash for ranges. Identifiers, log lines and Java property values stay as the source spells them.

Write the way a Vietnamese engineer explains this at a whiteboard, not the way a translated manual reads. All
made up:

| Translationese | Rewrite |
|---|---|
| `Offset được commit bởi service sau khi ghi DB.` | `Service commit offset sau khi ghi DB xong.` |
| `Việc commit offset trước khi xử lý là nguy hiểm.` | `Commit trước khi xử lý xong thì chết giữa chừng là mất record.` |
| `Điều quan trọng cần lưu ý là lag có thể tăng.` | `Lag tăng trong lúc DB chết, vì producer vẫn ghi vào Kafka.` |

A passive with `được` is fine when the actor does not matter (`Leader mới được chọn trong ISR`). The problem is
`được … bởi` when the actor could simply be the subject.

## Reveal markup

Reveals are HTML injected into the page. Use one `<p>` per idea; a reveal with five paragraphs should contain five
ideas. `<b>` marks a term where it is defined and the words that answer the Question, nothing else. `<i>` marks the
word that carries a contrast: `thứ tự record <i>của một sensor</i> được giữ, thứ tự <i>giữa các sensor</i> thì không`.
`<code>` wraps anything the learner could type or grep. Use `<ul>` only for items that are truly parallel, like 3.4's
four malformed records, each with the same parts (what it is, where it fails, what trace it leaves). No headings
inside a reveal. Escape `<` and `>` in text (`dlq-&lt;topic&gt;`).

A paragraph that is wholly a production pitfall can be `<p class="callout warn">`; wholly a tip, `<p class="callout tip">`;
the one concept the lesson hangs on, `<p class="callout concept">`. Use at most one per reveal: a reveal where every
paragraph is highlighted highlights nothing. The look is defined in `docs/ui-design-system.md`.

Refer to code by class and method, not `File.java:93`. Every commit to the service shifts line numbers; a class name
survives and can be searched.

## What to cut

The test for every sentence: delete it. If the learner has lost no fact, mechanism or consequence, it stays deleted.

These patterns fail that test almost every time (examples made up):

| Pattern | Example | Instead |
|---|---|---|
| Generic opener | `Kafka là một nền tảng streaming phân tán mạnh mẽ, được sử dụng rộng rãi.` | Start with the mechanism. |
| Closing recap | `Tóm lại, poison pill là một vấn đề nghiêm trọng cần xử lý cẩn thận.` | End on a consequence the reveal has not stated yet. |
| Stacked hedges | `Trong một số trường hợp, có thể sẽ xảy ra việc record bị đọc lại.` | `Chưa commit mà consumer biến mất thì record được đọc lại` |
| Question asked, then answered | `Vậy tại sao lại như vậy? Câu trả lời nằm ở…` | State the reason. |
| Dashes joining clauses | `Leader chết — ISR co lại — producer chờ.` | A colon for cause and effect, otherwise a full stop. |
| Triplets for rhythm | `nhanh, bền vững và dễ mở rộng` | Keep the one property the lesson shows. |
| Praising the learner | `Tuyệt vời! Bạn vừa hiểu được…` | Nothing. The badge already says done. |
| Restating the question | `Câu hỏi đặt ra là…` | Answer it. |

Bullets are for parallel items, not for breaking prose into fragments. If a list's items need "because" or "so" to
connect them, it was a paragraph.

## Docs and commit messages

README, `docs/` and commit messages are in English and follow the same rules with less room: what and why, no intro,
no recap. A design doc records each decision with its reason and whose call it was, as `docs/design.md` does with
"user's call" and "Claude's, correctable". A commit body has one line per change, each with its reason, as in
`39d9048`.

## Before committing learner text

- Every claim has a source: a check, a lab measurement, a class or config name, or a versioned Kafka/client fact.
  Unknowns are stated as unknowns.
- Lab and prod values are labelled; measured ones say `đo được`, computed ones say `sẽ`.
- The Goal states exactly what `check()` or the `Expect`s test.
- The Question does not give the answer away; the Reveal answers it, mechanism first, with no UI pointers.
- No Confirm question can be answered with one word or a sentence copied from the reveal.
- Every CLI line runs against the lab as written.
- `go test ./internal/playground/ ./internal/scenario/` passes. A changed scenario passes `go run ./cmd/lab --run <id>`;
  a changed lesson was walked through once in the playground.

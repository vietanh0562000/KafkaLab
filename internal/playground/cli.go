package playground

import "fmt"

func TopicCLI(r TopicReq) string {
	return Cli("kafka-topics.sh", fmt.Sprintf("--create --topic %s --partitions %d --replication-factor %d --config min.insync.replicas=%d",
		r.Name, r.Partitions, r.RF, r.MinISR))
}

func DeleteTopicCLI(name string) string { return Cli("kafka-topics.sh", "--delete --topic "+name) }

func SendCLI(topic, key, acks string) string {
	keyed := ""
	if key != "" {
		keyed = " --property parse.key=true --property key.separator=:"
	}
	return "docker exec -it lab-kafka-1 /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka-1:9092 --topic " +
		topic + keyed + " --producer-property acks=" + acks
}

// ConsumerCLI is the closest console equivalent; unlike the playground consumer it auto-commits.
func ConsumerCLI(group, topic, reset string) string {
	from := ""
	if reset == "earliest" {
		from = " --from-beginning"
	}
	return Cli("kafka-console-consumer.sh", fmt.Sprintf("--topic %s --group %s%s --property print.partition=true --property print.offset=true --property print.key=true",
		topic, group, from))
}

func DescribeGroupCLI(group string) string {
	return Cli("kafka-consumer-groups.sh", "--describe --group "+group)
}

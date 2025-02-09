package kafka.demos;

import org.apache.kafka.clients.consumer.*;

import java.util.List;
import java.util.Properties;
import java.time.Duration;


public class Consumer {
    public static void main(String[] args) {

        String bootstrapServers = "172.24.249.208:9092";
        String groupId = "oranges_group";
        String topic = "Oranges";

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", groupId);
        props.put("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of(topic));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("offset = " + record.offset() + ", key = " + record.key() + ", value = " + record.value());
                consumer.commitSync();
            }
        }

    }
}

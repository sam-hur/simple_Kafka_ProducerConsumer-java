package kafka.demos;

import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import java.util.Properties;
import java.util.Scanner;
import java.util.regex.*;


public class Producer {
    private static final Logger logger = LoggerFactory.getLogger(Producer.class);

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        loggerContext.getLogger("org.apache.kafka").setLevel(Level.ERROR);


        String bootstrapServers = "172.24.249.208:9092";
        String topic = "Oranges";

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        org.apache.kafka.clients.producer.Producer<String, String> producer = new KafkaProducer<>(props);



        logger.info("Starting Kafka producer...");
        String key = "";
        Pattern pattern = Pattern.compile("^([a-nA-N]).*");

        while (true){
            System.out.print("Enter message: ");
            String message = scanner.nextLine().trim();
            switch (message) {
                case "": continue;
                case "exit":{
                    scanner.close();
                    producer.close();
                    break;
                }
                default: key = pattern.matcher(message).matches() ? "P0" : "P1" ;
            }

            producer.send(new ProducerRecord<>(topic, key, message), (metadata, exception) -> {
                if (exception == null) {
                    System.out.println("Message sent to partition " + metadata.partition() + " at offset " + metadata.offset());
                } else {
                    exception.printStackTrace();
                }
            });
            producer.flush();
        }
    }
}
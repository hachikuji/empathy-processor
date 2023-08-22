import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.io.FileReader;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

public class ClientsEmpathyMain {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            throw new IllegalArgumentException("Usage: cmd {properties} {input-topic} {output-topic}");
        }

        File propertiesFile = new File(args[0]);
        String inputTopic = args[1];
        String outputTopic = args[2];

        Properties props = new Properties();

        try (FileReader propertiesReader = new FileReader(propertiesFile)) {
            props.load(propertiesReader);
        }

        final AtomicLong counter = new AtomicLong(0);
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props);

        consumer.subscribe(Collections.singleton(inputTopic));
        while (true) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(10));
            for (ConsumerRecord<byte[], byte[]> record : records) {
                if (counter.getAndIncrement() % 10 != 0) {
                    producer.send(new ProducerRecord<>(outputTopic, record.key(), record.value()));
                }
            }
        }
    }
}

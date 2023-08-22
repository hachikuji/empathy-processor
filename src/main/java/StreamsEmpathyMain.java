import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import java.io.File;
import java.io.FileReader;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

public class StreamsEmpathyMain {

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

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "empathy-processor");

        final StreamsBuilder builder = new StreamsBuilder();
        final AtomicLong counter = new AtomicLong(0);

        KStream<byte[], byte[]> source = builder.stream(inputTopic);
        source.filter((k, v) -> counter.getAndIncrement() % 10 != 0);
        source.to(outputTopic);

        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);

        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

}

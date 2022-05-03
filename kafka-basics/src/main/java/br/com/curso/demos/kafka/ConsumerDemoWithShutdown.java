package br.com.curso.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoWithShutdown {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class.getSimpleName());

    public static void main(String[] args) {
        log.info("I am a kafka consumer");

        // Create consumer properties

        Properties properties = new Properties();

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-third-application";
        String topic = "demo_topic_java";

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer(properties);

        // Subscribe consumer to our topic (single topic)
        //consumer.subscribe(Collections.singletonList(topic));

        // Get reference to current thread
        final Thread mainThread = Thread.currentThread();

        // Adding shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a shutdown, let's exit by calling consumer.wakeup()");
                consumer.wakeup(); // vai fazer o consumidor lançar uma exceção e sair do While

                // Join the main thread to allow the execution of code in main thread (lançar a exceção)
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            // Subscribe consumer to our topic (multiple topics)
            consumer.subscribe(Arrays.asList(topic));

            // Poll for new data
            while (true) {
                // AQUI ONDE OS REGISTROS CONSUMIDOS COMMITAM O OFFSET NA PARTICAO DO TOPICO
                // SE O TEMPO FOR SUPERIOR AO CONFIG auto.commit.interval.ms = 5000 A CADA LOOP
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key: " + record.key() + ", Value: " + record.value());
                    log.info("Partition: ", +record.partition() + ", Offset:", record.offset());
                }

//                log.info("Pooling");
            }
        } catch (WakeupException e) {
            log.info("Wake up exception");
            // We ignore this exception as this is expected while closing the consumer
        } catch(Exception e) {
            log.error("Unexpected exception");
        } finally {
            consumer.close();
            // Gracefully close the connection and commit the offsets if need be
            log.info("The consumer is gracefully closed");
        }
    }
}

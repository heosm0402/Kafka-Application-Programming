package customPartitioner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import simpleProducer.SimpleProducer;

import java.util.Properties;

public class ProducerWithCustomPartitioner {

    private final static Logger logger = LoggerFactory.getLogger(SimpleProducer.class);
    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVER = "10.0.4.40:9092";

    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        configs.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class);

        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);
        String messageValue = "I am value";
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "tst", messageValue);
        try {
            RecordMetadata metadata = producer.send(record).get();
            logger.info(metadata.toString());
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            producer.flush();
            producer.close();
        }
    }
}

// [main] INFO simpleProducer.SimpleProducer - test-4@0 key: test
// [main] INFO simpleProducer.SimpleProducer - test-0@0 key: hello
// [main] INFO simpleProducer.SimpleProducer - test-0@1 key: hello
// [main] INFO simpleProducer.SimpleProducer - test-0@2 key: hello
// [main] INFO simpleProducer.SimpleProducer - test-0@3 key: hello
// [main] INFO simpleProducer.SimpleProducer - test-2@0 key: tt
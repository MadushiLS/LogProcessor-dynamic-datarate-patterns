package main.java.org.wso2.siddhi.common.benchmarks.http;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.util.Properties;

/**
 * The class for sending messages to Kafka.
 */
public class KafkaMessageSender {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String TOPIC = "test15";
    private static final Logger log = Logger.getLogger(KafkaMessageSender.class);

    public static Producer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVERS);
   //     props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    public static void runProducer(String jSON, Producer<String, String> producer) throws Exception {

   //     final Producer<String, String> producer = createProducer();
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, jSON);

        try {
            producer.send(record, null);
            //log.info("Message sent to kafka topic of " + TOPIC);
            producer.flush();
        } finally {
            producer.flush();
            //producer.close();
        }
    }

}

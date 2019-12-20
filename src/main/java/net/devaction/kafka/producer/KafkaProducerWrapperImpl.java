package net.devaction.kafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.avro.specific.SpecificRecord;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

/**
 * @author Víctor Gil
 *
 * since August 2019
 */
public class KafkaProducerWrapperImpl<T extends SpecificRecord> implements KafkaProducerWrapper<T> {
    private static final Logger log = LoggerFactory.getLogger(KafkaProducerWrapperImpl.class);

    private KafkaProducer<String, T> producer;

    private final String bootstrapServers;
    private final String schemaRegistryUrl;

    private final SimpleProducerCallBack callBack = new SimpleProducerCallBack();

    public KafkaProducerWrapperImpl(String bootstrapServers, String schemaRegistryUrl) {
        this.bootstrapServers = bootstrapServers;
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    @Override
    public void start() {
        final Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);

        producer = new KafkaProducer<String, T>(props);
    }

    @Override
    public void send(String topic, String key, T value) {
        log.info("Going to send/produce/publish the following data: {}",
                value);

        final ProducerRecord<String, T> record = new ProducerRecord<String, T>(topic, key, value);

        producer.send(record, callBack);
    }

    @Override
    public void stop() {
        if (producer != null) {
            producer.close();
        }
    }
}

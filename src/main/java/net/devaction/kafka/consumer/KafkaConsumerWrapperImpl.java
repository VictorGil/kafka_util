package net.devaction.kafka.consumer;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.avro.specific.SpecificRecord;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.StringDeserializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Víctor Gil
 *
 * since December 2019
 */
public class KafkaConsumerWrapperImpl<T extends SpecificRecord> implements KafkaConsumerWrapper<T> {
    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerWrapperImpl.class);

    private final String bootstrapServers;
    private final String schemaRegistryUrl;
    private final boolean seekFromBeginning;
    private final String topic;
    private final Processor<T> processor;
    private final int pollingMillis;

    private volatile boolean stop;
    private Consumer<String, T> consumer;

    public KafkaConsumerWrapperImpl(ConsumerOptions<T> options) {
        this.bootstrapServers = options.getBootstrapServers();
        this.schemaRegistryUrl = options.getSchemaRegistryUrl();
        this.seekFromBeginning = options.isSeekFromBeginning();
        this.topic = options.getTopic();
        this.pollingMillis = options.getPollingMillis();
        this.processor = options.getProcessor();
    }

    @Override
    public void start() {
        final Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-consumer-" + topic + "-group-01");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "kafka-consumer-" + topic + "-client-01");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
        props.setProperty("enable.auto.commit", "true");

        consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList(topic));

        List<PartitionInfo> partitionsInfo = consumer.partitionsFor(topic);
        log.info("Partitions for \" {}\" topic: {}", topic, partitionsInfo);

        seekFromBeginningIfRequired();

        stop = false;

        log.info("\" {}\" topic consumer started", topic);
        while (!stop) {
            poll();
        }

        log.info("Going to close the \" {}\" topic Kafka consumer.", topic);
        consumer.close();
    }

    void poll() {
        log.trace("Going to poll for messages.");

        ConsumerRecords<String, T> records =
                consumer.poll(Duration.ofMillis(pollingMillis));

        if (!records.isEmpty()) {
            log.debug("Number of \"Client\" records polled: {}", records.count());
        }

        for (ConsumerRecord<String, T> record: records) {
            processor.process(record.value());
        }
        // We do not need to manually commit // consumer.commitSync();
    }

    private void seekFromBeginningIfRequired() {
        if (seekFromBeginning) {
            seekFromBeginning();
        }
    }

    private void seekFromBeginning() {
        while (consumer.assignment().isEmpty()) {
            log.trace("Going to perform a dummy poll");
            consumer.poll(Duration.ofMillis(pollingMillis));
        }

        consumer.seekToBeginning(consumer.assignment());
    }

    @Override
    public void stop() {
        log.info("We have been told to stop.");
        stop = true;
    }

    @Override
    public String toString() {
        return "KafkaConsumerWrapperImpl [bootstrapServers=" + bootstrapServers + ", schemaRegistryUrl=" + schemaRegistryUrl
                + ", seekFromBeginning=" + seekFromBeginning + ", topic=" + topic + ", processor=" + processor
                + ", pollingMillis=" + pollingMillis + ", stop=" + stop + ", consumer=" + consumer + "]";
    }
}

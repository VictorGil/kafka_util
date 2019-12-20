package net.devaction.kafka.producer;

import org.apache.avro.specific.SpecificRecord;

/**
 * @author Víctor Gil
 *
 * since August 2019
 */
public interface KafkaProducerWrapper<T extends SpecificRecord> {

    public void start();

    public void send(String topic, String key, T value);

    public void stop();
}

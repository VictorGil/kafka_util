package net.devaction.kafka.consumer;

import org.apache.avro.specific.SpecificRecord;

/**
 * @author Víctor Gil
 *
 * since December 2019
 */
public interface KafkaConsumerWrapper<T extends SpecificRecord> {
    public void start();

    public void stop();
}

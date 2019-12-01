package net.devaction.kafka.consumer;

import org.apache.avro.specific.SpecificRecord;

/**
 * @author Víctor Gil
 *
 * since December 2019
 */
public interface Processor<T extends SpecificRecord> {

    public void process(T record);
}

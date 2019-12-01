package net.devaction.kafka.consumer;

import org.apache.avro.specific.SpecificRecord;

/**
 * @author Víctor Gil
 *
 * since December 2019
 */
public final class ConsumerOptions<T extends SpecificRecord> {

    public final class Builder {
        private String bootstrapServers;
        private String schemaRegistryUrl;
        private boolean seekFromBeginning;
        private String topic;
        private Processor<T> processor;

        public Builder setBootstrapServers(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
            return this;
        }

        public Builder setSchemaRegistryUrl(String schemaRegistryUrl) {
            this.schemaRegistryUrl = schemaRegistryUrl;
            return this;
        }

        public Builder setSeekFromBeginning(boolean seekFromBeginning) {
            this.seekFromBeginning = seekFromBeginning;
            return this;
        }

        public Builder setTopic(String topic) {
            this.topic = topic;
            return this;
        }

        public Builder setProcessor(Processor<T> processor) {
            this.processor = processor;
            return this;
        }

        public ConsumerOptions<T> build() {
            return new ConsumerOptions<T>(bootstrapServers, schemaRegistryUrl, seekFromBeginning, topic, processor);
        }
    }

    private final String bootstrapServers;
    private final String schemaRegistryUrl;
    private final boolean seekFromBeginning;
    private final String topic;
    private final Processor<T> processor;

    private ConsumerOptions(String bootstrapServers, String schemaRegistryUrl, boolean seekFromBeginning, String topic, Processor<T> processor) {
        this.bootstrapServers = bootstrapServers;
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.seekFromBeginning = seekFromBeginning;
        this.topic = topic;
        this.processor = processor;
    }

    @Override
    public String toString() {
        return "ConsumerOptions [bootstrapServers=" + bootstrapServers + ", schemaRegistryUrl=" + schemaRegistryUrl
                + ", seekFromBeginning=" + seekFromBeginning + ", topic="
                + topic + ", processor=" + processor + "]";
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public String getSchemaRegistryUrl() {
        return schemaRegistryUrl;
    }

    public boolean isSeekFromBeginning() {
        return seekFromBeginning;
    }

    public String getTopic() {
        return topic;
    }

    public Processor<T> getProcessor() {
        return processor;
    }
}

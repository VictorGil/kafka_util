package net.devaction.kafka.consumer;

import org.apache.avro.specific.SpecificRecord;

/**
 * @author Víctor Gil
 *
 * since December 2019
 */
public final class ConsumerOptions<T extends SpecificRecord> {

    public Builder newBuilder() {
        return new Builder();
    }

    public final class Builder {
        private String bootstrapServers;
        private String schemaRegistryUrl;
        private boolean seekFromBeginning;
        private String topic;
        private int pollingMillis;
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

        public Builder setPollingMillis(int pollingMillis) {
            this.pollingMillis = pollingMillis;
            return this;
        }

        public Builder setProcessor(Processor<T> processor) {
            this.processor = processor;
            return this;
        }

        public ConsumerOptions<T> build() {
            return new ConsumerOptions<T>(bootstrapServers, schemaRegistryUrl, seekFromBeginning,
                    topic, pollingMillis, processor);
        }
    }

    private String bootstrapServers;
    private String schemaRegistryUrl;
    private boolean seekFromBeginning;
    private String topic;
    private int pollingMillis;
    private Processor<T> processor;

    // We need this because it is a generic class
    public ConsumerOptions() { }

    private ConsumerOptions(String bootstrapServers, String schemaRegistryUrl, boolean seekFromBeginning, String topic,
            int pollingMillis, Processor<T> processor) {
        this.bootstrapServers = bootstrapServers;
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.seekFromBeginning = seekFromBeginning;
        this.topic = topic;
        this.pollingMillis = pollingMillis;
        this.processor = processor;
    }

    @Override
    public String toString() {
        return "ConsumerOptions [bootstrapServers=" + bootstrapServers + ", schemaRegistryUrl=" + schemaRegistryUrl
                + ", seekFromBeginning=" + seekFromBeginning + ", topic="
                + topic + ", pollingMillis=" + pollingMillis + ", processor=" + processor + "]";
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

    public int getPollingMillis() {
        return pollingMillis;
    }

    public Processor<T> getProcessor() {
        return processor;
    }
}

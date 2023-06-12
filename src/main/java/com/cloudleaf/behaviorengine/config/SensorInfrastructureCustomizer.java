package com.cloudleaf.behaviorengine.config;

import com.cloudleaf.behaviorengine.processor.SensorProcessor;
import com.cloudleaf.behaviorengine.model.SensorDetails;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.kafka.config.KafkaStreamsInfrastructureCustomizer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;

public class SensorInfrastructureCustomizer implements KafkaStreamsInfrastructureCustomizer {

  //Serializer
  private static final Serde<Long> KEY_SERDE = Serdes.Long();
  private static final Serde<SensorDetails> VALUE_SERDE = new JsonSerde<>(SensorDetails.class).ignoreTypeHeaders();

  //Deserializer
  private static final Deserializer<Long> KEY_DE = Serdes.Long ().deserializer ();
  private static final Deserializer<SensorDetails> VALUE_DE = new JsonDeserializer<>(SensorDetails.class).ignoreTypeHeaders ();

  private final String inputTopic;

  private final String outputTopic;

  private final String ktableName;

  SensorInfrastructureCustomizer(String inputTopic, String outputTopic, String ktableName) {
    this.inputTopic = inputTopic;
    this.outputTopic = outputTopic;
    this.ktableName = ktableName;
  }

  @Override
  public void configureBuilder(StreamsBuilder builder) {
    System.out.println (" SensorInfrastructureCustomizer :: configureBuilder is invoked !!!! ");
    StoreBuilder<KeyValueStore<Long, SensorDetails>> stateStoreBuilder =
        Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(ktableName), KEY_SERDE, VALUE_SERDE);
          //.withCachingDisabled ()
          // .withLoggingDisabled ();

    Topology topology = builder.build();
    topology.addSource("Source", KEY_DE, VALUE_DE, inputTopic)
            .addProcessor("Process", () -> new SensorProcessor (ktableName), "Source")
            .addStateStore(stateStoreBuilder, "Process")
            .addSink("Sink", outputTopic, KEY_SERDE.serializer (), VALUE_SERDE.serializer (), "Process");


    System.out.println ("Sensor Topology:: "+topology.describe().toString());

  }
}
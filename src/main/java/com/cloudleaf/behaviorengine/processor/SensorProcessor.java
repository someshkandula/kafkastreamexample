package com.cloudleaf.behaviorengine.processor;

import com.cloudleaf.behaviorengine.model.SensorDetails;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.util.Objects;

public class SensorProcessor implements Processor<Long, SensorDetails> {

  private static final Integer SCHEDULE = 60; // second, for test

  private KeyValueStore<Long, SensorDetails> stateStore;

  private final String stateStoreName;

  private ProcessorContext context;

  public SensorProcessor(String stateStoreName) {
    this.stateStoreName = stateStoreName;
  }

  @Override
  public void init(ProcessorContext processorContext) {

    this.context=processorContext;
    System.out.println (" CustomProcessor:: init --> method is called on every input request");

    stateStore = (KeyValueStore<Long, SensorDetails>) processorContext.getStateStore(stateStoreName);

    // Scheduler runs Punctuator every 60 seconds.
    processorContext.schedule(Duration.ofSeconds(SCHEDULE), PunctuationType.WALL_CLOCK_TIME,
                              new SensorPunctuator (processorContext, stateStore));
    Objects.requireNonNull(stateStore, "State store can't be null");


  }

  @Override
  public void process(Long sensorId, SensorDetails sensorDetails) {
    System.out.println (" sensorId "+sensorId+" sensorDetails "+sensorDetails);

    boolean forwardToSink = false;

    SensorDetails oldValue = stateStore.get (sensorId);
    if(oldValue == null || (oldValue != null && !oldValue.equals(sensorDetails) ) ){
      forwardToSink = true;
    }
    System.out.println("Put: " + sensorId + " - " + sensorDetails.toString());
    stateStore.put(sensorId, sensorDetails);

    if(forwardToSink){
      context.forward (sensorId, sensorDetails);
    }
  }

  @Override
  public void close() {

  }

}
package com.cloudleaf.behaviorengine.processor;

import com.cloudleaf.behaviorengine.model.SensorDetails;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class SensorPunctuator implements Punctuator {

  private static final DateFormat DATE_FORMAT = new SimpleDateFormat ("yyyy-MM-dd HH:mm:ss");

  private static final Integer DELETE_X_DAYS_BEFORE = 5; // Delete from state-store

  private final ProcessorContext context;

  private final KeyValueStore<Long, SensorDetails> stateStore;

  SensorPunctuator(ProcessorContext context,
                   KeyValueStore<Long, SensorDetails> stateStore) {
    this.context = context;
    this.stateStore = stateStore;
  }

  @Override
  public void punctuate(long l) {
    System.out.println("###### Sensor Punctuator started #########");

    KeyValueIterator<Long, SensorDetails> iter = stateStore.all();
    Date delete_before_date = Date.from(LocalDateTime.now()
                                                     .minusDays(DELETE_X_DAYS_BEFORE)
                                                     .atZone(ZoneId.of("Europe/Istanbul"))
                                                     .toInstant());

    System.out.println (" Delete_before_date :: "+delete_before_date);
    List<Long> goodieListeners = new ArrayList<> ();
    while (iter.hasNext()) {
      KeyValue<Long, SensorDetails> entry = iter.next();
      System.out.println ("  ======> Entry :: key "+entry.key +" value "+entry.value);
      try {
        // If date is older than 5 days, delete with schedule which run every 60 seconds for test.
        if (DATE_FORMAT.parse(entry.value.getSensorData ().getEvt_time ()).before(delete_before_date)) {
          System.out.println (" &&&&&&&&&& --> Key's which are eligible for delete, key:: "+entry.key);
          stateStore.delete(entry.key);
        } else {
          //System.out.println ("**** Perform any update to the record's, if required and insert into the statestore ***** No Action");
          //stateStore.put(entry.key, entry.value);
        }
      } catch (ParseException e) {
        System.out.println("ERROR: " + entry.toString());
        System.out.println(e.toString());
      }
    }
    iter.close();

    // commit the current processing progress
    context.commit();
    System.out.println("######  Sensor Punctuator End #########");
  }
}
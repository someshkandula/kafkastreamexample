package com.cloudleaf.behaviorengine.controller;

import com.cloudleaf.behaviorengine.model.SensorDetails;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequiredArgsConstructor
public class RestService {
    private final StreamsBuilderFactoryBean factoryBean;

    @GetMapping("/getAllSensorDetails")
    public Map<Long, SensorDetails> getSensorDetails(@RequestParam(required = true, defaultValue="sensor-ktable") String storeName){
        final KafkaStreams kafkaStreams =  factoryBean.getKafkaStreams();
        System.out.println ("storeName "+storeName);
        Map<Long, SensorDetails> map = new HashMap<> ();
        ReadOnlyKeyValueStore<Long, SensorDetails> sensorDetails = null;
        try {
            sensorDetails = kafkaStreams.store (
                    StoreQueryParameters.fromNameAndType (storeName,
                            QueryableStoreTypes.keyValueStore ()));

            for (KeyValueIterator<Long, SensorDetails> it = sensorDetails.all (); it.hasNext (); ) {
                KeyValue<Long, SensorDetails> sDetails = it.next ();
                map.put (sDetails.key, sDetails.value);
            }
        } catch(Exception e){
            e.printStackTrace ();
        }
        return map;
    }

} 
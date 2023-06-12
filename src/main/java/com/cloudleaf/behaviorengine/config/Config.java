package com.cloudleaf.behaviorengine.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.streams.StreamsConfig.REPLICATION_FACTOR_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.consumerPrefix;

@Configuration
public class Config {

    @Value ("${spring.kafka.streams.bootstrap-servers}")
    private String KAFKA_BOOTSTRAP_SERVERS;
    @Value ("${spring.kafka.streams.consumer.auto.offset.reset}")
    private String CONSUMER_AUTO_OFFSET_RESET;
    @Value ("${spring.kafka.streams.sensor.application-id}")
    private String SENSOR_APPLICATION_ID;
    @Value ("${spring.kafka.streams.sensor.in.topic}")
    private String SENSOR_IN_TOPIC;
    @Value ("${spring.kafka.streams.sensor.out.topic}")
    private String SENSOR_OUT_TOPIC;
    @Value ("${spring.kafka.streams.sensor.ktable.name}")
    private String SENSOR_KTABLE_NAME;
    @Bean("sensorSBFactoryBean")
    @Primary
    public StreamsBuilderFactoryBean sensorSBFactoryBean()
            throws Exception {
        Map<String, Object> map = new HashMap<> ();
        map.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String ().getClass ().getName ());
        map.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String ().getClass ().getName ());
        map.put (StreamsConfig.APPLICATION_ID_CONFIG, SENSOR_APPLICATION_ID);
        map.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        map.put(consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), CONSUMER_AUTO_OFFSET_RESET);
        // hard coded
        map.put (StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        map.put (StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class.getName());
        map.put (StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
        map.put (REPLICATION_FACTOR_CONFIG, 1);
        map.put( StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka/"+Math.random ());

        KafkaStreamsConfiguration kafkaStreamsConfigConfiguration = new KafkaStreamsConfiguration (map);
        StreamsBuilderFactoryBean streamsBuilderFactoryBean = new StreamsBuilderFactoryBean(kafkaStreamsConfigConfiguration);
        streamsBuilderFactoryBean.afterPropertiesSet();
        System.out.println ("SINGER_IN_TOPIC "+SENSOR_IN_TOPIC +" , SENSOR_OUT_TOPIC "+ SENSOR_OUT_TOPIC +" , SENSOR_KTABLE_NAME "+ SENSOR_KTABLE_NAME);
        streamsBuilderFactoryBean.setInfrastructureCustomizer(new SensorInfrastructureCustomizer(SENSOR_IN_TOPIC, SENSOR_OUT_TOPIC, SENSOR_KTABLE_NAME));
        streamsBuilderFactoryBean.setCloseTimeout(10); //10 seconds
        return streamsBuilderFactoryBean;
    }
}

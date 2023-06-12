package com.cloudleaf.behaviorengine.producers;

import com.cloudleaf.behaviorengine.model.SensorData;
import com.cloudleaf.behaviorengine.model.SensorDetails;
import lombok.SneakyThrows;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.*;

public class SensorDetailsProducer {

    @Autowired
    public NewTopic sensorInTopic;

    @Autowired
    public NewTopic sensorOutTopic;

    public static void main(String[] args) {
        Map<String, Object> map = new HashMap<> ();
        map.put (ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "a14b267fa546e46449f4603acb5ffed6-535234883.ap-south-1.elb.amazonaws.com:9094");
        map.put (ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.LongSerializer");
        map.put (ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.springframework.kafka.support.serializer.JsonSerializer");

        KafkaProducer<Long, SensorDetails> sensorDetailsKafkaProducer = new KafkaProducer<> (map);

        //yyyy-MM-dd HH:mm:ss

        List<SensorDetails> list = new ArrayList<> ();

        populateList (list, "456786753", "2023-01-02 10:00:10", "push" , "Sendum","Location", "Exit", "05", "23232-09993-32932_Sendum");
        populateList (list, "456786754", "2023-01-01 10:00:10", "push" , "Sendum","Location", "Exit", "01", "23232-09993-32932_Sendum");
        populateList (list, "456786755", "2023-01-03 10:00:10", "push" , "Flighaware","Location", "Entry", "02", "98765-09993-34523_FlightAware");
        populateList (list, "456786756", "2023-01-03 10:00:10", "push" , "Flighaware","Location", "Entry", "03", "98765-09993-34523_FlightAware");
        populateList (list, "456786757", "2023-01-04 10:00:10", "push" , "Sendum","Temperature", "C", "04", "98765-09993-34523_Sendum");
        populateList (list, "456786758", "2023-01-05 10:00:10", "push" , "Sendum","Temperature", "C", "04", "98765-09993-34523_Sendum");
        populateList (list, "456786759", "2023-01-06 10:00:10", "push" , "Sendum","Temperature", "C", "04", "98765-09993-34523_Sendum");

        //populateList (list, "456786758", "2022-12-12 10:00:10", "push" , "Sendum","Temperature", "C", "05", "98765-09993-34523_Sendum");
        //populateList (list, "456786759", "2022-12-09 13:19:50", "push" , "Sendum","Temperature", "C", "05", "98765-09993-34523_Sendum");

        System.out.println (" list ==> " + list.size ());

        list.stream ()
                .map (sensorDetails -> new ProducerRecord<> ("sensor-in-topic", Long.valueOf (sensorDetails.getSensorId ()), sensorDetails))
                .forEach (record -> send (sensorDetailsKafkaProducer, record));
    }

    @SneakyThrows
    private static void send(KafkaProducer<Long, SensorDetails> favoriteSingersProducer, ProducerRecord<Long, SensorDetails> record) {
        RecordMetadata future
                = favoriteSingersProducer.send(record).get ();
        System.out.println ("Post publish::: ****> "+future.topic ()+" ,"+future.partition ());
    }


    //populateList (list, "efd28608-83c2-4dea-a369-b38194af8728", "2022-12-10 10:00:10", "push" , "Location", "Exit", "10", "23232-09993-32932_Sendum");

    private static void populateList(List<SensorDetails> list, String sensorId, String dateParameter, String cmd, String vendorName, String sId, String cId, String value, String leafId) {
        list.add (SensorDetails.builder()
                .sensorId (sensorId)
                .createDate (dateParameter)
                .cloudDate (dateParameter)
                .command (cmd)
                .vendor (vendorName)
                .sensorData (new SensorData (dateParameter, sId,cId,value,leafId, sensorId))
                .build());
    }

}

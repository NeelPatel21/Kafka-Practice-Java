package kafkatest;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.Scanner;

public class Consumer {
    public static void main(String... ar){
        Scanner sc = new Scanner(System.in);
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","192.168.138.130:9092");
        prop.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        prop.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        prop.setProperty("group.id","1");

        try(KafkaConsumer consumer = new KafkaConsumer(prop)){
            consumer.subscribe(Arrays.asList("test"));
            for(;true;){
//                System.out.println(consumer.listTopics());
                ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(10000));
//                System.out.println(records.count());
                for(ConsumerRecord<String,String> record: records)
                    System.out.println(record.partition()+" : " +record.offset()+" : "+record.timestamp()
                            +" : "+record.key()+" : "+record.value());
            }
        }
    }
}

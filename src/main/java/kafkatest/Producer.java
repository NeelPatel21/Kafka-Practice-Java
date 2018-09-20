package kafkatest;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Scanner;

public class Producer {
    public static void main(String... ar){
        Scanner sc = new Scanner(System.in);
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","192.168.138.130:9092");
        prop.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        prop.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(prop);
        for(;true;)
            producer.send(new ProducerRecord<>("test",sc.nextLine()));
    }
}

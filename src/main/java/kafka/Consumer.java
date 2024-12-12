package kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;
import java.util.Collections;

public class Consumer {
  public static void main(String[] args) {
    String topic = "filtered_articles";

    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Collections.singletonList(topic));

    try {
      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(1000);

        records.forEach(record -> {
          System.out.println("Received Data: " + record.value());
        });
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      consumer.close();
    }
  }
}

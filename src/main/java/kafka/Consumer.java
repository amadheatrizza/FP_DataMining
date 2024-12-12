package kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import org.apache.http.HttpHost;

import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.Properties;
import java.util.Collections;

public class Consumer {
  public static void main(String[] args) {
    String topic = "filtered_articles";
    String esHost = "localhost";
    int esPort = 9200;

    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Collections.singletonList(topic));

    try (RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost(esHost, esPort, "http")))){
      while (true) {
      ConsumerRecords<String, String> records = consumer.poll(java.time.Duration.ofMillis(1000));

      records.forEach(record -> {
        String id = record.key();
        String value = record.value();

        IndexRequest request = new IndexRequest("articles").id(id).source(value, XContentType.JSON);

        try {
          client.index(request, RequestOptions.DEFAULT);
          System.out.println("Indexed: " + id);
        } catch (Exception e) {
          System.err.println("Failed to index record with id: " + id);
          e.printStackTrace();
        }
      });
      }
    } catch (Exception e) {
      System.err.println("Error occurred in consumer loop");
      e.printStackTrace();
    } finally {
      consumer.close();
    }
  }
}

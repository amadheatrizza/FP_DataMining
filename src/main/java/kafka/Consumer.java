package kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import org.apache.http.HttpHost;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;

import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import co.elastic.clients.transport.ElasticsearchTransport;

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
    
    RestClientBuilder builder = RestClient.builder(new HttpHost(esHost, esPort));
    RestClient restClient = builder.build();
    RestClientTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
    ElasticsearchClient client = new ElasticsearchClient(transport);

    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Collections.singletonList(topic));

    try {
      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(java.time.Duration.ofMillis(100));

        records.forEach(record -> {
          System.out.println("Received Data: " + record.value());
            
          IndexRequest<Object> indexRequest = new IndexRequest.Builder<Object>()
            .index("articles")
            .document(JsonData.fromJson(record.value()))
            .build();
            
          try {
            client.index(indexRequest);
          } catch (Exception e) {
            e.printStackTrace();
          }
        });
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        client.close();
      } catch (Exception e) {
        e.printStackTrace();
      }

      consumer.close();
    }
  }
}

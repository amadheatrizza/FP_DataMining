package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;

import org.json.JSONArray;
import org.json.JSONObject;

import java.net.HttpURLConnection;
import java.net.URL;

import java.util.Properties;

public class Producer {

  public static void main(String[] args) {
    String topic = "raw_articles";
    String apiKey = "f486f07e2dc644ca9e069cd2bee2916b";
    Integer articleCount = 3; // Number of articles to fetch

    // Articles API with keyword "bitcoin", sort by latest articles
    String url = "https://newsapi.org/v2/everything?q=bitcoin&apiKey=" + apiKey;

    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    props.put(ProducerConfig.ACKS_CONFIG, "all");

    KafkaProducer<String, String> producer = new KafkaProducer<>(props);

    try {
      HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
      connection.setRequestMethod("GET");

      int responseCode = connection.getResponseCode();

      if (responseCode == HttpURLConnection.HTTP_OK) {
        String response = new String(connection.getInputStream().readAllBytes());

        JSONObject jsonResponse = new JSONObject(response);
        JSONArray articles = jsonResponse.getJSONArray("articles");

        for (int i = 0; i < Math.min(articleCount, articles.length()); i++) {
          JSONObject article = articles.getJSONObject(i);
          producer.send(new ProducerRecord<>(topic, article.toString()));
          System.out.println("Sent Data: " + article.toString());
        }

        producer.flush();

        Thread.sleep(60000);
      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      producer.close();
    }
  }
}

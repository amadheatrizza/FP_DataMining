package kafka;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import org.json.JSONObject;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.net.HttpURLConnection;
import java.net.URL;

import java.util.Properties;
import java.util.UUID;

public class Streamer {
  private static String getContent(String url) {
    try {
      Document doc = Jsoup.connect(url).get();

      return doc.text();
    } catch (Exception e) {
      System.err.println("Error fetching or parsing content from URL: " + url);
      e.printStackTrace();
      return "";
    }
  }

  private static String preprocess(String value) {
    try {
      JSONObject jsonData = new JSONObject(value);
      
      if (!jsonData.has("url")) {
        System.err.println("Invalid JSON data! Missing URL field.");
        return value;
      }

      String url = jsonData.getString("url");
      String content = getContent(url);

      jsonData.put("content", content);
    
      return jsonData.toString();
    } catch (Exception e) {
      System.err.println("Error parsing JSON data: " + value);
      e.printStackTrace();
      return value;
    }
  }

  public static void main(String[] args) {
    String inputTopic = "raw_articles";
    String outputTopic = "filtered_articles";

    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streamer");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, "org.apache.kafka.common.serialization.Serdes$StringSerde");
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, "org.apache.kafka.common.serialization.Serdes$StringSerde");

    StreamsBuilder builder = new StreamsBuilder();

    KStream<String, String> rawDataStream = builder.stream(inputTopic);
    KStream<String, String> filteredDataStream = rawDataStream.map((key, value) -> {
      String cleanValue = preprocess(value);
      String newKey = UUID.randomUUID().toString();
  
      return new KeyValue<>(newKey, cleanValue);
    });

    filteredDataStream.to(outputTopic);

    KafkaStreams streams = new KafkaStreams(builder.build(), props);
    streams.start();

    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }
}
